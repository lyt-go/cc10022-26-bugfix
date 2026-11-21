"""自动匹配任务领域服务：匹配任务调度、并行匹配执行与簇结果输出。

补跑（失败任务重跑）约定：

* 任务首次调度时保存输入记录快照（原始成员）和当时的规则快照；
* 失败任务重跑必须使用同一份输入记录重新计算，不能只把状态改成等待中；
* 同一任务的并发重试只允许确认一份成功输出，重复请求返回 duplicate；
* 输出簇同时保留成员身份、所依据的规则版本和任务发生时间；
* 输入成员不可恢复时，任务详情给出明确的失败原因，任务保持已失败。
"""
from __future__ import annotations

import copy
import datetime
import itertools
import threading
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 并发重试同一任务时，只允许一个线程完成确认并写出补跑结果。
_RETRY_LOCK = threading.RLock()

# 任务编号序列，避免同一秒内并发调度得到相同编号。
_TASK_SEQUENCE_LOCK = threading.Lock()
_TASK_SEQUENCE = 0

# 兼容任务记录被外部直接写入时使用的输入字段名，按优先级读取。
_INPUT_FIELD_NAMES = (
    "原始记录",
    "原始成员数据",
    "input_records",
    "inputRecords",
    "输入记录",
    "数据快照",
    "记录快照",
    "records_snapshot",
    "原始数据",
    "成员记录",
    "source_records",
    "records",
    "members",
)

_FAILED_STATUSES = {"已失败", "失败", "failed", "FAILED"}
_COMPLETED_STATUSES = {"已完成", "完成", "completed", "COMPLETED"}

RETRY_ACTIONS = {"重试", "重试匹配", "重新匹配", "补跑", "重新运行", "retry"}


def _utcnow() -> str:
    return datetime.datetime.utcnow().isoformat()


class AutoMatchJobService(BaseModuleService):
    MODULE = "auto_match_job"
    ENTITY = "匹配任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "实体模型", "匹配规则", "数据范围", "并行度", "匹配策略", "输出簇数", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "实体模型", "匹配规则"]
    STATUS_ORDER = ["草稿", "等待中", "运行中", "已完成", "已失败"]
    ACTION_RULES = {
        "提交运行": "草稿",
        "停止匹配": "运行中",
        "确认完成": "已完成",
        "重试匹配": "已完成",
    }
    NEGATIVE_ACTIONS = []

    # ── 基础工具 ──────────────────────────────────────────────
    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _status_of(job: dict[str, Any]) -> str:
        return str(job.get("任务状态") or job.get("status") or "")

    @classmethod
    def _is_failed(cls, job: dict[str, Any]) -> bool:
        return cls._status_of(job) in _FAILED_STATUSES

    @classmethod
    def _is_completed(cls, job: dict[str, Any]) -> bool:
        return cls._status_of(job) in _COMPLETED_STATUSES

    @classmethod
    def _next_task_id(cls) -> str:
        global _TASK_SEQUENCE
        with _TASK_SEQUENCE_LOCK:
            _TASK_SEQUENCE += 1
            sequence = _TASK_SEQUENCE
        stamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        return f"TASK-{stamp}-{sequence:04d}"

    @staticmethod
    def _member_identifier(record: dict[str, Any], index: int, task_id: str,
                           used: set[str] | None = None) -> str:
        """取原始成员身份：优先使用记录自带的业务主键，缺失时回退到任务内序号。"""
        identifier = ""
        for key in (
            "成员标识", "记录ID", "record_id", "来源记录", "source_id",
            "客户编号", "客户ID", "id",
        ):
            value = record.get(key)
            if value is not None and str(value).strip():
                identifier = str(value).strip()
                break
        if not identifier:
            identifier = f"{task_id}-M{index + 1}"
        if used is not None:
            candidate = identifier
            suffix = 1
            while candidate in used:
                suffix += 1
                candidate = f"{identifier}#{suffix}"
            identifier = candidate
            used.add(identifier)
        return identifier

    def _snapshot_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "规则编号", "实体模型", "匹配字段", "匹配算法", "相似度阈值",
            "模糊系数", "权重分配", "规则版本", "status",
        )
        snapshot = {key: copy.deepcopy(rule.get(key)) for key in keys if key in rule}
        snapshot["规则ID"] = rule.get("id")
        return snapshot

    @staticmethod
    def _rule_version(rule: dict[str, Any]) -> str:
        version = rule.get("规则版本")
        if version is None or not str(version).strip():
            version = "1.0"
        return str(version)

    # ── 规则解析 ──────────────────────────────────────────────
    def resolve_rule(self, rule_ref: Any) -> tuple[dict[str, Any] | None, str]:
        """按规则 ID 或规则编号解析可用匹配规则。

        补跑时使用匹配规则引擎中当前生效的规则配置（例如已更正的阈值），
        停用规则不参与补跑，避免用失效规则产出重复名单。
        """
        from app.services.match_rule import MatchRuleService

        rule_svc = MatchRuleService()
        rule: dict[str, Any] | None = None
        rule_id: int | None = None
        try:
            rule_id = int(str(rule_ref))
        except (TypeError, ValueError):
            rule_id = None
        if rule_id is not None:
            rule = rule_svc.get_entry(rule_id)
        if rule is None:
            for row in store.rows("match_rule"):
                if str(row.get("规则编号")) == str(rule_ref):
                    rule = row
                    break
        if rule is None:
            return None, f"匹配规则 {rule_ref} 不存在，无法补跑"
        if str(rule.get("status") or rule.get("处置状态") or "") == "已停用":
            return None, f"匹配规则 {rule_ref} 已停用，无法补跑"
        return rule, ""

    # ── 输入成员恢复 ──────────────────────────────────────────
    def resolve_input_records(self, job: dict[str, Any]) -> tuple[list[dict[str, Any]] | None, str]:
        """恢复任务首次执行时的原始成员记录。

        优先读取任务自身的输入快照；其次按任务编号从内部快照登记表恢复；
        最后尝试从该任务已发布的重复簇成员中还原。全部失败时返回原因，
        由任务详情展示，绝不用空输入伪造一份补跑结果。
        """
        for field in _INPUT_FIELD_NAMES:
            value = job.get(field)
            if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
                return copy.deepcopy(value), ""

        task_id = str(job.get("任务编号") or "")
        if task_id and task_id in _JOB_INPUTS:
            return copy.deepcopy(_JOB_INPUTS[task_id]), ""

        # 从该任务已发布的簇成员中恢复，按记录序号排序。
        recovered: list[tuple[int, dict[str, Any]]] = []
        for cluster in store.rows("dup_cluster"):
            if str(cluster.get("处置批次")) != task_id:
                continue
            for member in cluster.get("成员", []) or []:
                if not isinstance(member, dict):
                    continue
                record = member.get("原始记录") or member.get("记录")
                if isinstance(record, dict):
                    recovered.append((self._safe_int(member.get("记录序号"), len(recovered)), record))
        if recovered:
            recovered.sort(key=lambda item: item[0])
            return [copy.deepcopy(record) for _, record in recovered], ""

        return None, "无法恢复原始成员数据：任务未保存输入记录快照，也没有可还原的簇成员记录"

    # ── 簇结果构建与发布 ──────────────────────────────────────
    def _build_cluster_results(
        self,
        clusters: list[list[int]],
        records: list[dict[str, Any]],
        rule: dict[str, Any],
        job: dict[str, Any],
        executed_at: str,
    ) -> list[dict[str, Any]]:
        from app.services.match_rule import MatchRuleService

        rule_svc = MatchRuleService()
        task_id = str(job.get("任务编号") or "")
        rule_ref = str(rule.get("规则编号") or rule.get("id"))
        rule_version = self._rule_version(rule)
        original_task_time = job.get("原任务时间") or job.get("任务发生时间") or executed_at
        used_identifiers: set[str] = set()
        results: list[dict[str, Any]] = []

        for sequence, indexes in enumerate(clusters, start=1):
            members: list[dict[str, Any]] = []
            for index in indexes:
                record = records[index]
                member_id = self._member_identifier(record, index, task_id, used_identifiers)
                members.append({
                    "记录序号": index,
                    "成员标识": member_id,
                    "原始记录": copy.deepcopy(record),
                })

            similarities: list[float] = []
            for left, right in itertools.combinations(indexes, 2):
                similarities.append(rule_svc.compute_similarity(rule["id"], records[left], records[right]))
            average = round(sum(similarities) / len(similarities), 4) if similarities else 0.0

            results.append({
                "簇序号": sequence,
                "簇编号": f"DUP-{task_id}-C{sequence}",
                "成员索引": list(indexes),
                "成员": members,
                "成员标识": [member["成员标识"] for member in members],
                "簇大小": len(members),
                "相似度均值": average,
                "匹配规则": rule_ref,
                "规则ID": rule.get("id"),
                # 规则依据与任务发生时间分列保存，不能拼成同一个版本号。
                "规则版本": rule_version,
                "任务编号": task_id,
                "任务发生时间": executed_at,
                "原任务时间": original_task_time,
            })
        return results

    def _publish_review_clusters(self, job: dict[str, Any],
                                 results: list[dict[str, Any]],
                                 executed_at: str) -> list[dict[str, Any]]:
        """把补跑簇发布为审核人员可处置的重复簇记录。

        以「处置批次 + 簇序号」作为唯一键更新或新增，并发重试不会产生
        重复的复核入口；本次输出变少时清除该任务遗留的多余簇。
        """
        task_id = str(job.get("任务编号") or "")
        rows = store.rows("dup_cluster")
        existing = {
            self._safe_int(row.get("簇序号"), -1): row
            for row in rows
            if str(row.get("处置批次")) == task_id and "簇序号" in row
        }
        published: list[dict[str, Any]] = []
        kept_keys: set[int] = set()

        for result in results:
            sequence = int(result["簇序号"])
            kept_keys.add(sequence)
            row = existing.get(sequence)
            if row is None:
                row = {
                    "id": max((self._safe_int(item.get("id")) for item in rows), default=0) + 1,
                    "簇编号": result["簇编号"],
                    "实体模型": job.get("实体模型"),
                    "簇大小": str(result["簇大小"]),
                    "相似度均值": str(result["相似度均值"]),
                    "匹配规则": result["匹配规则"],
                    "创建时间": executed_at,
                    "处置状态": "待确认",
                    "处置批次": task_id,
                    "status": "待确认",
                    "pending": True,
                    "abnormal": False,
                }
                rows.append(row)

            row.update({
                "簇编号": result["簇编号"],
                "实体模型": job.get("实体模型"),
                "簇大小": str(result["簇大小"]),
                "相似度均值": str(result["相似度均值"]),
                "匹配规则": result["匹配规则"],
                "创建时间": executed_at,
                "处置状态": row.get("处置状态") or "待确认",
                "处置批次": task_id,
                "任务编号": task_id,
                "任务发生时间": executed_at,
                "簇序号": sequence,
                "成员": copy.deepcopy(result["成员"]),
                "成员标识": list(result["成员标识"]),
                "规则版本": result["规则版本"],
                "status": row.get("status") or "待确认",
            })
            published.append(row)

        # 清理同一任务下本次不再输出的旧簇，保持详情数量与复核入口一致。
        for sequence, row in existing.items():
            if sequence not in kept_keys:
                try:
                    rows.remove(row)
                except ValueError:
                    pass
        return published

    def _review_entry(self, job: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        task_id = str(job.get("任务编号") or "")
        cluster_ids = [result["簇编号"] for result in results]
        return {
            "module": "dup_cluster",
            "处置批次": task_id,
            "任务编号": task_id,
            "簇数量": len(results),
            "簇编号": cluster_ids,
            "path": f"/dup_cluster?处置批次={task_id}",
        }

    # ── 执行核心 ──────────────────────────────────────────────
    def _validate_rule_for_execution(self, rule: dict[str, Any]) -> str:
        """执行前校验规则参数；返回空串表示通过，否则返回失败原因。

        与匹配规则引擎保持一致：非数字配置按默认值参与计算；只有明确
        给出的数字超出 (0,1] 范围时才判定任务无法执行。
        """
        for field, label in (("相似度阈值", "相似度阈值"), ("模糊系数", "模糊系数")):
            raw = rule.get(field)
            if raw is None or str(raw).strip() == "":
                continue
            try:
                value = float(str(raw))
            except (TypeError, ValueError):
                continue
            if not 0 < value <= 1:
                return f"{label} {raw!r} 超出 (0,1] 范围，无法执行匹配"
        return ""

    def _execute_and_record(self, job: dict[str, Any], records: list[dict[str, Any]],
                            rule: dict[str, Any], *, is_retry: bool,
                            input_reason: str = "") -> tuple[bool, str]:
        """执行匹配并落库；规则无效或计算异常时把任务标记为已失败。"""
        rule_error = self._validate_rule_for_execution(rule)
        if rule_error:
            self._record_failure(job, rule_error, records_recoverable=True)
            return False, rule_error
        try:
            self._execute_job(job, records, rule, is_retry=is_retry, input_reason=input_reason)
        except Exception as exc:  # 计算失败必须留痕并允许后续重跑
            reason = f"首次执行失败：{exc}" if not is_retry else f"补跑执行失败：{exc}"
            self._record_failure(job, reason, records_recoverable=True)
            return False, reason
        return True, ""

    def _record_failure(self, job: dict[str, Any], reason: str,
                        *, records_recoverable: bool) -> None:
        job.update({
            "任务状态": "已失败",
            "status": "已失败",
            "pending": False,
            "abnormal": True,
            "输入成员可恢复": records_recoverable,
            "失败原因": reason,
            "failure_reason": reason,
            "reason": reason,
            "retry_confirmed": False,
            "confirmed_output": False,
            "失败时间": _utcnow(),
        })

    def _execute_job(self, job: dict[str, Any], records: list[dict[str, Any]],
                     rule: dict[str, Any], *, is_retry: bool,
                     input_reason: str = "") -> list[dict[str, Any]]:
        """用给定输入和规则重新计算簇结果，并写回任务与复核入口。"""
        from app.services.match_rule import MatchRuleService

        rule_svc = MatchRuleService()
        clusters = rule_svc.generate_duplicate_clusters(rule["id"], records)
        executed_at = _utcnow()
        if is_retry:
            # 补跑是一次新的执行：任务发生时间取本次执行时间，
            # 原任务时间单独保留，两者不合并。
            job.setdefault("原任务时间", job.get("任务发生时间") or executed_at)
            task_time = executed_at
        else:
            task_time = job.get("任务发生时间") or executed_at

        results = self._build_cluster_results(clusters, records, rule, job, executed_at)
        review_entry = self._review_entry(job, results)
        published = self._publish_review_clusters(job, results, executed_at)

        # 首次执行时冻结原始成员与规则；补跑保留原件并另存本次依据。
        if not job.get("原始记录"):
            job["原始记录"] = copy.deepcopy(records)
        job["原始成员"] = [
            self._member_identifier(record, index, str(job.get("任务编号") or ""))
            for index, record in enumerate(records)
        ]
        if not job.get("规则快照"):
            job["规则快照"] = self._snapshot_rule(rule)
        job["本次规则快照"] = self._snapshot_rule(rule)
        if is_retry:
            job["补跑规则快照"] = self._snapshot_rule(rule)

        job.update({
            "任务状态": "已完成",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "输出簇数": str(len(results)),
            "clusters": clusters,
            "簇结果": results,
            "cluster_results": results,
            "成员数": sum(result["簇大小"] for result in results),
            "规则版本": self._rule_version(rule),
            "执行时间": executed_at,
            "任务发生时间": task_time,
            "执行规则时间": task_time,
            "复核入口": review_entry["path"],
            "复核入口明细": review_entry,
            "review_entry": review_entry,
            "复核簇数": len(published),
            "输入成员可恢复": True,
            "失败原因": "",
            "failure_reason": "",
        })
        if is_retry:
            job["补跑结果"] = results
            job["补跑成员数"] = sum(result["簇大小"] for result in results)
            job["补跑执行时间"] = executed_at
        else:
            job["补跑结果"] = []
            job["补跑"] = False
        job["输入恢复说明"] = input_reason
        return results

    # ── 首次调度 ──────────────────────────────────────────────
    def schedule_match_job(self, rule_id: int, entity_model: str,
                           records: list[dict[str, Any]]) -> dict[str, Any]:
        """调度匹配任务：按匹配规则对实体模型记录执行自动匹配和去重检测。

        首次执行即冻结输入记录快照与规则快照，后续失败补跑可以凭同一份
        输入重新计算；输出簇同时记录成员身份、规则版本与任务发生时间。
        """
        rule, reason = self.resolve_rule(rule_id)
        if not rule:
            return {"ok": False, "reason": reason}
        if not isinstance(records, list):
            return {"ok": False, "reason": "匹配输入记录必须是列表"}

        records_snapshot = copy.deepcopy(records)
        with _RETRY_LOCK:
            task_id = self._next_task_id()
            job = {
                "id": max((self._safe_int(row.get("id")) for row in store.rows(self.MODULE)), default=0) + 1,
                "任务编号": task_id,
                "实体模型": entity_model,
                "匹配规则": str(rule.get("规则编号") or rule_id),
                "数据范围": f"{entity_model}:{len(records_snapshot)}条",
                "并行度": "4",
                "匹配策略": rule.get("匹配策略", "全量匹配"),
                "输出簇数": "0",
                "任务状态": "运行中",
                "status": "运行中",
                "pending": True,
                "abnormal": False,
                "原始记录": copy.deepcopy(records_snapshot),
                "规则快照": self._snapshot_rule(rule),
                "任务发生时间": _utcnow(),
                "retry_count": 0,
                "retry_confirmed": False,
                "confirmed_output": False,
            }
            store.rows(self.MODULE).append(job)
            _JOB_INPUTS[task_id] = copy.deepcopy(records_snapshot)
            succeeded, failure_reason = self._execute_and_record(
                job, records_snapshot, rule, is_retry=False)

        if not succeeded:
            return {
                "ok": False,
                "reason": failure_reason,
                "job": job,
                "clusters": [],
                "clusters_found": 0,
                "records_in_clusters": 0,
                "cluster_results": [],
            }

        results = job.get("簇结果") or []
        return {
            "ok": True,
            "job": job,
            "clusters": job.get("clusters", []),
            "clusters_found": len(results),
            "records_in_clusters": sum(result["簇大小"] for result in results),
            "cluster_results": results,
            "补跑结果": [],
            "review_entry": job.get("review_entry"),
        }

    # ── 失败任务重跑 ──────────────────────────────────────────
    def retry_failed_job(self, job_id: int) -> dict[str, Any]:
        """重跑单个失败任务：恢复原始成员与可用规则，重新计算补跑簇。

        同一任务并发重试时由锁串行化，只有第一次会确认成功输出；
        后续调用返回 duplicate=True，不重复写簇、不重复确认。
        """
        with _RETRY_LOCK:
            job = store.find(self.MODULE, int(job_id))
            if job is None:
                return {"ok": False, "reason": f"匹配任务 {job_id} 不存在", "duplicate": False}

            if job.get("retry_confirmed") or self._is_completed(job):
                return {
                    "ok": False,
                    "duplicate": True,
                    "reason": "任务已经确认成功输出，未重复确认",
                    "job": job,
                    "clusters": job.get("补跑结果") or job.get("簇结果") or [],
                }
            if not self._is_failed(job):
                return {
                    "ok": False,
                    "duplicate": False,
                    "reason": f"任务当前状态为{self._status_of(job) or '未知'}，只有已失败任务可以重试",
                    "job": job,
                }

            records, input_reason = self.resolve_input_records(job)
            if records is None:
                # 成员数据不可恢复：保持已失败，把原因写进任务详情。
                job.update({
                    "任务状态": "已失败",
                    "status": "已失败",
                    "pending": False,
                    "abnormal": True,
                    "输入成员可恢复": False,
                    "失败原因": input_reason,
                    "failure_reason": input_reason,
                    "reason": input_reason,
                    "最后重试时间": _utcnow(),
                })
                job["retry_count"] = self._safe_int(job.get("retry_count")) + 1
                return {"ok": False, "duplicate": False, "reason": input_reason, "job": job}

            rule, rule_reason = self.resolve_rule(job.get("匹配规则") or job.get("rule_id"))
            if rule is None:
                job.update({
                    "任务状态": "已失败",
                    "status": "已失败",
                    "pending": False,
                    "abnormal": True,
                    "输入成员可恢复": True,
                    "失败原因": rule_reason,
                    "failure_reason": rule_reason,
                    "reason": rule_reason,
                    "最后重试时间": _utcnow(),
                })
                job["retry_count"] = self._safe_int(job.get("retry_count")) + 1
                return {"ok": False, "duplicate": False, "reason": rule_reason, "job": job}

            # 保留首次执行的信息，补跑规则与时间另存，不能合并成一份版本。
            if not job.get("原任务时间"):
                job["原任务时间"] = job.get("任务发生时间") or job.get("执行时间") or _utcnow()
            if not job.get("原规则快照"):
                job["原规则快照"] = copy.deepcopy(job.get("规则快照") or {})
            if not job.get("原规则版本"):
                job["原规则版本"] = (
                    (job.get("规则快照") or {}).get("规则版本")
                    or job.get("规则版本")
                    or ""
                )
            job["补跑"] = True
            job["补跑输入条数"] = len(records)
            job["补跑使用的规则"] = str(rule.get("规则编号") or rule.get("id"))
            job["补跑规则版本"] = self._rule_version(rule)
            succeeded, failure_reason = self._execute_and_record(
                job, records, rule, is_retry=True, input_reason=input_reason)

            job["retry_count"] = self._safe_int(job.get("retry_count")) + 1
            job["最后重试时间"] = _utcnow()
            if not succeeded:
                return {
                    "ok": False,
                    "duplicate": False,
                    "reason": failure_reason,
                    "job": job,
                    "clusters": job.get("补跑结果") or [],
                }
            job["retry_confirmed"] = True
            job["confirmed_output"] = True
            job["确认输出时间"] = job["最后重试时间"]
            _JOB_INPUTS[str(job.get("任务编号"))] = copy.deepcopy(records)

        results = job.get("补跑结果") or []
        return {
            "ok": True,
            "duplicate": False,
            "job": job,
            "clusters": job.get("clusters", []),
            "cluster_results": results,
            "补跑结果": results,
            "review_entry": job.get("review_entry"),
            "message": f"匹配任务 {job.get('任务编号')} 已补跑完成，输出 {len(results)} 个簇",
        }

    # 常用别名，便于路由层和其它服务按不同语义调用。
    def rerun_failed_job(self, job_id: int) -> dict[str, Any]:
        return self.retry_failed_job(job_id)

    def retry_failed_jobs(self, entity_model: str | None = None) -> int:
        """重试已失败的匹配任务：恢复原始输入并重新调度模糊匹配计算。

        返回本次真正确认成功的任务数；无法恢复成员数据或规则不可用的
        任务保持已失败，并在任务详情中写明原因。
        """
        with _RETRY_LOCK:
            failed_jobs = [
                job for job in store.rows(self.MODULE)
                if self._is_failed(job)
                and (entity_model is None or job.get("实体模型") == entity_model)
            ]
            retried = 0
            for job in failed_jobs:
                result = self.retry_failed_job(self._safe_int(job.get("id")))
                if result.get("ok"):
                    retried += 1
            return retried

    def mark_job_failed(self, job_id: int, reason: str = "") -> dict[str, Any] | None:
        """把任务标记为已失败，保留输入快照，供重试链路使用。"""
        with _RETRY_LOCK:
            job = store.find(self.MODULE, int(job_id))
            if job is None:
                return None
            job.update({
                "任务状态": "已失败",
                "status": "已失败",
                "pending": False,
                "abnormal": True,
                "retry_confirmed": False,
                "confirmed_output": False,
                "失败原因": reason,
                "failure_reason": reason,
            })
            return job

    # 兼容旧命名。
    def fail_job(self, job_id: int, reason: str = "") -> dict[str, Any] | None:
        return self.mark_job_failed(job_id, reason)

    # ── 任务详情与统计 ────────────────────────────────────────
    def query_job_status(self, job_id: int) -> dict[str, Any] | None:
        """查询匹配任务详情：状态、实际输出簇数量与复核入口保持一致。

        数量来自真实簇结果而不是可能过期的输出簇数字段；同时说明原始成员
        是否可恢复、补跑所用规则版本与任务发生时间（两者分开保存）。
        """
        job = self.get_entry(job_id)
        if not job:
            return None

        results = job.get("簇结果") or job.get("cluster_results") or []
        if not isinstance(results, list):
            results = []
        recorded_output = self._safe_int(job.get("输出簇数"))
        clusters_output = len(results) if results else recorded_output
        member_count = sum(
            self._safe_int(result.get("簇大小"))
            for result in results
            if isinstance(result, dict)
        )

        task_id = str(job.get("任务编号") or "")
        review_rows = [
            row for row in store.rows("dup_cluster")
            if str(row.get("处置批次")) == task_id
        ]
        records, input_reason = self.resolve_input_records(job)
        failure_reason = job.get("失败原因") or job.get("failure_reason") or ""
        if records is None and not failure_reason:
            failure_reason = input_reason

        rule_version = job.get("规则版本")
        if not rule_version and isinstance(job.get("规则快照"), dict):
            rule_version = job["规则快照"].get("规则版本")

        review_entry = job.get("review_entry")
        if not isinstance(review_entry, dict):
            review_entry = {
                "module": "dup_cluster",
                "处置批次": task_id,
                "任务编号": task_id,
                "簇数量": len(results),
                "簇编号": [
                    result.get("簇编号") for result in results if isinstance(result, dict)
                ],
                "path": f"/dup_cluster?处置批次={task_id}",
            }

        return {
            "task_id": job.get("任务编号"),
            "status": self._status_of(job),
            "entity_model": job.get("实体模型"),
            "clusters_output": clusters_output,
            "cluster_count": clusters_output,
            "member_count": member_count,
            "parallelism": self._safe_int(job.get("并行度"), 1),
            "strategy": job.get("匹配策略"),
            "rule_id": job.get("补跑使用的规则") or job.get("匹配规则"),
            # 规则依据（版本）与任务发生时间分开输出，不合成一个版本号。
            "rule_version": rule_version or "",
            "original_rule_version": job.get("原规则版本") or "",
            "retry_rule_version": job.get("补跑规则版本") or "",
            "executed_at": job.get("执行时间") or "",
            "task_time": job.get("任务发生时间") or job.get("原任务时间") or "",
            "original_task_time": job.get("原任务时间") or "",
            "clusters": results,
            "簇结果": results,
            "review_entry": review_entry,
            "复核入口": review_entry.get("path"),
            "review_cluster_count": len(review_rows),
            "retry_count": self._safe_int(job.get("retry_count")),
            "retry_confirmed": bool(job.get("retry_confirmed")),
            "member_recoverable": records is not None,
            "输入成员可恢复": records is not None,
            "failure_reason": failure_reason,
            "失败原因": failure_reason,
            "reason": failure_reason,
        }

    # 兼容不同调用方对「详情」的命名。
    def get_job_detail(self, job_id: int) -> dict[str, Any] | None:
        return self.query_job_status(job_id)

    def query_job_detail(self, job_id: int) -> dict[str, Any] | None:
        return self.query_job_status(job_id)

    def aggregate_job_statistics(self, entity_model: str | None = None) -> dict[str, Any]:
        """聚合匹配任务统计：汇总已完成/失败任务数、输出簇总数和平均并行度。"""
        rows = store.rows(self.MODULE)
        if entity_model:
            rows = [row for row in rows if row.get("实体模型") == entity_model]
        completed = sum(1 for row in rows if row.get("任务状态") == "已完成")
        failed = sum(1 for row in rows if self._is_failed(row))
        total_clusters = sum(self._safe_int(row.get("输出簇数")) for row in rows)
        return {
            "total_jobs": len(rows),
            "completed": completed,
            "failed": failed,
            "total_clusters_output": total_clusters,
            "success_rate": round(completed / max(len(rows), 1) * 100, 2),
        }

    def validate_job_parameters(self, job_id: int) -> dict[str, Any]:
        """校验匹配任务的参数配置是否合理：并行度、匹配策略和数据范围。"""
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "reason": "任务不存在"}
        parallelism = self._safe_int(job.get("并行度"), 1)
        checks = {
            "parallelism_valid": 1 <= parallelism <= 16,
            "has_match_rule": bool(job.get("匹配规则")),
            "has_entity_model": bool(job.get("实体模型")),
            "has_strategy": bool(job.get("匹配策略")),
        }
        checks["all_valid"] = all(checks.values())
        return {"ok": checks["all_valid"], "checks": checks}

    # ── 动作路由 ──────────────────────────────────────────────
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        normalized = (action or "").strip()
        if normalized in RETRY_ACTIONS:
            result = self.retry_failed_job(entry_id)
            if result.get("ok"):
                return result.get("job"), result.get("message") or "匹配任务已重试完成"
            job = result.get("job") if result.get("duplicate") else None
            return job, str(result.get("reason") or "匹配任务重试失败")
        return super().run_action(entry_id, normalized)


# 进程内输入快照登记表：任务记录被裁剪时仍可凭任务编号恢复原始输入。
_JOB_INPUTS: dict[str, list[dict[str, Any]]] = {}
