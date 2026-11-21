"""失败匹配任务补跑链路测试：同输入重算、并发只确认一份、成员与规则可追溯。"""
from __future__ import annotations

import os
import sys
import threading

_backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from app.services.auto_match_job import AutoMatchJobService
from app.services.match_rule import MatchRuleService
from app.store import store


CUSTOMER_RECORDS = [
    {"成员标识": "CUST-1001", "name": "华东贸易有限公司", "phone": "021-55550001"},
    {"成员标识": "CUST-1002", "name": "华东贸易股份有限公司", "phone": "021-55550002"},
    {"成员标识": "CUST-1003", "name": "华东贸易有限公司", "phone": "021-55550003"},
]


class TestFailedJobRetry:
    """验证失败任务重跑按同一份输入重新计算并交付补跑簇。"""

    def _prepare_failed_job(self) -> tuple[AutoMatchJobService, MatchRuleService, dict]:
        """首次执行时阈值非法 -> 任务失败；随后更正阈值。"""
        job_svc = AutoMatchJobService()
        rule_svc = MatchRuleService()
        rule_svc.update_entry(4, {"相似度阈值": "1.20", "规则版本": "cust-rule-v1"})
        first = job_svc.schedule_match_job(4, "customer", CUSTOMER_RECORDS)
        assert first["ok"] is False, f"首次执行应失败: {first}"
        job = first["job"]
        assert job["任务状态"] == "已失败"
        assert first["cluster_results"] == [], "失败任务不应产出簇"
        # 失败任务详情说明原因且成员仍可恢复。
        detail = job_svc.query_job_status(job["id"])
        assert detail["status"] == "已失败"
        assert detail["member_recoverable"] is True
        assert "阈值" in detail["failure_reason"]
        assert detail["clusters_output"] == 0
        # 更正阈值规则。
        rule_svc.update_entry(4, {"相似度阈值": "0.70", "规则版本": "cust-rule-v2"})
        return job_svc, rule_svc, job

    def test_retry_uses_same_input_and_current_rule(self):
        """同一份输入 + 更正后的规则重新计算，交付补跑簇。"""
        job_svc, rule_svc, job = self._prepare_failed_job()
        before = [dict(record) for record in job["原始记录"]]

        result = job_svc.retry_failed_job(job["id"])
        assert result["ok"] is True, f"重试应成功: {result}"
        detail = job_svc.query_job_status(job["id"])

        assert detail["status"] == "已完成"
        assert detail["clusters_output"] == 1, f"三条相连简称应聚成一簇: {detail['clusters']}"
        assert detail["clusters"][0]["成员标识"] == ["CUST-1001", "CUST-1002", "CUST-1003"]
        assert detail["member_count"] == 3
        # 输入记录未被重跑修改。
        assert job["原始记录"] == before, "补跑不能篡改原始输入快照"
        # 规则依据与任务发生时间为两套版本，不能合并。
        assert detail["rule_version"] == "cust-rule-v2"
        assert detail["original_rule_version"] == "cust-rule-v1"
        assert detail["task_time"] and detail["original_task_time"]
        assert detail["task_time"] != detail["original_task_time"]
        for cluster in detail["clusters"]:
            assert cluster["规则版本"] == "cust-rule-v2"
            assert cluster["任务发生时间"] == detail["task_time"]

    def test_concurrent_retry_confirms_exactly_once(self):
        """并发重试同一失败任务只确认一份成功输出。"""
        job_svc, _rule_svc, job = self._prepare_failed_job()
        outcomes: list[dict] = []
        lock = threading.Lock()

        def worker() -> None:
            outcome = job_svc.retry_failed_job(job["id"])
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=worker) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        confirmed = [item for item in outcomes if item.get("ok")]
        duplicates = [item for item in outcomes if item.get("duplicate")]
        assert len(confirmed) == 1, f"只应确认一份成功输出，实际 {len(confirmed)}"
        assert len(duplicates) == len(outcomes) - 1

        detail = job_svc.query_job_status(job["id"])
        assert detail["clusters_output"] == 1
        assert detail["review_cluster_count"] == detail["clusters_output"], "详情数量与复核入口必须一致"
        review_rows = [
            row for row in store.rows("dup_cluster")
            if row.get("处置批次") == job["任务编号"]
        ]
        assert len(review_rows) == 1, "并发重试不能写出重复簇"
        assert review_rows[0]["成员标识"] == ["CUST-1001", "CUST-1002", "CUST-1003"]

    def test_unrecoverable_members_reported_in_detail(self):
        """成员数据无法恢复时，重试不伪造输出，由任务详情说明原因。"""
        job_svc = AutoMatchJobService()
        job = store.find("auto_match_job", 5)
        assert job is not None
        result = job_svc.retry_failed_job(5)
        assert result["ok"] is False
        assert result["duplicate"] is False
        detail = job_svc.query_job_status(5)
        assert detail["member_recoverable"] is False
        assert "无法恢复原始成员数据" in detail["failure_reason"]
        assert detail["clusters_output"] == 0
        assert detail["status"] == "已失败"


if __name__ == "__main__":
    runner = TestFailedJobRetry()
    checks = [
        ("同输入重算并交付补跑簇", runner.test_retry_uses_same_input_and_current_rule),
        ("并发只确认一份输出", runner.test_concurrent_retry_confirms_exactly_once),
        ("成员不可恢复时详情说明", runner.test_unrecoverable_members_reported_in_detail),
    ]
    passed = 0
    for name, check in checks:
        try:
            check()
            print(f"  PASS: {name}")
            passed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  FAIL: {name} -> {exc}")
    print(f"\n结果: {passed}/{len(checks)} 通过")
    assert passed == len(checks), "失败任务补跑链路测试未全部通过"
