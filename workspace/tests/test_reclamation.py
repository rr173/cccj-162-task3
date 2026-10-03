"""内容寻址存储可审计空间回收测试。

覆盖需求:
  1. 先生成带水位的回收计划 (候选/原因/预计释放量/存储水位), 再执行删除;
  2. 支持锁定需要保留的代际 (pin), 锁定代际的分片/输出不可回收;
  3. 计划生成后引用关系发生变化 -> 拒绝执行 (409 references_changed);
  4. 上传/恢复/回收并发不删除仍可访问的数据 (读写锁栅栏 + 逐项重算);
  5. 崩溃重启后可继续或安全重试同一计划 (事件重放 + 幂等删除);
  6. 查询接口/作业页展示预计释放量、实际删除项、跳过原因与最终结果。
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from hpc_cr import JobManager
from hpc_cr.epochs import Scheduler
from hpc_cr.errors import Conflict, NotFound, ValidationError
from hpc_cr.security import Signer
from hpc_cr.api import build_server
from hpc_cr.reclamation import (
    COMPLETED as GC_COMPLETED,
    PLANNED,
    REJECTED,
    REASON_REFERENCES_CHANGED,
    SKIP_BLOB_MISSING,
    SKIP_STILL_REFERENCED,
)

SECRET = "gc-secret"


def make_mgr(data_dir=None):
    return JobManager(Scheduler(Signer(SECRET)), data_dir=data_dir)


def full_cycle(mgr, jid, gen, world_size=2, nodes=None, suffix=b""):
    """抢占 -> 补齐 -> 提交 -> 释放 -> 恢复到 gen, 返回当前输出租约令牌。"""
    nodes = nodes or {r: f"n{r}" for r in range(world_size)}
    pre = mgr.preempt(jid, force_new_epoch=(gen > 1), grace_seconds=300)
    token = pre["epoch_token"]
    assert pre["epoch"]["generation"] == gen
    for r in range(world_size):
        mgr.upload_shard(
            jid, epoch_token=token, rank=r, index=0, node=nodes[r],
            input_pos=100 * gen + r,
            data=f"gen{gen}-shard-{r}".encode() + suffix)
    mgr.commit_checkpoint(jid, epoch_token=token)
    mgr.confirm_release(jid, epoch_token=token)
    rep = mgr.restore(jid, generation=gen)
    assert rep.success
    return mgr.get(jid).leases.current().token


class _ReclamationBaseMixin:
    """构造一个包含 gen0/gen1/gen2 历史垃圾的作业 j1 (4 个垃圾 blob)。"""

    def _populate(self):
        nodes = {r: f"n{r}" for r in range(2)}
        self.mgr.create_job(name="j", world_size=2, job_id="j1",
                            rank_nodes=nodes, lease_ttl=100000)
        lease0 = self.mgr.get("j1").leases.current().token
        self.gen0_out_token = lease0
        self.mgr.append_output("j1", lease_token=lease0, output_id="o0",
                               data=b"gen0-output-payload")
        self.lease1 = full_cycle(self.mgr, "j1", 1)
        self.mgr.append_output("j1", lease_token=self.lease1, output_id="o1",
                               data=b"gen1-output-payload")
        self.lease2 = full_cycle(self.mgr, "j1", 2)
        self.mgr.append_output("j1", lease_token=self.lease2, output_id="o2",
                               data=b"gen2-output-payload")

    def _live_gen2_blobs(self):
        cp2 = self.mgr.get("j1").cpm.get(2)
        shards = {sh.blob for sh in cp2.shards.values()}
        out2 = {o["blob"] for o in self.mgr.list_outputs("j1", 2)}
        return shards | out2

    def _garbage_blobs(self):
        cp1 = self.mgr.get("j1").cpm.get(1)
        blobs = {sh.blob for sh in cp1.shards.values()}
        blobs |= {o["blob"] for o in self.mgr.list_outputs("j1", 0)}
        blobs |= {o["blob"] for o in self.mgr.list_outputs("j1", 1)}
        return blobs


class ReclamationBaseTest(unittest.TestCase, _ReclamationBaseMixin):
    def setUp(self):
        self.mgr = make_mgr()
        self._populate()


class PlanAndExecuteTest(ReclamationBaseTest):
    def test_plan_enumerates_only_unreachable_with_watermark(self):
        plan = self.mgr.create_reclamation_plan(note="audit-run")
        # 候选: gen1 两分片 + gen0/gen1 输出 = 4 项
        self.assertEqual(len(plan["candidates"]), 4)
        kinds = sorted((c["kind"], c["reason"]) for c in plan["candidates"])
        self.assertEqual(kinds.count(("shard", "unreferenced_shard")), 2)
        self.assertEqual(kinds.count(("output", "unreferenced_output")), 2)
        garbage = self._garbage_blobs()
        cand_blobs = {c["blob"] for c in plan["candidates"]}
        self.assertEqual(cand_blobs, garbage)
        self.assertFalse(cand_blobs & self._live_gen2_blobs())
        self.assertGreater(plan["estimated_release_bytes"], 0)
        self.assertEqual(
            plan["estimated_release_bytes"],
            sum(c["size"] for c in plan["candidates"]))
        # 水位 = 最近一次引用事件的 seq
        self.assertEqual(plan["refs_seq"], self.mgr.gc.refs_seq)
        self.assertEqual(plan["state"], PLANNED)
        self.assertEqual(plan["store_stats"]["total_blobs"], 7)
        self.assertEqual(plan["store_stats"]["garbage_blobs"], 4)
        self.assertEqual(plan["note"], "audit-run")
        # 计划只是计划: 什么都还没删
        for b in garbage:
            self.assertTrue(self.mgr.store.exists(b))

    def test_execute_deletes_candidates_and_live_data_intact(self):
        plan = self.mgr.create_reclamation_plan()
        res = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertEqual(res["state"], GC_COMPLETED)
        self.assertEqual(res["result"], "completed")
        self.assertEqual(len(res["deleted"]), 4)
        self.assertEqual(res["deleted_bytes"],
                         plan["estimated_release_bytes"])
        for b in self._garbage_blobs():
            self.assertFalse(self.mgr.store.exists(b))
        for b in self._live_gen2_blobs():
            self.assertTrue(self.mgr.store.exists(b))
        # 恢复仍可访问 (全量重校验通过)
        rep = self.mgr.restore("j1", generation=2)
        self.assertTrue(rep.success)
        # 删除事件全部进入作业审计, 不含任何凭证
        audit = [a["event"] for a in self.mgr.get("j1").audit]
        self.assertIn("gc_blob_deleted", audit)
        self.assertEqual(audit.count("gc_blob_deleted"), 4)
        self.assertIn("gc_plan_completed", audit)
        self.assertIn("gc_plan_created", audit)

    def test_execute_is_idempotent_safe_retry(self):
        plan = self.mgr.create_reclamation_plan()
        r1 = self.mgr.execute_reclamation_plan(plan["plan_id"])
        r2 = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertEqual(r1["deleted"], r2["deleted"])
        self.assertEqual(len(r2["deleted"]), 4)

    def test_plan_stale_after_reference_change_is_rejected(self):
        plan = self.mgr.create_reclamation_plan()
        old_seq = plan["refs_seq"]
        # 计划生成后新的输出提交 -> 引用关系变化
        self.mgr.append_output("j1", lease_token=self.lease2, output_id="oX",
                               data=b"late-arriving-output")
        with self.assertRaises(Conflict) as cm:
            self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertEqual(cm.exception.http_status, 409)
        self.assertEqual(cm.exception.reason, REASON_REFERENCES_CHANGED)
        # 计划落为 REJECTED, 一项未删, changed_events 可审计
        rejected = self.mgr.get_reclamation_plan(plan["plan_id"])
        self.assertEqual(rejected["state"], REJECTED)
        self.assertEqual(rejected["reject_reason"],
                         REASON_REFERENCES_CHANGED)
        self.assertEqual(rejected["deleted"], [])
        self.assertTrue(any(
            e["type"] == "output_committed" and e["seq"] > old_seq
            for e in rejected["changed_events"]))
        # 被拒绝的计划不能再执行, 必须重新生成
        with self.assertRaises(Conflict):
            self.mgr.execute_reclamation_plan(plan["plan_id"])
        fresh = self.mgr.create_reclamation_plan()
        res = self.mgr.execute_reclamation_plan(fresh["plan_id"])
        self.assertEqual(res["state"], GC_COMPLETED)
        # 新输出是可达的, 绝未被删
        out = [o for o in self.mgr.list_outputs("j1", 2)
               if o["output_id"] == "oX"][0]
        self.assertTrue(self.mgr.store.exists(out["blob"]))

    def test_upload_after_plan_makes_plan_stale_and_keeps_blob(self):
        # 抢占 gen3 但只传一半 -> 生成计划 -> 补齐分片后旧计划必须拒绝
        pre = self.mgr.preempt("j1", force_new_epoch=True)
        self.mgr.upload_shard("j1", epoch_token=pre["epoch_token"], rank=0,
                              index=0, node="n0", input_pos=1, data=b"g3r0")
        plan = self.mgr.create_reclamation_plan()
        # 补齐: 新分片 attach 了 (可能全新的) blob
        self.mgr.upload_shard("j1", epoch_token=pre["epoch_token"], rank=1,
                              index=0, node="n1", input_pos=2, data=b"g3r1")
        with self.assertRaises(Conflict) as cm:
            self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertEqual(cm.exception.reason, REASON_REFERENCES_CHANGED)

    def test_aborted_job_shards_become_collectible(self):
        self.mgr.create_job(name="j2", world_size=1, job_id="j2",
                            rank_nodes={0: "a"})
        pre = self.mgr.preempt("j2")
        self.mgr.upload_shard("j2", epoch_token=pre["epoch_token"], rank=0,
                              index=0, node="a", input_pos=1,
                              data=b"aborted-gen-shard")
        self.mgr.abort_epoch("j2")
        plan = self.mgr.create_reclamation_plan()
        jobs = {c.get("job_id") for c in plan["candidates"]}
        self.assertIn("j2", jobs)

    def test_orphan_blob_collected(self):
        digest, _n, _ = self.mgr.store.put(b"nobody references me")
        plan = self.mgr.create_reclamation_plan()
        orphans = [c for c in plan["candidates"]
                   if c["reason"] == "orphan_blob"]
        self.assertEqual([c["blob"] for c in orphans], [digest])
        res = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertFalse(self.mgr.store.exists(digest))
        self.assertTrue(any(d["blob"] == digest and d["kind"] == "orphan"
                            for d in res["deleted"]))

    def test_dedup_blob_shared_with_live_generation_is_kept(self):
        # 新建作业: gen1 与 gen2 使用内容完全相同的分片 (CAS 天然去重),
        # gen1 虽已不可恢复, 共享 blob 因 gen2 可达而必须保留。
        self.mgr.create_job(name="dup", world_size=1, job_id="dup",
                            rank_nodes={0: "n"})
        shared = b"identical-checkpoint-bytes"
        for gen in (1, 2):
            pre = self.mgr.preempt("dup", force_new_epoch=(gen == 2))
            self.mgr.upload_shard(
                "dup", epoch_token=pre["epoch_token"], rank=0, index=0,
                node="n", input_pos=gen, data=shared)
            self.mgr.commit_checkpoint("dup", epoch_token=pre["epoch_token"])
            self.mgr.confirm_release(
                "dup", epoch_token=pre["epoch_token"])
            self.mgr.restore("dup", generation=gen)
        cp = self.mgr.get("dup").cpm
        self.assertEqual(cp.get(1).shards["r0:s0"].blob,
                         cp.get(2).shards["r0:s0"].blob)
        plan = self.mgr.create_reclamation_plan()
        shared_blob = cp.get(2).shards["r0:s0"].blob
        self.assertNotIn(shared_blob,
                         {c["blob"] for c in plan["candidates"]})
        res = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertTrue(self.mgr.store.exists(shared_blob))
        # 共享 blob 不在删除项
        self.assertNotIn(shared_blob, {d["blob"] for d in res["deleted"]})


class PinTest(ReclamationBaseTest):
    def test_pin_protects_generation_shards_and_outputs(self):
        self.mgr.pin_generation("j1", 1, note="legal-hold")
        plan = self.mgr.create_reclamation_plan()
        protected = self._garbage_blobs()  # 含 gen1 分片与 gen1 输出
        # gen1 的全部 2 分片 + 1 输出受保护; 仅 gen0 输出仍是候选
        self.assertEqual(
            {c["blob"] for c in plan["candidates"]},
            {o["blob"] for o in self.mgr.list_outputs("j1", 0)})
        self.assertTrue(any(
            p["job_id"] == "j1" and p["generation"] == 1
            for p in plan["pinned"]))
        res = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertEqual(len(res["deleted"]), 1)
        for b in protected - {o["blob"] for o in self.mgr.list_outputs("j1", 0)}:
            self.assertTrue(self.mgr.store.exists(b))
        # 锁定信息可查询
        pins = self.mgr.list_pins("j1")
        self.assertEqual([p["generation"] for p in pins], [1])

    def test_pin_is_idempotent_and_unpin_gates(self):
        p1 = self.mgr.pin_generation("j1", 1)
        p2 = self.mgr.pin_generation("j1", 1)
        self.assertEqual(p1["seq"], p2["seq"])
        self.mgr.unpin_generation("j1", 1)
        with self.assertRaises(NotFound):
            self.mgr.unpin_generation("j1", 1)
        plan = self.mgr.create_reclamation_plan()
        self.assertEqual(len(plan["candidates"]), 4)

    def test_pin_invalid_generation_rejected(self):
        with self.assertRaises(ValidationError):
            self.mgr.pin_generation("j1", -1)
        with self.assertRaises(NotFound):
            self.mgr.pin_generation("nope", 0)


class SkipAndWatermarkTest(ReclamationBaseTest):
    def test_blob_missing_skip_is_recorded(self):
        plan = self.mgr.create_reclamation_plan()
        # 执行前从物理层悄悄抹掉一个候选 (模拟计划后外部丢失): 跳过而非失败
        victim = plan["candidates"][0]["blob"]
        self.mgr.store.delete(victim)
        res = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertEqual(res["state"], GC_COMPLETED)
        skip = [s for s in res["skipped"] if s["blob"] == victim]
        self.assertEqual(len(skip), 1)
        self.assertEqual(skip[0]["reason"], SKIP_BLOB_MISSING)
        self.assertEqual(len(res["deleted"]), 3)
        self.assertEqual(res["result"], "completed_with_skips")

    def test_still_referenced_defensive_skip(self):
        # 直接构造"水位一致但 blob 仍可达"的纵深防御场景:
        # 让快照漏算可达集 (模拟未来代码路径的引用 bug), 删除必须被拦住。
        plan = self.mgr.create_reclamation_plan()
        live_blob = next(iter(self._live_gen2_blobs()))
        # 手工把一个可达 blob 塞进候选, 并保持水位不变
        plan_obj = self.mgr.gc.get_plan(plan["plan_id"])
        from hpc_cr.reclamation import Candidate
        plan_obj.candidates.append(Candidate(
            live_blob, 10, "shard", "unreferenced_shard",
            job_id="j1", generation=2, referrer="synthetic"))
        plan_obj.estimated_release_bytes += 10
        res = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertTrue(self.mgr.store.exists(live_blob))
        skips = [s for s in res["skipped"] if s["blob"] == live_blob]
        self.assertEqual(skips[0]["reason"], SKIP_STILL_REFERENCED)
        self.assertTrue(skips[0]["referrers"])  # 带引用者证据
        self.assertEqual(res["result"], "completed_with_skips")

    def test_min_bytes_excludes_small_blobs_but_audits_them(self):
        plan = self.mgr.create_reclamation_plan(min_bytes=10_000)
        self.assertEqual(plan["candidates"], [])
        self.assertEqual(len(plan["excluded"]), 4)
        self.assertTrue(all(
            c["reason"] in ("unreferenced_shard", "unreferenced_output")
            for c in plan["excluded"]))
        self.assertEqual(plan["estimated_release_bytes"], 0)
        res = self.mgr.execute_reclamation_plan(plan["plan_id"])
        self.assertEqual(res["deleted"], [])
        # 垃圾仍在
        self.assertEqual(self.mgr.reclamation_overview()["store"]
                         ["garbage_blobs"], 4)


class CancelTest(ReclamationBaseTest):
    def test_only_planned_can_be_cancelled(self):
        plan = self.mgr.create_reclamation_plan()
        out = self.mgr.cancel_reclamation_plan(plan["plan_id"])
        self.assertEqual(out["state"], "CANCELLED")
        with self.assertRaises(Conflict):
            self.mgr.execute_reclamation_plan(plan["plan_id"])
        with self.assertRaises(Conflict):
            self.mgr.cancel_reclamation_plan(plan["plan_id"])


class CrashResumeTest(unittest.TestCase, _ReclamationBaseMixin):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="hpc-gc-")
        self.mgr = make_mgr(self.dir)
        self._populate()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _fresh(self):
        # 同密钥重启 (与生产一致: data_dir 内 signer.key 或固定 secret)
        return make_mgr(self.dir)

    def test_resume_plan_after_crash_mid_execution(self):
        mgr = self._fresh()
        plan = mgr.create_reclamation_plan()
        pid = plan["plan_id"]
        cands = list(plan["candidates"])
        self.assertEqual(len(cands), 4)

        # 模拟执行到第二项后进程崩溃: 手工驱动计划状态与事件
        gc = mgr.gc
        pobj = gc.get_plan(pid)
        import time as _t
        with gc.rw.write():
            ts = _t.time()
            pobj.state = "RUNNING"
            pobj.started_ts = ts
            mgr._record_global("gc_plan_started", {"plan_id": pid}, ts=ts)
            for cand in cands[:2]:
                mgr.store.delete(cand["blob"])
                mgr._record_global("gc_blob_deleted", {
                    "plan_id": pid, "blob": cand["blob"],
                    "size": cand["size"],
                    "kind": cand["kind"], "job_id": cand["job_id"],
                    "generation": cand["generation"],
                    "reason": cand["reason"]},
                    ts=_t.time())
                pobj.deleted.append({"blob": cand["blob"]})
        del mgr

        # 重启: RUNNING 计划可继续, 已删两项不重复处理
        mgr2 = self._fresh()
        p2 = mgr2.gc.get_plan(pid)
        self.assertEqual(p2.state, "RUNNING")
        self.assertTrue(all(not mgr2.store.exists(c["blob"]) for c in cands[:2]))
        self.assertTrue(all(mgr2.store.exists(c["blob"]) for c in cands[2:]))
        res = mgr2.execute_reclamation_plan(pid)
        self.assertEqual(res["state"], GC_COMPLETED)
        self.assertEqual(len(res["deleted"]), 4)
        self.assertEqual(
            {d["blob"] for d in res["deleted"]},
            {c["blob"] for c in cands})
        for c in cands:
            self.assertFalse(mgr2.store.exists(c["blob"]))
        # 再重启: COMPLETED 幂等, 磁盘不再有垃圾
        mgr3 = self._fresh()
        p3 = mgr3.get_reclamation_plan(pid)
        self.assertEqual(p3["state"], GC_COMPLETED)
        again = mgr3.execute_reclamation_plan(pid)
        self.assertEqual(len(again["deleted"]), 4)

    def test_replay_physically_deletes_event_after_lost_unlink(self):
        # 已落 gc_blob_deleted 但磁盘 unlink 未完成 -> 重放补删
        mgr = self._fresh()
        plan = mgr.create_reclamation_plan()
        victim = plan["candidates"][0]
        # 仅落事件不删文件 (模拟"事件已 fsync, unlink 前崩溃"的镜像场景)
        mgr._record_global("gc_blob_deleted", {
            "plan_id": plan["plan_id"], "blob": victim["blob"],
            "size": victim["size"], "kind": victim["kind"],
            "job_id": victim["job_id"], "generation": victim["generation"],
            "reason": victim["reason"]})
        self.assertTrue(os.path.exists(
            os.path.join(self.dir, "blobs", victim["blob"])))
        mgr2 = self._fresh()
        self.assertFalse(os.path.exists(
            os.path.join(self.dir, "blobs", victim["blob"])))

    def test_pins_survive_restart(self):
        mgr = self._fresh()
        mgr.pin_generation("j1", 1, note="hold")
        mgr2 = self._fresh()
        self.assertEqual(
            [(p["job_id"], p["generation"]) for p in mgr2.list_pins()],
            [("j1", 1)])
        plan = mgr2.create_reclamation_plan()
        self.assertFalse(any(c.get("generation") == 1
                             for c in plan["candidates"]))


class ConcurrencyTest(ReclamationBaseTest):
    def test_gc_running_concurrent_uploads_never_loses_live_blob(self):
        # 抢占 gen3: 一边持续上传分片, 一边生成并执行 GC。
        pre = self.mgr.preempt("j1", force_new_epoch=True)
        token = pre["epoch_token"]
        outcomes: list[str] = []
        stop = threading.Event()
        plan_holder: dict = {}

        def uploader():
            # 反复上传 r0 分片 (同内容幂等), 期间 GC 可能在任意点执行
            for _ in range(200):
                try:
                    self.mgr.upload_shard(
                        "j1", epoch_token=token, rank=0, index=0,
                        node="n0", input_pos=7,
                        data=b"gen3-r0-bytes-here")
                    outcomes.append("ok")
                except Conflict:
                    outcomes.append("conflict")

        def recycler():
            for _ in range(20):
                plan = self.mgr.create_reclamation_plan()
                plan_holder["plan"] = plan
                try:
                    self.mgr.execute_reclamation_plan(plan["plan_id"])
                except Conflict as exc:
                    if exc.reason != REASON_REFERENCES_CHANGED:
                        raise
                if stop.is_set():
                    break

        t1 = threading.Thread(target=uploader)
        t2 = threading.Thread(target=recycler)
        t1.start()
        t2.start()
        t1.join(timeout=30)
        stop.set()
        t2.join(timeout=30)
        self.assertFalse(t1.is_alive())
        self.assertFalse(t2.is_alive())

        # gen3 r0 分片若上传成功过, 其 blob 必须仍然存在
        cp3 = self.mgr.get("j1").cpm.get(3)
        sh = cp3.shards.get("r0:s0")
        if sh is not None:
            self.assertTrue(self.mgr.store.exists(sh.blob),
                            "可达分片被并发回收删除!")
            self.assertTrue(self.mgr.store.verify(sh.blob))
        # 既有可达代际 gen2 全部完好
        for b in self._live_gen2_blobs():
            self.assertTrue(self.mgr.store.exists(b))
        # 没有任何已执行计划删除了仍可达 blob (跳过原因允许出现, 删除不允许)
        for p in self.mgr.gc.list_plans():
            for d in p.deleted:
                self.assertNotIn(d["blob"], self._live_gen2_blobs())

    def test_gc_concurrent_with_restore_and_output(self):
        plan = self.mgr.create_reclamation_plan()
        results: list = []

        def restore_loop():
            for _ in range(50):
                rep = self.mgr.restore("j1", generation=2)
                results.append(("restore", rep.success))

        def output_loop():
            lease = self.lease2
            for i in range(50):
                r = self.mgr.append_output(
                    "j1", lease_token=lease, output_id="oX",
                    data=b"concurrent output")
                results.append(("out", r["delivered"]))

        def gc_once():
            try:
                self.mgr.execute_reclamation_plan(plan["plan_id"])
                results.append(("gc", "done"))
            except Conflict:
                results.append(("gc", "stale"))

        ts = [threading.Thread(target=restore_loop),
              threading.Thread(target=output_loop),
              threading.Thread(target=gc_once)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        # 恢复永远成功 (可达分片从未被删), 输出恰好一次交付
        self.assertTrue(all(ok for k, ok in results if k == "restore"))
        self.assertEqual(sum(1 for k, d in results
                             if k == "out" and d), 1)
        self.assertIn(("gc", "stale"), results)


class QueryTest(ReclamationBaseTest):
    def test_overview_and_job_view_shadow_reclaim(self):
        plan = self.mgr.create_reclamation_plan(note="q")
        self.mgr.pin_generation("j1", 0)  # 保护 gen0 输出
        # 旧计划因此作废
        with self.assertRaises(Conflict):
            self.mgr.execute_reclamation_plan(plan["plan_id"])
        fresh = self.mgr.create_reclamation_plan()
        res = self.mgr.execute_reclamation_plan(fresh["plan_id"])
        self.assertEqual(len(res["deleted"]), 3)  # 仅 gen1 分片/输出

        ov = self.mgr.reclamation_overview()
        # pin 已在 fresh 计划执行后仍生效: gen0 输出被锁定保留, 垃圾清零
        self.assertEqual(ov["store"]["garbage_blobs"], 0)
        self.assertGreaterEqual(len(ov["plans"]), 2)
        summary = [p for p in ov["plans"]
                   if p["plan_id"] == fresh["plan_id"]][0]
        self.assertEqual(summary["deleted_count"], 3)
        self.assertIn("deleted_bytes", summary)
        self.assertIn("estimated_release_bytes", summary)

        view = self.mgr.gc.job_view("j1")
        self.assertEqual([p["generation"] for p in view["pins"]], [0])
        # 作业视图按本作业汇总删除/跳过 (按 plan_id 精确取刚执行的计划)
        done = [p for p in view["plans"]
                if p["plan_id"] == fresh["plan_id"]][0]
        self.assertEqual(done["job_deleted_count"], 3)
        self.assertEqual(done["job_skipped_count"], 0)

    def test_unknown_plan_404(self):
        with self.assertRaises(NotFound):
            self.mgr.get_reclamation_plan("plan-nope")


class HttpReclamationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mgr = make_mgr()
        cls.server = build_server(cls.mgr, host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _req(self, method, path, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_reclamation_api_and_pages(self):
        st, _ = self._req("POST", "/api/jobs", {
            "job_id": "gc-http", "name": "d", "world_size": 1,
            "rank_nodes": {0: "h0"}})
        self.assertEqual(st, 201)
        st, pre = self._req("POST", "/api/jobs/gc-http",
                            {"action": "preempt"})
        token = pre["epoch_token"]
        st, _ = self._post_with_token("gc-http", {
            "action": "upload_shard", "rank": 0, "index": 0, "node": "h0",
            "input_pos": 1,
            "data_b64": "Z2VuMS1vbmx5"}, token)  # gen1-only
        self.assertEqual(st, 200)
        st, _ = self._req("POST", "/api/jobs/gc-http",
                          {"action": "preempt", "force_new_epoch": True})
        token2 = _["epoch_token"]
        st, _ = self._post_with_token("gc-http", {
            "action": "upload_shard", "rank": 0, "index": 0, "node": "h0",
            "input_pos": 2,
            "data_b64": "Z2VuMi1saXZl"}, token2)  # gen2-live
        self.assertEqual(st, 200)
        # 无令牌 commit -> 401
        st, err = self._req("POST", "/api/jobs/gc-http",
                            {"action": "commit_checkpoint"})
        self.assertEqual(st, 401)
        self._post_with_token("gc-http", {"action": "commit_checkpoint"},
                              token2)
        self._post_with_token("gc-http", {"action": "confirm_release"},
                              token2)
        st, _ = self._req("POST", "/api/jobs/gc-http",
                          {"action": "restore", "generation": 2})
        self.assertEqual(st, 200)

        # 概览
        st, ov = self._req("GET", "/api/reclamation")
        self.assertEqual(st, 200)
        self.assertGreaterEqual(ov["store"]["garbage_blobs"], 1)

        # pin gen1
        st, pin = self._req("POST", "/api/jobs/gc-http",
                            {"action": "pin_generation", "generation": 1,
                             "note": "hold"})
        self.assertEqual(st, 200)
        self.assertEqual(pin["generation"], 1)

        st, plan = self._req("POST", "/api/reclamation/plans",
                             {"note": "http"})
        self.assertEqual(st, 201)
        self.assertIn("candidates", plan)
        self.assertEqual(plan["pinned"][0]["generation"], 1)
        # gen1 被锁定 -> 该作业无候选
        self.assertFalse(any(
            c.get("job_id") == "gc-http" for c in plan["candidates"]))

        # 解锁后旧计划作废
        st, _ = self._req("POST", "/api/jobs/gc-http",
                          {"action": "unpin_generation", "generation": 1})
        self.assertEqual(st, 200)
        st, rej = self._req(
            "POST", f"/api/reclamation/plans/{plan['plan_id']}/execute")
        self.assertEqual(st, 409)
        self.assertEqual(rej["reason"], REASON_REFERENCES_CHANGED)

        st, plan2 = self._req("POST", "/api/reclamation/plans", {})
        st, done = self._req(
            "POST", f"/api/reclamation/plans/{plan2['plan_id']}/execute")
        self.assertEqual(st, 200)
        self.assertEqual(done["state"], GC_COMPLETED)
        self.assertGreaterEqual(done["deleted_count"], 1)
        # 详情接口
        st, detail = self._req(
            "GET", f"/api/reclamation/plans/{plan2['plan_id']}")
        self.assertEqual(st, 200)
        self.assertTrue(all("blob" in d and "size" in d
                            for d in detail["deleted"]))

        # 全局回收页与作业页可渲染
        html1 = self._get_html("/ui/reclamation")
        self.assertIn("空间回收审计", html1)
        self.assertIn("实际删除", html1)
        html2 = self._get_html("/ui/jobs/gc-http")
        self.assertIn("保留代际锁定", html2)

        # 取消
        st, p3 = self._req("POST", "/api/reclamation/plans", {})
        st, c = self._req(
            "POST", f"/api/reclamation/plans/{p3['plan_id']}/cancel")
        self.assertEqual(st, 200)
        self.assertEqual(c["state"], "CANCELLED")

    def _post_with_token(self, jid, body, token):
        url = f"http://127.0.0.1:{self.port}/api/jobs/{jid}"
        req = urllib.request.Request(
            url, data=json.dumps(body).encode(), method="POST",
            headers={"Content-Type": "application/json",
                     "X-Epoch-Token": token})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())

    def _get_html(self, path):
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}{path}", timeout=5) as r:
            return r.read().decode()


if __name__ == "__main__":
    unittest.main()
