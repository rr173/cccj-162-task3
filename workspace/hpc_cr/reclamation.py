"""可审计的内容寻址存储空间回收 (garbage collection)。

长期运行后, CAS 中会堆积不再被任何 *可恢复状态* 引用的分片/输出载荷。
本模块实现两阶段、可审计、可安全重试的回收:

阶段一 —— 计划 (``create_plan``)
  在引用关系一致性快照上枚举 store 内全部 blob:

  * **可达集 (live)**: 每个作业"仍可访问"的代际所引用的分片与输出 ——
    RUNNING 作业的当前输出代际、PREEMPTING 作业的当前检查点代际与旧输出
    代际、RELEASED 作业唯一可恢复的代际, 以及被 **显式锁定 (pin)** 的
    代际; ABORTED 作业无可达代际。
  * 不在可达集内的 blob 成为候选: 按来源标注
    ``unreferenced_shard`` / ``unreferenced_output`` / ``orphan_blob``,
    记录大小、所属作业/代际、引用明细, 以及预计释放字节数。
  * 计划冻结 **引用水位** ``refs_seq`` (事件日志中最近一次引用变更的 seq)。

阶段二 —— 执行 (``execute_plan``)
  * 水位校验: 计划生成后引用图只要发生过变化 (抢占/分片上传/输出提交/
    释放/放弃/恢复/代际锁定变更), 一律拒绝执行
    (409 + ``reason=references_changed``), 计划落为 REJECTED, 必须重新
    生成 —— 绝不依据过期快照删除。
  * 写锁栅栏: 执行持有 :class:`RWLock` 写锁, 全部引用变更方持读锁,
    执行期引用关系冻结。写锁下对每个候选 *重新* 计算可达集: 仍被引用
    -> ``skip(still_referenced)``, 已不存在 -> ``skip(blob_missing)``,
    其余才物理删除。
  * 逐项落事件 ``gc_blob_deleted`` / ``gc_blob_skipped``, 最后落
    ``gc_plan_completed``。崩溃重启后重放即物理删除, RUNNING 计划可再次
    ``execute_plan`` 安全续跑; COMPLETED 计划重复执行幂等返回。

代际锁定 (pin): ``(job_id, generation)`` 持久化保留集; 解锁前该代际的
分片/输出始终可达, 锁定/解锁本身也是引用变更, 会作废旧计划水位。
"""

from __future__ import annotations

import contextlib
import threading
import time
import uuid
from dataclasses import dataclass, field

from .errors import Conflict, NotFound, ValidationError

# 计划状态
PLANNED = "PLANNED"
RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
REJECTED = "REJECTED"
CANCELLED = "CANCELLED"

# 候选来源
KIND_SHARD = "shard"
KIND_OUTPUT = "output"
KIND_ORPHAN = "orphan"

# 候选入选原因 (计划期)
REASON_UNREFERENCED_SHARD = "unreferenced_shard"
REASON_UNREFERENCED_OUTPUT = "unreferenced_output"
REASON_ORPHAN = "orphan_blob"
# 计划期被水位过滤排除
REASON_BELOW_MIN_BYTES = "below_min_bytes"
# 执行期跳过原因
SKIP_STILL_REFERENCED = "still_referenced"
SKIP_BLOB_MISSING = "blob_missing"
# 计划拒绝原因
REASON_REFERENCES_CHANGED = "references_changed"

# 最终结果
RESULT_COMPLETED = "completed"
RESULT_COMPLETED_WITH_SKIPS = "completed_with_skips"

# 会改变引用关系图的事件: 这些事件的 seq 即引用水位
REF_EVENTS = frozenset({
    "preempt_issued",          # 新代际检查点开放, 旧租约代际输出被栅栏
    "shard_uploaded",          # 分片引用新 blob
    "output_committed",        # 输出条目引用新 blob
    "resources_released",      # 可恢复代际收敛为唯一 released 代际
    "epoch_abandoned",         # 作业放弃 -> 无可达代际
    "restore_verified",        # RELEASED -> RUNNING, 输出代际迁移
    "gc_pin_added",            # 代际被锁定保留
    "gc_pin_removed",          # 代际锁定解除
})

# GC 事件名
EVT_PLAN_CREATED = "gc_plan_created"
EVT_PLAN_STARTED = "gc_plan_started"
EVT_BLOB_DELETED = "gc_blob_deleted"
EVT_BLOB_SKIPPED = "gc_blob_skipped"
EVT_PLAN_COMPLETED = "gc_plan_completed"
EVT_PLAN_REJECTED = "gc_plan_rejected"
EVT_PLAN_CANCELLED = "gc_plan_cancelled"
EVT_PIN_ADDED = "gc_pin_added"
EVT_PIN_REMOVED = "gc_pin_removed"


class RWLock:
    """写者优先的读写锁。

    引用变更方 (上传/输出/抢占/释放/恢复/pin) 共享读锁; 回收执行独占
    写锁。写者优先避免持续的读流量让回收永远无法开始。获取写锁时 *不*
    持有任何其他领域锁, 因此不存在锁顺序倒置。
    """

    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    def acquire_read(self) -> None:
        with self._cond:
            while self._writer_active or self._writers_waiting:
                self._cond.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self) -> None:
        with self._cond:
            self._writers_waiting += 1
            while self._writer_active or self._readers:
                self._cond.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self) -> None:
        with self._cond:
            self._writer_active = False
            self._cond.notify_all()

    @contextlib.contextmanager
    def read(self):
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextlib.contextmanager
    def write(self):
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()


# 作业状态字面量 (与 jobs.py 保持一致; 此处不导入以避免循环依赖)
_ST_RUNNING = "RUNNING"
_ST_PREEMPTING = "PREEMPTING"
_ST_RELEASED = "RELEASED"


@dataclass
class Candidate:
    """计划候选: 计划生成时不可达的单个 blob。"""

    blob: str
    size: int
    kind: str               # shard | output | orphan
    reason: str             # unreferenced_shard | unreferenced_output | orphan_blob
    job_id: str | None = None
    generation: int | None = None
    referrer: str | None = None  # 人类可读引用明细 (如 r3:s1 / output_id)

    def to_dict(self) -> dict:
        return {
            "blob": self.blob, "size": self.size, "kind": self.kind,
            "reason": self.reason, "job_id": self.job_id,
            "generation": self.generation, "referrer": self.referrer,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Candidate":
        return cls(
            blob=d["blob"], size=d.get("size", 0), kind=d["kind"],
            reason=d["reason"], job_id=d.get("job_id"),
            generation=d.get("generation"), referrer=d.get("referrer"))


@dataclass
class ReclamationPlan:
    plan_id: str
    created_ts: float
    refs_seq: int
    state: str = PLANNED
    min_bytes: int = 0
    note: str | None = None
    created_seq: int | None = None
    started_ts: float | None = None
    completed_ts: float | None = None
    result: str | None = None
    reject_reason: str | None = None
    changed_events: list[dict] = field(default_factory=list)
    # 生成计划时的存储水位 (审计用)
    store_stats: dict = field(default_factory=dict)
    # 生成计划时生效的全部代际锁定
    pinned: list[dict] = field(default_factory=list)
    estimated_release_bytes: int = 0
    candidates: list[Candidate] = field(default_factory=list)
    excluded: list[Candidate] = field(default_factory=list)
    deleted: list[dict] = field(default_factory=list)
    skipped: list[dict] = field(default_factory=list)

    @property
    def deleted_bytes(self) -> int:
        return sum(d.get("size", 0) for d in self.deleted)

    def affected_jobs(self) -> list[str]:
        jobs = {c.job_id for c in self.candidates if c.job_id is not None}
        jobs |= {d.get("job_id") for d in self.deleted if d.get("job_id")}
        jobs |= {s.get("job_id") for s in self.skipped if s.get("job_id")}
        return sorted(jobs)

    def summary(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "created_ts": self.created_ts,
            "created_seq": self.created_seq,
            "state": self.state,
            "refs_seq": self.refs_seq,
            "result": self.result,
            "reject_reason": self.reject_reason,
            "estimated_release_bytes": self.estimated_release_bytes,
            "candidate_count": len(self.candidates),
            "deleted_count": len(self.deleted),
            "deleted_bytes": self.deleted_bytes,
            "skipped_count": len(self.skipped),
            "excluded_count": len(self.excluded),
            "started_ts": self.started_ts,
            "completed_ts": self.completed_ts,
            "note": self.note,
        }

    def to_dict(self) -> dict:
        deleted_blobs = {d["blob"] for d in self.deleted}
        return {
            **self.summary(),
            "min_bytes": self.min_bytes,
            "changed_events": list(self.changed_events),
            "store_stats": dict(self.store_stats),
            "pinned": [dict(p) for p in self.pinned],
            "candidates": [c.to_dict() for c in self.candidates],
            "excluded": [c.to_dict() for c in self.excluded],
            "deleted": [dict(d) for d in self.deleted],
            "skipped": [dict(s) for s in self.skipped],
            "remaining_count": len(self.candidates) - len(deleted_blobs),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ReclamationPlan":
        return cls(
            plan_id=d["plan_id"],
            created_ts=d["created_ts"],
            refs_seq=d["refs_seq"],
            state=d.get("state", PLANNED),
            min_bytes=d.get("min_bytes", 0),
            note=d.get("note"),
            created_seq=d.get("created_seq"),
            started_ts=d.get("started_ts"),
            completed_ts=d.get("completed_ts"),
            result=d.get("result"),
            reject_reason=d.get("reject_reason"),
            changed_events=list(d.get("changed_events", [])),
            store_stats=dict(d.get("store_stats", {})),
            pinned=[dict(p) for p in d.get("pinned", [])],
            estimated_release_bytes=d.get("estimated_release_bytes", 0),
            candidates=[Candidate.from_dict(c)
                        for c in d.get("candidates", [])],
            excluded=[Candidate.from_dict(c)
                      for c in d.get("excluded", [])],
            deleted=[dict(x) for x in d.get("deleted", [])],
            skipped=[dict(x) for x in d.get("skipped", [])],
        )


def live_generations(job, pinned: dict[str, set[int]]) -> set[int]:
    """一个作业当前 *仍可访问* 的检查点/输出代际。

    * RUNNING:    当前输出生效代际 (恢复代际; 首次抢占前为 gen0);
    * PREEMPTING: 正在构建的当前检查点代际 + 尚未释放的旧输出代际
                  (抢占后到释放前, 旧输出仍可能被查询/读取);
    * RELEASED:   唯一可恢复的 released 代际;
    * ABORTED:    无;
    再并上该作业被显式锁定的代际。
    """
    st = job.state
    if st == _ST_RUNNING:
        gens = {job.output_generation}
    elif st == _ST_PREEMPTING:
        gens = {job.current_generation, job.output_generation}
    elif st == _ST_RELEASED:
        gens = ({job.released_generation}
                if job.released_generation is not None else set())
    else:
        gens = set()
    gens |= set(pinned.get(job.job_id, ()))
    return gens


class ReclamationManager:
    """回收计划注册表、代际锁定与引用水位。

    线程模型:
      * ``self._lock`` 保护计划注册表/锁定集/水位 (短临界区);
      * ``self.rw`` 协调引用变更方与回收执行;
      * 计划快照/执行内的加锁顺序固定: (写锁) -> JobManager._lock ->
        Job.lock -> 子管理器 -> store/journal。
    """

    def __init__(self):
        self.rw = RWLock()
        self._lock = threading.RLock()
        self._plans: dict[str, ReclamationPlan] = {}
        self._pins: dict[tuple[str, int], dict] = {}
        self._refs_seq = 0
        # 引用变更流水 (仅 seq/type), 用于计划拒绝时给出 changed_events
        self._ref_events: list[dict] = []

    # ============================================================== watermark
    @property
    def refs_seq(self) -> int:
        with self._lock:
            return self._refs_seq

    def note_event(self, event_type: str, seq: int) -> None:
        """实时路径: 事件落盘后推进引用水位。"""
        if event_type not in REF_EVENTS:
            return
        with self._lock:
            if seq > self._refs_seq:
                self._refs_seq = seq
            self._ref_events.append({"seq": seq, "type": event_type})

    # ------------------------------------------------------------------- pins
    def add_pin(self, mgr, job_id: str, generation: int,
                note: str | None = None) -> dict:
        """锁定保留某作业的某代际; 已存在则幂等返回。"""
        job = mgr.get(job_id)  # NotFound
        if not isinstance(generation, int) or isinstance(generation, bool) \
                or generation < 0:
            raise ValidationError("generation must be a non-negative int")
        key = (job_id, generation)
        with self.rw.read():
            with self._lock:
                existing = self._pins.get(key)
                if existing is not None:
                    return dict(existing)
                ts = time.time()
                pin = {"job_id": job_id, "generation": generation,
                       "note": note, "ts": ts}
                # 锁定是引用变更: 事件在 rw 读临界区内落盘, GC 执行无法
                # 与它并发; 已生成的旧计划因水位推进而作废。
                evt = mgr._record_global(EVT_PIN_ADDED, dict(pin), ts=ts)
                self._pins[key] = {**pin, "seq": evt["seq"]}
                return dict(self._pins[key])

    def remove_pin(self, mgr, job_id: str, generation: int) -> dict:
        mgr.get(job_id)
        key = (job_id, generation)
        with self.rw.read():
            with self._lock:
                existing = self._pins.get(key)
                if existing is None:
                    raise NotFound(
                        f"no retention pin for {job_id} gen{generation}")
                ts = time.time()
                mgr._record_global(EVT_PIN_REMOVED, {
                    "job_id": job_id, "generation": generation}, ts=ts)
                self._pins.pop(key, None)
                return {"job_id": job_id, "generation": generation,
                        "removed_ts": ts}

    def list_pins(self, job_id: str | None = None) -> list[dict]:
        with self._lock:
            pins = [dict(p) for p in self._pins.values()]
        if job_id is not None:
            pins = [p for p in pins if p["job_id"] == job_id]
        return sorted(pins, key=lambda p: (p["job_id"], p["generation"]))

    def _pinned_by_job(self) -> dict[str, set[int]]:
        out: dict[str, set[int]] = {}
        for (jid, gen) in self._pins:
            out.setdefault(jid, set()).add(gen)
        return out

    # ------------------------------------------------------------- snapshots
    def _snapshot_jobs(self, mgr) -> tuple[set[str], dict[str, list[dict]]]:
        """在调用方持有的锁保护下枚举全部引用。

        返回 (可达 blob 集合, blob -> 全部引用明细 [含不可达引用])。
        注册锁 (JobManager._lock) 只用于取作业列表快照, Job.lock 在其外按
        固定 job_id 顺序获取, 避免与 create_job 的加锁路径倒置。
        """
        live: set[str] = set()
        all_refs: dict[str, list[dict]] = {}

        def add_ref(blob: str, ref: dict) -> None:
            all_refs.setdefault(blob, []).append(ref)

        # 先在注册锁内取作业列表快照, 释放后再按固定顺序取 Job.lock ——
        # 不与 create_job (mgr._lock) 形成锁顺序倒置; 枚举期间的引用变更由
        # 调用方 (_consistent_snapshot/写锁) 的水位重试/写锁排空保证一致。
        with mgr._lock:
            jobs = sorted(mgr._jobs.values(), key=lambda j: j.job_id)
        with self._lock:
            pinned = self._pinned_by_job()
        for job in jobs:
            with job.lock:
                gens = live_generations(job, pinned)
                for cp in job.cpm.list_all():
                    for sid, sh in cp.shards.items():
                        ref = {
                            "kind": KIND_SHARD, "job_id": job.job_id,
                            "generation": cp.generation,
                            "referrer": f"{sid} (cp {cp.state})",
                            "live": cp.generation in gens,
                        }
                        add_ref(sh.blob, ref)
                        if cp.generation in gens:
                            live.add(sh.blob)
                for rec in job.outputs.list():
                    ref = {
                        "kind": KIND_OUTPUT, "job_id": job.job_id,
                        "generation": rec.generation,
                        "referrer": f"output {rec.output_id}",
                        "live": rec.generation in gens,
                    }
                    add_ref(rec.blob, ref)
                    if rec.generation in gens:
                        live.add(rec.blob)
        return live, all_refs

    def _consistent_snapshot(
        self, mgr
    ) -> tuple[int, set[str], dict[str, list[dict]]]:
        """读锁下取得与引用水位一致的快照; 遇并发引用变更则重试。

        状态变更与事件追加在同一 Job.lock 临界区, 因此只要快照前后水位
        相等, 快照就与该水位严格对应。
        """
        while True:
            before = self.refs_seq
            live, all_refs = self._snapshot_jobs(mgr)
            after = self.refs_seq
            if before == after:
                return after, live, all_refs

    # ----------------------------------------------------------- plan create
    def create_plan(
        self, mgr, *, min_bytes: int = 0, note: str | None = None,
    ) -> ReclamationPlan:
        """生成回收计划 (不删除任何数据)。"""
        if not isinstance(min_bytes, int) or isinstance(min_bytes, bool) \
                or min_bytes < 0:
            raise ValidationError("min_bytes must be a non-negative int")
        with self.rw.read():
            refs_seq, live, all_refs = self._consistent_snapshot(mgr)

            total_blobs = total_bytes = live_blobs = live_bytes = 0
            candidates: list[Candidate] = []
            excluded: list[Candidate] = []
            for digest in mgr.store.all_digests():
                size = mgr.store.blob_size(digest)
                size = size if size is not None else 0
                total_blobs += 1
                total_bytes += size
                if digest in live:
                    live_blobs += 1
                    live_bytes += size
                    continue
                refs = all_refs.get(digest, [])
                if not refs:
                    cand = Candidate(digest, size, KIND_ORPHAN, REASON_ORPHAN)
                else:
                    shard_refs = [r for r in refs if r["kind"] == KIND_SHARD]
                    r = shard_refs[0] if shard_refs else refs[0]
                    # 同一 blob 被多个代际引用时, 引用明细全部留档
                    detail = "; ".join(
                        f"{x['job_id']} gen{x['generation']} {x['referrer']}"
                        for x in refs)
                    cand = Candidate(
                        digest, size,
                        r["kind"],
                        (REASON_UNREFERENCED_SHARD if r["kind"] == KIND_SHARD
                         else REASON_UNREFERENCED_OUTPUT),
                        job_id=r["job_id"], generation=r["generation"],
                        referrer=detail)
                if size < min_bytes:
                    excluded.append(cand)
                else:
                    candidates.append(cand)

            plan = ReclamationPlan(
                plan_id=f"plan-{uuid.uuid4().hex[:10]}",
                created_ts=time.time(),
                refs_seq=refs_seq,
                min_bytes=min_bytes,
                note=note,
                store_stats={
                    "total_blobs": total_blobs,
                    "total_bytes": total_bytes,
                    "live_blobs": live_blobs,
                    "live_bytes": live_bytes,
                    "garbage_blobs": total_blobs - live_blobs,
                    "garbage_bytes": total_bytes - live_bytes,
                },
                pinned=self.list_pins(),
                estimated_release_bytes=sum(c.size for c in candidates),
                candidates=candidates,
                excluded=excluded,
            )
            with self._lock:
                self._plans[plan.plan_id] = plan
            evt = mgr._record_global(EVT_PLAN_CREATED, {
                "plan": plan.to_dict(),
                "affected_jobs": sorted({
                    c.job_id for c in candidates if c.job_id is not None}),
            },
                                     ts=plan.created_ts)
            plan.created_seq = evt["seq"]
            return plan

    # ---------------------------------------------------------- plan execute
    def execute_plan(self, mgr, plan_id: str) -> ReclamationPlan:
        """按计划执行删除。

        * COMPLETED  -> 幂等返回 (安全重试);
        * REJECTED/CANCELLED -> 409;
        * PLANNED/RUNNING (含崩溃重启后续跑) -> 水位校验 + 写锁逐项删除。
        """
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                raise NotFound(f"unknown reclamation plan {plan_id}")
            if plan.state == COMPLETED:
                return plan
            if plan.state in (REJECTED, CANCELLED):
                raise Conflict(f"reclamation plan {plan_id} is {plan.state}")

        # 写锁: 入队后等待全部持读锁的引用变更方退出临界区
        with self.rw.write():
            with self._lock:
                # 水位栅栏: 计划生成后引用关系发生过任何变化都拒绝
                if self._refs_seq != plan.refs_seq:
                    changed = [dict(e) for e in self._ref_events
                               if e["seq"] > plan.refs_seq]
                    ts = time.time()
                    plan.state = REJECTED
                    plan.reject_reason = REASON_REFERENCES_CHANGED
                    plan.changed_events = changed
                    plan.completed_ts = ts
                    mgr._record_global(EVT_PLAN_REJECTED, {
                        "plan_id": plan_id,
                        "reason": REASON_REFERENCES_CHANGED,
                        "changed_events": changed,
                        "affected_jobs": plan.affected_jobs(),
                    }, ts=ts)
                    raise Conflict(
                        "reclamation plan is stale: references changed since "
                        "the plan was generated; create a new plan",
                        reason=REASON_REFERENCES_CHANGED)

                if plan.state == PLANNED:
                    ts = time.time()
                    plan.state = RUNNING
                    plan.started_ts = ts
                    mgr._record_global(EVT_PLAN_STARTED,
                                       {"plan_id": plan_id}, ts=ts)

                deleted_blobs = {d["blob"] for d in plan.deleted}

            # 最终可达集重算: 此时无任何引用变更方能运行
            _seq, live, all_refs = self._snapshot_under_write(mgr)

            for cand in plan.candidates:
                if cand.blob in deleted_blobs:
                    continue  # 崩溃续跑: 已删除项不重复处理
                if not mgr.store.exists(cand.blob):
                    self._skip(mgr, plan, cand, SKIP_BLOB_MISSING, [])
                    continue
                if cand.blob in live:
                    # 纵深防御: 水位一致但 blob 仍可达 (理论上不可达),
                    # 绝不删除并给出引用者。
                    refs = [
                        {"job_id": r["job_id"], "generation": r["generation"],
                         "kind": r["kind"], "referrer": r["referrer"]}
                        for r in all_refs.get(cand.blob, []) if r["live"]
                    ]
                    self._skip(mgr, plan, cand, SKIP_STILL_REFERENCED, refs)
                    continue
                ts = time.time()
                removed = mgr.store.delete(cand.blob)
                if not removed:
                    self._skip(mgr, plan, cand, SKIP_BLOB_MISSING, [])
                    continue
                entry = {
                    "blob": cand.blob, "size": cand.size, "kind": cand.kind,
                    "job_id": cand.job_id, "generation": cand.generation,
                    "reason": cand.reason, "ts": ts,
                }
                evt = mgr._record_global(EVT_BLOB_DELETED, {
                    "plan_id": plan_id, **entry,
                }, ts=ts)
                entry["seq"] = evt["seq"]
                with self._lock:
                    plan.deleted.append(entry)
                    deleted_blobs.add(cand.blob)

            with self._lock:
                ts = time.time()
                plan.state = COMPLETED
                plan.completed_ts = ts
                plan.result = (
                    RESULT_COMPLETED_WITH_SKIPS if plan.skipped
                    else RESULT_COMPLETED)
                summary = {
                    "plan_id": plan_id,
                    "deleted_count": len(plan.deleted),
                    "deleted_bytes": plan.deleted_bytes,
                    "skipped_count": len(plan.skipped),
                    "result": plan.result,
                }
                mgr._record_global(EVT_PLAN_COMPLETED, {
                    **summary, "affected_jobs": plan.affected_jobs()}, ts=ts)
        return plan

    @staticmethod
    def _snapshot_under_write(mgr):
        """写锁已持有: 直接快照 (引用变更方已被排空, 水位不会变化)。"""
        gc = mgr.gc
        # _pinned_by_job 需要 gc._lock 保护 (此处与其他路径无竞争, 保持规范)
        with gc._lock:
            live, all_refs = gc._snapshot_jobs(mgr)
        return gc._refs_seq, live, all_refs

    def _skip(self, mgr, plan: ReclamationPlan, cand: Candidate,
              reason: str, referrers: list[dict]) -> None:
        ts = time.time()
        entry = {
            "blob": cand.blob, "size": cand.size, "kind": cand.kind,
            "job_id": cand.job_id, "generation": cand.generation,
            "reason": reason, "referrers": referrers, "ts": ts,
        }
        evt = mgr._record_global(EVT_BLOB_SKIPPED, {
            "plan_id": plan.plan_id, **entry}, ts=ts)
        entry["seq"] = evt["seq"]
        with self._lock:
            plan.skipped.append(entry)

    # ----------------------------------------------------------- plan cancel
    def cancel_plan(self, mgr, plan_id: str) -> ReclamationPlan:
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                raise NotFound(f"unknown reclamation plan {plan_id}")
            if plan.state != PLANNED:
                raise Conflict(
                    f"only {PLANNED} plans can be cancelled "
                    f"(current {plan.state})")
            ts = time.time()
            plan.state = CANCELLED
            plan.completed_ts = ts
            plan.reject_reason = "cancelled"
            mgr._record_global(EVT_PLAN_CANCELLED, {
                "plan_id": plan_id,
                "affected_jobs": plan.affected_jobs()}, ts=ts)
            return plan

    # ---------------------------------------------------------------- queries
    def get_plan(self, plan_id: str) -> ReclamationPlan:
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                raise NotFound(f"unknown reclamation plan {plan_id}")
            return plan

    def list_plans(self) -> list[ReclamationPlan]:
        with self._lock:
            return [self._plans[k] for k in sorted(
                self._plans, key=lambda p: self._plans[p].created_ts)]

    def overview(self, mgr) -> dict:
        """全局回收概览: 当前存储水位、可达/垃圾统计与计划列表。"""
        with self.rw.read():
            refs_seq, live, _all_refs = self._consistent_snapshot(mgr)
            total_blobs = total_bytes = live_bytes = 0
            for digest in mgr.store.all_digests():
                size = mgr.store.blob_size(digest)
                size = size if size is not None else 0
                total_blobs += 1
                total_bytes += size
                if digest in live:
                    live_bytes += size
            return {
                "refs_seq": refs_seq,
                "store": {
                    "total_blobs": total_blobs,
                    "total_bytes": total_bytes,
                    "live_blobs": len(live),
                    "live_bytes": live_bytes,
                    "garbage_blobs": total_blobs - len(live),
                    "garbage_bytes": total_bytes - live_bytes,
                },
                "pins": self.list_pins(),
                "plans": [p.summary() for p in reversed(self.list_plans())],
            }

    def job_view(self, job_id: str) -> dict:
        """作业页面聚合: 该作业的锁定与涉及该作业候选的计划。"""
        with self._lock:
            pins = [dict(p) for p in self._pins.values()
                    if p["job_id"] == job_id]
            related = []
            for plan in self._plans.values():
                touched = any(
                    c.job_id == job_id
                    for c in (list(plan.candidates) + list(plan.excluded)))
                deleted_here = [d for d in plan.deleted
                                if d.get("job_id") == job_id]
                skipped_here = [s for s in plan.skipped
                                if s.get("job_id") == job_id]
                if touched or deleted_here or skipped_here:
                    s = plan.summary()
                    s["job_deleted_count"] = len(deleted_here)
                    s["job_deleted_bytes"] = sum(
                        d.get("size", 0) for d in deleted_here)
                    s["job_skipped_count"] = len(skipped_here)
                    related.append(s)
        return {
            "pins": sorted(pins, key=lambda p: p["generation"]),
            "plans": list(reversed(
                sorted(related, key=lambda p: p["created_ts"]))),
        }

    # ================================================================ replay
    def restore_event(self, evt: dict, store) -> None:
        """事件重放: 重建水位、锁定集与计划 (并执行物理删除)。"""
        etype = evt["type"]
        seq = evt.get("seq") or 0
        if etype in REF_EVENTS:
            with self._lock:
                if seq > self._refs_seq:
                    self._refs_seq = seq
                self._ref_events.append({"seq": seq, "type": etype})

        if etype == EVT_PIN_ADDED:
            self._pins[(evt["job_id"], evt["generation"])] = {
                "job_id": evt["job_id"], "generation": evt["generation"],
                "note": evt.get("note"), "ts": evt.get("ts"),
                "seq": seq}
            return
        if etype == EVT_PIN_REMOVED:
            self._pins.pop((evt["job_id"], evt["generation"]), None)
            return

        if etype == EVT_PLAN_CREATED:
            plan = ReclamationPlan.from_dict(evt["plan"])
            plan.created_seq = seq
            with self._lock:
                self._plans[plan.plan_id] = plan
            return

        with self._lock:
            plan = self._plans.get(evt.get("plan_id"))
        if plan is None:
            return

        if etype == EVT_PLAN_STARTED:
            plan.state = RUNNING
            plan.started_ts = evt.get("ts")
        elif etype == EVT_BLOB_DELETED:
            # 重放即物理删除 (幂等), 保证崩溃后磁盘与引用状态一致
            store.delete(evt["blob"])
            if not any(d["blob"] == evt["blob"] for d in plan.deleted):
                plan.deleted.append({
                    "blob": evt["blob"], "size": evt.get("size", 0),
                    "kind": evt.get("kind"), "job_id": evt.get("job_id"),
                    "generation": evt.get("generation"),
                    "reason": evt.get("reason"), "ts": evt.get("ts"),
                    "seq": seq,
                })
        elif etype == EVT_BLOB_SKIPPED:
            if not any(s["blob"] == evt["blob"] for s in plan.skipped):
                plan.skipped.append({
                    "blob": evt["blob"], "size": evt.get("size", 0),
                    "kind": evt.get("kind"), "reason": evt.get("reason"),
                    "referrers": evt.get("referrers", []),
                    "ts": evt.get("ts"), "seq": seq,
                })
        elif etype == EVT_PLAN_COMPLETED:
            plan.state = COMPLETED
            plan.completed_ts = evt.get("ts")
            plan.result = evt.get("result")
        elif etype == EVT_PLAN_REJECTED:
            plan.state = REJECTED
            plan.reject_reason = evt.get("reason")
            plan.changed_events = evt.get("changed_events", [])
            plan.completed_ts = evt.get("ts")
        elif etype == EVT_PLAN_CANCELLED:
            plan.state = CANCELLED
            plan.completed_ts = evt.get("ts")
            plan.reject_reason = "cancelled"
