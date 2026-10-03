"""内容寻址存储 (CAS) 与输出提交日志。

ContentStore:
  put(data)  -> 若 sha256 已存在则直接复用 (重复检查点的天然去重),
                否则原子写入; 支持测试期注入损坏以验证提交期重校验。
  get(sha256) -> 重新从存储读出字节 (提交校验绝不信任客户端自报哈希)。
  verify(sha256) -> 重算哈希, 任何 bit-rot/损坏立即暴露。

PersistentContentStore: 叠加磁盘后端 (data_dir/blobs/<sha256>), 写入
fsync 落盘; 重启后 blob 按需惰性读回, 校验/恢复在重启后依然成立。

OutputLog:
  输出条目以 (generation, output_id) 唯一。同代际同 idempotency key 的
  重复提交: 内容相同 -> 幂等返回已有条目; 内容不同 -> Conflict。
  代际不同的条目彼此隔离; 能否写入由 LeaseManager 的栅栏决定, OutputLog
  只负责"一旦写入, 永不重复落盘/交付"。
"""

from __future__ import annotations

import os
import tempfile
import threading
from dataclasses import dataclass, field

from .errors import Conflict, NotFound, ValidationError
from .security import sha256_hex


class ContentStore:
    def __init__(self, data_dir: str | None = None):
        self._blobs: dict[str, bytes] = {}
        self._lock = threading.Lock()
        self._data_dir = data_dir
        self._disk = data_dir is not None
        if data_dir is not None:
            self._blob_dir = os.path.join(data_dir, "blobs")
            os.makedirs(self._blob_dir, exist_ok=True)
            # 重启后内存为空, 磁盘上已有哪些 blob 直接列目录
            for name in os.listdir(self._blob_dir):
                self._blobs.setdefault(name, None)  # 惰性加载
        else:
            self._blob_dir = None

    def put(self, data: bytes) -> tuple[str, int, bool]:
        """存储数据, 返回 (sha256, 长度, 是否新写入)。"""
        if not isinstance(data, (bytes, bytearray)):
            raise ValidationError("blob payload must be bytes")
        digest = sha256_hex(data)
        with self._lock:
            existed = digest in self._blobs
            if not existed:
                self._blobs[digest] = bytes(data)
                if self._disk:
                    self._write_durable(digest, bytes(data))
            return digest, len(data), not existed

    def _write_durable(self, digest: str, data: bytes) -> None:
        """同目录临时文件 + fsync + 原子 rename。"""
        path = os.path.join(self._blob_dir, digest)
        if os.path.exists(path):
            return
        fd, tmp = tempfile.mkstemp(dir=self._blob_dir, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as fp:
                fp.write(data)
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _load(self, digest: str) -> bytes | None:
        cached = self._blobs.get(digest)
        if cached is not None:
            return cached
        if self._disk:
            path = os.path.join(self._blob_dir, digest)
            if os.path.exists(path):
                with open(path, "rb") as fp:
                    data = fp.read()
                self._blobs[digest] = data
                return data
        return None

    def get(self, digest: str) -> bytes:
        with self._lock:
            data = self._load(digest)
            if data is None:
                raise NotFound(f"blob not found: {digest[:12]}")
            return data

    def has(self, digest: str) -> bool:
        with self._lock:
            return self._load(digest) is not None

    def exists(self, digest: str) -> bool:
        """仅判断 blob 是否存在 (不读入内容; 供 GC 逐项跳过判断)。"""
        with self._lock:
            if digest not in self._blobs:
                return False
            if self._disk:
                return os.path.exists(os.path.join(self._blob_dir, digest))
            return self._blobs[digest] is not None

    def all_digests(self) -> list[str]:
        """枚举 store 已知的全部 blob 摘要 (含磁盘惰性未载入项)。"""
        with self._lock:
            return list(self._blobs)

    def blob_size(self, digest: str) -> int | None:
        """返回 blob 字节数; 缺失返回 None。磁盘路径优先 stat, 避免读全量。"""
        with self._lock:
            if digest not in self._blobs:
                return None
            if self._disk:
                path = os.path.join(self._blob_dir, digest)
                try:
                    return os.path.getsize(path)
                except OSError:
                    return None
            data = self._blobs[digest]
            return len(data) if data is not None else None

    def delete(self, digest: str) -> bool:
        """物理删除一个 blob, 返回是否删除成功 (缺失为 False, 幂等)。

        GC 执行写锁内调用: 此时没有任何引用变更方能并发 attach 新引用。
        内存/磁盘都先于调用方的删除事件落盘 —— 崩溃时最坏情况是物理文件
        未删 (由重放补删), 绝不会出现"引用还在但文件没了"。
        """
        with self._lock:
            if digest not in self._blobs:
                return False
            if self._disk:
                path = os.path.join(self._blob_dir, digest)
                if not os.path.exists(path):
                    self._blobs.pop(digest, None)
                    return False
                os.unlink(path)
            self._blobs.pop(digest, None)
            return True

    def verify(self, digest: str) -> bool:
        """重算哈希, 损坏/缺失返回 False (调用方决定如何标记检查点)。"""
        with self._lock:
            data = self._load(digest)
        if data is None:
            return False
        return sha256_hex(data) == digest

    # -- 测试支持: 模拟存储 bit-rot / 节点磁盘损坏 --
    def corrupt(self, digest: str) -> None:
        with self._lock:
            data = self._load(digest)
            if data is None:
                raise NotFound(f"blob not found: {digest[:12]}")
            corrupted = data + b"\x00CORRUPT"
            self._blobs[digest] = corrupted
            if self._disk:
                # 内存损坏同时覆盖磁盘, 模拟真实 bit-rot 持久化
                with open(os.path.join(self._blob_dir, digest), "wb") as fp:
                    fp.write(corrupted)
                    fp.flush()
                    os.fsync(fp.fileno())

    def blob_count(self) -> int:
        with self._lock:
            return sum(1 for d in self._blobs if self._load(d) is not None)


@dataclass
class OutputRecord:
    generation: int
    output_id: str
    blob: str
    size: int
    rank: int | None
    ts: float

    def to_dict(self) -> dict:
        return {
            "generation": self.generation,
            "output_id": self.output_id,
            "blob": self.blob,
            "size": self.size,
            "rank": self.rank,
            "ts": self.ts,
        }


class OutputLog:
    """每作业一份的输出去重日志 (JobManager 内按 generation 隔离)。"""

    def __init__(self):
        self._lock = threading.Lock()
        # (generation, output_id) -> OutputRecord
        self._records: dict[tuple[int, str], OutputRecord] = {}

    def append(
        self,
        *,
        generation: int,
        output_id: str,
        blob: str,
        size: int,
        rank: int | None,
        ts: float,
    ) -> tuple[OutputRecord, bool]:
        """返回 (记录, 是否新写入)。重复但内容相同 -> 幂等; 冲突 -> 409。"""
        key = (generation, output_id)
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                if existing.blob != blob:
                    raise Conflict(
                        f"output_id {output_id!r} already committed in "
                        f"generation {generation} with different content"
                    )
                return existing, False
            rec = OutputRecord(
                generation=generation,
                output_id=output_id,
                blob=blob,
                size=size,
                rank=rank,
                ts=ts,
            )
            self._records[key] = rec
            return rec, True

    def restore(
        self,
        *,
        generation: int,
        output_id: str,
        blob: str,
        size: int,
        rank: int | None,
        ts: float,
    ) -> None:
        """事件重放入口: 已提交事实按 (generation, output_id) 唯一插入。"""
        self._records[(generation, output_id)] = OutputRecord(
            generation=generation, output_id=output_id, blob=blob,
            size=size, rank=rank, ts=ts)

    def list(self, generation: int | None = None) -> list[OutputRecord]:
        with self._lock:
            recs = list(self._records.values())
        if generation is not None:
            recs = [r for r in recs if r.generation == generation]
        recs.sort(key=lambda r: r.ts)
        return recs
