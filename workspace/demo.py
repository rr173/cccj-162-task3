#!/usr/bin/env python3
"""端到端演示: 抢占纪元 -> 节点失联 -> 分区恢复 -> 完整检查点 ->
确认释放 -> 同代际恢复 + 新输出租约, 以及旧进程迟到写入被栅栏拒绝。

用法:
    python3 demo.py            # 直接跑领域流程, 打印恢复验证报告 (Markdown)
    python3 demo.py --serve    # 同时起 HTTP 服务, 可用浏览器打开进度页
"""

from __future__ import annotations

import argparse

from hpc_cr import JobManager
from hpc_cr.api import build_server
from hpc_cr.epochs import Scheduler
from hpc_cr.errors import FenceViolation, InvalidToken
from hpc_cr.security import Signer


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def run_demo(data_dir: str | None = None) -> tuple[JobManager, str, str, str]:
    from hpc_cr.security import Signer as _S
    secret = "demo-secret-key"
    mgr = JobManager(Scheduler(_S(secret)), data_dir=data_dir)
    placement = {0: "n-a", 1: "n-a", 2: "n-b", 3: "n-b"}
    try:
        job = mgr.create_job(
            name="climate-simulation", world_size=4, shards_per_rank=2,
            rank_nodes=placement, job_id="climate-7", lease_ttl=600)
    except Exception:  # noqa: BLE001 - 持久化目录重跑时作业已存在
        job = mgr.get("climate-7")
        return mgr, "climate-7", "", job.leases.current().token
    print(f"创建作业 climate-7 (4 进程 × 2 分片), 节点: {placement}")

    # 进程持续推进输入位点
    for r in range(4):
        mgr.report_progress("climate-7", r, input_pos=10000 + r)
    gen0_lease = job.leases.current().token

    banner("1) 调度器回收节点 -> 签发抢占纪元 epoch1/gen1")
    pre = mgr.preempt("climate-7", reason="higher-priority-job",
                      grace_seconds=30)
    token = pre["epoch_token"]
    print(f"epoch={pre['epoch']['epoch']} generation={pre['epoch']['generation']}"
          f" 宽限={pre['epoch']['grace_seconds']}s")
    print("旧输出租约 gen0 已随抢占吊销:", job.leases.get(0).state)

    banner("2) 节点 n-b 失联 (ranks 2,3 的分片缺失)")
    lost = mgr.mark_node_lost("climate-7", "n-b")
    print("失联 ranks:", lost["lost_ranks"])
    for r in (0, 1):
        for i in range(2):
            pos = 10000 + r * 10 + i
            mgr.upload_shard(
                "climate-7", epoch_token=token, rank=r, index=i,
                node="n-a", input_pos=pos,
                data=f"rank{r}.part{i} bytes@{pos}".encode())
    print("r0/r1 分片已上传; 尝试提交 ->")
    try:
        mgr.commit_checkpoint("climate-7", epoch_token=token)
    except Exception as exc:  # noqa: BLE001
        print("  提交被拒绝:", exc)
    print("尝试确认释放 ->")
    try:
        mgr.confirm_release("climate-7", epoch_token=token)
    except Exception as exc:  # noqa: BLE001
        print("  释放被拒绝:", exc)

    banner("3) 网络分区恢复: n-b 心跳复活, 补齐分片")
    mgr.heartbeat("climate-7", "n-b")
    for r in (2, 3):
        for i in range(2):
            pos = 10000 + r * 10 + i
            mgr.upload_shard(
                "climate-7", epoch_token=token, rank=r, index=i,
                node="n-b", input_pos=pos,
                data=f"rank{r}.part{i} bytes@{pos}".encode())
    # 模拟重复投递: 同一分片再传一次 (幂等)
    res = mgr.upload_shard(
        "climate-7", epoch_token=token, rank=0, index=0,
        node="n-a", input_pos=10000,
        data=b"rank0.part0 bytes@10000")
    print("重复分片幂等 stored =", res["stored"])
    cp = mgr.commit_checkpoint("climate-7", epoch_token=token)
    print("检查点状态:", cp["checkpoint"]["state"],
          "分片:", cp["checkpoint"]["received"], "/",
          cp["checkpoint"]["expected"])

    banner("4) 全部分片校验完成 -> 确认释放资源")
    rel = mgr.confirm_release("climate-7", epoch_token=token)
    print("released =", rel["released"], " generation =", rel["generation"],
          " 作业状态 =", job.state)

    banner("5) 同代际恢复: 再次全量重校验 + 重新签发输出租约")
    new_placement = dict(placement)
    new_placement[3] = "n-c"  # rank3 调度到替代节点
    report = mgr.restore("climate-7", generation=1,
                         new_rank_nodes=new_placement)
    print(report.to_markdown())

    banner("6) 旧进程迟到写入 vs 新进程写入 (双重输出防护)")
    try:
        mgr.append_output(
            "climate-7", lease_token=gen0_lease,
            output_id="climate-output-0001",
            data=b"late write from preempted process")
        print("!! 旧进程写入竟然成功 (错误)")
    except (FenceViolation, InvalidToken) as exc:
        print(f"旧进程迟到写入被拒绝 ({exc.http_status}/"
              f"{exc.error_code}/{exc.reason}): {exc}")
        sec = mgr.security_events("climate-7")
        print("已写入安全事件 seq:", [e["seq"] for e in sec],
              "原因:", [e["reason"] for e in sec])

    new_lease = job.leases.current().token
    out1 = mgr.append_output(
        "climate-7", lease_token=new_lease,
        output_id="climate-output-0001", rank=0,
        data=b"authoritative output after restore")
    out2 = mgr.append_output(
        "climate-7", lease_token=new_lease,
        output_id="climate-output-0001", rank=0,
        data=b"authoritative output after restore")
    print(f"新进程首次写入 delivered={out1['delivered']}, "
          f"重试 delivered={out2['delivered']} (幂等不重复)")
    print("输出日志:", [(o["generation"], o["output_id"])
                       for o in mgr.list_outputs("climate-7")])
    print("审计事件:", [a["event"] for a in job.audit])

    banner("7) 第二轮抢占 (gen1 -> gen2): 旧代际分片/输出成为回收垃圾")
    pre2 = mgr.preempt("climate-7", force_new_epoch=True)
    token2 = pre2["epoch_token"]
    placement2 = {r: ("n-c" if r == 3 else ("n-a" if r in (0, 1) else "n-b"))
                  for r in range(4)}
    for r in range(4):
        for i in range(2):
            pos = 20000 + r * 10 + i
            mgr.upload_shard(
                "climate-7", epoch_token=token2, rank=r, index=i,
                node=placement2[r], input_pos=pos,
                data=f"gen2-rank{r}.part{i}@{pos}".encode())
    mgr.commit_checkpoint("climate-7", epoch_token=token2)
    mgr.confirm_release("climate-7", epoch_token=token2)
    mgr.restore("climate-7", generation=2, new_rank_nodes=placement2)
    new_lease = job.leases.current().token
    mgr.append_output(
        "climate-7", lease_token=new_lease, output_id="climate-output-0002",
        rank=0, data=b"gen2 authoritative output")
    print("gen2 已恢复运行; gen1 的 8 个分片 + 1 个输出现已不可恢复。")

    banner("8) 空间回收: 计划(水位) -> 引用变化拒绝 -> 新计划执行")
    # 先锁定 gen1 保留 (示例: 合规要求保留该代际)
    try:
        mgr.pin_generation("climate-7", 1, note="合规保留 gen1")
        print("已锁定 gen1 (该代际分片/输出不会被回收)")
    except Exception as exc:  # noqa: BLE001 - 持久化重跑时已锁定
        print("gen1 锁定已存在:", getattr(exc, "reason", exc))

    plan = mgr.create_reclamation_plan(note="demo-reclaim")
    print(f"计划 {plan['plan_id']}: 引用水位 seq={plan['refs_seq']}, "
          f"候选 {len(plan['candidates'])} 项, "
          f"预计释放 {plan['estimated_release_bytes']} 字节")
    for c in plan["candidates"][:5]:
        print(f"  - {c['kind']:6s} {c['blob'][:12]} "
              f"({c['size']}B, {c['reason']})")

    # 解锁后引用关系变化 -> 旧计划必须被拒绝
    mgr.unpin_generation("climate-7", 1)
    print("已解除 gen1 锁定; 旧计划水位作废")
    try:
        mgr.execute_reclamation_plan(plan["plan_id"])
    except Exception as exc:  # noqa: BLE001
        print(f"旧计划执行被拒绝 ({exc.http_status}/{exc.reason})")

    fresh = mgr.create_reclamation_plan(note="demo-reclaim-v2")
    result = mgr.execute_reclamation_plan(fresh["plan_id"])
    print(f"新计划执行完成: {result['result']}, 实际删除 "
          f"{len(result['deleted'])} 项 / {result['deleted_bytes']} 字节, "
          f"跳过 {len(result['skipped'])} 项")
    ov = mgr.reclamation_overview()
    print("回收后存储水位:", ov["store"])
    print("全局回收审计页: GET /ui/reclamation")
    print("计划详情:       /api/reclamation/plans/" + fresh["plan_id"])

    return mgr, "climate-7", token2, new_lease


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true",
                    help="演示后继续运行 HTTP 服务")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--data-dir", default=None,
                    help="持久化数据目录; 给定后重启仍可查询作业/安全事件")
    args = ap.parse_args()

    mgr, job_id, _, _ = run_demo(data_dir=args.data_dir)
    if not args.serve:
        return
    server = build_server(mgr, host="0.0.0.0", port=args.port, verbose=True)
    print(f"\nHTTP 服务已启动: http://127.0.0.1:{args.port}")
    print(f"节点进度页面:   http://127.0.0.1:{args.port}/ui/jobs/{job_id}")
    print("Ctrl-C 退出")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
