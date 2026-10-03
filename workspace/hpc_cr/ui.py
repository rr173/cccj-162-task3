"""节点进度页面 (服务端渲染 HTML, 2 秒自动刷新)。

展示: 作业与抢占纪元概览、检查点分片矩阵、节点心跳/失联、进程位点、
输出条目、审计事件流 (含栅栏拒绝记录)。
"""

from __future__ import annotations

import html
import time


def _h(v) -> str:
    return html.escape(str(v), quote=True)


_STATE_COLORS = {
    "COMPLETE": "#0a7d2c",
    "PENDING": "#b26a00",
    "INCOMPLETE": "#b00020",
    "SUPERSEDED": "#666",
    "RUNNING": "#0a7d2c",
    "PREEMPTING": "#b26a00",
    "RELEASED": "#1a4fb3",
    "ABORTED": "#b00020",
    "UP": "#0a7d2c",
    "ALIVE": "#0a7d2c",
    "LOST": "#b00020",
    "PLANNED": "#1a4fb3",
    "RUNNING": "#b26a00",
    "COMPLETED": "#0a7d2c",
    "REJECTED": "#b00020",
    "CANCELLED": "#666",
}


def _badge(state: str) -> str:
    color = _STATE_COLORS.get(state, "#444")
    return (f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:10px;font-size:12px">{_h(state)}</span>')


def _ts(ts: float | None) -> str:
    if not ts:
        return "-"
    return time.strftime("%H:%M:%S", time.localtime(ts))


def _gb(n: int) -> str:
    """人类可读字节数。"""
    f = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if f < 1024 or unit == "TiB":
            return f"{f:.1f} {unit}" if unit != "B" else f"{int(f)} B"
        f /= 1024
    return f"{f:.1f} TiB"


_SKIP_REASON_CN = {
    "still_referenced": "仍被引用",
    "blob_missing": "blob 已不存在",
    "references_changed": "计划生成后引用发生变化",
    "cancelled": "已取消",
}


def render_reclamation_job_block(recl: dict) -> str:
    """作业页面内的空间回收区块 (代际锁定 + 涉及该作业的计划)。"""
    pins = recl.get("pins") or []
    pin_rows = "".join(
        f"<tr><td>gen{_h(p['generation'])}</td>"
        f"<td>{_ts(p.get('ts'))}</td><td>{_h(p.get('note') or '-')}</td></tr>"
        for p in pins
    ) or '<tr><td colspan="3">无锁定代际</td></tr>'

    def plan_row(p):
        return (
            f"<tr><td><code>{_h(p['plan_id'])}</code><br>"
            f"<small><a href=\"/api/reclamation/plans/{_h(p['plan_id'])}\">"
            f"JSON</a></small></td>"
            f"<td>{_badge(p['state'])}</td>"
            f"<td>{_gb(p['estimated_release_bytes'])}</td>"
            f"<td>{p.get('job_deleted_count', 0)} 项 / "
            f"{_gb(p.get('job_deleted_bytes', 0))}</td>"
            f"<td>{p.get('job_skipped_count', 0)}</td>"
            f"<td>{_h(_SKIP_REASON_CN.get(p.get('reject_reason'), ''))}</td>"
            f"<td>{_ts(p['created_ts'])}</td></tr>"
        )

    plan_rows = "".join(plan_row(p) for p in recl.get("plans") or []) \
        or '<tr><td colspan="7">尚无涉及本作业的回收计划</td></tr>'
    return f"""
<h2>空间回收 (内容寻址存储)</h2>
<p><small>完整回收审计: <a href="/ui/reclamation">全局回收页面</a> ·
<a href="/api/reclamation">/api/reclamation</a></small></p>
<h3>保留代际锁定 (pin)</h3>
<table border="1" cellpadding="6"><tr><th>代际</th><th>锁定时间</th>
<th>备注</th></tr>{pin_rows}</table>
<h3>涉及本作业的回收计划</h3>
<table border="1" cellpadding="6"><tr><th>计划</th><th>状态</th>
<th>预计释放</th><th>本作业实际删除</th><th>跳过</th>
<th>拒绝原因</th><th>生成时间</th></tr>{plan_rows}</table>"""


def render_reclamation_page(data: dict) -> str:
    store = data["store"]
    pins = data.get("pins") or []
    plans = data.get("plans") or []

    pin_rows = "".join(
        f"<tr><td>{_h(p['job_id'])}</td><td>gen{_h(p['generation'])}</td>"
        f"<td>{_ts(p.get('ts'))}</td><td>{_h(p.get('note') or '-')}</td></tr>"
        for p in pins
    ) or '<tr><td colspan="4">无</td></tr>'

    def plan_row(p):
        result = p.get("result") or _h(_SKIP_REASON_CN.get(
            p.get("reject_reason"), ""))
        return (
            f"<tr><td><code>{_h(p['plan_id'])}</code></td>"
            f"<td>{_badge(p['state'])}</td>"
            f"<td>{p['candidate_count']} 项 / "
            f"{_gb(p['estimated_release_bytes'])}</td>"
            f"<td>{p['deleted_count']} 项 / {_gb(p['deleted_bytes'])}</td>"
            f"<td>{p['skipped_count']}</td>"
            f"<td>{_h(result)}</td>"
            f"<td>{_ts(p['created_ts'])}</td>"
            f"<td><small><a href=\"/api/reclamation/plans/{_h(p['plan_id'])}\">"
            f"详情 JSON</a></small></td></tr>"
        )

    plan_rows = "".join(plan_row(p) for p in plans) \
        or '<tr><td colspan="8">尚无回收计划</td></tr>'
    return f"""<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta http-equiv="refresh" content="5">
<title>空间回收 — HPC</title>
<style>
 body{{font-family:system-ui,sans-serif;margin:24px;color:#222}}
 table{{border-collapse:collapse;margin:8px 0 20px}}
 td,th{{font-size:13px;text-align:left;vertical-align:top}}
 code{{background:#f4f4f4;padding:1px 4px;border-radius:3px}}
</style></head><body>
<p><a href="/">← 作业列表</a></p>
<h1>内容寻址存储 · 空间回收审计</h1>
<h2>存储水位 <small>(引用水位 seq {_h(data.get('refs_seq'))})</small></h2>
<table border="1" cellpadding="8">
<tr><th>总 blob</th><th>总占用</th><th>可达 (保留)</th>
<th>可回收垃圾</th></tr><tr>
<td>{store['total_blobs']}</td>
<td>{_gb(store['total_bytes'])} ({store['total_bytes']} B)</td>
<td>{store['live_blobs']} / {_gb(store['live_bytes'])}</td>
<td><b>{store['garbage_blobs']}</b> / <b>{_gb(store['garbage_bytes'])}</b>
 ({store['garbage_bytes']} B)</td>
</tr></table>
<h2>保留代际锁定 (pin)</h2>
<table border="1" cellpadding="6"><tr><th>作业</th><th>代际</th>
<th>锁定时间</th><th>备注</th></tr>{pin_rows}</table>
<h2>回收计划</h2>
<p><small>计划先枚举全部不可达 blob 并冻结引用水位; 执行时若引用关系发生
变化将被拒绝 (状态 REJECTED)。崩溃后续跑/重试同一计划是安全的。
共 {len(plans)} 个, 最近在前。</small></p>
<table border="1" cellpadding="6"><tr><th>计划 ID</th><th>状态</th>
<th>预计释放 (候选)</th><th>实际删除</th><th>跳过</th>
<th>最终结果</th><th>生成时间</th><th></th></tr>{plan_rows}</table>
<p><small>API: <code>POST /api/reclamation/plans</code> 生成 ·
<code>POST /api/reclamation/plans/&lt;id&gt;/execute</code> 执行 ·
<code>/cancel</code> 取消 · 作业内
<code>action=pin_generation/unpin_generation</code> 锁定/解锁代际 ·
页面每 5 秒自动刷新</small></p>
</body></html>"""


def render_progress_page(data: dict) -> str:
    job = data["job"]
    ep = job.get("epoch")
    cp = job.get("current_checkpoint")
    shards = data["shards"]
    audit = data["audit"]

    # --- 分片矩阵: 每个代际一块, 行=rank 列=shard index ---
    matrices = []
    for c in data["checkpoints"]:
        g = c["generation"]
        head = "".join(
            f"<th>s{i}</th>" for i in range(c["shards_per_rank"])
        )
        rows = []
        for r in range(c["world_size"]):
            cells = []
            for i in range(c["shards_per_rank"]):
                key = f"gen{g}:r{r}:s{i}"
                s = shards.get(key)
                if s:
                    cells.append(
                        '<td title="blob ' + _h(s["blob"][:16]) + ' @'
                        + _h(s["node"]) + '" style="background:#e6f4ea">✓ '
                        + _h(s["blob"][:8]) + f'<br><small>pos {s["input_pos"]}'
                        + "</small></td>"
                    )
                else:
                    cells.append('<td style="background:#fdecea">—</td>')
            node = next(
                (p["node"] for p in c["processes"] if p["rank"] == r), "?"
            )
            rows.append(
                f"<tr><td><b>r{r}</b><br><small>{_h(node)}</small></td>"
                + "".join(cells) + "</tr>"
            )
        matrices.append(f"""
        <h3>检查点 gen{g} (epoch {c['epoch']}) {_badge(c['state'])}</h3>
        <table border="1" cellpadding="6" style="border-collapse:collapse">
        <tr><th>rank @ node</th>{head}</tr>
        {''.join(rows)}
        </table>
        <p><small>进度 {c['received']}/{c['expected']} 分片,
        缺失: {_h(c['missing'] or '无')}
        {'· 完成于 ' + _ts(c['completed_ts']) if c['completed_ts'] else ''}
        </small></p>""")

    node_rows = "".join(
        f"<tr><td>{_h(n['node_id'])}</td><td>{_badge(n['state'])}</td>"
        f"<td>{_ts(n['last_heartbeat'])}</td>"
        f"<td>{_h(n['ranks'])}</td></tr>"
        for n in data["nodes"]
    )
    proc_rows = "".join(
        f"<tr><td>r{p['rank']}</td><td>{_h(p['node'])}</td>"
        f"<td>{_badge(p['state'])}</td>"
        f"<td>inc {p['incarnation']}</td>"
        f"<td>{p['last_input_pos']}</td>"
        f"<td>{_ts(p['last_progress_ts'])}</td></tr>"
        for p in data["processes"]
    )
    out_rows = "".join(
        f"<tr><td>gen{o['generation']}</td><td>{_h(o['output_id'])}</td>"
        f"<td>{_h(o['blob'][:12])}</td><td>{o['size']}</td>"
        f"<td>{_h(o['rank'])}</td><td>{_ts(o['ts'])}</td></tr>"
        for o in data["outputs"]
    ) or '<tr><td colspan=6>暂无输出</td></tr>'
    audit_rows = "".join(
        f"<tr><td>{_ts(a['ts'])}</td><td><code>{_h(a['event'])}</code></td>"
        f"<td><small>{_h(a['detail'])}</small></td></tr>"
        for a in audit[:60]
    )
    security = data.get("security_events") or []
    sec_rows = "".join(
        f"<tr><td>{_h(e.get('seq'))}</td><td>{_ts(e['ts'])}</td>"
        f"<td><code>{_h(e.get('credential'))}</code></td>"
        f"<td><code>{_h(e.get('operation'))}</code></td>"
        f"<td>{_h(e.get('reason'))}</td>"
        f"<td>{_h(e.get('http_status'))}</td>"
        f"<td>{_h(e.get('expected_generation'))}</td>"
        f"<td><small>{_h(e.get('output_id', ''))} "
        f"{_h(e.get('claimed', {}))}</small></td></tr>"
        for e in security[:100]
    ) or '<tr><td colspan=8>暂无安全事件</td></tr>'

    grace = job.get("epoch_grace_remaining")
    ep_html = ""
    if ep:
        ep_html = f"""
        <h2>抢占纪元 epoch {ep['epoch']} / gen{ep['generation']}</h2>
        <p>原因: {_h(ep['reason'])} · 范围: {_h(ep['kind'])}
        {_h(ep['nodes'])} · 剩余宽限: <b>{grace:.1f}s</b>
        · 截止 {_ts(ep['deadline_ts'])}</p>"""

    return f"""<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta http-equiv="refresh" content="2">
<title>节点进度 — {_h(job['job_id'])}</title>
<style>
 body{{font-family:system-ui,sans-serif;margin:24px;color:#222}}
 table{{border-collapse:collapse;margin:8px 0 20px}}
 td,th{{font-size:13px;text-align:left;vertical-align:top}}
 code{{background:#f4f4f4;padding:1px 4px;border-radius:3px}}
</style></head><body>
<p><a href="/">← 作业列表</a> · <a href="/api/jobs/{_h(job['job_id'])}/progress.json">JSON</a></p>
<h1>作业 {_h(job['job_id'])} <small>({_h(job['name'])})</small> {_badge(job['state'])}</h1>
<p>规模 {job['world_size']} 进程 × {job['shards_per_rank']} 分片 ·
当前代际 gen{job['current_generation']} ·
输出生效代际 gen{job['output_generation']} ·
已释放代际 {('gen'+str(job['released_generation'])) if job['released_generation'] is not None else '-'} ·
已恢复代际 {('gen'+str(job['restored_generation'])) if job['restored_generation'] is not None else '-'} ·
输出条目 {job['outputs_total']}</p>
{ep_html}
<h2>检查点分片矩阵</h2>
{''.join(matrices) or '<p>尚无检查点</p>'}
<h2>节点</h2>
<table border="1" cellpadding="6"><tr><th>节点</th><th>状态</th>
<th>最近心跳</th><th>ranks</th></tr>{node_rows}</table>
<h2>进程与输入位点</h2>
<table border="1" cellpadding="6"><tr><th>rank</th><th>节点</th><th>状态</th>
<th>incarnation</th><th>input_pos</th><th>上报时间</th></tr>{proc_rows}</table>
<h2>已提交输出 (代际隔离, 幂等去重)</h2>
<table border="1" cellpadding="6"><tr><th>代际</th><th>output_id</th>
<th>blob</th><th>size</th><th>rank</th><th>时间</th></tr>{out_rows}</table>
<h2>安全事件 — 授权失败 (顺序化, 不含凭证原文)</h2>
<p><small>共 {len(security)} 条, 显示最近 100 条 ·
<a href="/api/jobs/{_h(job['job_id'])}/security-events">JSON</a></small></p>
<table border="1" cellpadding="6"><tr><th>seq</th><th>时间</th>
<th>凭证类型</th><th>操作</th><th>原因</th><th>HTTP</th>
<th>期望代际</th><th>目标/声明绑定</th></tr>{sec_rows}</table>
{render_reclamation_job_block(data.get("reclamation") or {"pins": [], "plans": []})}
<h2>审计事件 (最近 60 条)</h2>
<table border="1" cellpadding="6"><tr><th>时间</th><th>事件</th>
<th>详情</th></tr>{audit_rows}</table>
<p><small>页面每 2 秒自动刷新 · server_ts {_ts(data['server_ts'])}</small></p>
</body></html>"""
