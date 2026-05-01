from __future__ import annotations

from agent.notifiers.base import FormattedMessage


def _fmt_pct(value: float) -> str:
    return f"{value:+.2%}"


def _fmt_money(value: float | None) -> str:
    return "N/A" if value is None else f"¥{value:.2f}"


def build_message(payload: dict) -> FormattedMessage:
    header = f"[Papertrade] {payload['strategy']} · {payload['sample']} · {payload['rebalance_freq']}调仓"
    holdings_lines = []
    for item in payload["holdings"]:
        move = "N/A" if item.pct_chg is None else _fmt_pct(item.pct_chg / 100.0)
        holdings_lines.append(f"{item.code} {item.name}  {_fmt_money(item.close)} ({move})")
    body = "\n".join(
        [
            f"日期: {payload['today']:%Y-%m-%d}",
            "",
            f"当前持仓 ({payload['holdings_count']} 只)",
            *holdings_lines,
            "",
            "当日表现",
            f"策略 long 收益率: {_fmt_pct(payload['latest_long'])}",
            f"基准收益率: {_fmt_pct(payload['latest_bench_ret'])}",
            f"超额收益率: {_fmt_pct(payload['excess_ret'])}",
            "",
            "累计净值",
            f"策略: {payload['strategy_nav']:.3f} ({_fmt_pct(payload['strategy_total_ret'])})",
            f"基准: {payload['bench_nav']:.3f} ({_fmt_pct(payload['bench_total_ret'])})",
            f"夏普: {payload['sharpe']:.2f}",
            "",
            "换仓状态",
            f"下一换仓日: {payload['next_rebalance_date']:%Y-%m-%d} (距今 {payload['days_until_rebalance']} 天)",
        ]
    )
    return FormattedMessage(subject=header, body=body)
