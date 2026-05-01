from __future__ import annotations

from agent.notifiers.base import FormattedMessage


def build_message(payload: dict) -> FormattedMessage:
    subject = f"[Livetrade] {payload['strategy']} · {payload['today']:%Y-%m-%d}"
    if not payload["is_rebalance_day"]:
        lines = [f"今日无需操作, 持有以下 {payload['holdings_count']} 只:"]
        lines.extend(f"{code} {name}" for code, name in payload["holdings"])
        lines.extend(["", f"下一换仓日预估: {payload['next_rebalance_date']:%Y-%m-%d}"])
        return FormattedMessage(subject=subject, body="\n".join(lines))
    lines = ["今日为换仓日。", "", f"卖出 ({len(payload['sells'])} 只)"]
    lines.extend(f"{code} {name}  ~ ¥{payload['per_stock_amount']:,.0f}" for code, name in payload["sells"])
    lines.extend(["", f"买入 ({len(payload['buys'])} 只)"])
    lines.extend(f"{code} {name}  ~ ¥{payload['per_stock_amount']:,.0f}" for code, name in payload["buys"])
    lines.extend(["", f"预估手续费: ¥{payload['est_fee']:.0f}"])
    return FormattedMessage(subject=f"{subject} ★换仓日★", body="\n".join(lines))
