from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from agent.config import AgentConfig
from agent.runners.papertrade import _read_holdings_csv, _resolve_holdings_path
from agent.utils.trading_calendar import format_trade_date, next_rebalance_date

FEE_RATE = 0.00025


def _parse_codes(row: pd.Series) -> list[str]:
    return [item.strip() for item in str(row["持仓股票"]).split(",") if item.strip()]


def _parse_names(row: pd.Series) -> dict[str, str]:
    codes = _parse_codes(row)
    names = [item.strip() for item in str(row["持仓股票名称"]).split(",") if item.strip()]
    return {code: names[idx] if idx < len(names) else code for idx, code in enumerate(codes)}


def build(cfg: AgentConfig, today: pd.Timestamp | None = None) -> dict[str, Any]:
    holdings_path = _resolve_holdings_path(cfg)
    df = _read_holdings_csv(holdings_path)
    if len(df) < 1:
        raise RuntimeError(f"No holdings found in {holdings_path}")
    current = df.iloc[0]
    asof = today.date() if today is not None else pd.Timestamp.now().date()
    rebalance_date = format_trade_date(int(current["换仓日"]))
    current_names = _parse_names(current)
    current_codes = _parse_codes(current)
    if rebalance_date != asof or len(df) < 2:
        return {
            "strategy": cfg.strategy,
            "today": asof,
            "is_rebalance_day": False,
            "holdings": [(code, current_names[code]) for code in current_codes],
            "holdings_count": int(current["持仓数量"]),
            "next_rebalance_date": next_rebalance_date(asof, cfg.rebalance_freq, cfg.project_root / "Data" / "Metadata" / "trade_day.csv"),
            "holdings_path": str(holdings_path),
        }
    previous = df.iloc[1]
    previous_names = _parse_names(previous)
    previous_codes = _parse_codes(previous)
    current_set = set(current_codes)
    previous_set = set(previous_codes)
    sells = [(code, previous_names.get(code, code)) for code in previous_codes if code not in current_set]
    buys = [(code, current_names.get(code, code)) for code in current_codes if code not in previous_set]
    per_stock_amount = cfg.account_size_cny / cfg.n_top
    est_fee = cfg.account_size_cny * ((len(sells) + len(buys)) / cfg.n_top) * FEE_RATE
    return {
        "strategy": cfg.strategy,
        "today": asof,
        "is_rebalance_day": True,
        "sells": sells,
        "buys": buys,
        "per_stock_amount": per_stock_amount,
        "est_fee": est_fee,
        "holdings_path": str(holdings_path),
    }
