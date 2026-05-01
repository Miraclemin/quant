from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd


def _load_trade_days(csv_path: Path) -> list[date]:
    df = pd.read_csv(csv_path, dtype={"cal_date": str})
    dates = pd.to_datetime(df["cal_date"], format="%Y%m%d", errors="coerce").dropna()
    return sorted(d.date() for d in dates)


def is_trading_day(target: date, csv_path: Path) -> bool:
    return target in set(_load_trade_days(csv_path))


def next_rebalance_date(today: date, freq: str, csv_path: Path) -> date:
    trade_days = _load_trade_days(csv_path)
    if not trade_days:
        raise ValueError(f"No trading days found in {csv_path}")
    future_days = [day for day in trade_days if day >= today]
    if not future_days:
        raise ValueError(f"No future trading days available for {today}")
    if freq == "日度":
        return future_days[0]
    if freq == "周度":
        current_week = today.isocalendar()[:2]
        same_week = [day for day in future_days if day.isocalendar()[:2] == current_week]
        return same_week[-1] if same_week else future_days[0]
    if freq == "月度":
        same_month = [day for day in future_days if day.year == today.year and day.month == today.month]
        return same_month[-1] if same_month else future_days[0]
    raise ValueError(f"Unsupported frequency: {freq}")


def format_trade_date(value: int | str) -> date:
    return datetime.strptime(str(value), "%Y%m%d").date()
