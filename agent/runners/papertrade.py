from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import pandas as pd

from agent.config import AgentConfig
from agent.utils.trading_calendar import format_trade_date, next_rebalance_date


@dataclass(slots=True)
class HoldingRow:
    code: str
    name: str
    close: float | None
    pct_chg: float | None


def _import_duckdb():
    import duckdb

    return duckdb


def _resolve_holdings_path(cfg: AgentConfig) -> Path:
    for output_dir in cfg.output_candidates:
        path = output_dir / "trade_holdings.csv"
        if path.exists():
            return path
    raise FileNotFoundError("trade_holdings.csv not found in output directories")


def _read_holdings_csv(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "gb18030", "gbk", "utf-8"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Failed to read holdings csv: {path}") from last_error


def _load_latest_market_data(cfg: AgentConfig, codes: list[str]) -> dict[str, tuple[float | None, float | None]]:
    if not codes:
        return {}
    try:
        duckdb = _import_duckdb()
    except ModuleNotFoundError:
        if os.getenv("AGENT_DRY_RUN") == "1":
            return {code: (None, None) for code in codes}
        raise
    placeholders = ", ".join(["?"] * len(codes))
    query = f"""
        WITH latest AS (
            SELECT MAX(trade_date) AS max_trade_date
            FROM stock_bar
        )
        SELECT ts_code, close, pct_chg
        FROM stock_bar, latest
        WHERE trade_date = latest.max_trade_date
          AND ts_code IN ({placeholders})
    """
    with duckdb.connect(str(cfg.db_path), read_only=True) as conn:
        rows = conn.execute(query, codes).fetchall()
    return {str(code): (float(close), float(pct_chg)) for code, close, pct_chg in rows}


def _load_trade_stats(cfg: AgentConfig, fallback_trade_date: int | None = None) -> dict[str, Any]:
    try:
        duckdb = _import_duckdb()
    except ModuleNotFoundError:
        if os.getenv("AGENT_DRY_RUN") == "1":
            return {
                "latest_trade_date": fallback_trade_date or int(pd.Timestamp.now().strftime("%Y%m%d")),
                "latest_long": 0.0,
                "latest_bench_ret": 0.0,
                "strategy_nav": 1.0,
                "bench_nav": 1.0,
                "sharpe": 0.0,
            }
        raise
    table = f"{cfg.strategy}_trade_daily_ret"
    with duckdb.connect(str(cfg.db_path), read_only=True) as conn:
        latest = conn.execute(
            f"SELECT trade_date, long, bench_ret FROM {table} ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        daily_rows = conn.execute(
            f"SELECT trade_date, long, bench_ret FROM {table} ORDER BY trade_date"
        ).fetch_df()
    if latest is None:
        raise RuntimeError(f"No rows found in {table}")
    strategy_curve = (1 + daily_rows["long"].fillna(0.0)).cumprod()
    bench_curve = (1 + daily_rows["bench_ret"].fillna(0.0)).cumprod()
    sharpe = 0.0
    long_series = daily_rows["long"].dropna()
    if not long_series.empty and long_series.std() not in (0, 0.0):
        sharpe = float(long_series.mean() / long_series.std() * (242 ** 0.5))
    return {
        "latest_trade_date": int(latest[0]),
        "latest_long": float(latest[1]),
        "latest_bench_ret": float(latest[2]),
        "strategy_nav": float(strategy_curve.iloc[-1]),
        "bench_nav": float(bench_curve.iloc[-1]),
        "sharpe": sharpe,
    }


def build(cfg: AgentConfig) -> dict[str, Any]:
    holdings_path = _resolve_holdings_path(cfg)
    holdings_df = _read_holdings_csv(holdings_path)
    if holdings_df.empty:
        raise RuntimeError(f"No holdings found in {holdings_path}")
    new_row = holdings_df.iloc[0]
    # iloc[1] 是今天实际持有的（昨天选出），与今日收益对应；iloc[0] 是明天才持有的新仓
    today_row = holdings_df.iloc[1] if len(holdings_df) > 1 else new_row
    codes = [item.strip() for item in str(today_row["持仓股票"]).split(",") if item.strip()]
    names = [item.strip() for item in str(today_row["持仓股票名称"]).split(",") if item.strip()]
    market_data = _load_latest_market_data(cfg, codes)
    holdings = [
        HoldingRow(
            code=code,
            name=names[idx] if idx < len(names) else code,
            close=market_data.get(code, (None, None))[0],
            pct_chg=market_data.get(code, (None, None))[1],
        )
        for idx, code in enumerate(codes)
    ]
    stats = _load_trade_stats(cfg, fallback_trade_date=int(latest_row["换仓日"]))
    today = format_trade_date(stats["latest_trade_date"])
    trade_day_csv = cfg.project_root / "Data" / "Metadata" / "trade_day.csv"
    try:
        next_day = next_rebalance_date(today, cfg.rebalance_freq, trade_day_csv)
    except ValueError:
        if os.getenv("AGENT_DRY_RUN") == "1":
            next_day = today
        else:
            raise
    return {
        "strategy": cfg.strategy,
        "sample": str(today_row["样本"]),
        "rebalance_freq": cfg.rebalance_freq,
        "today": today,
        "holdings": holdings,
        "holdings_count": int(today_row["持仓数量"]),
        "latest_long": stats["latest_long"],
        "latest_bench_ret": stats["latest_bench_ret"],
        "excess_ret": stats["latest_long"] - stats["latest_bench_ret"],
        "strategy_nav": stats["strategy_nav"],
        "bench_nav": stats["bench_nav"],
        "strategy_total_ret": stats["strategy_nav"] - 1.0,
        "bench_total_ret": stats["bench_nav"] - 1.0,
        "sharpe": stats["sharpe"],
        "next_rebalance_date": next_day,
        "days_until_rebalance": (next_day - today).days,
        "holdings_path": str(holdings_path),
    }
