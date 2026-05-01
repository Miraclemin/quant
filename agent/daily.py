from __future__ import annotations

import sys
import traceback
from datetime import date
from pathlib import Path

from agent.config import AgentConfig, load_config
from agent.formatters.alert_msg import build_message as build_alert_message
from agent.formatters.livetrade_msg import build_message as build_livetrade_message
from agent.formatters.papertrade_msg import build_message as build_papertrade_message
from agent.notifiers import notify_all
from agent.runners import livetrade, papertrade
from agent.utils.strategy_runner import run_strategy_main
from agent.utils.trading_calendar import is_trading_day


def _journal_path(cfg: AgentConfig, today: date) -> Path:
    journal_dir = cfg.agent_dir / "journal"
    journal_dir.mkdir(parents=True, exist_ok=True)
    return journal_dir / f"{today:%Y-%m-%d}.md"


def _write_journal(cfg: AgentConfig, text: str) -> None:
    today = date.today()
    path = _journal_path(cfg, today)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(text.rstrip() + "\n")


def run_phase(cfg: AgentConfig) -> None:
    today = date.today()
    trade_day_csv = cfg.project_root / "Data" / "Metadata" / "trade_day.csv"
    if cfg.data_update.skip_if_not_trading_day and not is_trading_day(today, trade_day_csv):
        _write_journal(cfg, f"- {today}: non-trading day, skipped")
        return
    if cfg.data_update.auto_run_main:
        result = run_strategy_main(cfg.project_root, cfg.strategy)
        _write_journal(cfg, f"- strategy main ok: {result.args}")
    if cfg.phase == "papertrade":
        payload = papertrade.build(cfg)
        message = build_papertrade_message(payload)
    elif cfg.phase == "livetrade":
        payload = livetrade.build(cfg)
        message = build_livetrade_message(payload)
    else:
        raise ValueError(f"Unsupported phase: {cfg.phase}")
    notify_all(cfg, message)
    _write_journal(cfg, f"- {cfg.phase} notification sent: {message.subject}")


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    phase_override = args[0] if args else None
    try:
        cfg = load_config(phase_override=phase_override)
    except Exception:
        trace = traceback.format_exc()
        print(trace, file=sys.stderr)
        return 1
    try:
        run_phase(cfg)
        return 0
    except Exception as exc:
        trace = traceback.format_exc()
        alert = build_alert_message(cfg.phase, exc, trace)
        try:
            notify_all(cfg, alert, is_alert=True)
        finally:
            _write_journal(cfg, f"- {cfg.phase} failed: {exc}")
        print(trace, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
