from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from agent.config import ResendConfig, load_config
from agent.daily import main
from agent.formatters.alert_msg import build_message as build_alert_message
from agent.notifiers.resend import ResendNotifier
from agent.notifiers.telegram import truncate_utf8


def _write_config(tmp_path: Path) -> Path:
    agent_dir = tmp_path / "agent"
    agent_dir.mkdir()
    (agent_dir / ".env").write_text(
        "EMAIL_FROM=a@qq.com\nEMAIL_PASSWORD=pw\nEMAIL_TO=b@qq.com\nTG_BOT_TOKEN=t\nTG_CHAT_ID=1\n",
        encoding="utf-8",
    )
    payload = {
        "strategy": "spec_vol",
        "phase": "papertrade",
        "account_size_cny": 500000,
        "n_top": 5,
        "rebalance_freq": "月度",
        "sample": "全市场",
        "output_dir": "output2",
        "notify": {
            "channels": ["telegram"],
            "email": {
                "enabled": False,
                "smtp_host": "smtp.qq.com",
                "smtp_port": 465,
                "use_ssl": True,
                "from_addr": "${EMAIL_FROM}",
                "from_password": "${EMAIL_PASSWORD}",
                "to_addrs": ["${EMAIL_TO}"],
            },
            "telegram": {"enabled": True, "bot_token": "${TG_BOT_TOKEN}", "chat_id": "${TG_CHAT_ID}"},
        },
        "schedule": {"papertrade_time": "16:30", "livetrade_time": "09:00"},
        "data_update": {"auto_run_main": False, "skip_if_not_trading_day": False},
    }
    path = agent_dir / "config.yaml"
    path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    return path


def test_load_config_expands_env(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    cfg = load_config(config_path)
    assert cfg.notify.telegram.bot_token == "t"
    assert cfg.notify.email.to_addrs == ["b@qq.com"]


def test_telegram_truncate() -> None:
    text = "中" * 3000
    truncated = truncate_utf8(text, limit_bytes=200)
    assert len(truncated.encode("utf-8")) <= 200
    assert truncated.endswith("...(truncated)")


def test_alert_truncate() -> None:
    msg = build_alert_message("papertrade", RuntimeError("boom"), "x" * 5000)
    assert len(msg.body.encode("utf-8")) <= 2000


def test_resend_notifier_sends_correct_payload() -> None:
    cfg = ResendConfig(
        enabled=True,
        api_key="re_test_key",
        from_addr="quant@example.com",
        to_addrs=["me@example.com"],
    )
    notifier = ResendNotifier(cfg)
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    with patch("agent.notifiers.resend.requests.post", return_value=mock_resp) as mock_post:
        notifier.send("Test Subject", "Test body")
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    assert kwargs["json"]["to"] == ["me@example.com"]
    assert kwargs["json"]["from"] == "quant@example.com"
    assert kwargs["json"]["subject"] == "Test Subject"
    assert "re_test_key" in kwargs["headers"]["Authorization"]


def test_resend_notifier_skips_when_disabled() -> None:
    cfg = ResendConfig(enabled=False, api_key="re_x", from_addr="a@b.com", to_addrs=["c@d.com"])
    with patch("agent.notifiers.resend.requests.post") as mock_post:
        ResendNotifier(cfg).send("s", "b")
    mock_post.assert_not_called()


def test_load_config_with_resend(tmp_path: Path) -> None:
    agent_dir = tmp_path / "agent"
    agent_dir.mkdir()
    (agent_dir / ".env").write_text("RESEND_API_KEY=re_abc\nEMAIL_TO=you@example.com\n", encoding="utf-8")
    payload = {
        "strategy": "spec_vol",
        "phase": "papertrade",
        "account_size_cny": 500000,
        "n_top": 5,
        "rebalance_freq": "月度",
        "sample": "全市场",
        "output_dir": "output2",
        "notify": {
            "channels": ["resend"],
            "email": {"enabled": False, "smtp_host": "smtp.qq.com", "smtp_port": 465, "use_ssl": True, "from_addr": "", "from_password": "", "to_addrs": []},
            "telegram": {"enabled": False, "bot_token": "", "chat_id": ""},
            "resend": {"enabled": True, "api_key": "${RESEND_API_KEY}", "from_addr": "quant@example.com", "to_addrs": ["${EMAIL_TO}"]},
        },
        "schedule": {"papertrade_time": "16:30", "livetrade_time": "09:00"},
        "data_update": {"auto_run_main": False, "skip_if_not_trading_day": False},
    }
    path = agent_dir / "config.yaml"
    path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    cfg = load_config(path)
    assert cfg.notify.resend is not None
    assert cfg.notify.resend.api_key == "re_abc"
    assert cfg.notify.resend.to_addrs == ["you@example.com"]


def test_daily_smoke(monkeypatch) -> None:
    from agent import daily
    from agent.notifiers.base import FormattedMessage

    class DummyCfg:
        phase = "papertrade"
        strategy = "spec_vol"
        project_root = Path("/Users/wanghanming1/quant")
        agent_dir = Path("/Users/wanghanming1/quant/agent")
        data_update = type("DU", (), {"skip_if_not_trading_day": False, "auto_run_main": False})()

    sent: list[FormattedMessage] = []

    monkeypatch.setattr(daily, "load_config", lambda phase_override=None: DummyCfg())
    monkeypatch.setattr(daily.papertrade, "build", lambda cfg: {"strategy": "spec_vol", "sample": "全市场", "rebalance_freq": "月度", "today": date(2026, 4, 27), "holdings": [], "holdings_count": 0, "latest_long": 0.01, "latest_bench_ret": 0.0, "excess_ret": 0.01, "strategy_nav": 1.1, "bench_nav": 1.0, "strategy_total_ret": 0.1, "bench_total_ret": 0.0, "sharpe": 1.2, "next_rebalance_date": date(2026, 4, 30), "days_until_rebalance": 3})
    monkeypatch.setattr(daily, "notify_all", lambda cfg, message, is_alert=False: sent.append(message))
    assert main(["papertrade"]) == 0
    assert sent
