from __future__ import annotations

import os

from agent.config import AgentConfig
from agent.notifiers.base import FormattedMessage
from agent.notifiers.email import EmailNotifier
from agent.notifiers.resend import ResendNotifier
from agent.notifiers.telegram import TelegramNotifier


def notify_all(cfg: AgentConfig, message: FormattedMessage, is_alert: bool = False) -> None:
    if os.getenv("AGENT_DRY_RUN") == "1":
        print(f"[DRY-RUN] {message.subject}\n{message.body}")
        return
    for channel in cfg.notify.channels:
        notifier = get_notifier(channel, cfg)
        notifier.send(message.subject, message.body, is_alert=is_alert)


def get_notifier(channel: str, cfg: AgentConfig):
    if channel == "telegram":
        return TelegramNotifier(cfg.notify.telegram)
    if channel == "email":
        return EmailNotifier(cfg.notify.email)
    if channel == "resend":
        if cfg.notify.resend is None:
            raise ValueError("resend channel enabled but [notify.resend] not configured")
        return ResendNotifier(cfg.notify.resend)
    raise ValueError(f"Unsupported notifier channel: {channel}")
