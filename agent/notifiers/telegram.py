from __future__ import annotations

from agent.config import TelegramConfig
from agent.notifiers.base import Notifier

TRUNCATE_SUFFIX = "\n...(truncated)"
MAX_BYTES = 4000


def truncate_utf8(text: str, limit_bytes: int = MAX_BYTES, suffix: str = TRUNCATE_SUFFIX) -> str:
    raw = text.encode("utf-8")
    if len(raw) <= limit_bytes:
        return text
    budget = limit_bytes - len(suffix.encode("utf-8"))
    clipped = raw[:budget]
    while True:
        try:
            return clipped.decode("utf-8") + suffix
        except UnicodeDecodeError:
            clipped = clipped[:-1]


class TelegramNotifier(Notifier):
    def __init__(self, cfg: TelegramConfig) -> None:
        self.cfg = cfg

    def send(self, subject: str, body: str, is_alert: bool = False) -> None:
        import requests

        if not self.cfg.enabled:
            return
        url = f"https://api.telegram.org/bot{self.cfg.bot_token}/sendMessage"
        text = truncate_utf8(f"*{subject}*\n\n{body}")
        response = requests.post(
            url,
            json={
                "chat_id": self.cfg.chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        response.raise_for_status()
