from __future__ import annotations

import requests

from agent.config import ResendConfig
from agent.notifiers.base import Notifier

_RESEND_URL = "https://api.resend.com/emails"


class ResendNotifier(Notifier):
    def __init__(self, cfg: ResendConfig) -> None:
        self.cfg = cfg

    def send(self, subject: str, body: str, is_alert: bool = False) -> None:
        if not self.cfg.enabled:
            return
        resp = requests.post(
            _RESEND_URL,
            headers={"Authorization": f"Bearer {self.cfg.api_key}"},
            json={
                "from": self.cfg.from_addr,
                "to": self.cfg.to_addrs,
                "subject": subject,
                "text": body,
            },
            timeout=20,
        )
        if not resp.ok:
            raise requests.HTTPError(
                f"{resp.status_code} {resp.reason}: {resp.text}", response=resp
            )
