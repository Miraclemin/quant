from __future__ import annotations

import smtplib
from email.message import EmailMessage

from agent.config import EmailConfig
from agent.notifiers.base import Notifier


class EmailNotifier(Notifier):
    def __init__(self, cfg: EmailConfig) -> None:
        self.cfg = cfg

    def send(self, subject: str, body: str, is_alert: bool = False) -> None:
        if not self.cfg.enabled:
            return
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.cfg.from_addr
        message["To"] = ", ".join(self.cfg.to_addrs)
        message.set_content(body)
        if self.cfg.use_ssl:
            with smtplib.SMTP_SSL(self.cfg.smtp_host, self.cfg.smtp_port, timeout=20) as server:
                server.login(self.cfg.from_addr, self.cfg.from_password)
                server.send_message(message)
            return
        with smtplib.SMTP(self.cfg.smtp_host, self.cfg.smtp_port, timeout=20) as server:
            server.starttls()
            server.login(self.cfg.from_addr, self.cfg.from_password)
            server.send_message(message)
