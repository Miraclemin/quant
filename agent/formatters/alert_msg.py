from __future__ import annotations

from agent.notifiers.base import FormattedMessage

MAX_ALERT_BYTES = 2000
TRUNCATED = "\n...(truncated)"


def _truncate(text: str) -> str:
    raw = text.encode("utf-8")
    if len(raw) <= MAX_ALERT_BYTES:
        return text
    budget = MAX_ALERT_BYTES - len(TRUNCATED.encode("utf-8"))
    clipped = raw[:budget]
    while True:
        try:
            return clipped.decode("utf-8") + TRUNCATED
        except UnicodeDecodeError:
            clipped = clipped[:-1]


def build_message(phase: str, exc: Exception, trace: str) -> FormattedMessage:
    subject = f"[ALERT] {phase} failed"
    body = _truncate(f"{type(exc).__name__}: {exc}\n\n{trace}")
    return FormattedMessage(subject=subject, body=body)
