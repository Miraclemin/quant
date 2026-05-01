from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(slots=True)
class FormattedMessage:
    subject: str
    body: str


class Notifier(ABC):
    @abstractmethod
    def send(self, subject: str, body: str, is_alert: bool = False) -> None:
        raise NotImplementedError
