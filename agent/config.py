from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

VALID_PHASES = {"papertrade", "livetrade"}
VALID_FREQS = {"日度", "周度", "月度"}


@dataclass(slots=True)
class EmailConfig:
    enabled: bool
    smtp_host: str
    smtp_port: int
    use_ssl: bool
    from_addr: str
    from_password: str
    to_addrs: list[str]


@dataclass(slots=True)
class ResendConfig:
    enabled: bool
    api_key: str
    from_addr: str
    to_addrs: list[str]


@dataclass(slots=True)
class TelegramConfig:
    enabled: bool
    bot_token: str
    chat_id: str


@dataclass(slots=True)
class NotifyConfig:
    channels: list[str]
    email: EmailConfig
    telegram: TelegramConfig
    resend: ResendConfig | None = None

    def __post_init__(self) -> None:
        allowed = {"email", "telegram", "resend"}
        unknown = set(self.channels) - allowed
        if unknown:
            raise ValueError(f"Unsupported notify channels: {sorted(unknown)}")


@dataclass(slots=True)
class ScheduleConfig:
    papertrade_time: str
    livetrade_time: str


@dataclass(slots=True)
class DataUpdateConfig:
    auto_run_main: bool
    skip_if_not_trading_day: bool


@dataclass(slots=True)
class AgentConfig:
    strategy: str
    phase: str
    account_size_cny: float
    n_top: int
    rebalance_freq: str
    sample: str
    output_dir: str
    notify: NotifyConfig
    schedule: ScheduleConfig
    data_update: DataUpdateConfig
    project_root: Path
    agent_dir: Path
    db_path: Path

    def __post_init__(self) -> None:
        if self.phase not in VALID_PHASES:
            raise ValueError(f"phase must be one of {sorted(VALID_PHASES)}")
        if self.rebalance_freq not in VALID_FREQS:
            raise ValueError(f"rebalance_freq must be one of {sorted(VALID_FREQS)}")
        if self.n_top <= 0:
            raise ValueError("n_top must be > 0")
        if self.account_size_cny <= 0:
            raise ValueError("account_size_cny must be > 0")

    @property
    def strategy_dir(self) -> Path:
        return self.project_root / "factor_mining" / self.strategy

    @property
    def output_candidates(self) -> list[Path]:
        names = [self.output_dir]
        if self.output_dir == "output2":
            names.append("output")
        elif self.output_dir == "output":
            names.append("output2")
        else:
            names.extend(["output2", "output"])
        seen: list[Path] = []
        for name in names:
            candidate = self.strategy_dir / name
            if candidate not in seen:
                seen.append(candidate)
        return seen


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _expand_value(value: Any, env: dict[str, str]) -> Any:
    if isinstance(value, str):
        for _ in range(10):
            updated = _VAR_RE.sub(lambda m: env.get(m.group(1), os.getenv(m.group(1), m.group(0))), value)
            if updated == value:
                break
            value = updated
        return value
    if isinstance(value, list):
        return [_expand_value(item, env) for item in value]
    if isinstance(value, dict):
        return {key: _expand_value(item, env) for key, item in value.items()}
    return value


def _build_config(data: dict[str, Any], agent_dir: Path) -> AgentConfig:
    notify_raw = data["notify"]
    project_root = agent_dir.parent
    resend_raw = notify_raw.get("resend")
    return AgentConfig(
        strategy=str(data["strategy"]),
        phase=str(data["phase"]),
        account_size_cny=float(data["account_size_cny"]),
        n_top=int(data["n_top"]),
        rebalance_freq=str(data["rebalance_freq"]),
        sample=str(data["sample"]),
        output_dir=str(data.get("output_dir", "output2")),
        notify=NotifyConfig(
            channels=[str(item) for item in notify_raw["channels"]],
            email=EmailConfig(**notify_raw["email"]),
            telegram=TelegramConfig(**notify_raw["telegram"]),
            resend=ResendConfig(**resend_raw) if resend_raw else None,
        ),
        schedule=ScheduleConfig(**data["schedule"]),
        data_update=DataUpdateConfig(**data["data_update"]),
        project_root=project_root,
        agent_dir=agent_dir,
        db_path=project_root / "Data" / "data.db",
    )


def load_config(config_path: str | Path | None = None, phase_override: str | None = None) -> AgentConfig:
    default_agent_dir = Path(__file__).resolve().parent
    resolved = Path(config_path) if config_path else default_agent_dir / "config.yaml"
    if not resolved.exists():
        raise FileNotFoundError(f"Config not found: {resolved}")
    agent_dir = resolved.parent
    project_root = agent_dir.parent
    env = {**_parse_env_file(project_root / ".env"), **_parse_env_file(agent_dir / ".env")}
    raw = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    raw = _expand_value(raw, env)
    if phase_override:
        raw["phase"] = phase_override
    return _build_config(raw, agent_dir)
