"""
后台常驻调度器，替代 macOS launchd。
用法：
    nohup python -m agent.scheduler &           # 后台运行
    python -m agent.scheduler                   # 前台运行（调试）
    kill $(cat /tmp/quant_scheduler.pid)        # 停止
"""
from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

import schedule

from agent.config import load_config
from agent.daily import run_phase


def _job(phase: str) -> None:
    try:
        cfg = load_config(phase_override=phase)
        run_phase(cfg)
    except Exception as exc:
        print(f"[scheduler] {phase} 执行失败: {exc}", file=sys.stderr)


def _setup_schedule(papertrade_time: str, livetrade_time: str) -> None:
    schedule.every().monday.at(papertrade_time).do(_job, phase="papertrade")
    schedule.every().tuesday.at(papertrade_time).do(_job, phase="papertrade")
    schedule.every().wednesday.at(papertrade_time).do(_job, phase="papertrade")
    schedule.every().thursday.at(papertrade_time).do(_job, phase="papertrade")
    schedule.every().friday.at(papertrade_time).do(_job, phase="papertrade")

    schedule.every().monday.at(livetrade_time).do(_job, phase="livetrade")
    schedule.every().tuesday.at(livetrade_time).do(_job, phase="livetrade")
    schedule.every().wednesday.at(livetrade_time).do(_job, phase="livetrade")
    schedule.every().thursday.at(livetrade_time).do(_job, phase="livetrade")
    schedule.every().friday.at(livetrade_time).do(_job, phase="livetrade")


def main() -> None:
    pid_file = Path("/tmp/quant_scheduler.pid")
    pid_file.write_text(str(os.getpid()))

    def _on_exit(signum, frame):
        pid_file.unlink(missing_ok=True)
        print("[scheduler] 退出")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _on_exit)
    signal.signal(signal.SIGINT, _on_exit)

    cfg = load_config()
    papertrade_time = cfg.schedule.papertrade_time  # "16:30"
    livetrade_time = cfg.schedule.livetrade_time    # "09:00"

    _setup_schedule(papertrade_time, livetrade_time)
    print(f"[scheduler] 启动 PID={os.getpid()}")
    print(f"  papertrade: 周一至周五 {papertrade_time}")
    print(f"  livetrade:  周一至周五 {livetrade_time}")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
