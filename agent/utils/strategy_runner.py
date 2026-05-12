from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run_strategy_main(project_root: Path, strategy: str, timeout_seconds: int = 1800) -> subprocess.CompletedProcess[str]:
    strategy_dir = project_root / "factor_mining" / strategy
    run_daily_path = strategy_dir / "run_daily.py"
    script = "run_daily.py" if run_daily_path.exists() else "main.py"
    main_path = strategy_dir / script
    if not main_path.exists():
        raise FileNotFoundError(f"Strategy runner not found: {main_path}")
    env = os.environ.copy()
    src_path = str(project_root / "src")
    env["PYTHONPATH"] = src_path if not env.get("PYTHONPATH") else f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
    return subprocess.run(
        [sys.executable, script],
        cwd=str(strategy_dir),
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
        check=True,
    )
