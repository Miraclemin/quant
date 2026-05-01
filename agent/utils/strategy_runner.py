from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run_strategy_main(project_root: Path, strategy: str, timeout_seconds: int = 1800) -> subprocess.CompletedProcess[str]:
    strategy_dir = project_root / "factor_mining" / strategy
    main_path = strategy_dir / "main.py"
    if not main_path.exists():
        raise FileNotFoundError(f"Strategy main.py not found: {main_path}")
    env = os.environ.copy()
    src_path = str(project_root / "src")
    env["PYTHONPATH"] = src_path if not env.get("PYTHONPATH") else f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
    return subprocess.run(
        [sys.executable, "main.py"],
        cwd=str(strategy_dir),
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
        check=True,
    )
