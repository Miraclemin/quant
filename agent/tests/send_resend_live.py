"""
手动冒烟测试 - 真实调用 Resend API 发一封测试邮件
用法：
    cd /Users/wanghanming1/quant
    python agent/tests/send_resend_live.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# 让 agent 包可以被找到
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent.config import ResendConfig, _parse_env_file
from agent.notifiers.resend import ResendNotifier

ROOT = Path(__file__).resolve().parents[2]
AGENT_DIR = ROOT / "agent"

root_env_path = ROOT / ".env"
agent_env_path = AGENT_DIR / ".env"
print(f"[debug] root  .env: {root_env_path} exists={root_env_path.exists()}")
print(f"[debug] agent .env: {agent_env_path} exists={agent_env_path.exists()}")

env = {**_parse_env_file(root_env_path), **_parse_env_file(agent_env_path)}
print(f"[debug] EMAIL_TO={env.get('EMAIL_TO', '(未找到)')}")
print(f"[debug] RESEND_API_KEY={'(已设置)' if env.get('RESEND_API_KEY') else '(未找到)'}")

api_key = env.get("RESEND_API_KEY", "")
email_to = env.get("EMAIL_TO", "")
from_addr = env.get("RESEND_FROM", "onboarding@resend.dev")  # 没配域名时用官方测试地址

if not api_key:
    print("ERROR: RESEND_API_KEY 未在 .env 中配置")
    sys.exit(1)
if not email_to:
    print("ERROR: EMAIL_TO 未在 .env 中配置")
    sys.exit(1)

cfg = ResendConfig(
    enabled=True,
    api_key=api_key,
    from_addr=from_addr,
    to_addrs=[email_to],
)

print(f"发送测试邮件...")
print(f"  from : {from_addr}")
print(f"  to   : {email_to}")

ResendNotifier(cfg).send(
    subject="[quant-agent] Resend 连通性测试",
    body="这封邮件由 send_resend_live.py 发出，说明 Resend 配置正确。",
)

print("发送成功 ✓")
