# agent

`agent/` 提供单策略的定时通知骨架，支持两种入口：

- `python -m agent.daily papertrade`
- `python -m agent.daily livetrade`

先在 [config.example.yaml](/Users/wanghanming1/quant/agent/config.example.yaml) 基础上创建 `agent/config.yaml`，再按 [.env.example](/Users/wanghanming1/quant/agent/.env.example) 创建 `agent/.env`。`config.yaml` 中的 `${VAR}` 会递归从 `.env` 和当前环境变量展开。

`papertrade` 会读取 `factor_mining/{strategy}/{output2|output}/trade_holdings.csv` 首行和 DuckDB `{strategy}_trade_daily_ret` 最新数据，生成当日表现与累计净值消息。`livetrade` 会对比前两行持仓，若首行 `换仓日` 等于当天则输出买卖差异，否则提示持仓不变。

通知通道支持 Telegram 和 QQ 邮箱。设置 `AGENT_DRY_RUN=1` 时不会真实发送，而是把消息打印到 stdout，便于本地验收。

`run.sh` 会切到项目根目录并在存在 `.venv` 时自动激活，然后执行 `python -m agent.daily $phase`。

`launchd` 部署：

1. 复制 [com.user.quant.papertrade.plist](/Users/wanghanming1/quant/agent/launchd/com.user.quant.papertrade.plist) 和 [com.user.quant.livetrade.plist](/Users/wanghanming1/quant/agent/launchd/com.user.quant.livetrade.plist) 到 `~/Library/LaunchAgents/`
2. 执行 `launchctl load ~/Library/LaunchAgents/com.user.quant.papertrade.plist`
3. 执行 `launchctl load ~/Library/LaunchAgents/com.user.quant.livetrade.plist`
4. 用 `launchctl start com.user.quant.papertrade` 或 `launchctl start com.user.quant.livetrade` 手动触发测试

测试：

```bash
pytest agent/tests/ -v
```
