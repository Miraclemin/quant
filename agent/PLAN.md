# quant/agent — v0 每日定时通知系统

**日期**:2026-04-27
**目标**:把现有 `quant` 项目变成"每天自动跑一次,把结果推到邮箱或 Telegram"。
**不做**:LLM 自动研究因子(那是 v1+ 的事)。

---

## 1. 范围

| 阶段 | 推送时机 | 推送内容 |
|------|---------|----------|
| `papertrade` | 每个交易日 **16:30**(收盘后 30 分钟,数据已更新) | 当日 PnL + 当前持仓 + 最近表现 + 是否换仓 |
| `livetrade` | 每个交易日 **09:00**(开盘前 30 分钟) | 今日是否换仓:不换 → "持仓不变";换仓 → 卖出/买入清单 + 股票代码 + 估算金额 |

**约束**:
- 单一策略(一个 agent 只盯一个 `factor_mining/{strategy}/`),多策略后续支持
- 月度调仓策略大多数日子是"无操作",这是预期行为
- 失败要发**告警通知**(数据没更新、跑挂了等),不能静默挂掉

---

## 2. 目录结构

```
quant/agent/
├── PLAN.md                # 本文
├── config.yaml            # 用户配置
├── config.example.yaml    # 模板
├── daily.py               # 主入口
├── runners/
│   ├── papertrade.py      # papertrade 流程
│   └── livetrade.py       # livetrade 流程
├── notifiers/
│   ├── email.py           # smtplib
│   ├── telegram.py        # requests + bot API
│   └── base.py            # 抽象基类
├── formatters/
│   ├── papertrade_msg.py  # papertrade 消息模板
│   └── livetrade_msg.py   # livetrade 消息模板
├── journal/
│   └── 2026-04-27.md      # 每日运行记录(自动写)
├── run.sh                 # cron / launchd 入口
├── launchd/
│   ├── quant.papertrade.plist
│   └── quant.livetrade.plist
└── tests/
    └── test_smoke.py
```

---

## 3. 配置文件 `config.yaml`

```yaml
# 用户改这一份就够了
strategy: spec_vol             # 对应 factor_mining/{strategy}/
phase: papertrade              # papertrade | livetrade

# 资金和仓位(livetrade 用,papertrade 也用于显示估算 PnL)
account_size_cny: 500000
n_top: 5

# 调仓频率(用于判断今天是否需要操作)
rebalance_freq: 月度           # 日度 | 周度 | 月度

# 样本池
sample: 全市场                  # 中证800 | 中证1000 | 全市场

# 通知通道
notify:
  channels: [telegram]         # 可填 [email] / [telegram] / [email, telegram]
  email:
    enabled: false
    smtp_host: smtp.qq.com
    smtp_port: 465
    use_ssl: true
    from_addr: "${EMAIL_FROM}"
    from_password: "${EMAIL_PASSWORD}"
    to_addrs: ["${EMAIL_TO}"]
  telegram:
    enabled: true
    bot_token: "${TG_BOT_TOKEN}"
    chat_id: "${TG_CHAT_ID}"

# 调度时间(launchd / cron 使用,代码内部不依赖)
schedule:
  papertrade_time: "16:30"
  livetrade_time: "09:00"

# 数据更新策略
data_update:
  auto_run_main: true          # papertrade 推送前先跑 main.py 更新数据 + 因子
  skip_if_not_trading_day: true
```

`${...}` 占位符从同目录 `.env` 读取。

---

## 4. 主流程 `daily.py`

```python
def main():
    cfg = load_config("config.yaml")

    if cfg.data_update.skip_if_not_trading_day and not is_trading_day(today):
        log_journal("非交易日,跳过")
        return

    try:
        if cfg.data_update.auto_run_main:
            run_strategy_main(cfg.strategy)   # 等价于在 factor_mining/{strategy}/ 跑 python main.py

        if cfg.phase == "papertrade":
            payload = runners.papertrade.build(cfg)
        elif cfg.phase == "livetrade":
            payload = runners.livetrade.build(cfg)

        message = formatters.format(cfg.phase, payload)
        notify_all(cfg.notify, message)
        log_journal_success(payload)

    except Exception as e:
        notify_all(cfg.notify, format_alert(e, traceback.format_exc()))
        log_journal_failure(e)
        sys.exit(1)
```

---

## 5. Papertrade 推送内容

```
📊 [Papertrade] spec_vol · 全市场 · 月度调仓
日期: 2026-04-27 (周一)

📌 当前持仓 (5 只)
  002561.SZ 徐家汇      ¥??? (+1.2%)
  600088.SH 中视传媒    ¥??? (-0.5%)
  ...

📈 当日表现
  策略 long 收益率:  +0.8%
  基准(沪深300):    +0.3%
  超额:             +0.5%

📅 累计净值(自策略启动)
  策略:   1.124  (+12.4%)
  基准:   1.063  (+6.3%)
  夏普:   1.32

🔄 换仓状态
  下一换仓日: 2026-05-30 (距今 33 天)
```

数据来源:
- 当前持仓:`trade_holdings.csv` 首行
- 当日表现:DuckDB 表 `{factor_table}_trade_daily_ret` 最新一行
- 累计净值:同表累乘
- 是否换仓日:用 `quant_infra` 现有交易日历 + `rebalance_freq` 计算

---

## 6. Livetrade 推送内容

**情况 A:今日非换仓日**
```
✅ [Livetrade] spec_vol · 2026-04-27
今日无需操作,持有以下 5 只:

002561.SZ 徐家汇
600088.SH 中视传媒
603107.SH 上海汽配
301370.SZ 国科恒泰
601599.SH 浙文影业

下一换仓日预估: 2026-04-30
```

**情况 B:今日是换仓日**
```
🔔 [Livetrade] spec_vol · 2026-04-27 ★换仓日★

✂️ 卖出 (3 只)
  600036.SH 招商银行  ~ ¥100,000
  000001.SZ 平安银行  ~ ¥100,000
  601318.SH 中国平安  ~ ¥100,000

🛒 买入 (3 只)
  002561.SZ 徐家汇    ~ ¥100,000
  600088.SH 中视传媒  ~ ¥100,000
  603107.SH 上海汽配  ~ ¥100,000

(每只 ¥100,000 = 50 万 / 5)
预估手续费: ¥150
```

换仓判定逻辑:对比 `trade_holdings.csv` 最新一行 vs 上一行,diff 出卖出/买入。

---

## 7. 通知器抽象

```python
class Notifier(ABC):
    @abstractmethod
    def send(self, subject: str, body: str, is_alert: bool = False): ...

class EmailNotifier(Notifier): ...   # smtplib
class TelegramNotifier(Notifier): ... # requests POST sendMessage

def notify_all(cfg, message):
    for ch in cfg.channels:
        get_notifier(ch, cfg).send(message.subject, message.body)
```

Telegram 优先(消息即时、有格式),Email 作为兜底/备份。

---

## 8. 调度方式 (macOS launchd)

`launchd/quant.papertrade.plist`:
```xml
<plist>
  <dict>
    <key>Label</key><string>com.user.quant.papertrade</string>
    <key>ProgramArguments</key>
    <array>
      <string>/Users/wanghanming1/quant/agent/run.sh</string>
      <string>papertrade</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
      <key>Hour</key><integer>16</integer>
      <key>Minute</key><integer>30</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/wanghanming1/quant/agent/journal/launchd.out.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/wanghanming1/quant/agent/journal/launchd.err.log</string>
  </dict>
</plist>
```

部署:
```bash
launchctl load ~/Library/LaunchAgents/com.user.quant.papertrade.plist
launchctl start com.user.quant.papertrade   # 立刻跑一次测试
```

`run.sh` 负责 cd + 激活 venv + 调 `python -m agent.daily $1`。

---

## 9. .env 模板 (`.env.example`)

```bash
# Telegram
TG_BOT_TOKEN=123456:abcdef
TG_CHAT_ID=12345678

# Email (可选)
EMAIL_FROM=you@qq.com
EMAIL_PASSWORD=xxxxx           # QQ 邮箱授权码,不是登录密码
EMAIL_TO=you@qq.com

# Tushare (现有项目已有)
TS_TOKEN=xxx
```

---

## 10. 实现优先级 / 任务拆解

| # | 任务 | 估时 |
|---|------|------|
| 1 | `config.yaml` schema + loader(支持 `.env` 占位符) | 30 min |
| 2 | `notifiers/telegram.py` + `notifiers/email.py` | 1 h |
| 3 | `runners/papertrade.py`(读 trade_holdings + DuckDB 净值表) | 1.5 h |
| 4 | `runners/livetrade.py`(diff 持仓变化) | 1 h |
| 5 | `formatters/*` 消息模板 | 1 h |
| 6 | `daily.py` 主入口 + 异常告警 + journal | 1 h |
| 7 | `run.sh` + launchd plist + 部署文档 | 30 min |
| 8 | 烟雾测试:手动跑 papertrade,Telegram 收到消息 | 30 min |

总计 **约 1 个工作日**。

---

## 11. 不做的事(明确边界,防止滚雪球)

- ❌ LLM / 自动研究 / Scout / Meta — v1+
- ❌ 多策略并行 — 先单策略,稳定后再扩(配置改为 `strategies: [...]` 即可)
- ❌ 自动下单 / QMT 接入 — 永远 Telegram 推清单 + 人工下单
- ❌ Web Dashboard / Streamlit — 终端 + 日志足够
- ❌ 复杂的回测 / 风控 — 现有 `quant_infra` 已经做了
- ❌ Docker / sandbox — 自己跑自己的代码,不需要

---

## 12. 验收标准

1. 写好 `config.yaml`,把 `phase` 设为 `papertrade`,16:30 收到一条 Telegram 消息,内容包含当前持仓 + 当日 PnL。
2. 把 `phase` 改成 `livetrade`,09:00 收到消息;非换仓日提示"无需操作",换仓日给出明确买卖清单。
3. 故意把 Tushare TOKEN 改错,跑一次,应该收到一条**告警消息**,不是静默失败。
4. 连续 3 个交易日自动触发,无需人工干预。

---

## 13. v1 演进方向(本期不做,但留口)

- 加 `strategies: [spec_vol, week_factor, ...]` 支持多策略并发推送
- 加 LLM agent 在每周日跑一次:扫论文/研报 → 提议新因子 → 自动生成 `factor_mining/auto_xxx/main.py` → 跑回测 → 战胜 baseline 留下
- 加 Streamlit dashboard 看历史 PnL
- 加实盘成交回填(`/done` Telegram 命令),计算实盘 vs paper divergence
