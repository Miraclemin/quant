# 变更分析报告

> 生成时间：2026-05-01
> 当前分支：`proof-report-rolling-spec-vol`
> 对比基准：`origin/main`

---

## 一、变更概览

| 类别 | 文件 / 目录 | 状态 |
|------|-------------|------|
| 已提交 | `src/quant_infra/factor_calc.py` | 重构核心因子计算逻辑 |
| 已提交 | `src/quant_infra/const.py` | 新增窗口常量 |
| 已提交 | `src/quant_infra/get_data.py` | 改进 token 加载 & 并行数控制 |
| 已提交 | `pyproject.toml` | 新增 `scipy`、`python-dotenv` 依赖 |
| 已提交 | `README.md` / `.env.example` | 文档更新 |
| 未暂存 | `pyproject.toml` | 新增 `PyYAML`、`requests` 依赖 |
| 未追踪 | `agent/` | 全新定时通知 Agent 模块 |
| 未追踪 | `proof/` | 滚动残差证明脚本与报告 |
| 未追踪 | `docs/` | 设计文档 |

---

## 二、核心功能变更详解

### 2.1 滚动残差回归（最大变更）

**文件**：`src/quant_infra/factor_calc.py`

**旧实现**：
- `calc_single_beta()`：对每只股票用全部历史数据做一次静态 OLS，生成固定 beta 写入 `stock_betas` 表。
- `calc_single_resid()`：用静态 beta 乘以因子得到拟合值，计算残差。
- 缺陷：beta 不随时间更新，在市场风格切换时会显著失真。

**新实现**：
- 删除 `calc_single_beta()` 和 `stock_betas` 表依赖。
- 新增 `calc_single_resid_rolling(code, stock_df, reg_window=RESID_REG_WINDOW)`：
  - 对每个交易日 `t`，取最近 `reg_window`（默认 252）个交易日滑窗回归四因子模型。
  - 仅保留第 `t` 天的残差，beta 不落库，每日自动更新。
  - 查询时自动向前多取 `reg_window * 1.6` 个自然日作为暖启动缓冲。
  - 结果写入 `stock_resids` 时过滤掉缓冲期数据，只保留待计算日期区间。

**关键常量**（`src/quant_infra/const.py`）：
```python
RESID_REG_WINDOW = 252   # 滚动回归窗口（交易日数）
SPEC_VOL_WINDOW  = 20    # 特质波动率滚动窗口
DATA_FETCH_JOBS  = 4     # 数据下载并行进程数
```

### 2.2 环境变量改进

**文件**：`src/quant_infra/get_data.py`

- 新增 `from dotenv import load_dotenv`，自动从项目根目录 `.env` 文件加载 `TS_TOKEN`，无需手动 `export`。
- 数据并行下载由 `n_jobs=-1`（全核心）改为 `n_jobs=DATA_FETCH_JOBS`（=4），避免触发 Tushare API 限频。

### 2.3 新增依赖

**`pyproject.toml`**（已提交 + 未暂存合并）：

| 包 | 版本 | 用途 |
|----|------|------|
| `scipy` | ≥1.16.3 | 统计计算 |
| `python-dotenv` | ≥1.1.1 | `.env` 文件加载 |
| `PyYAML` | ≥6.0.2 | agent 配置解析 |
| `requests` | ≥2.32.3 | agent 通知 HTTP 请求 |

### 2.4 全新 `agent/` 模块

定时通知骨架，支持模拟盘（papertrade）和实盘（livetrade）两种入口：

| 子目录 | 功能 |
|--------|------|
| `runners/` | `papertrade.py` 读取持仓 CSV + DuckDB 净值；`livetrade.py` 对比换仓差异 |
| `notifiers/` | Telegram Bot / QQ 邮箱发送器 |
| `formatters/` | 消息模板格式化 |
| `utils/` | 交易日历、策略运行工具 |
| `launchd/` | macOS 定时任务 plist 配置 |
| `config.py` | 递归展开 `${VAR}` 的 YAML 配置加载器 |

运行方式：
```bash
python -m agent.daily papertrade
python -m agent.daily livetrade
```

### 2.5 `proof/` 验证模块

`proof/run_proof.py` 通过三组实验量化证明滚动回归优于静态回归：

- **实验1**：滚动残差的因子暴露（R²、L1 系数范数）显著低于静态残差，说明因子剥离更干净。
- **实验2**：验证股票 beta 随时间漂移（MKT beta 标准差均值 0.22，HML 高达 0.46），证明固定 beta 存在模型误设。
- **实验3**：分子区间（2017-2019 / 2020-2022 / 2023-2026）稳健性检验。

---

## 三、验证方法

### 3.1 环境准备

```bash
# 安装依赖
pip install -e ".[dev]"

# 配置 Tushare token
cp .env.example .env
# 编辑 .env，填入 TS_TOKEN=<你的token>
```

### 3.2 验证滚动残差计算

```bash
# 验证常量导入正常
python -c "from quant_infra.const import RESID_REG_WINDOW, SPEC_VOL_WINDOW; print(RESID_REG_WINDOW, SPEC_VOL_WINDOW)"
# 期望输出：252 20

# 验证 .env 加载（需要有效 token）
python -c "from quant_infra.get_data import token; print('token ok' if token else 'token missing')"

# 运行残差计算（需要 Data/data.db 已有数据）
python -c "from quant_infra.factor_calc import calc_resid; calc_resid()"
```

### 3.3 验证 agent 模块

```bash
# 单元测试（不需要真实数据）
pytest agent/tests/ -v

# 干跑模拟盘通知（不发送真实消息）
AGENT_DRY_RUN=1 python -m agent.daily papertrade

# 干跑实盘通知
AGENT_DRY_RUN=1 python -m agent.daily livetrade
```

### 3.4 运行 proof 验证报告

```bash
# 需要 Data/data.db 已有 stock_resids 数据
python proof/run_proof.py
# 输出写入 proof/output/analysis_report.md 和 proof/output/figures/
```

### 3.5 查看已有 proof 结果

已有验证结论（`proof/output/analysis_report.md`）：

| 指标 | 滚动方法 | 静态方法 |
|------|---------|---------|
| 中位数 R² | **0.004** | 0.016 |
| 均值 R² | **0.008** | 0.024 |
| 因子系数 L1 范数 | **0.302** | 0.603 |

> 滚动方法在所有分子区间均优于静态方法，因子暴露残余量减少约 50%。

---

## 四、风险与注意事项

1. **计算成本**：滚动回归时间复杂度为 O(N × T × reg_window)，全市场 5000 股 × 10 年数据首次运行耗时较长，建议分批或增量运行。
2. **数据库兼容性**：`stock_betas` 表不再写入，历史残数据若依赖该表需清理或迁移。
3. **`factor_mining` 子模块**：当前有未提交修改，本报告未涵盖，需单独确认。
4. **agent 配置**：首次使用需复制 `agent/config.example.yaml` 为 `agent/config.yaml` 并填入通知渠道信息。
