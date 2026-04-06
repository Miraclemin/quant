# DuckDB 数据库使用指南

## 概述

本项目使用 DuckDB 数据库替代 CSV 文件读写，提升数据查询和分析性能。

- **数据路径**: `.\Data\data.db`
- **脚本文件地址**：`.\src\quant_infra\db_utils.py`
- **支持操作**: 查询、写入（覆盖/追加）

## API参考


### 1. read_sql(query) - 执行 SQL 查询

```python
from src.quant_infra.db_utils import read_sql

# 简单查询
df = read_sql("SELECT * FROM stock_bar WHERE trade_date >= '20240101'")

# 复杂聚合查询
df = read_sql("""
    SELECT 
        ts_code,
        trade_date,
        AVG(close) as avg_close,
        AVG(pct_chg) as avg_return
    FROM stock_bar
    WHERE trade_date >= '20240101'
    GROUP BY ts_code, trade_date
    ORDER BY trade_date, avg_return DESC
""")
```

**参数**:
- `query`: SQL 查询语句

**返回**: `pandas.DataFrame`

---

### 2. write_to_db(df, table_name, save_mode='replace') - 写入数据

```python
from src.quant_infra.db_utils import write_to_db
import pandas as pd

# 准备数据(通常是tushare下载的df数据)
df = pd.DataFrame({
    'ts_code': ['000001.SZ', '600000.SH'],
    'trade_date': ['20240101', '20240101'],
    'close': [10.5, 15.2]
})

# 覆盖写入
write_to_db(df, 'stock_bar', save_mode='replace')

# 追加写入
write_to_db(df, 'stock_bar', save_mode='append')
```

**参数**:
- `df`: pandas DataFrame
- `table_name`: 表名
- `save_mode`: `'replace'` (覆盖) 或 `'append'` (追加)

---



## 性能优化建议

### ✅ 好的做法

**1. 使用日期过滤**
```python
df = read_sql("SELECT * FROM stock_bar WHERE trade_date >= '20240101'")
```

**2. 只选择需要的列**
```python
df = read_sql("""
    SELECT ts_code, trade_date, close, pct_chg
    FROM stock_bar
    WHERE trade_date >= '20240101'
""")
```

**3. 在数据库中完成聚合**
```python
df = read_sql("""
    SELECT ts_code, AVG(pct_chg) as avg_return
    FROM stock_bar
    GROUP BY ts_code
""")
```

### ❌ 不好的做法

```python
# 查询全部数据再筛选
df = read_sql("SELECT * FROM stock_bar")
df = df[df['trade_date'] >= '20240101']

# SELECT * 查询所有列
df = read_sql("SELECT * FROM stock_bar WHERE trade_date >= '20240101'")

# Python 层面聚合
df = read_sql("SELECT * FROM stock_bar WHERE trade_date >= '20240101'")
df = df.groupby('ts_code')['pct_chg'].mean()
```

---

## 常见问题

### Q1: 为什么选择 DuckDB？

**A**: 
- **列式存储**: 分析查询比 SQLite 快 10-100 倍
- **嵌入式**: 无需服务器，开箱即用
- **OLAP 优化**: 专为数据分析设计
- **内存友好**: 大数据量时比 Pandas 更高效

### Q2: 数据库会自动更新吗？

**A**: 是的，调用数据获取函数时会自动检测已有数据，只下载新的交易日数据。


### Q3: 支持哪些 SQL 功能？

**A**: DuckDB 支持完整的 SQL 标准：
- 窗口函数（OVER, PARTITION BY）
- 通用表表达式（WITH ... AS）
- 子查询、连接（JOIN）
- 聚合函数（AVG, SUM, COUNT 等）
- 字符串函数、日期函数
- CASE WHEN 条件表达式

更多参考：[DuckDB SQL 文档](https://duckdb.org/docs/sql/introduction)

---

## 数据库表结构（建议使用DBeaver查看，更直观）

### stock_bar - 股票日线数据
| 字段 | 说明 |
|------|------|
| ts_code | 股票代码 |
| trade_date | 交易日期 |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| close | 收盘价 |
| pct_chg | 涨跌幅 |

### daily_basic - 每日基本信息
| 字段 | 说明 |
|------|------|
| ts_code | 股票代码 |
| trade_date | 交易日期 |
| total_mv | 总市值 |
| pb | 市净率 |
| pe | 市盈率 |
| turnover_rate | 换手率 |

### index_data - 指数数据
| 字段 | 说明 |
|------|------|
| ts_code | 指数代码 |
| trade_date | 交易日期 |
| close | 收盘点位 |
| pct_chg | 涨跌幅 |

---

## 故障排查

### 问题 1: ModuleNotFoundError: No module named 'duckdb'

**解决**:
```bash
pip install duckdb
```

### 问题 2: 数据库文件被占用

**解决**:
1. 在DBeaver中断开数据库连接
2. 重新运行数据获取脚本

### 问题 3: 查询速度慢

**可能原因**:
- 没有使用日期过滤
- SELECT * 查询所有列
- 在 Python 中做聚合而不是在 SQL 中
- 多进程访问数据库时，没有把数据先放在内存

**解决**: 参考[性能优化建议](#性能优化建议)

---

## 学习资源

- [DuckDB 官方网站](https://duckdb.org/)
- [DuckDB SQL 参考](https://duckdb.org/docs/sql/introduction)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)

---

**最后更新**: 2024-03-19
