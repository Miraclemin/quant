# Python 与 SQL 简明教程 - 量化实战版

## 目录
- [Python 核心特性](#python-核心特性)
- [SQL 核心特性 (DuckDB)](#sql-核心特性-duckdb)

---

## Python 核心特性

### 1. 图灵完备基础

#### 1.1 变量与数据类型
```python
# 基本数据类型
name = '中证 800'          # 字符串
index_code = '000906.SH'  # 字符串
threshold = 0.05          # 浮点数
count = 100               # 整数
is_valid = True           # 布尔值

# 容器类型
stocks = ['000001.SZ', '000002.SZ']  # 列表
index_map = {'沪深 300': '000300.SH'} # 字典
unique_codes = {'000001.SZ', '000002.SZ'}  # 集合
```

#### 1.2 控制流
```python
# 条件判断
if table_exists:
    if save_mode == 'replace':
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    else:
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
else:
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")

# 循环遍历
for date in tqdm(dates_to_download, desc="下载进度"):
    result = fetch_data(date)
    
```

#### 1.3 函数定义
```python
TUSHARE_TOKEN = os.getenv('TB_TOKEN')
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()
BASIC_INFO_PATH = 'Data/Metadata'
def get_trade(start_date, end_date):
    """获取交易日历"""
    df = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    df.to_csv(f'{BASIC_INFO_PATH}/trade_day.csv', index=False)
    return df
```

#### 1.4 异常处理（防止错误中断函数）
```python
try:
    df = pro.daily(trade_date=date)
    time.sleep(0.15)  # 避免频率限制
    return (date, df)
except Exception as e:
    print(f"获取数据失败：{e}")
    return (date, None)
finally:
    conn.close()  # 确保资源释放
```

### 2. 模块导入
```python
# 导入标准库
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# 导入第三方库
import tushare as ts
import duckdb
from joblib import Parallel, delayed
from tqdm import tqdm

# 导入本地模块
from quant_infra import db_utils
from quant_infra.const import INDEX_NAME_TO_CODE, FREQ_MAP
```


### 3. Pandas 数据处理

#### 3.1 DataFrame 基础操作
```python
# 创建 DataFrame
df = pd.DataFrame({'con_code': ins_codes})

# 读取 CSV
trade_dates = pd.read_csv('Data/Metadata/trade_day.csv')

# 保存 CSV
df.to_csv(f'{BASIC_INFO_PATH}/{index_code}_ins.csv', index=False)

# 选择列，返回一维序列
codes = ins['con_code'].unique()

# 条件筛选
filtered = trade_dates[trade_dates['cal_date'].astype(str) > max_date]

# 排序
new_data.sort_values(
    by=['trade_date', 'ts_code'],
    ascending=False,
    inplace=True
)
```

#### 3.2 数据聚合
```python
# 分组聚合
stk_p = (stk.dropna(subset=[period_col])
        .groupby(["ts_code", period_col], as_index=False)
        .agg(**{
            ret_col: ("ret", lambda x: (1 + x).prod() - 1),
            "trade_date": ("trade_date", "last")
        }))

# 使用 apply 进行组内操作
ic_series = df.groupby('trade_date').apply(
    lambda x: x[fac_col].corr(x[next_ret_col], method='spearman'),
    include_groups=False
)
```

#### 3.3 数据合并
```python
# 合并两个 DataFrame
df = (
    fac_p[["ts_code", "trade_date", period_col, fac_col]]
       .merge(stk_p[["ts_code", "trade_date", next_ret_col]], 
              on=["ts_code", "trade_date"], 
              how="inner")
       .dropna(subset=[fac_col, next_ret_col])
)
```

#### 3.4 时间序列处理
```python
# 日期计算
today = datetime.now()
latest_date = today.date() - timedelta(days=1)

# 格式化为字符串
date_str = latest_date.strftime('%Y%m%d')

# Period 转换（用于周/月线）
stk[period_col] = stk["date"].dt.to_period('W')

# 移位操作（计算下一期收益）
stk_p[next_ret_col] = stk_p.groupby("ts_code")[ret_col].shift(-1)
```

### 4. 高级特性

#### 4.1 并行计算
```python
# 使用 joblib 并行下载数据
results = Parallel(n_jobs=7)(
    delayed(single_function)(date, TUSHARE_TOKEN) 
    for date in tqdm(dates_to_download, desc="下载进度")
)

# 过滤有效结果
all_df_list = [df for date, df in results 
               if df is not None and not df.empty]
```

#### 4.2 路径处理
```python
from pathlib import Path

# 自动创建目录
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

"src\quant_infra\get_data.py"
# 检查文件是否存在
constituent_path = Path(f'./Data/Metadata/{code}_ins.csv')
if constituent_path.exists():
    constituents = pd.read_csv(constituent_path)
```

#### 4.3 装饰器与高阶函数
```python
# 通用数据获取框架
def get_data_by_date(single_function, table_name):
    """
    按交易日循环获取股票日频数据
    
    Args:
        single_function: 下载单日数据的函数（作为参数传入）
        table_name: DuckDB 表名
    """
    dates_to_download = get_dates_todo(table_name)
    
    results = Parallel(n_jobs=7)(
        delayed(single_function)(date, TUSHARE_TOKEN) 
        for date in tqdm(dates_to_download)
    )
```

---

## SQL 核心特性 (DuckDB)

### 1. 基础

#### 1.1 数据查询
```sql
-- 基础查询
SELECT * FROM stock_bar;

-- 条件筛选
SELECT * FROM stock_bar 
WHERE trade_date > '20240101' 
  AND ts_code = '000001.SZ';

-- 聚合统计
SELECT 
    ts_code,
    COUNT(*) as count,
    AVG(close) as avg_close
FROM stock_bar
GROUP BY ts_code
HAVING COUNT(*) > 100;
```

#### 1.2 表操作
```sql
-- 创建表
CREATE TABLE stock_bar AS 
SELECT * FROM temp_df;

-- 删除表
DROP TABLE IF EXISTS old_table;

-- 插入数据
INSERT INTO stock_bar 
SELECT * FROM new_data;

-- 查看表结构
DESCRIBE stock_bar;

-- 查看所有表
SHOW TABLES;
```

### 2. 高级 SQL 特性

#### 2.1 窗口函数
```sql
-- 计算移动平均
SELECT 
    ts_code,
    trade_date,
    close,
    AVG(close) OVER (
        PARTITION BY ts_code 
        ORDER BY trade_date 
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) as ma5
FROM stock_bar;

-- 排名
SELECT 
    ts_code,
    factor,
    RANK() OVER (
        PARTITION BY trade_date 
        ORDER BY factor DESC
    ) as factor_rank
FROM week_factor;
```

#### 2.2 CTE（公共表表达式）
```sql
WITH daily_return AS (
    SELECT 
        ts_code,
        trade_date,
        (close - LAG(close) OVER (PARTITION BY ts_code ORDER BY trade_date)) 
        / LAG(close) OVER (PARTITION BY ts_code ORDER BY trade_date) as ret
    FROM stock_bar
)
SELECT * FROM daily_return 
WHERE ret IS NOT NULL;
```


### 3. DuckDB 与 Pandas 集成

#### 3.1 Python 中执行 SQL
```python
import duckdb
import pandas as pd

# 连接数据库
conn = duckdb.connect('./Data/data.db')

# 执行 SQL 返回 DataFrame
result = conn.execute("""
    SELECT ts_code, trade_date, close
    FROM stock_bar
    WHERE trade_date > '20240101'
""").fetch_df()

# 注册 DataFrame 为临时表
conn.register('temp_df', df)

# 在 SQL 中使用 DataFrame
conn.execute("""
    CREATE TABLE stock_bar AS 
    SELECT * FROM temp_df
""")
```

#### 3.2 可视化展示
```python
# 方式 A：直接打印表格
conn.table('pricing_factors').show()

# 方式 B：获取列名
columns = conn.execute(
    "PRAGMA table_info('week_factor')"
).fetchall()
column_names = [col[1] for col in columns]
```



## 关键要点总结

### Python 特性
1. **图灵完备**：支持条件、循环、函数、递归等完整计算能力
2. **动态类型**：变量无需声明类型，灵活但需注意类型安全
3. **丰富的库**：Pandas、NumPy、Joblib 等提供强大功能

### SQL 特性 (DuckDB)
1. **声明式语言**：描述"要什么"而非"怎么做"
2. **集合操作**：天然适合批量数据处理
3. **与 Python 互补**：SQL 擅长数据查询，Python 擅长逻辑控制

### 最佳实践
1. 使用常量集中管理配置（如 [const.py](file://c:\file\量化\quant\src\quant_infra\const.py)）
2. 数据库操作封装成工具函数（如 [db_utils.py](file://c:\file\量化\quant\src\quant_infra\db_utils.py)）
3. SQL 查询优先，复杂逻辑用 Python
