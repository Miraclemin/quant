# -*- coding: utf-8 -*-
"""
DuckDB数据库工具模块 - 替代大CSV文件的读写操作
"""
import duckdb
import pandas as pd
import os
from pathlib import Path
from quant_infra.const import *
def init_db():
    """初始化数据库连接
    如果数据库文件不存在，会自动创建
    1、解析路径，2、定位到父目录，3、创建父目录（如果不存在），4、如果存在也不报错
    """
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    try:
        return duckdb.connect(DB_PATH)
    except duckdb.IOException as e:
        wrong_message = str(e)
        if any(keyword in wrong_message for keyword in ('Could not set lock', 'already open', '另一个程序', 'resource temporarily unavailable')):
            raise RuntimeError(
                f"数据库文件 '{DB_PATH}' 正被其他程序占用，请先关闭后重试。\n({wrong_message.splitlines()[0]})"
            ) from None
        raise
def read_sql(query):
    """
    执行SQL查询并返回DataFrame
    
    Args:
        query: SQL查询语句
        
    Returns:
        DataFrame
    """
    conn = init_db()
    try:
        result = conn.execute(query).fetch_df()
        return result
    finally:
        conn.close()

def write_to_db(df, table_name, save_mode ='replace'):
    """
    将DataFrame写入数据库
    
    Args:
        df: DataFrame
        table_name: 目标表名
        save_mode: 'replace'|'append'
    """
    conn = init_db()
    try:
        conn.register('temp_df', df)
        # 检查表格是否存在
        table_exists_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ## 取第一行.fetchone()的第一列[0]，如果大于0说明表格存在
        table_exists = conn.execute(table_exists_query).fetchone()[0] > 0

        if table_exists:
            if save_mode == 'replace':
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
            else:  # append
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        else:
            # 如果表格不存在，直接创建
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
        print(f"{table_name}新增数据：{len(df)} 行")
    finally:
        conn.close()

