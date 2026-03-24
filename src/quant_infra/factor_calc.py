import pandas as pd
import numpy as np
import statsmodels.api as sm
import os
from joblib import Parallel, delayed
from tqdm import tqdm
from quant_infra import db_utils, get_data
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthBegin

#按日期计算定价因子   
# 定义单日计算函数
def calc_single_pricing_factors(trade_date, day_df):
    """计算单个交易日的定价因子"""
    # 过滤有效数据
    valid_data = day_df.dropna(subset=['pct_chg'])
    
    if len(valid_data) < 9:  # 至少需要9只股票才能分3组
        return None
        
    ## MKT因子：当日所有股票的平均收益率
    mkt_factor = valid_data['pct_chg'].mean()
    
    factors_dict = {'SMB': np.nan, 'HML': np.nan, 'UMD': np.nan}
    # 配置格式: (因子名称, 排序基于的列名, 是否反转计算_即用低组减去高组)
    factor_configs = [
        ('SMB', 'month_mv', True),   # 小减大 (前33% - 后33%) 
        ('HML', 'month_pb', True),   # 价值减成长 (低PB即前33% - 高PB即后33%)
        ('UMD', 'month_ret', False)  # 赢家减输家 (高收益即后33% - 低收益即前33%)
    ]
    
    for factor_name, col, low_minus_high in factor_configs:
        sub_data = valid_data.dropna(subset=[col])
        if len(sub_data) >= 3:
            sub_data = sub_data.sort_values(col)
            n_third = max(1, len(sub_data) // 3)
            ret_low = sub_data.iloc[:n_third]['pct_chg'].mean()
            ret_high = sub_data.iloc[-n_third:]['pct_chg'].mean()
            # low_minus_high 为 True 时：用 低组（前33%） 减 高组（后33%）
            factors_dict[factor_name] = (ret_low - ret_high) if low_minus_high else (ret_high - ret_low)
    
    return {
        'trade_date': int(trade_date),
        'MKT': mkt_factor,
        **factors_dict
    }
def compute_pricing_factors():
    """
    计算定价因子（MKT、SMB、HML、UMD）
    依据 README 中定价因子计算部分的步骤：
    1. SMB: 上一月底的流动市值排名后三分之一股票组合的收益减去前三分之一股票组合的收益
    2. HML: 上一月底按账面市值比前三分之一股票组合的收益减去后三分之一股票组合的收益
    3. UMD: 上一月底按当月累积收益排名前三分之一股票组合的收益减去后三分之一股票组合的收益
    4. MKT: 当日所有股票的平均收益率
    """    
    dates_to_download = get_data.get_dates_todo('pricing_factors')

    if not dates_to_download:
        print("定价因子数据已是最新")
        return 
    
    print('开始计算新的定价因子')
    # 过滤出需要计算的日期，由于是几个定价因子都是依据上一个月末的数据算的，所以要有“上个月第一个天————最新日期的完整数据”
    # 除了第一次计算要算beta外，后续计算时，只用算出几个定价因子值就可以了
    first_month_date = pd.to_datetime(str(dates_to_download[0]), format='%Y%m%d') - MonthBegin(2)
    first_month_date_str = first_month_date.strftime('%Y%m%d')
    last_date = dates_to_download[-1]

    # 使用 SQL 直接合并股票数据和财务数据
    query = f"""
    SELECT b.ts_code, b.trade_date, b.pct_chg, d.total_mv, d.pb
    FROM stock_bar b
    INNER JOIN daily_basic d 
    ON b.ts_code = d.ts_code AND b.trade_date = d.trade_date
    WHERE b.trade_date >= '{first_month_date_str}' AND b.trade_date <= '{last_date}'
    """
    df = db_utils.read_sql(query)

    if len(df) == 0:
        print('daily_basic和 stock_bar 合并后没有数据，检查两表的最新数据是否下载成功')
        return
    

    # 3. 添加年月标识（用于分组，基于trade_date）
    # print('计算年月标识...')
    df['date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    df['year_month'] = df['date'].dt.to_period('M')
    
    df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)

    # 1. 先计算出每只股票每个月唯一的指标（月度表）
    monthly_df = df.groupby(['ts_code', 'year_month']).agg({
        'pct_chg': 'sum',
        'total_mv': 'last',
        'pb': 'last'
    }).reset_index()

    # 2. monthly_df 已经是原有的df按月聚合的表了，也就是所有股票，每月一行。
    # 现在对它进行按股票分组，为每个股票的全部月数据，进行 shift(1) 才是真正的上个月，得到上月末的指标值
    # 这样计算定价因子时才是真正的用的上月末数据，不会出现未来数据问题
    # 必须先按股票和时间排序，再按股票分组 shift
    monthly_df = monthly_df.sort_values(['ts_code', 'year_month'])
    monthly_df['month_ret'] = monthly_df.groupby('ts_code')['pct_chg'].shift(1)
    monthly_df['month_mv'] = monthly_df.groupby('ts_code')['total_mv'].shift(1)
    monthly_df['month_pb'] = monthly_df.groupby('ts_code')['pb'].shift(1)
    ## 注意：这里的 month_ret、month_mv、month_pb 都是上月末的值了，后续计算定价因子时就不会有未来数据问题了
    ## 也可以用monthly_df['target_month'] = monthly_df['year_month'] + 1，后续用target_month来merge，但直接shift(1)更简单直接

    # 3. 将这些“上月值”合并回原有的日线 df
    # 删掉月度表里原本的当月值列（由日数据聚合得到的），避免重名冲突
    monthly_df = monthly_df.drop(columns=['pct_chg', 'total_mv', 'pb'])
    df = pd.merge(df[['ts_code', 'trade_date', 'pct_chg', 'year_month']], monthly_df, on=['ts_code', 'year_month'], how='left')
    df.dropna(subset=['month_ret', 'month_mv', 'month_pb'], inplace=True)
    df = df[df['trade_date'].isin(dates_to_download)]
    # 5.按交易日分组，并行计算每个交易日的定价因子
    daily_groups = list(df.groupby('trade_date'))
    
    pricing_factors = Parallel(n_jobs=-1)(
        delayed(calc_single_pricing_factors)(trade_date, day_df) 
        for trade_date, day_df in tqdm(daily_groups, desc='计算定价因子', ncols=80, position=0, leave=True)
    )
    
    # 过滤掉None结果
    pricing_factors = [f for f in pricing_factors if f is not None]
        
    # 输出结果
    result_df = pd.DataFrame(pricing_factors)
    result_df = result_df.dropna(subset=['MKT', 'SMB', 'HML', 'UMD'])  # 过滤掉因子值为NaN的行
    # 将结果写入数据库
    db_utils.write_to_db(result_df, 'pricing_factors', save_mode='append')
    return 

def calc_single_beta(ts_code, stock_df):
    """计算单只股票的beta系数 - 用全历史数据做回归（日收益率y = 截距 + MKT/SMB/HML/UMD）"""
    try:
        if len(stock_df) < 220:  # 至少需要220个交易日
            return
                 
        # 用单只股票的所有历史数据做回归
        X_full = np.column_stack([np.ones(len(stock_df)), stock_df[['MKT', 'SMB', 'HML', 'UMD']].to_numpy()])
        y_full = stock_df['pct_chg'].to_numpy()

        beta = np.linalg.lstsq(X_full, y_full, rcond=None)[0]
        # 保存单套beta信息
        beta_info = {
            'ts_code': ts_code,
            'intercept': float(beta[0]),
            'MKT_beta': float(beta[1]),
            'SMB_beta': float(beta[2]),
            'HML_beta': float(beta[3]),
            'UMD_beta': float(beta[4]),
            'update_date': datetime.now().strftime('%Y%m%d')
        }

        return beta_info
        
    except Exception as e:
        # 完全静默，不抛出异常
        return

def calc_single_resid(code, stock_df):
    try:
        X_full = np.column_stack([np.ones(len(stock_df)), stock_df[['MKT', 'SMB', 'HML', 'UMD']].to_numpy()])
        y_full = stock_df['pct_chg'].to_numpy()

        # 单只股票在该分组内应只有一套 beta，取首行并转为 1D 向量
        beta_vec = stock_df[['intercept', 'MKT_beta', 'SMB_beta', 'HML_beta', 'UMD_beta']].iloc[0].to_numpy(dtype=float)
        y_hat = X_full @ beta_vec
        stock_df['resid'] = y_full - y_hat
        return stock_df[['ts_code', 'trade_date', 'resid']]
    except Exception as e:
        return pd.DataFrame()


def calc_resid():
    """
    需要能够判断是否需要获取beta，没有就计算
    计算resid后，直接写入数据库
    还应支持不会每次都重新计算
    """
    compute_pricing_factors()
    dates_to_download = get_data.get_dates_todo('stock_resids')
    if not dates_to_download:
        print("残差数据已是最新")
        return
    # ========== 检查是否已有beta系数 ==========
    existing_betas = False
    try:
        # 读取全部 beta 表（更健壮），如果存在 status 列则过滤掉被标记为 failed 的记录
        count = db_utils.read_sql("SELECT Count(*) FROM stock_betas").squeeze()
        if count > 5000:
            existing_betas = True
    except Exception:
        print('未找到已有beta数据')
        existing_betas = False
    
    # 把定价因子和股票日线数据合并在一起，减少后续计算时的重复读取和合并 
    query = f"""
    SELECT b.ts_code, b.trade_date, b.pct_chg, p.MKT, p.SMB, p.HML, p.UMD
    FROM stock_bar b
    LEFT JOIN (SELECT trade_date, MKT, SMB, HML, UMD FROM pricing_factors) p
    ON b.trade_date = p.trade_date
    WHERE b.trade_date >= '{dates_to_download[0]}' AND b.trade_date <= '{dates_to_download[-1]}'
    ORDER BY b.trade_date, b.ts_code
    """
    df = db_utils.read_sql(query)
    
    # **过滤掉定价因子为NaN的行**
    df = df.dropna(subset=['pct_chg', 'MKT', 'SMB', 'HML', 'UMD'])
    
    ## 如果本地没有beta，就先计算出beta并保存到数据库，后续计算因子时就可以直接读取beta了
    if not existing_betas:
        # 计算beta
        # 按股票代码分组
        groups = df.groupby('ts_code')
        # 在 Parallel 中直接使用迭代器
        beta_results = Parallel(n_jobs=-1)(
            delayed(calc_single_beta)(code, stock_df) 
            for code, stock_df in groups
        )
        beta_results = [x for x in beta_results if x is not None]
        if not beta_results:
            print('未生成可用beta：样本可能不足（当前阈值>=220）或数据异常')
            return
        # 保存beta
        beta_df = pd.DataFrame(beta_results)
        db_utils.write_to_db(beta_df, 'stock_betas', save_mode='replace')
        
    
    # 计算每日的残差
    beta_df = db_utils.read_sql("SELECT ts_code, intercept, MKT_beta, SMB_beta, HML_beta, UMD_beta FROM stock_betas")
    beta_df.dropna(inplace=True)
    combined_df = df.merge(beta_df, on='ts_code', how='left')
    groups = combined_df.groupby('ts_code')

    resid_results = Parallel(n_jobs=-1)(
        delayed(calc_single_resid)(code, group_df) 
    for code, group_df in tqdm(groups, desc='计算残差', ncols=80, position=0, leave=True)
    )
    resid_results = [x for x in resid_results if x is not None and not x.empty]
    if not resid_results:
        print('未生成可用残差：请检查beta表或有效样本区间')
        return
    # 合并结果
    all_resid = pd.concat(resid_results)
    db_utils.write_to_db(all_resid, 'stock_resids', save_mode='append')

def calc_spec_vol():
    """
    基于 stock_resids 计算特质波动率因子（日频）
    特质波动率 = 近20个交易日残差的波动率 = std(residuals)
    结果存入 spec_vol 表，列为 (ts_code, trade_date, factor)
    """
    dates_todo = get_data.get_dates_todo('spec_vol')
    if not dates_todo:
        print("特质波动率因子数据已是最新")
        return

    # 往前多取 45 自然日作为缓冲，确保能填满 20 交易日滚动窗口
    start_dt = pd.to_datetime(str(dates_todo[0]), format='%Y%m%d') - timedelta(days=45)
    start_str = start_dt.strftime('%Y%m%d')

    query = f"""
    SELECT ts_code, trade_date, resid
    FROM stock_resids
    WHERE trade_date >= '{start_str}' AND trade_date <= '{dates_todo[-1]}'
    ORDER BY ts_code, trade_date
    """
    df = db_utils.read_sql(query)

    if df.empty:
        print("stock_resids 为空，请先运行 calc_resid()")
        return

    df['trade_date'] = df['trade_date'].astype(str)
    df = df.sort_values(['ts_code', 'trade_date'])

    # 按股票分组，计算滚动 20 日波动率
    df['factor'] = df.groupby('ts_code')['resid'].transform(
        lambda x: x.rolling(window=20, min_periods=20).std()
    )

    result = df[df['trade_date'].isin(dates_todo)][['ts_code', 'trade_date', 'factor']]
    result = result.dropna(subset=['factor'])

    if result.empty:
        print("没有可保存的特质波动率数据（残差历史可能不足 20 个交易日）")
        return

    db_utils.write_to_db(result, 'spec_vol', save_mode='append')
    print(f"特质波动率因子计算完成，共 {len(result)} 条记录")


def winsorize(series, n=3):
    """按 n 倍标准差截尾"""
    mean, std = series.mean(), series.std()
    return series.clip(mean - n * std, mean + n * std)