# -*- coding: utf-8 -*-
"""
因子构建模块 - 所有因子计算逻辑
支持使用DuckDB数据库而不是直接读取CSV文件
"""
import pandas as pd
import numpy as np
import statsmodels.api as sm
import os
from joblib import Parallel, delayed
from tqdm import tqdm
from quant_infra import db_utils, get_data

#按日期计算定价因子   
# 定义单日计算函数
def calc_single_day_factors(trade_date, day_df):
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
        print("数据已是最新")
        return 
    
    # 过滤出需要计算的日期，由于是几个定价因子都是当月值，所以要有“当月第一个天————最新日期的完整数据”
    # 除了第一次计算要算beta外，后续计算时，只用算出几个定价因子值就可以了
    first_month_date = pd.to_datetime(str(dates_to_download[0]), format='%Y%m%d').to_period('M').to_timestamp() 
    first_month_date_str = first_month_date.strftime('%Y%m%d')
    last_date = dates_to_download[-1]

    # 1. 读取股票数据
    stock_data = db_utils.read_sql(f"SELECT ts_code, trade_date, pct_chg FROM stock_bar WHERE trade_date >= '{first_month_date_str}' AND trade_date <= '{last_date}'")
    
    # 2. 读取财务数据（包含市值和PB）
    financial_data = db_utils.read_sql(f"SELECT ts_code, trade_date, total_mv, pb FROM daily_basic WHERE trade_date >= '{first_month_date_str}' AND trade_date <= '{last_date}'")    

    # 合并股票数据和财务数据
    df = pd.merge(stock_data, financial_data, on=['ts_code', 'trade_date'], how='inner')
    
    if len(df) == 0:
        print('合并后没有数据')
        return
    
    print('开始计算新数据')
    # 3. 添加年月标识（用于分组，基于trade_date）
    # print('计算年月标识...')
    df['date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    df['year_month'] = df['date'].dt.to_period('M')
    
    # 4. 计算当月截面数据（使用 transform 直接在原表完成计算，避免额外的 merge）
    # 执行了 sort_values(['ts_code', 'trade_date'])， 'last' 取到的就是月内最后（月末）数据
    df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
    grouped = df.groupby(['ts_code', 'year_month'])
    df['month_ret'] = grouped['pct_chg'].transform('sum')    # 当月累积收益（用于UMD）
    df['month_mv'] = grouped['total_mv'].transform('last')   # 当月末市值（用于SMB）
    df['month_pb'] = grouped['pb'].transform('last')         # 当月末PB（用于HML）
    
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
    
    # 5.按交易日分组，并行计算每个交易日的定价因子
    daily_groups = list(df.groupby('trade_date'))
    
    pricing_factors = Parallel(n_jobs=-1)(
        delayed(calc_single_day_factors)(trade_date, day_df) 
        for trade_date, day_df in tqdm(daily_groups, desc='计算定价因子', ncols=80, position=0, leave=True)
    )
    
    # 过滤掉None结果
    pricing_factors = [f for f in pricing_factors if f is not None]
        
    # 输出结果
    result_df = pd.DataFrame(pricing_factors)
    # 将结果写入数据库
    db_utils.write_to_db(result_df, 'pricing_factors', save_mode='append')
    print("定价因子计算完成")
    return 

def calc_single_beta(ts_code, stock_df):
    """计算单只股票的beta因子 - 用全历史数据估计beta"""
    try:
        if len(stock_df) < 30:  # 至少需要30个交易日
            return {'factors': [], 'beta': None}   
                 
        # 用单只股票的所有历史数据做回归（截距 + MKT/SMB/HML/UMD）
        X_full = np.column_stack([np.ones(len(stock_df)), stock_df[['MKT', 'SMB', 'HML', 'UMD']].to_numpy()])
        y_full = stock_df['pct_chg'].to_numpy()

        # 检查数据是否有效
        if not (np.isfinite(X_full).all() and np.isfinite(y_full).all()):
            return {'factors': [], 'beta': None, 'status': '未知失败'}

        try:
            beta = np.linalg.lstsq(X_full, y_full, rcond=None)[0]
        except (np.linalg.LinAlgError, ValueError):
            return {'factors': [], 'beta': None, 'status': '未知失败'}

        if not np.isfinite(beta).all():
            return {'factors': [], 'beta': None, 'status': '未知失败'}

        # 保存单套beta信息
        beta_info = {
            'ts_code': ts_code,
            'intercept': float(beta[0]),
            'mkt': float(beta[1]),
            'smb': float(beta[2]),
            'hml': float(beta[3]),
            'umd': float(beta[4]),
            'update_date': pd.Timestamp.now().strftime('%Y%m%d')
        }

        # 构建X矩阵用于计算残差（分别对周一与周内样本计算残差，但均使用相同的 beta）
        X_monday = np.column_stack([np.ones(len(monday_df)), monday_df[['MKT', 'SMB', 'HML', 'UMD']].to_numpy()])
        y_monday = monday_df['pct_chg'].to_numpy()
        X_weekday = np.column_stack([np.ones(len(weekday_df)), weekday_df[['MKT', 'SMB', 'HML', 'UMD']].to_numpy()])
        y_weekday = weekday_df['pct_chg'].to_numpy()

        if not (np.isfinite(X_monday).all() and np.isfinite(y_monday).all() and 
                np.isfinite(X_weekday).all() and np.isfinite(y_weekday).all()):
            return {'factors': [], 'beta': None, 'status': '未知失败'}

        monday_df = monday_df.copy()
        weekday_df = weekday_df.copy()
        monday_df['resid'] = y_monday - X_monday @ beta
        weekday_df['resid'] = y_weekday - X_weekday @ beta
        
        # 按月聚合残差
        results = []
        for year_month in stock_df['year_month'].unique():
            monday_month = monday_df[monday_df['year_month'] == year_month]
            weekday_month = weekday_df[weekday_df['year_month'] == year_month]
            
            if len(monday_month) > 0 and len(weekday_month) > 0:
                # 周末效应因子 = 周一残差均值 - 周内残差均值
                raw_factor = monday_month['resid'].mean() - weekday_month['resid'].mean()
                
                results.append({
                    'ts_code': ts_code,
                    'year_month': str(year_month),
                    'trade_date': monday_month['trade_date'].max(),  # 使用该月最后一个交易日
                    'raw_factor': raw_factor
                })
        
        return {'factors': results, 'beta': beta_info, 'status': 'success'}
        
    except Exception as e:
        # 完全静默，不输出任何错误
        return {'factors': [], 'beta': None, 'status': '未知失败'}
def compute_week_effect():
    """
    计算周末效应原始因子（Carhart四因子模型）- 按README算法优化版
    
    算法步骤（参考README）：
    1. 读取已有的定价因子（MKT, SMB, HML, UMD）并合并为df_pricing_merge
    2. 降维：将日线数据转换为周一、周内其他时间
    4. 使用Joblib多进程并行计算每只股票的残差
    5. 因子 = 周一残差 - 周内残差
    6. 月度聚合
    
    使用DuckDB读取和写入数据
    """   
    #     
    # ========== 检查week_factor表中已有哪些交易日的数据，返回需要计算的日期list ==========
    dates_to_download = get_data.get_dates_todo('week_factor')
    if not dates_to_download:
        print("因子已是最新")
        return

    # ========== 检查是否已有beta系数 ==========
    try:
        # 读取全部 beta 表（更健壮），如果存在 status 列则过滤掉被标记为 failed 的记录
        count = db_utils.read_sql("SELECT Count(*) FROM stock_betas").squeeze()
        if count > 5000:
            existing_betas = True
    except Exception:
        print('未找到已有beta数据')
        existing_betas = False
    
    # 把定价因子和股票日线数据合并在一起，减少后续计算时的重复读取和合并 
    query = """
    SELECT b.ts_code, b.trade_date, b.pct_chg, p.MKT, p.SMB, p.HML, p.UMD
    FROM stock_bar b
    LEFT JOIN (SELECT MKT, SMB, HML, UMD FROM pricing_factors) p
    ON b.trade_date = p.trade_date
    """
    df = db_utils.read_sql(query)
    df["date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d", errors="coerce")
    
    # **过滤掉定价因子为NaN的行**
    df = df.dropna(subset=['MKT', 'SMB', 'HML', 'UMD'])
    
    # 按股票代码分组，字典的查询效率远高于全量DataFrame
    stock_groups = dict(df.groupby('ts_code'))
    
    if not existing_betas:
        # 计算beta
        beta_results = Parallel(n_jobs=-1, verbose=0)(
            delayed(calc_single_beta)(code, stock_df, None)
            for code, stock_df in tqdm(stock_groups.items(), desc='计算beta', ncols=80, position=0, leave=True)
        )
        
        # 保存beta
        new_betas = []
        for i, code in enumerate(need_beta_codes):
            if beta_results[i]['beta'] is not None:
                new_betas.append(beta_results[i]['beta'])
            else:
                # 失败的也标记，使用具体的失败原因（保存单套 beta 的占位）
                status = beta_results[i].get('status', '未知失败')
                new_betas.append({
                    'ts_code': code,
                    'intercept': np.nan, 'mkt': np.nan, 'smb': np.nan,
                    'hml': np.nan, 'umd': np.nan,
                    'update_date': pd.Timestamp.now().strftime('%Y%m%d'),
                    'status': status
                })
    
    if len(new_betas) > 0:
        new_beta_df = pd.DataFrame(new_betas)
        success_count = len([b for b in new_betas if b.get('status') is None or b.get('status') == 'success'])
        insufficient_count = len([b for b in new_betas if b.get('status') == '交易日数不足'])
        unknown_fail_count = len([b for b in new_betas if b.get('status') == '未知失败'])
        print(f'保存beta: 成功 {success_count} 只, 交易日数不足 {insufficient_count} 只, 未知失败 {unknown_fail_count} 只')
        
        # 合并保存
        if existing_betas is not None and len(existing_betas) > 0:
            all_betas_df = pd.concat([existing_betas, new_beta_df], ignore_index=True)
        else:
            all_betas_df = new_beta_df
        db_utils.write_to_db(all_betas_df, 'stock_betas', save_mode='replace')
        
        # 更新existing_beta_dict
        for beta in new_betas:
            if beta.get('status') != 'failed':
                existing_beta_dict[beta['ts_code']] = beta
    
    # 第二步：计算所有需要的因子
    print(f'计算 {len(need_calc_codes)} 只股票的周末效应因子...')
    factor_results = Parallel(n_jobs=-1, prefer='threads', verbose=0)(
        delayed(calc_stock_factor_fast)(code, stock_groups[code], existing_beta_dict.get(code))
        for code in tqdm(need_calc_codes, desc='计算因子', ncols=80, position=0, leave=True)
    )
    
    # 合并结果，只收集因子
    all_factors = []
    for result in factor_results:
        all_factors.extend(result['factors'])
    
    if len(all_factors) == 0:
        print('警告：没有成功计算出任何因子！')
        return existing_week_factors if existing_week_factors is not None else pd.DataFrame()
    
    factor_df = pd.DataFrame(all_factors)
    
    # ========== 月度聚合 ==========
    print('月度聚合...')
    new_monthly_factor = factor_df.groupby(['ts_code', 'year_month'], as_index=False).agg({'raw_factor': 'mean','trade_date': 'last'})
    
    new_monthly_factor = new_monthly_factor.sort_values(['ts_code', 'year_month']).reset_index(drop=True)
    
    # 合并新旧因子数据
    if existing_week_factors is not None and len(existing_stocks) > 0:
        print(f'合并新旧因子数据...')
        old_factors = db_utils.read_sql("SELECT * FROM week_factor")
        monthly_factor = pd.concat([old_factors, new_monthly_factor], ignore_index=True)
        monthly_factor = monthly_factor.drop_duplicates(subset=['ts_code', 'year_month'], keep='last')
        monthly_factor = monthly_factor.sort_values(['ts_code', 'year_month']).reset_index(drop=True)
    else:
        monthly_factor = new_monthly_factor
    
    # 保存结果
    print('保存到DuckDB...')
    db_utils.write_to_db(monthly_factor, 'week_factor', save_mode='replace')
    print(f'✓ 周末效应因子已保存到 DuckDB 表: week_factor')
    
    print(f'  - 共 {len(monthly_factor)} 条记录（月度）')
    print(f'  - 覆盖 {monthly_factor["ts_code"].nunique()} 只股票')
    print(f'  - 时间范围: {monthly_factor["year_month"].min()} ~ {monthly_factor["year_month"].max()}')
    
    return monthly_factor

