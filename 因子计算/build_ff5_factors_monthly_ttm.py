import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import warnings

warnings.filterwarnings('ignore')

# --- 步骤 1: 定义数据中心路径 (严格按照词典) ---
#
# 数据中心绝对路径
DATA_CENTER_PATH = '/home/zy/桌面/数据中心/quant_data_center' 

# 定义"原材料"路径
# 股票基础信息
PATH_STOCK_BASIC = os.path.join(DATA_CENTER_PATH, 'stock_basic.parquet') 
# 股票日K线（后复权）
PATH_DAILY_HFQ_DIR = os.path.join(DATA_CENTER_PATH, 'stock/daily_hfq') 
# 股票每日基础指标
PATH_DAILY_BASIC = os.path.join(DATA_CENTER_PATH, 'stock/daily_basic/daily_basic_all.parquet') 
# 无风险利率
PATH_RFR = os.path.join(DATA_CENTER_PATH, 'factors/risk_free/rfr_daily.parquet') 
# 现金流量表 (v2.0新增)
PATH_CASHFLOW_DIR = os.path.join(DATA_CENTER_PATH, 'stock/financial_tables/cashflow') 
# 资产负债表
PATH_BALANCE_DIR = os.path.join(DATA_CENTER_PATH, 'stock/financial_tables/balancesheet')
# 财务指标 (v2.0新增，用于获取有形资产)
PATH_FINA_INDICATOR_DIR = os.path.join(DATA_CENTER_PATH, 'stock/fina_indicator') 

# 定义“成品”输出路径 (新)
OUTPUT_DIR_FF5 = os.path.join(DATA_CENTER_PATH, 'factors/fama_french_5')
OUTPUT_PATH_FF5 = os.path.join(OUTPUT_DIR_FF5, 'ff_5_factors_daily.parquet')

print(f"数据中心路径: {DATA_CENTER_PATH}")
print(f"输出路径: {OUTPUT_PATH_FF5}")


def load_all_financials():
    """
    步骤 2: 加载 *所有* 财务报表并预计算 TTM 因子 (v2.0优化版)
    这是实现月度调仓的关键，我们一次性加载所有季报数据
    
    v2.0 优化：
    - RMW因子：使用经营现金流(OCF)替代营业利润(OP)
    - CMA因子：使用经营性净资产(NOA)替代总资产(Inv)
    """
    print("步骤 2: 正在加载所有财务报表 (Cashflow, BalanceSheet, FinaIndicator)...")
    
    # 1. 加载现金流量表 (v2.0核心)
    print("  加载现金流量表...")
    df_cashflow = ds.dataset(PATH_CASHFLOW_DIR, format='parquet').to_table(
        columns=['ts_code', 'ann_date', 'end_date', 'n_cashflow_act']
    ).to_pandas()
    
    # 2. 加载资产负债表
    print("  加载资产负债表...")
    df_balance = ds.dataset(PATH_BALANCE_DIR, format='parquet').to_table(
        columns=['ts_code', 'ann_date', 'end_date', 'total_assets', 'total_liab']
    ).to_pandas()
    
    # 3. 加载财务指标表(用于获取有形资产) (v2.0核心)
    print("  加载财务指标表...")
    # 注意：不使用dataset方式，因为tangible_asset字段可能存在schema不一致问题
    fina_files = [f for f in os.listdir(PATH_FINA_INDICATOR_DIR) if f.endswith('.parquet')]
    df_fina_list = []
    for i, file in enumerate(fina_files):
        if (i + 1) % 1000 == 0:
            print(f"    已加载 {i+1}/{len(fina_files)} 个文件...")
        df_temp = pd.read_parquet(
            os.path.join(PATH_FINA_INDICATOR_DIR, file),
            columns=['ts_code', 'ann_date', 'end_date', 'tangible_asset']
        )
        df_fina_list.append(df_temp)
    df_fina = pd.concat(df_fina_list, ignore_index=True)
    print(f"  财务指标表加载完成: {len(df_fina)} 条记录")

    # 统一日期格式
    for df in [df_cashflow, df_balance, df_fina]:
        df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d')
        df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d')

    # ====================
    # v2.0 优化 1: 计算 OCF_TTM (经营现金流)
    # ====================
    print("  计算OCF_TTM（经营现金流TTM）...")
    df_cashflow = df_cashflow.sort_values(by=['ts_code', 'end_date']).drop_duplicates()
    # 计算单季OCF (Tushare的n_cashflow_act是累计值，需要转为单季)
    df_cashflow['ocf_single_quarter'] = df_cashflow.groupby(['ts_code', df_cashflow.end_date.dt.year])['n_cashflow_act'].diff()
    # Q1的单季值 = 累计值
    is_q1 = (df_cashflow['end_date'].dt.month == 3)
    df_cashflow.loc[is_q1, 'ocf_single_quarter'] = df_cashflow.loc[is_q1, 'n_cashflow_act']
    
    # 计算 TTM OCF = 滚动4个季度的单季OCF之和
    df_cashflow['OCF_TTM'] = df_cashflow.groupby('ts_code')['ocf_single_quarter'].rolling(window=4, min_periods=4).sum().reset_index(level=0, drop=True)
    
    # ====================
    # v2.0 优化 2: 计算 NOA_Inv (经营性净资产投资)
    # ====================
    print("  计算NOA_Inv（经营性净资产投资因子）...")
    # 合并有形资产和总资产
    df_balance = pd.merge(
        df_balance,
        df_fina[['ts_code', 'ann_date', 'end_date', 'tangible_asset']],
        on=['ts_code', 'ann_date', 'end_date'],
        how='left'
    )
    
    df_balance = df_balance.sort_values(by=['ts_code', 'end_date']).drop_duplicates()
    
    # NOA = 有形资产 (tangible_asset包含固定资产+在建工程+无形资产等)
    df_balance['NOA'] = df_balance['tangible_asset'].fillna(0)
    
    # 找到 4 个季度前 (即 1 年前) 的NOA和总资产
    df_balance['NOA_t_4'] = df_balance.groupby('ts_code')['NOA'].shift(4)
    df_balance['assets_t_4'] = df_balance.groupby('ts_code')['total_assets'].shift(4)
    
    # 计算NOA投资因子: NOA_Inv = (NOA_t - NOA_t-4) / 总资产_t-4
    df_balance['NOA_Inv'] = (df_balance['NOA'] - df_balance['NOA_t_4']) / df_balance['assets_t_4']
    
    # 3. 计算 Book Equity (净资产 B)
    df_balance['B_latest'] = df_balance['total_assets'] - df_balance['total_liab'] 
    
    # ====================
    # 合并所有TTM因子
    # ====================
    print("  合并所有TTM因子...")
    df_financials = pd.merge(
        df_cashflow[['ts_code', 'ann_date', 'end_date', 'OCF_TTM']],
        df_balance[['ts_code', 'ann_date', 'end_date', 'NOA_Inv', 'B_latest']],
        on=['ts_code', 'ann_date', 'end_date'],
        how='outer'
    )
    
    # 过滤掉 TTM 计算初期的空值
    df_financials = df_financials.dropna(subset=['OCF_TTM', 'NOA_Inv', 'B_latest'])
    
    print(f"  财务数据 TTM 预计算完成，共 {len(df_financials)} 条有效 TTM 记录")
    print(f"  v2.0 优化指标: OCF_TTM (经营现金流), NOA_Inv (经营性净资产投资)")
    return df_financials


def load_all_market_data(start_date, end_date):
    """
    步骤 3: 加载 *所有* 市场数据 (K线/市值) 并预计算收益率
    """
    print("步骤 3: 正在加载所有市场数据 (HFQ K线, Daily Basic)...")
    
    # 1. 加载日K线(后复权)
    #
    # 注意：需要转换日期格式为YYYYMMDD（去掉横杠）
    start_date_int = start_date.replace('-', '')
    end_date_int = end_date.replace('-', '')
    
    hfq_dataset = ds.dataset(PATH_DAILY_HFQ_DIR, format='parquet')
    df_hfq = hfq_dataset.to_table(
        columns=['ts_code', 'trade_date', 'close'],
        filter=(ds.field('trade_date') >= start_date_int) & 
               (ds.field('trade_date') <= end_date_int)
    ).to_pandas()
    df_hfq['trade_date'] = pd.to_datetime(df_hfq['trade_date'], format='%Y%m%d')

    # 2. 计算日收益率 (Return)
    df_hfq = df_hfq.sort_values(by=['ts_code', 'trade_date'])
    df_hfq['return'] = df_hfq.groupby('ts_code')['close'].pct_change()
    df_hfq = df_hfq[['ts_code', 'trade_date', 'return']].dropna()
    
    # 3. 加载每日市值 (MV)
    #
    # 注意：需要转换日期格式为YYYYMMDD（去掉横杠）
    start_date_int = start_date.replace('-', '')
    end_date_int = end_date.replace('-', '')
    
    df_basic_daily = pd.read_parquet(
        PATH_DAILY_BASIC,
        columns=['ts_code', 'trade_date', 'total_mv'],
        filters=[('trade_date', '>=', start_date_int), ('trade_date', '<=', end_date_int)]
    )
    df_basic_daily['trade_date'] = pd.to_datetime(df_basic_daily['trade_date'], format='%Y%m%d')
    # MV单位为万元
    df_basic_daily['total_mv'] = df_basic_daily['total_mv'] * 10000 
    
    # 4. 合并收益率和市值
    df_market = pd.merge(
        df_hfq,
        df_basic_daily,
        on=['ts_code', 'trade_date'],
        how='inner'
    )
    
    # 5. 准备 *昨日* 市值 (prev_mv) 用于加权
    df_market = df_market.sort_values(by=['ts_code', 'trade_date'])
    df_market['prev_mv'] = df_market.groupby('ts_code')['total_mv'].shift(1)
    
    df_market = df_market.dropna(subset=['return', 'prev_mv'])
    
    print(f"  市场数据加载和预处理完成，共 {len(df_market)} 条日度记录")
    return df_market


def get_sorting_snapshot_monthly(month_end, df_financials_ttm, df_market_daily, df_basic_info):
    """
    步骤 4: (每月执行) 获取月度 TTM 财务快照和当期市值 (v2.0优化版)
    
    v2.0优化：
    - OP → OCF_B (经营现金流/净资产)
    - Inv → NOA_Inv (经营性净资产投资)
    """
    
    # 1. 筛选 *截至* month_end *已公告* 的 *最新* 财务数据
    # 严格遵守 ann_date
    df_fin_available = df_financials_ttm[df_financials_ttm['ann_date'] <= month_end]
    # 取每个股票的 *最新* 一份 TTM 报告
    df_fin_snapshot = df_fin_available.sort_values('end_date').groupby('ts_code').last()
    
    # 2. 获取 *当月最后一天* 的市值 (MV)
    df_mv_snapshot = df_market_daily[df_market_daily['trade_date'] <= month_end]
    df_mv_snapshot = df_mv_snapshot.sort_values('trade_date').groupby('ts_code').last()

    # 3. 合并财务快照与市值快照
    df_snapshot = pd.merge(
        df_fin_snapshot[['OCF_TTM', 'NOA_Inv', 'B_latest']],
        df_mv_snapshot[['total_mv']],
        left_index=True,
        right_index=True,
        how='inner' # 必须同时有财务数据和市值
    )

    # 4. 计算当期排序因子 B/M, OCF_B, NOA_Inv (v2.0优化)
    df_snapshot['OCF_B'] = df_snapshot['OCF_TTM'] / df_snapshot['B_latest']  # v2.0: 经营现金流/净资产
    df_snapshot['NOA_Inv'] = df_snapshot['NOA_Inv']  # v2.0: 经营性净资产投资
    df_snapshot['B/M'] = df_snapshot['B_latest'] / df_snapshot['total_mv']
    df_snapshot['MV'] = df_snapshot['total_mv']  # MV 即 Size
    
    # 5. 过滤股票池
    # 过滤金融股
    non_financial_stocks = df_basic_info[
        ~df_basic_info['industry'].str.contains('银行|保险|证券|金融', na=False) 
    ]['ts_code']
    df_snapshot = df_snapshot[df_snapshot.index.isin(non_financial_stocks)]
    
    # 过滤 B > 0 且因子有效的
    df_snapshot = df_snapshot[
        (df_snapshot['B_latest'] > 0) &
        df_snapshot['B/M'].notna() &
        df_snapshot['OCF_B'].notna() &
        df_snapshot['NOA_Inv'].notna()
    ]
    
    return df_snapshot[['MV', 'B/M', 'OCF_B', 'NOA_Inv']]


def get_portfolios_monthly(df_snapshot):
    """
    步骤 5: (每月执行) 独立 2x3 排序 (v2.0优化版)
    
    v2.0优化：
    - Prof组：基于OCF_B(经营现金流/净资产)分组
    - Invest组：基于NOA_Inv(经营性净资产投资)分组
    """
    
    # 1. 规模断点 (Size Breakpoint) - 中位数
    mv_median = df_snapshot['MV'].median()
    df_snapshot['Size'] = np.where(df_snapshot['MV'] <= mv_median, 'S', 'B')
    
    # 2. 因子断点 (30%, 70% 分位数)
    
    # B/M (HML) 断点
    bm_30 = df_snapshot['B/M'].quantile(0.3)
    bm_70 = df_snapshot['B/M'].quantile(0.7)
    df_snapshot['Value'] = np.where(df_snapshot['B/M'] <= bm_30, 'L',
                               np.where(df_snapshot['B/M'] > bm_70, 'H', 'M'))
    
    # OCF_B (RMW) 断点 - v2.0优化: 经营现金流质量
    ocf_30 = df_snapshot['OCF_B'].quantile(0.3)
    ocf_70 = df_snapshot['OCF_B'].quantile(0.7)
    df_snapshot['Prof'] = np.where(df_snapshot['OCF_B'] <= ocf_30, 'W',
                               np.where(df_snapshot['OCF_B'] > ocf_70, 'R', 'M'))
    
    # NOA_Inv (CMA) 断点 - v2.0优化: 经营性净资产投资
    noa_inv_30 = df_snapshot['NOA_Inv'].quantile(0.3)
    noa_inv_70 = df_snapshot['NOA_Inv'].quantile(0.7)
    df_snapshot['Invest'] = np.where(df_snapshot['NOA_Inv'] <= noa_inv_30, 'C',
                                np.where(df_snapshot['NOA_Inv'] > noa_inv_70, 'A', 'M'))

    # 3. 构建投资组合（按照 Fama-French 2015 标准）
    portfolios = {}
    
    # B/M 维度的 2×3 = 6 个组合（用于 HML 和 SMB_B/M）
    portfolios['SL'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Value'] == 'L')].index
    portfolios['SM_BM'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Value'] == 'M')].index
    portfolios['SH'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Value'] == 'H')].index
    portfolios['BL'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Value'] == 'L')].index
    portfolios['BM_BM'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Value'] == 'M')].index
    portfolios['BH'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Value'] == 'H')].index
    
    # OCF_B 维度的 2×3 = 6 个组合（用于 RMW 和 SMB_OP）- v2.0优化
    portfolios['SW'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Prof'] == 'W')].index
    portfolios['SM_OP'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Prof'] == 'M')].index
    portfolios['SR'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Prof'] == 'R')].index
    portfolios['BW'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Prof'] == 'W')].index
    portfolios['BM_OP'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Prof'] == 'M')].index
    portfolios['BR'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Prof'] == 'R')].index
    
    # NOA_Inv 维度的 2×3 = 6 个组合（用于 CMA 和 SMB_Inv）- v2.0优化
    portfolios['SA'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Invest'] == 'A')].index
    portfolios['SM_Inv'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Invest'] == 'M')].index
    portfolios['SC'] = df_snapshot[(df_snapshot['Size'] == 'S') & (df_snapshot['Invest'] == 'C')].index
    portfolios['BA'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Invest'] == 'A')].index
    portfolios['BM_Inv'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Invest'] == 'M')].index
    portfolios['BC'] = df_snapshot[(df_snapshot['Size'] == 'B') & (df_snapshot['Invest'] == 'C')].index

    # MKT 组合
    portfolios['MKT'] = df_snapshot.index

    return portfolios


def calculate_daily_factors(df_month_market, portfolios, df_rfr_month):
    """
    步骤 6: (每月执行) 计算下一个月每一天的因子收益 (v2.0优化版)
    
    v2.0优化说明：
    - RMW因子：基于OCF_B(经营现金流质量)分组计算
    - CMA因子：基于NOA_Inv(经营性净资产投资)分组计算
    - 计算公式不变，但底层组合已经使用新指标
    """
    
    # 1. 将日度数据转换为计算友好的 pivot 格式
    df_returns = df_month_market.pivot(index='trade_date', columns='ts_code', values='return')
    df_prev_mv = df_month_market.pivot(index='trade_date', columns='ts_code', values='prev_mv')

    def get_vw_return(stocks_idx, date):
        """(Helper) 计算市值加权收益"""
        if stocks_idx.empty:
            return 0
        
        # 筛选出当日在 portfolio 中且有数据的股票
        valid_stocks = stocks_idx.intersection(df_returns.columns).intersection(df_prev_mv.columns)
        if valid_stocks.empty:
            return 0
            
        day_returns = df_returns.loc[date, valid_stocks].dropna()
        day_weights = df_prev_mv.loc[date, valid_stocks].dropna()
        
        common_stocks = day_returns.index.intersection(day_weights.index)
        if common_stocks.empty:
            return 0
            
        day_returns = day_returns[common_stocks]
        day_weights = day_weights[common_stocks]
        
        total_mv = day_weights.sum()
        if total_mv == 0:
            return 0
            
        return (day_returns * day_weights).sum() / total_mv

    daily_factors = []
    
    for date in df_returns.index: # 遍历下个月的所有交易日
        
        # 1. HML (High Minus Low) - 基于 B/M 维度
        R_SH = get_vw_return(portfolios['SH'], date)
        R_SL = get_vw_return(portfolios['SL'], date)
        R_BH = get_vw_return(portfolios['BH'], date)
        R_BL = get_vw_return(portfolios['BL'], date)
        HML = (R_SH + R_BH) / 2.0 - (R_SL + R_BL) / 2.0

        # 2. RMW (Robust Minus Weak) - 基于 OP 维度
        R_SR = get_vw_return(portfolios['SR'], date)
        R_SW = get_vw_return(portfolios['SW'], date)
        R_BR = get_vw_return(portfolios['BR'], date)
        R_BW = get_vw_return(portfolios['BW'], date)
        RMW = (R_SR + R_BR) / 2.0 - (R_SW + R_BW) / 2.0
        
        # 3. CMA (Conservative Minus Aggressive) - 基于 Inv 维度
        R_SC = get_vw_return(portfolios['SC'], date)
        R_SA = get_vw_return(portfolios['SA'], date)
        R_BC = get_vw_return(portfolios['BC'], date)
        R_BA = get_vw_return(portfolios['BA'], date)
        CMA = (R_SC + R_BC) / 2.0 - (R_SA + R_BA) / 2.0
        
        # 4. SMB (Small Minus Big) - 标准 FF(2015) 方法
        # SMB_B/M: 在 B/M 维度计算 Small - Big
        R_SL = get_vw_return(portfolios['SL'], date)
        R_SM_BM = get_vw_return(portfolios['SM_BM'], date)
        R_SH = get_vw_return(portfolios['SH'], date)
        R_BL = get_vw_return(portfolios['BL'], date)
        R_BM_BM = get_vw_return(portfolios['BM_BM'], date)
        R_BH = get_vw_return(portfolios['BH'], date)
        SMB_BM = (R_SL + R_SM_BM + R_SH) / 3.0 - (R_BL + R_BM_BM + R_BH) / 3.0
        
        # SMB_OP: 在 OP 维度计算 Small - Big
        R_SW = get_vw_return(portfolios['SW'], date)
        R_SM_OP = get_vw_return(portfolios['SM_OP'], date)
        R_SR = get_vw_return(portfolios['SR'], date)
        R_BW = get_vw_return(portfolios['BW'], date)
        R_BM_OP = get_vw_return(portfolios['BM_OP'], date)
        R_BR = get_vw_return(portfolios['BR'], date)
        SMB_OP = (R_SW + R_SM_OP + R_SR) / 3.0 - (R_BW + R_BM_OP + R_BR) / 3.0
        
        # SMB_Inv: 在 Inv 维度计算 Small - Big
        R_SA = get_vw_return(portfolios['SA'], date)
        R_SM_Inv = get_vw_return(portfolios['SM_Inv'], date)
        R_SC = get_vw_return(portfolios['SC'], date)
        R_BA = get_vw_return(portfolios['BA'], date)
        R_BM_Inv = get_vw_return(portfolios['BM_Inv'], date)
        R_BC = get_vw_return(portfolios['BC'], date)
        SMB_Inv = (R_SA + R_SM_Inv + R_SC) / 3.0 - (R_BA + R_BM_Inv + R_BC) / 3.0
        
        # SMB: 三个维度的平均
        SMB = (SMB_BM + SMB_OP + SMB_Inv) / 3.0
        
        # 5. MKT_RF (Market Risk Premium)
        MKT = get_vw_return(portfolios['MKT'], date)
        RF = df_rfr_month.loc[date, 'rf']
        MKT_RF = MKT - RF
        
        daily_factors.append({
            'trade_date_dt': date,
            'MKT_RF': MKT_RF,
            'SMB': SMB,
            'HML': HML,
            'RMW': RMW,
            'CMA': CMA
        })

    df_factors = pd.DataFrame(daily_factors).set_index('trade_date_dt')
    return df_factors


def main():
    """
    主执行函数：循环每月构建因子 (月度TTM方案 v2.0优化版)
    
    v2.0 核心优化：
    1. RMW因子：使用经营现金流(OCF)替代营业利润(OP)，更关注盈利质量
    2. CMA因子：使用经营性净资产(NOA)替代总资产(Inv)，更准确衡量真实投资
    """
    print("--- Fama-French 五因子 (月度TTM版 v2.0优化版) 构建开始 ---")
    print("v2.0 优化: RMW采用经营现金流(OCF), CMA采用经营性净资产(NOA)")
    
    # --- A. 设定起止日期 ---
    # TTM 需要 4 季度财报，Inv 需要 t-4 财报，共需 8 季度 (2年) 财务数据
    # 市值数据从2012年开始，财务数据2011年已充足
    # 从 2012年1月 开始加载市场数据
    # 从 2012年10月 开始构建因子（确保有足够的TTM数据，2010-2011两年共8个季度）
    START_DATE_STR = "2012-01-01"  # 市场数据从2012年开始加载
    START_LOOP_DATE = pd.to_datetime("2012-10-01")  # 因子构建从2012年10月开始
    END_DATE_STR = datetime.now().strftime('%Y-%m-%d')
    
    # --- B. 一次性加载所有“原材料” ---
    print("步骤 1: 正在一次性加载所有“原材料”...")
    
    #
    df_basic_info = pd.read_parquet(PATH_STOCK_BASIC, columns=['ts_code', 'industry', 'list_date'])
    
    #
    df_rfr_all = pd.read_parquet(PATH_RFR)
    df_rfr_all['trade_date'] = pd.to_datetime(df_rfr_all['trade_date'], format='%Y%m%d')
    df_rfr_all['rf'] = df_rfr_all['rf'] / 100.0 # 转为小数
    df_rfr_all = df_rfr_all.set_index('trade_date').sort_index()

    #
    df_financials_ttm = load_all_financials()
    
    #
    print(f"  加载市场数据: {START_DATE_STR} ~ {END_DATE_STR}")
    df_market_daily = load_all_market_data(START_DATE_STR, END_DATE_STR)
    print(f"  市场数据实际范围: {df_market_daily['trade_date'].min()} ~ {df_market_daily['trade_date'].max()}")
    
    all_factors_list = []
    
    # --- C. 按月循环构建 ---
    print("\n步骤 7: 开始按月度循环构建因子...")
    
    # freq='M' 表示月末
    loop_dates = pd.date_range(START_LOOP_DATE, END_DATE_STR, freq='M')
    
    # 添加当前月份（即使数据不完整）
    # 这样可以每天更新最新的因子数据
    current_date = datetime.now()
    current_month_end = pd.to_datetime(current_date.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1))
    
    # 如果当前月末不在loop_dates中，添加进去
    if current_month_end not in loop_dates:
        loop_dates = loop_dates.insert(len(loop_dates), current_month_end)
        print(f"  [实时模式] 添加当前月份 {current_month_end.strftime('%Y-%m')} 用于计算最新因子")
    
    for month_end in loop_dates:
        
        # 1. 定义日期
        # 排序日: month_end
        # 调仓期: 下个月的第一个交易日 -> 下下个月的第一个交易日
        period_start_date = month_end + pd.DateOffset(days=1)
        period_end_date = month_end + pd.DateOffset(months=1) # 月末
        
        print(f"\n[处理月份: {month_end.strftime('%Y-%m')}]")
        print(f"  排序日: {month_end.date()}")
        
        # 改进：允许计算当前月份的不完整数据
        # 只要有一天的数据就可以计算
        market_max_date = df_market_daily['trade_date'].max()
        
        if period_start_date > market_max_date:
            # 检查调仓期的月份
            period_start_month = period_start_date.to_period('M')
            current_month = pd.to_datetime(datetime.now()).to_period('M')
            
            if period_start_month == current_month:
                # 当前月份，但还没有交易数据
                print(f"  当前月份({period_start_month})数据尚未开始，跳过。")
            else:
                # 未来月份
                print(f"  调仓期尚未开始（未来月份），跳过。")
            continue
        
        # 调整period_end_date：使用实际可用的最大日期
        if period_end_date > market_max_date:
            actual_end_date = market_max_date
            print(f"  注意: 使用最新数据至 {actual_end_date.date()} (当月数据不完整)")
        else:
            actual_end_date = period_end_date

        # 2. 获取月度快照 (TTM财务 + 当期市值)
        df_snapshot = get_sorting_snapshot_monthly(month_end, df_financials_ttm, df_market_daily, df_basic_info)
        
        if df_snapshot.empty:
            # 详细诊断
            df_fin_test = df_financials_ttm[df_financials_ttm['ann_date'] <= month_end]
            df_mv_test = df_market_daily[df_market_daily['trade_date'] <= month_end]
            print(f"  警告: {month_end.date()} 无有效数据进行分组")
            print(f"    - 可用财务数据: {len(df_fin_test)} 条 ({df_fin_test['ts_code'].nunique() if len(df_fin_test) > 0 else 0} 只股票)")
            print(f"    - 可用市值数据: {len(df_mv_test)} 条 ({df_mv_test['ts_code'].nunique() if len(df_mv_test) > 0 else 0} 只股票)")
            if len(df_fin_test) == 0:
                print(f"    → 财务数据不足（TTM需要至少4个季度数据）")
            continue
            
        # 3. 执行月度分组
        portfolios = get_portfolios_monthly(df_snapshot)
        
        # 4. 筛选下个月的日度数据（使用actual_end_date）
        df_month_market = df_market_daily[
            (df_market_daily['trade_date'] >= period_start_date) &
            (df_market_daily['trade_date'] <= actual_end_date)
        ].copy()
        
        df_rfr_month = df_rfr_all[
            (df_rfr_all.index >= period_start_date) &
            (df_rfr_all.index <= actual_end_date)
        ].copy()
        
        if df_month_market.empty or df_rfr_month.empty:
            print(f"  警告: {month_end.strftime('%Y-%m')} 调仓期内无有效K线或无风险利率数据，跳过")
            continue

        # 5. 计算下个月的每日因子
        df_factors = calculate_daily_factors(df_month_market, portfolios, df_rfr_month)
        all_factors_list.append(df_factors)
        print(f"  {month_end.strftime('%Y-%m')} 因子计算完成，共 {len(df_factors)} 个交易日")

    # --- D. 汇总并存储 ---
    if not all_factors_list:
        print("错误：未能计算任何因子数据，程序终止。")
        return

    print("\n步骤 8: 汇总所有因子数据并存储...")
    final_ff5_df = pd.concat(all_factors_list).sort_index()
    
    # 转换为 %
    final_ff5_df = final_ff5_df * 100
    
    # 转换索引 'trade_date' 为 YYYYMMDD 字符串，符合词典规范
    #
    final_ff5_df.index.name = 'trade_date_dt'
    final_ff5_df = final_ff5_df.reset_index()
    final_ff5_df['trade_date'] = final_ff5_df['trade_date_dt'].dt.strftime('%Y%m%d')
    
    # 词典规范字段 (扩展为5因子)
    final_ff5_df = final_ff5_df[['trade_date', 'MKT_RF', 'SMB', 'HML', 'RMW', 'CMA']]
    
    # 创建新目录
    if not os.path.exists(OUTPUT_DIR_FF5):
        os.makedirs(OUTPUT_DIR_FF5)
        print(f"  已创建新目录: {OUTPUT_DIR_FF5}")
        
    # 存储到数据中心
    final_ff5_df.to_parquet(OUTPUT_PATH_FF5, engine='pyarrow', index=False)
    
    print(f"\n--- Fama-French 五因子 (月度TTM版 v2.0优化版) 构建成功! ---")
    print(f"v2.0 优化指标: RMW=经营现金流质量, CMA=经营性净资产投资")
    print(f"总计 {len(final_ff5_df)} 条记录已保存至:")
    print(f"{OUTPUT_PATH_FF5}")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\n总耗时: {end_time - start_time:.2f} 秒")
