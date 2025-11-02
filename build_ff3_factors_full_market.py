# /home/zy/桌面/数据中心/build_ff3_factors_full_market.py
#
# TrueNorth v5.3 - 任務 1.2: 構建"全市場FF3因子"
# 核心邏輯: 
# 1. (全市場) 不再過濾金融股
# 2. (月度TTM) 使用月度財報和市值
# 3. (FF3) 執行 2x3 (Size x B/M) 排序

import pandas as pd
import numpy as np
import os
import sys
import time
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pyarrow.dataset as ds
import warnings

# --- 1. 配置 ---
warnings.filterwarnings('ignore')

# 設置日誌
log_dir = './logs' # 假設數據中心項目有 logs 目錄
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/build_ff3_full_market_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 2. 核心路徑 (基於《數據詞典》v1.2) ---
#
DATA_CENTER_PATH = '/home/zy/桌面/数据中心/quant_data_center'

# "原材料"路徑
#
PATH_STOCK_BASIC = os.path.join(DATA_CENTER_PATH, 'stock_basic.parquet')
#
PATH_DAILY_HFQ_DIR = os.path.join(DATA_CENTER_PATH, 'stock/daily_hfq')
#
PATH_DAILY_BASIC = os.path.join(DATA_CENTER_PATH, 'stock/daily_basic/daily_basic_all.parquet')
#
PATH_RFR = os.path.join(DATA_CENTER_PATH, 'factors/risk_free/rfr_daily.parquet')
#
PATH_BALANCE_DIR = os.path.join(DATA_CENTER_PATH, 'stock/financial_tables/balancesheet')

# "成品"輸出路徑 (新)
OUTPUT_DIR_FF3_FULL = os.path.join(DATA_CENTER_PATH, 'factors/fama_french_3')
OUTPUT_PATH_FF3_FULL = os.path.join(OUTPUT_DIR_FF3_FULL, 'ff_3_factors_daily.parquet')

logger.info(f"數據中心路徑: {DATA_CENTER_PATH}")
logger.info(f"輸出路徑: {OUTPUT_PATH_FF3_FULL}")

# --- 3. 核心參數 ---
START_DATE = pd.to_datetime("2012-01-01") # TTM 計算需要數據
START_LOOP_DATE = pd.to_datetime("2013-01-01") # 回測循環開始日期
END_LOOP_DATE = pd.to_datetime(datetime.now().strftime('%Y-%m-01')) - relativedelta(days=1) # 上个月底

N_SIZE_BUCKETS = 2  # 規模分桶數 (S, B)
N_VALUE_BUCKETS = 3 # 價值分桶數 (L, M, H)

# --- 4. 數據加載與預處理 ---

def load_all_financials():
    """
    加載 FF3 所需的財務數據 (資產負債表)
    """
    logger.info("  正在加載所有資產負債表 (BalanceSheet)...")
    
    #
    df_balance = ds.dataset(PATH_BALANCE_DIR, format='parquet').to_table(
        columns=['ts_code', 'ann_date', 'end_date', 'total_assets', 'total_liab']
    ).to_pandas()

    # 統一日期格式
    df_balance['ann_date'] = pd.to_datetime(df_balance['ann_date'], format='%Y%m%d')
    df_balance['end_date'] = pd.to_datetime(df_balance['end_date'], format='%Y%m%d')

    # 計算 Book Equity (淨資產 B)
    df_balance = df_balance.sort_values(by=['ts_code', 'end_date']).drop_duplicates()
    df_balance['B_latest'] = df_balance['total_assets'] - df_balance['total_liab'] 
    
    # 僅保留 TTM 計算所需的核心字段
    df_financials = df_balance[['ts_code', 'ann_date', 'end_date', 'B_latest']]
    
    # 過濾 B > 0
    df_financials = df_financials[df_financials['B_latest'] > 0]
    
    logger.info(f"  財務數據預處理完成，共 {len(df_financials)} 條有效 B>0 記錄")
    return df_financials

def load_all_market_data(start_date, end_date):
    """
    加載 *所有* 市場數據 (K線/市值) 並預計算收益率
    """
    logger.info("  正在加載所有市場數據 (HFQ K線, Daily Basic)...")
    
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    
    # 1. 加載日K線(後復權)
    #
    hfq_dataset = ds.dataset(PATH_DAILY_HFQ_DIR, format='parquet')
    df_hfq = hfq_dataset.to_table(
        columns=['ts_code', 'trade_date', 'close'],
        filter=(ds.field('trade_date') >= start_date_str) & 
               (ds.field('trade_date') <= end_date_str)
    ).to_pandas()
    df_hfq['trade_date'] = pd.to_datetime(df_hfq['trade_date'], format='%Y%m%d')

    # 2. 計算日收益率 (Return)
    df_hfq = df_hfq.sort_values(by=['ts_code', 'trade_date'])
    df_hfq['return'] = df_hfq.groupby('ts_code')['close'].pct_change()
    df_hfq = df_hfq[['ts_code', 'trade_date', 'return']].dropna()
    
    # 3. 加載每日市值 (MV)
    #
    df_basic_daily = pd.read_parquet(
        PATH_DAILY_BASIC,
        columns=['ts_code', 'trade_date', 'total_mv'],
        filters=[('trade_date', '>=', start_date_str), ('trade_date', '<=', end_date_str)]
    )
    df_basic_daily['trade_date'] = pd.to_datetime(df_basic_daily['trade_date'], format='%Y%m%d')
    # MV單位為万元 -> 元
    df_basic_daily['total_mv'] = df_basic_daily['total_mv'] * 10000 
    
    # 4. 合併收益率和市值
    df_market = pd.merge(
        df_hfq,
        df_basic_daily,
        on=['ts_code', 'trade_date'],
        how='inner'
    )
    
    # 5. 準備 *昨日* 市值 (prev_mv) 用於加權
    df_market = df_market.sort_values(by=['ts_code', 'trade_date'])
    df_market['prev_mv'] = df_market.groupby('ts_code')['total_mv'].shift(1)
    
    df_market = df_market.dropna(subset=['return', 'prev_mv'])
    
    logger.info(f"  市場數據加載和預處理完成，共 {len(df_market)} 條日度記錄")
    return df_market

# --- 5. 月度排序與計算 ---

def get_sorting_snapshot_monthly(month_end, df_financials, df_market_daily, df_basic_info):
    """
    (每月執行) 獲取月度 TTM 財務快照和當期市值
    """
    
    # 1. 篩選 *截至* month_end *已公告* 的 *最新* 財務數據
    # 嚴格遵守 ann_date
    df_fin_available = df_financials[df_financials['ann_date'] <= month_end]
    df_fin_snapshot = df_fin_available.sort_values('end_date').groupby('ts_code').last()
    
    # 2. 獲取 *當月最後一天* 的市值 (MV)
    #
    df_mv_snapshot = df_market_daily[df_market_daily['trade_date'] <= month_end]
    df_mv_snapshot = df_mv_snapshot.sort_values('trade_date').groupby('ts_code').last()

    # 3. 合並財務快照與市值快照
    df_snapshot = pd.merge(
        df_fin_snapshot[['B_latest']],
        df_mv_snapshot[['total_mv']],
        left_index=True,
        right_index=True,
        how='inner' # 必須同時有財務數據和市值
    )

    # 4. **關鍵邏輯：v5.3/v5.4 全市場**
    # 我們只過濾上市日期，不過濾行業
    #
    # 注意：df_snapshot的索引是ts_code，df_basic_info需要先设置索引
    df_basic_info_indexed = df_basic_info.set_index('ts_code') if 'ts_code' in df_basic_info.columns else df_basic_info
    df_snapshot = pd.merge(
        df_snapshot,
        df_basic_info_indexed[['list_date']], # 只需 list_date
        left_index=True,
        right_index=True,
        how='left'
    )
    
    # 過濾上市不足 1 年的股票 (FF 標準)
    min_list_date = (month_end - relativedelta(years=1))
    # 如果list_date是字符串格式，先转换为日期
    if df_snapshot['list_date'].dtype == 'object':
        df_snapshot['list_date'] = pd.to_datetime(df_snapshot['list_date'], format='%Y%m%d', errors='coerce')
    df_snapshot = df_snapshot[df_snapshot['list_date'] < min_list_date]
    
    # 5. 計算當期排序因子 B/M, Size
    df_snapshot['B/M'] = df_snapshot['B_latest'] / df_snapshot['total_mv']
    df_snapshot['Size'] = df_snapshot['total_mv']
    
    # 6. 過濾 B > 0 且因子有效的
    df_snapshot = df_snapshot[
        (df_snapshot['B_latest'] > 0) &
        (df_snapshot['B/M'].notna()) &
        (df_snapshot['Size'] > 0)
    ]
    # 處理極端值
    df_snapshot['B/M'] = df_snapshot['B/M'].clip(df_snapshot['B/M'].quantile(0.01), df_snapshot['B/M'].quantile(0.99))
    
    return df_snapshot[['Size', 'B/M']]

def get_portfolios_monthly(df_snapshot):
    """
    (每月執行) 執行 FF3 經典 2x3 排序
    """
    
    # 1. 規模斷點 (Size Breakpoint) - 中位數
    #
    mv_median = df_snapshot['Size'].median()
    df_snapshot['size_group'] = np.where(df_snapshot['Size'] <= mv_median, 'S', 'B')
    
    # 2. 價值斷点 (Value Breakpoints) - B/M 30%, 70%
    #
    bm_30 = df_snapshot['B/M'].quantile(0.3)
    bm_70 = df_snapshot['B/M'].quantile(0.7)
    df_snapshot['value_group'] = np.where(
        df_snapshot['B/M'] <= bm_30, 'L',
        np.where(df_snapshot['B/M'] > bm_70, 'H', 'M')
    )

    # 3. 構建 6 個投資組合 (S/L, S/M, S/H, B/L, B/M, B/H)
    portfolios = {}
    for s_group in ['S', 'B']:
        for v_group in ['L', 'M', 'H']:
            group_name = f"{s_group}{v_group}"
            stocks = df_snapshot[
                (df_snapshot['size_group'] == s_group) & 
                (df_snapshot['value_group'] == v_group)
            ].index
            portfolios[group_name] = stocks
    
    # 4. MKT 組合 (全體)
    portfolios['MKT'] = df_snapshot.index

    return portfolios

def calculate_daily_factors(df_month_market, portfolios, df_rfr_month):
    """
    (每月執行) 計算下一個月每一天的 FF3 因子收益
    """
    
    # 1. 將日度數據轉換為 pivot 格式
    df_returns = df_month_market.pivot(index='trade_date', columns='ts_code', values='return')
    df_prev_mv = df_month_market.pivot(index='trade_date', columns='ts_code', values='prev_mv')

    def get_vw_return(stocks_idx, date):
        """(Helper) 計算市值加權收益"""
        if stocks_idx.empty:
            return 0
        
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
    
    for date in df_returns.index: # 遍歷下個月的所有交易日
        
        # 獲取 6 個組合的收益
        R_SL = get_vw_return(portfolios['SL'], date)
        R_SM = get_vw_return(portfolios['SM'], date)
        R_SH = get_vw_return(portfolios['SH'], date)
        R_BL = get_vw_return(portfolios['BL'], date)
        R_BM = get_vw_return(portfolios['BM'], date)
        R_BH = get_vw_return(portfolios['BH'], date)

        # 1. SMB (Small Minus Big)
        R_S = (R_SL + R_SM + R_SH) / 3.0
        R_B = (R_BL + R_BM + R_BH) / 3.0
        SMB = R_S - R_B
        
        # 2. HML (High Minus Low)
        R_H = (R_SH + R_BH) / 2.0
        R_L = (R_SL + R_BL) / 2.0
        HML = R_H - R_L
        
        # 3. MKT_RF (Market Risk Premium)
        MKT = get_vw_return(portfolios['MKT'], date)
        #
        RF = df_rfr_month.loc[date, 'rf']
        MKT_RF = MKT - RF
        
        daily_factors.append({
            'trade_date_dt': date,
            'MKT_RF': MKT_RF,
            'SMB': SMB,
            'HML': HML
        })

    df_factors = pd.DataFrame(daily_factors).set_index('trade_date_dt')
    return df_factors

# --- 6. 主執行器 ---

def main():
    """
    主執行函數 (v5.3 - 任務 1.2)
    """
    logger.info("=" * 70)
    logger.info("TrueNorth v5.3 - 任務 1.2: 構建\"全市場FF3因子\" (月度TTM版)")
    logger.info("關鍵邏輯: 包含金融股, 2x3 (Size x B/M) 排序")
    logger.info("=" * 70)
    
    start_time_total = time.time()
    
    # --- A. 一次性預加載所有"原材料" ---
    logger.info("步驟 1: 一次性預加載所有\"原材料\"...")
    
    #
    # **關鍵**：加載基礎信息，但 *不* 過濾金融股
    df_basic_info = pd.read_parquet(PATH_STOCK_BASIC, columns=['ts_code', 'industry', 'list_date'])
    logger.info(f"  已加載 {len(df_basic_info)} 隻股票基礎信息 (全市場)")
    
    #
    df_rfr_all = pd.read_parquet(PATH_RFR)
    df_rfr_all['trade_date'] = pd.to_datetime(df_rfr_all['trade_date'], format='%Y%m%d')
    # 轉為小數
    df_rfr_all['rf'] = df_rfr_all['rf'] / 100.0 
    df_rfr_all = df_rfr_all.set_index('trade_date').sort_index()

    #
    df_financials = load_all_financials()
    
    #
    df_market_daily = load_all_market_data(START_DATE, END_LOOP_DATE)
    
    logger.info("✅ \"原材料\" 預加載完畢!")
    
    all_factors_list = []
    
    # --- B. 按月循環構建 ---
    logger.info("\n步驟 2: 開始按月度循環構建因子...")
    
    loop_dates = pd.date_range(START_LOOP_DATE, END_LOOP_DATE, freq='M')
    
    for month_end in loop_dates:
        logger.info(f"--- 處理 {month_end.strftime('%Y-%m')} ---")
        
        # 1. 獲取月度快照 (B/M, Size)
        df_snapshot = get_sorting_snapshot_monthly(month_end, df_financials, df_market_daily, df_basic_info)
        
        if df_snapshot.empty:
            logger.warning(f"  {month_end.date()} 無有效數據進行分組，跳過")
            continue
            
        # 2. 執行 2x3 分組
        portfolios = get_portfolios_monthly(df_snapshot)
        
        # 3. 篩選下個月的日度數據
        period_start_date = month_end + relativedelta(days=1)
        period_end_date = month_end + relativedelta(months=1)
        
        df_month_market = df_market_daily[
            (df_market_daily['trade_date'] >= period_start_date) &
            (df_market_daily['trade_date'] <= period_end_date)
        ].copy()
        
        df_rfr_month = df_rfr_all[
            (df_rfr_all.index >= period_start_date) &
            (df_rfr_all.index <= period_end_date)
        ].copy()
        
        if df_month_market.empty or df_rfr_month.empty:
            logger.warning(f"  {month_end.strftime('%Y-%m')} 調倉期內無有效K線或無風險利率數據，跳過")
            continue

        # 4. 計算下個月的每日因子
        df_factors = calculate_daily_factors(df_month_market, portfolios, df_rfr_month)
        all_factors_list.append(df_factors)
        
    # --- C. 匯總並存儲 ---
    if not all_factors_list:
        logger.error("錯誤：未能計算任何因子數據，程序終止。")
        return

    logger.info("\n步驟 3: 匯總所有因子數據並存儲...")
    final_ff3_df = pd.concat(all_factors_list).sort_index()
    
    # 轉換為 %
    final_ff3_df = final_ff3_df * 100
    
    # 轉換索引 'trade_date' 為 YYYYMMDD 字符串
    #
    final_ff3_df.index.name = 'trade_date_dt'
    final_ff3_df = final_ff3_df.reset_index()
    final_ff3_df['trade_date'] = final_ff3_df['trade_date_dt'].dt.strftime('%Y%m%d')
    
    # 規範字段
    final_ff3_df = final_ff3_df[['trade_date', 'MKT_RF', 'SMB', 'HML']]
    
    # 創建新目錄
    if not os.path.exists(OUTPUT_DIR_FF3_FULL):
        os.makedirs(OUTPUT_DIR_FF3_FULL)
        logger.info(f"  已創建新目錄: {OUTPUT_DIR_FF3_FULL}")
        
    # 存儲到數據中心
    final_ff3_df.to_parquet(OUTPUT_PATH_FF3_FULL, engine='pyarrow', index=False)
    
    end_time_total = time.time()
    logger.info("\n" + "=" * 70)
    logger.info("🎉 TrueNorth v5.3 - 任務 1.2: \"全市場FF3因子\" 構建成功!")
    logger.info(f"總計 {len(final_ff3_df)} 條記錄已保存至:")
    logger.info(f"{OUTPUT_PATH_FF3_FULL}")
    logger.info(f"總耗時: {(end_time_total - start_time_total) / 60:.2f} 分鐘")
    logger.info("=" * 70)

if __name__ == "__main__":
    main()

