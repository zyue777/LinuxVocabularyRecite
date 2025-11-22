# /home/zy/桌面/数据中心/build_custom_factors.py
#
# TrueNorth v6.0 - 任務 1.1: 構建 UMD (動量) 和 LIQ (流動性) 因子
# 核心邏輯: 
# 1. (全市場) 包含金融股
# 2. (月度TTM) 使用月度財報和市值
# 3. (UMD) 2x3 (Size x 12-1MOM) 排序
# 4. (LIQ) 2x3 (Size x Turnover Rate) 排序

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
log_dir = './logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/build_custom_factors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
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

# "成品"輸出路徑 (新)
OUTPUT_DIR_CUSTOM = os.path.join(DATA_CENTER_PATH, 'factors/custom')
OUTPUT_PATH_UMD = os.path.join(OUTPUT_DIR_CUSTOM, 'umd_daily.parquet')
OUTPUT_PATH_LIQ = os.path.join(OUTPUT_DIR_CUSTOM, 'liq_daily.parquet')

logger.info(f"數據中心路徑: {DATA_CENTER_PATH}")
logger.info(f"UMD 輸出路徑: {OUTPUT_PATH_UMD}")
logger.info(f"LIQ 輸出路徑: {OUTPUT_PATH_LIQ}")

# --- 3. 核心參數 ---
START_DATE = pd.to_datetime("2012-01-01") # 需要預留12個月用於MOM計算
START_LOOP_DATE = pd.to_datetime("2013-01-01") # 回測循環開始日期
END_LOOP_DATE = pd.to_datetime(datetime.now().strftime('%Y-%m-01')) - relativedelta(days=1) # 上个月底

# --- 4. 數據加載與預處理 ---

def load_all_market_data(start_date, end_date):
    """
    加載 *所有* 市場數據 (K線/市值/換手率)，並預計算MOM和LIQ因子
    """
    logger.info("  正在加載所有市場數據 (HFQ K線, Daily Basic)...")
    
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    
    # 需要預留12個月數據用於MOM計算
    mom_start_date = (start_date - relativedelta(months=13)).strftime('%Y%m%d')
    
    # 1. 加載日K線(後復權) - 需要更早的數據用於MOM計算
    #
    hfq_dataset = ds.dataset(PATH_DAILY_HFQ_DIR, format='parquet')
    df_hfq = hfq_dataset.to_table(
        columns=['ts_code', 'trade_date', 'close'],
        filter=(ds.field('trade_date') >= mom_start_date) & 
               (ds.field('trade_date') <= end_date_str)
    ).to_pandas()
    df_hfq['trade_date'] = pd.to_datetime(df_hfq['trade_date'], format='%Y%m%d')
    df_hfq = df_hfq.sort_values(by=['ts_code', 'trade_date'])
    
    # 2. 計算日收益率 (Return)
    df_hfq['return'] = df_hfq.groupby('ts_code')['close'].pct_change()
    
    # 3. 加載每日基礎指標 (MV 和 換手率)
    #
    df_basic_daily = pd.read_parquet(
        PATH_DAILY_BASIC,
        columns=['ts_code', 'trade_date', 'total_mv', 'turnover_rate'],
        filters=[('trade_date', '>=', start_date_str), ('trade_date', '<=', end_date_str)]
    )
    df_basic_daily['trade_date'] = pd.to_datetime(df_basic_daily['trade_date'], format='%Y%m%d')
    # MV單位為万元 -> 元
    df_basic_daily['total_mv'] = df_basic_daily['total_mv'] * 10000 
    
    # 4. 合併收益率和市值
    df_market = pd.merge(
        df_hfq[['ts_code', 'trade_date', 'return']],
        df_basic_daily,
        on=['ts_code', 'trade_date'],
        how='inner'
    )
    
    # 5. 準備 *昨日* 市值 (prev_mv) 用於加權
    df_market = df_market.sort_values(by=['ts_code', 'trade_date'])
    df_market['prev_mv'] = df_market.groupby('ts_code')['total_mv'].shift(1)
    
    df_market = df_market.dropna(subset=['return', 'prev_mv', 'turnover_rate'])
    
    # 6. 預計算MOM因子 (12-1月動量)
    logger.info("  預計算 12-1 月動量 (MOM)...")
    df_prices = df_hfq.pivot(index='trade_date', columns='ts_code', values='close')
    
    # t-1 月價格 (約21個交易日)
    mom_end_prices = df_prices.shift(21)
    # t-12 月價格 (約252個交易日)
    mom_start_prices = df_prices.shift(252)
    
    # 計算12-1月動量: (P_t-1 / P_t-12) - 1
    df_mom = (mom_end_prices / mom_start_prices) - 1
    df_mom = df_mom.stack(dropna=False).reset_index()
    df_mom.columns = ['trade_date', 'ts_code', 'MOM']
    
    # 7. 預計算LIQ因子 (21日均換手率)
    logger.info("  預計算月均換手率 (LIQ)...")
    df_market_sorted = df_market.set_index('trade_date').sort_index()
    df_liq = df_market_sorted.groupby('ts_code')['turnover_rate'].rolling(
        window=21, min_periods=15
    ).mean().reset_index()
    df_liq.columns = ['ts_code', 'trade_date', 'LIQ_MA21']
    df_liq['trade_date'] = pd.to_datetime(df_liq['trade_date'])
    
    # 8. 合併所有因子
    df_market = pd.merge(df_market, df_mom, on=['trade_date', 'ts_code'], how='left')
    df_market = pd.merge(df_market, df_liq, on=['trade_date', 'ts_code'], how='left')
    
    # 只保留計算期間的數據
    df_market = df_market[df_market['trade_date'] >= start_date]

    logger.info(f"  市場數據加載和預處理完成，共 {len(df_market)} 條日度記錄")
    return df_market

# --- 5. 月度排序與計算 ---

def get_sorting_snapshot_monthly(month_end, df_market_daily, df_basic_info):
    """
    (每月執行) 獲取月度快照 (Size, MOM, LIQ)
    """
    
    # 1. 獲取 *當月最後一天* 的市值和因子數據
    #
    df_snapshot = df_market_daily[df_market_daily['trade_date'] <= month_end].copy()
    df_snapshot = df_snapshot.sort_values('trade_date').groupby('ts_code').last().reset_index()

    # 2. **關鍵邏輯：v6.0 全市場**
    #
    df_basic_info_indexed = df_basic_info.set_index('ts_code')
    df_snapshot = pd.merge(
        df_snapshot,
        df_basic_info_indexed[['list_date']],
        left_on='ts_code',
        right_index=True,
        how='left'
    )
    
    # 過濾上市不足 1 年的股票 (FF 標準)
    min_list_date = (month_end - relativedelta(years=1))
    if df_snapshot['list_date'].dtype == 'object':
        df_snapshot['list_date'] = pd.to_datetime(df_snapshot['list_date'], format='%Y%m%d', errors='coerce')
    df_snapshot = df_snapshot[df_snapshot['list_date'] < min_list_date]
    
    # 3. 獲取當期排序因子
    df_snapshot['Size'] = df_snapshot['total_mv']
    df_snapshot['MOM'] = df_snapshot['MOM']
    df_snapshot['LIQ'] = df_snapshot['LIQ_MA21']  # 使用21日均線作為流動性代理
    
    # 4. 過濾無效數據
    df_snapshot = df_snapshot.dropna(subset=['Size', 'MOM', 'LIQ'])
    df_snapshot = df_snapshot[df_snapshot['Size'] > 0]
    
    # 處理極端值
    if len(df_snapshot) > 0:
        df_snapshot['MOM'] = df_snapshot['MOM'].clip(
            df_snapshot['MOM'].quantile(0.01), 
            df_snapshot['MOM'].quantile(0.99)
        )
        df_snapshot['LIQ'] = df_snapshot['LIQ'].clip(
            df_snapshot['LIQ'].quantile(0.01), 
            df_snapshot['LIQ'].quantile(0.99)
        )
    
    return df_snapshot[['ts_code', 'Size', 'MOM', 'LIQ']]

def get_portfolios_monthly(df_snapshot):
    """
    (每月執行) 執行 2x3 排序 (Size x MOM) 和 (Size x LIQ)
    """
    
    portfolios = {'UMD': {}, 'LIQ': {}}
    
    # --- A. UMD 因子構建 (Size x MOM) ---
    # 1. 規模斷點 (Size Breakpoint) - 中位數
    mv_median = df_snapshot['Size'].median()
    df_snapshot['size_group'] = np.where(df_snapshot['Size'] <= mv_median, 'S', 'B')
    
    # 2. 動量斷點 (MOM Breakpoints) - 30%, 70%
    mom_30 = df_snapshot['MOM'].quantile(0.3)
    mom_70 = df_snapshot['MOM'].quantile(0.7)
    df_snapshot['mom_group'] = np.where(
        df_snapshot['MOM'] <= mom_30, 'L',  # Low momentum
        np.where(df_snapshot['MOM'] > mom_70, 'H', 'M')  # High momentum, Medium
    )
    
    # 構建 6 個 UMD 組合 (SL, SM, SH, BL, BM, BH)
    for s_group in ['S', 'B']:
        for m_group in ['L', 'M', 'H']:
            group_name = f"{s_group}{m_group}"
            stocks = df_snapshot[
                (df_snapshot['size_group'] == s_group) & 
                (df_snapshot['mom_group'] == m_group)
            ]['ts_code'].values
            portfolios['UMD'][group_name] = stocks

    # --- B. LIQ 因子構建 (Size x LIQ) ---
    # 1. 流動性斷點 (LIQ Breakpoints) - 30%, 70%
    # turnover_rate (換手率): 低換手率 = Illiquid, 高換手率 = Liquid
    liq_30 = df_snapshot['LIQ'].quantile(0.3)
    liq_70 = df_snapshot['LIQ'].quantile(0.7)
    df_snapshot['liq_group'] = np.where(
        df_snapshot['LIQ'] <= liq_30, 'I',  # Illiquid (低換手率)
        np.where(df_snapshot['LIQ'] > liq_70, 'L', 'M')  # Liquid (高換手率), Medium
    )
    
    # 構建 6 個 LIQ 組合 (SI, SM, SL, BI, BM, BL)
    for s_group in ['S', 'B']:
        for l_group in ['I', 'M', 'L']:
            group_name = f"{s_group}{l_group}"
            stocks = df_snapshot[
                (df_snapshot['size_group'] == s_group) & 
                (df_snapshot['liq_group'] == l_group)
            ]['ts_code'].values
            portfolios['LIQ'][group_name] = stocks

    return portfolios

def calculate_daily_factors(df_month_market, portfolios):
    """
    (每月執行) 計算下一個月每一天的 UMD 和 LIQ 因子收益
    """
    
    # 1. 將日度數據轉換為 pivot 格式
    df_returns = df_month_market.pivot(index='trade_date', columns='ts_code', values='return')
    df_prev_mv = df_month_market.pivot(index='trade_date', columns='ts_code', values='prev_mv')

    def get_vw_return(stocks, date):
        """(Helper) 計算市值加權收益"""
        if len(stocks) == 0:
            return 0
        
        stocks_idx = pd.Index(stocks)
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
        
        # 1. UMD (Up Minus Down, or High Minus Low Momentum)
        R_SH = get_vw_return(portfolios['UMD']['SH'], date)
        R_SL = get_vw_return(portfolios['UMD']['SL'], date)
        R_BH = get_vw_return(portfolios['UMD']['BH'], date)
        R_BL = get_vw_return(portfolios['UMD']['BL'], date)
        UMD = (R_SH + R_BH) / 2.0 - (R_SL + R_BL) / 2.0
        
        # 2. LIQ (Illiquid Minus Liquid)
        # 買入低換手 (I), 賣出高換手 (L)
        R_SI = get_vw_return(portfolios['LIQ']['SI'], date)
        R_SL_liq = get_vw_return(portfolios['LIQ']['SL'], date)
        R_BI = get_vw_return(portfolios['LIQ']['BI'], date)
        R_BL_liq = get_vw_return(portfolios['LIQ']['BL'], date)
        LIQ = (R_SI + R_BI) / 2.0 - (R_SL_liq + R_BL_liq) / 2.0
        
        daily_factors.append({
            'trade_date_dt': date,
            'UMD': UMD,
            'LIQ': LIQ
        })

    df_factors = pd.DataFrame(daily_factors).set_index('trade_date_dt')
    return df_factors

# --- 6. 主執行器 ---

def main():
    """
    主執行函數 (v6.0 - 任務 1.1)
    """
    logger.info("=" * 70)
    logger.info("TrueNorth v6.0 - 任務 1.1: 構建 UMD 和 LIQ 因子 (月度TTM版)")
    logger.info("關鍵邏輯: 包含金融股, 2x3 (Size x MOM), 2x3 (Size x LIQ) 排序")
    logger.info("=" * 70)
    
    start_time_total = time.time()
    
    # --- A. 一次性預加載所有"原材料" ---
    logger.info("步驟 1: 一次性預加載所有\"原材料\"...")
    
    #
    # **關鍵**：加載基礎信息，但 *不* 過濾金融股
    df_basic_info = pd.read_parquet(PATH_STOCK_BASIC, columns=['ts_code', 'industry', 'list_date'])
    df_basic_info['list_date'] = pd.to_datetime(df_basic_info['list_date'], format='%Y%m%d')
    logger.info(f"  已加載 {len(df_basic_info)} 隻股票基礎信息 (全市場)")
    
    #
    df_market_daily = load_all_market_data(START_DATE, END_LOOP_DATE)
    
    logger.info("✅ \"原材料\" 預加載完畢!")
    
    all_umd_factors_list = []
    all_liq_factors_list = []
    
    # --- B. 按月循環構建 ---
    logger.info("\n步驟 2: 開始按月度循環構建因子...")
    
    loop_dates = pd.date_range(START_LOOP_DATE, END_LOOP_DATE, freq='ME')
    
    for month_end in loop_dates:
        logger.info(f"--- 處理 {month_end.strftime('%Y-%m')} ---")
        
        # 1. 獲取月度快照 (Size, MOM, LIQ)
        df_snapshot = get_sorting_snapshot_monthly(month_end, df_market_daily, df_basic_info)
        
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
        
        if df_month_market.empty:
            logger.warning(f"  {month_end.strftime('%Y-%m')} 調倉期內無有效K線數據，跳過")
            continue

        # 4. 計算下個月的每日因子
        df_factors = calculate_daily_factors(df_month_market, portfolios)
        
        # 分別存儲UMD和LIQ
        df_umd = df_factors[['UMD']].copy()
        df_liq = df_factors[['LIQ']].copy()
        
        all_umd_factors_list.append(df_umd)
        all_liq_factors_list.append(df_liq)
        
    # --- C. 匯總並存儲 ---
    if not all_umd_factors_list or not all_liq_factors_list:
        logger.error("錯誤：未能計算任何因子數據，程序終止。")
        return

    logger.info("\n步驟 3: 匯總所有因子數據並存儲...")
    
    # 匯總UMD因子
    final_umd_df = pd.concat(all_umd_factors_list).sort_index()
    final_umd_df = final_umd_df * 100  # 轉換為 %
    final_umd_df.index.name = 'trade_date_dt'
    final_umd_df = final_umd_df.reset_index()
    final_umd_df['trade_date'] = final_umd_df['trade_date_dt'].dt.strftime('%Y%m%d')
    final_umd_df = final_umd_df[['trade_date', 'UMD']]
    
    # 匯總LIQ因子
    final_liq_df = pd.concat(all_liq_factors_list).sort_index()
    final_liq_df = final_liq_df * 100  # 轉換為 %
    final_liq_df.index.name = 'trade_date_dt'
    final_liq_df = final_liq_df.reset_index()
    final_liq_df['trade_date'] = final_liq_df['trade_date_dt'].dt.strftime('%Y%m%d')
    final_liq_df = final_liq_df[['trade_date', 'LIQ']]
    
    # 創建新目錄
    if not os.path.exists(OUTPUT_DIR_CUSTOM):
        os.makedirs(OUTPUT_DIR_CUSTOM)
        logger.info(f"  已創建新目錄: {OUTPUT_DIR_CUSTOM}")
        
    # 存儲到數據中心
    final_umd_df.to_parquet(OUTPUT_PATH_UMD, engine='pyarrow', index=False)
    final_liq_df.to_parquet(OUTPUT_PATH_LIQ, engine='pyarrow', index=False)
    
    end_time_total = time.time()
    logger.info("\n" + "=" * 70)
    logger.info("🎉 TrueNorth v6.0 - 任務 1.1: UMD 和 LIQ 因子構建成功!")
    logger.info(f"UMD因子: {len(final_umd_df)} 條記錄已保存至: {OUTPUT_PATH_UMD}")
    logger.info(f"LIQ因子: {len(final_liq_df)} 條記錄已保存至: {OUTPUT_PATH_LIQ}")
    logger.info(f"總耗時: {(end_time_total - start_time_total) / 60:.2f} 分鐘")
    logger.info("=" * 70)

if __name__ == "__main__":
    logger.info(f"當前工作目錄: {os.getcwd()}")
    main()

