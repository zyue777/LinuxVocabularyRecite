# /home/zy/桌面/数据中心/build_ch3_factors.py
#
# TrueNorth v7.0 - 階段一: 構建"中國版三因子" (CH-3)
# 核心邏輯 (CQR): 
# 1. (全市場) 包含金融股
# 2. (剔除殼價值) 剔除市值最小的 30% 股票
# 3. (價值因子) 使用盈利收益率 (E/P)
# 4. (排序) 2x3 (Size x E/P) 排序

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
        logging.FileHandler(f'logs/build_ch3_factors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 2. 核心路徑 (基於《數據詞典》) ---
DATA_CENTER_PATH = '/home/zy/桌面/数据中心/quant_data_center'

# "原材料"路徑
PATH_STOCK_BASIC = os.path.join(DATA_CENTER_PATH, 'stock_basic.parquet')
PATH_DAILY_HFQ_DIR = os.path.join(DATA_CENTER_PATH, 'stock/daily_hfq')
PATH_DAILY_BASIC = os.path.join(DATA_CENTER_PATH, 'stock/daily_basic/daily_basic_all.parquet')
PATH_RFR = os.path.join(DATA_CENTER_PATH, 'factors/risk_free/rfr_daily.parquet')

# "成品"輸出路徑 (新)
OUTPUT_DIR_CH3 = os.path.join(DATA_CENTER_PATH, 'factors/ch_3_factors')
OUTPUT_PATH_CH3 = os.path.join(OUTPUT_DIR_CH3, 'ch_3_factors_daily.parquet')

logger.info(f"數據中心路徑: {DATA_CENTER_PATH}")
logger.info(f"CH-3 輸出路徑: {OUTPUT_PATH_CH3}")

# --- 3. 核心參數 ---
START_DATE = pd.to_datetime("2012-01-01")  # 需要預留數據
START_LOOP_DATE = pd.to_datetime("2013-01-01")  # 回測循環開始日期（從2013年開始）
END_LOOP_DATE = pd.to_datetime(datetime.now().strftime('%Y-%m-01')) - relativedelta(days=1)  # 上个月底

SHELL_VALUE_CUTOFF = 0.3  # 剔除市值最小的 30%
VALUE_FACTOR_BREAKPOINTS = [0.3, 0.7]  # 30%/70% 分位數

# --- 4. 數據加載與預處理 ---

def load_all_market_data(start_date, end_date):
    """
    加載 *所有* 市場數據 (K線/市值/PE)
    """
    logger.info("  正在加載所有市場數據 (HFQ K線, Daily Basic)...")
    
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    
    # 1. 加載日K線(後復權)
    hfq_dataset = ds.dataset(PATH_DAILY_HFQ_DIR, format='parquet')
    df_hfq = hfq_dataset.to_table(
        columns=['ts_code', 'trade_date', 'close'],
        filter=(ds.field('trade_date') >= start_date_str) & 
               (ds.field('trade_date') <= end_date_str)
    ).to_pandas()
    df_hfq['trade_date'] = pd.to_datetime(df_hfq['trade_date'], format='%Y%m%d')
    df_hfq = df_hfq.sort_values(by=['ts_code', 'trade_date'])
    df_hfq['return'] = df_hfq.groupby('ts_code')['close'].pct_change()
    df_hfq = df_hfq[['ts_code', 'trade_date', 'return']].dropna()
    
    # 2. 加載每日基礎指標 (MV 和 PE)
    df_basic_daily = pd.read_parquet(
        PATH_DAILY_BASIC,
        columns=['ts_code', 'trade_date', 'total_mv', 'pe'],
        filters=[('trade_date', '>=', start_date_str), ('trade_date', '<=', end_date_str)]
    )
    df_basic_daily['trade_date'] = pd.to_datetime(df_basic_daily['trade_date'], format='%Y%m%d')
    # MV單位為万元 -> 元
    df_basic_daily['total_mv'] = df_basic_daily['total_mv'] * 10000 
    
    # 3. 合併收益率和市值
    df_market = pd.merge(
        df_hfq,
        df_basic_daily,
        on=['ts_code', 'trade_date'],
        how='inner'
    )
    
    # 4. 準備 *昨日* 市值 (prev_mv) 用於加權
    df_market = df_market.sort_values(by=['ts_code', 'trade_date'])
    df_market['prev_mv'] = df_market.groupby('ts_code')['total_mv'].shift(1)
    
    df_market = df_market.dropna(subset=['return', 'prev_mv', 'pe', 'total_mv'])
    
    logger.info(f"  市場數據加載和預處理完成，共 {len(df_market)} 條日度記錄")
    return df_market

# --- 5. 月度排序與計算 ---

def get_sorting_snapshot_monthly(month_end, df_market_daily, df_basic_info):
    """
    (每月執行) 獲取月度快照 (Size, E/P)
    """
    
    # 1. 獲取 *當月最後一天* 的市值 (MV) 和 市盈率 (PE)
    df_snapshot = df_market_daily[df_market_daily['trade_date'] <= month_end]
    df_snapshot = df_snapshot.sort_values('trade_date').groupby('ts_code').last().reset_index()

    # 2. **關鍵邏輯 v7.0**: 全市場 (不過濾金融股)
    df_basic_info_indexed = df_basic_info.set_index('ts_code') if 'ts_code' in df_basic_info.columns else df_basic_info
    df_snapshot = pd.merge(
        df_snapshot,
        df_basic_info_indexed[['list_date']],
        left_on='ts_code',
        right_index=True,
        how='left'
    )
    
    # 過濾上市不足 1 年的股票 (FF 標準)
    min_list_date = (month_end - relativedelta(years=1))
    # 如果list_date是字符串格式，先转换为日期
    if df_snapshot['list_date'].dtype == 'object':
        df_snapshot['list_date'] = pd.to_datetime(df_snapshot['list_date'], format='%Y%m%d', errors='coerce')
    df_snapshot = df_snapshot[df_snapshot['list_date'] < min_list_date]
    
    # 3. 計算當期排序因子 Size 和 E/P
    df_snapshot['Size'] = df_snapshot['total_mv']
    # CH-3 使用 E/P (盈利收益率)
    df_snapshot['E/P'] = 1.0 / df_snapshot['pe'] 
    
    # 4. 過濾無效數據 (PE 為負或極小)
    df_snapshot = df_snapshot[
        (df_snapshot['Size'] > 0) &
        (df_snapshot['pe'] > 0) &  # 確保 E/P 為正
        (df_snapshot['E/P'].notna())
    ]
    
    # 5. **關鍵邏輯 v7.0：剔除"殼價值"**
    # 剔除市值最小的 30% 股票
    original_count = len(df_snapshot)
    size_cutoff = df_snapshot['Size'].quantile(SHELL_VALUE_CUTOFF) 
    df_snapshot = df_snapshot[df_snapshot['Size'] > size_cutoff]
    
    logger.info(f"  剔除殼價值後，剩餘 {len(df_snapshot)} 隻股票 (原 {original_count} 隻，剔除 {original_count - len(df_snapshot)} 隻)")
    
    # 處理極端值
    df_snapshot['E/P'] = df_snapshot['E/P'].clip(
        df_snapshot['E/P'].quantile(0.01), 
        df_snapshot['E/P'].quantile(0.99)
    )
    
    return df_snapshot[['ts_code', 'Size', 'E/P']]

def get_portfolios_monthly(df_snapshot):
    """
    (每月執行) 執行 CH-3 2x3 排序 (Size x E/P)
    """
    
    # 1. 規模斷點 (Size Breakpoint) - 中位數
    # (注意：這是在 *剔除最小30%後* 的股票池中取中位數)
    mv_median = df_snapshot['Size'].median()
    df_snapshot['size_group'] = np.where(df_snapshot['Size'] <= mv_median, 'S', 'B')
    
    # 2. 價值斷點 (Value Breakpoints) - E/P 30%, 70%
    ep_30 = df_snapshot['E/P'].quantile(VALUE_FACTOR_BREAKPOINTS[0])
    ep_70 = df_snapshot['E/P'].quantile(VALUE_FACTOR_BREAKPOINTS[1])
    # Low E/P = Growth, High E/P = Value
    df_snapshot['value_group'] = np.where(
        df_snapshot['E/P'] <= ep_30, 'L',  # Low E/P (Growth)
        np.where(df_snapshot['E/P'] > ep_70, 'H', 'M')  # High E/P (Value)
    )

    # 3. 構建 6 個投資組合 (SL, SM, SH, BL, BM, BH)
    portfolios = {}
    for s_group in ['S', 'B']:
        for v_group in ['L', 'M', 'H']:
            group_name = f"{s_group}{v_group}"
            stocks = df_snapshot[
                (df_snapshot['size_group'] == s_group) & 
                (df_snapshot['value_group'] == v_group)
            ]['ts_code'].values
            portfolios[group_name] = stocks
    
    # 4. MKT 組合 (全體，用於計算市場收益)
    portfolios['MKT'] = df_snapshot['ts_code'].values

    return portfolios

def calculate_daily_factors(df_month_market, portfolios, df_rfr_month):
    """
    (每月執行) 計算下一個月每一天的 CH-3 因子收益 (SMB_CH, VMG_CH)
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
    
    for date in df_returns.index:  # 遍歷下個月的所有交易日
        
        # 獲取 6 個組合的收益
        R_SL = get_vw_return(portfolios['SL'], date)
        R_SM = get_vw_return(portfolios['SM'], date)
        R_SH = get_vw_return(portfolios['SH'], date)
        R_BL = get_vw_return(portfolios['BL'], date)
        R_BM = get_vw_return(portfolios['BM'], date)
        R_BH = get_vw_return(portfolios['BH'], date)

        # 1. SMB_CH (Small Minus Big, China ver.)
        R_S = (R_SL + R_SM + R_SH) / 3.0
        R_B = (R_BL + R_BM + R_BH) / 3.0
        SMB_CH = R_S - R_B
        
        # 2. VMG_CH (Value Minus Growth, China ver.)
        R_H = (R_SH + R_BH) / 2.0  # High E/P (Value)
        R_L = (R_SL + R_BL) / 2.0  # Low E/P (Growth)
        VMG_CH = R_H - R_L
        
        # 3. MKT_RF (Market Risk Premium)
        MKT = get_vw_return(portfolios['MKT'], date)
        # 獲取無風險利率
        if date in df_rfr_month.index:
            RF = df_rfr_month.loc[date, 'rf']
        else:
            # 如果當日沒有無風險利率，使用前一個有效值
            RF = df_rfr_month.loc[df_rfr_month.index <= date, 'rf'].iloc[-1] if len(df_rfr_month[df_rfr_month.index <= date]) > 0 else 0
        
        MKT_RF = MKT - RF
        
        daily_factors.append({
            'trade_date_dt': date,
            'MKT_RF': MKT_RF,
            'SMB_CH': SMB_CH,
            'VMG_CH': VMG_CH
        })

    df_factors = pd.DataFrame(daily_factors).set_index('trade_date_dt')
    return df_factors

# --- 6. 主執行器 ---

def main():
    """
    主執行函數 (v7.0 - 階段一)
    """
    logger.info("=" * 70)
    logger.info("TrueNorth v7.0 - 階段一: 構建\"中國版三因子\" (CH-3)")
    logger.info("關鍵邏輯: 包含金融股, 剔除殼價值(最小30%), 使用E/P, 2x3 (Size x E/P) 排序")
    logger.info("=" * 70)
    
    start_time_total = time.time()
    
    # --- A. 一次性預加載所有"原材料" ---
    logger.info("步驟 1: 一次性預加載所有\"原材料\"...")
    
    df_basic_info = pd.read_parquet(PATH_STOCK_BASIC, columns=['ts_code', 'industry', 'list_date'])
    logger.info(f"  已加載 {len(df_basic_info)} 隻股票基礎信息 (全市場)")
    
    df_rfr_all = pd.read_parquet(PATH_RFR)
    df_rfr_all['trade_date'] = pd.to_datetime(df_rfr_all['trade_date'], format='%Y%m%d')
    # 轉為小數
    df_rfr_all['rf'] = df_rfr_all['rf'] / 100.0 
    df_rfr_all = df_rfr_all.set_index('trade_date').sort_index()

    df_market_daily = load_all_market_data(START_DATE, END_LOOP_DATE)
    
    logger.info("✅ \"原材料\" 預加載完畢!")
    
    all_factors_list = []
    
    # --- B. 按月循環構建 ---
    logger.info("\n步驟 2: 開始按月度循環構建因子...")
    
    loop_dates = pd.date_range(START_LOOP_DATE, END_LOOP_DATE, freq='M')
    
    for month_end in loop_dates:
        logger.info(f"--- 處理 {month_end.strftime('%Y-%m')} ---")
        
        # 1. 獲取月度快照 (Size, E/P)
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
    final_ch3_df = pd.concat(all_factors_list).sort_index()
    
    # 轉換為 %
    final_ch3_df = final_ch3_df * 100
    
    # 轉換索引 'trade_date' 為 YYYYMMDD 字符串
    final_ch3_df = final_ch3_df.reset_index()
    final_ch3_df['trade_date'] = final_ch3_df['trade_date_dt'].dt.strftime('%Y%m%d')
    
    # 規範字段
    final_ch3_df = final_ch3_df[['trade_date', 'MKT_RF', 'SMB_CH', 'VMG_CH']]
    
    # 創建新目錄
    if not os.path.exists(OUTPUT_DIR_CH3):
        os.makedirs(OUTPUT_DIR_CH3)
        logger.info(f"  已創建新目錄: {OUTPUT_DIR_CH3}")
        
    # 存儲到數據中心
    final_ch3_df.to_parquet(OUTPUT_PATH_CH3, engine='pyarrow', index=False)
    
    end_time_total = time.time()
    logger.info("\n" + "=" * 70)
    logger.info("🎉 TrueNorth v7.0 - 階段一: \"中國版三因子\" (CH-3) 構建成功!")
    logger.info(f"總計 {len(final_ch3_df)} 條記錄已保存至:")
    logger.info(f"{OUTPUT_PATH_CH3}")
    logger.info(f"總耗時: {(end_time_total - start_time_total) / 60:.2f} 分鐘")
    logger.info("=" * 70)

if __name__ == "__main__":
    # 假設此腳本在數據中心項目根目錄運行
    logger.info(f"當前工作目錄: {os.getcwd()}")
    main()

