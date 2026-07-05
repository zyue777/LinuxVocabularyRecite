#!/home/zy/miniconda3/envs/dailyreport/bin/python
# -*- coding: utf-8 -*-
"""
国债收益率数据增量更新脚本
使用 AkShare 获取中美国债收益率（非 Tushare 代理接口）
策略选项14 内的国债更新走 QuantDataManager.pro.yc_cb（Tushare 代理）
"""

import sys
import pandas as pd
import akshare as ak
from pathlib import Path
from datetime import datetime

# 数据中心根目录走 config（可用 QUANT_DATA_CENTER 覆盖，与全局单一真相源一致）
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config

def update_bond_yield_incremental():
    """
    增量更新10年期国债收益率数据
    
    Returns:
        bool: 更新是否成功
    """
    print('=' * 80)
    print('国债收益率数据增量更新')
    print('=' * 80)
    
    # 数据文件路径
    data_file = Path('/home/zy/桌面/数据中心/quant_data_center/factors/macro/china_bond_yield_10y.parquet')
    backup_parquet = Path('/home/zy/桌面/数据中心/backup_china_bond_yield_10y.parquet')
    backup_csv = Path('/home/zy/桌面/数据中心/backup_china_bond_yield_10y.csv')
    
    try:
        # 定义需要更新的期限及其对应的列名和文件名
        tasks = [
            {
                'name': '10年期',
                'col': '中国国债收益率10年',
                'file': config.DATA_CENTER_PATH / 'factors' / 'macro' / 'china_bond_yield_10y.parquet',
                'ts_code': 'CN10YR.IB',
                'term': 10.0
            },
            {
                'name': '30年期',
                'col': '中国国债收益率30年',
                'file': config.DATA_CENTER_PATH / 'factors' / 'macro' / 'china_bond_yield_30y.parquet',
                'ts_code': 'CN30YR.IB',
                'term': 30.0
            }
        ]

        # 1. 从AkShare下载最新数据（一次下载，多次使用）
        print('\n正在从AkShare下载全量国债收益率数据...')
        df_raw = ak.bond_zh_us_rate()
        print(f'下载成功: {len(df_raw)} 条原始记录')
        # 调试打印列名，确认数据存在
        # print("包含的列:", df_raw.columns.tolist())

        # 遍历任务进行更新
        for task in tasks:
            print(f"\n[{task['name']}国债] 处理中...")
            
            # 检查列是否存在
            if task['col'] not in df_raw.columns:
                print(f"⚠️ 警告: 数据源中未找到 {task['col']} 列，跳过。")
                continue

            # 读取现有数据
            if task['file'].exists():
                df_existing = pd.read_parquet(task['file'])
                latest_date = df_existing['trade_date'].max()
                print(f'  现有数据: {len(df_existing)} 条，最新日期: {latest_date}')
            else:
                df_existing = None
                latest_date = '20100101'
                print('  未找到现有数据，将生成全量历史数据')

            # 处理新数据
            df_new_raw = df_raw[['日期', task['col']]].copy()
            df_new_raw.columns = ['trade_date', 'yield']
            
            # 转换日期格式
            df_new_raw['trade_date'] = pd.to_datetime(df_new_raw['trade_date']).dt.strftime('%Y%m%d')
            
            # 转换收益率
            df_new_raw['yield'] = pd.to_numeric(df_new_raw['yield'], errors='coerce')
            df_new_raw = df_new_raw.dropna()
            
            # 筛选2010年之后的数据
            df_new_raw = df_new_raw[df_new_raw['trade_date'] >= '20100101']
            
            # 增量筛选
            if df_existing is not None:
                df_update = df_new_raw[df_new_raw['trade_date'] > latest_date]
                if df_update.empty:
                    print(f'  ✅ {task["name"]} 数据已是最新')
                    continue
                else:
                    print(f'  发现 {len(df_update)} 条新记录')
                    df_combined = pd.concat([df_existing, df_update], ignore_index=True)
            else:
                df_combined = df_new_raw
                print(f'  首次载入: {len(df_combined)} 条记录')
            
            # 排序去重
            df_combined = df_combined.sort_values('trade_date').drop_duplicates(subset=['trade_date'])
            
            # 添加元数据字段
            df_combined['ts_code'] = task['ts_code']
            df_combined['curve_name'] = f'中国国债收益率{task["name"]}'
            df_combined['curve_type'] = '0'
            df_combined['curve_term'] = task['term']
            
            # 调整列顺序
            df_combined = df_combined[['trade_date', 'ts_code', 'curve_name', 'curve_type', 'curve_term', 'yield']]
            
            # 保存
            # 确保父目录存在
            task['file'].parent.mkdir(parents=True, exist_ok=True)
            df_combined.to_parquet(task['file'], engine='pyarrow', index=False)
            print(f'  ✅ 已更新数据库: {task["file"].name}')
            print(f'     最新日期: {df_combined["trade_date"].max()}, 最新收益率: {df_combined["yield"].iloc[-1]:.4f}')
        
        print('\n' + '=' * 80)
        print('✅ 所有国债收益率数据更新完成！')
        print('=' * 80)
        
        return True
        
    except Exception as e:
        print(f'\n❌ 更新失败: {e}')
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = update_bond_yield_incremental()
    exit(0 if success else 1)
