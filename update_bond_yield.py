#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国债收益率数据增量更新脚本
使用AkShare获取中美国债收益率数据，自动增量更新
"""

import pandas as pd
import akshare as ak
from pathlib import Path
from datetime import datetime

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
        # 1. 读取现有数据
        if data_file.exists():
            df_existing = pd.read_parquet(data_file)
            latest_date = df_existing['trade_date'].max()
            print(f'\n现有数据: {len(df_existing)} 条记录')
            print(f'最新日期: {latest_date}')
        else:
            df_existing = None
            latest_date = '20100101'
            print('\n未找到现有数据，将下载完整历史数据')
        
        # 2. 从AkShare下载最新数据
        print('\n正在从AkShare下载数据...')
        df_raw = ak.bond_zh_us_rate()
        print(f'下载成功: {len(df_raw)} 条原始记录')
        
        # 3. 处理数据
        df_10y = df_raw[['日期', '中国国债收益率10年']].copy()
        df_10y.columns = ['trade_date', 'yield']
        
        # 转换日期格式
        df_10y['trade_date'] = pd.to_datetime(df_10y['trade_date']).dt.strftime('%Y%m%d')
        
        # 转换收益率为float，删除缺失值
        df_10y['yield'] = pd.to_numeric(df_10y['yield'], errors='coerce')
        df_10y = df_10y.dropna()
        
        # 筛选2010年之后的数据
        df_10y = df_10y[df_10y['trade_date'] >= '20100101']
        
        # 4. 筛选新数据
        if df_existing is not None:
            df_new = df_10y[df_10y['trade_date'] > latest_date]
            if df_new.empty:
                print('\n✅ 数据已是最新，无需更新')
                return True
            
            print(f'\n发现 {len(df_new)} 条新记录')
            print(f'新数据日期范围: {df_new["trade_date"].min()} - {df_new["trade_date"].max()}')
            
            # 合并数据
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_10y
            print(f'\n首次下载: {len(df_combined)} 条记录')
        
        # 排序并去重
        df_combined = df_combined.sort_values('trade_date').drop_duplicates(subset=['trade_date'])
        
        # 添加其他字段
        df_combined['ts_code'] = 'CN10YR.IB'
        df_combined['curve_name'] = '中国国债收益率10年'
        df_combined['curve_type'] = '0'
        df_combined['curve_term'] = 10.0
        
        # 调整列顺序
        df_combined = df_combined[['trade_date', 'ts_code', 'curve_name', 'curve_type', 'curve_term', 'yield']]
        
        # 5. 保存数据
        df_combined.to_parquet(data_file, engine='pyarrow', index=False)
        print(f'\n✅ 已更新数据库: {data_file}')
        print(f'   总记录数: {len(df_combined)}')
        print(f'   日期范围: {df_combined["trade_date"].min()} - {df_combined["trade_date"].max()}')
        
        # 6. 更新备份
        df_combined.to_parquet(backup_parquet, engine='pyarrow', index=False)
        df_combined.to_csv(backup_csv, index=False, encoding='utf-8-sig')
        print(f'\n✅ 已更新备份文件')
        print(f'   Parquet: {backup_parquet}')
        print(f'   CSV: {backup_csv}')
        
        # 7. 显示最新数据
        print(f'\n最新10条记录:')
        print(df_combined.tail(10)[['trade_date', 'yield']])
        
        print('\n' + '=' * 80)
        print('✅ 国债收益率数据更新完成！')
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
