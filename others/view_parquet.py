#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速查看 Parquet 文件的工具
"""

import pandas as pd
import sys

if len(sys.argv) < 2:
    print("用法: python view_parquet.py <文件路径>")
    print("例如: python view_parquet.py quant_data_center/stock/daily_hfq/000001.SZ.parquet")
    sys.exit(1)

file_path = sys.argv[1]

try:
    # 读取parquet文件
    print(f"正在读取: {file_path}")
    df = pd.read_parquet(file_path, engine='pyarrow')
    
    print(f"\n数据形状: {df.shape} (行数 x 列数)")
    print(f"\n列名: {list(df.columns)}")
    print(f"\n前10行数据:")
    print(df.head(10))
    
    print(f"\n数据类型:")
    print(df.dtypes)
    
    if 'trade_date' in df.columns:
        print(f"\n日期范围: {df['trade_date'].min()} 到 {df['trade_date'].max()}")
    elif 'end_date' in df.columns:
        print(f"\n日期范围: {df['end_date'].min()} 到 {df['end_date'].max()}")
    
    print(f"\n统计信息:")
    print(df.describe())
    
    # 询问是否导出为CSV
    export = input("\n是否导出为CSV文件？(y/n): ")
    if export.lower() == 'y':
        csv_path = file_path.replace('.parquet', '.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"已导出到: {csv_path}")
    
except FileNotFoundError:
    print(f"错误: 文件不存在: {file_path}")
except Exception as e:
    print(f"错误: {e}")

