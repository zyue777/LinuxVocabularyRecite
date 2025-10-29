#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
导入SHIBOR历史数据到数据中心
从SHIBOR.xlsx文件导入数据并合并到现有的无风险利率数据中
"""

import pandas as pd
from pathlib import Path
from datetime import datetime


def import_shibor_from_excel(excel_path='SHIBOR.xlsx', data_center_path='quant_data_center'):
    """
    从Excel文件导入SHIBOR数据并合并到现有的无风险利率数据
    
    Args:
        excel_path: SHIBOR Excel文件路径
        data_center_path: 数据中心路径
    """
    print("=" * 60)
    print("开始导入SHIBOR历史数据")
    print("=" * 60)
    
    # 定义路径
    excel_file = Path(excel_path)
    rfr_file = Path(data_center_path) / "factors" / "risk_free" / "rfr_daily.parquet"
    
    # 检查Excel文件是否存在
    if not excel_file.exists():
        print(f"错误: Excel文件不存在: {excel_file}")
        return
    
    print(f"读取Excel文件: {excel_file}")
    
    # 读取Excel文件
    try:
        df_excel = pd.read_excel(excel_file, skiprows=1)
        
        # 重命名列
        df_excel.columns = ['trade_date', 'rf']
        
        # 转换日期格式为字符串 YYYYMMDD
        df_excel['trade_date'] = pd.to_datetime(df_excel['trade_date']).dt.strftime('%Y%m%d')
        
        # 删除空值
        df_excel = df_excel.dropna()
        
        print(f"从Excel读取了 {len(df_excel)} 条记录")
        print(f"数据日期范围: {df_excel['trade_date'].min()} ~ {df_excel['trade_date'].max()}")
        
    except Exception as e:
        print(f"读取Excel文件失败: {e}")
        return
    
    # 检查现有数据
    existing_df = None
    if rfr_file.exists():
        print(f"\n读取现有数据: {rfr_file}")
        try:
            existing_df = pd.read_parquet(rfr_file, engine='pyarrow')
            print(f"现有数据: {len(existing_df)} 条记录")
            print(f"现有数据日期范围: {existing_df['trade_date'].min()} ~ {existing_df['trade_date'].max()}")
        except Exception as e:
            print(f"读取现有数据失败: {e}")
            print("将创建新文件")
    else:
        print("现有数据文件不存在，将创建新文件")
    
    # 合并数据
    if existing_df is not None and not existing_df.empty:
        # 合并数据
        combined_df = pd.concat([existing_df, df_excel], ignore_index=True)
        
        # 去重（保留Excel数据的优先级）
        # 先按trade_date分组，取每组最后出现的值（Excel数据在后）
        combined_df = combined_df.drop_duplicates(subset=['trade_date'], keep='last')
        
        # 按日期排序
        combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)
        
        # 计算新增了多少数据
        existing_dates = set(existing_df['trade_date'].values)
        new_dates = set(df_excel['trade_date'].values)
        added_count = len(new_dates - existing_dates)
        updated_count = len(new_dates & existing_dates)
        
        print(f"\n数据合并结果:")
        print(f"  Excel新增记录: {added_count} 条")
        print(f"  Excel更新记录: {updated_count} 条")
        print(f"  合并后总记录: {len(combined_df)} 条")
        
    else:
        combined_df = df_excel.sort_values('trade_date').reset_index(drop=True)
        print(f"\n创建新文件，保存 {len(combined_df)} 条记录")
    
    # 保存数据
    try:
        rfr_file.parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_parquet(rfr_file, engine='pyarrow', index=False)
        print(f"\n✅ 成功保存数据到: {rfr_file}")
        print(f"   最终数据记录数: {len(combined_df)}")
        print(f"   日期范围: {combined_df['trade_date'].min()} ~ {combined_df['trade_date'].max()}")
    except Exception as e:
        print(f"\n❌ 保存数据失败: {e}")
        return
    
    print("\n" + "=" * 60)
    print("✅ SHIBOR数据导入完成")
    print("=" * 60)


def main():
    """主函数"""
    import sys
    
    # 获取命令行参数
    if len(sys.argv) > 1:
        excel_path = sys.argv[1]
    else:
        excel_path = 'SHIBOR.xlsx'
    
    if len(sys.argv) > 2:
        data_center_path = sys.argv[2]
    else:
        data_center_path = 'quant_data_center'
    
    import_shibor_from_excel(excel_path, data_center_path)


if __name__ == "__main__":
    main()

