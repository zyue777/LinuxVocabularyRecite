#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据中心使用示例
演示如何在其他项目中调用数据中心的数据
"""

import pandas as pd
import sys
sys.path.append('/home/zy/桌面/数据中心')
import data_config_example as data_config

def example_1_basic_usage():
    """示例1: 基础数据读取"""
    print("=" * 60)
    print("示例1: 基础数据读取")
    print("=" * 60)
    
    # 读取股票基础信息
    df_basic = pd.read_parquet(data_config.STOCK_BASIC_PATH)
    print(f"✅ 读取股票基础信息: {len(df_basic)} 只股票")
    print(f"   列名: {list(df_basic.columns)}")
    print(f"   前3条记录:")
    print(df_basic.head(3))
    

def example_2_stock_data():
    """示例2: 读取单只股票数据"""
    print("\n" + "=" * 60)
    print("示例2: 读取单只股票数据")
    print("=" * 60)
    
    # 读取平安银行的日K线数据
    ts_code = '000001.SZ'
    stock_path = data_config.get_stock_daily_path(ts_code)
    df_stock = pd.read_parquet(stock_path)
    
    print(f"✅ 读取 {ts_code} 日K线数据")
    print(f"   数据量: {len(df_stock)} 条")
    print(f"   日期范围: {df_stock['trade_date'].min()} - {df_stock['trade_date'].max()}")
    print(f"   最新收盘价: {df_stock['close'].iloc[-1]:.2f}")
    

def example_3_multiple_stocks():
    """示例3: 批量读取多只股票"""
    print("\n" + "=" * 60)
    print("示例3: 批量读取多只股票")
    print("=" * 60)
    
    stock_codes = ['000001.SZ', '000002.SZ', '000004.SZ']
    stock_data = {}
    
    for code in stock_codes:
        path = data_config.get_stock_daily_path(code)
        try:
            stock_data[code] = pd.read_parquet(path)
        except FileNotFoundError:
            print(f"   ⚠️  {code} 数据文件不存在，跳过")
            continue
    
    print(f"✅ 成功读取 {len(stock_data)} 只股票")
    for code, df in stock_data.items():
        latest_close = df['close'].iloc[-1]
        print(f"   {code}: {len(df)} 条数据, 最新收盘价 {latest_close:.2f}")


def example_4_index_constituents():
    """示例4: 读取指数成分股"""
    print("\n" + "=" * 60)
    print("示例4: 读取指数成分股")
    print("=" * 60)
    
    # 读取沪深300成分股
    index_code = '399300.SZ'
    const_path = data_config.get_index_constituents_path(index_code)
    df_const = pd.read_parquet(const_path)
    
    print(f"✅ 读取 {index_code} 成分股数据")
    print(f"   数据量: {len(df_const)} 条")
    
    # 获取最新一天的成分股
    latest_date = df_const['trade_date'].max()
    latest_constituents = df_const[df_const['trade_date'] == latest_date]
    
    print(f"   最新日期: {latest_date}")
    print(f"   成分股数量: {len(latest_constituents)} 只")
    print(f"   前5只成分股:")
    print(latest_constituents.head(5)[['con_code', 'weight']])


def example_5_fama_french():
    """示例5: 读取Fama-French三因子"""
    print("\n" + "=" * 60)
    print("示例5: 读取Fama-French三因子")
    print("=" * 60)
    
    # 读取三因子数据
    df_ff3 = pd.read_parquet(data_config.FF3_FACTORS_PATH)
    
    print(f"✅ 读取Fama-French三因子数据")
    print(f"   数据量: {len(df_ff3)} 条")
    print(f"   日期范围: {df_ff3['trade_date'].min()} - {df_ff3['trade_date'].max()}")
    print(f"   列名: {list(df_ff3.columns)}")
    print(f"   最近5天数据:")
    print(df_ff3.tail(5))


def example_6_combine_data():
    """示例6: 组合多个数据源"""
    print("\n" + "=" * 60)
    print("示例6: 组合多个数据源")
    print("=" * 60)
    
    # 读取股票日K线
    ts_code = '000001.SZ'
    df_stock = pd.read_parquet(data_config.get_stock_daily_path(ts_code))
    df_stock['return'] = df_stock['close'].pct_change()
    
    # 读取Fama-French三因子
    df_ff3 = pd.read_parquet(data_config.FF3_FACTORS_PATH)
    
    # 合并数据
    df_merged = df_stock.merge(df_ff3, on='trade_date', how='inner')
    
    print(f"✅ 成功合并股票数据和三因子数据")
    print(f"   合并后数据量: {len(df_merged)} 条")
    print(f"   列名: {list(df_merged.columns)}")
    
    # 计算超额收益
    df_merged['excess_return'] = df_merged['return'] - df_merged['MKT_RF']
    
    print(f"   平均超额收益: {df_merged['excess_return'].mean():.4%}")
    print(f"   超额收益标准差: {df_merged['excess_return'].std():.4%}")


def example_7_cross_project():
    """示例7: 跨项目使用场景"""
    print("\n" + "=" * 60)
    print("示例7: 跨项目使用场景演示")
    print("=" * 60)
    
    print("在其他项目中使用数据中心的典型步骤:")
    print("\n1. 复制配置文件:")
    print("   cp /home/zy/桌面/数据中心/data_config_example.py your_project/data_config.py")
    
    print("\n2. 在代码中导入:")
    print("   import data_config")
    print("   import pandas as pd")
    
    print("\n3. 使用绝对路径读取数据:")
    print("   df = pd.read_parquet(data_config.STOCK_BASIC_PATH)")
    
    print("\n4. 或使用便捷函数:")
    print("   stock_path = data_config.get_stock_daily_path('000001.SZ')")
    print("   df = pd.read_parquet(stock_path)")
    
    print("\n5. 设置环境变量（可选）:")
    print("   export QUANT_DATA_CENTER='/home/zy/桌面/数据中心/quant_data_center'")
    
    print("\n✅ 这样就可以在任何项目中使用数据中心的数据了！")


def main():
    """主函数"""
    print("=" * 60)
    print("数据中心使用示例演示")
    print("=" * 60)
    
    # 检查数据中心是否存在
    if not data_config.check_data_center_exists():
        print(f"❌ 数据中心不存在: {data_config.DATA_CENTER_PATH}")
        return
    
    print(f"✅ 数据中心路径: {data_config.DATA_CENTER_PATH}\n")
    
    # 运行所有示例
    try:
        example_1_basic_usage()
        example_2_stock_data()
        example_3_multiple_stocks()
        example_4_index_constituents()
        example_5_fama_french()
        example_6_combine_data()
        example_7_cross_project()
        
        print("\n" + "=" * 60)
        print("✅ 所有示例运行完成！")
        print("=" * 60)
        print("\n📚 更多信息请参考: 数据词典.md")
        
    except Exception as e:
        print(f"\n❌ 运行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

