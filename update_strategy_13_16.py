#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股量化研究数据中心 - 策略数据更新脚本
直接调用download_data_manager.py中的13-16选项的更新方法

文件名：update_strategy_13_16.py
创建时间：2025-11-29
"""

import sys
from pathlib import Path

# 添加当前目录到sys.path
sys.path.append(str(Path(__file__).parent))

from download_data_manager import QuantDataManager

def main():
    """主函数"""
    print("=" * 80)
    print("🚀 A股量化研究数据中心 - 策略数据更新脚本")
    print("直接调用download_data_manager.py中的13-16选项的更新方法")
    print("=" * 80)
    
    # 创建数据管理器
    try:
        manager = QuantDataManager()
        print(f"✅ 数据管理器初始化成功")
        print(f"📁 数据中心路径: {manager.data_center_path}")
    except Exception as e:
        print(f"❌ 数据管理器初始化失败: {e}")
        return 1
    
    # 解析命令行参数：如果未提供参数，默认执行 'all'
    if len(sys.argv) < 2:
        option = 'all'
        print("\nℹ️ 未提供选项，默认执行 'all'（更新所有策略数据）")
    else:
        option = sys.argv[1]
        if option in ('-h', '--help'):
            print("\n📋 使用方法:")
            print("  python update_strategy_13_16.py [选项]")
            print("\n📋 可用选项:")
            print("  13 - 更新港股通个股日K线数据")
            print("  14 - 更新四维择时策略所需数据（指数估值PE/PB + 国债收益率）")
            print("  15 - 更新CFFEX期货主力合约前20名会员持仓数据")
            print("  16 - 更新股票每日筹码分布统计数据")
            print("  all - 更新所有策略数据（13-16）")
            return 0
    
    # 执行对应的更新
    try:
        if option == '13':
            print("\n🇭🇰 更新港股通个股日K线数据 (选项13)")
            manager.update_hk_stock_daily_hfq()
        elif option == '14':
            print("\n📊 更新四维择时策略所需数据 (选项14)")
            manager.update_missing_data_for_strategy(
                include_index_valuation=True,
                include_bond_yield=True,
                include_options_pcr=False,
                include_futures_holding=False
            )
        elif option == '15':
            print("\n📈 更新CFFEX期货主力合约前20名会员持仓数据 (选项15)")
            manager.update_future_holdings()
        elif option == '16':
            print("\n🎲 更新股票每日筹码分布统计数据 (选项16)")
            manager.update_stock_cyq_perf()
        elif option == 'all':
            print("\n🚀 更新所有策略数据 (选项13-16)")
            
            print("\n1️⃣ 更新港股通个股日K线数据 (选项13)")
            manager.update_hk_stock_daily_hfq()
            
            print("\n2️⃣ 更新四维择时策略所需数据 (选项14)")
            manager.update_missing_data_for_strategy(
                include_index_valuation=True,
                include_bond_yield=True,
                include_options_pcr=False,
                include_futures_holding=False
            )
            
            print("\n3️⃣ 更新CFFEX期货主力合约前20名会员持仓数据 (选项15)")
            manager.update_future_holdings()
            
            print("\n4️⃣ 更新股票每日筹码分布统计数据 (选项16)")
            manager.update_stock_cyq_perf()
            
            print("\n✅ 所有策略数据更新完成！")
        else:
            print(f"❌ 未知选项: {option}")
            return 1
    except Exception as e:
        print(f"❌ 更新失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
