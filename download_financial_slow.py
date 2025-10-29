#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
财务三大报表数据下载脚本 - 低速稳定版

特点：
  - 每批次只处理50只股票
  - 每分钟严格控制在200次API调用以内
  - 适合首次大批量下载或补充遗漏数据
  - 自动跳过已下载的股票（增量更新）
  
使用方法：
  python download_financial_slow.py
  
预计时间：
  5000只股票 × 3张表 ÷ 200次/分钟 = 约75分钟/表
  三张表合计：约4小时
"""

import time
from datetime import datetime
from download_data_manager import QuantDataManager

def print_separator(char='=', length=80):
    """打印分隔线"""
    print(char * length)

def print_progress_bar(current, total, prefix='进度', suffix='', length=50):
    """打印进度条"""
    percent = 100 * (current / float(total))
    filled_length = int(length * current // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent:.1f}% {suffix}', end='', flush=True)
    if current == total:
        print()

def download_financial_tables_slow():
    """低速稳定下载财务三大报表（带预检查）"""
    
    print_separator()
    print("财务三大报表数据下载 - 低速稳定版 v2.0")
    print_separator()
    print("\n✨ 新功能:")
    print("  - ✅ 预检查数据完整性")
    print("  - ✅ 智能跳过已是最新的股票")
    print("  - ✅ 自动计算实际需要下载的数量")
    print("  - ✅ 动态调整预估时间")
    
    print("\n配置参数:")
    print("  - 批次大小: 50只股票/批")
    print("  - 并发线程: 1个")
    print("  - API限制: 严格控制在200次/分钟以内")
    print("  - 增量更新: Worker内部自动判断")
    
    print("\n" + "=" * 80)
    
    # 初始化数据管理器（先初始化，用于预检查）
    manager = QuantDataManager()
    
    # ========== 预检查阶段 ==========
    print("\n🔍 步骤0: 预检查数据完整性...")
    print("-" * 80)
    
    from pathlib import Path
    import pandas as pd
    
    # 获取股票列表
    print("获取股票列表...")
    stock_basic = manager._safe_api_call(manager.pro.stock_basic, exchange='', list_status='L', fields='ts_code')
    if stock_basic is None:
        print("❌ 获取股票列表失败")
        return
    
    all_stocks = stock_basic['ts_code'].tolist()
    total_stocks = len(all_stocks)
    end_date = datetime.now().strftime('%Y%m%d')
    
    print(f"总股票数: {total_stocks} 只")
    
    # 检查三张表的数据完整性
    tables_info = {
        'income': {
            'name': '利润表',
            'path': manager.paths['stock_financial_tables'] / 'income'
        },
        'balancesheet': {
            'name': '资产负债表',
            'path': manager.paths['stock_financial_tables'] / 'balancesheet'
        },
        'cashflow': {
            'name': '现金流量表',
            'path': manager.paths['stock_financial_tables'] / 'cashflow'
        }
    }
    
    def check_table_status(table_path, stocks_list, end_date):
        """检查某张表的完整性"""
        need_update = []
        up_to_date = []
        
        for ts_code in stocks_list:
            file_path = table_path / f"{ts_code}.parquet"
            
            if not file_path.exists():
                need_update.append(ts_code)
            else:
                latest_date = manager._get_latest_date(file_path, 'end_date')
                if latest_date:
                    from datetime import timedelta
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                    
                    if start_date_actual >= end_date:
                        up_to_date.append(ts_code)
                    else:
                        need_update.append(ts_code)
                else:
                    need_update.append(ts_code)
        
        return need_update, up_to_date
    
    print("\n检查各表数据状态:")
    for table_key, table_info in tables_info.items():
        need_update, up_to_date = check_table_status(table_info['path'], all_stocks, end_date)
        table_info['need_update'] = need_update
        table_info['up_to_date'] = up_to_date
        
        print(f"\n  📊 {table_info['name']}:")
        print(f"     ✅ 已是最新: {len(up_to_date)} 只 ({len(up_to_date)/total_stocks*100:.1f}%)")
        print(f"     📥 需要更新: {len(need_update)} 只 ({len(need_update)/total_stocks*100:.1f}%)")
    
    # 计算总需要更新的数量
    total_need_update = sum(len(info['need_update']) for info in tables_info.values())
    total_up_to_date = sum(len(info['up_to_date']) for info in tables_info.values())
    
    print("\n" + "=" * 80)
    print(f"📊 汇总:")
    print(f"  总任务数: {total_stocks * 3} 个 ({total_stocks}只 × 3张表)")
    print(f"  ✅ 已是最新: {total_up_to_date} 个 ({total_up_to_date/(total_stocks*3)*100:.1f}%)")
    print(f"  📥 需要更新: {total_need_update} 个 ({total_need_update/(total_stocks*3)*100:.1f}%)")
    print("=" * 80)
    
    if total_need_update == 0:
        print("\n🎉 所有财务数据均已是最新，无需下载！")
        return
    
    # 动态估算时间（基于实际需要更新的数量）
    estimated_minutes = total_need_update / 200  # 200次/分钟
    estimated_hours = estimated_minutes / 60
    
    print(f"\n⏱️  预计总耗时: {estimated_minutes:.1f} 分钟 ({estimated_hours:.2f} 小时)")
    print(f"   （基于实际需要更新的 {total_need_update} 个任务）")
    
    # 确认是否继续
    response = input("\n是否开始下载？(y/n): ").strip().lower()
    if response != 'y':
        print("已取消")
        return
    
    start_time = datetime.now()
    print(f"\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print_separator()
    
    # 配置参数（低速稳定）
    batch_size = 50      # 每批次50只股票
    max_workers = 1      # 单线程（最稳定）
    
    # 理论并发：50 × 1 = 50次/批次
    # 安全余量：每批次间隔15秒 → 约200次/分钟
    
    # ========== 1. 下载利润表 ==========
    print("\n【步骤 1/3】下载利润表数据")
    print("-" * 80)
    
    income_stocks = tables_info['income']['need_update']
    if len(income_stocks) == 0:
        print("✅ 利润表数据已是最新，跳过")
    else:
        print(f"需要更新: {len(income_stocks)} 只股票")
        step_start = datetime.now()
        
        try:
            manager.update_income_table(
                stock_list=income_stocks,  # 只处理需要更新的
                batch_size=batch_size,
                max_workers=max_workers
            )
            step_end = datetime.now()
            step_duration = (step_end - step_start).total_seconds() / 60
            print(f"✅ 利润表下载完成，耗时: {step_duration:.1f} 分钟")
        except Exception as e:
            print(f"❌ 利润表下载失败: {e}")
            return
        
        # 休息30秒，让API完全重置
        print("\n等待30秒，让API限制重置...")
        time.sleep(30)
    
    # ========== 2. 下载资产负债表 ==========
    print("\n【步骤 2/3】下载资产负债表数据")
    print("-" * 80)
    
    balance_stocks = tables_info['balancesheet']['need_update']
    if len(balance_stocks) == 0:
        print("✅ 资产负债表数据已是最新，跳过")
    else:
        print(f"需要更新: {len(balance_stocks)} 只股票")
        step_start = datetime.now()
        
        try:
            manager.update_balancesheet_table(
                stock_list=balance_stocks,  # 只处理需要更新的
                batch_size=batch_size,
                max_workers=max_workers
            )
            step_end = datetime.now()
            step_duration = (step_end - step_start).total_seconds() / 60
            print(f"✅ 资产负债表下载完成，耗时: {step_duration:.1f} 分钟")
        except Exception as e:
            print(f"❌ 资产负债表下载失败: {e}")
            return
        
        # 休息30秒
        print("\n等待30秒，让API限制重置...")
        time.sleep(30)
    
    # ========== 3. 下载现金流量表 ==========
    print("\n【步骤 3/3】下载现金流量表数据")
    print("-" * 80)
    
    cashflow_stocks = tables_info['cashflow']['need_update']
    if len(cashflow_stocks) == 0:
        print("✅ 现金流量表数据已是最新，跳过")
    else:
        print(f"需要更新: {len(cashflow_stocks)} 只股票")
        step_start = datetime.now()
        
        try:
            manager.update_cashflow_table(
                stock_list=cashflow_stocks,  # 只处理需要更新的
                batch_size=batch_size,
                max_workers=max_workers
            )
            step_end = datetime.now()
            step_duration = (step_end - step_start).total_seconds() / 60
            print(f"✅ 现金流量表下载完成，耗时: {step_duration:.1f} 分钟")
        except Exception as e:
            print(f"❌ 现金流量表下载失败: {e}")
            return
    
    # ========== 完成 ==========
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds() / 60
    
    print("\n" + "=" * 80)
    print("✅ 所有财务报表下载完成！")
    print("=" * 80)
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {total_duration:.1f} 分钟 ({total_duration/60:.2f} 小时)")
    print("=" * 80)
    
    print("\n下载报告文件位置:")
    print("  - 查看 quant_data_center/download_report_*.txt")
    
    print("\n下一步：")
    print("  1. 检查下载报告，确认数据完整性")
    print("  2. 运行五因子构建脚本:")
    print("     python build_ff5_factors_monthly_ttm.py")

def main():
    """主函数"""
    try:
        download_financial_tables_slow()
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断下载")
        print("提示: 下次运行时会自动跳过已下载的股票，继续未完成的部分")
    except Exception as e:
        print(f"\n\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        print("\n提示: 修复问题后重新运行即可，已下载的数据会保留")

if __name__ == "__main__":
    main()

