#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据中心完整性检查工具 (v2.0优化版)
用于检查构建Fama-French五因子所需的数据是否完整

v2.0 更新说明:
- 新增现金流量表检查 (RMW因子: 经营现金流)
- 新增财务指标表字段检查 (CMA因子: 有形资产)
- 优化检查逻辑和输出格式
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# 数据中心路径
DATA_CENTER_PATH = '/home/zy/桌面/数据中心/quant_data_center'
REQUIRED_START_DATE = '20100101'  # 五因子所需的最早日期
CURRENT_DATE = datetime.now().strftime('%Y%m%d')

print("=" * 80)
print("数据中心完整性检查工具 (v2.0优化版)")
print(f"检查日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"数据中心路径: {DATA_CENTER_PATH}")
print(f"五因子版本: v2.0 (OCF + NOA优化)")
print("=" * 80)


def check_stock_basic():
    """检查股票基础信息"""
    print("\n【1. 股票基础信息】")
    try:
        file_path = Path(DATA_CENTER_PATH) / 'stock_basic.parquet'
        if not file_path.exists():
            print("  ❌ 文件不存在")
            return False
        
        df = pd.read_parquet(file_path)
        print(f"  ✅ 文件存在")
        print(f"     - 股票数量: {len(df)} 只")
        print(f"     - 字段: {list(df.columns)}")
        return True
    except Exception as e:
        print(f"  ❌ 读取失败: {e}")
        return False


def check_daily_hfq():
    """检查股票日K线数据（后复权）"""
    print("\n【2. 股票日K线数据（后复权）】")
    try:
        dir_path = Path(DATA_CENTER_PATH) / 'stock' / 'daily_hfq'
        if not dir_path.exists():
            print("  ❌ 目录不存在")
            return False
        
        # 统计文件数量
        files = list(dir_path.glob('*.parquet'))
        if not files:
            print("  ❌ 没有任何K线数据文件")
            return False
        
        print(f"  ✅ 目录存在，共 {len(files)} 个股票文件")
        
        # 采样检查几个文件
        sample_files = files[:5]  # 检查前5个
        date_ranges = []
        
        for file in sample_files:
            try:
                df = pd.read_parquet(file)
                if 'trade_date' in df.columns:
                    dates = df['trade_date'].astype(str)
                    min_date = dates.min()
                    max_date = dates.max()
                    date_ranges.append((file.stem, min_date, max_date, len(df)))
            except Exception as e:
                print(f"    ⚠️  {file.name} 读取失败: {e}")
        
        if date_ranges:
            print(f"\n  采样检查结果:")
            for ts_code, min_date, max_date, count in date_ranges:
                start_ok = "✅" if min_date <= REQUIRED_START_DATE else "❌"
                end_ok = "✅" if max_date >= CURRENT_DATE else "⚠️"
                print(f"    {ts_code}: {min_date} ~ {max_date} ({count}条) {start_ok}{end_ok}")
            
            # 统计整体情况
            all_min_dates = [r[1] for r in date_ranges]
            all_max_dates = [r[2] for r in date_ranges]
            
            overall_min = min(all_min_dates)
            overall_max = max(all_max_dates)
            
            print(f"\n  整体数据范围: {overall_min} ~ {overall_max}")
            if overall_min > REQUIRED_START_DATE:
                print(f"  ⚠️  警告: 最早数据晚于所需日期({REQUIRED_START_DATE})")
        
        return True
    except Exception as e:
        print(f"  ❌ 检查失败: {e}")
        return False


def check_daily_basic():
    """检查股票每日基础指标"""
    print("\n【3. 股票每日基础指标】")
    try:
        file_path = Path(DATA_CENTER_PATH) / 'stock' / 'daily_basic' / 'daily_basic_all.parquet'
        if not file_path.exists():
            print("  ❌ 文件不存在")
            return False
        
        # 读取日期范围
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema.to_arrow_schema()
        
        if 'trade_date' in schema.names:
            # 读取文件获取日期范围
            df = pd.read_parquet(file_path, columns=['trade_date'])
            dates = df['trade_date'].astype(str)
            min_date = dates.min()
            max_date = dates.max()
            
            print(f"  ✅ 文件存在")
            print(f"     - 总记录数: {len(df)}")
            print(f"     - 日期范围: {min_date} ~ {max_date}")
            
            start_ok = "✅" if min_date <= REQUIRED_START_DATE else "❌"
            end_ok = "✅" if max_date >= CURRENT_DATE else "⚠️"
            
            if min_date > REQUIRED_START_DATE:
                print(f"     - 状态: {start_ok} 最早数据晚于所需日期")
            if max_date < CURRENT_DATE:
                print(f"     - 状态: {end_ok} 数据未更新到最新")
            
            return True
        else:
            print("  ⚠️  文件存在但无trade_date字段")
            return False
    except Exception as e:
        print(f"  ❌ 检查失败: {e}")
        return False


def check_risk_free_rate():
    """检查无风险利率"""
    print("\n【4. 无风险利率】")
    try:
        file_path = Path(DATA_CENTER_PATH) / 'factors' / 'risk_free' / 'rfr_daily.parquet'
        if not file_path.exists():
            print("  ❌ 文件不存在")
            return False
        
        df = pd.read_parquet(file_path, columns=['trade_date'])
        dates = df['trade_date'].astype(str)
        min_date = dates.min()
        max_date = dates.max()
        
        print(f"  ✅ 文件存在")
        print(f"     - 总记录数: {len(df)}")
        print(f"     - 日期范围: {min_date} ~ {max_date}")
        
        start_ok = "✅" if min_date <= REQUIRED_START_DATE else "❌"
        end_ok = "✅" if max_date >= CURRENT_DATE else "⚠️"
        
        print(f"     - 开始日期检查: {start_ok}")
        print(f"     - 结束日期检查: {end_ok}")
        
        if min_date > REQUIRED_START_DATE:
            print(f"  ⚠️  警告: 最早数据晚于所需日期({REQUIRED_START_DATE})")
        
        return True
    except Exception as e:
        print(f"  ❌ 检查失败: {e}")
        return False


def check_financial_tables():
    """检查财务三大表 (v2.0优化版)"""
    print("\n【5. 财务三大表 (v2.0优化)】")
    
    # v2.0: cashflow和balancesheet是必需的
    tables = {
        'cashflow': {'required': True, 'key_field': 'n_cashflow_act', 'usage': 'RMW因子(经营现金流)'},
        'balancesheet': {'required': True, 'key_field': 'total_assets', 'usage': 'CMA因子+净资产'},
        'income': {'required': False, 'key_field': 'operate_profit', 'usage': '备用数据'}
    }
    
    results = {}
    
    for table, info in tables.items():
        try:
            dir_path = Path(DATA_CENTER_PATH) / 'stock' / 'financial_tables' / table
            if not dir_path.exists():
                status = "❌ 必需" if info['required'] else "⚠️  可选"
                print(f"  {status} {table}: 目录不存在 - 用途: {info['usage']}")
                results[table] = False if info['required'] else True
                continue
            
            files = list(dir_path.glob('*.parquet'))
            if not files:
                status = "❌ 必需" if info['required'] else "⚠️  可选"
                print(f"  {status} {table}: 没有任何数据文件 - 用途: {info['usage']}")
                results[table] = False if info['required'] else True
                continue
            
            # v2.0: 检查关键字段
            sample_file = files[0]
            try:
                df_sample = pd.read_parquet(sample_file, columns=[info['key_field']])
                has_data = df_sample[info['key_field']].notna().sum()
                total = len(df_sample)
                
                print(f"  ✅ {table}: 共 {len(files)} 个文件")
                print(f"     用途: {info['usage']}")
                print(f"     关键字段: {info['key_field']} ({has_data}/{total} 条有效)")
                
                # 采样检查日期范围
                sample_files = files[:2]
                for file in sample_files:
                    try:
                        df = pd.read_parquet(file, columns=['end_date', 'ann_date'])
                        if not df.empty:
                            end_dates = df['end_date'].astype(str)
                            min_date = end_dates.min()
                            max_date = end_dates.max()
                            print(f"     {file.stem}: {min_date} ~ {max_date} ({len(df)}条)")
                    except:
                        pass
                
                results[table] = True
            except KeyError:
                print(f"  ⚠️  {table}: 缺少关键字段 {info['key_field']}")
                results[table] = False if info['required'] else True
                
        except Exception as e:
            print(f"  ❌ {table}检查失败: {e}")
            results[table] = False if info['required'] else True
    
    return all(results.values())


def check_fina_indicator():
    """检查财务指标 (v2.0核心: 有形资产字段)"""
    print("\n【6. 财务指标 (v2.0核心)】")
    try:
        dir_path = Path(DATA_CENTER_PATH) / 'stock' / 'fina_indicator'
        if not dir_path.exists():
            print("  ❌ 必需 - 目录不存在")
            print("     用途: CMA因子 (有形资产NOA)")
            return False
        
        files = list(dir_path.glob('*.parquet'))
        if not files:
            print("  ❌ 必需 - 没有任何数据文件")
            print("     用途: CMA因子 (有形资产NOA)")
            return False
        
        print(f"  ✅ 目录存在，共 {len(files)} 个股票文件")
        print(f"     用途: CMA因子 - 有形资产(tangible_asset)")
        
        # v2.0: 检查tangible_asset字段的可用性
        sample_files = files[:10]
        valid_count = 0
        total_checked = 0
        
        for file in sample_files:
            try:
                df = pd.read_parquet(file, columns=['tangible_asset'])
                has_data = df['tangible_asset'].notna().sum()
                total = len(df)
                if has_data > 0:
                    valid_count += 1
                total_checked += 1
            except:
                pass
        
        if total_checked > 0:
            coverage = valid_count / total_checked * 100
            print(f"     字段检查: tangible_asset 在 {valid_count}/{total_checked} 个样本中有数据 ({coverage:.1f}%)")
            
            if coverage < 50:
                print(f"     ⚠️  警告: tangible_asset字段覆盖率较低，可能影响CMA因子质量")
            else:
                print(f"     ✅ tangible_asset字段覆盖率良好")
        
        return True
    except Exception as e:
        print(f"  ❌ 检查失败: {e}")
        return False


def check_index_data():
    """检查指数数据"""
    print("\n【7. 指数数据】")
    
    # 定义指数及其别名（000300.SH和399300.SZ是同一个指数）
    indices = [
        {'codes': ['399300.SZ'], 'name': '沪深300'},
        {'codes': ['000905.SH'], 'name': '中证500'},
        {'codes': ['399006.SZ'], 'name': '创业板指'},
        {'codes': ['000300.SH', '399300.SZ'], 'name': '沪深300(上交所代码)'}  # 可能的别名
    ]
    
    results = {}
    checked_indices = set()  # 避免重复检查
    
    for idx_info in indices:
        codes = idx_info['codes']
        name = idx_info['name']
        
        # 尝试所有可能的代码
        found_daily = False
        found_const = False
        used_code = None
        
        for index in codes:
            if index in checked_indices:
                continue
                
            try:
                # 检查指数日K线
                daily_path = Path(DATA_CENTER_PATH) / 'index' / 'daily' / f'{index}.parquet'
                
                if daily_path.exists() and not found_daily:
                    df = pd.read_parquet(daily_path, columns=['trade_date'])
                    dates = df['trade_date'].astype(str)
                    min_date = dates.min()
                    max_date = dates.max()
                    print(f"  ✅ {name} 日K线 ({index}): {min_date} ~ {max_date} ({len(df)}条)")
                    results[f'{index}_daily'] = True
                    found_daily = True
                    used_code = index
                    checked_indices.add(index)
                
                # 检查指数成分股
                const_path = Path(DATA_CENTER_PATH) / 'index' / 'constituents' / f'{index}_const.parquet'
                
                if const_path.exists() and not found_const:
                    df = pd.read_parquet(const_path, columns=['trade_date'])
                    dates = df['trade_date'].astype(str)
                    min_date = dates.min()
                    max_date = dates.max()
                    print(f"  ✅ {name} 成分股 ({index}): {min_date} ~ {max_date} ({len(df)}条)")
                    results[f'{index}_const'] = True
                    found_const = True
                    checked_indices.add(index)
                    
            except Exception as e:
                continue
        
        # 如果一个都没找到，才报错
        if not found_daily and not found_const:
            if len(codes) == 1:
                print(f"  ⚠️  {name} ({codes[0]}): 日K线和成分股数据均不存在")
                results[f'{codes[0]}'] = False
    
    # 统计结果
    if not results:
        print("  ⚠️  未找到任何指数数据")
        return False
    
    # 只要有核心指数数据就算通过
    core_indices_ok = any('399300.SZ' in k or '000905.SH' in k or '399006.SZ' in k for k in results.keys())
    
    return core_indices_ok


def generate_report():
    """生成完整性报告"""
    print("\n" + "=" * 80)
    print("生成完整性报告...")
    print("=" * 80)
    
    checks = [
        ("股票基础信息", check_stock_basic),
        ("股票日K线数据", check_daily_hfq),
        ("股票每日基础指标", check_daily_basic),
        ("无风险利率", check_risk_free_rate),
        ("财务三大表", check_financial_tables),
        ("财务指标", check_fina_indicator),
        ("指数数据", check_index_data),
    ]
    
    results = []
    for name, check_func in checks:
        try:
            success = check_func()
            results.append((name, success))
        except Exception as e:
            print(f"  ❌ {name}: 检查异常 {e}")
            results.append((name, False))
    
    # 总结报告
    print("\n" + "=" * 80)
    print("完整性总结报告")
    print("=" * 80)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    print(f"\n总体完整度: {passed}/{total} ({passed/total*100:.1f}%)")
    print("\n详细结果:")
    for name, success in results:
        status = "✅ 完整" if success else "❌ 缺失"
        print(f"  - {name}: {status}")
    
    print("\n" + "=" * 80)
    
    if passed == total:
        print("🎉 所有数据完整性检查通过！可以开始构建v2.0五因子。")
        print("\nv2.0五因子数据源总结:")
        print("  ✅ RMW因子: stock/financial_tables/cashflow/ (经营现金流)")
        print("  ✅ CMA因子: stock/fina_indicator/ (有形资产)")
        print("  ✅ 其他因子: daily_hfq, daily_basic, balancesheet, risk_free")
        print("\n运行构建命令:")
        print("  python3 build_ff5_factors_monthly_ttm.py")
        return True
    else:
        print("⚠️  部分数据缺失或数据不完整，请先更新缺失的数据。")
        print("\n建议操作:")
        print("  1. 运行 python data_manager.py")
        print("  2. 选择相应的数据更新选项")
        print("\nv2.0版本特别提醒:")
        print("  ⚠️  cashflow表和fina_indicator表是v2.0版本必需的！")
        print("  ⚠️  如果这两个表缺失，请优先更新")
        return False


if __name__ == "__main__":
    try:
        success = generate_report()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ 检查过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

