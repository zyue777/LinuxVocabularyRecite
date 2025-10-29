#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据中心配置文件示例
用于其他项目调用数据中心数据

使用方法:
1. 复制此文件到你的项目中
2. 根据实际情况修改 DATA_CENTER_PATH
3. 在代码中 import data_config
"""

import os
from pathlib import Path

# ============================================================
# 数据中心路径配置
# ============================================================

# 方法1: 使用绝对路径（推荐）
DATA_CENTER_PATH = '/home/zy/桌面/数据中心/quant_data_center'

# 方法2: 使用环境变量（最灵活）
# 可以通过设置环境变量来覆盖默认路径
# export QUANT_DATA_CENTER='/your/custom/path'
# DATA_CENTER_PATH = os.environ.get('QUANT_DATA_CENTER', '/home/zy/桌面/数据中心/quant_data_center')

# 方法3: 使用相对路径（不推荐，仅用于特殊情况）
# DATA_CENTER_PATH = os.path.join(os.path.dirname(__file__), '../数据中心/quant_data_center')

# ============================================================
# 数据路径便捷函数
# ============================================================

def get_data_path(relative_path: str = '') -> str:
    """
    获取数据文件的完整路径
    
    Args:
        relative_path: 相对于数据中心的路径
        
    Returns:
        完整的绝对路径
        
    Example:
        >>> get_data_path('stock_basic.parquet')
        '/home/zy/桌面/数据中心/quant_data_center/stock_basic.parquet'
        
        >>> get_data_path('stock/daily_hfq/000001.SZ.parquet')
        '/home/zy/桌面/数据中心/quant_data_center/stock/daily_hfq/000001.SZ.parquet'
    """
    return os.path.join(DATA_CENTER_PATH, relative_path)


def get_stock_daily_path(ts_code: str) -> str:
    """
    获取股票日K线数据路径
    
    Args:
        ts_code: 股票代码（如 '000001.SZ'）
        
    Returns:
        股票日K线数据文件路径
    """
    return get_data_path(f'stock/daily_hfq/{ts_code}.parquet')


def get_stock_fina_path(ts_code: str) -> str:
    """
    获取股票财务指标数据路径
    
    Args:
        ts_code: 股票代码（如 '000001.SZ'）
        
    Returns:
        股票财务指标数据文件路径
    """
    return get_data_path(f'stock/fina_indicator/{ts_code}.parquet')


def get_index_daily_path(index_code: str) -> str:
    """
    获取指数日K线数据路径
    
    Args:
        index_code: 指数代码（如 '399300.SZ'）
        
    Returns:
        指数日K线数据文件路径
    """
    return get_data_path(f'index/daily/{index_code}.parquet')


def get_index_constituents_path(index_code: str) -> str:
    """
    获取指数成分股数据路径
    
    Args:
        index_code: 指数代码（如 '399300.SZ'）
        
    Returns:
        指数成分股数据文件路径
    """
    return get_data_path(f'index/constituents/{index_code}_const.parquet')


def check_data_center_exists() -> bool:
    """
    检查数据中心路径是否存在
    
    Returns:
        True if exists, False otherwise
    """
    return os.path.exists(DATA_CENTER_PATH)


# ============================================================
# 常用数据路径常量
# ============================================================

# 股票基础信息
STOCK_BASIC_PATH = get_data_path('stock_basic.parquet')

# 股票每日基础指标
STOCK_DAILY_BASIC_PATH = get_data_path('stock/daily_basic/daily_basic_all.parquet')

# Fama-French五因子
FF5_FACTORS_PATH = get_data_path('factors/fama_french_5/ff_5_factors_daily.parquet')

# 无风险利率
RISK_FREE_PATH = get_data_path('factors/risk_free/rfr_daily.parquet')

# 申万行业分类
SW_INDUSTRY_PATH = get_data_path('classification/industry_sw/sw_l1_daily.parquet')

# ============================================================
# 初始化检查
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("数据中心配置检查")
    print("=" * 60)
    
    print(f"数据中心路径: {DATA_CENTER_PATH}")
    
    if check_data_center_exists():
        print("✅ 数据中心路径存在")
        
        # 检查关键文件
        print("\n关键文件检查:")
        files_to_check = {
            '股票基础信息': STOCK_BASIC_PATH,
            '股票每日基础指标': STOCK_DAILY_BASIC_PATH,
            'Fama-French五因子': FF5_FACTORS_PATH,
            '无风险利率': RISK_FREE_PATH,
            '申万行业分类': SW_INDUSTRY_PATH,
        }
        
        for name, path in files_to_check.items():
            exists = os.path.exists(path)
            status = "✅" if exists else "❌"
            print(f"  {status} {name}: {path}")
    else:
        print("❌ 数据中心路径不存在，请检查配置!")
        print(f"   配置的路径: {DATA_CENTER_PATH}")

