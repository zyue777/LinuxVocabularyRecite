#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据中心工具函数模块
提供数据转换、元数据生成等辅助功能
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Tuple
from datetime import datetime


def convert_hfq_to_qfq(ts_code: str, data_center_path: str = None, latest_real_price: float = None) -> Tuple[Optional[pd.DataFrame], Optional[float]]:
    """
    将后复权数据转换为前复权数据（方案B：实时转换）
    
    原理：
    - 前复权：以最新价格为基准，向前调整历史价格
    - 后复权：以最早价格为基准，向后调整历史价格
    - 转换公式：qfq_price = hfq_price * (latest_real_price / hfq_price_at_latest_date)
    
    Args:
        ts_code: 股票代码
        data_center_path: 数据中心路径，如果为None则使用默认路径
        latest_real_price: 最新真实收盘价（不复权），用于计算复权因子。如果为None，将无法正确转换。
    
    Returns:
        (前复权DataFrame, 复权因子)，如果文件不存在或失败则返回 (None, None)
    """
    if data_center_path is None:
        data_center_path = Path.cwd() / "quant_data_center"
    else:
        data_center_path = Path(data_center_path)
    
    hfq_file = data_center_path / "stock" / "daily_hfq" / f"{ts_code}.parquet"
    
    if not hfq_file.exists():
        return None, None
    
    try:
        # 读取后复权数据
        df_hfq = pd.read_parquet(hfq_file, engine='pyarrow')
        
        if df_hfq.empty:
            return None, None
        
        # 确保按日期排序
        df_hfq = df_hfq.sort_values('trade_date').reset_index(drop=True)
        
        # 获取后复权数据的最新收盘价
        hfq_latest_close = df_hfq.iloc[-1]['close']
        
        # 计算复权因子
        if latest_real_price is not None:
            # 正确公式：真实最新价 / 后复权最新价
            adj_factor = latest_real_price / hfq_latest_close
        else:
            # 如果未提供真实价格，无法计算正确的因子
            # 为了兼容性，暂时返回None，或者抛出警告
            print(f"警告: 转换 {ts_code} 时未提供最新真实价格，无法计算准确的前复权数据")
            return None, None
        
        # 创建前复权数据副本
        df_qfq = df_hfq.copy()
        
        # 转换价格字段
        price_fields = ['open', 'high', 'low', 'close', 'pre_close']
        for field in price_fields:
            if field in df_qfq.columns:
                df_qfq[field] = df_hfq[field] * adj_factor
        
        # 调整涨跌额（change）和涨跌幅（pct_chg）保持不变
        
        return df_qfq, adj_factor
        
    except Exception as e:
        print(f"转换 {ts_code} 前复权数据失败: {e}")
        return None, None


def generate_market_metadata(data_center_path: str = None) -> bool:
    """
    生成市场元数据文件
    
    创建以下文件：
    - chinext_stocks.parquet: 创业板股票标记
    - stock_market_map.parquet: 股票所属市场映射（主板、中小板、创业板、科创板）
    
    Args:
        data_center_path: 数据中心路径
    
    Returns:
        是否成功生成
    """
    if data_center_path is None:
        data_center_path = Path.cwd() / "quant_data_center"
    else:
        data_center_path = Path(data_center_path)
    
    # 创建market_metadata目录
    metadata_dir = data_center_path / "market_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 读取股票基础信息
        stock_basic_file = data_center_path / "stock_basic.parquet"
        if not stock_basic_file.exists():
            print("❌ 股票基础信息文件不存在，请先运行 update_stock_basic()")
            return False
        
        df_basic = pd.read_parquet(stock_basic_file, engine='pyarrow')
        
        # 1. 生成创业板股票标记文件
        print("生成创业板股票标记...")
        chinext_stocks = []
        
        for _, row in df_basic.iterrows():
            ts_code = row['ts_code']
            symbol = row.get('symbol', '')
            
            # 判断是否为创业板（300开头）
            is_chinext = symbol.startswith('300') if symbol else False
            
            # 判断市场类型
            market = 'UNKNOWN'
            if symbol.startswith('300'):
                market = 'CHINEXT'  # 创业板
            elif symbol.startswith('688'):
                market = 'STAR'  # 科创板
            elif symbol.startswith('002') or symbol.startswith('003'):
                market = 'SME'  # 中小板
            elif symbol.startswith('000') or symbol.startswith('001'):
                market = 'MAIN_SZ'  # 深市主板
            elif symbol.startswith('600') or symbol.startswith('601') or symbol.startswith('603') or symbol.startswith('605'):
                market = 'MAIN_SH'  # 沪市主板
            
            chinext_stocks.append({
                'ts_code': ts_code,
                'symbol': symbol,
                'name': row.get('name', ''),
                'is_chinext': is_chinext,
                'market': market,
                'list_date': row.get('list_date', ''),
                'area': row.get('area', ''),
                'industry': row.get('industry', '')
            })
        
        df_chinext = pd.DataFrame(chinext_stocks)
        chinext_file = metadata_dir / "chinext_stocks.parquet"
        df_chinext.to_parquet(chinext_file, engine='pyarrow', index=False)
        print(f"✅ 已生成创业板股票标记: {len(df_chinext)} 只股票")
        print(f"   其中创业板股票: {df_chinext['is_chinext'].sum()} 只")
        
        # 2. 生成股票市场映射文件（更详细的版本）
        print("生成股票市场映射...")
        market_map_file = metadata_dir / "stock_market_map.parquet"
        df_chinext[['ts_code', 'symbol', 'name', 'market', 'is_chinext', 'list_date']].to_parquet(
            market_map_file, engine='pyarrow', index=False
        )
        print(f"✅ 已生成股票市场映射文件")
        
        # 3. 生成更新状态文件（JSON格式，记录更新时间）
        import json
        update_status = {
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_stocks': len(df_chinext),
            'chinext_count': int(df_chinext['is_chinext'].sum()),
            'market_distribution': df_chinext['market'].value_counts().to_dict()
        }
        
        status_file = metadata_dir / "update_status.json"
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump(update_status, f, ensure_ascii=False, indent=2)
        print(f"✅ 已生成更新状态文件")
        
        # 打印统计信息
        print("\n市场分布统计:")
        market_dist = df_chinext['market'].value_counts()
        for market, count in market_dist.items():
            print(f"  {market}: {count} 只")
        
        return True
        
    except Exception as e:
        print(f"❌ 生成市场元数据失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_chinext_stocks(data_center_path: str = None) -> List[str]:
    """
    获取创业板股票列表
    
    Args:
        data_center_path: 数据中心路径
    
    Returns:
        创业板股票代码列表
    """
    if data_center_path is None:
        data_center_path = Path.cwd() / "quant_data_center"
    else:
        data_center_path = Path(data_center_path)
    
    chinext_file = data_center_path / "market_metadata" / "chinext_stocks.parquet"
    
    if not chinext_file.exists():
        # 如果文件不存在，尝试生成
        print("创业板股票标记文件不存在，正在生成...")
        if generate_market_metadata(data_center_path):
            # 重新读取
            pass
        else:
            return []
    
    try:
        df = pd.read_parquet(chinext_file, engine='pyarrow')
        chinext_stocks = df[df['is_chinext'] == True]['ts_code'].tolist()
        return chinext_stocks
    except Exception as e:
        print(f"读取创业板股票列表失败: {e}")
        return []


def filter_stocks_by_market(market: str, data_center_path: str = None) -> pd.DataFrame:
    """
    按市场筛选股票
    
    Args:
        market: 市场类型 ('CHINEXT', 'STAR', 'SME', 'MAIN_SZ', 'MAIN_SH')
        data_center_path: 数据中心路径
    
    Returns:
        符合条件的股票DataFrame
    """
    if data_center_path is None:
        data_center_path = Path.cwd() / "quant_data_center"
    else:
        data_center_path = Path(data_center_path)
    
    market_map_file = data_center_path / "market_metadata" / "stock_market_map.parquet"
    
    if not market_map_file.exists():
        # 如果文件不存在，尝试生成
        print("股票市场映射文件不存在，正在生成...")
        if not generate_market_metadata(data_center_path):
            return pd.DataFrame()
    
    try:
        df = pd.read_parquet(market_map_file, engine='pyarrow')
        filtered = df[df['market'] == market].copy()
        return filtered
    except Exception as e:
        print(f"按市场筛选股票失败: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # 测试代码
    print("测试市场元数据生成...")
    generate_market_metadata()
    
    print("\n测试获取创业板股票列表...")
    chinext_list = get_chinext_stocks()
    print(f"创业板股票数量: {len(chinext_list)}")
    if chinext_list:
        print(f"前10只: {chinext_list[:10]}")

