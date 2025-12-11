#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
港股通股票行业分类补全脚本
通过 akshare 的 stock_hk_company_profile_em 接口获取每只港股的行业分类信息，
并更新本地缓存文件 hk_stock_info_cache.parquet

使用方法:
  python enrich_hk_industry_classification.py [--batch-size 10] [--delay 0.5]
"""

import sys
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 添加当前目录到 sys.path
sys.path.append(str(Path(__file__).parent))

def fetch_hk_stock_industry(code: str, delay: float = 0.5) -> Optional[Dict[str, str]]:
    """
    通过 akshare 获取单只港股的行业分类
    
    参数:
        code: 港股代码（不含.HK后缀）
        delay: 请求间隔（秒）
    
    返回:
        {'code': '00001', 'name': '长和', 'industry': '综合企业'} 或 None
    """
    try:
        import akshare as ak
        
        # 延迟以避免过度请求
        if delay > 0:
            time.sleep(delay)
        
        # 获取公司信息
        result = ak.stock_hk_company_profile_em(symbol=code)
        
        if result is not None and len(result) > 0:
            company_name = result.iloc[0].get('公司名称', '')
            industry = result.iloc[0].get('所属行业', 'Unclassified')
            
            return {
                'code': code,
                'name': company_name,
                'industry': industry if industry else 'Unclassified'
            }
        else:
            logger.warning(f"代码 {code}: 无返回数据")
            return None
            
    except Exception as e:
        logger.error(f"代码 {code}: 获取失败 - {e}")
        return None


def enrich_hk_industry_classification(
    cache_file: Path,
    batch_size: int = 10,
    max_workers: int = 4,
    delay: float = 0.5,
    only_unclassified: bool = True
) -> None:
    """
    补全港股行业分类
    
    参数:
        cache_file: 缓存文件路径
        batch_size: 批处理大小
        max_workers: 并发工作线程数
        delay: 单个请求延迟（秒）
        only_unclassified: 只补全未分类的股票
    """
    
    # 读取缓存文件
    logger.info(f"读取缓存文件: {cache_file}")
    hk_df = pd.read_parquet(cache_file)
    logger.info(f"缓存文件包含 {len(hk_df)} 只港股")
    
    # 统计初始状态
    unclassified_count = (hk_df['industry'] == 'Unclassified').sum()
    logger.info(f"未分类股票数: {unclassified_count}")
    
    # 确定要更新的股票
    if only_unclassified:
        to_update = hk_df[hk_df['industry'] == 'Unclassified'].copy()
    else:
        to_update = hk_df.copy()
    
    if len(to_update) == 0:
        logger.info("✅ 所有股票均已分类，无需更新")
        return
    
    logger.info(f"需要更新 {len(to_update)} 只股票的行业分类")
    
    # 创建代码列表
    codes_to_fetch = to_update['code'].tolist()
    
    # 并发获取行业信息
    logger.info(f"开始并发获取行业分类 (workers={max_workers}, batch_size={batch_size})...")
    
    industry_map = {}  # code -> industry
    name_map = {}      # code -> name
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = {
            executor.submit(fetch_hk_stock_industry, code, delay): code 
            for code in codes_to_fetch
        }
        
        # 处理完成的任务
        completed = 0
        failed = 0
        
        for future in as_completed(futures):
            code = futures[future]
            try:
                result = future.result()
                if result:
                    industry_map[code] = result['industry']
                    name_map[code] = result['name']
                    completed += 1
                    
                    # 定期输出进度
                    if (completed + failed) % 50 == 0:
                        logger.info(f"进度: {completed + failed}/{len(codes_to_fetch)} "
                                  f"(成功: {completed}, 失败: {failed})")
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"处理代码 {code} 时出错: {e}")
                failed += 1
        
        logger.info(f"获取完成: 成功 {completed}, 失败 {failed}")
    
    # 更新原始数据框
    logger.info("更新行业分类数据...")
    
    # 使用向量化操作更新数据
    for code, industry in industry_map.items():
        code_mask = hk_df['code'] == code
        hk_df.loc[code_mask, 'industry'] = industry
        
        # 更新name字段（如果原来是代码或为空）
        if code in name_map and name_map[code]:
            original_names = hk_df.loc[code_mask, 'name'].tolist()  # type: ignore
            if original_names and original_names[0] in (code, '', None):  # type: ignore
                hk_df.loc[code_mask, 'name'] = name_map[code]
    
    # 保存更新后的数据
    logger.info(f"保存更新后的缓存文件...")
    hk_df.to_parquet(cache_file, index=False)
    logger.info(f"✅ 缓存文件已保存")
    
    # 最终统计
    final_unclassified = (hk_df['industry'] == 'Unclassified').sum()
    logger.info(f"\n=== 更新完成 ===")
    logger.info(f"原始未分类数: {unclassified_count}")
    logger.info(f"更新后未分类数: {final_unclassified}")
    logger.info(f"新增分类数: {unclassified_count - final_unclassified}")
    logger.info(f"\n行业分布:")
    print(hk_df['industry'].value_counts().head(15))


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='港股通股票行业分类补全脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  # 使用默认参数补全未分类的股票
  python enrich_hk_industry_classification.py
  
  # 增加请求延迟（避免被限流）
  python enrich_hk_industry_classification.py --delay 1.0
  
  # 增加并发工作线程
  python enrich_hk_industry_classification.py --max-workers 8
        '''
    )
    
    parser.add_argument('--batch-size', type=int, default=10,
                       help='批处理大小 (默认: 10)')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='并发工作线程数 (默认: 4)')
    parser.add_argument('--delay', type=float, default=0.5,
                       help='请求延迟，单位秒 (默认: 0.5)')
    parser.add_argument('--all', action='store_true',
                       help='更新所有股票，包括已分类的')
    
    args = parser.parse_args()
    
    # 确定缓存文件路径
    data_center_path = Path(__file__).parent / 'quant_data_center'
    cache_file = data_center_path / 'hk_stock_info_cache.parquet'
    
    if not cache_file.exists():
        logger.error(f"缓存文件不存在: {cache_file}")
        return 1
    
    print("=" * 80)
    print("🇭🇰 港股通股票行业分类补全脚本")
    print("=" * 80)
    print(f"缓存文件: {cache_file}")
    print(f"批处理大小: {args.batch_size}")
    print(f"并发线程数: {args.max_workers}")
    print(f"请求延迟: {args.delay}s")
    print(f"更新范围: {'所有股票' if args.all else '仅未分类股票'}")
    print("=" * 80)
    print()
    
    try:
        enrich_hk_industry_classification(
            cache_file,
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            delay=args.delay,
            only_unclassified=not args.all
        )
        return 0
    except Exception as e:
        logger.error(f"执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
