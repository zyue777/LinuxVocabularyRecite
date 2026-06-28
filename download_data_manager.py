#!/home/zy/miniconda3/envs/dailyreport/bin/python
# -*- coding: utf-8 -*-
"""
A股量化研究数据中心 - 核心数据管理脚本
支持增量更新，使用Parquet格式存储，基于Tushare Pro API
"""

import os
import time
import pandas as pd
import akshare as ak
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from tushare_client import get_pro, get_rate_limit, merge_adj_bars
warnings.filterwarnings('ignore')


class RateLimiter:
    """
    线程安全的限流器
    使用令牌桶算法或简单的最小间隔算法
    """
    def __init__(self, max_calls: int, period: float = 60.0):
        """
        初始化限流器
        
        Args:
            max_calls: 时间周期内允许的最大调用次数
            period: 时间周期（秒）
        """
        self.interval = period / max_calls  # 每次调用之间的最小间隔
        self.last_call = 0.0
        self.lock = threading.Lock()
    
    def wait(self):
        """等待直到可以进行下一次调用"""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.interval:
                sleep_time = self.interval - elapsed
                time.sleep(sleep_time)
            self.last_call = time.time()


class QuantDataManager:
    """
    量化数据中心管理器
    
    注意: 
    - FF5/FF3/CH3/自定义因子(UMD/LIQ)的构建功能已从本管理器中剥离
    - 无风险利率(RFR)和宏观数据功能仍保留
    - 因子构建脚本仍可在 '因子计算/' 目录下独立运行
    """
    
    def __init__(self, data_center_path: Optional[str] = None, token: Optional[str] = None):
        """
        初始化数据管理器
        
        Args:
            data_center_path: 数据中心根目录路径
            token: Tushare Pro Token，如果为None则尝试从config.py读取
        """
        # 设置数据中心路径
        if data_center_path is None:
            self.data_center_path = Path.cwd() / "quant_data_center"
        else:
            self.data_center_path = Path(data_center_path)
        
        # 初始化Tushare Pro（凭证与代理端点见 .env / tushare_client.py）
        try:
            self.pro = get_pro()
            print(f"Tushare Pro 已连接: {self.pro._DataApi__http_url}")
        except RuntimeError as e:
            print(f"警告: {e}")
            raise
        
        # 限流：按套餐上限留 10% 余量（默认 150 次/分 → 135）
        rate_limit = max(1, int(get_rate_limit() * 0.9))
        self.rate_limiter = RateLimiter(max_calls=rate_limit, period=60)
        
        # 定义路径
        self.paths = {
            'stock_basic': self.data_center_path / "stock/basic",
            'stock_daily_hfq': self.data_center_path / "stock/daily_hfq",
            'stock_daily_qfq': self.data_center_path / "stock/daily_qfq",
            'stock_moneyflow': self.data_center_path / "stock/moneyflow",
            'stock_cyq_perf': self.data_center_path / "stock/cyq_perf",  # 🆕 每日筹码分布统计数据
            'stock_hk_daily_hfq': self.data_center_path / "stock_hk/daily_hfq",  # 🆕 港股后复权
            'stock_hk_daily_qfq': self.data_center_path / "stock_hk/daily_qfq",  # 🆕 港股前复权
            'index_daily': self.data_center_path / "index/daily",
            'index_global_daily': self.data_center_path / "index/global_daily",  # 🆕 全球指数
            'index_constituents': self.data_center_path / "index/constituents",
            'index_weight': self.data_center_path / "index/weight",
            'factors_rfr': self.data_center_path / "factors/risk_free",  # 无风险利率数据（保留）
            'industry_sw': self.data_center_path / "classification/industry_sw",
            'stock_daily_basic': self.data_center_path / "stock/daily_basic",
            'stock_fina_indicator': self.data_center_path / "stock/fina_indicator",
            'market_margin_total': self.data_center_path / "market/margin_total",
            'market_margin_detail': self.data_center_path / "market/margin_detail",
            'market_hsgt': self.data_center_path / "market/hsgt",
            'financial_income': self.data_center_path / "stock/financial_tables/income",
            'financial_balancesheet': self.data_center_path / "stock/financial_tables/balancesheet",
            'financial_cashflow': self.data_center_path / "stock/financial_tables/cashflow",
            'futures_holding': self.data_center_path / "market/derivatives/futures/holding",
        }
        
        # 确保所有目录存在
        for path in self.paths.values():
            path.mkdir(parents=True, exist_ok=True)
        
        # 导入前复权转换函数
        try:
            from data_utils import convert_hfq_to_qfq
            self.convert_hfq_to_qfq = convert_hfq_to_qfq
        except ImportError:
            print("警告: 无法导入 data_utils.convert_hfq_to_qfq，前复权自动转换功能将不可用")
            self.convert_hfq_to_qfq = None
        
        print(f"数据中心管理器初始化完成")
        print(f"根目录: {self.data_center_path}")
    
    def _get_latest_date(self, file_path: Path, date_col: str = 'trade_date') -> Optional[str]:
        """获取Parquet文件中的最新日期"""
        if not file_path.exists():
            return None
        
        try:
            # 先读取文件的所有列，检查是否存在目标列
            df = pd.read_parquet(file_path, engine='pyarrow')
            if df.empty:
                return None
            
            # 检查列是否存在
            if date_col not in df.columns:
                # 尝试查找可能的日期列（trade_date, end_date等）
                possible_date_cols = ['trade_date', 'end_date', 'ann_date', 'cal_date']
                for col in possible_date_cols:
                    if col in df.columns:
                        date_col = col
                        break
                else:
                    # 如果找不到任何日期列，返回None
                    return None
            
            # 只读取日期列以提高效率
            if date_col in df.columns:
                return df[date_col].max()
            return None
        except Exception as e:
            # 如果文件损坏或读取失败，返回None（允许重新下载）
            return None
    
    def _get_daily_real_prices(self, trade_date: str) -> Dict[str, float]:
        """
        获取指定日期的所有股票真实收盘价（不复权）
        用于计算复权因子
        """
        try:
            print(f"获取 {trade_date} 的真实收盘价数据...")
            # 使用pro.daily获取不复权数据
            df = self._safe_api_call(self.pro.daily, trade_date=trade_date)
            
            if df is not None and not df.empty:
                # 创建代码到收盘价的映射
                price_map = dict(zip(df['ts_code'], df['close']))
                print(f"  ✅ 获取到 {len(price_map)} 条价格数据")
                return price_map
            else:
                print(f"  ⚠️  {trade_date} 无交易数据（可能是非交易日或数据尚未更新）")
                print(f"     这不影响后续处理，将使用历史复权因子")
                return {}
        except Exception as e:
            print(f"  ⚠️  获取真实价格失败: {e}")
            return {}
    
    def _get_tushare_latest_date(self) -> str:
        """
        获取Tushare实际最新数据日期
        通过查询上证指数获取实际可用的最新交易日
        
        Returns:
            最新数据日期 (YYYYMMDD格式)
        """
        try:
            # 查询上证指数的最新数据，获取实际最新日期
            df_latest = self._safe_api_call(
                self.pro.index_daily,
                ts_code='000001.SH',  # 上证指数
                start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d')
            )
            
            if df_latest is not None and not df_latest.empty:
                latest_date = df_latest['trade_date'].max()
                return latest_date
            else:
                # 如果查询失败，使用昨天作为保底
                return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        except Exception as e:
            # 查询失败，使用昨天作为保底
            return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    def _safe_api_call(self, func, *args, **kwargs):
        """
        安全的API调用，包含重试机制和智能限流处理
        
        注意：
        - API返回空数据（DataFrame为空）被视为正常情况，直接返回None，不重试
        - 遇到限流错误（每分钟500次）时，自动等待60秒后重试，无限次重试直到成功
        - 其他网络错误、API异常会进行3次重试
        """
        max_retries = 3
        retry_delay = 1
        rate_limit_retries = 0  # 限流重试次数（用于显示）
        
        attempt = 0
        while attempt < max_retries:
            try:
                # 在调用API前进行限流等待
                self.rate_limiter.wait()
                
                result = func(*args, **kwargs)
                # API调用成功，无论数据是否为空都直接返回
                if result is not None and not result.empty:
                    return result
                else:
                    # 数据为空是正常情况（如股票未上市、数据未发布等），直接返回None，不重试
                    return None
            except Exception as e:
                error_msg = str(e)
                
                # 检测是否是API限流错误
                if '每天最多访问该接口' in error_msg:
                    # 每天限制 - 这是硬限制，无法通过等待解决
                    print(f"❌ API每日限制已达到: {error_msg}")
                    print(f"   建议:")
                    print(f"   1. 等待明天再继续下载")
                    print(f"   2. 升级Tushare账户以获得更高的调用限额")
                    print(f"   3. 分批下载，每天下载少量日期")
                    return None  # 直接返回None，不重试
                elif '每分钟最多访问该接口' in error_msg or 'rate limit' in error_msg.lower():
                    rate_limit_retries += 1
                    print(f"⚠️  API限流 (第{rate_limit_retries}次): {error_msg}")
                    print(f"   等待60秒后自动重试...")
                    time.sleep(60)  # 等待60秒让限流重置
                    # 不增加attempt计数，继续重试（无限次）
                    continue
                
                # 其他错误：正常重试逻辑
                attempt += 1
                print(f"API调用失败 (尝试 {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay * attempt)
                else:
                    raise e
        
        return None
    
    def _fetch_daily_hfq_worker(self, ts_code: str, start_date: str, end_date: str, stock_info: Dict[str, Any]) -> Dict[str, Any]:
        """并发工作函数：获取单只股票的后复权日K线数据"""
        try:
            file_path = self.paths['stock_daily_hfq'] / f"{ts_code}.parquet"
            
            # 确定开始日期（简化逻辑：直接获取到end_date的所有数据）
            start_date_actual = start_date
            if start_date is None or start_date == '':
                # 获取本地最新日期
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date:
                    # 检查数据是否已经是最新
                    if local_latest_date >= end_date:
                        return {'ts_code': ts_code, 'status': 'up_to_date'}
                    
                    # 从最新日期的下一天开始（Tushare API会自动处理交易日历）
                    latest_dt = datetime.strptime(local_latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    # 如果文件不存在，使用上市日期或默认日期
                    list_date = stock_info.get(ts_code)
                    start_date_actual = list_date if list_date and list_date != 'None' else '19900101'

            if start_date_actual > end_date:
                return {'ts_code': ts_code, 'status': 'up_to_date'}

            # SDK 文档路径: pro.daily + pro.adj_factor（各走一次限流，等价 pro_bar(api=pro, adj='hfq')）
            daily_df = self._safe_api_call(
                self.pro.daily,
                ts_code=ts_code,
                start_date=start_date_actual,
                end_date=end_date,
            )
            if daily_df is None or daily_df.empty:
                return {'ts_code': ts_code, 'status': 'api_empty'}
            adj_df = self._safe_api_call(
                self.pro.adj_factor,
                ts_code=ts_code,
                start_date=start_date_actual,
                end_date=end_date,
            )
            df = merge_adj_bars(daily_df, adj_df, adj='hfq')
            
            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def _fetch_moneyflow_worker(self, ts_code: str, start_date: str, end_date: str, stock_info: Dict[str, Any]) -> Dict[str, Any]:
        """并发工作函数：获取单只股票的资金流向数据（增量更新）"""
        try:
            file_path = self.paths['stock_moneyflow'] / f"{ts_code}.parquet"
            
            # 确定开始日期（简化逻辑：直接获取到end_date的所有数据）
            if start_date is not None and start_date != '':
                start_date_actual = start_date
            else:
                # 获取本地最新日期
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date:
                    # 检查数据是否已经是最新
                    if local_latest_date >= end_date:
                        return {'ts_code': ts_code, 'status': 'up_to_date'}
                    
                    # 从最新日期的下一天开始（Tushare API会自动处理交易日历）
                    latest_dt = datetime.strptime(local_latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    # 如果文件不存在，使用上市日期或默认日期
                    list_date = stock_info.get(ts_code)
                    start_date_actual = list_date if list_date and list_date != 'None' else '20100101'  # 资金流数据从2010年开始

            # 获取数据（直接调用API，让Tushare处理交易日历）
            df = self._safe_api_call(self.pro.moneyflow,
                                   ts_code=ts_code,
                                   start_date=start_date_actual,
                                   end_date=end_date)
            
            if df is not None and not df.empty:
                # 验证数据格式：确保有必需的列
                required_cols = ['ts_code', 'trade_date']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    # 如果缺少必需列，记录错误但不中断
                    return {'ts_code': ts_code, 'status': 'error',
                           'message': f'API返回数据缺少必需列: {missing_cols}, 实际列: {list(df.columns)}'}
                
                # 确保ts_code列存在（如果API没有返回，则添加）
                if 'ts_code' not in df.columns:
                    df['ts_code'] = ts_code
                
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def update_stock_daily_hfq(self, start_date: Optional[str] = None, end_date: Optional[str] = None, 
                              stock_list: Optional[List[str]] = None, batch_size: int = 150, max_workers: int = 5):
        """
        更新股票日K线数据（后复权） - 支持高并发下载
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            stock_list: 股票代码列表，如果为None则获取所有股票
            batch_size: 批处理大小 (用于分批提交任务到线程池)
            max_workers: 最大并发线程数
        """
        print("=" * 60)
        print("开始更新股票日K线数据（高并发模式）")
        print(f"最大并发线程数: {max_workers}")
        print(f"注意: API 限流约 {int(get_rate_limit() * 0.9)} 次/分钟（套餐 {get_rate_limit()} 次/分）")
        print("=" * 60)
        
        # 🎯 直接获取Tushare实际最新数据日期
        print(f"📅 初始 end_date 参数: {end_date}")
        if end_date is None:
            print("查询 Tushare 实际最新数据日期...")
            end_date = self._get_tushare_latest_date()
            print(f"✅ Tushare 实际最新数据日期: {end_date}")
        else:
            print(f"使用指定的 end_date: {end_date}")
        
        # 获取股票列表
        if stock_list is None:
            print("获取股票列表...")
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            exchange='', 
                                            list_status='L', 
                                            fields='ts_code,list_date')
            if stock_basic_df is None:
                print("获取股票列表失败")
                return
            stock_list = stock_basic_df['ts_code'].tolist()
            stock_info = dict(zip(stock_basic_df['ts_code'], stock_basic_df['list_date']))
        else:
            # 如果提供了股票列表，也需要获取上市日期信息
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            ts_code=','.join(stock_list),
                                            fields='ts_code,list_date')
            stock_info = dict(zip(stock_basic_df['ts_code'], stock_basic_df['list_date'])) if stock_basic_df is not None else {}
        
        # 类型守护：保证stock_list和end_date不是None
        assert stock_list is not None
        assert end_date is not None  # end_date在前面已经确保有值
        print(f"共需更新 {len(stock_list)} 只股票")
        
        # 🚀 优化：全局预检查 - 采样检查是否需要更新
        print(f"\n检查本地数据状态（采样检查前100只股票）...")
        print(f"目标更新日期: {end_date}")
        hfq_dir = self.paths['stock_daily_hfq']
        sample_size = min(100, len(stock_list))
        sample_stocks = stock_list[:sample_size]
        
        up_to_date_count_sample = 0
        for ts_code in sample_stocks:
            file_path = hfq_dir / f"{ts_code}.parquet"
            if file_path.exists():
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date and local_latest_date >= end_date:
                    up_to_date_count_sample += 1
        
        # 如果采样中超过95%的股票都是最新的，说明整体数据已经是最新
        if up_to_date_count_sample >= sample_size * 0.95:
            print(f"✅ 采样检查: {up_to_date_count_sample}/{sample_size} 只股票数据已是最新")
            print(f"✅ 数据已经更新到 {end_date}，无需重复下载")
            print(f"\n跳过批量下载，直接进行前复权数据同步...")
            
            # 直接进行前复权数据同步
            if self.convert_hfq_to_qfq:
                print("\n" + "=" * 60)
                print("检查前复权数据完整性...")
                print("=" * 60)
                self._sync_qfq_data()
            
            # 生成报告（所有股票都是up_to_date）
            self._generate_download_report(
                data_type='股票日K线(后复权)',
                total_count=len(stock_list),
                success_count=0,
                up_to_date_count=len(stock_list),
                empty_stocks=[],
                failed_stocks=[]
            )
            return
        else:
            print(f"📊 采样检查: {up_to_date_count_sample}/{sample_size} 只股票已是最新，继续检查其他股票...")
        
        # 获取目标日期的真实收盘价（用于计算复权因子）
        # 只有在确实需要更新时才获取
        real_prices_map = self._get_daily_real_prices(end_date)
        
        up_to_date_stocks = []
        success_stocks = []
        empty_stocks = []
        failed_stocks = []
        batch_data_to_save = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(stock_list), batch_size):
                batch_end = min(batch_start + batch_size, len(stock_list))
                batch_stocks = stock_list[batch_start:batch_end]
                
                print(f"\n提交批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_end} ({len(batch_stocks)} 只)")
                
                futures = {executor.submit(self._fetch_daily_hfq_worker, ts_code, start_date or '', end_date, stock_info): ts_code for ts_code in batch_stocks}
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        ts_code = result['ts_code']
                        status = result['status']

                        if status == 'success':
                            batch_data_to_save.append(result['data'])
                            success_stocks.append(ts_code)
                        elif status == 'up_to_date':
                            up_to_date_stocks.append(ts_code)
                        elif status == 'api_empty':
                            empty_stocks.append(ts_code)
                        elif status == 'error':
                            failed_stocks.append((ts_code, result['message']))
                    except Exception as e:
                        # Future itself might fail
                        failed_stocks.append((futures[future], str(e)))

                if batch_data_to_save:
                    print(f"  批量保存 {len(batch_data_to_save)} 只股票的后复权数据...")
                    stocks_to_convert = []  # 记录需要转换前复权的股票
                    
                    for df, file_path in batch_data_to_save:
                        try:
                            # 保存后复权数据
                            if file_path.exists():
                                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                                
                                # 检查是否需要更新前复权数据
                                # 获取后复权数据的最新日期
                                hfq_latest_date = combined_df['trade_date'].max()
                                
                                # 检查前复权文件是否存在以及是否需要更新
                                qfq_file = self.paths['stock_daily_qfq'] / f"{file_path.stem}.parquet"
                                need_convert = False
                                
                                if not qfq_file.exists():
                                    # 前复权文件不存在，需要转换
                                    need_convert = True
                                else:
                                    # 检查前复权文件的最新日期
                                    try:
                                        qfq_df = pd.read_parquet(qfq_file, engine='pyarrow')
                                        if not qfq_df.empty and 'trade_date' in qfq_df.columns:
                                            qfq_latest_date = qfq_df['trade_date'].max()
                                            # 如果后复权数据更新了，需要重新转换
                                            if hfq_latest_date > qfq_latest_date:
                                                need_convert = True
                                        else:
                                            need_convert = True
                                    except:
                                        # 如果读取失败，重新转换
                                        need_convert = True
                                
                                if need_convert:
                                    stocks_to_convert.append(file_path.stem)
                            else:
                                # 新文件，直接保存
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                                # 新文件需要转换前复权
                                stocks_to_convert.append(file_path.stem)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    
                    # 批量转换前复权数据（增量更新）
                    if stocks_to_convert and self.convert_hfq_to_qfq:
                        print(f"  转换 {len(stocks_to_convert)} 只股票的前复权数据（增量更新）...")
                        self._batch_convert_to_qfq(stocks_to_convert, real_prices_map=real_prices_map)
                    
                    batch_data_to_save.clear()
                
                # 限流：每批次之间等待时间
                # 由于已经实现了全局限流器，这里不再需要强制等待
                # time.sleep(0.4)

        # 最后进行一次完整的前复权数据检查和补充（确保数据完整性）
        if self.convert_hfq_to_qfq:
            print("\n" + "=" * 60)
            print("检查前复权数据完整性...")
            print("=" * 60)
            self._sync_qfq_data()

        self._generate_download_report(
            data_type='股票日K线(后复权)',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )

    def _fetch_hk_daily_hfq_worker(self, ts_code: str, start_date: str, end_date: str, stock_info: Dict[str, Any]) -> Dict[str, Any]:
        """并发工作函数：获取单只港股的后复权日K线数据 (使用 Akshare)"""
        return self._fetch_hk_daily_worker_generic(ts_code, start_date, end_date, stock_info, 'hfq', self.paths['stock_hk_daily_hfq'])

    def update_hk_stock_daily_hfq(self, start_date: Optional[str] = None, end_date: Optional[str] = None, 
                                 stock_list: Optional[List[str]] = None, batch_size: int = 150, max_workers: int = 5):
        """
        更新港股通个股日K线数据（后复权）
        使用 Tushare 获取列表，Akshare 获取复权数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            stock_list: 股票代码列表，如果为None则获取所有港股
            batch_size: 批处理大小
            max_workers: 最大并发线程数
        """
        print("=" * 60)
        print("开始更新港股通个股日K线数据（高并发模式 - Akshare源）")
        print(f"最大并发线程数: {max_workers}")
        print("=" * 60)
        
        # 确保目录存在
        self.paths['stock_hk_daily_hfq'].mkdir(parents=True, exist_ok=True)
        
        # 🎯 直接获取Tushare实际最新数据日期
        if end_date is None:
            print("查询 Tushare 实际最新数据日期...")
            end_date = self._get_tushare_latest_date()
            print(f"✅ Tushare 实际最新数据日期: {end_date}")
        
        # 获取港股列表 (使用 Tushare)
        if stock_list is None:
            print("获取港股列表 (Tushare)...")
            # 使用 hk_basic 获取港股列表
            stock_basic_df = self._safe_api_call(self.pro.hk_basic, list_status='L')
            
            if stock_basic_df is None:
                print("获取港股列表失败")
                return
            stock_list = stock_basic_df['ts_code'].tolist()
            stock_info = dict(zip(stock_basic_df['ts_code'], stock_basic_df['list_date']))
        else:
            stock_info = {} # 如果指定列表，暂时不获取上市日期，使用默认
        
        # 类型守护
        assert stock_list is not None
        assert end_date is not None
        print(f"共需更新 {len(stock_list)} 只港股")
        
        # 🚀 优化：全局预检查
        print(f"\n检查本地数据状态（采样检查前100只股票）...")
        print(f"目标更新日期: {end_date}")
        hfq_dir = self.paths['stock_hk_daily_hfq']
        sample_size = min(100, len(stock_list))
        sample_stocks = stock_list[:sample_size]
        
        up_to_date_count_sample = 0
        for ts_code in sample_stocks:
            file_path = hfq_dir / f"{ts_code}.parquet"
            if file_path.exists():
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date and local_latest_date >= end_date:
                    up_to_date_count_sample += 1
        
        if up_to_date_count_sample >= sample_size * 0.95:
            print(f"✅ 采样检查: {up_to_date_count_sample}/{sample_size} 只股票数据已是最新")
            print(f"✅ 数据已经更新到 {end_date}，无需重复下载")
            return
        else:
            print(f"📊 采样检查: {up_to_date_count_sample}/{sample_size} 只股票已是最新，继续检查其他股票...")
        
        # 港股后复权下载
        self._process_hk_download_batch(stock_list, start_date, end_date, stock_info, 'hfq', self.paths['stock_hk_daily_hfq'], max_workers, batch_size, "后复权")

    def _process_hk_download_batch(self, stock_list, start_date, end_date, stock_info, adj, save_dir, max_workers, batch_size, label):
        """处理港股批量下载"""
        success_stocks = []
        up_to_date_stocks = []
        empty_stocks = []
        failed_stocks = []
        batch_data_to_save = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(stock_list), batch_size):
                batch_end = min(batch_start + batch_size, len(stock_list))
                batch_stocks = stock_list[batch_start:batch_end]
                
                print(f"\n[{label}] 提交批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_end}")
                
                futures = {executor.submit(self._fetch_hk_daily_worker_generic, ts_code, start_date, end_date, stock_info, adj, save_dir): ts_code for ts_code in batch_stocks}
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        ts_code = result['ts_code']
                        status = result['status']

                        if status == 'success':
                            batch_data_to_save.append(result['data'])
                            success_stocks.append(ts_code)
                        elif status == 'up_to_date':
                            up_to_date_stocks.append(ts_code)
                        elif status == 'api_empty':
                            empty_stocks.append(ts_code)
                        elif status == 'error':
                            failed_stocks.append((ts_code, result['message']))
                    except Exception as e:
                        failed_stocks.append((futures[future], str(e)))

                if batch_data_to_save:
                    print(f"  批量保存 {len(batch_data_to_save)} 只港股的数据...")
                    for df, file_path in batch_data_to_save:
                        try:
                            if file_path.exists():
                                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    batch_data_to_save.clear()

        self._generate_download_report(
            data_type='港股日K线(后复权)',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )
    def _fetch_hk_daily_worker_generic(self, ts_code: str, start_date: str, end_date: str, stock_info: Dict[str, Any], adj: str, save_dir: Path) -> Dict[str, Any]:
        """通用的港股日线下载Worker (Akshare)"""
        try:
            file_path = save_dir / f"{ts_code}.parquet"
            
            start_date_actual = start_date
            if start_date is None or start_date == '':
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date:
                    if local_latest_date >= end_date:
                        return {'ts_code': ts_code, 'status': 'up_to_date'}
                    latest_dt = datetime.strptime(local_latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    list_date = stock_info.get(ts_code)
                    start_date_actual = list_date if list_date and list_date != 'None' else '20100101'

            # Akshare 代码转换: 00700.HK -> 00700
            symbol = ts_code.split('.')[0]
            
            # Akshare 获取数据 (一次性获取所有历史数据)
            # adjust: "qfq", "hfq", ""
            ak_adj = adj if adj in ['hfq', 'qfq'] else ""
            
            try:
                df = ak.stock_hk_daily(symbol=symbol, adjust=ak_adj)
            except Exception as e:
                # Akshare 可能会抛出异常如果股票代码不对或无数据
                return {'ts_code': ts_code, 'status': 'error', 'message': f"Akshare error: {str(e)}"}

            if df is not None and not df.empty:
                # 标准化列名
                # Akshare columns: date, open, high, low, close, volume
                df = df.rename(columns={
                    'date': 'trade_date',
                    'volume': 'vol'
                })
                
                # 格式化日期 YYYY-MM-DD -> YYYYMMDD
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
                
                # 筛选日期范围
                df = df[(df['trade_date'] >= start_date_actual) & (df['trade_date'] <= end_date)]
                
                if df.empty:
                     return {'ts_code': ts_code, 'status': 'api_empty'}

                # 添加 ts_code
                df['ts_code'] = ts_code
                
                # 补充缺失列 (amount, pct_chg)
                if 'amount' not in df.columns:
                    df['amount'] = df['close'] * df['vol'] # 估算成交额
                
                if 'pct_chg' not in df.columns:
                    df['pct_chg'] = df['close'].pct_change() * 100
                    df['pct_chg'] = df['pct_chg'].fillna(0)

                # 确保列顺序和类型
                cols = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
                # 补齐其他可能缺失的列
                for col in cols:
                    if col not in df.columns:
                        df[col] = 0.0 # 或者 np.nan
                
                # 简单计算 change 和 pre_close 如果缺失
                if 'pre_close' not in df.columns or (df['pre_close'] == 0).all():
                     df['pre_close'] = df['close'].shift(1)
                     df['pre_close'] = df['pre_close'].fillna(df['open']) # 第一天用开盘价代替
                
                if 'change' not in df.columns or (df['change'] == 0).all():
                    df['change'] = df['close'] - df['pre_close']

                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def _batch_convert_to_qfq(self, stock_list: List[str], max_workers: int = 4, real_prices_map: Optional[Dict[str, float]] = None):
        """
        批量转换后复权数据为前复权数据（增量更新）
        
        Args:
            stock_list: 需要转换的股票代码列表
            max_workers: 最大并发线程数
            real_prices_map: 真实收盘价映射 {ts_code: close}，用于加速计算
        """
        if not self.convert_hfq_to_qfq:
            return
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        qfq_dir = self.paths['stock_daily_qfq']
        qfq_dir.mkdir(parents=True, exist_ok=True)
        
        success_count = 0
        append_count = 0
        rewrite_count = 0
        failed_count = 0
        
        def convert_worker(ts_code: str):
            """转换工作函数"""
            try:
                hfq_file = self.paths['stock_daily_hfq'] / f"{ts_code}.parquet"
                if not hfq_file.exists():
                    return {'ts_code': ts_code, 'status': 'error', 'message': 'HFQ文件不存在'}
                
                # 读取HFQ数据
                df_hfq = pd.read_parquet(hfq_file, engine='pyarrow')
                if df_hfq.empty:
                    return {'ts_code': ts_code, 'status': 'empty'}
                
                df_hfq = df_hfq.sort_values('trade_date').reset_index(drop=True)
                hfq_latest_date = df_hfq.iloc[-1]['trade_date']
                hfq_latest_close = df_hfq.iloc[-1]['close']
                
                # 获取真实收盘价
                real_close = None
                if real_prices_map and ts_code in real_prices_map:
                    # 只有当HFQ最新日期与Map日期一致时才使用Map中的价格
                    # 这里假设Map是基于最新交易日的，如果HFQ日期较旧（如停牌），则不能使用Map
                    # 但为了简化，如果Map中有且日期匹配（或者我们信任Map是针对end_date的），可以尝试
                    # 更严谨的做法是：如果HFQ最新日期 == Map的日期（通常是end_date），则使用
                    # 但这里我们没有Map的日期信息，只能假设调用者传入的是正确的
                    real_close = real_prices_map[ts_code]
                
                # 如果Map中没有，或者我们需要针对特定日期获取
                if real_close is None:
                    # 尝试单独获取该日期的真实价格
                    try:
                        daily_df = self._safe_api_call(self.pro.daily, ts_code=ts_code, start_date=hfq_latest_date, end_date=hfq_latest_date)
                        if daily_df is not None and not daily_df.empty:
                            real_close = daily_df.iloc[0]['close']
                    except:
                        pass
                
                if real_close is None:
                    return {'ts_code': ts_code, 'status': 'error', 'message': '无法获取真实收盘价，无法计算复权因子'}
                
                # 计算新的复权因子
                new_adj_factor = real_close / hfq_latest_close
                
                # 检查是否可以增量更新
                qfq_file = qfq_dir / f"{ts_code}.parquet"
                can_append = False
                last_date = None
                df_qfq_old = pd.DataFrame()
                
                if qfq_file.exists():
                    try:
                        df_qfq_old = pd.read_parquet(qfq_file, engine='pyarrow')
                        if not df_qfq_old.empty:
                            df_qfq_old = df_qfq_old.sort_values('trade_date')
                            qfq_last_date = df_qfq_old.iloc[-1]['trade_date']
                            qfq_last_close = df_qfq_old.iloc[-1]['close']
                            
                            # 如果QFQ已经包含最新日期，则无需更新
                            if qfq_last_date >= hfq_latest_date:
                                return {'ts_code': ts_code, 'status': 'up_to_date'}
                            
                            # 找到HFQ中对应qfq_last_date的收盘价
                            hfq_match = df_hfq[df_hfq['trade_date'] == qfq_last_date]
                            if not hfq_match.empty:
                                hfq_close_at_match = hfq_match.iloc[0]['close']
                                # 计算旧的复权因子
                                old_adj_factor = qfq_last_close / hfq_close_at_match
                                
                                # 比较因子是否变化（允许微小误差）
                                if abs(new_adj_factor - old_adj_factor) < 1e-6:
                                    can_append = True
                                    last_date = qfq_last_date
                    except:
                        # 读取失败，只能重写
                        pass
                
                if can_append:
                    # 增量追加模式
                    # 获取需要追加的HFQ行
                    new_rows = df_hfq[df_hfq['trade_date'] > last_date].copy()
                    if new_rows.empty:
                        return {'ts_code': ts_code, 'status': 'up_to_date'}
                    
                    # 应用复权因子
                    price_fields = ['open', 'high', 'low', 'close', 'pre_close']
                    for field in price_fields:
                        if field in new_rows.columns:
                            new_rows[field] = new_rows[field] * new_adj_factor
                    
                    # 追加保存
                    combined_df = pd.concat([df_qfq_old, new_rows], ignore_index=True)
                    combined_df.to_parquet(qfq_file, engine='pyarrow', index=False)
                    return {'ts_code': ts_code, 'status': 'success_append'}
                else:
                    # 全量重写模式
                    # 调用data_utils中的转换函数
                    if self.convert_hfq_to_qfq:
                        df_qfq, _ = self.convert_hfq_to_qfq(ts_code, str(self.data_center_path), latest_real_price=real_close)
                    else:
                        return {'ts_code': ts_code, 'status': 'error', 'message': '转换函数未加载'}
                    
                    if df_qfq is not None:
                        df_qfq.to_parquet(qfq_file, engine='pyarrow', index=False)
                        return {'ts_code': ts_code, 'status': 'success_rewrite'}
                    else:
                        return {'ts_code': ts_code, 'status': 'error', 'message': '转换失败'}
                        
            except Exception as e:
                return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
        
        # 并发转换
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(convert_worker, ts_code): ts_code for ts_code in stock_list}
            
            for future in as_completed(futures):
                result = future.result()
                status = result['status']
                
                if status == 'success_append':
                    success_count += 1
                    append_count += 1
                elif status == 'success_rewrite':
                    success_count += 1
                    rewrite_count += 1
                elif status == 'up_to_date':
                    pass
                else:
                    failed_count += 1
                    if status == 'error':
                        print(f"    ⚠️  转换 {result['ts_code']} 失败: {result.get('message', '未知错误')}")
        
        if success_count > 0:
            print(f"  ✅ 成功处理 {success_count} 只股票的前复权数据 (追加: {append_count}, 重写: {rewrite_count})")
        if failed_count > 0:
            print(f"  ⚠️  {failed_count} 只股票转换失败或数据为空")
    
    def _sync_qfq_data(self):
        """
        同步前复权数据：检查所有后复权文件，确保对应的前复权文件存在且最新
        只处理缺失或需要更新的文件，实现增量更新
        """
        if not self.convert_hfq_to_qfq:
            return
        
        hfq_dir = self.paths['stock_daily_hfq']
        qfq_dir = self.paths['stock_daily_qfq']
        qfq_dir.mkdir(parents=True, exist_ok=True)
        
        if not hfq_dir.exists():
            return
        
        print("检查前复权数据完整性...")
        
        # 获取所有后复权文件
        hfq_files = list(hfq_dir.glob("*.parquet"))
        total_count = len(hfq_files)
        
        if total_count == 0:
            print("  无后复权数据文件")
            return
        
        # 检查哪些需要转换或更新
        stocks_to_convert = []
        up_to_date_count = 0
        
        for hfq_file in hfq_files:
            ts_code = hfq_file.stem
            qfq_file = qfq_dir / f"{ts_code}.parquet"
            
            need_convert = False
            
            if not qfq_file.exists():
                # 前复权文件不存在
                need_convert = True
            else:
                # 检查日期是否最新
                try:
                    # 读取后复权文件的最新日期
                    hfq_df = pd.read_parquet(hfq_file, engine='pyarrow')
                    if hfq_df.empty or 'trade_date' not in hfq_df.columns:
                        continue
                    
                    hfq_latest_date = hfq_df['trade_date'].max()
                    
                    # 读取前复权文件的最新日期
                    qfq_df = pd.read_parquet(qfq_file, engine='pyarrow')
                    if qfq_df.empty or 'trade_date' not in qfq_df.columns:
                        need_convert = True
                    else:
                        qfq_latest_date = qfq_df['trade_date'].max()
                        # 如果后复权数据更新了，需要重新转换
                        if hfq_latest_date > qfq_latest_date:
                            need_convert = True
                        else:
                            up_to_date_count += 1
                except Exception as e:
                    # 读取失败，重新转换
                    need_convert = True
            
            if need_convert:
                stocks_to_convert.append(ts_code)
        
        if stocks_to_convert:
            print(f"  发现 {len(stocks_to_convert)} 只股票需要转换/更新前复权数据")
            print(f"  {up_to_date_count} 只股票前复权数据已是最新")
            
            # 批量转换（使用较小的并发数，避免内存压力）
            self._batch_convert_to_qfq(stocks_to_convert, max_workers=4)
        else:
            print(f"  ✅ 所有 {total_count} 只股票的前复权数据已是最新")
    
    def _fetch_income_worker(self, ts_code: str) -> Dict[str, Any]:
        """并发工作函数：获取单只股票的利润表数据"""
        try:
            table_path = self.paths['stock_financial_tables'] / 'income'
            table_path.mkdir(parents=True, exist_ok=True)
            file_path = table_path / f"{ts_code}.parquet"
            
            # 确定开始日期（基于现有数据）
            start_date_actual = None
            if file_path.exists():
                latest_date = self._get_latest_date(file_path, 'end_date')
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            
            end_date = datetime.now().strftime('%Y%m%d')
            
            if start_date_actual and start_date_actual >= end_date:
                return {'ts_code': ts_code, 'status': 'up_to_date'}

            # 获取数据
            df = self._safe_api_call(self.pro.income,
                                   ts_code=ts_code,
                                   start_date=start_date_actual if start_date_actual else '19900101',
                                   end_date=end_date)
            
            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def _fetch_balancesheet_worker(self, ts_code: str) -> Dict[str, Any]:
        """并发工作函数：获取单只股票的资产负债表数据"""
        try:
            table_path = self.paths['stock_financial_tables'] / 'balancesheet'
            table_path.mkdir(parents=True, exist_ok=True)
            file_path = table_path / f"{ts_code}.parquet"
            
            # 确定开始日期（基于现有数据）
            start_date_actual = None
            if file_path.exists():
                latest_date = self._get_latest_date(file_path, 'end_date')
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            
            end_date = datetime.now().strftime('%Y%m%d')
            
            if start_date_actual and start_date_actual >= end_date:
                return {'ts_code': ts_code, 'status': 'up_to_date'}

            # 获取数据
            df = self._safe_api_call(self.pro.balancesheet,
                                   ts_code=ts_code,
                                   start_date=start_date_actual if start_date_actual else '19900101',
                                   end_date=end_date)
            
            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def _fetch_cashflow_worker(self, ts_code: str) -> Dict[str, Any]:
        """并发工作函数：获取单只股票的现金流量表数据"""
        try:
            table_path = self.paths['stock_financial_tables'] / 'cashflow'
            table_path.mkdir(parents=True, exist_ok=True)
            file_path = table_path / f"{ts_code}.parquet"
            
            # 确定开始日期（基于现有数据）
            start_date_actual = None
            if file_path.exists():
                latest_date = self._get_latest_date(file_path, 'end_date')
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            
            end_date = datetime.now().strftime('%Y%m%d')
            
            if start_date_actual and start_date_actual >= end_date:
                return {'ts_code': ts_code, 'status': 'up_to_date'}

            # 获取数据
            df = self._safe_api_call(self.pro.cashflow,
                                   ts_code=ts_code,
                                   start_date=start_date_actual if start_date_actual else '19900101',
                                   end_date=end_date)
            
            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def update_income_table(self, stock_list: Optional[List[str]] = None, batch_size: int = 50, max_workers: int = 1):
        """
        更新利润表数据 - 支持批量下载
        
        Args:
            stock_list: 股票代码列表，如果为None则获取所有股票
            batch_size: 批处理大小
            max_workers: 最大并发线程数
        """
        print("=" * 60)
        print("开始更新利润表数据")
        print(f"最大并发线程数: {max_workers}")
        print("=" * 60)
        
        # 获取股票列表
        if stock_list is None:
            print("获取股票列表...")
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            exchange='', 
                                            list_status='L', 
                                            fields='ts_code')
            if stock_basic_df is None:
                print("获取股票列表失败")
                return
            stock_list = stock_basic_df['ts_code'].tolist()
        
        # 类型守护：保证stock_list不是None
        assert stock_list is not None
        print(f"共需更新 {len(stock_list)} 只股票")
        
        up_to_date_stocks = []
        success_stocks = []
        empty_stocks = []
        failed_stocks = []
        batch_data_to_save = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(stock_list), batch_size):
                batch_end = min(batch_start + batch_size, len(stock_list))
                batch_stocks = stock_list[batch_start:batch_end]
                
                print(f"\n提交批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_end} ({len(batch_stocks)} 只)")
                
                futures = {executor.submit(self._fetch_income_worker, ts_code): ts_code for ts_code in batch_stocks}
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        ts_code = result['ts_code']
                        status = result['status']

                        if status == 'success':
                            batch_data_to_save.append(result['data'])
                            success_stocks.append(ts_code)
                        elif status == 'up_to_date':
                            up_to_date_stocks.append(ts_code)
                        elif status == 'api_empty':
                            empty_stocks.append(ts_code)
                        elif status == 'error':
                            failed_stocks.append((ts_code, result['message']))
                    except Exception as e:
                        failed_stocks.append((futures[future], str(e)))

                if batch_data_to_save:
                    print(f"  批量保存 {len(batch_data_to_save)} 只股票的数据...")
                    for df, file_path in batch_data_to_save:
                        try:
                            if file_path.exists():
                                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['ts_code', 'end_date']).sort_values('end_date')
                                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df = df.drop_duplicates(subset=['ts_code', 'end_date']).sort_values('end_date')
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    batch_data_to_save.clear()
                
                # 限流：每批次之间等待时间（控制API调用频率）
                time.sleep(15)  # 每批次等待15秒，确保不超过200次/分钟

        self._generate_download_report(
            data_type='利润表',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )
    
    def update_balancesheet_table(self, stock_list: Optional[List[str]] = None, batch_size: int = 50, max_workers: int = 1):
        """
        更新资产负债表数据 - 支持批量下载
        
        Args:
            stock_list: 股票代码列表，如果为None则获取所有股票
            batch_size: 批处理大小
            max_workers: 最大并发线程数
        """
        print("=" * 60)
        print("开始更新资产负债表数据")
        print(f"最大并发线程数: {max_workers}")
        print("=" * 60)
        
        # 获取股票列表
        if stock_list is None:
            print("获取股票列表...")
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            exchange='', 
                                            list_status='L', 
                                            fields='ts_code')
            if stock_basic_df is None:
                print("获取股票列表失败")
                return
            stock_list = stock_basic_df['ts_code'].tolist()
        
        # 类型守护：保证stock_list不是None
        assert stock_list is not None
        print(f"共需更新 {len(stock_list)} 只股票")
        
        up_to_date_stocks = []
        success_stocks = []
        empty_stocks = []
        failed_stocks = []
        batch_data_to_save = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(stock_list), batch_size):
                batch_end = min(batch_start + batch_size, len(stock_list))
                batch_stocks = stock_list[batch_start:batch_end]
                
                print(f"\n提交批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_end} ({len(batch_stocks)} 只)")
                
                futures = {executor.submit(self._fetch_balancesheet_worker, ts_code): ts_code for ts_code in batch_stocks}
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        ts_code = result['ts_code']
                        status = result['status']

                        if status == 'success':
                            batch_data_to_save.append(result['data'])
                            success_stocks.append(ts_code)
                        elif status == 'up_to_date':
                            up_to_date_stocks.append(ts_code)
                        elif status == 'api_empty':
                            empty_stocks.append(ts_code)
                        elif status == 'error':
                            failed_stocks.append((ts_code, result['message']))
                    except Exception as e:
                        failed_stocks.append((futures[future], str(e)))

                if batch_data_to_save:
                    print(f"  批量保存 {len(batch_data_to_save)} 只股票的数据...")
                    for df, file_path in batch_data_to_save:
                        try:
                            if file_path.exists():
                                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['ts_code', 'end_date']).sort_values('end_date')
                                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df = df.drop_duplicates(subset=['ts_code', 'end_date']).sort_values('end_date')
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    batch_data_to_save.clear()
                
                # 限流：每批次之间等待时间（控制API调用频率）
                time.sleep(15)  # 每批次等待15秒，确保不超过200次/分钟

        self._generate_download_report(
            data_type='资产负债表',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )
    
    def update_cashflow_table(self, stock_list: Optional[List[str]] = None, batch_size: int = 50, max_workers: int = 1):
        """
        更新现金流量表数据 - 支持批量下载
        
        Args:
            stock_list: 股票代码列表，如果为None则获取所有股票
            batch_size: 批处理大小
            max_workers: 最大并发线程数
        """
        print("=" * 60)
        print("开始更新现金流量表数据")
        print(f"最大并发线程数: {max_workers}")
        print("=" * 60)
        
        # 获取股票列表
        if stock_list is None:
            print("获取股票列表...")
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            exchange='', 
                                            list_status='L', 
                                            fields='ts_code')
            if stock_basic_df is None:
                print("获取股票列表失败")
                return
            stock_list = stock_basic_df['ts_code'].tolist()
        
        # 类型守护：保证stock_list不是None
        assert stock_list is not None
        print(f"共需更新 {len(stock_list)} 只股票")
        
        up_to_date_stocks = []
        success_stocks = []
        empty_stocks = []
        failed_stocks = []
        batch_data_to_save = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(stock_list), batch_size):
                batch_end = min(batch_start + batch_size, len(stock_list))
                batch_stocks = stock_list[batch_start:batch_end]
                
                print(f"\n提交批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_end} ({len(batch_stocks)} 只)")
                
                futures = {executor.submit(self._fetch_cashflow_worker, ts_code): ts_code for ts_code in batch_stocks}
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        ts_code = result['ts_code']
                        status = result['status']

                        if status == 'success':
                            batch_data_to_save.append(result['data'])
                            success_stocks.append(ts_code)
                        elif status == 'up_to_date':
                            up_to_date_stocks.append(ts_code)
                        elif status == 'api_empty':
                            empty_stocks.append(ts_code)
                        elif status == 'error':
                            failed_stocks.append((ts_code, result['message']))
                    except Exception as e:
                        failed_stocks.append((futures[future], str(e)))

                if batch_data_to_save:
                    print(f"  批量保存 {len(batch_data_to_save)} 只股票的数据...")
                    for df, file_path in batch_data_to_save:
                        try:
                            if file_path.exists():
                                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['ts_code', 'end_date']).sort_values('end_date')
                                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df = df.drop_duplicates(subset=['ts_code', 'end_date']).sort_values('end_date')
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    batch_data_to_save.clear()
                
                # 限流：每批次之间等待时间（控制API调用频率）
                time.sleep(15)  # 每批次等待15秒，确保不超过200次/分钟

        self._generate_download_report(
            data_type='现金流量表',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )
    
    def update_index_constituents(self, index_codes: Optional[List[str]] = None, 
                                start_year: int = 2010):
        """
        更新指数历史成分股数据（月度）- 支持增量更新
        
        Args:
            index_codes: 指数代码列表
            start_year: 开始年份
        """
        print("=" * 60)
        print("开始更新指数历史成分股数据（增量更新）")
        print("=" * 60)
        
        if index_codes is None:
            index_codes = ['399300.SZ', '000905.SH', '399006.SZ']  # 沪深300、中证500、创业板指
        
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        for index_code in index_codes:
            print(f"\n处理指数: {index_code}")
            file_path = self.paths['index_constituents'] / f"{index_code}_const.parquet"
            
            # 检查现有数据，确定需要更新的月份
            months_to_update = []
            
            if file_path.exists():
                # 读取现有数据，确定最新日期
                try:
                    df_existing = pd.read_parquet(file_path, engine='pyarrow')
                    if not df_existing.empty and 'trade_date' in df_existing.columns:
                        latest_date = df_existing['trade_date'].max()
                        latest_year = int(latest_date[:4])
                        latest_month = int(latest_date[4:6])
                        
                        print(f"  现有数据最新: {latest_year}年{latest_month}月")
                        
                        # 计算需要更新的月份
                        if latest_year < current_year:
                            # 需要更新到当前年份
                            for year in range(latest_year, current_year + 1):
                                if year == latest_year:
                                    # 从最新月份的下一个月开始
                                    start_month = latest_month + 1
                                    end_month = 12
                                elif year == current_year:
                                    # 到当前月份
                                    start_month = 1
                                    end_month = current_month
                                else:
                                    # 完整年份
                                    start_month = 1
                                    end_month = 12
                                
                                for month in range(start_month, end_month + 1):
                                    months_to_update.append((year, month))
                        elif latest_year == current_year and latest_month < current_month:
                            # 同一年内，从下一个月开始
                            for month in range(latest_month + 1, current_month + 1):
                                months_to_update.append((current_year, month))
                        
                        if not months_to_update:
                            print(f"  {index_code}: 数据已是最新，无需更新")
                            continue
                        else:
                            print(f"  需要更新 {len(months_to_update)} 个月的数据")
                    else:
                        # 文件存在但为空，从头开始
                        months_to_update = [(year, month) for year in range(start_year, current_year + 1) 
                                          for month in range(1, 13) 
                                          if not (year == current_year and month > current_month)]
                        print(f"  文件为空，从头开始获取数据")
                except Exception as e:
                    print(f"  读取现有数据失败: {e}，从头开始获取")
                    months_to_update = [(year, month) for year in range(start_year, current_year + 1) 
                                      for month in range(1, 13) 
                                      if not (year == current_year and month > current_month)]
            else:
                # 文件不存在，从头开始
                months_to_update = [(year, month) for year in range(start_year, current_year + 1) 
                                  for month in range(1, 13) 
                                  if not (year == current_year and month > current_month)]
                print(f"  文件不存在，从头开始获取数据")
            
            # 获取需要更新的月份数据
            new_data = []
            for year, month in months_to_update:
                # 构造当月第一天和最后一天的日期
                start_date = f"{year}{month:02d}01"
                # 计算当月最后一天
                if month == 12:
                    end_date = f"{year}{month:02d}31"
                elif month in [4, 6, 9, 11]:
                    end_date = f"{year}{month:02d}30"
                elif month == 2:
                    # 简单处理闰年
                    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                        end_date = f"{year}{month:02d}29"
                    else:
                        end_date = f"{year}{month:02d}28"
                else:
                    end_date = f"{year}{month:02d}31"
                
                # 检查是否是当前月份，如果是则使用当前日期作为结束日期
                current_date = datetime.now()
                if year == current_date.year and month == current_date.month:
                    end_date = current_date.strftime('%Y%m%d')
                    print(f"  获取 {year}年{month}月 数据 ({start_date} 到 {end_date}) [当前月份]...")
                else:
                    print(f"  获取 {year}年{month}月 数据 ({start_date} 到 {end_date})...")
                
                try:
                    # 使用正确的API参数：start_date 和 end_date
                    df = self._safe_api_call(self.pro.index_weight,
                                           index_code=index_code,
                                           start_date=start_date,
                                           end_date=end_date)
                    
                    if df is not None and not df.empty:
                        new_data.append(df)
                        print(f"    获取到 {len(df)} 条记录")
                    else:
                        # 如果是当前月份或未来月份，提示数据可能尚未发布
                        if year >= current_date.year and month >= current_date.month:
                            print(f"    无数据 (数据可能尚未发布，指数成分股通常有1-2周延迟)")
                        else:
                            print(f"    无数据")
                    
                    # API限制控制
                    time.sleep(0.2)
                    
                except Exception as e:
                    print(f"    获取失败: {e}")
                    continue
            
            # 合并并保存数据
            if new_data:
                new_df = pd.concat(new_data, ignore_index=True)
                
                if file_path.exists():
                    # 增量更新：合并现有数据和新数据
                    existing_df = pd.read_parquet(file_path, engine='pyarrow')
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    # 去重并排序
                    combined_df = combined_df.drop_duplicates(subset=['index_code', 'trade_date', 'con_code']).sort_values('trade_date')
                    combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                    print(f"  {index_code}: 成功增量更新 {len(new_df)} 条新记录，总计 {len(combined_df)} 条记录")
                else:
                    # 新建文件
                    new_df = new_df.drop_duplicates(subset=['index_code', 'trade_date', 'con_code']).sort_values('trade_date')
                    new_df.to_parquet(file_path, engine='pyarrow', index=False)
                    print(f"  {index_code}: 成功新建文件，保存 {len(new_df)} 条记录")
            else:
                print(f"  {index_code}: 无新数据可保存")
    
    def update_risk_free_rate(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新无风险利率数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        print("=" * 60)
        print("开始更新无风险利率数据")
        print("=" * 60)
        
        file_path = self.paths['factors_rfr'] / "rfr_daily.parquet"
        
        # 确定开始日期
        if start_date is None:
            latest_date = self._get_latest_date(file_path)
            if latest_date:
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            else:
                start_date_actual = '19900101'
        else:
            start_date_actual = start_date
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if start_date_actual >= end_date:
            print("无风险利率数据已是最新")
            return
        
        print(f"获取 {start_date_actual} 到 {end_date} 的无风险利率数据")
        
        try:
            # 尝试获取Shibor数据
            df = self._safe_api_call(self.pro.shibor,
                                   start_date=start_date_actual,
                                   end_date=end_date)
            
            if df is not None and not df.empty:
                # 重命名Shibor数据的列名
                df = df.rename(columns={'date': 'trade_date'})
                # 使用3M期限作为无风险利率
                df = df[['trade_date', '3m']].copy()
                df.columns = ['trade_date', 'rf']
            
            if df is None or df.empty:
                print("Shibor数据获取失败，尝试LPR数据...")
                # 备用方案：LPR数据
                df = self._safe_api_call(self.pro.shibor_lpr,
                                       start_date=start_date_actual,
                                       end_date=end_date,
                                       fields='date,1y')
                
                if df is not None and not df.empty:
                    # 重命名列以保持一致性
                    df = df.rename(columns={'date': 'trade_date', '1y': 'rf'})
            
            if df is None or df.empty:
                print("无风险利率数据获取失败")
                return
            
            # 保存数据
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['trade_date']).sort_values('trade_date')
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                df.to_parquet(file_path, engine='pyarrow', index=False)
            
            print(f"成功更新 {len(df)} 条无风险利率记录")
            
        except Exception as e:
            print(f"更新无风险利率数据失败: {e}")
    
    def update_sw_industry_daily(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新申万行业分类数据（日度）
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        print("=" * 60)
        print("开始更新申万行业分类数据")
        print("=" * 60)
        
        file_path = self.paths['industry_sw'] / "sw_l1_daily.parquet"
        
        # 确定开始日期
        if start_date is None:
            latest_date = self._get_latest_date(file_path)
            if latest_date:
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            else:
                start_date_actual = '19900101'
        else:
            start_date_actual = start_date
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if start_date_actual >= end_date:
            print("申万行业分类数据已是最新")
            return
        
        print(f"获取 {start_date_actual} 到 {end_date} 的申万行业分类数据")
        
        try:
            # 注意：Tushare Pro可能没有直接的申万行业日度接口
            # 这里使用股票基本信息获取行业分类
            print("注意：Tushare Pro可能没有直接的申万行业日度接口")
            print("尝试获取股票基本信息中的行业分类...")
            
            # 获取股票基本信息
            stock_basic = self._safe_api_call(self.pro.stock_basic,
                                            exchange='',
                                            list_status='L',
                                            fields='ts_code,name,industry')
            
            if stock_basic is None or stock_basic.empty:
                print("无法获取股票基本信息")
                return
            
            # 构造行业分类数据（简化版本）
            # 这里只是示例，实际使用时需要更复杂的逻辑
            df = stock_basic.copy()
            df['trade_date'] = end_date  # 使用结束日期作为交易日期
            df['sw_l1'] = df['industry']  # 使用industry字段作为申万一级行业
            df['sw_l2'] = df['industry']  # 暂时使用相同值
            df['sw_l3'] = df['industry']  # 暂时使用相同值
            
            if df is None or df.empty:
                print("无新数据")
                return
            
            # 保存数据
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['trade_date', 'ts_code']).sort_values('trade_date')
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                df.to_parquet(file_path, engine='pyarrow', index=False)
            
            print(f"成功更新 {len(df)} 条申万行业分类记录")
            
        except Exception as e:
            print(f"更新申万行业分类数据失败: {e}")
    
    def update_sw_industry_member_latest(self):
        """
        更新申万行业成分股数据（最新数据）
        
        申万行业分类是固定的，只需要获取最新的成分股数据即可。
        流程：
        1. 获取所有申万三级行业代码（从现有数据 sw_l1_daily.parquet）
        2. 对每个三级行业代码，使用 index_member_all(l3_code='850531.SI') 获取成分股
        3. 存储为行业代码与成分股的关联关系
        
        **API**: `index_member_all` - 按三级分类提取申万行业成分
        **API限制**: 单次最大2000行，总量不限制
        
        **存储**:
        - 文件：sw_l3_member.parquet（复用现有文件）
        - 内容：申万三级行业代码与成分股的关联关系（最新数据）
        """
        print("=" * 60)
        print("开始更新申万行业成分股数据（最新）")
        print("方法: 获取所有申万三级行业，然后下载每个行业的成分股")
        print("⚠️  API限制: 单次最多2000条")
        print("=" * 60)
        
        member_file_path = self.paths['industry_sw'] / "sw_l3_member.parquet"
        
        try:
            # 1. 从现有数据获取所有申万三级行业代码
            sw_index_file = self.paths['industry_sw'] / "sw_l1_daily.parquet"
            industry_codes = []
            
            if sw_index_file.exists():
                print("从现有数据中获取申万三级行业代码...")
                df_sw = pd.read_parquet(sw_index_file, engine='pyarrow')
                # 筛选出行业指数代码（.SI结尾）
                si_data = df_sw[df_sw['ts_code'].str.endswith('.SI', na=False)]
                if not si_data.empty:
                    # 获取最新的行业代码列表
                    latest_date = si_data['trade_date'].max()
                    latest_industries = si_data[si_data['trade_date'] == latest_date]
                    industry_codes = latest_industries['ts_code'].unique().tolist()
                    print(f"  从现有数据获取到 {len(industry_codes)} 个行业代码（日期: {latest_date}）")
            
            # 如果现有数据没有，提示用户先运行 update_sw_industry_daily
            if not industry_codes:
                print("⚠️  未找到行业代码，请先运行 update_sw_industry_daily 获取行业列表")
                return
            
            print(f"\n开始获取 {len(industry_codes)} 个行业的成分股数据...")
            
            all_member_data = []
            success_count = 0
            empty_count = 0
            error_count = 0
            
            for i, l3_code in enumerate(industry_codes, 1):
                if i % 20 == 0 or i == 1:
                    print(f"  进度: {i}/{len(industry_codes)} ({i/len(industry_codes)*100:.1f}%)")
                
                try:
                    # 获取该行业的所有成分股
                    df_member = self._safe_api_call(self.pro.index_member_all,
                                                   l3_code=l3_code)
                    
                    if df_member is not None and not df_member.empty:
                        # 检查是否达到2000条限制
                        if len(df_member) >= 2000:
                            print(f"    ⚠️  警告: {l3_code} 成分股数据达到限制 ({len(df_member)} 条)")
                        
                        all_member_data.append(df_member)
                        success_count += 1
                        
                        if i % 50 == 0:
                            print(f"    已获取 {success_count} 个行业的成分股数据")
                    else:
                        empty_count += 1
                    
                    # API调用频率控制
                    time.sleep(0.1)
                    
                except Exception as e:
                    error_count += 1
                    print(f"    获取 {l3_code} 成分股失败: {e}")
                    continue
            
            # 保存数据
            print("\n" + "=" * 60)
            print("保存数据...")
            if all_member_data:
                df_all_members = pd.concat(all_member_data, ignore_index=True)
                
                # 检查数据字段，确保去重逻辑正确
                print(f"  数据字段: {df_all_members.columns.tolist()}")
                
                # 保存为最新数据（覆盖旧数据，因为申万分类是固定的）
                # 根据实际返回的字段去重
                if 'l3_code' in df_all_members.columns and 'ts_code' in df_all_members.columns:
                    df_all_members = df_all_members.drop_duplicates(subset=['l3_code', 'ts_code']).sort_values('l3_code')
                elif 'index_code' in df_all_members.columns and 'con_code' in df_all_members.columns:
                    df_all_members = df_all_members.drop_duplicates(subset=['index_code', 'con_code']).sort_values('index_code')
                else:
                    df_all_members = df_all_members.drop_duplicates().sort_values(df_all_members.columns[0])
                
                df_all_members.to_parquet(member_file_path, engine='pyarrow', index=False)
                print(f"✅ 成功更新申万行业成分股数据（最新）:")
                print(f"   总计: {len(df_all_members)} 条记录")
                print(f"   涉及 {df_all_members['l3_code'].nunique() if 'l3_code' in df_all_members.columns else 'N/A'} 个行业")
                
                print(f"\n统计:")
                print(f"   成功获取: {success_count} 个行业")
                print(f"   空数据: {empty_count} 个行业")
                print(f"   失败: {error_count} 个行业")
            else:
                print("⚠️  无成分股数据可保存")
            
            print("=" * 60)
            
        except Exception as e:
            print(f"更新申万行业成分股数据失败: {e}")
            import traceback
            traceback.print_exc()
    
    def update_sw_l2_member_history(self):
        """
        更新申万二级行业个股历史映射数据
        
        获取历史上所有申万成分股对应的二级行业代码和名称（sw_l2），
        以及其划入（in_date）和划出（out_date）日期。
        
        **API**: `index_member_all` - 按股票代码提取所属分类
        **逻辑**: 
        1. 获取所有股票列表
        2. 对每只股票调用 index_member_all(ts_code='xxx') 获取申万历史归属
        3. 提取二级行业相关数据（l2_code, l2_name, in_date, out_date）
        
        **存储**:
        - 文件：industry_sw_member.parquet
        - 内容：个股的申万L2历史归属（包含in_date和out_date）
        
        **API限制**: 单次最大2000行，总量不限制
        """
        print("=" * 60)
        print("开始更新申万二级行业个股历史映射数据")
        print("使用 API: index_member_all")
        print("⚠️  API限制: 单次最多2000条")
        print("=" * 60)
        
        member_file_path = self.paths['industry_sw'] / "industry_sw_member.parquet"
        
        try:
            # 1. 获取所有股票列表
            print("获取股票列表...")
            stock_basic = self._safe_api_call(self.pro.stock_basic,
                                            exchange='',
                                            list_status='L',
                                            fields='ts_code')
            
            if stock_basic is None or stock_basic.empty:
                print("无法获取股票列表")
                return
            
            stock_list = stock_basic['ts_code'].tolist()
            print(f"获取到 {len(stock_list)} 只股票")
            
            print(f"\n开始获取每只股票的申万L2历史归属...")
            print(f"注意: 需要逐只查询，可能需要较长时间")
            
            all_member_data = []
            success_count = 0
            empty_count = 0
            error_count = 0
            
            for i, ts_code in enumerate(stock_list, 1):
                if i % 100 == 0 or i == 1:
                    print(f"  进度: {i}/{len(stock_list)} ({i/len(stock_list)*100:.1f}%)")
                
                try:
                    # 获取该股票的申万历史归属
                    df_member = self._safe_api_call(self.pro.index_member_all,
                                                   ts_code=ts_code)
                    
                    if df_member is not None and not df_member.empty:
                        # 提取二级行业相关数据
                        # 保留所有字段，包括 l1, l2, l3 信息，但重点关注 l2
                        all_member_data.append(df_member)
                        success_count += 1
                        
                        if i % 500 == 0:
                            print(f"    已获取 {success_count} 只股票的历史归属")
                    else:
                        empty_count += 1
                    
                    # API调用频率控制
                    time.sleep(0.05)
                    
                except Exception as e:
                    error_count += 1
                    if error_count <= 10:  # 只打印前10个错误
                        print(f"    获取 {ts_code} 失败: {e}")
                    continue
            
            # 保存数据（增量更新）
            print("\n" + "=" * 60)
            print("保存数据（增量更新）...")
            if all_member_data:
                df_new_members = pd.concat(all_member_data, ignore_index=True)
                
                # 检查数据字段
                print(f"  数据字段: {df_new_members.columns.tolist()}")
                
                # 确保有必要的字段
                required_fields = ['ts_code', 'l2_code', 'l2_name', 'in_date', 'out_date']
                missing_fields = [f for f in required_fields if f not in df_new_members.columns]
                if missing_fields:
                    print(f"  ⚠️  警告: 缺少字段 {missing_fields}")
                
                # 去重：根据股票代码和行业代码去重（同一股票在同一行业应该只有一条记录）
                if 'l2_code' in df_new_members.columns:
                    df_new_members = df_new_members.drop_duplicates(subset=['ts_code', 'l2_code']).sort_values(['ts_code', 'in_date'])
                else:
                    df_new_members = df_new_members.drop_duplicates(subset=['ts_code']).sort_values('ts_code')
                
                # 增量更新逻辑
                if member_file_path.exists():
                    print("  读取现有数据...")
                    existing_df = pd.read_parquet(member_file_path, engine='pyarrow')
                    
                    # 合并数据
                    combined_df = pd.concat([existing_df, df_new_members], ignore_index=True)
                    
                    # 去重：同一股票在同一行业只保留最新数据（根据in_date判断）
                    if 'l2_code' in combined_df.columns and 'in_date' in combined_df.columns:
                        # 按ts_code和l2_code分组，保留in_date最大的记录
                        combined_df = combined_df.sort_values('in_date', na_position='last').drop_duplicates(
                            subset=['ts_code', 'l2_code'], 
                            keep='last'
                        ).sort_values(['ts_code', 'in_date'])
                    elif 'l2_code' in combined_df.columns:
                        combined_df = combined_df.drop_duplicates(subset=['ts_code', 'l2_code'], keep='last').sort_values(['ts_code'])
                    else:
                        combined_df = combined_df.drop_duplicates(subset=['ts_code'], keep='last').sort_values('ts_code')
                    
                    combined_df.to_parquet(member_file_path, engine='pyarrow', index=False)
                    print(f"✅ 成功更新申万L2历史映射数据（增量）:")
                    print(f"   本次新增/更新: {len(df_new_members)} 条记录")
                    print(f"   总计: {len(combined_df)} 条记录")
                    print(f"   涉及 {combined_df['ts_code'].nunique()} 只股票")
                    if 'l2_code' in combined_df.columns:
                        print(f"   涉及 {combined_df['l2_code'].nunique()} 个申万二级行业")
                else:
                    # 首次创建
                    df_new_members.to_parquet(member_file_path, engine='pyarrow', index=False)
                    print(f"✅ 成功创建申万L2历史映射数据:")
                    print(f"   总计: {len(df_new_members)} 条记录")
                    print(f"   涉及 {df_new_members['ts_code'].nunique()} 只股票")
                    if 'l2_code' in df_new_members.columns:
                        print(f"   涉及 {df_new_members['l2_code'].nunique()} 个申万二级行业")
                
                print(f"\n统计:")
                print(f"   成功获取: {success_count} 只股票")
                print(f"   空数据: {empty_count} 只股票")
                print(f"   失败: {error_count} 只股票")
            else:
                print("⚠️  无数据可保存")
            
            print("=" * 60)
            
        except Exception as e:
            print(f"更新申万L2历史映射数据失败: {e}")
            import traceback
            traceback.print_exc()
    
    def update_global_indices(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新全球重要指数日K线数据
        支持: HSI(恒生指数), HSTECH(恒生科技), SPX(标普500), IXIC(纳斯达克)
        """
        print("=" * 60)
        print("开始更新全球重要指数日K线数据")
        print("=" * 60)
        
        # 确保目录存在
        global_index_dir = self.paths['index_global_daily']
        global_index_dir.mkdir(parents=True, exist_ok=True)
        
        # 🎯 直接获取Tushare实际最新数据日期
        if end_date is None:
            print("查询 Tushare 实际最新数据日期...")
            end_date = self._get_tushare_latest_date()
            print(f"✅ Tushare 实际最新数据日期: {end_date}")
            
        # 定义要获取的指数列表
        # 注意：Tushare全球指数代码可能不同，这里使用标准代码，下载时映射
        # HSI: 恒生指数, HSTECH: 恒生科技, SPX: 标普500, IXIC: 纳斯达克
        indices = {
            'HSI': 'HSI',       # 恒生指数
            'HSTECH': 'HSTECH', # 恒生科技
            'SPX': 'SPX',       # 标普500
            'IXIC': 'IXIC'      # 纳斯达克
        }
        
        for name, ts_code in indices.items():
            file_path = global_index_dir / f"{name}.parquet"
            
            try:
                # 确定开始日期
                start_date_actual = start_date
                if start_date is None:
                    local_latest_date = self._get_latest_date(file_path, 'trade_date')
                    if local_latest_date:
                        # 检查数据是否已经是最新
                        if local_latest_date >= end_date:
                            print(f"  {name}: 数据已是最新")
                            continue
                        
                        latest_dt = datetime.strptime(local_latest_date, '%Y%m%d')
                        start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                    else:
                        start_date_actual = '20100101'
                
                print(f"获取 {name} ({ts_code}) 数据...")
                
                # 使用 index_global 接口获取全球指数
                df = self._safe_api_call(self.pro.index_global, 
                                       ts_code=ts_code,
                                       start_date=start_date_actual,
                                       end_date=end_date)
                
                if df is not None and not df.empty:
                    # 确保有ts_code列
                    if 'ts_code' not in df.columns:
                        df['ts_code'] = ts_code
                        
                    if file_path.exists():
                        existing_df = pd.read_parquet(file_path, engine='pyarrow')
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=['trade_date']).sort_values('trade_date')
                        combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                        new_latest_date = combined_df['trade_date'].max()
                        print(f"  {name}: 成功更新 {len(df)} 条记录，最新日期: {new_latest_date}")
                    else:
                        df = df.sort_values('trade_date')
                        df.to_parquet(file_path, engine='pyarrow', index=False)
                        latest_date = df['trade_date'].max()
                        print(f"  {name}: 成功创建文件，保存 {len(df)} 条记录，最新日期: {latest_date}")
                else:
                    print(f"  {name}: 无新数据")
                
                # API限制控制
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  {name}: 更新失败 - {e}")
                continue
    
    def update_index_daily(self, index_codes: Optional[List[str]] = None, 
                          start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新指数日K线数据
        
        Args:
            index_codes: 指数代码列表
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        print("=" * 60)
        print("开始更新指数日K线数据")
        print("=" * 60)
        
        if index_codes is None:
            index_codes = ['000001.SH', '399300.SZ', '000905.SH', '399006.SZ', '000300.SH']  # 上证指数、沪深300、中证500、创业板指、沪深300（另一个代码）
        
        # ✅ 优化：先获取 Tushare 最新交易日（避免周末/节假日无效请求）
        if end_date is None:
            print("获取 Tushare 最新交易日...")
            cal_df = self._safe_api_call(self.pro.trade_cal,
                                        exchange='SSE',
                                        is_open='1',
                                        start_date=(datetime.now() - timedelta(days=10)).strftime('%Y%m%d'),
                                        end_date=datetime.now().strftime('%Y%m%d'))
            
            if cal_df is None or cal_df.empty:
                print("⚠️  无法获取交易日历，使用当前日期")
                end_date = datetime.now().strftime('%Y%m%d')
            else:
                end_date = cal_df['cal_date'].max()
                print(f"✅ Tushare 数据最新到: {end_date}")
        
        # Ensure end_date is not None for type checking
        assert end_date is not None
        
        for index_code in index_codes:
            print(f"\n处理指数: {index_code}")
            file_path = self.paths['index_daily'] / f"{index_code}.parquet"
            
            # 获取本地最新日期
            local_latest_date = self._get_latest_date(file_path)
            
            # 简单判断：如果本地最新日期 >= Tushare最新日期，则跳过
            if local_latest_date and local_latest_date >= end_date:
                print(f"  {index_code}: 数据已是最新（本地: {local_latest_date}, Tushare: {end_date}）")
                continue
            
            # 确定开始日期（增量更新）
            if start_date is None:
                if local_latest_date:
                    latest_dt = datetime.strptime(local_latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    start_date_actual = '19900101'
            else:
                start_date_actual = start_date
            
            try:
                print(f"  获取 {start_date_actual} 到 {end_date} 的数据")
                
                df = self._safe_api_call(self.pro.index_daily,
                                       ts_code=index_code,
                                       start_date=start_date_actual,
                                       end_date=end_date)
                
                if df is None or df.empty:
                    # 检查是否是因为日期范围内没有交易日
                    if start_date_actual >= end_date:
                        print(f"  {index_code}: 数据已是最新（开始日期 >= 结束日期）")
                    else:
                        print(f"  {index_code}: 无新数据（可能是非交易日或数据尚未发布）")
                    continue
                
                # 保存数据
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path, engine='pyarrow')
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                    combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                    
                    # 显示更新后的日期范围
                    new_latest_date = combined_df['trade_date'].max()
                    print(f"  {index_code}: 成功更新 {len(df)} 条记录，最新日期: {new_latest_date}")
                else:
                    df.to_parquet(file_path, engine='pyarrow', index=False)
                    latest_date = df['trade_date'].max()
                    print(f"  {index_code}: 成功创建文件，保存 {len(df)} 条记录，最新日期: {latest_date}")
                
                # API限制控制
                time.sleep(0.2)
                
            except Exception as e:
                print(f"  {index_code}: 更新失败 - {e}")
                continue
    
    def update_stock_basic(self):
        """
        更新股票基础信息 - 支持增量更新
        目标：获取 'list_date' (上市日期), 'industry' (用于过滤金融股)
        Tushare API: stock_basic 接口
        路径: /quant_data_center/stock_basic.parquet
        """
        print("=" * 60)
        print("开始更新股票基础信息（增量更新）")
        print("=" * 60)
        
        file_path = self.data_center_path / "stock_basic.parquet"
        
        try:
            print("获取最新股票基础信息...")
            df_new = self._safe_api_call(self.pro.stock_basic,
                                   exchange='',
                                   list_status='L',
                                   fields='ts_code,symbol,name,area,industry,list_date,delist_date,is_hs')
            
            if df_new is None or df_new.empty:
                print("获取股票基础信息失败")
                return
            
            # 检查现有数据
            if file_path.exists():
                print("检查现有数据...")
                df_existing = pd.read_parquet(file_path, engine='pyarrow')
                
                # 比较现有数据和新数据
                # 找出新增的股票（新上市）
                new_stocks = df_new[~df_new['ts_code'].isin(df_existing['ts_code'])]
                
                # 找出退市的股票（从现有数据中移除）
                delisted_stocks = df_existing[~df_existing['ts_code'].isin(df_new['ts_code'])]
                
                # 找出信息有变化的股票
                common_stocks = df_new[df_new['ts_code'].isin(df_existing['ts_code'])]
                if not common_stocks.empty:
                    # 合并现有数据和新数据进行比较
                    merged = common_stocks.merge(df_existing, on='ts_code', suffixes=('_new', '_old'))
                    
                    # 检查关键字段是否有变化
                    changed_fields = []
                    for col in ['symbol', 'name', 'area', 'industry', 'list_date', 'delist_date', 'is_hs']:
                        if col in merged.columns:
                            changed = merged[merged[f'{col}_new'] != merged[f'{col}_old']]
                            if not changed.empty:
                                changed_fields.append(col)
                    
                    # 获取有变化的股票
                    changed_stocks = merged[merged[changed_fields].apply(lambda x: x.any(), axis=1)]['ts_code'].tolist() if changed_fields else []
                else:
                    changed_stocks = []
                
                # 统计变化
                print(f"数据变化统计:")
                print(f"  新增股票: {len(new_stocks)} 只")
                print(f"  退市股票: {len(delisted_stocks)} 只")
                print(f"  信息变化: {len(changed_stocks)} 只")
                
                if len(new_stocks) == 0 and len(delisted_stocks) == 0 and len(changed_stocks) == 0:
                    print("✅ 股票基础信息已是最新，无需更新")
                    return
                
                # 更新数据：移除退市股票，添加新股票，更新变化股票
                # 1. 移除退市股票
                df_updated = df_existing[~df_existing['ts_code'].isin(delisted_stocks['ts_code'])]
                
                # 2. 添加新股票
                if not new_stocks.empty:
                    df_updated = pd.concat([df_updated, new_stocks], ignore_index=True)
                
                # 3. 更新变化股票的信息
                if changed_stocks:
                    # 移除旧的变化股票记录
                    df_updated = df_updated[~df_updated['ts_code'].isin(changed_stocks)]
                    # 添加新的变化股票记录
                    changed_stocks_data = df_new[df_new['ts_code'].isin(changed_stocks)]
                    df_updated = pd.concat([df_updated, changed_stocks_data], ignore_index=True)
                
                # 按ts_code排序
                df_updated = df_updated.sort_values('ts_code').reset_index(drop=True)
                
                # 保存更新后的数据
                df_updated.to_parquet(file_path, engine='pyarrow', index=False)
                print(f"✅ 成功更新股票基础信息:")
                print(f"   总记录数: {len(df_updated)}")
                print(f"   新增: {len(new_stocks)} 只")
                print(f"   退市: {len(delisted_stocks)} 只")
                print(f"   变化: {len(changed_stocks)} 只")
                
            else:
                # 文件不存在，直接保存新数据
                df_new.to_parquet(file_path, engine='pyarrow', index=False)
                print(f"✅ 成功创建股票基础信息文件，保存 {len(df_new)} 条记录")
            
        except Exception as e:
            print(f"更新股票基础信息失败: {e}")
            import traceback
            traceback.print_exc()
    
    def update_stock_moneyflow(self, start_date: Optional[str] = None, end_date: Optional[str] = None, 
                              stock_list: Optional[List[str]] = None, batch_size: int = 150, max_workers: int = 5):
        """
        更新股票资金流向数据（moneyflow）- 支持高并发下载和增量更新
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            stock_list: 股票代码列表，如果为None则获取所有股票
            batch_size: 批处理大小 (用于分批提交任务到线程池)
            max_workers: 最大并发线程数
        """
        print("=" * 60)
        print("开始更新股票资金流向数据（高并发模式）")
        print(f"最大并发线程数: {max_workers}")
        print(f"注意: API 限流约 {int(get_rate_limit() * 0.9)} 次/分钟（套餐 {get_rate_limit()} 次/分）")
        print("=" * 60)
        
        # 确保目录存在
        self.paths['stock_moneyflow'].mkdir(parents=True, exist_ok=True)
        
        # 🎯 直接获取Tushare实际最新数据日期
        if end_date is None:
            print("查询 Tushare 实际最新数据日期...")
            end_date = self._get_tushare_latest_date()
            print(f"✅ Tushare 实际最新数据日期: {end_date}")
        
        # 获取股票列表
        if stock_list is None:
            print("获取股票列表...")
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            exchange='', 
                                            list_status='L', 
                                            fields='ts_code,list_date')
            if stock_basic_df is None:
                print("获取股票列表失败")
                return
            stock_list = stock_basic_df['ts_code'].tolist()
            stock_info = dict(zip(stock_basic_df['ts_code'], stock_basic_df['list_date']))
        else:
            # 如果提供了股票列表，也需要获取上市日期信息
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            ts_code=','.join(stock_list),
                                            fields='ts_code,list_date')
            stock_info = dict(zip(stock_basic_df['ts_code'], stock_basic_df['list_date'])) if stock_basic_df is not None else {}
        
        # 类型守护：保证stock_list和end_date不是None
        assert stock_list is not None
        assert end_date is not None  # end_date在前面已经确保有值
        print(f"共需更新 {len(stock_list)} 只股票")
        
        # 🚀 优化：全局预检查 - 采样检查是否需要更新
        print(f"\n检查本地数据状态（采样检查前100只股票）...")
        print(f"目标更新日期: {end_date}")
        moneyflow_dir = self.paths['stock_moneyflow']
        sample_size = min(100, len(stock_list))
        sample_stocks = stock_list[:sample_size]
        
        up_to_date_count_sample = 0
        for ts_code in sample_stocks:
            file_path = moneyflow_dir / f"{ts_code}.parquet"
            if file_path.exists():
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date and local_latest_date >= end_date:
                    up_to_date_count_sample += 1
        
        # 如果采样中超过95%的股票都是最新的，说明整体数据已经是最新
        if up_to_date_count_sample >= sample_size * 0.95:
            print(f"✅ 采样检查: {up_to_date_count_sample}/{sample_size} 只股票数据已是最新")
            print(f"✅ 数据已经更新到 {end_date}，无需重复下载")
            
            # 生成报告（所有股票都是up_to_date）
            self._generate_download_report(
                data_type='股票资金流向',
                total_count=len(stock_list),
                success_count=0,
                up_to_date_count=len(stock_list),
                empty_stocks=[],
                failed_stocks=[]
            )
            return
        else:
            print(f"📊 采样检查: {up_to_date_count_sample}/{sample_size} 只股票已是最新，继续检查其他股票...")
        
        up_to_date_stocks = []
        success_stocks = []
        empty_stocks = []
        failed_stocks = []
        batch_data_to_save = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(stock_list), batch_size):
                batch_end = min(batch_start + batch_size, len(stock_list))
                batch_stocks = stock_list[batch_start:batch_end]
                
                print(f"\n提交批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_end} ({len(batch_stocks)} 只)")
                
                futures = {executor.submit(self._fetch_moneyflow_worker, ts_code, start_date or '', end_date, stock_info): ts_code for ts_code in batch_stocks}
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        ts_code = result['ts_code']
                        status = result['status']

                        if status == 'success':
                            batch_data_to_save.append(result['data'])
                            success_stocks.append(ts_code)
                        elif status == 'up_to_date':
                            up_to_date_stocks.append(ts_code)
                        elif status == 'api_empty':
                            empty_stocks.append(ts_code)
                        elif status == 'error':
                            failed_stocks.append((ts_code, result['message']))
                    except Exception as e:
                        # Future itself might fail
                        failed_stocks.append((futures[future], str(e)))

                if batch_data_to_save:
                    print(f"  批量保存 {len(batch_data_to_save)} 只股票的数据...")
                    for df, file_path in batch_data_to_save:
                        try:
                            # 确保数据有必要的列
                            if df.empty:
                                continue
                            
                            # 检查必需的列是否存在
                            required_cols = ['ts_code', 'trade_date']
                            missing_cols = [col for col in required_cols if col not in df.columns]
                            if missing_cols:
                                print(f"    ⚠️  {file_path.name}: 缺少必需列 {missing_cols}，跳过保存")
                                failed_stocks.append((file_path.stem, f"缺少列: {missing_cols}"))
                                continue
                            
                            if file_path.exists():
                                try:
                                    existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                    if not existing_df.empty:
                                        # 确保现有数据也有必需的列
                                        if all(col in existing_df.columns for col in required_cols):
                                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                                            combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                                            combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                                        else:
                                            # 如果现有文件格式不对，直接覆盖
                                            df.to_parquet(file_path, engine='pyarrow', index=False)
                                    else:
                                        # 现有文件为空，直接保存新数据
                                        df.to_parquet(file_path, engine='pyarrow', index=False)
                                except Exception as e:
                                    # 如果读取现有文件失败，直接覆盖
                                    print(f"    读取现有文件失败，将覆盖: {file_path.name}")
                                    df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    batch_data_to_save.clear()
                
                # 限流：控制API调用频率为500次/分钟（约8.33次/秒）
                # 每批次150个请求，需要等待 60/(500/150) = 18秒
                # 刚好卡在Tushare的500次/分钟限制
                time.sleep(18)

        self._generate_download_report(
            data_type='股票资金流向',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )
    
    def _fetch_cyq_perf_worker(self, ts_code: str, start_date: str, end_date: str, stock_info: Dict[str, Any]) -> Dict[str, Any]:
        """并发工作函数：获取单只股票的每日筹码统计数据（增量更新）"""
        try:
            file_path = self.paths['stock_cyq_perf'] / f"{ts_code}.parquet"
            
            # 确定开始日期
            if start_date is not None and start_date != '':
                start_date_actual = start_date
            else:
                # 获取本地最新日期
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date:
                    # 检查数据是否已经是最新
                    if local_latest_date >= end_date:
                        return {'ts_code': ts_code, 'status': 'up_to_date'}
                    
                    # 从最新日期的下一天开始（Tushare API会自动处理交易日历）
                    latest_dt = datetime.strptime(local_latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    # 如果文件不存在，使用上市日期或默认日期
                    list_date = stock_info.get(ts_code)
                    start_date_actual = list_date if list_date and list_date != 'None' else '20100101'  # 筹码数据从2010年开始
            
            # 获取数据（直接调用API，让Tushare处理交易日历）
            df = self._safe_api_call(self.pro.cyq_perf,
                                   ts_code=ts_code,
                                   start_date=start_date_actual,
                                   end_date=end_date)
            
            if df is not None and not df.empty:
                # 验证数据格式：确保有必需的列
                required_cols = ['ts_code', 'trade_date']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    # 如果缺少必需列，记录错误但不中断
                    return {'ts_code': ts_code, 'status': 'error',
                           'message': f'API返回数据缺少必需列: {missing_cols}, 实际列: {list(df.columns)}'}
                
                # 确保ts_code列存在（如果API没有返回，则添加）
                if 'ts_code' not in df.columns:
                    df['ts_code'] = ts_code
                
                # 数据清洗：确保trade_date为字符串格式，数值字段为float
                if 'trade_date' in df.columns:
                    df['trade_date'] = df['trade_date'].astype(str)
                
                # 确保数值字段类型正确
                numeric_cols = ['cost_5pct', 'cost_95pct', 'weight_avg', 'winner_rate']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def update_stock_cyq_perf(self, start_date: Optional[str] = None, end_date: Optional[str] = None, 
                              stock_list: Optional[List[str]] = None, batch_size: int = 100, max_workers: int = 3):
        """
        更新股票每日筹码分布统计数据（cyq_perf）- 支持高并发下载和增量更新
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            stock_list: 股票代码列表，如果为None则获取所有股票
            batch_size: 批处理大小 (用于分批提交任务到线程池，默认100，适配200次/分钟限制)
            max_workers: 最大并发线程数（默认3，降低并发以适配API限制）
        """
        print("=" * 60)
        print("开始更新股票每日筹码分布统计数据（高并发模式）")
        print(f"最大并发线程数: {max_workers}")
        print("注意: cyq_perf接口限制为每分钟200次，已优化限流策略")
        print("=" * 60)
        
        # 确保目录存在
        self.paths['stock_cyq_perf'].mkdir(parents=True, exist_ok=True)
        
        # 🎯 直接获取Tushare实际最新数据日期
        if end_date is None:
            print("查询 Tushare 实际最新数据日期...")
            end_date = self._get_tushare_latest_date()
            print(f"✅ Tushare 实际最新数据日期: {end_date}")
        
        # 获取股票列表
        if stock_list is None:
            print("获取股票列表...")
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            exchange='', 
                                            list_status='L', 
                                            fields='ts_code,list_date')
            if stock_basic_df is None:
                print("获取股票列表失败")
                return
            stock_list = stock_basic_df['ts_code'].tolist()
            stock_info = dict(zip(stock_basic_df['ts_code'], stock_basic_df['list_date']))
        else:
            # 如果提供了股票列表，也需要获取上市日期信息
            stock_basic_df = self._safe_api_call(self.pro.stock_basic, 
                                            ts_code=','.join(stock_list),
                                            fields='ts_code,list_date')
            stock_info = dict(zip(stock_basic_df['ts_code'], stock_basic_df['list_date'])) if stock_basic_df is not None else {}
        
        # 类型守护：保证stock_list和end_date不是None
        assert stock_list is not None
        assert end_date is not None  # end_date在前面已经确保有值
        print(f"共需更新 {len(stock_list)} 只股票")
        
        # 🚀 优化：全局预检查 - 采样检查是否需要更新
        # 改进：均匀间隔采样（覆盖开头、中间、结尾），避免只检查前100只导致误判
        print(f"\n检查本地数据状态（均匀间隔采样检查）...")
        print(f"目标更新日期: {end_date}")
        cyq_perf_dir = self.paths['stock_cyq_perf']
        
        # 均匀间隔采样：确保覆盖整个列表（开头、中间、结尾都有）
        sample_size = min(200, len(stock_list))  # 采样数量200
        if len(stock_list) <= sample_size:
            sample_stocks = stock_list
        else:
            # 使用均匀间隔采样，确保覆盖整个列表
            # 计算采样间隔，确保能覆盖到最后一个
            step = (len(stock_list) - 1) / (sample_size - 1) if sample_size > 1 else 0
            sample_stocks = []
            for i in range(sample_size):
                idx = int(i * step) if step > 0 else i
                if idx < len(stock_list):
                    sample_stocks.append(stock_list[idx])
            
            # 确保包含最后一个（边界检查）
            if sample_stocks[-1] != stock_list[-1]:
                sample_stocks[-1] = stock_list[-1]
            
            # 去重（保持顺序）
            seen = set()
            sample_stocks = [x for x in sample_stocks if not (x in seen or seen.add(x))]
        
        file_exists_count = 0
        up_to_date_count_sample = 0
        for ts_code in sample_stocks:
            file_path = cyq_perf_dir / f"{ts_code}.parquet"
            if file_path.exists():
                file_exists_count += 1
                local_latest_date = self._get_latest_date(file_path, 'trade_date')
                if local_latest_date and local_latest_date >= end_date:
                    up_to_date_count_sample += 1
        
        # 计算文件存在率
        file_existence_rate = file_exists_count / sample_size if sample_size > 0 else 0
        
        # 改进逻辑：只有当文件存在率 > 90% 且 最新率 > 95% 时才跳过
        # 这样可以避免：1) 很多股票没有文件 2) 文件存在但数据不完整
        if file_exists_count > 0:
            up_to_date_rate = up_to_date_count_sample / file_exists_count
        else:
            up_to_date_rate = 0
        
        # 只有当文件存在率 >= 90% 且 在已存在的文件中最新率 >= 95% 时才跳过
        if file_existence_rate >= 0.90 and up_to_date_rate >= 0.95:
            print(f"✅ 采样检查: {up_to_date_count_sample}/{file_exists_count} 只股票数据已是最新")
            print(f"✅ 文件存在率: {file_existence_rate:.1%} ({file_exists_count}/{sample_size})")
            print(f"✅ 数据已经更新到 {end_date}，无需重复下载")
            
            # 生成报告（所有股票都是up_to_date）
            self._generate_download_report(
                data_type='股票每日筹码统计',
                total_count=len(stock_list),
                success_count=0,
                up_to_date_count=len(stock_list),
                empty_stocks=[],
                failed_stocks=[]
            )
            return
        else:
            print(f"📊 采样检查结果:")
            print(f"   - 文件存在率: {file_existence_rate:.1%} ({file_exists_count}/{sample_size})")
            if file_exists_count > 0:
                print(f"   - 数据最新率: {up_to_date_rate:.1%} ({up_to_date_count_sample}/{file_exists_count})")
            else:
                print(f"   - 数据最新率: 0% (无文件)")
            print(f"📊 继续检查所有股票...")
        
        up_to_date_stocks = []
        success_stocks = []
        empty_stocks = []
        failed_stocks = []
        batch_data_to_save = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(stock_list), batch_size):
                batch_end = min(batch_start + batch_size, len(stock_list))
                batch_stocks = stock_list[batch_start:batch_end]
                
                print(f"\n提交批次 {batch_start//batch_size + 1}: 股票 {batch_start+1}-{batch_end} ({len(batch_stocks)} 只)")
                
                futures = {executor.submit(self._fetch_cyq_perf_worker, ts_code, start_date or '', end_date, stock_info): ts_code for ts_code in batch_stocks}
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        ts_code = result['ts_code']
                        status = result['status']

                        if status == 'success':
                            batch_data_to_save.append(result['data'])
                            success_stocks.append(ts_code)
                        elif status == 'up_to_date':
                            up_to_date_stocks.append(ts_code)
                        elif status == 'api_empty':
                            empty_stocks.append(ts_code)
                        elif status == 'error':
                            failed_stocks.append((ts_code, result['message']))
                    except Exception as e:
                        # Future itself might fail
                        failed_stocks.append((futures[future], str(e)))

                if batch_data_to_save:
                    print(f"  批量保存 {len(batch_data_to_save)} 只股票的数据...")
                    for df, file_path in batch_data_to_save:
                        try:
                            # 确保数据有必要的列
                            if df.empty:
                                continue
                            
                            # 检查必需的列是否存在
                            required_cols = ['ts_code', 'trade_date']
                            missing_cols = [col for col in required_cols if col not in df.columns]
                            if missing_cols:
                                print(f"    ⚠️  {file_path.name}: 缺少必需列 {missing_cols}，跳过保存")
                                failed_stocks.append((file_path.stem, f"缺少列: {missing_cols}"))
                                continue
                            
                            if file_path.exists():
                                try:
                                    existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                    if not existing_df.empty:
                                        # 确保现有数据也有必需的列
                                        if all(col in existing_df.columns for col in required_cols):
                                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                                            combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                                            combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                                        else:
                                            # 如果现有文件格式不对，直接覆盖
                                            df.to_parquet(file_path, engine='pyarrow', index=False)
                                    else:
                                        # 现有文件为空，直接保存新数据
                                        df.to_parquet(file_path, engine='pyarrow', index=False)
                                except Exception as e:
                                    # 如果读取现有文件失败，直接覆盖
                                    print(f"    读取现有文件失败，将覆盖: {file_path.name}")
                                    df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    batch_data_to_save.clear()
                
                # 限流：控制API调用频率为200次/分钟（cyq_perf接口限制）
                # 每批次100个请求，需要等待 60/(200/100) = 30秒
                # 确保不超过Tushare的200次/分钟限制
                time.sleep(30)

        self._generate_download_report(
            data_type='股票每日筹码统计',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )
    
    def update_daily_basic(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新股票每日基础指标
        目标：获取 'total_mv' (总市值) 和 'pb' (市净率)
        'total_mv' 是 Size 因子 (SMB) 的核心
        'pb' 是 Value 因子 (HML) 的核心 (B/M = 1/PB)
        Tushare API: daily_basic 接口 (注意：此API按交易日获取，需要循环调用)
        路径: /quant_data_center/stock/daily_basic/daily_basic_all.parquet
        """
        print("=" * 60)
        print("开始更新股票每日基础指标")
        print("=" * 60)
        
        file_path = self.paths['stock_daily_basic'] / "daily_basic_all.parquet"
        
        # 确定开始日期
        if start_date is None:
            latest_date = self._get_latest_date(file_path)
            if latest_date:
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            else:
                start_date_actual = '19900101'
        else:
            start_date_actual = start_date
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if start_date_actual >= end_date:
            print("股票每日基础指标数据已是最新")
            return
        
        print(f"获取 {start_date_actual} 到 {end_date} 的股票每日基础指标数据")
        print("注意：daily_basic API 需要按交易日逐日获取，可能需要较长时间...")
        
        try:
            # 获取交易日历
            cal_df = self._safe_api_call(self.pro.trade_cal,
                                        exchange='SSE',
                                        start_date=start_date_actual,
                                        end_date=end_date,
                                        is_open='1')
            
            if cal_df is None or cal_df.empty:
                print("无交易日数据")
                return
            
            trade_dates = sorted(cal_df['cal_date'].unique())
            print(f"共需处理 {len(trade_dates)} 个交易日")
            
            all_data = []
            for i, trade_date in enumerate(trade_dates, 1):
                if i % 10 == 0:
                    print(f"  进度: {i}/{len(trade_dates)} ({i/len(trade_dates)*100:.1f}%)")
                
                df_day = self._safe_api_call(self.pro.daily_basic,
                                           trade_date=trade_date,
                                           fields='ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pb,ps,total_share,float_share,total_mv,circ_mv')
                
                if df_day is not None and not df_day.empty:
                    all_data.append(df_day)
            
            if not all_data:
                print("无新数据")
                return
            
            # 合并所有数据
            df = pd.concat(all_data, ignore_index=True)
            
            # 确保目录存在
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 保存数据
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                df.to_parquet(file_path, engine='pyarrow', index=False)
            
            print(f"成功更新 {len(df)} 条股票每日基础指标记录 (共 {len(trade_dates)} 个交易日)")
            
        except Exception as e:
            print(f"更新股票每日基础指标失败: {e}")
    
    def update_margin_total(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新融资融券交易汇总数据（市场总额）
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        print("=" * 60)
        print("开始更新融资融券交易汇总数据（市场总额）")
        print("=" * 60)
        
        file_path = self.paths['market_margin_total'] / "margin_total.parquet"
        
        # 确定开始日期
        if start_date is None:
            latest_date = self._get_latest_date(file_path, 'trade_date')
            if latest_date:
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            else:
                start_date_actual = '20100101'  # 融资融券数据从2010年开始
        else:
            start_date_actual = start_date
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if start_date_actual >= end_date:
            print("融资融券交易汇总数据已是最新")
            return
        
        print(f"获取 {start_date_actual} 到 {end_date} 的融资融券交易汇总数据")
        
        try:
            df = self._safe_api_call(self.pro.margin,
                                   start_date=start_date_actual,
                                   end_date=end_date)
            
            if df is None or df.empty:
                print("无新数据")
                return
            
            # 保存数据
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['trade_date']).sort_values('trade_date')
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                df = df.drop_duplicates(subset=['trade_date']).sort_values('trade_date')
                df.to_parquet(file_path, engine='pyarrow', index=False)
            
            print(f"成功更新 {len(df)} 条融资融券交易汇总记录")
            
        except Exception as e:
            print(f"更新融资融券交易汇总数据失败: {e}")
    
    def update_stock_margin_detail(self, start_date: Optional[str] = None, end_date: Optional[str] = None,
                                  stock_list: Optional[List[str]] = None, batch_size: int = 50, max_workers: int = 1):
        """
        更新个股融资融券交易明细数据（增量下载）
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            stock_list: 股票代码列表，如果为None则获取所有股票
            batch_size: 批处理大小
            max_workers: 最大并发线程数（建议设为1，避免API限流）
        """
        print("=" * 60)
        print("开始更新个股融资融券交易明细数据（增量下载）")
        print(f"最大并发线程数: {max_workers}")
        print("注意: 已限制API调用频率，防止封IP")
        print("=" * 60)
        
        # 确保目录存在
        self.paths['market_margin_detail'].mkdir(parents=True, exist_ok=True)
        
        # 获取最新交易日
        if end_date is None:
            print("获取 Tushare 最新交易日...")
            cal_df = self._safe_api_call(self.pro.trade_cal,
                                        exchange='SSE',
                                        is_open='1',
                                        start_date=(datetime.now() - timedelta(days=10)).strftime('%Y%m%d'),
                                        end_date=datetime.now().strftime('%Y%m%d'))
            
            if cal_df is None or cal_df.empty:
                print("⚠️  无法获取交易日历，使用当前日期")
                end_date = datetime.now().strftime('%Y%m%d')
            else:
                end_date = cal_df['cal_date'].max()
                print(f"✅ Tushare 数据最新到: {end_date}")
        
        # 按日期循环处理（因为margin_detail API需要按日期查询）
        if start_date is None:
            # 获取所有需要更新的日期
            # 先检查已有数据的日期范围
            existing_dates = set()
            for date_file in self.paths['market_margin_detail'].glob("*.parquet"):
                try:
                    # 文件名格式：{trade_date}.parquet
                    date_str = date_file.stem
                    if date_str.isdigit() and len(date_str) == 8:
                        existing_dates.add(date_str)
                except:
                    pass
            
            # 获取交易日历
            cal_df = self._safe_api_call(self.pro.trade_cal,
                                        exchange='SSE',
                                        is_open='1',
                                        start_date='20100101',  # 融资融券数据从2010年开始
                                        end_date=end_date)
            
            if cal_df is None or cal_df.empty:
                print("无法获取交易日历")
                return
            
            trade_dates = sorted(cal_df['cal_date'].unique())
            # 过滤出需要更新的日期
            dates_to_update = [d for d in trade_dates if d not in existing_dates]
            
            if not dates_to_update:
                print("所有日期的数据已存在，无需更新")
                return
            
            print(f"需要更新 {len(dates_to_update)} 个交易日的数据")
        else:
            # 如果指定了开始日期，获取该日期范围内的交易日
            cal_df = self._safe_api_call(self.pro.trade_cal,
                                        exchange='SSE',
                                        is_open='1',
                                        start_date=start_date,
                                        end_date=end_date)
            
            if cal_df is None or cal_df.empty:
                print("无法获取交易日历")
                return
            
            dates_to_update = sorted(cal_df['cal_date'].unique())
            print(f"需要更新 {len(dates_to_update)} 个交易日的数据")
        
        success_dates = []
        failed_dates = []
        
        # 按日期循环处理
        for i, trade_date in enumerate(dates_to_update, 1):
            if i % 10 == 0 or i == 1:
                print(f"\n进度: {i}/{len(dates_to_update)} ({i/len(dates_to_update)*100:.1f}%) - 日期: {trade_date}")
            
            try:
                # 获取该日期的所有股票融资融券明细
                df = self._safe_api_call(self.pro.margin_detail,
                                       trade_date=trade_date)
                
                if df is None or df.empty:
                    continue
                
                # 按日期保存为单个文件
                file_path = self.paths['market_margin_detail'] / f"{trade_date}.parquet"
                
                try:
                    # 去重并排序
                    df = df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('ts_code')
                    df.to_parquet(file_path, engine='pyarrow', index=False)
                    success_dates.append(trade_date)
                except Exception as e:
                    failed_dates.append((trade_date, str(e)))
                
                # 强制频控：每次API调用后等待（防止封IP）
                time.sleep(2)
                
            except Exception as e:
                print(f"  获取 {trade_date} 数据失败: {e}")
                failed_dates.append((trade_date, str(e)))
                continue
        
        print("\n" + "=" * 60)
        print(f"更新完成:")
        print(f"  成功: {len(success_dates)} 个交易日")
        print(f"  失败: {len(failed_dates)} 个交易日")
        print("=" * 60)
    
    def update_moneyflow_hsgt(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新沪深港通资金流向数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        print("=" * 60)
        print("开始更新沪深港通资金流向数据")
        print("=" * 60)
        
        file_path = self.paths['market_hsgt'] / "moneyflow_hsgt.parquet"
        
        # 确定开始日期
        if start_date is None:
            latest_date = self._get_latest_date(file_path, 'trade_date')
            if latest_date:
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            else:
                start_date_actual = '20141117'  # 沪港通开通日期
        else:
            start_date_actual = start_date
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if start_date_actual >= end_date:
            print("沪深港通资金流向数据已是最新")
            return
        
        print(f"获取 {start_date_actual} 到 {end_date} 的沪深港通资金流向数据")
        
        try:
            df = self._safe_api_call(self.pro.moneyflow_hsgt,
                                   start_date=start_date_actual,
                                   end_date=end_date)
            
            if df is None or df.empty:
                print("无新数据")
                return
            
            # 保存数据
            if file_path.exists():
                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                # 去重：根据trade_date去重（每条记录对应一个交易日）
                if 'trade_date' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=['trade_date'], keep='last').sort_values('trade_date')
                else:
                    combined_df = combined_df.drop_duplicates()
                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
            else:
                # 新文件，按trade_date去重
                if 'trade_date' in df.columns:
                    df = df.drop_duplicates(subset=['trade_date'], keep='last').sort_values('trade_date')
                else:
                    df = df.drop_duplicates()
                df.to_parquet(file_path, engine='pyarrow', index=False)
            
            print(f"成功更新 {len(df)} 条沪深港通资金流向记录")
            
        except Exception as e:
            print(f"更新沪深港通资金流向数据失败: {e}")
    
    def update_future_holdings(self, varieties: Optional[List[str]] = None, 
                               start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新CFFEX期货主力合约前20名会员持仓数据
        
        用于构建"情绪面"多空比指标，支持IF、IC、IM、IH等品种
        
        Args:
            varieties: 期货品种列表，默认为 ['IF', 'IC', 'IM', 'IH']
            start_date: 开始日期 (YYYYMMDD)，默认为None（自动从本地最新日期开始）
            end_date: 结束日期 (YYYYMMDD)，默认为None（使用当前日期）
        """
        print("=" * 60)
        print("开始更新CFFEX期货主力合约前20名会员持仓数据")
        print("=" * 60)
        
        if varieties is None:
            varieties = ['IF', 'IC', 'IM', 'IH']
        
        # 确保目录存在
        save_dir = self.paths['futures_holding']
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # 确定结束日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        for variety in varieties:
            print(f"\n处理品种: {variety}")
            file_path = save_dir / f"{variety}_top20.parquet"
            
            try:
                # 1. 获取该品种的历史主力合约映射表
                print(f"  获取 {variety} 主力合约映射...")
                mapping_df = self._safe_api_call(
                    self.pro.fut_mapping,
                    ts_code=f'{variety}.CFX'
                )
                
                if mapping_df is None or mapping_df.empty:
                    print(f"  ⚠️  无法获取 {variety} 的主力合约映射数据")
                    continue
                
                # 确保 trade_date 是字符串格式
                mapping_df['trade_date'] = mapping_df['trade_date'].astype(str)
                mapping_df = mapping_df.sort_values('trade_date')
                
                # 确定开始日期（增量更新）
                start_date_actual = start_date
                if start_date is None:
                    if file_path.exists():
                        try:
                            existing_df = pd.read_parquet(file_path, engine='pyarrow')
                            if not existing_df.empty and 'trade_date' in existing_df.columns:
                                last_date = existing_df['trade_date'].max()
                                # 从下一天开始更新
                                last_dt = datetime.strptime(str(last_date), '%Y%m%d')
                                start_date_actual = (last_dt + timedelta(days=1)).strftime('%Y%m%d')
                                print(f"  本地最新日期: {last_date}，从 {start_date_actual} 开始增量更新")
                            else:
                                start_date_actual = '20160101'  # 默认从2016年开始
                        except Exception as e:
                            print(f"  ⚠️  读取本地文件失败: {e}，从头开始下载")
                            start_date_actual = '20160101'
                    else:
                        start_date_actual = '20160101'
                
                # 筛选需要下载的日期范围
                target_mapping = mapping_df[
                    (mapping_df['trade_date'] >= start_date_actual) & 
                    (mapping_df['trade_date'] <= end_date)
                ].copy()
                
                if target_mapping.empty:
                    print(f"  ✅ {variety} 数据已是最新（无需更新）")
                    continue
                
                print(f"  需要下载 {len(target_mapping)} 个交易日的数据")
                
                # 2. 逐日下载持仓数据
                new_data = []
                success_count = 0
                failed_count = 0
                
                for idx, row in target_mapping.iterrows():
                    trade_date = str(row['trade_date'])
                    # mapping_ts_code 如 'IF2006.CFX' -> symbol 'IF2006'
                    mapping_ts_code = str(row['mapping_ts_code'])
                    contract = mapping_ts_code.split('.')[0] if '.' in mapping_ts_code else mapping_ts_code
                    
                    if (success_count + failed_count) % 50 == 0 and (success_count + failed_count) > 0:
                        print(f"    进度: {success_count + failed_count}/{len(target_mapping)} "
                              f"({(success_count + failed_count)/len(target_mapping)*100:.1f}%)")
                    
                    try:
                        # 获取当日前20名持仓
                        # 注意：fut_holding 的 symbol 参数只需要合约名（如 IF2406），不需要后缀
                        df = self._safe_api_call(
                            self.pro.fut_holding,
                            trade_date=trade_date,
                            symbol=contract,
                            exchange='CFFEX'
                        )
                        
                        if df is not None and not df.empty:
                            # 添加品种和合约标识
                            df['ts_code'] = variety  # 标记品种
                            df['contract'] = contract  # 标记具体合约
                            
                            # 确保 trade_date 列存在且格式正确
                            if 'trade_date' not in df.columns:
                                df['trade_date'] = trade_date
                            else:
                                df['trade_date'] = df['trade_date'].astype(str)
                            
                            # 只保留前20名（如果API返回了排名字段）
                            # 注意：fut_holding API 可能已经只返回前20名，这里做双重保险
                            if 'rank' in df.columns:
                                df = df[df['rank'] <= 20].copy()
                            
                            new_data.append(df)
                            success_count += 1
                        else:
                            # 空数据可能是非交易日或数据尚未发布
                            failed_count += 1
                        
                        # API限流控制
                        time.sleep(0.2)
                        
                    except Exception as e:
                        failed_count += 1
                        if failed_count <= 5:  # 只打印前5个错误
                            print(f"    ⚠️  获取 {trade_date} {contract} 失败: {e}")
                        continue
                
                # 3. 保存数据（增量更新）
                if new_data:
                    df_new = pd.concat(new_data, ignore_index=True)
                    
                    # 确保数据去重（按 trade_date, contract, broker）
                    df_new = df_new.drop_duplicates(
                        subset=['trade_date', 'contract', 'broker'],
                        keep='last'
                    ).sort_values(['trade_date', 'contract'])
                    
                    # 合并现有数据
                    if file_path.exists():
                        try:
                            df_old = pd.read_parquet(file_path, engine='pyarrow')
                            if not df_old.empty:
                                # 合并并去重
                                df_final = pd.concat([df_old, df_new], ignore_index=True)
                                df_final = df_final.drop_duplicates(
                                    subset=['trade_date', 'contract', 'broker'],
                                    keep='last'
                                ).sort_values(['trade_date', 'contract'])
                            else:
                                df_final = df_new
                        except Exception as e:
                            print(f"  ⚠️  读取现有文件失败，将覆盖: {e}")
                            df_final = df_new
                    else:
                        df_final = df_new
                    
                    # 保存文件
                    df_final.to_parquet(file_path, engine='pyarrow', index=False)
                    
                    print(f"  ✅ {variety} 更新完成:")
                    print(f"     本次新增: {len(df_new)} 条记录")
                    print(f"     总计: {len(df_final)} 条记录")
                    print(f"     成功: {success_count} 个交易日")
                    print(f"     失败/空数据: {failed_count} 个交易日")
                else:
                    print(f"  ⚠️  {variety} 无新数据可保存")
                    if failed_count > 0:
                        print(f"     失败/空数据: {failed_count} 个交易日")
                
            except Exception as e:
                print(f"  ❌ 处理 {variety} 失败: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print("\n" + "=" * 60)
        print("CFFEX期货主力合约持仓数据更新完成")
        print("=" * 60)
    
    # 注意：财务指标更新功能已移除，请使用专门的程序单独更新（每季度一次）

    def _get_latest_date_from_files(self, data_type: str) -> Optional[str]:
        """从数据文件中获取最新日期"""
        try:
            if data_type == '股票日K线(后复权)':
                # 获取所有股票日K线文件的最新日期
                hfq_dir = self.paths['stock_daily_hfq']
                if not hfq_dir.exists():
                    return None
                
                latest_dates = []
                for file_path in hfq_dir.glob("*.parquet"):
                    try:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dates.append(latest_date)
                    except:
                        continue
                
                if latest_dates:
                    return max(latest_dates)
                return None
            
            elif data_type == '股票每日基础指标':
                file_path = self.paths['stock_daily_basic'] / "daily_basic_all.parquet"
                return self._get_latest_date(file_path, 'trade_date')
            
            elif data_type == '股票财务指标':
                # 获取所有财务指标文件的最新日期
                fina_dir = self.paths['stock_fina_indicator']
                if not fina_dir.exists():
                    return None
                
                latest_dates = []
                for file_path in fina_dir.glob("*.parquet"):
                    try:
                        latest_date = self._get_latest_date(file_path, 'end_date')
                        if latest_date:
                            latest_dates.append(latest_date)
                    except:
                        continue
                
                if latest_dates:
                    return max(latest_dates)
                return None
            
            elif data_type == '无风险利率':
                file_path = self.paths['factors_rfr'] / "rfr_daily.parquet"
                return self._get_latest_date(file_path, 'trade_date')
            
            elif data_type == '指数日K线':
                # 获取所有指数日K线文件的最新日期
                index_dir = self.paths['index_daily']
                if not index_dir.exists():
                    return None
                
                latest_dates = []
                for file_path in index_dir.glob("*.parquet"):
                    try:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dates.append(latest_date)
                    except:
                        continue
                
                if latest_dates:
                    return max(latest_dates)
                return None
            
            elif data_type == '指数成分股':
                # 获取所有指数成分股文件的最新日期
                const_dir = self.paths['index_constituents']
                if not const_dir.exists():
                    return None
                
                latest_dates = []
                for file_path in const_dir.glob("*_const.parquet"):
                    try:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dates.append(latest_date)
                    except:
                        continue
                
                if latest_dates:
                    return max(latest_dates)
                return None
            
            elif data_type == '股票资金流向':
                # 获取所有资金流向文件的最新日期
                moneyflow_dir = self.paths['stock_moneyflow']
                if not moneyflow_dir.exists():
                    return None
                
                latest_dates = []
                for file_path in moneyflow_dir.glob("*.parquet"):
                    try:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dates.append(latest_date)
                    except:
                        continue
                
                if latest_dates:
                    return max(latest_dates)
                return None
            
        except Exception as e:
            print(f"获取最新日期失败: {e}")
            return None
        
        return None

    def _generate_download_report(self, data_type: str, total_count: int, success_count: int, up_to_date_count: int, empty_stocks: List[str], failed_stocks: List[Tuple[str, str]]):
        """生成并打印下载报告"""
        report_time = datetime.now()
        sanitized_data_type = data_type.replace('(', '').replace(')', '').replace('/', '')
        report_filename = f"download_report_{sanitized_data_type}_{report_time.strftime('%Y%m%d_%H%M%S')}.txt"
        report_path = self.data_center_path / report_filename

        empty_count = len(empty_stocks)
        failed_count = len(failed_stocks)

        completeness = (success_count + up_to_date_count) / total_count if total_count > 0 else 0
        
        # 获取最新数据截止日期
        latest_date = self._get_latest_date_from_files(data_type)
        latest_date_str = latest_date if latest_date else "未知"

        # --- Console Output ---
        print("\n" + "="*60)
        print(f"📊 {data_type} 数据下载完整度报告")
        print(f"报告时间: {report_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        print(f"总计任务: {total_count} 只")
        print(f"  ✅ 成功下载/更新: {success_count} 只")
        print(f"  -  数据已是最新: {up_to_date_count} 只")
        print(f"  ⚠️  API返回空数据: {empty_count} 只")
        print(f"  ❌ 下载失败: {failed_count} 只")
        print(f"\n数据完整度: {completeness:.2%}")
        print(f"最新数据截止日期: {latest_date_str}")
        
        print(f"\n详情已保存至报告文件: {report_path}")
        print("="*60)

        # --- File Output ---
        # 始终生成报告文件，包含最新日期信息
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"{data_type} 数据下载报告\n")
            f.write(f"报告时间: {report_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*50 + "\n\n")
            
            f.write(f"摘要:\n")
            f.write(f"  - 总计任务: {total_count}\n")
            f.write(f"  - 成功下载/更新: {success_count}\n")
            f.write(f"  - 数据已是最新: {up_to_date_count}\n")
            f.write(f"  - API返回空数据: {empty_count}\n")
            f.write(f"  - 下载失败: {failed_count}\n")
            f.write(f"  - 数据完整度: {completeness:.2%}\n")
            f.write(f"  - 最新数据截止日期: {latest_date_str}\n\n")

            if empty_count > 0:
                f.write("="*50 + "\n")
                f.write(f"API返回空数据列表 ({empty_count} 只):\n")
                f.write("="*50 + "\n")
                f.write("\n".join(empty_stocks))
                f.write("\n\n")

            if failed_count > 0:
                f.write("="*50 + "\n")
                f.write(f"下载失败列表 ({failed_count} 只):\n")
                f.write("="*50 + "\n")
                for ts_code, message in failed_stocks:
                    f.write(f"{ts_code}: {message}\n")

    def update_missing_data_for_strategy(self, start_date: Optional[str] = None, end_date: Optional[str] = None,
                                        include_index_valuation: bool = True,
                                        include_bond_yield: bool = True,
                                        include_options_pcr: bool = False,
                                        include_futures_holding: bool = False):
        """
        补齐四维择时策略所需的：指数估值、国债收益率、期货持仓、期权PCR
        
        Args:
            start_date: 开始日期 (YYYYMMDD)，默认为None（自动从本地最新日期开始）
            end_date: 结束日期 (YYYYMMDD)，默认为None（使用当前日期）
            include_index_valuation: 是否更新指数估值数据，默认True
            include_bond_yield: 是否更新国债收益率数据，默认True
            include_options_pcr: 是否更新期权PCR数据，默认False（可在其他项目获取）
            include_futures_holding: 是否更新期货持仓数据，默认False（可在其他项目获取）
        """
        print("=" * 80)
        print("开始补齐四维择时策略所需数据")
        print("=" * 80)
        
        # 确定结束日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        failed_steps = []
        
        # ========== 1. 更新指数每日估值 (PE/PB) ==========
        if include_index_valuation:
            print("\n" + "=" * 60)
            print("步骤1: 更新指数每日估值数据 (PE/PB)")
            print("=" * 60)
            try:
                index_codes = ['000001.SH', '000300.SH', '000905.SH', '399006.SZ', '000852.SH']
                save_dir = self.data_center_path / 'index' / 'daily_basic'
                save_dir.mkdir(parents=True, exist_ok=True)
                
                for code in index_codes:
                    file_path = save_dir / f'{code}.parquet'
                    
                    # 确定开始日期
                    latest_date = None
                    if start_date is None:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                            start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                        else:
                            start_date_actual = '20100101'
                    else:
                        start_date_actual = start_date
                    
                    # 检查是否需要更新
                    if latest_date and latest_date >= end_date:
                        print(f"  {code}: 数据已是最新 (截止 {latest_date})")
                        continue
                    
                    # 下载数据
                    print(f"  {code}: 下载 {start_date_actual} 至 {end_date} 的估值数据...")
                    df = self._safe_api_call(
                        self.pro.index_dailybasic,
                        ts_code=code,
                        start_date=start_date_actual,
                        end_date=end_date
                    )
                    
                    if df is not None and not df.empty:
                        # 合并数据
                        if file_path.exists():
                            existing_df = pd.read_parquet(file_path, engine='pyarrow')
                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                            combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                            combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                        else:
                            df.to_parquet(file_path, engine='pyarrow', index=False)
                        
                        print(f"  {code}: ✅ 成功更新 {len(df)} 条记录")
                    else:
                        print(f"  {code}: ⚠️  无新数据")
                    
                    time.sleep(0.2)  # API限流
            except Exception as e:
                print(f"❌ 更新指数估值数据失败: {e}")
                failed_steps.append("指数估值数据")
        
        # ========== 2. 更新10年期国债收益率 ==========
        if include_bond_yield:
            print("\n" + "=" * 60)
            print("步骤2: 更新10年期国债收益率")
            print("=" * 60)
            try:
                macro_dir = self.data_center_path / 'factors' / 'macro'
                macro_dir.mkdir(parents=True, exist_ok=True)
                file_path = macro_dir / 'china_bond_yield_10y.parquet'
                
                # 确定开始日期
                latest_date = None
                if start_date is None:
                    latest_date = self._get_latest_date(file_path, 'trade_date')
                    if latest_date:
                        latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                        start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                    else:
                        start_date_actual = '20100101'
                else:
                    start_date_actual = start_date
                
                # 检查是否需要更新
                if latest_date and latest_date >= end_date:
                    print(f"  数据已是最新 (截止 {latest_date})")
                else:
                    print(f"  下载 {start_date_actual} 至 {end_date} 的国债收益率数据...")
                    # Tushare API: yield_curve, curve_type='0' (国债), period='10.0' (10年期)
                    df = self._safe_api_call(
                        self.pro.yc_cb,
                        curve_type='0',
                        start_date=start_date_actual,
                        end_date=end_date
                    )
                    
                    if df is not None and not df.empty:
                        # 筛选10年期数据
                        df = df[df['curve_term'] == 10.0].copy()
                        
                        if not df.empty:
                            # 合并数据
                            if file_path.exists():
                                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['trade_date']).sort_values('trade_date')
                                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                            
                            print(f"  ✅ 成功更新 {len(df)} 条记录")
                        else:
                            print(f"  ⚠️  无10年期数据")
                    else:
                        print(f"  ⚠️  无新数据")
            except Exception as e:
                print(f"❌ 更新国债收益率数据失败: {e}")
                failed_steps.append("国债收益率数据")
        
        # ========== 3. 更新期权PCR (聚合数据) ==========
        if include_options_pcr:
            print("\n" + "=" * 60)
            print("步骤3: 更新期权PCR数据")
            print("=" * 60)
            try:
                opt_dir = self.data_center_path / 'market' / 'derivatives' / 'options'
                opt_dir.mkdir(parents=True, exist_ok=True)
                pcr_file = opt_dir / 'pcr_daily.parquet'
                
                # 确定开始日期
                latest_date = None
                if start_date is None:
                    latest_date = self._get_latest_date(pcr_file, 'trade_date')
                    if latest_date:
                        latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                        start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                    else:
                        start_date_actual = '20150101'  # 期权数据从2015年开始
                else:
                    start_date_actual = start_date
                
                # 检查是否需要更新
                if latest_date and latest_date >= end_date:
                    print(f"  数据已是最新 (截止 {latest_date})")
                else:
                    print(f"  下载 {start_date_actual} 至 {end_date} 的期权数据并计算PCR...")
                    print(f"  注意：此步骤可能需要较长时间...")
                    
                    # 生成日期列表
                    start_dt = datetime.strptime(start_date_actual, '%Y%m%d')
                    end_dt = datetime.strptime(end_date, '%Y%m%d')
                    date_list = []
                    current_dt = start_dt
                    while current_dt <= end_dt:
                        date_list.append(current_dt.strftime('%Y%m%d'))
                        current_dt += timedelta(days=1)
                    
                    # 按日下载并计算PCR
                    pcr_records = []
                    for trade_date in date_list:
                        try:
                            # 下载当日所有期权数据（SSE和SZSE）
                            df_sse = self._safe_api_call(
                                self.pro.opt_daily,
                                exchange='SSE',
                                trade_date=trade_date
                            )
                            
                            df_szse = self._safe_api_call(
                                self.pro.opt_daily,
                                exchange='SZSE',
                                trade_date=trade_date
                            )
                            
                            # 合并数据
                            df_list = []
                            if df_sse is not None and not df_sse.empty:
                                df_list.append(df_sse)
                            if df_szse is not None and not df_szse.empty:
                                df_list.append(df_szse)
                            
                            if df_list:
                                df_opt = pd.concat(df_list, ignore_index=True)
                                
                                # 计算PCR (Put-Call Ratio)
                                # PCR = Put成交量 / Call成交量
                                put_vol = df_opt[df_opt['call_put'] == 'P']['vol'].sum()
                                call_vol = df_opt[df_opt['call_put'] == 'C']['vol'].sum()
                                
                                if call_vol > 0:
                                    pcr = put_vol / call_vol
                                    pcr_records.append({
                                        'trade_date': trade_date,
                                        'put_volume': put_vol,
                                        'call_volume': call_vol,
                                        'pcr': pcr
                                    })
                            
                            # 限流
                            time.sleep(0.5)
                        except Exception as e:
                            print(f"  ⚠️  {trade_date} 下载失败: {e}")
                            continue
                    
                    if pcr_records:
                        df_pcr = pd.DataFrame(pcr_records)
                        
                        # 合并数据
                        if pcr_file.exists():
                            existing_df = pd.read_parquet(pcr_file, engine='pyarrow')
                            combined_df = pd.concat([existing_df, df_pcr], ignore_index=True)
                            combined_df = combined_df.drop_duplicates(subset=['trade_date']).sort_values('trade_date')
                            combined_df.to_parquet(pcr_file, engine='pyarrow', index=False)
                        else:
                            df_pcr.to_parquet(pcr_file, engine='pyarrow', index=False)
                        
                        print(f"  ✅ 成功更新 {len(pcr_records)} 条PCR记录")
                    else:
                        print(f"  ⚠️  无新数据")
            except Exception as e:
                print(f"❌ 更新期权PCR数据失败: {e}")
                failed_steps.append("期权PCR数据")
        
        # ========== 4. 更新期货主力持仓 ==========
        if include_futures_holding:
            print("\n" + "=" * 60)
            print("步骤4: 更新期货主力持仓")
            print("=" * 60)
            try:
                fut_dir = self.data_center_path / 'market' / 'derivatives' / 'futures' / 'holding'
                fut_dir.mkdir(parents=True, exist_ok=True)
                
                # 股指期货品种
                varieties = ['IF', 'IC', 'IM', 'IH']
                
                for variety in varieties:
                    print(f"\n  处理 {variety} 期货持仓数据...")
                    file_path = fut_dir / f'{variety}_holding.parquet'
                    
                    # 确定开始日期
                    latest_date = None
                    if start_date is None:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                            start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                        else:
                            start_date_actual = '20150101'  # 期货数据从2015年开始
                    else:
                        start_date_actual = start_date
                    
                    # 检查是否需要更新
                    if latest_date and latest_date >= end_date:
                        print(f"    {variety}: 数据已是最新 (截止 {latest_date})")
                        continue
                    
                    # 下载持仓数据
                    print(f"    {variety}: 下载 {start_date_actual} 至 {end_date} 的持仓数据...")
                    df = self._safe_api_call(
                        self.pro.fut_holding,
                        symbol=variety,
                        start_date=start_date_actual,
                        end_date=end_date
                    )
                    
                    if df is not None and not df.empty:
                        # 合并数据
                        if file_path.exists():
                            existing_df = pd.read_parquet(file_path, engine='pyarrow')
                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                            combined_df = combined_df.drop_duplicates(subset=['trade_date', 'symbol', 'broker']).sort_values('trade_date')
                            combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                        else:
                            df.to_parquet(file_path, engine='pyarrow', index=False)
                        
                        print(f"    {variety}: ✅ 成功更新 {len(df)} 条记录")
                    else:
                        print(f"    {variety}: ⚠️  无新数据")
                    
                    time.sleep(0.5)  # API限流
            except Exception as e:
                print(f"❌ 更新期货持仓数据失败: {e}")
                failed_steps.append("期货持仓数据")
        
        # ========== 总结 ==========
        print("\n" + "=" * 80)
        if failed_steps:
            print(f"⚠️  四维择时策略数据更新完成，但有 {len(failed_steps)} 个步骤失败:")
            for step in failed_steps:
                print(f"   - {step}")
        else:
            print("✅ 四维择时策略所需数据全部更新完成！")
        print("=" * 80)


    def update_all(self, start_date: Optional[str] = None, end_date: Optional[str] = None, 
                   include_sw_member: bool = False):
        """
        更新所有基础原料数据（仅下载，不含因子计算）
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            include_sw_member: 是否包含申万行业成分股数据（可选，默认False，因为数据量大且更新不频繁）
        
        注意：
            - 此函数只负责下载基础原料数据，不包含任何因子计算步骤
            - 财务三大表（利润表、资产负债表、现金流量表）请使用 download_financial_slow.py 脚本单独下载
            - 个股融资融券明细数据量大，建议单独运行 update_stock_margin_detail()
            - 申万行业成分股数据更新较慢，默认不包含，可通过 include_sw_member=True 启用
            - 因子计算请使用菜单选项12或直接运行对应的构建脚本
        """
        print("=" * 80)
        print("开始更新所有基础原料数据（仅下载，不含因子计算）")
        print("=" * 80)
        
        failed_steps = []
        
        # 更新各种数据
        print("\n步骤1: 更新基础数据...")
        try:
            self.update_stock_basic()
        except Exception as e:
            print(f"❌ 更新股票基础信息失败: {e}")
            failed_steps.append("股票基础信息")
        
        try:
            self.update_risk_free_rate(start_date, end_date)
        except Exception as e:
            print(f"❌ 更新无风险利率失败: {e}")
            failed_steps.append("无风险利率")
        
        try:
            self.update_sw_industry_daily(start_date, end_date)
        except Exception as e:
            print(f"❌ 更新申万行业分类失败: {e}")
            failed_steps.append("申万行业分类")
        
        try:
            self.update_index_daily(None, start_date, end_date)  # type: ignore
        except Exception as e:
            print(f"❌ 更新指数日K线失败: {e}")
            failed_steps.append("指数日K线")
            
        try:
            self.update_global_indices(start_date, end_date)  # 🆕 全球指数
        except Exception as e:
            print(f"❌ 更新全球重要指数失败: {e}")
            failed_steps.append("全球重要指数")
        
        try:
            self.update_index_constituents()
        except Exception as e:
            print(f"❌ 更新指数成分股失败: {e}")
            failed_steps.append("指数成分股")
        
        print("\n步骤2: 更新股票核心数据...")
        try:
            self.update_stock_daily_hfq(start_date, end_date)  # 包含自动前复权转换
        except Exception as e:
            print(f"❌ 更新股票日K线数据失败: {e}")
            failed_steps.append("股票日K线数据")
            
        # try:
        #     self.update_hk_stock_daily_hfq(start_date, end_date)  # 🆕 港股通数据 (已移除，需单独更新)
        # except Exception as e:
        #     print(f"❌ 更新港股通日K线数据失败: {e}")
        #     failed_steps.append("港股通日K线数据")
        
        try:
            self.update_stock_moneyflow(start_date, end_date)  # 🆕 资金流向数据
        except Exception as e:
            print(f"❌ 更新股票资金流向数据失败: {e}")
            failed_steps.append("股票资金流向数据")
        
        print("\n步骤3: 更新因子模型原材料...")
        try:
            self.update_daily_basic(start_date, end_date)
        except Exception as e:
            print(f"❌ 更新股票每日基础指标失败: {e}")
            failed_steps.append("股票每日基础指标")
        
        print("\n步骤4: 更新市场数据（融资融券、沪深港通）...")
        try:
            self.update_margin_total(start_date, end_date)
        except Exception as e:
            print(f"❌ 更新融资融券交易汇总失败: {e}")
            failed_steps.append("融资融券交易汇总")
        
        # try:
        #     self.update_moneyflow_hsgt(start_date, end_date)
        # except Exception as e:
        #     print(f"❌ 更新沪深港通资金流向失败: {e}")
        #     failed_steps.append("沪深港通资金流向")
        
        # 注意：财务指标数据请使用专门的程序单独更新（每季度一次）
        # 注意：个股融资融券明细数据量大，建议单独运行 update_stock_margin_detail()
        
        # 可选：申万行业成分股数据（数据量大，更新较慢）
        if include_sw_member:
            print("\n步骤5: 更新申万行业成分股数据（可选）...")
            print("  注意：此步骤可能需要较长时间")
            try:
                self.update_sw_industry_member_latest()
            except Exception as e:
                print(f"❌ 更新申万行业成分股失败: {e}")
                failed_steps.append("申万行业成分股")
            # update_sw_l2_member_history() 数据量更大，建议单独运行
        
        print("\n" + "=" * 80)
        if failed_steps:
            print(f"⚠️  数据更新完成，但有 {len(failed_steps)} 个步骤失败:")
            for step in failed_steps:
                print(f"   - {step}")
            print("\n💡 建议：检查上述失败的步骤，必要时单独运行更新")
        else:
            print("✅ 数据更新完成！")
        print("=" * 80)


def main():
    """主函数"""
    print("=" * 80)
    print("A股量化研究数据中心 - 数据管理工具")
    print("=" * 80)
    
    # 获取配置
    data_center_path = input("请输入数据中心路径 (直接回车使用当前目录下的quant_data_center): ").strip()
    if not data_center_path:
        data_center_path = None
    else:
        # 验证路径
        test_path = Path(data_center_path)
        if not test_path.is_absolute():
            # 如果是相对路径，转换为绝对路径
            data_center_path = str(Path.cwd() / data_center_path)
        
        # 检查路径是否合理（避免输入单个字符如"1"）
        if len(data_center_path) < 5 or data_center_path.endswith('/1') or data_center_path.endswith('\\1'):
            print(f"⚠️  警告: 路径 '{data_center_path}' 看起来不正确")
            print(f"   默认路径应该是: {Path.cwd() / 'quant_data_center'}")
            confirm = input("   是否继续使用此路径? (y/n): ").strip().lower()
            if confirm != 'y':
                print("已取消，使用默认路径")
                data_center_path = None
    
    # 创建数据管理器（自动从config.py读取Token）
    try:
        manager = QuantDataManager(data_center_path)  # type: ignore
        print(f"\n✅ 数据中心路径: {manager.data_center_path}")
        print(f"   如果路径不正确，请按 Ctrl+C 退出并重新运行\n")
        
        # 选择更新模式
        print("\n请选择更新模式:")
        print("1. 更新所有数据（仅下载，不含因子计算）")
        print("2. 更新股票日K线数据")
        print("3. 更新股票资金流向数据")
        print("4. 更新指数成分股数据")
        print("5. 更新无风险利率")
        print("6. 更新申万行业数据（分类+成分股+历史映射）")
        print("7. 更新指数日K线（含全球重要指数）")
        print("8. 更新股票基础信息")
        print("9. 更新股票每日基础指标")
        print("10. 更新/计算因子策略")
        print("11. 更新融资融券交易汇总")
        print("12. 更新个股融资融券明细")
        print("13. 更新港股通个股日K线 (🆕)")
        print("14. 更新四维择时策略所需数据（指数估值PE/PB + 国债收益率）")
        print("15. 更新CFFEX期货主力合约前20名会员持仓数据 (🆕)")
        print("16. 更新股票每日筹码分布统计数据 (🆕)")
        # 注意：财务指标数据请使用专门的程序单独更新（每季度一次）
        
        choice = input("请输入选择 (1-16): ").strip()
        
        
        if choice == '1':
            manager.update_all()
        elif choice == '2':
            manager.update_stock_daily_hfq()
        elif choice == '3':
            manager.update_stock_moneyflow()
        elif choice == '16':
            manager.update_stock_cyq_perf()
        elif choice == '4':
            manager.update_index_constituents()
        elif choice == '5':
            manager.update_risk_free_rate()
        elif choice == '6':
            # 合并的申万行业数据更新
            print("\n" + "="*60)
            print("更新申万行业数据（包含分类、成分股、历史映射）")
            print("="*60)
            manager.update_sw_industry_daily()
            manager.update_sw_industry_member_latest()
            manager.update_sw_l2_member_history()
        elif choice == '7':
            # 合并的指数日K线更新（含全球指数）
            print("\n" + "="*60)
            print("更新指数日K线（包含A股指数和全球重要指数）")
            print("="*60)
            manager.update_index_daily()
            manager.update_global_indices()
        elif choice == '8':
            manager.update_stock_basic()
        elif choice == '9':
            manager.update_daily_basic()
        elif choice == '10':
            print("\n" + "=" * 80)
            print("因子策略计算")
            print("=" * 80)
            print("\n请选择要执行的因子计算:")
            print("  1. 构建Fama-French五因子")
            print("  2. 构建Fama-French三因子（全市场）")
            print("  3. 构建自定义因子")
            print("  4. 构建CH3因子")
            print("\n提示: 请运行对应的构建脚本:")
            print("  - python build_ff5_factors_monthly_ttm.py")
            print("  - python build_ff3_factors_full_market.py")
            print("  - python build_custom_factors.py")
            print("  - python build_ch3_factors.py")
            print("=" * 80)
        elif choice == '11':
            manager.update_margin_total()
        elif choice == '12':
            manager.update_stock_margin_detail()
        elif choice == '13':
            manager.update_hk_stock_daily_hfq()
        elif choice == '14':
            # 更新四维择时策略所需数据
            print("\n" + "="*60)
            print("更新四维择时策略所需数据")
            print("="*60)
            manager.update_missing_data_for_strategy(
                include_index_valuation=True,
                include_bond_yield=True,
                include_options_pcr=False,
                include_futures_holding=False
            )
        elif choice == '15':
            # 更新CFFEX期货主力合约前20名会员持仓数据
            manager.update_future_holdings()
        else:
            print("无效选择")
            return 1
        
    except Exception as e:
        print(f"初始化失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
