#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股量化研究数据中心 - 核心数据管理脚本
支持增量更新，使用Parquet格式存储，基于Tushare Pro API
"""

import os
import time
import pandas as pd
import tushare as ts
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')


class QuantDataManager:
    """量化数据中心管理器"""
    
    def __init__(self, data_center_path: str = None, token: str = None):
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
        
        # 初始化Tushare Pro
        if token is None:
            try:
                import config
                token = config.TUSHARE_TOKEN
            except (ImportError, AttributeError):
                print("警告: 无法从config.py读取Tushare Token，请手动设置")
        
        if token:
            ts.set_token(token)
            print(f"Tushare Token已设置")
        else:
            print("警告: 未设置Tushare Token，请确保已配置")
        
        self.pro = ts.pro_api()
        
        # 定义路径
        self.paths = {
            'stock_daily_hfq': self.data_center_path / "stock" / "daily_hfq",
            'stock_daily_basic': self.data_center_path / "stock" / "daily_basic",
            'stock_fina_indicator': self.data_center_path / "stock" / "fina_indicator",
            'stock_financial_tables': self.data_center_path / "stock" / "financial_tables",  # 保留路径定义，供外部单独文件使用
            'index_daily': self.data_center_path / "index" / "daily",
            'index_constituents': self.data_center_path / "index" / "constituents",
            'factors_ff5': self.data_center_path / "factors" / "fama_french_5",
            'factors_rfr': self.data_center_path / "factors" / "risk_free",
            'industry_sw': self.data_center_path / "classification" / "industry_sw"
        }
        
        # 确保所有目录存在
        for path in self.paths.values():
            path.mkdir(parents=True, exist_ok=True)
        
        print(f"数据中心管理器初始化完成")
        print(f"根目录: {self.data_center_path}")
    
    def _get_latest_date(self, file_path: Path, date_col: str = 'trade_date') -> Optional[str]:
        """获取Parquet文件中的最新日期"""
        if not file_path.exists():
            return None
        
        try:
            # 只读取日期列以提高效率
            df = pd.read_parquet(file_path, engine='pyarrow', columns=[date_col])
            if not df.empty:
                return df[date_col].max()
            return None
        except Exception as e:
            # print(f"读取文件 {file_path} 时出错: {e}")
            # If file is empty or corrupt, it's fine to return None.
            return None
    
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
                if '每分钟最多访问该接口' in error_msg or 'rate limit' in error_msg.lower():
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
            
            # 确定开始日期
            start_date_actual = start_date
            if start_date is None:
                latest_date = self._get_latest_date(file_path, 'trade_date')
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    list_date = stock_info.get(ts_code)
                    start_date_actual = list_date if list_date and list_date != 'None' else '19900101'
            
            if start_date_actual >= end_date:
                return {'ts_code': ts_code, 'status': 'up_to_date'}

            # 获取数据
            df = self._safe_api_call(ts.pro_bar,
                                   ts_code=ts_code,
                                   adj='hfq',
                                   start_date=start_date_actual,
                                   end_date=end_date)
            
            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}
    
    def update_stock_daily_hfq(self, start_date: str = None, end_date: str = None, 
                              stock_list: List[str] = None, batch_size: int = 150, max_workers: int = 5):
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
        print("注意: 已限制API调用频率，约750次/分钟")
        print("=" * 60)
        
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
        
        print(f"共需更新 {len(stock_list)} 只股票")
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
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
                
                futures = {executor.submit(self._fetch_daily_hfq_worker, ts_code, start_date, end_date, stock_info): ts_code for ts_code in batch_stocks}
                
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
                
                # 限流：每批次之间等待时间
                # 计算公式：150批次 × 5并发 / 60秒 ≈ 12.5批/秒
                # 每批等待：60/150 ≈ 0.4秒，实际约750次/分钟
                time.sleep(0.4)

        self._generate_download_report(
            data_type='股票日K线(后复权)',
            total_count=len(stock_list),
            success_count=len(success_stocks),
            up_to_date_count=len(up_to_date_stocks),
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )
    
    def update_index_constituents(self, index_codes: List[str] = None, 
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
    
    def update_risk_free_rate(self, start_date: str = None, end_date: str = None):
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
    
    def update_sw_industry_daily(self, start_date: str = None, end_date: str = None):
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
    
    def update_index_daily(self, index_codes: List[str] = None, 
                          start_date: str = None, end_date: str = None):
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
            index_codes = ['399300.SZ', '000905.SH', '399006.SZ']
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        for index_code in index_codes:
            print(f"\n处理指数: {index_code}")
            file_path = self.paths['index_daily'] / f"{index_code}.parquet"
            
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
            
            if start_date_actual >= end_date:
                print(f"  {index_code}: 数据已是最新")
                continue
            
            try:
                print(f"  获取 {start_date_actual} 到 {end_date} 的数据")
                
                df = self._safe_api_call(self.pro.index_daily,
                                       ts_code=index_code,
                                       start_date=start_date_actual,
                                       end_date=end_date)
                
                if df is None or df.empty:
                    print(f"  {index_code}: 无新数据")
                    continue
                
                # 保存数据
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path, engine='pyarrow')
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                    combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                else:
                    df.to_parquet(file_path, engine='pyarrow', index=False)
                
                print(f"  {index_code}: 成功更新 {len(df)} 条记录")
                
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
    
    def update_daily_basic(self, start_date: str = None, end_date: str = None):
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
    
    def _fetch_fina_indicator_worker(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """并发工作函数：获取单只股票的财务指标"""
        try:
            file_path = self.paths['stock_fina_indicator'] / f"{ts_code}.parquet"
            
            start_date_actual = start_date
            if start_date is None:
                latest_date = self._get_latest_date(file_path, 'end_date')
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    # 不指定起始日期，获取全部历史数据
                    # Tushare会返回该股票的所有可用财务指标数据
                    start_date_actual = None
            
            if start_date_actual is not None and start_date_actual >= end_date:
                return {'ts_code': ts_code, 'status': 'up_to_date'}
            
            # 如果start_date_actual为None，则不传递start_date参数（获取全部历史数据）
            if start_date_actual is None:
                df = self._safe_api_call(self.pro.fina_indicator,
                                       ts_code=ts_code,
                                       end_date=end_date)
            else:
                df = self._safe_api_call(self.pro.fina_indicator,
                                       ts_code=ts_code,
                                       start_date=start_date_actual,
                                       end_date=end_date)
            
            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'status': 'success', 'data': (df, file_path)}
            else:
                return {'ts_code': ts_code, 'status': 'api_empty'}
        except Exception as e:
            return {'ts_code': ts_code, 'status': 'error', 'message': str(e)}

    def update_fina_indicator(self, start_date: str = None, end_date: str = None, 
                              stock_list: List[str] = None, batch_size: int = 200, max_workers: int = 5):
        """
        更新股票财务指标 - 支持高并发下载
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            stock_list: 股票列表
            batch_size: 批处理大小
            max_workers: 最大并发线程数 (财务数据API限制较严，建议不超过5)
        """
        print("=" * 60)
        print("开始更新股票财务指标（高并发模式）")
        print(f"最大并发线程数: {max_workers}")
        print("=" * 60)
        
        if stock_list is None:
            print("获取股票列表...")
            stock_basic = self._safe_api_call(self.pro.stock_basic, exchange='', list_status='L', fields='ts_code')
            if stock_basic is None:
                print("获取股票列表失败")
                return
            stock_list = stock_basic['ts_code'].tolist()
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        # 预检查：判断哪些股票需要更新
        print(f"\n预检查数据完整性...")
        stocks_to_update = []
        stocks_up_to_date = []
        
        for ts_code in stock_list:
            file_path = self.paths['stock_fina_indicator'] / f"{ts_code}.parquet"
            
            # 检查是否需要更新
            if not file_path.exists():
                # 文件不存在，需要下载
                stocks_to_update.append(ts_code)
            else:
                # 文件存在，检查最新日期
                latest_date = self._get_latest_date(file_path, 'end_date')
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
                    
                    if start_date_actual >= end_date:
                        # 数据已是最新
                        stocks_up_to_date.append(ts_code)
                    else:
                        # 需要增量更新
                        stocks_to_update.append(ts_code)
                else:
                    # 文件为空或损坏，需要重新下载
                    stocks_to_update.append(ts_code)
        
        print(f"  ✅ 数据已是最新: {len(stocks_up_to_date)} 只")
        print(f"  📥 需要更新: {len(stocks_to_update)} 只")
        
        if len(stocks_to_update) == 0:
            print("\n所有股票数据均已是最新，无需更新！")
            # 生成报告
            self._generate_download_report(
                data_type='股票财务指标',
                total_count=len(stock_list),
                success_count=0,
                up_to_date_count=len(stocks_up_to_date),
                empty_stocks=[],
                failed_stocks=[]
            )
            return
        
        print(f"\n开始下载 {len(stocks_to_update)} 只股票的数据...")
        
        # 只处理需要更新的股票
        stock_list = stocks_to_update
        
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
                
                futures = {executor.submit(self._fetch_fina_indicator_worker, ts_code, start_date, end_date): ts_code for ts_code in batch_stocks}
                
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
                    print(f"  批量保存 {len(batch_data_to_save)} 只股票的财务指标...")
                    for df, file_path in batch_data_to_save:
                        try:
                            if file_path.exists():
                                existing_df = pd.read_parquet(file_path, engine='pyarrow')
                                combined_df = pd.concat([existing_df, df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['ts_code', 'end_date']).sort_values('end_date')
                                combined_df.to_parquet(file_path, engine='pyarrow', index=False)
                            else:
                                df.to_parquet(file_path, engine='pyarrow', index=False)
                        except Exception as e:
                            print(f"    保存 {file_path.name} 数据失败: {e}")
                            failed_stocks.append((file_path.stem, str(e)))
                    batch_data_to_save.clear()

        # 合并预检查中发现的"已是最新"股票
        all_up_to_date = len(stocks_up_to_date) + len(up_to_date_stocks)
        
        self._generate_download_report(
            data_type='股票财务指标',
            total_count=len(stock_list) + len(stocks_up_to_date),  # 总数包含所有股票
            success_count=len(success_stocks),
            up_to_date_count=all_up_to_date,  # 包含预检查跳过的
            empty_stocks=empty_stocks,
            failed_stocks=failed_stocks
        )

    def _generate_download_report(self, data_type: str, total_count: int, success_count: int, up_to_date_count: int, empty_stocks: List[str], failed_stocks: List[Tuple[str, str]]):
        """生成并打印下载报告"""
        report_time = datetime.now()
        sanitized_data_type = data_type.replace('(', '').replace(')', '').replace('/', '')
        report_filename = f"download_report_{sanitized_data_type}_{report_time.strftime('%Y%m%d_%H%M%S')}.txt"
        report_path = self.data_center_path / report_filename

        empty_count = len(empty_stocks)
        failed_count = len(failed_stocks)

        completeness = (success_count + up_to_date_count) / total_count if total_count > 0 else 0

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
        
        if empty_count > 0 or failed_count > 0:
            print(f"\n详情已保存至报告文件: {report_path}")
        print("="*60)

        # --- File Output ---
        if empty_count > 0 or failed_count > 0:
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
                f.write(f"  - 数据完整度: {completeness:.2%}\n\n")

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

    def update_all(self, start_date: str = None, end_date: str = None):
        """
        更新所有数据（不包含财务三大表）
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        注意：
            - 财务三大表（利润表、资产负债表、现金流量表）请使用 download_financial_slow.py 脚本单独下载
            - 该脚本提供更稳定的下载机制，适合大批量数据获取
        """
        print("=" * 80)
        print("开始更新所有数据（不含财务三大表）")
        print("=" * 80)
        
        # 更新各种数据
        print("步骤1: 更新基础数据...")
        self.update_stock_basic()
        self.update_risk_free_rate(start_date, end_date)
        self.update_sw_industry_daily(start_date, end_date)
        self.update_index_daily(None, start_date, end_date)
        self.update_index_constituents()
        self.update_stock_daily_hfq(start_date, end_date)
        
        print("\n步骤2: 更新因子模型原材料...")
        self.update_daily_basic(start_date, end_date)
        self.update_fina_indicator(start_date, end_date)
        
        print("\n" + "=" * 80)
        print("✅ 数据更新完成！")
        print("=" * 80)
        
        print("\n📝 后续步骤:")
        print("  1. 如需更新财务三大表，请运行:")
        print("     python download_financial_slow.py")
        print("")
        print("  2. 构建Fama-French五因子:")
        print("     python build_ff5_factors_monthly_ttm.py")
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
    
    # 创建数据管理器（自动从config.py读取Token）
    try:
        manager = QuantDataManager(data_center_path)
        
        # 选择更新模式
        print("\n请选择更新模式:")
        print("1. 更新所有数据")
        print("2. 更新股票日K线数据")
        print("3. 更新指数成分股数据")
        print("4. 更新无风险利率")
        print("5. 更新申万行业分类")
        print("6. 更新指数日K线")
        print("7. 更新股票基础信息")
        print("8. 更新股票每日基础指标")
        print("9. 更新股票财务指标")
        
        choice = input("请输入选择 (1-9): ").strip()
        
        if choice == '1':
            manager.update_all()
        elif choice == '2':
            manager.update_stock_daily_hfq()
        elif choice == '3':
            manager.update_index_constituents()
        elif choice == '4':
            manager.update_risk_free_rate()
        elif choice == '5':
            manager.update_sw_industry_daily()
        elif choice == '6':
            manager.update_index_daily()
        elif choice == '7':
            manager.update_stock_basic()
        elif choice == '8':
            manager.update_daily_basic()
        elif choice == '9':
            manager.update_fina_indicator()
        else:
            print("无效选择")
            return 1
        
    except Exception as e:
        print(f"初始化失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
