#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通达信板块分类数据下载脚本（独立版本）

由于API限制严格（每天最多5次），单独管理更方便
API限制：
- tdx_index: 单次最多3000条，每分钟最多2次，每天最多5次
- tdx_member: 单次最多3000条
"""

import os
import time
import pandas as pd
import tushare as ts
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import warnings
warnings.filterwarnings('ignore')


class TDXIndustryDownloader:
    """通达信板块分类数据下载器"""
    
    def __init__(self, data_center_path: Optional[str] = None, token: Optional[str] = None):
        """
        初始化下载器
        
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
        self.industry_tdx_path = self.data_center_path / "classification" / "industry_tdx"
        self.industry_tdx_path.mkdir(parents=True, exist_ok=True)
        
        self.index_file_path = self.industry_tdx_path / "tdx_index_daily.parquet"
        self.member_file_path = self.industry_tdx_path / "tdx_member_daily.parquet"
        
        print(f"通达信板块数据下载器初始化完成")
        print(f"数据保存路径: {self.industry_tdx_path}")
    
    def _get_latest_date(self, file_path: Path, date_col: str = 'trade_date') -> Optional[str]:
        """获取Parquet文件中的最新日期"""
        if not file_path.exists():
            return None
        
        try:
            df = pd.read_parquet(file_path, engine='pyarrow', columns=[date_col])
            if not df.empty:
                return df[date_col].max()
            return None
        except Exception as e:
            return None
    
    def _safe_api_call(self, func, *args, **kwargs):
        """
        安全的API调用，包含重试机制和智能限流处理
        """
        max_retries = 3
        retry_delay = 1
        rate_limit_retries = 0
        
        attempt = 0
        while attempt < max_retries:
            try:
                result = func(*args, **kwargs)
                if result is not None and not result.empty:
                    return result
                else:
                    return None
            except Exception as e:
                error_msg = str(e)
                
                # 检测是否是API限流错误
                if '每天最多访问该接口' in error_msg:
                    print(f"❌ API每日限制已达到: {error_msg}")
                    print(f"   建议:")
                    print(f"   1. 等待明天再继续下载")
                    print(f"   2. 升级Tushare账户以获得更高的调用限额")
                    print(f"   3. 分批下载，每天下载少量日期")
                    return None
                elif '每分钟最多访问该接口' in error_msg or 'rate limit' in error_msg.lower():
                    rate_limit_retries += 1
                    print(f"⚠️  API限流 (第{rate_limit_retries}次): {error_msg}")
                    print(f"   等待60秒后自动重试...")
                    time.sleep(60)
                    continue
                
                # 其他错误：正常重试逻辑
                attempt += 1
                print(f"API调用失败 (尝试 {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay * attempt)
                else:
                    raise e
        
        return None
    
    def _fetch_tdx_index_batch(self, trade_date: str, idx_type: Optional[str] = None):
        """
        分批获取通达信板块列表（处理3000条限制）
        
        Args:
            trade_date: 交易日期
            idx_type: 板块类型（可选，用于分批）
        
        Returns:
            DataFrame: 板块列表数据
        """
        all_data = []
        MAX_LIMIT = 3000
        
        try:
            params = {
                'trade_date': trade_date,
                'fields': 'ts_code,name,idx_type,idx_count'
            }
            
            df = self._safe_api_call(self.pro.tdx_index, **params)
            
            if df is not None and not df.empty:
                # 检查是否达到限制
                if len(df) >= MAX_LIMIT:
                    print(f"    ⚠️  警告: {trade_date} 板块数据达到限制 ({len(df)} 条)，可能需要分批获取")
                    if 'idx_type' in df.columns:
                        idx_types = df['idx_type'].unique()
                        print(f"    发现 {len(idx_types)} 种板块类型，尝试按类型分批获取...")
                
                all_data.append(df)
            
            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                result = result.drop_duplicates(subset=['ts_code']).sort_values('ts_code')
                return result
            return pd.DataFrame()
            
        except Exception as e:
            print(f"    获取板块列表失败: {e}")
            return pd.DataFrame()
    
    def _fetch_tdx_member_batch(self, trade_date: str, ts_code: str):
        """
        分批获取通达信板块成分股（处理3000条限制）
        
        Args:
            trade_date: 交易日期
            ts_code: 板块代码
        
        Returns:
            DataFrame: 板块成分股数据
        """
        all_data = []
        MAX_LIMIT = 3000
        
        try:
            df = self._safe_api_call(self.pro.tdx_member,
                                   trade_date=trade_date,
                                   ts_code=ts_code)
            
            if df is not None and not df.empty:
                if len(df) >= MAX_LIMIT:
                    print(f"      ⚠️  警告: 板块 {ts_code} 成分股达到限制 ({len(df)} 只)")
                
                all_data.append(df)
            
            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                # 去重（根据实际返回的字段）
                if 'code' in result.columns:
                    result = result.drop_duplicates(subset=['code'])
                elif 'con_code' in result.columns:
                    result = result.drop_duplicates(subset=['con_code'])
                else:
                    result = result.drop_duplicates()
                return result
            return pd.DataFrame()
            
        except Exception as e:
            return pd.DataFrame()
    
    def update_tdx_industry(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新通达信板块分类数据（日度）
        
        下载两个数据：
        1. 板块列表数据 (tdx_index): 每日的板块列表，包含板块代码、名称、类型、成分股数量等
        2. 板块成分股数据 (tdx_member): 每日各板块的成分股
        
        **API限制处理**:
        - tdx_index: 单次最多3000条，按日期分批获取
        - tdx_member: 单次最多3000条，按板块代码分批获取
        
        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        """
        print("=" * 60)
        print("开始更新通达信板块分类数据")
        print("⚠️  API限制说明（重要）:")
        print("   - tdx_index: 单次最多3000条，每分钟最多2次，每天最多5次")
        print("   - tdx_member: 单次最多3000条")
        print("   注意: 由于每天限制，建议分批下载或升级账户")
        print("=" * 60)
        
        # 确定开始日期
        if start_date is None:
            latest_date = self._get_latest_date(self.index_file_path)
            if latest_date:
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                start_date_actual = (latest_dt + timedelta(days=1)).strftime('%Y%m%d')
            else:
                start_date_actual = '20200101'  # TDX板块数据通常从2020年开始
        else:
            start_date_actual = start_date
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if start_date_actual >= end_date:
            print("通达信板块分类数据已是最新")
            return
        
        print(f"获取 {start_date_actual} 到 {end_date} 的通达信板块分类数据")
        
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
            
            # 检查：由于每天限制5次，计算需要多少天
            days_needed = (len(trade_dates) + 4) // 5  # 向上取整
            if len(trade_dates) > 5:
                print(f"⚠️  警告: 由于tdx_index每天最多5次，完整下载需要约 {days_needed} 天")
                print(f"   建议: 分批下载，每次下载少量日期（建议每次不超过5天）")
                print(f"   继续吗？程序会在遇到每日限制时自动停止")
            
            all_index_data = []
            all_member_data = []
            total_plates = 0
            total_members = 0
            daily_call_count = 0  # 记录当天的调用次数
            
            for i, trade_date in enumerate(trade_dates, 1):
                if i % 10 == 0 or i == 1:
                    print(f"  进度: {i}/{len(trade_dates)} ({i/len(trade_dates)*100:.1f}%)")
                
                # 1. 分批获取当日板块列表（处理3000条限制）
                # ⚠️ tdx_index API限制：每分钟最多2次，每天最多5次
                # 检查每日限制
                if daily_call_count >= 5:
                    print(f"\n⚠️  已达到每日API调用限制（5次）")
                    print(f"   已处理 {i-1}/{len(trade_dates)} 个交易日")
                    print(f"   请明天继续下载，或升级账户以获得更高限额")
                    print(f"   建议使用增量更新，每天只下载最新数据")
                    break
                
                print(f"    [{trade_date}] 获取板块列表...", end=' ', flush=True)
                df_index = self._fetch_tdx_index_batch(trade_date)
                
                # 如果API返回None（可能是每日限制），停止处理
                if df_index is None:
                    print(f"\n⚠️  遇到API限制，停止下载")
                    print(f"   已处理 {i-1}/{len(trade_dates)} 个交易日")
                    break
                
                daily_call_count += 1
                
                if df_index is not None and not df_index.empty:
                    df_index['trade_date'] = trade_date
                    all_index_data.append(df_index)
                    plate_count = len(df_index)
                    total_plates += plate_count
                    print(f"✓ 获取 {plate_count} 个板块")
                    
                    # 2. 分批获取每个板块的成分股（处理3000条限制）
                    for j, (_, row) in enumerate(df_index.iterrows(), 1):
                        ts_code = row['ts_code']
                        plate_name = row.get('name', ts_code)
                        
                        # 每10个板块显示一次进度
                        if j % 10 == 0 or j == len(df_index):
                            print(f"      板块成分股进度: {j}/{len(df_index)}", end='\r', flush=True)
                        
                        time.sleep(0.2)  # 控制API调用频率
                        
                        df_member = self._fetch_tdx_member_batch(trade_date, ts_code)
                        
                        if df_member is not None and not df_member.empty:
                            df_member['trade_date'] = trade_date
                            all_member_data.append(df_member)
                            total_members += len(df_member)
                        
                        # 每处理50个板块稍作等待，避免API限流
                        if j % 50 == 0:
                            time.sleep(1.0)
                    
                    if len(df_index) > 0:
                        print()  # 换行
                else:
                    print("× 无数据")
                
                # ⚠️ 关键：tdx_index API每分钟最多2次，每次调用后必须等待至少30秒
                # 等待时间设为31秒，确保不会触发限流
                if i < len(trade_dates):  # 最后一个交易日不需要等待
                    wait_seconds = 31
                    print(f"    等待 {wait_seconds} 秒以避免API限流（tdx_index每分钟最多2次）...", end=' ', flush=True)
                    time.sleep(wait_seconds)
                    print("✓")
            
            # 保存板块列表数据
            print("\n" + "=" * 60)
            print("保存数据...")
            if all_index_data:
                df_index_all = pd.concat(all_index_data, ignore_index=True)
                
                if self.index_file_path.exists():
                    existing_df = pd.read_parquet(self.index_file_path, engine='pyarrow')
                    combined_df = pd.concat([existing_df, df_index_all], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                    combined_df.to_parquet(self.index_file_path, engine='pyarrow', index=False)
                    print(f"✅ 成功更新板块列表:")
                    print(f"   新增: {len(df_index_all)} 条记录")
                    print(f"   总计: {len(combined_df)} 条记录")
                    print(f"   累计板块数: {total_plates}")
                else:
                    df_index_all = df_index_all.drop_duplicates(subset=['ts_code', 'trade_date']).sort_values('trade_date')
                    df_index_all.to_parquet(self.index_file_path, engine='pyarrow', index=False)
                    print(f"✅ 成功创建板块列表文件: {len(df_index_all)} 条记录")
            else:
                print("⚠️  无板块列表数据可保存")
            
            # 保存板块成分股数据
            if all_member_data:
                df_member_all = pd.concat(all_member_data, ignore_index=True)
                
                if self.member_file_path.exists():
                    existing_df = pd.read_parquet(self.member_file_path, engine='pyarrow')
                    combined_df = pd.concat([existing_df, df_member_all], ignore_index=True)
                    # 根据实际返回的字段去重
                    if 'code' in combined_df.columns:
                        combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date', 'code']).sort_values('trade_date')
                    elif 'con_code' in combined_df.columns:
                        combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date', 'con_code']).sort_values('trade_date')
                    else:
                        combined_df = combined_df.drop_duplicates().sort_values('trade_date')
                    combined_df.to_parquet(self.member_file_path, engine='pyarrow', index=False)
                    print(f"✅ 成功更新板块成分股:")
                    print(f"   新增: {len(df_member_all)} 条记录")
                    print(f"   总计: {len(combined_df)} 条记录")
                    print(f"   累计成分股数: {total_members}")
                else:
                    # 根据实际返回的字段去重
                    if 'code' in df_member_all.columns:
                        df_member_all = df_member_all.drop_duplicates(subset=['ts_code', 'trade_date', 'code']).sort_values('trade_date')
                    elif 'con_code' in df_member_all.columns:
                        df_member_all = df_member_all.drop_duplicates(subset=['ts_code', 'trade_date', 'con_code']).sort_values('trade_date')
                    else:
                        df_member_all = df_member_all.drop_duplicates().sort_values('trade_date')
                    df_member_all.to_parquet(self.member_file_path, engine='pyarrow', index=False)
                    print(f"✅ 成功创建板块成分股文件: {len(df_member_all)} 条记录")
            else:
                print("⚠️  无板块成分股数据可保存")
            
            print("=" * 60)
            
        except Exception as e:
            print(f"更新通达信板块分类数据失败: {e}")
            import traceback
            traceback.print_exc()


def main():
    """主函数"""
    print("=" * 80)
    print("通达信板块分类数据下载工具")
    print("=" * 80)
    
    # 获取配置
    data_center_path = input("请输入数据中心路径 (直接回车使用当前目录下的quant_data_center): ").strip()
    if not data_center_path:
        data_center_path = None
    
    try:
        downloader = TDXIndustryDownloader(data_center_path)  # type: ignore
        
        print("\n请选择操作:")
        print("1. 增量更新（从最新日期继续）")
        print("2. 指定日期范围更新")
        
        choice = input("请输入选择 (1-2): ").strip()
        
        if choice == '1':
            downloader.update_tdx_industry()
        elif choice == '2':
            start_date = input("请输入开始日期 (YYYYMMDD，直接回车使用默认): ").strip()
            end_date = input("请输入结束日期 (YYYYMMDD，直接回车使用今天): ").strip()
            
            start_date = start_date if start_date else None
            end_date = end_date if end_date else None
            
            downloader.update_tdx_industry(start_date=start_date, end_date=end_date)  # type: ignore
        else:
            print("无效选择")
            return 1
        
    except Exception as e:
        print(f"初始化失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

