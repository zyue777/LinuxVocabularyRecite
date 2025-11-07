#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据状态查看工具
一键查看数据中心各项数据的最新日期
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple


class DataStatusChecker:
    """数据状态检查器"""
    
    def __init__(self, data_center_path: str = None):
        """
        初始化数据状态检查器
        
        Args:
            data_center_path: 数据中心根目录路径
        """
        if data_center_path is None:
            self.data_center_path = Path.cwd() / "quant_data_center"
        else:
            self.data_center_path = Path(data_center_path)
        
        if not self.data_center_path.exists():
            print(f"❌ 数据中心路径不存在: {self.data_center_path}")
            return
        
        print(f"数据状态检查器初始化完成")
        print(f"数据中心路径: {self.data_center_path}")
    
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
    
    def _get_latest_date_from_dir(self, dir_path: Path, date_col: str = 'trade_date', 
                                   pattern: str = "*.parquet") -> Optional[str]:
        """从目录中所有文件获取最新日期"""
        if not dir_path.exists():
            return None
        
        latest_dates = []
        for file_path in dir_path.glob(pattern):
            try:
                latest_date = self._get_latest_date(file_path, date_col)
                if latest_date:
                    latest_dates.append(latest_date)
            except:
                continue
        
        if latest_dates:
            return max(latest_dates)
        return None
    
    def check_stock_basic(self) -> Dict[str, any]:
        """检查股票基础信息"""
        file_path = self.data_center_path / "stock_basic.parquet"
        
        status = {
            'name': '股票基础信息',
            'path': str(file_path),
            'exists': file_path.exists(),
            'latest_date': None,
            'count': 0
        }
        
        if file_path.exists():
            try:
                df = pd.read_parquet(file_path, engine='pyarrow')
                status['count'] = len(df)
                # 股票基础信息没有日期字段，使用文件修改时间
                status['latest_date'] = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        return status
    
    def check_stock_daily_hfq(self) -> Dict[str, any]:
        """检查股票日K线（后复权）"""
        dir_path = self.data_center_path / "stock" / "daily_hfq"
        
        status = {
            'name': '股票日K线（后复权）',
            'path': str(dir_path),
            'exists': dir_path.exists(),
            'latest_date': None,
            'file_count': 0,
            'total_records': 0
        }
        
        if dir_path.exists():
            files = list(dir_path.glob("*.parquet"))
            status['file_count'] = len(files)
            
            if files:
                latest_date = self._get_latest_date_from_dir(dir_path, 'trade_date')
                status['latest_date'] = latest_date
                
                # 统计总记录数（采样）
                if len(files) <= 100:
                    # 文件少时统计全部
                    total = 0
                    for f in files:
                        try:
                            df = pd.read_parquet(f, engine='pyarrow')
                            total += len(df)
                        except:
                            pass
                    status['total_records'] = total
                else:
                    # 文件多时采样统计
                    sample_files = files[:100]
                    sample_total = 0
                    for f in sample_files:
                        try:
                            df = pd.read_parquet(f, engine='pyarrow')
                            sample_total += len(df)
                        except:
                            pass
                    status['total_records'] = int(sample_total * len(files) / 100)
        
        return status
    
    def check_daily_basic(self) -> Dict[str, any]:
        """检查股票每日基础指标"""
        file_path = self.data_center_path / "stock" / "daily_basic" / "daily_basic_all.parquet"
        
        status = {
            'name': '股票每日基础指标',
            'path': str(file_path),
            'exists': file_path.exists(),
            'latest_date': None,
            'count': 0
        }
        
        if file_path.exists():
            try:
                latest_date = self._get_latest_date(file_path, 'trade_date')
                status['latest_date'] = latest_date
                
                df = pd.read_parquet(file_path, engine='pyarrow', columns=['trade_date'])
                status['count'] = len(df)
            except:
                pass
        
        return status
    
    def check_fina_indicator(self) -> Dict[str, any]:
        """检查股票财务指标"""
        dir_path = self.data_center_path / "stock" / "fina_indicator"
        
        status = {
            'name': '股票财务指标',
            'path': str(dir_path),
            'exists': dir_path.exists(),
            'latest_date': None,
            'file_count': 0
        }
        
        if dir_path.exists():
            files = list(dir_path.glob("*.parquet"))
            status['file_count'] = len(files)
            
            if files:
                latest_date = self._get_latest_date_from_dir(dir_path, 'end_date')
                status['latest_date'] = latest_date
        
        return status
    
    def check_index_daily(self) -> Dict[str, any]:
        """检查指数日K线"""
        dir_path = self.data_center_path / "index" / "daily"
        
        status = {
            'name': '指数日K线',
            'path': str(dir_path),
            'exists': dir_path.exists(),
            'latest_date': None,
            'file_count': 0,
            'indices': []
        }
        
        if dir_path.exists():
            files = list(dir_path.glob("*.parquet"))
            status['file_count'] = len(files)
            
            if files:
                latest_dates = []
                for file_path in files:
                    try:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dates.append(latest_date)
                            index_code = file_path.stem
                            status['indices'].append({
                                'code': index_code,
                                'latest_date': latest_date
                            })
                    except:
                        continue
                
                if latest_dates:
                    status['latest_date'] = max(latest_dates)
        
        return status
    
    def check_index_constituents(self) -> Dict[str, any]:
        """检查指数成分股"""
        dir_path = self.data_center_path / "index" / "constituents"
        
        status = {
            'name': '指数成分股',
            'path': str(dir_path),
            'exists': dir_path.exists(),
            'latest_date': None,
            'file_count': 0,
            'indices': []
        }
        
        if dir_path.exists():
            files = list(dir_path.glob("*_const.parquet"))
            status['file_count'] = len(files)
            
            if files:
                latest_dates = []
                for file_path in files:
                    try:
                        latest_date = self._get_latest_date(file_path, 'trade_date')
                        if latest_date:
                            latest_dates.append(latest_date)
                            index_code = file_path.stem.replace('_const', '')
                            status['indices'].append({
                                'code': index_code,
                                'latest_date': latest_date
                            })
                    except:
                        continue
                
                if latest_dates:
                    status['latest_date'] = max(latest_dates)
        
        return status
    
    def check_risk_free_rate(self) -> Dict[str, any]:
        """检查无风险利率"""
        file_path = self.data_center_path / "factors" / "risk_free" / "rfr_daily.parquet"
        
        status = {
            'name': '无风险利率',
            'path': str(file_path),
            'exists': file_path.exists(),
            'latest_date': None,
            'count': 0
        }
        
        if file_path.exists():
            try:
                latest_date = self._get_latest_date(file_path, 'trade_date')
                status['latest_date'] = latest_date
                
                df = pd.read_parquet(file_path, engine='pyarrow', columns=['trade_date'])
                status['count'] = len(df)
            except:
                pass
        
        return status
    
    def check_ff3_factors(self) -> Dict[str, any]:
        """检查Fama-French三因子"""
        file_path = self.data_center_path / "factors" / "fama_french_3" / "ff_3_factors_daily.parquet"
        
        status = {
            'name': 'Fama-French三因子（含金融股）',
            'path': str(file_path),
            'exists': file_path.exists(),
            'latest_date': None,
            'count': 0
        }
        
        if file_path.exists():
            try:
                latest_date = self._get_latest_date(file_path, 'trade_date')
                status['latest_date'] = latest_date
                
                df = pd.read_parquet(file_path, engine='pyarrow', columns=['trade_date'])
                status['count'] = len(df)
            except:
                pass
        
        return status
    
    def check_ff5_factors(self) -> Dict[str, any]:
        """检查Fama-French五因子"""
        file_path = self.data_center_path / "factors" / "fama_french_5" / "ff_5_factors_daily.parquet"
        
        status = {
            'name': 'Fama-French五因子（不含金融股）',
            'path': str(file_path),
            'exists': file_path.exists(),
            'latest_date': None,
            'count': 0
        }
        
        if file_path.exists():
            try:
                latest_date = self._get_latest_date(file_path, 'trade_date')
                status['latest_date'] = latest_date
                
                df = pd.read_parquet(file_path, engine='pyarrow', columns=['trade_date'])
                status['count'] = len(df)
            except:
                pass
        
        return status
    
    def check_ch3_factors(self) -> Dict[str, any]:
        """检查中国版三因子（CH-3）"""
        file_path = self.data_center_path / "factors" / "ch_3_factors" / "ch_3_factors_daily.parquet"
        
        status = {
            'name': '中国版三因子（CH-3）',
            'path': str(file_path),
            'exists': file_path.exists(),
            'latest_date': None,
            'count': 0
        }
        
        if file_path.exists():
            try:
                latest_date = self._get_latest_date(file_path, 'trade_date')
                status['latest_date'] = latest_date
                
                df = pd.read_parquet(file_path, engine='pyarrow', columns=['trade_date'])
                status['count'] = len(df)
            except:
                pass
        
        return status
    
    def check_custom_factors(self) -> Dict[str, any]:
        """检查自定义因子（UMD、LIQ）"""
        umd_path = self.data_center_path / "factors" / "custom" / "umd_daily.parquet"
        liq_path = self.data_center_path / "factors" / "custom" / "liq_daily.parquet"
        
        status = {
            'name': '自定义因子（UMD、LIQ）',
            'path': str(self.data_center_path / "factors" / "custom"),
            'exists': umd_path.exists() or liq_path.exists(),
            'latest_date': None,
            'umd_date': None,
            'liq_date': None,
            'umd_count': 0,
            'liq_count': 0
        }
        
        if umd_path.exists():
            try:
                latest_date = self._get_latest_date(umd_path, 'trade_date')
                status['umd_date'] = latest_date
                df = pd.read_parquet(umd_path, engine='pyarrow', columns=['trade_date'])
                status['umd_count'] = len(df)
            except:
                pass
        
        if liq_path.exists():
            try:
                latest_date = self._get_latest_date(liq_path, 'trade_date')
                status['liq_date'] = latest_date
                df = pd.read_parquet(liq_path, engine='pyarrow', columns=['trade_date'])
                status['liq_count'] = len(df)
            except:
                pass
        
        # 取两者中较新的日期
        dates = [d for d in [status['umd_date'], status['liq_date']] if d]
        if dates:
            status['latest_date'] = max(dates)
        
        return status
    
    def check_sw_industry(self) -> Dict[str, any]:
        """检查申万行业数据"""
        daily_path = self.data_center_path / "classification" / "industry_sw" / "sw_l1_daily.parquet"
        member_path = self.data_center_path / "classification" / "industry_sw" / "industry_sw_member.parquet"
        
        status = {
            'name': '申万行业数据',
            'path': str(self.data_center_path / "classification" / "industry_sw"),
            'exists': daily_path.exists() or member_path.exists(),
            'daily_latest_date': None,
            'member_count': 0
        }
        
        if daily_path.exists():
            try:
                latest_date = self._get_latest_date(daily_path, 'trade_date')
                status['daily_latest_date'] = latest_date
            except:
                pass
        
        if member_path.exists():
            try:
                df = pd.read_parquet(member_path, engine='pyarrow')
                status['member_count'] = len(df)
            except:
                pass
        
        return status
    
    def check_all(self) -> List[Dict[str, any]]:
        """检查所有数据状态"""
        print("\n" + "="*80)
        print("开始检查数据状态...")
        print("="*80)
        
        all_status = []
        
        # 基础数据
        all_status.append(self.check_stock_basic())
        all_status.append(self.check_stock_daily_hfq())
        all_status.append(self.check_daily_basic())
        all_status.append(self.check_fina_indicator())
        
        # 指数数据
        all_status.append(self.check_index_daily())
        all_status.append(self.check_index_constituents())
        
        # 因子数据
        all_status.append(self.check_risk_free_rate())
        all_status.append(self.check_ff3_factors())
        all_status.append(self.check_ff5_factors())
        all_status.append(self.check_ch3_factors())
        all_status.append(self.check_custom_factors())
        
        # 分类数据
        all_status.append(self.check_sw_industry())
        
        return all_status
    
    def print_status(self, status_list: List[Dict[str, any]]):
        """打印数据状态"""
        print("\n" + "="*80)
        print("📊 数据中心状态报告")
        print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        for status in status_list:
            name = status['name']
            exists = status['exists']
            
            if not exists:
                print(f"\n❌ {name}")
                print(f"   状态: 数据不存在")
                print(f"   路径: {status['path']}")
                continue
            
            print(f"\n✅ {name}")
            
            # 显示最新日期
            if 'latest_date' in status and status['latest_date']:
                latest_date = status['latest_date']
                # 格式化日期显示
                if len(latest_date) == 8:  # YYYYMMDD格式
                    formatted_date = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:8]}"
                else:
                    formatted_date = latest_date
                print(f"   最新日期: {formatted_date}")
            
            # 显示UMD和LIQ的单独日期
            if 'umd_date' in status and status['umd_date']:
                umd_date = status['umd_date']
                if len(umd_date) == 8:
                    formatted_date = f"{umd_date[:4]}-{umd_date[4:6]}-{umd_date[6:8]}"
                else:
                    formatted_date = umd_date
                print(f"   UMD最新日期: {formatted_date}")
            
            if 'liq_date' in status and status['liq_date']:
                liq_date = status['liq_date']
                if len(liq_date) == 8:
                    formatted_date = f"{liq_date[:4]}-{liq_date[4:6]}-{liq_date[6:8]}"
                else:
                    formatted_date = liq_date
                print(f"   LIQ最新日期: {formatted_date}")
            
            # 显示记录数
            if 'count' in status and status['count'] > 0:
                print(f"   记录数: {status['count']:,} 条")
            
            if 'file_count' in status and status['file_count'] > 0:
                print(f"   文件数: {status['file_count']:,} 个")
                if 'total_records' in status and status['total_records'] > 0:
                    print(f"   总记录数（估算）: {status['total_records']:,} 条")
            
            if 'member_count' in status and status['member_count'] > 0:
                print(f"   映射记录数: {status['member_count']:,} 条")
            
            # 显示指数详情
            if 'indices' in status and status['indices']:
                print(f"   指数详情:")
                for idx in status['indices']:
                    idx_date = idx['latest_date']
                    if len(idx_date) == 8:
                        formatted_date = f"{idx_date[:4]}-{idx_date[4:6]}-{idx_date[6:8]}"
                    else:
                        formatted_date = idx_date
                    print(f"     - {idx['code']}: {formatted_date}")
            
            # 显示申万行业日度数据日期
            if 'daily_latest_date' in status and status['daily_latest_date']:
                daily_date = status['daily_latest_date']
                if len(daily_date) == 8:
                    formatted_date = f"{daily_date[:4]}-{daily_date[4:6]}-{daily_date[6:8]}"
                else:
                    formatted_date = daily_date
                print(f"   行业指数最新日期: {formatted_date}")
        
        print("\n" + "="*80)
        print("✅ 数据状态检查完成")
        print("="*80)


def main():
    """主函数"""
    import sys
    
    # 获取数据中心路径
    if len(sys.argv) > 1:
        data_center_path = sys.argv[1]
    else:
        # 默认使用当前目录下的quant_data_center
        data_center_path = None
    
    # 创建检查器
    checker = DataStatusChecker(data_center_path)
    
    # 检查所有数据状态
    status_list = checker.check_all()
    
    # 打印状态
    checker.print_status(status_list)


if __name__ == "__main__":
    main()

