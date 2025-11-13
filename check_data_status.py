#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据状态查看工具
一键查看数据中心各项数据的最新日期
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# 尝试导入Tushare用于获取交易日历
try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    print("警告: Tushare未安装，数据断层检查将使用简化逻辑")


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
    
    def check_stock_moneyflow(self) -> Dict[str, any]:
        """检查股票资金流向数据"""
        dir_path = self.data_center_path / "stock" / "moneyflow"
        
        status = {
            'name': '股票资金流向',
            'path': str(dir_path),
            'exists': dir_path.exists(),
            'latest_date': None,
            'file_count': 0,
            'total_records': 0
        }
        
        if dir_path.exists():
            try:
                files = list(dir_path.glob("*.parquet"))
                status['file_count'] = len(files)
                
                if files:
                    latest_date = self._get_latest_date_from_dir(dir_path, 'trade_date')
                    status['latest_date'] = latest_date
                    
                    # 统计总记录数（采样）
                    if len(files) <= 100:
                        total = 0
                        for f in files:
                            try:
                                df = pd.read_parquet(f, engine='pyarrow')
                                total += len(df)
                            except:
                                pass
                        status['total_records'] = total
                    else:
                        sample_files = files[:100]
                        sample_total = 0
                        for f in sample_files:
                            try:
                                df = pd.read_parquet(f, engine='pyarrow')
                                sample_total += len(df)
                            except:
                                pass
                        status['total_records'] = int(sample_total * len(files) / 100)
            except Exception as e:
                # 如果获取文件列表失败，记录错误但继续
                status['error'] = str(e)
        
        return status
    
    def check_qfq_conversion(self) -> Dict[str, any]:
        """检查前复权转换功能"""
        status = {
            'name': '前复权转换功能',
            'path': 'data_utils.convert_hfq_to_qfq()',
            'available': False,
            'test_result': None
        }
        
        try:
            # 尝试导入data_utils模块
            import sys
            sys.path.insert(0, str(self.data_center_path.parent))
            from data_utils import convert_hfq_to_qfq
            
            status['available'] = True
            
            # 测试转换功能（使用一个存在的股票）
            daily_hfq_dir = self.data_center_path / "stock" / "daily_hfq"
            if daily_hfq_dir.exists():
                hfq_files = list(daily_hfq_dir.glob("*.parquet"))
                if hfq_files:
                    test_file = hfq_files[0]
                    test_code = test_file.stem
                    try:
                        result = convert_hfq_to_qfq(test_code, str(self.data_center_path))
                        if result is not None and not result.empty:
                            status['test_result'] = f'✅ 测试通过 ({test_code})'
                        else:
                            status['test_result'] = f'⚠️  测试返回空数据 ({test_code})'
                    except Exception as e:
                        status['test_result'] = f'❌ 测试失败: {str(e)[:50]}'
        except ImportError:
            status['test_result'] = '❌ data_utils模块不存在'
        except Exception as e:
            status['test_result'] = f'❌ 检查失败: {str(e)[:50]}'
        
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
        all_status.append(self.check_stock_moneyflow())  # 🆕 新增
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
        
        # 前复权转换功能 🆕 新增
        all_status.append(self.check_qfq_conversion())
        
        return all_status
    
    def print_status(self, status_list: List[Dict[str, any]]):
        """打印数据状态"""
        print("\n" + "="*80)
        print("📊 数据中心状态报告")
        print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        for status in status_list:
            name = status['name']
            
            # 处理不同的状态字典结构
            exists = status.get('exists', True)  # 默认为True，兼容没有exists键的情况
            available = status.get('available', None)  # 前复权转换功能使用available
            
            # 如果使用available字段，则根据available判断
            if available is not None:
                if not available:
                    print(f"\n❌ {name}")
                    print(f"   状态: 功能不可用")
                    if 'test_result' in status:
                        print(f"   {status['test_result']}")
                    continue
                else:
                    print(f"\n✅ {name}")
            elif not exists:
                print(f"\n❌ {name}")
                print(f"   状态: 数据不存在")
                if 'path' in status:
                    print(f"   路径: {status['path']}")
                continue
            else:
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
            
            # 显示前复权转换功能详情
            if 'available' in status:
                if status.get('available'):
                    print(f"   功能状态: ✅ 可用")
                    if 'test_result' in status and status['test_result']:
                        print(f"   {status['test_result']}")
                else:
                    print(f"   功能状态: ❌ 不可用")
                    if 'test_result' in status and status['test_result']:
                        print(f"   {status['test_result']}")
        
        print("\n" + "="*80)
        print("✅ 数据状态检查完成")
        print("="*80)
    
    def check_data_completeness(self) -> Dict[str, any]:
        """
        数据完整度检查
        检查交易日数据断层、文件是否齐全等
        """
        print("\n" + "="*80)
        print("🔍 数据完整度检查")
        print("="*80)
        
        completeness_report = {
            'stock_basic_completeness': None,
            'daily_hfq_completeness': None,
            'moneyflow_completeness': None,
            'file_count_issues': [],
            'date_gap_issues': []
        }
        
        # 1. 检查股票基础信息与日K线文件数量是否匹配
        print("\n【1. 文件数量完整性检查】")
        stock_basic_file = self.data_center_path / "stock_basic.parquet"
        if stock_basic_file.exists():
            df_basic = pd.read_parquet(stock_basic_file, engine='pyarrow')
            basic_count = len(df_basic)
            print(f"  股票基础信息: {basic_count} 只")
            
            # 检查日K线文件数量
            daily_hfq_dir = self.data_center_path / "stock" / "daily_hfq"
            if daily_hfq_dir.exists():
                hfq_files = list(daily_hfq_dir.glob("*.parquet"))
                hfq_count = len(hfq_files)
                print(f"  日K线文件数: {hfq_count} 个")
                
                if hfq_count < basic_count * 0.9:  # 允许10%的缺失
                    completeness_report['file_count_issues'].append({
                        'type': 'daily_hfq',
                        'expected': basic_count,
                        'actual': hfq_count,
                        'missing': basic_count - hfq_count
                    })
                    print(f"  ⚠️  警告: 日K线文件缺失 {basic_count - hfq_count} 个")
                else:
                    print(f"  ✅ 日K线文件数量正常")
            
            # 检查资金流向文件数量
            moneyflow_dir = self.data_center_path / "stock" / "moneyflow"
            if moneyflow_dir.exists():
                try:
                    moneyflow_files = list(moneyflow_dir.glob("*.parquet"))
                    moneyflow_count = len(moneyflow_files)
                    print(f"  资金流向文件数: {moneyflow_count} 个")
                    
                    if moneyflow_count == 0:
                        print(f"  ⚠️  资金流向数据目录存在但无文件")
                    elif moneyflow_count < basic_count * 0.5:
                        missing_stocks = []
                        # 找出缺失资金流向数据的股票
                        existing_moneyflow_stocks = {f.stem for f in moneyflow_files}
                        missing_stocks = [code for code in df_basic['ts_code'] if code not in existing_moneyflow_stocks]
                        
                        completeness_report['file_count_issues'].append({
                            'type': 'moneyflow',
                            'expected': basic_count,
                            'actual': moneyflow_count,
                            'missing': basic_count - moneyflow_count,
                            'missing_stocks': missing_stocks[:50]  # 只保存前50个，避免日志过大
                        })
                        print(f"  ⚠️  警告: 资金流向文件缺失 {basic_count - moneyflow_count} 个")
                        if missing_stocks:
                            print(f"    缺失示例（前10个）: {missing_stocks[:10]}")
                    else:
                        print(f"  ✅ 资金流向文件数量正常 ({moneyflow_count}/{basic_count})")
                except Exception as e:
                    print(f"  ❌ 检查资金流向文件时出错: {e}")
            else:
                print(f"  ⚠️  资金流向数据目录不存在")
            
        
        # 2. 检查交易日数据断层（采样检查，使用A股交易日历）
        print("\n【2. 交易日数据断层检查（采样，基于A股交易日历）】")
        daily_hfq_dir = self.data_center_path / "stock" / "daily_hfq"
        if daily_hfq_dir.exists():
            hfq_files = list(daily_hfq_dir.glob("*.parquet"))
            if hfq_files:
                # 采样检查前10个文件
                sample_files = hfq_files[:10]
                gap_issues = []
                
                # 获取A股交易日历（用于判断真正的数据缺失）
                trade_calendar = None
                if TUSHARE_AVAILABLE:
                    try:
                        # 尝试从config获取token
                        try:
                            import config
                            if hasattr(config, 'TUSHARE_TOKEN'):
                                ts.set_token(config.TUSHARE_TOKEN)
                        except:
                            pass
                        
                        pro = ts.pro_api()
                        # 获取最近5年的交易日历
                        end_date = datetime.now().strftime('%Y%m%d')
                        start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y%m%d')
                        cal_df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')
                        if cal_df is not None and not cal_df.empty:
                            trade_calendar = set(cal_df['cal_date'].astype(str).tolist())
                            print(f"  已加载A股交易日历: {len(trade_calendar)} 个交易日")
                    except Exception as e:
                        print(f"  ⚠️  无法获取交易日历，将使用简化检查: {e}")
                
                for file_path in sample_files:
                    try:
                        df = pd.read_parquet(file_path, engine='pyarrow', columns=['trade_date'])
                        if df.empty:
                            continue
                        
                        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                        df = df.sort_values('trade_date')
                        df['trade_date_str'] = df['trade_date'].dt.strftime('%Y%m%d')
                        
                        # 获取数据日期范围
                        min_date = df['trade_date'].min()
                        max_date = df['trade_date'].max()
                        
                        if trade_calendar:
                            # 使用交易日历：找出该日期范围内所有交易日，检查哪些缺失
                            date_range = pd.date_range(min_date, max_date, freq='D')
                            date_range_str = [d.strftime('%Y%m%d') for d in date_range]
                            
                            # 找出交易日中缺失的日期
                            existing_dates = set(df['trade_date_str'].tolist())
                            missing_trade_dates = [d for d in date_range_str if d in trade_calendar and d not in existing_dates]
                            
                            if len(missing_trade_dates) > 0:
                                # 计算连续缺失的区间
                                missing_dates_sorted = sorted(missing_trade_dates)
                                gap_count = 0
                                max_gap_days = 0
                                
                                i = 0
                                while i < len(missing_dates_sorted):
                                    gap_start = missing_dates_sorted[i]
                                    gap_end = gap_start
                                    j = i + 1
                                    while j < len(missing_dates_sorted):
                                        # 检查是否连续
                                        prev_date = pd.to_datetime(missing_dates_sorted[j-1], format='%Y%m%d')
                                        curr_date = pd.to_datetime(missing_dates_sorted[j], format='%Y%m%d')
                                        if (curr_date - prev_date).days == 1:
                                            gap_end = missing_dates_sorted[j]
                                            j += 1
                                        else:
                                            break
                                    
                                    gap_count += 1
                                    gap_start_dt = pd.to_datetime(gap_start, format='%Y%m%d')
                                    gap_end_dt = pd.to_datetime(gap_end, format='%Y%m%d')
                                    gap_days = (gap_end_dt - gap_start_dt).days + 1
                                    max_gap_days = max(max_gap_days, gap_days)
                                    
                                    i = j
                                
                                if gap_count > 0:
                                    gap_issues.append({
                                        'file': file_path.name,
                                        'gaps': gap_count,
                                        'max_gap_days': max_gap_days,
                                        'missing_count': len(missing_trade_dates),
                                        'missing_dates_sample': missing_trade_dates[:5]  # 保存前5个缺失日期作为示例
                                    })
                        else:
                            # 简化检查：计算日期差，但只标记大于10天的间隔（可能是数据断层）
                            date_diffs = df['trade_date'].diff().dt.days
                            large_gaps = date_diffs[date_diffs > 10]  # 大于10天可能是数据断层
                            
                            if len(large_gaps) > 0:
                                gap_issues.append({
                                    'file': file_path.name,
                                    'gaps': len(large_gaps),
                                    'max_gap_days': int(large_gaps.max()),
                                    'note': '未使用交易日历，结果可能包含节假日'
                                })
                    except Exception as e:
                        continue
                
                if gap_issues:
                    print(f"  ⚠️  发现 {len(gap_issues)} 个文件存在数据断层")
                    for issue in gap_issues[:5]:  # 只显示前5个
                        if 'missing_count' in issue:
                            print(f"    - {issue['file']}: {issue['missing_count']} 个交易日缺失，{issue['gaps']} 处断层，最大间隔 {issue['max_gap_days']} 天")
                            if issue.get('missing_dates_sample'):
                                print(f"      缺失日期示例: {', '.join(issue['missing_dates_sample'])}")
                        else:
                            print(f"    - {issue['file']}: {issue['gaps']} 处断层，最大间隔 {issue['max_gap_days']} 天 {issue.get('note', '')}")
                    completeness_report['date_gap_issues'] = gap_issues
                else:
                    print(f"  ✅ 采样检查未发现数据断层（基于A股交易日历）")
        
        return completeness_report
    
    def check_duplicates_and_deduplicate(self, auto_fix: bool = False) -> Dict[str, any]:
        """
        数据去重检查与清洗
        
        Args:
            auto_fix: 是否自动修复（删除重复数据）
        """
        print("\n" + "="*80)
        print("🧹 数据去重检查")
        print("="*80)
        
        dedup_report = {
            'daily_hfq_duplicates': [],
            'moneyflow_duplicates': [],
            'daily_basic_duplicates': None,
            'fixed_count': 0
        }
        
        # 1. 检查日K线数据重复
        print("\n【1. 日K线数据去重检查】")
        daily_hfq_dir = self.data_center_path / "stock" / "daily_hfq"
        if daily_hfq_dir.exists():
            hfq_files = list(daily_hfq_dir.glob("*.parquet"))
            sample_files = hfq_files[:20]  # 采样检查前20个
            
            for file_path in sample_files:
                try:
                    df = pd.read_parquet(file_path, engine='pyarrow')
                    if df.empty:
                        continue
                    
                    # 检查基于ts_code + trade_date的重复
                    initial_count = len(df)
                    duplicates = df.duplicated(subset=['ts_code', 'trade_date'], keep=False)
                    dup_count = duplicates.sum()
                    
                    if dup_count > 0:
                        dedup_report['daily_hfq_duplicates'].append({
                            'file': file_path.name,
                            'total': initial_count,
                            'duplicates': dup_count
                        })
                        
                        if auto_fix:
                            # 自动去重
                            df_clean = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last').sort_values('trade_date')
                            df_clean.to_parquet(file_path, engine='pyarrow', index=False)
                            dedup_report['fixed_count'] += 1
                            print(f"  ✅ {file_path.name}: 已去重 {dup_count} 条重复记录")
                        else:
                            print(f"  ⚠️  {file_path.name}: 发现 {dup_count} 条重复记录")
                except Exception as e:
                    continue
            
            if not dedup_report['daily_hfq_duplicates']:
                print(f"  ✅ 采样检查未发现重复数据")
        
        # 2. 检查资金流向数据重复
        print("\n【2. 资金流向数据去重检查】")
        moneyflow_dir = self.data_center_path / "stock" / "moneyflow"
        if moneyflow_dir.exists():
            moneyflow_files = list(moneyflow_dir.glob("*.parquet"))
            if moneyflow_files:
                sample_files = moneyflow_files[:20]  # 采样检查前20个
                
                for file_path in sample_files:
                    try:
                        df = pd.read_parquet(file_path, engine='pyarrow')
                        if df.empty:
                            continue
                        
                        initial_count = len(df)
                        duplicates = df.duplicated(subset=['ts_code', 'trade_date'], keep=False)
                        dup_count = duplicates.sum()
                        
                        if dup_count > 0:
                            dedup_report['moneyflow_duplicates'].append({
                                'file': file_path.name,
                                'total': initial_count,
                                'duplicates': dup_count
                            })
                            
                            if auto_fix:
                                df_clean = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last').sort_values('trade_date')
                                df_clean.to_parquet(file_path, engine='pyarrow', index=False)
                                dedup_report['fixed_count'] += 1
                                print(f"  ✅ {file_path.name}: 已去重 {dup_count} 条重复记录")
                            else:
                                print(f"  ⚠️  {file_path.name}: 发现 {dup_count} 条重复记录")
                    except Exception as e:
                        continue
                
                if not dedup_report['moneyflow_duplicates']:
                    print(f"  ✅ 采样检查未发现重复数据")
        
        # 3. 检查daily_basic_all.parquet重复
        print("\n【3. 每日基础指标数据去重检查】")
        daily_basic_file = self.data_center_path / "stock" / "daily_basic" / "daily_basic_all.parquet"
        if daily_basic_file.exists():
            try:
                df = pd.read_parquet(daily_basic_file, engine='pyarrow', columns=['ts_code', 'trade_date'])
                initial_count = len(df)
                duplicates = df.duplicated(subset=['ts_code', 'trade_date'], keep=False)
                dup_count = duplicates.sum()
                
                if dup_count > 0:
                    dedup_report['daily_basic_duplicates'] = {
                        'total': initial_count,
                        'duplicates': dup_count
                    }
                    
                    if auto_fix:
                        df_full = pd.read_parquet(daily_basic_file, engine='pyarrow')
                        df_clean = df_full.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last').sort_values(['trade_date', 'ts_code'])
                        df_clean.to_parquet(daily_basic_file, engine='pyarrow', index=False)
                        dedup_report['fixed_count'] += 1
                        print(f"  ✅ 已去重 {dup_count} 条重复记录")
                    else:
                        print(f"  ⚠️  发现 {dup_count} 条重复记录")
                else:
                    print(f"  ✅ 未发现重复数据")
            except Exception as e:
                print(f"  ❌ 检查失败: {e}")
        
        if auto_fix and dedup_report['fixed_count'] > 0:
            print(f"\n✅ 共修复 {dedup_report['fixed_count']} 个文件的重复数据")
        elif not auto_fix:
            # 统计重复数据总数
            total_duplicates = 0
            duplicate_files = []
            
            if dedup_report['daily_hfq_duplicates']:
                for item in dedup_report['daily_hfq_duplicates']:
                    total_duplicates += item['duplicates']
                    duplicate_files.append(f"日K线: {item['file']} ({item['duplicates']} 条重复)")
            
            if dedup_report['moneyflow_duplicates']:
                for item in dedup_report['moneyflow_duplicates']:
                    total_duplicates += item['duplicates']
                    duplicate_files.append(f"资金流向: {item['file']} ({item['duplicates']} 条重复)")
            
            if dedup_report['daily_basic_duplicates']:
                total_duplicates += dedup_report['daily_basic_duplicates']['duplicates']
                duplicate_files.append(f"每日基础指标: {dedup_report['daily_basic_duplicates']['duplicates']} 条重复")
            
            if total_duplicates > 0:
                print(f"\n⚠️  发现 {total_duplicates} 条重复数据，涉及 {len(duplicate_files)} 个文件/表")
                print(f"   重复详情:")
                for item in duplicate_files[:10]:  # 只显示前10个
                    print(f"     - {item}")
                if len(duplicate_files) > 10:
                    print(f"     ... 还有 {len(duplicate_files) - 10} 个文件存在重复")
                print(f"\n💡 提示: 使用 --auto-fix 参数可自动修复重复数据")
                print(f"   命令: python check_data_status.py --dedup --auto-fix")
        
        return dedup_report
    
    def check_anomalies(self) -> Dict[str, any]:
        """
        异常值检测
        检查负数、零值等明显异常
        """
        print("\n" + "="*80)
        print("🔎 异常值检测")
        print("="*80)
        
        anomaly_report = {
            'negative_values': [],
            'zero_prices': [],
            'negative_volumes': [],
            'negative_amounts': []
        }
        
        # 1. 检查日K线异常值
        print("\n【1. 日K线异常值检查（采样）】")
        daily_hfq_dir = self.data_center_path / "stock" / "daily_hfq"
        if daily_hfq_dir.exists():
            hfq_files = list(daily_hfq_dir.glob("*.parquet"))
            sample_files = hfq_files[:20]  # 采样检查前20个
            
            for file_path in sample_files:
                try:
                    df = pd.read_parquet(file_path, engine='pyarrow')
                    if df.empty:
                        continue
                    
                    # 检查价格为0或负数
                    price_cols = ['open', 'high', 'low', 'close']
                    for col in price_cols:
                        if col in df.columns:
                            zero_prices = (df[col] == 0).sum()
                            negative_prices = (df[col] < 0).sum()
                            
                            if zero_prices > 0 or negative_prices > 0:
                                anomaly_report['zero_prices'].append({
                                    'file': file_path.name,
                                    'column': col,
                                    'zero_count': int(zero_prices),
                                    'negative_count': int(negative_prices)
                                })
                    
                    # 检查成交量为负数
                    if 'vol' in df.columns:
                        negative_vol = (df['vol'] < 0).sum()
                        if negative_vol > 0:
                            anomaly_report['negative_volumes'].append({
                                'file': file_path.name,
                                'count': int(negative_vol)
                            })
                    
                    # 检查成交额为负数
                    if 'amount' in df.columns:
                        negative_amt = (df['amount'] < 0).sum()
                        if negative_amt > 0:
                            anomaly_report['negative_amounts'].append({
                                'file': file_path.name,
                                'count': int(negative_amt)
                            })
                except Exception as e:
                    continue
            
            # 汇总报告
            if anomaly_report['zero_prices']:
                print(f"  ⚠️  发现 {len(anomaly_report['zero_prices'])} 个文件存在价格异常")
            if anomaly_report['negative_volumes']:
                print(f"  ⚠️  发现 {len(anomaly_report['negative_volumes'])} 个文件存在负成交量")
            if anomaly_report['negative_amounts']:
                print(f"  ⚠️  发现 {len(anomaly_report['negative_amounts'])} 个文件存在负成交额")
            
            if not any([anomaly_report['zero_prices'], anomaly_report['negative_volumes'], anomaly_report['negative_amounts']]):
                print(f"  ✅ 采样检查未发现明显异常值")
        
        # 2. 检查资金流向异常值
        print("\n【2. 资金流向异常值检查（采样）】")
        moneyflow_dir = self.data_center_path / "stock" / "moneyflow"
        if moneyflow_dir.exists():
            moneyflow_files = list(moneyflow_dir.glob("*.parquet"))
            if moneyflow_files:
                sample_files = moneyflow_files[:20]
                
                for file_path in sample_files:
                    try:
                        df = pd.read_parquet(file_path, engine='pyarrow')
                        if df.empty:
                            continue
                        
                        # 检查金额字段为负数（某些字段可能允许负数，但需要检查）
                        amount_cols = ['buy_sm_amount', 'sell_sm_amount', 'buy_md_amount', 
                                      'sell_md_amount', 'buy_lg_amount', 'sell_lg_amount',
                                      'buy_elg_amount', 'sell_elg_amount']
                        
                        for col in amount_cols:
                            if col in df.columns:
                                negative_count = (df[col] < 0).sum()
                                if negative_count > 0:
                                    # 资金流向的买入/卖出金额理论上不应该为负，但需要根据实际情况判断
                                    pass
                    except Exception as e:
                        continue
        
        return anomaly_report
    
    def run_full_check(self, auto_fix_duplicates: bool = False):
        """
        运行完整的数据质量检查
        
        Args:
            auto_fix_duplicates: 是否自动修复重复数据
        """
        print("\n" + "="*80)
        print("🔍 数据中心完整质量检查")
        print("="*80)
        
        # 1. 基础状态检查（保留原有功能）
        status_list = self.check_all()
        self.print_status(status_list)
        
        # 2. 数据完整度检查
        completeness = self.check_data_completeness()
        
        # 3. 去重检查
        dedup = self.check_duplicates_and_deduplicate(auto_fix=auto_fix_duplicates)
        
        # 4. 异常值检测
        anomalies = self.check_anomalies()
        
        # 汇总报告
        print("\n" + "="*80)
        print("📊 数据质量检查汇总")
        print("="*80)
        
        # 详细统计
        file_issues_count = len(completeness['file_count_issues'])
        gap_issues_count = len(completeness['date_gap_issues'])
        
        dup_issues_count = len(dedup['daily_hfq_duplicates']) + len(dedup['moneyflow_duplicates']) + (1 if dedup['daily_basic_duplicates'] else 0)
        anomaly_issues_count = len(anomalies['zero_prices']) + len(anomalies['negative_volumes']) + len(anomalies['negative_amounts'])
        
        print(f"\n【问题统计】")
        print(f"  文件数量问题: {file_issues_count} 项")
        print(f"  数据断层问题: {gap_issues_count} 项")
        print(f"  重复数据问题: {dup_issues_count} 项")
        print(f"  异常值问题: {anomaly_issues_count} 项")
        
        # 详细日志：文件数量问题
        if file_issues_count > 0:
            print(f"\n【文件数量问题详情】")
            for issue in completeness['file_count_issues']:
                print(f"  {issue['type']}:")
                print(f"    期望: {issue['expected']} 个文件")
                print(f"    实际: {issue['actual']} 个文件")
                print(f"    缺失: {issue['missing']} 个文件")
                if 'missing_stocks' in issue and issue['missing_stocks']:
                    print(f"    缺失股票示例: {', '.join(issue['missing_stocks'][:10])}")
                    if len(issue['missing_stocks']) > 10:
                        print(f"    ... 还有 {len(issue['missing_stocks']) - 10} 只股票缺失数据")
        
        # 详细日志：数据断层问题
        if gap_issues_count > 0:
            print(f"\n【数据断层问题详情】")
            for issue in completeness['date_gap_issues'][:10]:  # 只显示前10个
                if 'missing_count' in issue:
                    print(f"  {issue['file']}: {issue['missing_count']} 个交易日缺失，{issue['gaps']} 处断层，最大间隔 {issue['max_gap_days']} 天")
                    if issue.get('missing_dates_sample'):
                        print(f"    缺失日期示例: {', '.join(issue['missing_dates_sample'])}")
                else:
                    print(f"  {issue['file']}: {issue['gaps']} 处断层，最大间隔 {issue['max_gap_days']} 天 {issue.get('note', '')}")
            if len(completeness['date_gap_issues']) > 10:
                print(f"  ... 还有 {len(completeness['date_gap_issues']) - 10} 个文件存在数据断层")
        
        # 详细日志：重复数据问题
        if dup_issues_count > 0:
            print(f"\n【重复数据问题详情】")
            if dedup['daily_hfq_duplicates']:
                print(f"  日K线数据重复:")
                for item in dedup['daily_hfq_duplicates'][:10]:
                    print(f"    - {item['file']}: {item['duplicates']} 条重复 (总计 {item['total']} 条)")
                if len(dedup['daily_hfq_duplicates']) > 10:
                    print(f"    ... 还有 {len(dedup['daily_hfq_duplicates']) - 10} 个文件存在重复")
            
            if dedup['moneyflow_duplicates']:
                print(f"  资金流向数据重复:")
                for item in dedup['moneyflow_duplicates'][:10]:
                    print(f"    - {item['file']}: {item['duplicates']} 条重复 (总计 {item['total']} 条)")
                if len(dedup['moneyflow_duplicates']) > 10:
                    print(f"    ... 还有 {len(dedup['moneyflow_duplicates']) - 10} 个文件存在重复")
            
            if dedup['daily_basic_duplicates']:
                print(f"  每日基础指标重复: {dedup['daily_basic_duplicates']['duplicates']} 条 (总计 {dedup['daily_basic_duplicates']['total']} 条)")
            
            if not auto_fix_duplicates:
                print(f"\n  💡 提示: 使用 --auto-fix 参数可自动修复重复数据")
                print(f"     命令: python check_data_status.py --full --auto-fix")
        
        # 详细日志：异常值问题
        if anomaly_issues_count > 0:
            print(f"\n【异常值问题详情】")
            if anomalies['zero_prices']:
                print(f"  价格异常:")
                for item in anomalies['zero_prices'][:10]:
                    print(f"    - {item['file']} ({item['column']}): {item['zero_count']} 个零值, {item['negative_count']} 个负值")
            if anomalies['negative_volumes']:
                print(f"  成交量异常:")
                for item in anomalies['negative_volumes'][:10]:
                    print(f"    - {item['file']}: {item['count']} 个负值")
            if anomalies['negative_amounts']:
                print(f"  成交额异常:")
                for item in anomalies['negative_amounts'][:10]:
                    print(f"    - {item['file']}: {item['count']} 个负值")
        
        if auto_fix_duplicates and dedup['fixed_count'] > 0:
            print(f"\n✅ 已自动修复 {dedup['fixed_count']} 个文件的重复数据")
        
        # 总体评估
        total_issues = file_issues_count + gap_issues_count + dup_issues_count + anomaly_issues_count
        if total_issues == 0:
            print(f"\n🎉 数据质量检查通过！未发现任何问题")
        else:
            print(f"\n⚠️  共发现 {total_issues} 类问题，建议及时处理")
        
        print("\n" + "="*80)


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
    
    # 检查命令行参数
    if len(sys.argv) > 2:
        mode = sys.argv[2]
        if mode == '--full':
            # 完整检查（包含完整度、去重、异常值）
            auto_fix = '--auto-fix' in sys.argv
            checker.run_full_check(auto_fix_duplicates=auto_fix)
        elif mode == '--simple':
            # 简单检查（只检查基础状态）
            status_list = checker.check_all()
            checker.print_status(status_list)
        elif mode == '--completeness':
            # 只检查完整度
            checker.check_data_completeness()
        elif mode == '--dedup':
            # 只检查去重
            auto_fix = '--auto-fix' in sys.argv
            checker.check_duplicates_and_deduplicate(auto_fix=auto_fix)
        elif mode == '--anomalies':
            # 只检查异常值
            checker.check_anomalies()
        else:
            print("未知模式，使用默认完整检查")
            auto_fix = '--auto-fix' in sys.argv
            checker.run_full_check(auto_fix_duplicates=auto_fix)
    else:
        # 默认：运行完整质量检查
        print("\n" + "="*80)
        print("🔍 运行完整数据质量检查（默认模式）")
        print("="*80)
        print("提示: 使用 --simple 参数可只检查基础状态")
        print("="*80)
        
        # 询问是否自动修复重复数据
        auto_fix = False
        if '--auto-fix' in sys.argv:
            auto_fix = True
        elif '--ask-fix' in sys.argv:
            # 检查是否有重复数据，如果有则询问
            dedup_preview = checker.check_duplicates_and_deduplicate(auto_fix=False)
            total_dup_files = len(dedup_preview['daily_hfq_duplicates']) + len(dedup_preview['moneyflow_duplicates']) + (1 if dedup_preview['daily_basic_duplicates'] else 0)
            if total_dup_files > 0:
                response = input(f"\n发现 {total_dup_files} 个文件/表存在重复数据，是否自动修复? (y/n): ").strip().lower()
                auto_fix = (response == 'y')
        
        # 运行完整检查
        checker.run_full_check(auto_fix_duplicates=auto_fix)
        
        print("\n💡 其他检查模式:")
        print("  python check_data_status.py [路径] --simple        # 只检查基础状态（最新日期）")
        print("  python check_data_status.py [路径] --full --auto-fix  # 完整检查并自动修复重复数据")
        print("  python check_data_status.py [路径] --completeness    # 只检查完整度")
        print("  python check_data_status.py [路径] --dedup           # 只检查去重")
        print("  python check_data_status.py [路径] --anomalies       # 只检查异常值")


if __name__ == "__main__":
    main()

