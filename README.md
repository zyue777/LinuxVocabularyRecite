# A股量化研究数据中心 📊

[![数据版本](https://img.shields.io/badge/数据版本-v2.0-blue.svg)](https://github.com)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://www.python.org/)
[![五因子](https://img.shields.io/badge/五因子-FF5%20v2.0-orange.svg)](https://github.com)
[![数据源](https://img.shields.io/badge/数据源-Tushare%20Pro-red.svg)](https://tushare.pro)

## 🎯 项目概述

这是一个专为A股量化研究设计的**专业级本地数据中心**，支持**Fama-French五因子模型(v2.0优化版)**、残差动量、多因子策略等高阶量化研究。

**核心特性**:
- ✅ **FF5 v2.0优化版**: 使用经营现金流(OCF)和经营性净资产(NOA)优化RMW和CMA因子
- ✅ **完整数据体系**: 5,400+只股票，15年+历史数据
- ✅ **高效存储**: Parquet列式存储，查询速度快
- ✅ **增量更新**: 智能检测，只下载新数据
- ✅ **开箱即用**: 配置简单，文档完善
- 🆕 **资金流向数据**: 个股资金流向（小单、中单、大单、特大单）
- 🆕 **市场元数据**: 创业板、科创板等市场分类快速筛选
- 🆕 **前复权转换**: 实时转换后复权为前复权，无需额外存储
- 🆕 **数据质量保障**: 完整度检查、去重、异常值检测

---

## 📁 项目结构

```
数据中心/
├── 📄 核心脚本
│   ├── download_data_manager.py     # 数据下载管理器（主程序）
│   ├── download_financial_slow.py   # 财务数据下载（慢速稳定版）
│   ├── build_ff5_factors_monthly_ttm.py  # FF5因子构建脚本 (v2.0)
│   ├── check_data_completeness.py   # 数据完整性检查工具 (v2.0)
│   ├── config.py                    # 全局配置文件
│   └── daily_update_factors.sh      # 定时更新脚本
│
├── 📚 文档
│   ├── README.md                    # 本文件
│   ├── 数据词典.md                   # 数据结构详细说明
│   ├── 五因子构建方法详解.md          # FF5构建方法（v2.0）
│   ├── FF5_v2.0_更新说明.md          # v2.0版本更新说明
│   └── 银行股与FF5因子影响分析.md     # 银行股数据分析
│
├── 🗂️ others/ (辅助工具)
│   ├── data_config_example.py       # 配置文件模板
│   ├── example_usage.py             # 使用示例代码
│   ├── validate_ff5_model.py        # FF5模型验证工具
│   ├── view_parquet.py              # Parquet文件查看器
│   └── import_shibor_from_excel.py  # SHIBOR数据导入工具
│
└── 💾 quant_data_center/ (数据存储)
    ├── stock_basic.parquet          # 股票基础信息 (5,444只)
    ├── stock/
    │   ├── daily_hfq/              # 日K线-后复权 (5,422只股票)
    │   ├── hk_daily_hfq/           # 港股行情数据 (New): 日K线（源自 Akshare）
    │   ├── daily_basic/            # 每日基础指标 (市值、PE/PB)
    │   ├── moneyflow/              # 资金流向数据 🆕 (5,400+只股票)
    │   ├── fina_indicator/         # 财务指标 (5,444只)
    │   └── financial_tables/       # 财务三大表
    ├── market_metadata/            # 市场元数据 🆕
    │   ├── chinext_stocks.parquet  # 创业板股票标记
    │   └── stock_market_map.parquet # 市场分类映射
    │       ├── income/             # 利润表 (5,444只)
    │       ├── balancesheet/       # 资产负债表 (5,444只)
    │       └── cashflow/           # 现金流量表 (5,444只) ⭐v2.0核心
    ├── factors/
    │   ├── fama_french_5/          # FF5因子 (v2.0优化版) ⭐
    │   │   └── ff_5_factors_daily.parquet
    │   └── risk_free/              # 无风险利率 (SHIBOR)
    │       └── rfr_daily.parquet
    ├── index/
    │   ├── daily/                  # 指数日K线 (沪深300/中证500/创业板)
    │   └── constituents/           # 指数成分股历史数据
    └── classification/
        └── industry_sw/            # 申万行业分类
            └── sw_l1_daily.parquet
```

---

## 🚀 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install pandas pyarrow tushare openpyxl

# Python版本要求
python >= 3.8
```

### 2. 配置Tushare Pro Token

编辑 `config.py`:

```python
TUSHARE_TOKEN = 'your_token_here'  # 替换为你的Token
```

或设置环境变量:

```bash
export TUSHARE_TOKEN=your_token_here
```

> 💡 获取Token: https://tushare.pro/register

### 3. 数据完整性检查

```bash
# 基础状态检查
python check_data_status.py

# 完整质量检查（包含完整度、去重、异常值）
python check_data_status.py --full

# 完整检查并自动修复重复数据
python check_data_status.py --full --auto-fix
```

**检查功能**:
- ✅ 数据最新日期检查
- 🆕 文件数量完整性检查
- 🆕 交易日数据断层检测
- 🆕 重复数据检测与自动去重
- 🆕 异常值检测（负数、零值等）

### 4. 生成市场元数据（可选）

```bash
python -c "from data_utils import generate_market_metadata; generate_market_metadata()"
```

这将生成创业板股票标记和市场分类映射文件，用于快速筛选不同市场的股票。

### 5. 构建Fama-French五因子 (v2.0)

```bash
python build_ff5_factors_monthly_ttm.py
```

**运行时间**: 约10-15分钟  
**输出文件**: `quant_data_center/factors/fama_french_5/ff_5_factors_daily.parquet`

---

## 💡 核心功能

### 1️⃣ Fama-French五因子模型 (v2.0优化版) ⭐

**v2.0核心优化**:
- **RMW因子**: 使用**经营现金流(OCF)**替代营业利润，更准确衡量盈利质量
- **CMA因子**: 使用**经营性净资产(NOA)**替代总资产，更精确捕捉真实投资

| 因子 | 名称 | v2.0计算方法 | 用途 |
|------|------|-------------|------|
| **MKT_RF** | 市场风险溢价 | 市场收益 - 无风险利率 | 市场整体风险 |
| **SMB** | 规模因子 | 小市值 - 大市值 | Size效应 |
| **HML** | 价值因子 | 高B/M - 低B/M | Value效应 |
| **RMW** | 盈利质量因子 | 高OCF/B - 低OCF/B | ⭐现金流质量 |
| **CMA** | 投资因子 | 保守NOA - 激进NOA | ⭐真实投资 |

**数据范围**: 2012-11-01 至今  
**调仓频率**: 月度  
**详细文档**: 见 [五因子构建方法详解.md](五因子构建方法详解.md)

---

### 2️⃣ 完整的股票数据体系

#### A. 股票日K线（后复权）
- **数量**: 5,422只股票
- **字段**: open, close, high, low, vol, amount, pct_chg
- **特点**: 自动复权，支持增量更新
- **用途**: 计算收益率、技术指标

#### B. 财务三大表（季度数据）
- **利润表** (income): 营业收入、净利润等
- **资产负债表** (balancesheet): 总资产、净资产等
- **现金流量表** (cashflow): 经营现金流 ⭐v2.0核心

**数据特点**:
- ✅ 严格使用`ann_date`（公告日期），避免未来函数
- ✅ 支持TTM（滚动12个月）计算
- ✅ 每只股票独立文件，查询高效

#### C. 财务指标 (fina_indicator)
- **数量**: 5,444只股票
- **关键字段**: 
  - `tangible_asset` - 有形资产 ⭐v2.0核心（CMA因子）
  - `roe`, `roa` - 盈利能力
  - `debt_to_assets` - 偿债能力
  - `eps`, `bps` - 每股指标

---

### 3️⃣ 指数数据

#### A. 指数日K线
- 沪深300 (399300.SZ / 000300.SH)
- 中证500 (000905.SH)
- 创业板指 (399006.SZ)

#### B. 指数成分股（历史）
- **数据量**: 20万+条记录
- **频率**: 月度调整
- **用途**: 
  - 构建股票池
  - 指数增强策略
  - 因子有效性检验

---

### 4️⃣ 资金流向数据 🆕

**绝对路径**: `quant_data_center/stock/moneyflow/{ts_code}.parquet`  
**用途**: 个股资金流向分析，包含小单、中单、大单、特大单的买入卖出金额

**关键字段**:
- `buy_sm_amount`, `sell_sm_amount`: 小单买入/卖出金额
- `buy_lg_amount`, `sell_lg_amount`: 大单买入/卖出金额
- `buy_elg_amount`, `sell_elg_amount`: 特大单买入/卖出金额
- `net_mf_amount`: 净流入金额

**使用示例**:
```python
import pandas as pd
from data_utils import get_chinext_stocks

# 读取资金流向数据
df_mf = pd.read_parquet('quant_data_center/stock/moneyflow/000001.SZ.parquet')

# 计算散户净流入（小单）
df_mf['retail_net'] = df_mf['buy_sm_amount'] - df_mf['sell_sm_amount']

# 批量读取创业板资金流向
chinext_stocks = get_chinext_stocks()
for code in chinext_stocks[:10]:
    df = pd.read_parquet(f'quant_data_center/stock/moneyflow/{code}.parquet')
```

**更新数据**:
```python
from download_data_manager import QuantDataManager
manager = QuantDataManager()
manager.update_stock_moneyflow()  # 增量更新
```

---

### 5️⃣ 市场元数据与工具函数 🆕

**市场元数据**:
- `market_metadata/chinext_stocks.parquet`: 创业板股票标记
- `market_metadata/stock_market_map.parquet`: 市场分类映射

**工具函数** (`data_utils.py`):
- `get_chinext_stocks()`: 获取创业板股票列表
- `filter_stocks_by_market()`: 按市场筛选股票
- `convert_hfq_to_qfq()`: 后复权转前复权（实时转换）
- `generate_market_metadata()`: 生成市场元数据

**使用示例**:
```python
from data_utils import get_chinext_stocks, convert_hfq_to_qfq

# 获取创业板股票
chinext_list = get_chinext_stocks()
print(f"创业板股票: {len(chinext_list)} 只")

# 前复权转换
df_qfq = convert_hfq_to_qfq('000001.SZ')
```

---

### 6️⃣ 数据完整性检查 (v2.0优化 + 新增功能)

```bash
python check_data_completeness.py
```

**检查功能**:
- ✅ 数据最新日期检查（保留原有功能）
- 🆕 文件数量完整性检查（股票基础信息 vs 日K线文件数）
- 🆕 交易日数据断层检测（采样检查）
- 🆕 重复数据检测与自动去重（基于ts_code + trade_date）
- 🆕 异常值检测（价格为0或负数、成交量为负数等）

**输出示例**:
```
【5. 财务三大表 (v2.0优化)】
  ✅ cashflow: 共 5444 个文件
     用途: RMW因子(经营现金流)
     关键字段: n_cashflow_act (84/84 条有效)
     
【6. 财务指标 (v2.0核心)】
  ✅ 目录存在，共 5444 个股票文件
     用途: CMA因子 - 有形资产(tangible_asset)
     字段检查: tangible_asset 在 9/10 个样本中有数据 (90.0%)
     ✅ tangible_asset字段覆盖率良好
```

---

## 📖 使用示例

### 示例1: 读取FF5因子并进行回归

```python
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# 数据中心路径
DATA_CENTER = '/home/zy/桌面/数据中心/quant_data_center'

# 1. 读取FF5因子 (v2.0)
df_ff5 = pd.read_parquet(f'{DATA_CENTER}/factors/fama_french_5/ff_5_factors_daily.parquet')

# 2. 读取股票收益率
df_stock = pd.read_parquet(f'{DATA_CENTER}/stock/daily_hfq/000001.SZ.parquet')
df_stock['return'] = df_stock['close'].pct_change() * 100  # 转为%

# 3. 合并数据
df = df_stock.merge(df_ff5, on='trade_date', how='inner')

# 4. FF5回归
X = df[['MKT_RF', 'SMB', 'HML', 'RMW', 'CMA']].values
y = (df['return'] - df_ff5['MKT_RF']).values  # 超额收益

model = LinearRegression()
model.fit(X, y)

print(f"Alpha (年化): {model.intercept_ * 252:.2f}%")
print(f"Beta (MKT): {model.coef_[0]:.3f}")
print(f"Beta (SMB): {model.coef_[1]:.3f}")
print(f"Beta (HML): {model.coef_[2]:.3f}")
print(f"Beta (RMW): {model.coef_[3]:.3f}")  # v2.0: 现金流质量
print(f"Beta (CMA): {model.coef_[4]:.3f}")  # v2.0: 投资因子
```

---

### 示例2: 构建股票池

```python
import pandas as pd
from datetime import datetime, timedelta

DATA_CENTER = '/home/zy/桌面/数据中心/quant_data_center'

# 1. 读取基础信息
df_basic = pd.read_parquet(f'{DATA_CENTER}/stock_basic.parquet')

# 2. 读取指数成分股
df_const = pd.read_parquet(f'{DATA_CENTER}/index/constituents/399300.SZ_const.parquet')

# 3. 构建股票池：沪深300成分股 + 非金融股 + 上市超过1年
target_date = '20240101'
one_year_ago = (datetime.strptime(target_date, '%Y%m%d') - timedelta(days=365)).strftime('%Y%m%d')

# 沪深300成分股
hs300_stocks = df_const[df_const['trade_date'] == target_date]['con_code'].tolist()

# 非金融股
non_financial = df_basic[
    ~df_basic['industry'].str.contains('银行|保险|证券|金融', na=False)
]['ts_code'].tolist()

# 上市超过1年
mature_stocks = df_basic[df_basic['list_date'] < one_year_ago]['ts_code'].tolist()

# 取交集
stock_pool = list(set(hs300_stocks) & set(non_financial) & set(mature_stocks))
print(f"股票池规模: {len(stock_pool)} 只")
```

---

### 示例3: 计算残差动量

```python
import pandas as pd
from sklearn.linear_model import LinearRegression

DATA_CENTER = '/home/zy/桌面/数据中心/quant_data_center'

def calculate_residual_momentum(ts_code, lookback=60):
    """计算残差动量因子"""
    
    # 读取股票和因子数据
    df_stock = pd.read_parquet(f'{DATA_CENTER}/stock/daily_hfq/{ts_code}.parquet')
    df_ff5 = pd.read_parquet(f'{DATA_CENTER}/factors/fama_french_5/ff_5_factors_daily.parquet')
    
    # 计算收益率
    df_stock['return'] = df_stock['close'].pct_change() * 100
    
    # 合并
    df = df_stock.merge(df_ff5, on='trade_date', how='inner').dropna()
    
    # 滚动回归
    residuals = []
    for i in range(lookback, len(df)):
        window = df.iloc[i-lookback:i]
        
        X = window[['MKT_RF', 'SMB', 'HML', 'RMW', 'CMA']].values
        y = window['return'].values
        
        model = LinearRegression()
        model.fit(X, y)
        
        # 当日残差
        residual = y[-1] - model.predict(X[-1:])
        residuals.append(residual[0])
    
    df['residual_momentum'] = [np.nan] * lookback + residuals
    return df[['trade_date', 'residual_momentum']]

# 使用示例
result = calculate_residual_momentum('000001.SZ', lookback=60)
print(result.tail())
```

---

## 🔄 数据更新

### 自动更新（推荐）

```bash
# 设置定时任务（每天18:00更新）
crontab -e

# 添加以下行
0 18 * * * cd /home/zy/桌面/数据中心 && bash daily_update_factors.sh
```

### 手动更新

```bash
# 更新所有数据
python download_data_manager.py

# 只更新FF5因子
python build_ff5_factors_monthly_ttm.py
```

### 增量更新机制

所有数据模块支持智能增量更新：

1. ✅ **自动检测**: 读取现有文件最新日期
2. ✅ **智能续传**: 只下载新数据，避免重复
3. ✅ **数据合并**: 自动合并新旧数据并去重
4. ✅ **错误处理**: 包含重试机制和断点续传

---

## 📚 文档导航

| 文档 | 说明 | 适合人群 |
|------|------|---------|
| [README.md](README.md) | 项目总览（本文件） | 所有用户 |
| [数据词典.md](数据词典.md) | 数据结构详细说明 | 开发者 |
| [五因子构建方法详解.md](五因子构建方法详解.md) | FF5构建方法（v2.0） | 研究员 |
| [FF5_v2.0_更新说明.md](FF5_v2.0_更新说明.md) | v2.0版本更新日志 | 所有用户 |
| [银行股与FF5因子影响分析.md](银行股与FF5因子影响分析.md) | 银行股数据分析 | 研究员 |

---

## 🛠️ 工具脚本 (others/)

### data_config_example.py
配置文件模板，方便在其他项目中调用数据中心：

```python
# 复制到你的项目
cp others/data_config_example.py your_project/data_config.py

# 在项目中使用
import data_config
df = pd.read_parquet(data_config.get_stock_daily_path('000001.SZ'))
```

### validate_ff5_model.py
FF5模型验证工具，检查因子有效性：

```bash
python others/validate_ff5_model.py
```

### view_parquet.py
Parquet文件快速查看器：

```bash
python others/view_parquet.py quant_data_center/stock_basic.parquet
```

### example_usage.py
完整的使用示例代码，包含各种常见场景

---

## ⚙️ 配置说明

### config.py

```python
# Tushare配置
TUSHARE_TOKEN = 'your_token_here'

# 数据中心路径
DATA_CENTER_PATH = '/home/zy/桌面/数据中心/quant_data_center'

# API限制
API_DELAY = 0.5  # 每次API调用间隔（秒）
RETRY_TIMES = 3   # 失败重试次数
```

---

## 📊 数据统计

### 当前数据规模

| 数据类型 | 数量 | 存储大小（约） |
|---------|------|--------------|
| 股票基础信息 | 5,444只 | 1 MB |
| 股票日K线 | 5,422只×3,000天 | 2 GB |
| 每日基础指标 | 1,700万条 | 500 MB |
| 财务三大表 | 5,444只×3表×60季 | 800 MB |
| 财务指标 | 5,444只×100指标 | 200 MB |
| FF5因子 | 3,094天 | < 1 MB |
| 指数数据 | 4个×3,800天 | 10 MB |
| **总计** | - | **约3.5 GB** |

### 数据覆盖范围

- **时间跨度**: 2010-01-01 至今
- **港股数据支持**: 集成 Akshare，支持下载港股通个股的复权（HFQ）日线数据。
- **财务数据**: 2006年至今（部分股票）
- **因子数据**: 2012-11-01 至今

---

## 🎯 应用场景

### 1. 多因子策略研究
- 因子有效性检验
- 因子组合优化
- 风险归因分析

### 2. 残差动量策略
- FF5回归去除系统性风险
- 提取个股特异性收益
- 构建市场中性组合

### 3. 指数增强策略
- 基于FF5因子的增强
- 行业中性化处理
- Smart Beta策略

### 4. 风险管理
- 基于FF5的风险分解
- VaR/CVaR计算
- 压力测试

---

## ⚠️ 注意事项

### 1. 银行股与tangible_asset

**现象**: 银行股的`tangible_asset`字段100%缺失  
**原因**: 银行资产主要是金融资产，而非有形资产  
**影响**: 
- ✅ 银行股仍参与MKT_RF, SMB, HML, RMW（4个因子）
- ❌ 银行股被CMA因子排除（这是合理的）

**详细分析**: 见 [银行股与FF5因子影响分析.md](银行股与FF5因子影响分析.md)

### 2. API限制

- Tushare Pro有API调用频率限制
- 建议使用高级账户（2000+积分）
- 首次下载建议分批进行

### 3. 存储空间

- 完整数据约需3.5 GB磁盘空间
- 建议预留10 GB以上空间（含增量数据）

### 4. 未来函数

- 所有财务数据严格使用`ann_date`（公告日期）
- 月度调仓在月末使用已公告数据
- 避免前视偏差

---

## 🐛 故障排除

### 常见问题

**1. Token错误**
```
解决方案: 检查config.py中的TUSHARE_TOKEN是否正确
```

**2. 数据不完整**
```bash
# 运行完整性检查
python check_data_completeness.py

# 根据提示更新缺失数据
python download_data_manager.py
```

**3. FF5构建失败**
```
可能原因:
- 财务数据不完整 → 运行check_data_completeness.py
- tangible_asset字段缺失 → 正常现象（部分股票）
- 内存不足 → 关闭其他程序或增加swap
```

**4. 指数数据检查失败**
```
注意: 000300.SH和399300.SZ是同一指数的不同代码
解决: v2.0检查工具已自动处理
```

### 调试模式

```python
# 在脚本开头添加
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 🔮 未来规划

- [ ] 支持更多数据源（Wind、东方财富）
- [ ] 增加更多因子（动量、质量、低波）
- [ ] 提供Web可视化界面
- [ ] 支持实时数据推送
- [ ] 增加机器学习特征工程模块

---

## 📞 技术支持

### 问题反馈

如遇问题，请按顺序检查：

1. ✅ 运行 `check_data_completeness.py`
2. ✅ 查看相关文档（数据词典、五因子详解）
3. ✅ 检查错误日志
4. ✅ 查阅 Tushare Pro API 文档

### 相关资源

- **Tushare Pro**: https://tushare.pro
- **Fama-French官网**: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/
- **Pandas文档**: https://pandas.pydata.org
- **PyArrow文档**: https://arrow.apache.org/docs/python/

---

## 📜 版本历史

| 版本 | 日期 | 主要更新 |
|------|------|---------|
| **v2.1** | 2025-11-10 | 🆕 新增资金流向数据（moneyflow）<br>🆕 新增市场元数据（创业板标记等）<br>🆕 前复权转换功能<br>🆕 数据质量检查升级（完整度、去重、异常值） |
| **v2.0** | 2025-10-28 | ⭐ FF5因子优化（OCF+NOA）<br>⭐ 新增数据完整性检查v2.0<br>⭐ 完善文档体系 |
| v1.1 | 2025-10-27 | 新增数据词典<br>优化数据结构 |
| v1.0 | 2025-10-26 | 初始版本<br>基础数据下载和FF5构建 |

---

## 📄 许可证

本项目仅供学习和研究使用。

**免责声明**: 
- 本数据中心仅供学术研究和技术学习
- 投资有风险，使用本工具产生的投资决策后果自负
- 数据来源于Tushare Pro，使用需遵守其服务条款

---

## 🙏 致谢

- **Tushare Pro** - 提供优质的金融数据API
- **Fama & French** - 开创性的因子模型研究
- **Pandas & PyArrow** - 强大的数据处理工具

---

<div align="center">

**祝您量化研究顺利！** 🚀📈

*Built with ❤️ for Quantitative Research*

**最后更新**: 2025-11-10  
**数据版本**: v2.1  
**项目状态**: ✅ 生产就绪

</div>
