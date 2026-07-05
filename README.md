# A股量化研究数据中心 📊

[![数据版本](https://img.shields.io/badge/数据版本-v1.11-blue.svg)](https://github.com)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://www.python.org/)
[![数据源](https://img.shields.io/badge/数据源-Tushare%20Pro-red.svg)](https://tushare.pro)

## ⚡ 2026-07-04 重要变更（日线提速18倍 + bug修复）

- **日线更新改用 `fast_daily_update.py`（按交易日拉全市场）**：旧逐只法约80分钟，
  新法补一天≈10秒、补3个月≈5分钟。口径经 Tushare 官方 pro_bar 独立对照一致（误差≈0）。
  `daily_run.py` / `download_data_manager.py` 选项1、选项2 均已改走此脚本。
- **修复停更bug**：旧 `update_stock_daily_hfq` 的"抽样前100只、95%最新就整体跳过"
  短路逻辑，导致92%股票停在2026-03-26。已用快脚本旁路，旧方法保留备查不再调用。
  本次已把全市场补齐到最新交易日。
- **moneyflow 停更**：无下游读取的死数据（1.5G）且逐只拉约41分钟，已从每日流程移除，
  需要时手动跑菜单选项恢复。
- 备份：改造前 daily_hfq/qfq 已备份于 `quant_data_center/stock/_bak_daily_*_20260704/`
  （约1.6G，确认无误后可删）。

### 2026-07-05（T18 数据中心收尾）
- **新股补齐**：全市场缺失的 48 只次新股已全量补 daily_hfq/qfq（口径同 fast_daily，
  对官方 pro_bar 一致），fast_daily 重跑 `无本地文件=0`。
- **死数据冷冻归档**：`stock/moneyflow`(1.5G) + `stock/cyq_perf`(196M) 已 mv 至
  `quant_data_center/_冷冻归档/stock/`，清单+恢复法见 `_冷冻归档/清单.md`；解冻= mv 回原路径。
  注：QuantDataManager 初始化按标准结构重建空占位目录，属正常（0 文件、不重下载）。
  其余 4 类候选（margin_detail/stock_hk/hsgt/derivatives）当时保留原位待评估——
  **已于 T15（下方 2026-07-05 节）逐类实测后全部归档**。
- **旧备份**：`_bak_daily_*_20260704`（1.6G）validate 已通过，按用户决定本次暂保留不删。

### 2026-07-05（T15 共用数据 canonical 统一）
- **4 类死数据归档**：T18 遗留的 margin_detail(339M)/stock_hk(303M)/hsgt(24K)/derivatives(2.7M)，
  逐类 command grep 实测消费方后用户拍板全部冷冻归档（均不设 canonical，非共用序列），
  已 mv 至 `_冷冻归档/{market,stock_hk}/`，共释放 645M；明细见 `_冷冻归档/清单.md` 第二批节。
- **根目录可配**：config.py 的 DATA_CENTER_PATH 改读 `QUANT_DATA_CENTER` 环境变量（默认=现路径、零破坏）；
  国债维护脚本 update_bond_yield.py 同步改从 config 取路径。
- **canonical 注册表**：5 条共用序列的唯一源+消费方+切换状态登记于 `_公共/规范/canonical注册表.md`；
  SHIBOR/idx_sse 两处重复副本待切，见难题清单 T15-a/b。

## 🎯 项目概述

这是一个专为A股量化研究设计的**专业级本地数据中心**，提供完整的股票、财务、指数、市场数据支持。

**核心特性**:
- ✅ **完整数据体系**: 5,459只股票，15年+历史数据
- ✅ **高效存储**: Parquet列式存储，查询速度快
- ✅ **增量更新**: 智能检测，只下载新数据
- ✅ **开箱即用**: 配置简单，文档完善
- 🆕 **资金流向数据**: 个股资金流向（小单、中单、大单、特大单）
- 🆕 **筹码分布数据**: 每日筹码分布统计（成本分位数、获利比例）
- 🆕 **市场元数据**: 创业板、科创板等市场分类快速筛选
- 🆕 **前复权转换**: 实时转换后复权为前复权，无需额外存储
- 🆕 **数据质量保障**: 完整度检查、去重、异常值检测
- 🆕 **期货持仓数据**: CFFEX期指主力合约前20名会员持仓，支持情绪面多空比分析

> 📖 **新手？** 请先阅读 [快速开始指南 (QUICK_START.md)](QUICK_START.md)，了解每个下载程序的功能和使用方法！

---

## 📁 项目结构

```
数据中心/
├── 📄 核心脚本
│   ├── download_data_manager.py     # 数据下载管理器（主程序）
│   ├── download_financial_slow.py   # 财务数据下载（慢速稳定版）
│   ├── update_bond_yield.py         # 国债收益率增量更新脚本 🆕
│   ├── check_data_completeness.py   # 数据完整性检查工具 (v2.0)
│   ├── config.py                    # 全局配置文件
│   └── data_utils.py                # 数据处理工具函数
│
├── 📚 文档
│   ├── README.md                    # 本文件
│   ├── QUICK_START.md               # 快速开始指南 🆕
│   ├── 数据词典.md                   # 数据结构详细说明
│   └── DEPRECATED_FACTORS.md        # 因子功能废弃说明 🆕
│
├── 🗂️ others/ (辅助工具)
│   ├── data_config_example.py       # 配置文件模板
│   ├── example_usage.py             # 使用示例代码
│   └── view_parquet.py              # Parquet文件查看器
│
└── 💾 quant_data_center/ (数据存储)
     ├── stock_basic.parquet          # 股票基础信息 (5,452只)
     ├── stock/                     # 股票数据
     │   ├── daily_hfq/             # 日K线-后复权 (5,459只股票)
     │   ├── daily_qfq/             # 日K线-前复权 (5,459只股票) 🆕
     │   ├── daily_basic/           # 每日基础指标 (市值、PE/PB)
     │   │   └── daily_basic_all.parquet
     │   ├── moneyflow/             # 资金流向数据 🆕 (5,370只股票)
     │   ├── cyq_perf/             # 每日筹码分布统计数据 🆕 (5,457只股票)
     │   ├── fina_indicator/        # 财务指标 (5,444只)
     │   └── financial_tables/      # 财务三大表
     │       ├── income/            # 利润表 (5,445只)
     │       ├── balancesheet/      # 资产负债表 (5,444只)
     │       └── cashflow/          # 现金流量表 (5,444只)
     ├── stock_hk/                  # 港股数据 🆕
     │   ├── daily_hfq/            # 港股日K线（后复权）(2,686只)
     │   └── daily_qfq/            # 港股日K线（前复权）(可实时转换)
     ├── market_metadata/            # 市场元数据 🆕
     │   ├── chinext_stocks.parquet # 创业板股票标记
     │   └── stock_market_map.parquet # 市场分类映射
     ├── market/                    # 市场数据 🆕
     │   ├── margin_total/          # 融资融券交易汇总
     │   │   └── margin_total.parquet
     │   ├── margin_detail/         # 融资融券交易明细（按日期存储）
     │   ├── hsgt/                 # 沪深港通资金流向
     │   │   └── moneyflow_hsgt.parquet
     │   └── derivatives/           # 衍生品数据 🆕
     │       └── futures/           # 期货数据
     │           └── holding/       # 期货主力合约持仓
     │               ├── IF_top20.parquet  # 沪深300期指前20名会员持仓
     │               ├── IC_top20.parquet  # 中证500期指前20名会员持仓
     │               ├── IM_top20.parquet  # 中证1000期指前20名会员持仓
     │               └── IH_top20.parquet  # 上证50期指前20名会员持仓
     ├── factors/                   # 因子数据（⚠️ 部分已剥离）
     │   ├── macro/                # 宏观因子 (国债收益率) 🆕
     │   │   └── china_bond_yield_10y.parquet
     │   └── risk_free/           # 无风险利率 (SHIBOR)
     │       └── rfr_daily.parquet
     ├── index/                     # 指数数据
     │   ├── daily/                # 指数日K线 (5个指数)
     │   ├── daily_basic/          # 指数每日估值 (PE/PB) 🆕
     │   ├── constituents/         # 指数成分股历史数据
     │   ├── global_daily/         # 全球重要指数日K线 🆕
     │   └── weight/               # 指数成分股权重
     ├── classification/            # 分类数据
     │   └── industry_sw/          # 申万行业分类
     │       ├── sw_l1_daily.parquet              # 申万行业指数行情
     │       ├── industry_sw_member.parquet       # 申万二级行业个股历史映射
     │       └── sw_l3_member.parquet             # 申万行业成分股
     └── signals/                   # 策略信号数据
         ├── alpha_strategy_historical_signals.parquet  # Alpha策略历史信号
         └── alpha_strategy_historical_signals.csv     # Alpha策略历史信号（CSV格式）
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

### 1️⃣ 完整的股票数据体系

#### A. 股票日K线（后复权）
- **数量**: 5,422只股票
- **字段**: open, close, high, low, vol, amount, pct_chg
- **特点**: 自动复权，支持增量更新
- **用途**: 计算收益率、技术指标

#### B. 财务三大表（季度数据）
- **利润表** (income): 营业收入、净利润等
- **资产负债表** (balancesheet): 总资产、净资产等
- **现金流量表** (cashflow): 经营现金流

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

### 5️⃣ 每日筹码分布统计数据 🆕

**绝对路径**: `quant_data_center/stock/cyq_perf/{ts_code}.parquet`  
**用途**: 每日筹码分布衍生的统计数据，用于筹码分析、压力支撑判断、情绪指标构建等  
**数据源**: Tushare Pro API (`cyq_perf`)，文档ID: 294

**核心字段物理含义**:
- **cost_5pct** (成本5%分位数): 持仓成本最低的5%股东的平均持仓成本（元）
  - 反映低价筹码的集中度
  - 当股价接近此值时，可能面临低价筹码的抛压
- **cost_95pct** (成本95%分位数): 持仓成本最高的5%股东的平均持仓成本（元）
  - 反映高价筹码的集中度
  - 当股价接近此值时，可能面临高成本持仓的解套压力
- **weight_avg** (加权平均成本): 所有股东持仓成本的加权平均值（元）
  - 反映整体持仓成本水平
  - 用于判断当前股价相对于平均成本的位置
  - 是筹码分布的核心指标之一
- **winner_rate** (获利比例): 当前股价高于持仓成本的股东比例（%）
  - 反映市场整体盈亏情况
  - 用于判断市场情绪和抛压压力
  - 获利比例高时，可能面临获利了结压力
  - 获利比例低时，可能面临套牢盘压力

**数据特点**:
- ✅ **更新频率**: 每日更新
- ✅ **数据范围**: 从2010年开始（或股票上市日期）
- ✅ **存储方式**: 每只股票一个独立文件 (`{ts_code}.parquet`)
- ✅ **数据格式**: Parquet（高效列式存储）
- ✅ **增量更新**: 支持断点续传和增量更新
- ⚠️ **API限制**: 每分钟最多200次调用（需要适当积分权限）

**使用示例**:
```python
import pandas as pd

# 读取单只股票的筹码统计数据
df = pd.read_parquet('quant_data_center/stock/cyq_perf/000001.SZ.parquet')

# 计算当前获利比例
current_winner_rate = df['winner_rate'].iloc[-1]
print(f"当前获利比例: {current_winner_rate:.2f}%")

# 分析成本分布
print(f"成本5%分位数: {df['cost_5pct'].iloc[-1]:.2f}元")
print(f"加权平均成本: {df['weight_avg'].iloc[-1]:.2f}元")
print(f"成本95%分位数: {df['cost_95pct'].iloc[-1]:.2f}元")

# 判断当前股价相对于成本分布的位置
current_price = 10.0
if current_price < df['cost_5pct'].iloc[-1]:
    print("当前股价低于5%分位数，可能面临低价筹码抛压")
elif current_price > df['cost_95pct'].iloc[-1]:
    print("当前股价高于95%分位数，可能面临高成本持仓解套压力")
else:
    print("当前股价在成本分布区间内")
```

**应用场景**:
- 筹码分析：分析股票筹码分布情况，判断主力成本区间
- 压力支撑：通过成本分布判断关键压力位和支撑位
- 情绪指标：通过获利比例判断市场情绪和抛压压力
- 择时策略：结合价格和成本分布构建择时策略
- 风险控制：识别高成本持仓集中的风险区域

**更新数据**:
```python
from download_data_manager import QuantDataManager

manager = QuantDataManager()
manager.update_stock_cyq_perf()  # 增量更新

# 或指定股票列表
manager.update_stock_cyq_perf(stock_list=['000001.SZ', '000002.SZ'])
```

**注意事项**:
- Tushare的`cyq_perf`接口可能需要一定的积分权限，请确保账户有足够权限
- 部分股票可能没有筹码统计数据（特别是新上市股票），会返回空数据
- 建议每日更新，确保数据及时性

---

### 6️⃣ 国债收益率数据 🆕

**绝对路径**: `quant_data_center/factors/macro/china_bond_yield_10y.parquet`  
**用途**: 10年期国债收益率，作为无风险利率参考或宏观择时因子

**数据特点**:
- ✅ **完整历史数据**: 3,972条记录，从2010-01-04至今
- ✅ **每日更新**: 覆盖所有交易日
- ✅ **数据源**: AkShare (中美国债收益率对比数据)
- ✅ **自动备份**: Parquet和CSV双格式备份

**关键字段**:
- `trade_date`: 交易日期 (YYYYMMDD格式)
- `yield`: 10年期国债收益率 (%)
- `curve_term`: 期限 (10.0年)

**使用示例**:
```python
import pandas as pd

# 读取国债收益率数据
df_bond = pd.read_parquet('quant_data_center/factors/macro/china_bond_yield_10y.parquet')

# 计算收益率分位数（用于择时）
current_yield = df_bond['yield'].iloc[-1]
percentile = (df_bond['yield'] < current_yield).mean()
print(f"当前收益率: {current_yield:.2f}%, 历史分位数: {percentile:.1%}")

# 绘制收益率走势
import matplotlib.pyplot as plt
df_bond['trade_date'] = pd.to_datetime(df_bond['trade_date'])
df_bond.set_index('trade_date')['yield'].plot(title='10年期国债收益率走势')
```

**更新数据**:
```bash
# 增量更新（推荐）
python update_bond_yield.py

# 或通过数据管理器
python download_data_manager.py
# 选择: 14
```

**备份文件**:
- Parquet格式: `/home/zy/桌面/数据中心/backup_china_bond_yield_10y.parquet`
- CSV格式: `/home/zy/桌面/数据中心/backup_china_bond_yield_10y.csv`

---

### 7️⃣ CFFEX期货主力合约前20名会员持仓数据 🆕

**绝对路径**: `quant_data_center/market/derivatives/futures/holding/{variety}_top20.parquet`  
**用途**: CFFEX（中金所）期货主力合约前20名会员持仓数据，用于构建"情绪面"多空比指标，支持期指择时策略

**数据特点**:
- ✅ **主力合约自动跟踪**: 通过 `fut_mapping` API获取每日主力合约，确保数据连续性
- ✅ **前20名会员**: 只保留每日、每合约、多空排名均为前20名的会员数据
- ✅ **支持品种**: IF（沪深300）、IC（中证500）、IM（中证1000）、IH（上证50）
- ✅ **增量更新**: 自动从本地最新日期开始更新，避免重复下载

**关键字段**:
- `trade_date`: 交易日期 (YYYYMMDD格式)
- `ts_code`: 品种代码 (IF/IC/IM/IH)
- `contract`: 具体合约代码 (如 IF2406)
- `broker`: 期货公司会员名称
- `long_hld`: 多单持仓量
- `short_hld`: 空单持仓量
- `long_chg`: 多单增减
- `short_chg`: 空单增减

**使用示例**:
```python
import pandas as pd

# 读取IF（沪深300期指）持仓数据
df_if = pd.read_parquet('quant_data_center/market/derivatives/futures/holding/IF_top20.parquet')

# 计算多空比（前20名会员）
df_20240315 = df_if[df_if['trade_date'] == '20240315']
long_total = df_20240315['long_hld'].sum()
short_total = df_20240315['short_hld'].sum()
long_short_ratio = long_total / short_total if short_total > 0 else 0
print(f"IF多空比: {long_short_ratio:.4f}")

# 计算历史多空比序列（用于择时）
daily_stats = df_if.groupby('trade_date').agg({
    'long_hld': 'sum',
    'short_hld': 'sum'
}).reset_index()
daily_stats['long_short_ratio'] = daily_stats['long_hld'] / daily_stats['short_hld']
```

**更新数据**:
```python
from download_data_manager import QuantDataManager

manager = QuantDataManager()
manager.update_future_holdings()  # 更新所有品种（IF, IC, IM, IH）

# 或只更新指定品种
manager.update_future_holdings(varieties=['IF', 'IC'])
```

**应用场景**:
- 构建期指择时策略（IF/IC/IM对应不同指数）
- 分析机构资金在期指市场的多空倾向
- 实时监控期指市场主力资金的情绪变化

---

### 8️⃣ 市场元数据与工具函数 🆕

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

### 9️⃣ 数据完整性检查 (v2.0优化 + 新增功能)

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

### 10️⃣ 港股通数据 🆕

**绝对路径**: 
- `quant_data_center/stock_hk/daily_hfq/{ts_code}.parquet` (后复权)
- `quant_data_center/stock_hk/daily_qfq/{ts_code}.parquet` (前复权)

**用途**: 港股通个股的日K线数据，支持港股投资研究

**港股通数据**:
- ✅ **数据源**: 集成 Akshare，支持下载港股通个股的复权（HFQ）日线数据
- ✅ **数据量**: 2,678个后复权文件（前复权数据可实时转换）
- ✅ **复权处理**: 提供后复权数据，前复权数据可实时转换
- ✅ **增量更新**: 支持增量更新，自动从最新日期开始下载

**使用示例**:
```python
import pandas as pd

# 读取港股后复权数据
df_hk_hfq = pd.read_parquet('quant_data_center/stock_hk/daily_hfq/00700.HK.parquet')

# 读取港股前复权数据
df_hk_qfq = pd.read_parquet('quant_data_center/stock_hk/daily_qfq/00700.HK.parquet')

# 计算收益率
df_hk_hfq['return'] = df_hk_hfq['close'].pct_change()
```

---

### 11️⃣ 申万行业分类数据 🆕

**绝对路径**: 
- `quant_data_center/classification/industry_sw/sw_l1_daily.parquet` (申万行业指数行情)
- `quant_data_center/classification/industry_sw/industry_sw_member.parquet` (申万二级行业个股历史映射)
- `quant_data_center/classification/industry_sw/sw_l3_member.parquet` (申万行业成分股)

**申万行业指数行情**:
- **数据量**: 47,592条记录（2025-10-13 至 2025-11-18）
- **用途**: 申万行业指数的行情数据（价格、成交量、PE、PB等）
- **关键字段**: ts_code, trade_date, name, open, high, low, close, pe, pb, float_mv, total_mv

**申万二级行业个股历史映射**:
- **数据量**: 5,443条记录（5,443只股票的历史申万行业归属）
- **用途**: 历史上所有申万成分股对应的二级行业代码和名称，以及其划入和划出日期
- **关键字段**: ts_code, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, in_date, out_date

**申万行业成分股**:
- **数据量**: 5,472条记录
- **用途**: 申万行业指数与成分股的关联关系，用于行业分析、成分股查询等
- **关键字段**: l3_code, ts_code

**使用示例**:
```python
import pandas as pd

# 读取申万行业指数数据
df_sw_index = pd.read_parquet('quant_data_center/classification/industry_sw/sw_l1_daily.parquet')

# 获取某一天的各行业指数表现
df_20240101 = df_sw_index[df_sw_index['trade_date'] == '20240101']

# 查看涨幅前5的行业
top_industries = df_20240101.nlargest(5, 'pct_change')[['name', 'pct_change', 'pe', 'pb']]

# 读取申万L2历史映射数据
df_member = pd.read_parquet('quant_data_center/classification/industry_sw/industry_sw_member.parquet')

# 查询某个申万二级行业的所有成分股（当前仍在该行业）
l2_code = '801783.SI'  # 股份制银行Ⅱ
current_stocks = df_member[
    (df_member['l2_code'] == l2_code) & 
    (df_member['out_date'].isna())
]['ts_code'].tolist()
```

---

## 📖 使用示例

### 示例1: 构建股票池

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

### 示例2: 分析资金流向

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
| [QUICK_START.md](QUICK_START.md) | 快速开始指南 | 新手用户 |
| [数据词典.md](数据词典.md) | 数据结构详细说明 | 开发者 |
| [DEPRECATED_FACTORS.md](DEPRECATED_FACTORS.md) | 因子功能废弃说明 | 所有用户 |

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
| 股票基础信息 | 5,452只 | 1 MB |
| 股票日K线（后复权） | 5,453只×3,000天 | 2 GB |
| 股票日K线（前复权） | 5,453只×3,000天 | 2 GB |
| 每日基础指标 | 1,705万条 | 500 MB |
| 财务三大表 | 5,444只×3表×60季 | 800 MB |
| 财务指标 | 5,444只×100指标 | 200 MB |
| 指数数据 | 5个×3,800天 | 10 MB |
| 指数每日估值(PE/PB) | 4个指数 | < 1 MB |
| 全球指数数据 | 3个指数 | < 1 MB |
| **申万行业数据**: 
  - 申万行业指数: 47,592条记录
  - 申万L2历史映射: 5,443条记录
  - 申万L3成分股: 5,472条记录
| 资金流向数据 | 5,367个文件 | 500 MB |
| 每日筹码分布统计 | 5,452个文件 | 1 GB |
| 融资融券交易汇总 | 1,667条记录 | < 1 MB |
| 融资融券交易明细 | 3,799个文件 | 2 GB |
| 沪深港通资金流向 | 341条记录 | < 1 MB |
| CFFEX期货持仓数据 | 4个品种 | 100 MB |
| 港股日K线数据 | 2,678个文件 | 500 MB |
| 无风险利率 | 3,857条记录 | < 1 MB |
| 国债收益率 | 3,972条记录 | < 1 MB |
| **总计** | - | **约10 GB** |

### 数据覆盖范围

- **时间跨度**: 2010-01-01 至今
- **股票数据**: 5,452只A股，2,678只港股通
- **港股数据支持**: 集成 Akshare，支持下载港股通个股的复权（HFQ）日线数据，2,678只股票
- **财务数据**: 2006年至今（部分股票）
- **市场数据**: 
  - 融资融券汇总: 2019-01-08 至今（1,667条记录）
  - 融资融券明细: 2010-03-31 至今（3,799个文件）
  - 沪深港通: 2018-01-25 至 2024-08-16（341条记录）
  - 期货持仓: 2016-01-04 至今（4个品种）
- **指数数据**: 1993-01-11 至今（上证指数）
- **无风险利率**: 2010-01-25 至今
- **国债收益率**: 2010-01-04 至今

---

## 🎯 应用场景

### 1. 量化选股策略
- 基于财务指标的价值投资
- 成长股筛选
- 行业轮动策略

### 2. 技术分析研究
- 趋势跟踪策略
- 动量策略
- 均值回归策略

### 3. 指数增强策略
- 行业中性化处理
- Smart Beta策略
- 指数成分股优化

### 4. 风险管理
- VaR/CVaR计算
- 压力测试
- 组合风险监控

---

## ⚠️ 注意事项

### 1. API限制

- Tushare Pro有API调用频率限制
- 建议使用高级账户（2000+积分）
- 首次下载建议分批进行

### 2. 存储空间

- 完整数据约需3.5 GB磁盘空间
- 建议预留10 GB以上空间（含增量数据）

### 3. 未来函数

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

**3. 指数数据检查失败**
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
| **v1.10** | 2025-11-22 | 🆕 新增股票每日筹码分布统计数据（cyq_perf）<br>🆕 包含成本分位数、加权平均成本、获利比例等核心指标<br>🆕 支持筹码分析、压力支撑判断、情绪指标构建等应用场景<br>🆕 优化采样检查逻辑（均匀间隔采样，覆盖整个列表） |
| **v2.3** | 2025-11-22 | 🆕 新增股票每日筹码分布统计数据（cyq_perf）<br>🆕 包含成本分位数、加权平均成本、获利比例等核心指标<br>🆕 支持筹码分析、压力支撑判断、情绪指标构建等应用场景<br>🆕 优化采样检查逻辑（均匀间隔采样，覆盖整个列表） |
| **v2.2** | 2025-11-20 | 🆕 新增CFFEX期货主力合约前20名会员持仓数据<br>🆕 支持IF、IC、IM、IH四个期指品种<br>🆕 自动跟踪每日主力合约，确保数据连续性<br>🆕 用于构建"情绪面"多空比指标，支持期指择时策略 |
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

**最后更新**: 2025-11-22  
**数据版本**: v1.10  
**项目状态**: ✅ 生产就绪

</div>
