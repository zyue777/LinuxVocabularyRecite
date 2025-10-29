# Fama-French五因子 v2.0 优化版 - 更新说明

**更新日期**: 2025-10-28  
**版本**: v2.0  
**作者**: 数据中心团队

---

## 📋 更新概述

根据最新的五因子构建方法详解文档，对`build_ff5_factors_monthly_ttm.py`进行了重大优化，主要针对**RMW因子**和**CMA因子**的计算方法进行改进，使其更贴近A股实战。

---

## 🔄 核心变更

### 1. **RMW因子优化：从营业利润到经营现金流**

#### 原方法 (v1.0)
```python
# 使用营业利润(OP)
OP = TTM营业利润 / 净资产
```

#### 新方法 (v2.0)
```python
# 使用经营现金流(OCF)
OCF_B = TTM经营现金流 / 净资产
```

**优化理由**:
- **盈利质量**: 经营现金流反映真实的现金流入，避免"纸面富贵"
- **A股特点**: A股企业财务操纵空间大，现金流比利润更可靠
- **实战效果**: 现金流充沛的公司通常有更强的抗风险能力

**数据来源**:
- 现金流量表 (`cashflow/`): `n_cashflow_act` 字段（经营活动现金流净额）

---

### 2. **CMA因子优化：从总资产到经营性净资产**

#### 原方法 (v1.0)
```python
# 使用总资产增长率
Inv = (今年总资产 - 去年总资产) / 去年总资产
```

#### 新方法 (v2.0)
```python
# 使用经营性净资产(NOA)增长率
NOA = 有形资产 (包含固定资产+在建工程+无形资产等)
NOA_Inv = (今年NOA - 去年NOA) / 去年总资产
```

**优化理由**:
- **真实投资**: 有形资产更准确反映企业的真实资本开支
- **在建工程**: 包含在建工程，避免滞后计入投资的问题
- **排除金融资产**: 剔除金融投资和应收账款等，聚焦主营业务投资

**数据来源**:
- 财务指标表 (`fina_indicator/`): `tangible_asset` 字段（有形资产）

---

## 📊 代码变更详情

### 数据路径新增

```python
# v2.0 新增
PATH_CASHFLOW_DIR = os.path.join(DATA_CENTER_PATH, 'stock/financial_tables/cashflow')
PATH_FINA_INDICATOR_DIR = os.path.join(DATA_CENTER_PATH, 'stock/fina_indicator')
```

### `load_all_financials()` 函数

**v1.0 → v2.0 变更**:

| 步骤 | v1.0 | v2.0 |
|------|------|------|
| 加载财务报表 | 利润表、资产负债表 | **现金流量表**、资产负债表、**财务指标表** |
| 计算盈利指标 | OP_TTM (营业利润TTM) | **OCF_TTM (经营现金流TTM)** |
| 计算投资指标 | Inv (总资产增长率) | **NOA_Inv (经营性净资产增长率)** |
| 输出字段 | `['OP_TTM', 'Inv', 'B_latest']` | `['OCF_TTM', 'NOA_Inv', 'B_latest']` |

### `get_sorting_snapshot_monthly()` 函数

**v1.0 → v2.0 变更**:

```python
# v1.0
df_snapshot['OP'] = df_snapshot['OP_TTM'] / df_snapshot['B_latest']
df_snapshot['Inv'] = df_snapshot['Inv']

# v2.0
df_snapshot['OCF_B'] = df_snapshot['OCF_TTM'] / df_snapshot['B_latest']  # 经营现金流/净资产
df_snapshot['NOA_Inv'] = df_snapshot['NOA_Inv']  # 经营性净资产投资
```

### `get_portfolios_monthly()` 函数

**v1.0 → v2.0 变更**:

```python
# v1.0: 基于OP分组
op_30 = df_snapshot['OP'].quantile(0.3)
op_70 = df_snapshot['OP'].quantile(0.7)
df_snapshot['Prof'] = np.where(df_snapshot['OP'] <= op_30, 'W', ...)

# v2.0: 基于OCF_B分组
ocf_30 = df_snapshot['OCF_B'].quantile(0.3)
ocf_70 = df_snapshot['OCF_B'].quantile(0.7)
df_snapshot['Prof'] = np.where(df_snapshot['OCF_B'] <= ocf_30, 'W', ...)
```

```python
# v1.0: 基于Inv分组
inv_30 = df_snapshot['Inv'].quantile(0.3)
inv_70 = df_snapshot['Inv'].quantile(0.7)
df_snapshot['Invest'] = np.where(df_snapshot['Inv'] <= inv_30, 'C', ...)

# v2.0: 基于NOA_Inv分组
noa_inv_30 = df_snapshot['NOA_Inv'].quantile(0.3)
noa_inv_70 = df_snapshot['NOA_Inv'].quantile(0.7)
df_snapshot['Invest'] = np.where(df_snapshot['NOA_Inv'] <= noa_inv_30, 'C', ...)
```

---

## 🔧 技术细节

### OCF_TTM 计算逻辑

```python
# 1. 将累计现金流转为单季现金流
df_cashflow['ocf_single_quarter'] = df_cashflow.groupby(['ts_code', year])['n_cashflow_act'].diff()

# 2. Q1的单季值 = 累计值
is_q1 = (df_cashflow['end_date'].dt.month == 3)
df_cashflow.loc[is_q1, 'ocf_single_quarter'] = df_cashflow.loc[is_q1, 'n_cashflow_act']

# 3. 计算TTM (滚动4个季度)
df_cashflow['OCF_TTM'] = df_cashflow.groupby('ts_code')['ocf_single_quarter'].rolling(window=4, min_periods=4).sum()
```

### NOA_Inv 计算逻辑

```python
# 1. 定义经营性净资产 (NOA)
df_balance['NOA'] = df_balance['tangible_asset'].fillna(0)

# 2. 获取一年前的NOA和总资产
df_balance['NOA_t_4'] = df_balance.groupby('ts_code')['NOA'].shift(4)
df_balance['assets_t_4'] = df_balance.groupby('ts_code')['total_assets'].shift(4)

# 3. 计算NOA投资因子
df_balance['NOA_Inv'] = (df_balance['NOA'] - df_balance['NOA_t_4']) / df_balance['assets_t_4']
```

---

## 📦 数据依赖

### v1.0 数据依赖
```
├── stock/financial_tables/income/          # 利润表
├── stock/financial_tables/balancesheet/    # 资产负债表
└── stock/daily_basic/                       # 市值数据
```

### v2.0 数据依赖 (新增)
```
├── stock/financial_tables/cashflow/        # 现金流量表 ⭐新增
├── stock/financial_tables/balancesheet/    # 资产负债表
├── stock/fina_indicator/                   # 财务指标表 ⭐新增
└── stock/daily_basic/                       # 市值数据
```

---

## ⚠️ 注意事项

### 1. **数据加载方式变更**

由于`tangible_asset`字段在某些文件中可能存在schema不一致，v2.0采用逐文件读取方式：

```python
# v1.0: 使用PyArrow Dataset (快速但可能报错)
df_fina = ds.dataset(PATH_FINA_INDICATOR_DIR).to_table().to_pandas()

# v2.0: 逐文件读取 (稳定但稍慢)
fina_files = [f for f in os.listdir(PATH_FINA_INDICATOR_DIR) if f.endswith('.parquet')]
df_fina_list = []
for file in fina_files:
    df_temp = pd.read_parquet(os.path.join(PATH_FINA_INDICATOR_DIR, file), ...)
    df_fina_list.append(df_temp)
df_fina = pd.concat(df_fina_list)
```

### 2. **计算时间**

由于新增了财务指标表的加载（5,444个文件），预计总运行时间会增加约**20-30%**。

### 3. **向后兼容性**

⚠️ **v2.0与v1.0不兼容**，因子值会有显著差异。建议：
- 重新计算所有历史因子数据
- 更新所有依赖FF5因子的策略和模型
- 保留v1.0结果用于对比分析

---

## 🚀 使用方法

### 运行构建脚本

```bash
cd /home/zy/桌面/数据中心
python3 build_ff5_factors_monthly_ttm.py
```

**预计运行时间**: 10-15分钟（取决于机器性能）

### 输出文件

```
quant_data_center/factors/fama_french_5/ff_5_factors_daily.parquet
```

**字段**:
- `trade_date`: 交易日期 (YYYYMMDD)
- `MKT_RF`: 市场风险溢价 (%)
- `SMB`: 规模因子 (%)
- `HML`: 价值因子 (%)
- `RMW`: **盈利质量因子 (v2.0优化: 基于经营现金流)** (%)
- `CMA`: **投资因子 (v2.0优化: 基于经营性净资产)** (%)

---

## 📈 预期效果

### RMW因子 (经营现金流质量)

**优势**:
- ✅ 更能识别真实的盈利质量
- ✅ 降低财务造假风险
- ✅ 提高因子在A股市场的有效性

**潜在影响**:
- 高现金流公司权重增加
- 低现金流公司（如高应收账款公司）权重降低

### CMA因子 (经营性净资产投资)

**优势**:
- ✅ 更准确捕捉真实资本开支
- ✅ 包含在建工程，避免投资滞后
- ✅ 排除金融资产干扰

**潜在影响**:
- 重资产行业（制造业、基建）投资因子更敏感
- 轻资产行业（互联网、服务业）投资因子更平稳

---

## 📚 参考文档

- **构建方法详解**: `/home/zy/桌面/数据中心/五因子构建方法详解.md` (v2.0版)
- **数据词典**: `/home/zy/桌面/数据中心/数据词典.md`
- **验证脚本**: `/home/zy/桌面/数据中心/validate_ff5_model.py`

---

## 🔬 验证测试

运行测试脚本验证数据加载和计算逻辑：

```bash
python3 test_ff5_v2_update.py
```

---

## 📝 修订记录

| 版本 | 日期 | 主要变更 |
|------|------|---------|
| v1.0 | 2025-10-27 | 初始版本，使用OP和Inv |
| v2.0 | 2025-10-28 | **实战优化**：RMW改为OCF_B，CMA改为NOA_Inv |

---

**更新完成时间**: 2025-10-28  
**测试状态**: ✅ 已通过  
**生产就绪**: ✅ 是

如有问题请参考文档或联系数据中心团队。

