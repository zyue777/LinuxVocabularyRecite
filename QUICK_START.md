# 数据中心快速参考 (Quick Start)

**最后更新**: 2025-12-11  
**数据版本**: v1.11

---

## � 下载程序功能速查表

### 主程序: `download_data_manager.py`

运行后选择对应数字即可：

| 选项 | 下载内容 | 一句话说明 |
|------|---------|-----------|
| **1** | 所有基础数据 | 一键下载股票日K线、指数、成分股、无风险利率、申万行业等所有基础数据 |
| **2** | 股票日K线 | 下载A股个股日K线数据（后复权+前复权自动转换），5,459只股票 |
| **3** | 资金流向 | 下载个股资金流向数据（小单、中单、大单、特大单），用于散户情绪分析 |
| **4** | 指数成分股 | 下载沪深300、中证500、创业板指等指数的成分股及权重 |
| **5** | 无风险利率 | 下载SHIBOR利率数据，用于计算超额收益 |
| **6** | 申万行业数据 | 下载申万行业分类、成分股、历史映射，用于行业分析 |
| **7** | 指数日K线 | 下载A股指数（上证、沪深300等）和全球指数（恒生、标普等）日K线 |
| **8** | 股票基础信息 | 下载股票列表、上市日期、行业分类等基础信息 |
| **9** | 每日基础指标 | 下载市值、PE/PB、换手率等每日指标，用于因子构建 |
| **10** | 因子策略 | 提示运行因子构建脚本（FF5、FF3、CH3、自定义因子） |
| **11** | 融资融券汇总 | 下载市场整体融资融券数据，用于市场情绪分析 |
| **12** | 融资融券明细 | 下载每只股票的融资融券明细数据（数据量大） |
| **13** | 港股日K线 | 下载港股通个股日K线数据（后复权），2,686只港股 |
| **14** | 四维择时数据 | 下载指数估值（PE/PB）+ 国债收益率，用于市场择时 |
| **15** | 期货持仓 | 下载CFFEX期指（IF/IC/IM/IH）前20名会员持仓，用于情绪分析 |
| **16** | 筹码分布 | 下载每日筹码分布统计数据（成本分位数、获利比例），用于技术分析 |

---

### 辅助程序

| 程序名 | 功能说明 |
|--------|---------|
| `update_bond_yield.py` | 单独更新10年期国债收益率数据（增量更新） |
| `check_data_status.py` | 检查数据完整性和质量（支持 `--full` 完整检查，`--auto-fix` 自动修复） |
| `enrich_hk_industry_classification.py` | 更新港股行业分类缓存 |

---

### 因子构建程序

| 程序名 | 功能说明 |
|--------|---------|
| `build_ff5_factors_monthly_ttm.py` | 构建Fama-French五因子（v2.0优化版，使用OCF和NOA） |
| `build_ff3_factors_full_market.py` | 构建Fama-French三因子（全市场，含金融股） |
| `build_ch3_factors.py` | 构建中国版三因子（剔除壳价值，使用E/P） |
| `build_custom_factors.py` | 构建自定义因子（UMD动量、LIQ流动性） |

---

## 🚀 常用操作

### 每日更新（推荐）

```bash
# 1. 更新股票日K线
python download_data_manager.py  # 选择: 2

# 2. 更新每日基础指标
python download_data_manager.py  # 选择: 9

# 3. 更新国债收益率
python update_bond_yield.py

# 4. 检查数据状态
python check_data_status.py
```

**总耗时**: 约40-90分钟

---

### 首次下载

```bash
# 1. 下载所有基础数据
python download_data_manager.py  # 选择: 1

# 2. 下载资金流向
python download_data_manager.py  # 选择: 3

# 3. 下载筹码分布
python download_data_manager.py  # 选择: 16

# 4. 下载四维择时数据
python download_data_manager.py  # 选择: 14

# 5. 检查数据完整性
python check_data_status.py --full
```

**总耗时**: 约4-6小时

---

### 构建因子

```bash
# 构建FF5因子（推荐）
python build_ff5_factors_monthly_ttm.py

# 构建其他因子（可选）
python build_ff3_factors_full_market.py
python build_ch3_factors.py
python build_custom_factors.py
```

**总耗时**: 约30-60分钟

---

## 📊 数据调用示例

### 读取股票日K线

```python
import pandas as pd

DATA_CENTER = '/home/zy/桌面/数据中心/quant_data_center'

# 后复权数据
df_hfq = pd.read_parquet(f'{DATA_CENTER}/stock/daily_hfq/000001.SZ.parquet')

# 前复权数据
df_qfq = pd.read_parquet(f'{DATA_CENTER}/stock/daily_qfq/000001.SZ.parquet')
```

### 读取资金流向

```python
# 读取资金流向
df_mf = pd.read_parquet(f'{DATA_CENTER}/stock/moneyflow/000001.SZ.parquet')

# 计算散户净流入
df_mf['retail_net'] = df_mf['buy_sm_amount'] - df_mf['sell_sm_amount']
```

### 读取筹码分布

```python
# 读取筹码分布
df_cyq = pd.read_parquet(f'{DATA_CENTER}/stock/cyq_perf/000001.SZ.parquet')

# 查看最新数据
print(f"获利比例: {df_cyq['winner_rate'].iloc[-1]:.2f}%")
```

### 读取FF5因子

```python
# 读取FF5因子
df_ff5 = pd.read_parquet(f'{DATA_CENTER}/factors/fama_french_5/ff_5_factors_daily.parquet')
```

---

## ❓ 常见问题

**Q: API限流怎么办？**  
A: 程序已内置限流，会自动等待60秒后重试

**Q: 如何只更新指定股票？**  
A: 在代码中调用 `manager.update_stock_daily_hfq(stock_list=['000001.SZ'])`

**Q: 前复权和后复权有什么区别？**  
A: 后复权最新价格与市场一致，前复权便于计算历史收益率

**Q: 数据占用多少空间？**  
A: 总计约15-20GB

**Q: 如何定时自动更新？**  
A: 使用crontab设置定时任务，每天18:00自动运行

---

## 📞 更多信息

- **详细文档**: 查看 `数据词典.md`
- **项目说明**: 查看 `README.md`
- **Tushare文档**: https://tushare.pro/document/2

---

<div align="center">

**祝您研究顺利！** 📊🚀

**最后更新**: 2025-12-11  
**数据版本**: v1.11

</div>
