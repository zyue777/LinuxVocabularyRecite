#!/bin/bash
################################################################################
# Fama-French 五因子每日更新脚本
# 
# 功能：
#   1. 更新原始数据（股票日线、财务数据、市值等）
#   2. 重新构建Fama-French五因子
#   3. 验证数据时效性
#
# 使用方法：
#   bash daily_update_factors.sh
#
# 定时任务：
#   crontab -e
#   0 20 * * 1-5 /home/zy/桌面/数据中心/daily_update_factors.sh >> /tmp/ff5_update.log 2>&1
#
################################################################################

# 工作目录
cd /home/zy/桌面/数据中心 || exit 1

echo "========================================================================"
echo "Fama-French 五因子每日更新任务开始"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================================"

# 步骤1：更新原始数据
echo ""
echo "[步骤1/3] 更新原始数据..."
echo "------------------------------------------------------------------------"

python data_manager.py << EOF
1
EOF

if [ $? -ne 0 ]; then
    echo "❌ 错误：原始数据更新失败！"
    exit 1
fi

echo "✅ 原始数据更新完成"

# 步骤2：重新构建五因子
echo ""
echo "[步骤2/3] 重新构建Fama-French五因子..."
echo "------------------------------------------------------------------------"

python build_ff5_factors_monthly_ttm.py

if [ $? -ne 0 ]; then
    echo "❌ 错误：五因子构建失败！"
    exit 1
fi

echo "✅ 五因子构建完成"

# 步骤3：验证数据时效性
echo ""
echo "[步骤3/3] 验证数据时效性..."
echo "------------------------------------------------------------------------"

python << 'PYTHON_SCRIPT'
import pandas as pd
from datetime import datetime

df = pd.read_parquet('quant_data_center/factors/fama_french_5/ff_5_factors_daily.parquet')
df['trade_date'] = pd.to_datetime(df['trade_date'])

latest_date = df['trade_date'].max().date()
today = datetime.now().date()
lag_days = (today - latest_date).days

print(f"✅ 五因子数据验证:")
print(f"   总记录数: {len(df)} 条")
print(f"   最新日期: {latest_date}")
print(f"   时效性: 距今 {lag_days} 天")

if lag_days <= 1:
    print(f"   状态: ✅ 数据时效性优秀（{lag_days}天延迟）")
elif lag_days <= 3:
    print(f"   状态: ⚠️  数据略有延迟（{lag_days}天）")
else:
    print(f"   状态: ❌ 数据延迟较大（{lag_days}天），请检查！")
    exit(1)
PYTHON_SCRIPT

if [ $? -ne 0 ]; then
    echo "❌ 错误：数据验证失败！"
    exit 1
fi

# 完成
echo ""
echo "========================================================================"
echo "✅ Fama-French 五因子更新任务完成！"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================================"
echo ""

exit 0

