#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""补齐后校验：① 新鲜度分布 ② 与 Tushare 官方 pro_bar 复权输出独立对照。"""
import random
import pandas as pd
from tushare_client import get_pro, pro_bar
from config import DATA_CENTER_PATH

HFQ = DATA_CENTER_PATH / "stock" / "daily_hfq"
QFQ = DATA_CENTER_PATH / "stock" / "daily_qfq"
pro = get_pro()

# 1) 新鲜度分布（抽样 500）
import collections
fs = list(HFQ.glob("*.parquet"))
fs = [f for f in fs if not f.stem.startswith("_bak")]
random.seed(2)
cnt = collections.Counter()
for f in random.sample(fs, 500):
    try:
        cnt[str(pd.read_parquet(f, columns=["trade_date"])["trade_date"].iloc[-1])] += 1
    except Exception:
        cnt["ERR"] += 1
print("[新鲜度] 抽样500文件末行日期(top6):")
for d, c in cnt.most_common(6):
    print(f"    {d}: {c}")

# 2) 独立对照：5 只活跃股，比对 pro_bar 官方复权 vs 本地
codes = ["000001.SZ", "600519.SH", "300750.SZ", "601318.SH", "000651.SZ"]
S, E = "20260401", "20260703"
maxh = maxq = 0.0
n = 0
for code in codes:
    loc_h = pd.read_parquet(HFQ / f"{code}.parquet").set_index("trade_date")
    loc_q = pd.read_parquet(QFQ / f"{code}.parquet").set_index("trade_date")
    ref_h = pro_bar(ts_code=code, adj="hfq", start_date=S, end_date=E)
    ref_q = pro_bar(ts_code=code, adj="qfq", start_date=S, end_date=E)
    if ref_h is None or ref_q is None:
        print(f"    {code}: pro_bar 无数据，跳过")
        continue
    ref_h = ref_h.set_index("trade_date"); ref_q = ref_q.set_index("trade_date")
    for d in ref_h.index:
        if d in loc_h.index:
            maxh = max(maxh, abs(float(loc_h.loc[d, "close"]) - float(ref_h.loc[d, "close"])))
            n += 1
        if d in loc_q.index:
            maxq = max(maxq, abs(float(loc_q.loc[d, "close"]) - float(ref_q.loc[d, "close"])))
    print(f"    {code}: 本地末行 {loc_h.index[-1]}，对照 {len(ref_h)} 天")

print(f"[对照] 比对 {n} 个点位")
print(f"[对照] hfq vs 官方 最大误差：{maxh:.4f}")
print(f"[对照] qfq vs 官方 最大误差：{maxq:.4f}")
print("[结论] 误差在浮点精度内(≈0)即证明补齐数据与 Tushare 官方复权口径一致。")
