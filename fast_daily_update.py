#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按交易日增量更新 daily_hfq + daily_qfq（比逐只法快约 18 倍）。
口径严格同 tushare_client.merge_adj_bars：hfq = raw × adj_factor；qfq = hfq / 最新factor。
写入「先写 .tmp 再原子替换」，不删原文件，幂等可重复运行。
- 作模块：from fast_daily_update import run_fast_update; run_fast_update()
- 作脚本：python3 fast_daily_update.py [起点YYYYMMDD] [终点YYYYMMDD]
起点缺省=自动侦测活跃前沿(抽样众数)；终点缺省=最近交易日。
"""
import sys
import time
import random
import collections
from datetime import datetime, timedelta

import pandas as pd
from tushare_client import get_pro
from config import DATA_CENTER_PATH

HFQ = DATA_CENTER_PATH / "stock" / "daily_hfq"
QFQ = DATA_CENTER_PATH / "stock" / "daily_qfq"
PRICE = ["open", "high", "low", "close", "pre_close"]


def _latest_trade_date(pro, today):
    lo = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")
    cal = pro.trade_cal(exchange="SSE", start_date=lo, end_date=today, is_open="1")
    return max(str(x) for x in cal["cal_date"].tolist())


def _detect_start(_):
    """抽样 500 个文件取末行日期众数=活跃前沿；仅用于选起点，不跳过任何股票。"""
    fs = [f for f in HFQ.glob("*.parquet") if not f.stem.startswith("_bak")]
    random.seed(0)
    cnt = collections.Counter()
    for f in random.sample(fs, min(500, len(fs))):
        try:
            cnt[str(pd.read_parquet(f, columns=["trade_date"])["trade_date"].iloc[-1])] += 1
        except Exception:
            pass
    return cnt.most_common(1)[0][0]


def run_fast_update(start=None, end=None):
    pro = get_pro()
    today = datetime.now().strftime("%Y%m%d")
    end = end or _latest_trade_date(pro, today)
    start = start or _detect_start(pro)
    cal = pro.trade_cal(exchange="SSE", start_date=start, end_date=end, is_open="1")
    dates = sorted(str(x) for x in cal["cal_date"].tolist())
    if not dates:
        print("[fast_daily] 无待补交易日，已最新", flush=True)
        return (0, 0, 0)
    print(f"[fast_daily] 待补 {len(dates)} 个交易日：{dates[0]}..{dates[-1]}", flush=True)

    t0 = time.time()
    fd, fa = [], []
    for i, d in enumerate(dates, 1):
        dd = pro.daily(trade_date=d)
        aa = pro.adj_factor(trade_date=d)
        if dd is not None and not dd.empty:
            fd.append(dd)
        if aa is not None and not aa.empty:
            fa.append(aa)
        if i % 10 == 0:
            print(f"  已拉 {i}/{len(dates)} 天，{time.time()-t0:.0f}s", flush=True)
    if not fd:
        print("[fast_daily] 拉取为空（可能非交易时段/数据未出）", flush=True)
        return (0, 0, 0)
    big_d = pd.concat(fd, ignore_index=True)
    big_a = pd.concat(fa, ignore_index=True)
    m = big_d.merge(big_a[["ts_code", "trade_date", "adj_factor"]],
                    on=["ts_code", "trade_date"], how="left").dropna(subset=["adj_factor"])
    print(f"[fast_daily] 拉取完成 {time.time()-t0:.0f}s，行数={len(m)}", flush=True)

    updated = nofile = noupd = 0
    for code, g in m.groupby("ts_code"):
        fh = HFQ / f"{code}.parquet"
        if not fh.exists():
            nofile += 1
            continue
        sh = pd.read_parquet(fh)
        last = str(sh["trade_date"].iloc[-1])
        gnew = g[g["trade_date"].astype(str) > last].copy()
        if gnew.empty:
            noupd += 1
            continue
        for c in PRICE:
            gnew[c] = gnew[c] * gnew["adj_factor"]
        gnew["change"] = gnew["close"] - gnew["pre_close"]
        gnew["pct_chg"] = gnew["change"] / gnew["pre_close"] * 100
        hf = pd.concat([sh, gnew[list(sh.columns)]], ignore_index=True)
        hf = hf.drop_duplicates("trade_date", keep="last").sort_values("trade_date").reset_index(drop=True)
        base = float(g.sort_values("trade_date")["adj_factor"].iloc[-1])
        qf = hf.copy()
        for c in PRICE + ["change"]:
            qf[c] = qf[c] / base
        th = fh.with_suffix(".tmp")
        tq = (QFQ / f"{code}.parquet").with_suffix(".tmp")
        hf.to_parquet(th)
        qf.to_parquet(tq)
        th.replace(fh)
        tq.replace(QFQ / f"{code}.parquet")
        updated += 1

    print(f"[fast_daily] 完成：更新 {updated} 只，已最新跳过 {noupd} 只，"
          f"无本地文件(新股待全量) {nofile} 只，总耗时 {time.time()-t0:.0f}s", flush=True)
    return (updated, noupd, nofile)


if __name__ == "__main__":
    s = sys.argv[1] if len(sys.argv) > 1 else None
    e = sys.argv[2] if len(sys.argv) > 2 else None
    run_fast_update(s, e)
