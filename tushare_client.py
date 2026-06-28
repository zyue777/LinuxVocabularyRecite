#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare 代理接入 — 严格遵循 ts.gyzcloud.top SDK 说明。

【SDK接入（推荐）】
    import tushare as ts
    ts.set_token('your_token')
    pro = ts.pro_api()
    pro._DataApi__http_url = "https://ts.gyzcloud.top/api"

【HTTP直接调用】
    GET https://ts.gyzcloud.top/api/{api_name}?token=...&...

凭证读 .env（见 .env.example）；文档 https://ts.gyzcloud.top/docs
"""
import functools
import os
from pathlib import Path

import pandas as pd
import tushare as ts
from dotenv import dotenv_values

_ENV_PATH = Path(__file__).resolve().parent / ".env"
_CFG = dotenv_values(_ENV_PATH)

_PRICE_COLS = ("open", "close", "high", "low", "pre_close")


def _need(key: str) -> str:
    val = _CFG.get(key) or os.environ.get(key)
    if not val:
        raise RuntimeError(f"缺凭证 {key}，请检查 {_ENV_PATH}")
    return val


@functools.lru_cache(1)
def get_pro():
    """SDK 三行接入（与代理文档一致）。"""
    ts.set_token(_need("TUSHARE_TOKEN"))
    pro = ts.pro_api()
    pro._DataApi__http_url = _need("TUSHARE_API_URL").rstrip("/")
    return pro


def get_rate_limit() -> int:
    """套餐频率限制（默认 150 次/分钟）。"""
    raw = _CFG.get("TUSHARE_RATE_LIMIT") or os.environ.get("TUSHARE_RATE_LIMIT") or "150"
    return max(1, int(raw))


def pro_bar(**kwargs):
    """
    ts.pro_bar 必须显式传入 api=get_pro()，否则会新建官方端点客户端导致 token 报错。
    与 SDK 文档配套的标准用法。
    """
    if kwargs.get("start_date") and kwargs.get("end_date"):
        s = kwargs["start_date"].replace("-", "")
        e = kwargs["end_date"].replace("-", "")
        if s > e:
            return None
    return ts.pro_bar(api=get_pro(), **kwargs)


def merge_adj_bars(daily_df: pd.DataFrame, adj_df: pd.DataFrame, adj: str = "hfq") -> pd.DataFrame | None:
    """用 pro.daily + pro.adj_factor 合并复权 K 线（与 ts.pro_bar 内部逻辑一致）。"""
    if daily_df is None or daily_df.empty:
        return None
    if adj_df is None or adj_df.empty or "adj_factor" not in adj_df.columns:
        return None

    fcts = adj_df[["trade_date", "adj_factor"]].copy()
    data = daily_df.set_index("trade_date", drop=False).merge(
        fcts.set_index("trade_date"), left_index=True, right_index=True, how="left"
    )
    data["adj_factor"] = data["adj_factor"].ffill()
    base = float(fcts["adj_factor"].iloc[0])
    for col in _PRICE_COLS:
        if col not in data.columns:
            continue
        if adj == "hfq":
            data[col] = data[col] * data["adj_factor"]
        elif adj == "qfq":
            data[col] = data[col] * data["adj_factor"] / base
        data[col] = data[col].astype(float)
    data = data.drop("adj_factor", axis=1)
    data["change"] = data["close"] - data["pre_close"]
    data["pct_chg"] = (data["change"] / data["pre_close"] * 100).astype(float)
    return data.reset_index(drop=True)


def fetch_hfq_daily(pro, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    """
    后复权日 K：仅用 SDK 文档推荐的 pro 实例调 pro.daily + pro.adj_factor。
    pro 须为 get_pro() 返回值。
    """
    s = start_date.replace("-", "")
    e = end_date.replace("-", "")
    if s > e:
        return None
    daily = pro.daily(ts_code=ts_code, start_date=s, end_date=e)
    if daily is None or daily.empty:
        return None
    adj = pro.adj_factor(ts_code=ts_code, start_date=s, end_date=e)
    return merge_adj_bars(daily, adj, adj="hfq")


def http_get(api_name: str, **params):
    """HTTP 直接调用（代理文档示例）。"""
    import requests

    url = f"{_need('TUSHARE_API_URL').rstrip('/')}/{api_name.lstrip('/')}"
    params = {"token": _need("TUSHARE_TOKEN"), **params}
    resp = requests.get(
        url,
        params=params,
        headers={"Accept-Encoding": "gzip"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()
