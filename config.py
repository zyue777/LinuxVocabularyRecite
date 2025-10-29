#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据中心全局配置文件
"""

import os
from pathlib import Path

# --- 全局数据中心根目录 ---
DATA_CENTER_PATH = Path(r"/home/zy/桌面/数据中心/quant_data_center")

# --- 自动生成子目录路径 ---
PATH_STOCK_DAILY_HFQ = DATA_CENTER_PATH / "stock" / "daily_hfq"
PATH_STOCK_DAILY_BASIC = DATA_CENTER_PATH / "stock" / "daily_basic"
PATH_INDEX_CONST = DATA_CENTER_PATH / "index" / "constituents"
PATH_INDEX_DAILY = DATA_CENTER_PATH / "index" / "daily"
PATH_FACTORS_FF3 = DATA_CENTER_PATH / "factors" / "fama_french_3"
PATH_FACTORS_RFR = DATA_CENTER_PATH / "factors" / "risk_free"
PATH_INDUSTRY_SW = DATA_CENTER_PATH / "classification" / "industry_sw"

# 确保在导入时，如果路径不存在，会收到提示
if not DATA_CENTER_PATH.exists():
    print(f"警告：数据中心路径不存在: {DATA_CENTER_PATH}")
    print("请先运行 create_folders.py 脚本，并确保 config.py 中的路径设置正确。")
else:
    print(f"数据中心路径已确认: {DATA_CENTER_PATH}")

TUSHARE_TOKEN = '15d0e2fdd93c6f7eced2d5e4e01d2c5a1c3a19d189ab8b206a512899'
