#!/home/zy/miniconda3/envs/dailyreport/bin/python
# -*- coding: utf-8 -*-
"""
daily_run.py - 常规数据下载一键执行脚本
=============================================
按顺序执行以下三个步骤：
  步骤 1：调用 download_data_manager.py 的"更新所有数据"功能
  步骤 2：运行 update_strategy_13_16.py（更新策略数据 13-16）
  步骤 3：运行 期权/RUNoption.py（期权数据下载与分析）

最终在终端打印本次下载的汇总状态报告。

使用方法：
    python daily_run.py
    或
    /home/zy/miniconda3/envs/dailyreport/bin/python /home/zy/桌面/数据中心/daily_run.py
"""

import os
import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path

# ─────────────────────── 路径配置 ───────────────────────
# 数据中心目录（本脚本所在目录）
DATA_CENTER_DIR = Path(__file__).parent.resolve()

# 期权脚本目录
OPTION_DIR = Path("/home/zy/桌面/LearnPY/大盘日报/期权")

# 使用与本脚本相同的 Python 解释器
PYTHON = sys.executable

# ─────────────────────── 工具函数 ───────────────────────

def separator(title: str, width: int = 80, char: str = "="):
    print("\n" + char * width)
    print(f"  {title}")
    print(char * width)


def run_step(step_num: int, title: str, cmd: list, cwd=None, stdin_input: str = None) -> dict:
    """
    执行单个步骤（subprocess），返回结果字典。
    cmd:        命令列表，如 [PYTHON, 'script.py', 'arg']
    stdin_input: 若脚本有交互提示，传入自动输入的字符串（含换行符）
    """
    separator(f"步骤 {step_num} / 3：{title}", char="─")
    start_time = datetime.now()
    print(f"⏰ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    success = False
    error_msg = ""

    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            input=stdin_input,
            text=True,
            check=True
        )
        success = (result.returncode == 0)
    except subprocess.CalledProcessError as e:
        error_msg = f"进程返回错误码 {e.returncode}"
        success = False
    except Exception as e:
        error_msg = str(e)
        success = False

    end_time = datetime.now()
    duration = end_time - start_time
    minutes, seconds = divmod(int(duration.total_seconds()), 60)

    status_icon = "✅" if success else "❌"
    print(f"\n{status_icon} 步骤 {step_num} 完成")
    print(f"⏱  用时: {minutes} 分 {seconds} 秒")
    if error_msg:
        print(f"⚠️  错误: {error_msg}")

    return {
        "step": step_num,
        "title": title,
        "success": success,
        "duration": f"{minutes}分{seconds}秒",
        "error": error_msg,
        "start": start_time.strftime("%H:%M:%S"),
        "end": end_time.strftime("%H:%M:%S"),
    }




def build_report(results: list, total_start: datetime) -> str:
    """生成下载状态汇总报告"""
    total_end = datetime.now()
    total_duration = total_end - total_start
    total_min, total_sec = divmod(int(total_duration.total_seconds()), 60)

    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("  📊  本次下载汇总报告")
    lines.append("=" * 80)
    lines.append(f"  执行日期：{total_start.strftime('%Y-%m-%d')}")
    lines.append(f"  开始时间：{total_start.strftime('%H:%M:%S')}")
    lines.append(f"  结束时间：{total_end.strftime('%H:%M:%S')}")
    lines.append(f"  总耗时：  {total_min} 分 {total_sec} 秒")
    lines.append("-" * 80)
    lines.append(f"  {'步骤':<4}  {'状态':<5}  {'用时':<8}  {'开始':<8}  {'结束':<8}  {'名称'}")
    lines.append("-" * 80)

    all_ok = True
    for r in results:
        icon = "✅ 成功" if r["success"] else "❌ 失败"
        if not r["success"]:
            all_ok = False
        lines.append(
            f"  {r['step']:<4}  {icon:<6}  {r['duration']:<8}  "
            f"{r['start']:<8}  {r['end']:<8}  {r['title']}"
        )
        if r["error"]:
            lines.append(f"         ⚠️  错误详情: {r['error']}")

    lines.append("-" * 80)
    if all_ok:
        lines.append("  🎉  所有步骤执行成功！数据已更新到最新状态。")
    else:
        failed = [r["title"] for r in results if not r["success"]]
        lines.append(f"  ⚠️  以下步骤执行失败，请检查日志：")
        for f in failed:
            lines.append(f"     - {f}")
    lines.append("=" * 80 + "\n")

    return "\n".join(lines)


# ─────────────────────── 主流程 ───────────────────────

def main():
    total_start = datetime.now()

    separator(f"🚀 常规数据下载 & 分析 - 一键执行", char="█")
    print(f"  执行时间: {total_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Python:   {PYTHON}")
    print("  期权目录: " + str(OPTION_DIR))
    
    # 验证 Tushare 配置
    try:
        import config
        token = config.TUSHARE_TOKEN
        api_url = config.TUSHARE_API_URL
        expire = config.TUSHARE_EXPIRE
        if token and len(token) > 10:
            token_display = f"{token[:4]}...{token[-4:]}"
            print(f"  Tushare Token: {token_display} (已配置)")
            print(f"  Tushare 端点:  {api_url}")
            if expire:
                print(f"  套餐到期:    {expire}")
        else:
            print("  Tushare Token: 未配置，请检查 .env")
    except Exception:
        print("  Tushare: 无法加载 config.py / .env")

    results = []

    # ── 步骤 1：更新所有基础数据 ──
    # 自动输入：第一行回车（使用默认数据路径），第二行输入 1（选项1=更新所有数据）
    r1 = run_step(
        step_num=1,
        title="下载基础数据（download_data_manager 选项1：更新所有数据）",
        cmd=[PYTHON, str(DATA_CENTER_DIR / "download_data_manager.py")],
        cwd=DATA_CENTER_DIR,
        stdin_input="\n1\n",  # 回车=使用默认路径，1=更新所有数据
    )
    results.append(r1)

    # ── 步骤 2：更新策略数据 13-16 ──
    r2 = run_step(
        step_num=2,
        title="更新策略数据 13-16（update_strategy_13_16）",
        cmd=[PYTHON, str(DATA_CENTER_DIR / "update_strategy_13_16.py"), "all"],
        cwd=DATA_CENTER_DIR,
    )
    results.append(r2)

    # ── 步骤 3：期权数据下载与分析 ──
    r3 = run_step(
        step_num=3,
        title="期权数据下载与分析（RUNoption）",
        cmd=[PYTHON, str(OPTION_DIR / "RUNoption.py")],
        cwd=OPTION_DIR,
    )
    results.append(r3)

    # ── 打印汇总报告 ──
    report = build_report(results, total_start)
    print(report)

    # 根据整体结果返回退出码
    all_ok = all(r["success"] for r in results)
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
