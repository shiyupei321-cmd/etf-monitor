"""
ETF 定投监控 — 历史数据回填脚本
================================
背景：历史库 2026-11-04 建库，30/60/90 日均需约 1.5 个月自然积累。
本脚本用两个免费接口一次性回填约 1.5 年历史，让均值与趋势立即生效：

  1. 腾讯 K 线接口（不复权）: web.ifzq.gtimg.cn/appstock/app/fqkline/get
     → 历史每日收盘价（并推算成交额，写入 turnover 列）
  2. 天天基金净值接口: fund.eastmoney.com/pingzhongdata/{code}.js
     → Data_netWorthTrend 历史每日官方净值（约 4 年）

口径与实时入库完全一致（与 history.py 的 record_daily 同构）：
  溢价率 = (当日收盘价 - T-1 官方净值) / T-1 官方净值 × 100%
  回填时 T-1 净值 = 净值序列中"最近一个严格早于当日"的净值日期，
  对应行情接口字段 [78]（T-1 日官方净值）。

幂等设计：INSERT OR REPLACE，主键 (date, code)；跳过今日及以后的记录，
保护 history.py 15:05 用实时行情写入的当日记录。可重复执行。

用法：
  python -X utf8 backfill.py             # 回填 320 个交易日（约 1.5 年）
  python -X utf8 backfill.py --days 500  # 自定义回填交易日数
  python -X utf8 backfill.py --dry-run   # 只拉取核对，不写库
"""
import argparse
import bisect
import json
import re
import sqlite3
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import config  # 复用监控池配置
from history import DB_PATH, init_db  # 复用历史库路径与建表
from monitor import fetch_etf_data, parse_etf_data  # 复用行情抓取获取 ETF 名称

# ── 抓取参数 ─────────────────────────────────────────────────
KLINE_TIMEOUT = 20        # 接口超时（秒）
FETCH_RETRIES = 2         # 失败重试次数（含首次）
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def _beijing_now() -> datetime:
    """当前北京时间。"""
    return datetime.now(timezone.utc) + timedelta(hours=8)


def _http_get(url: str, referer: str) -> str:
    """带 UA/Referer 的 GET 请求，网络异常自动重试，返回响应文本。"""
    last_err = None
    for _ in range(FETCH_RETRIES):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": referer})
            with urllib.request.urlopen(req, timeout=KLINE_TIMEOUT) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:  # 网络抖动重试
            last_err = exc
            time.sleep(1)
    raise RuntimeError(f"请求失败 {url}: {last_err}")


def _fetch_kline(code: str, days: int) -> list:
    """腾讯 K 线接口：历史每日行情（不复权），返回 [(日期, 收盘价, 成交量_手), ...] 升序。

    接口返回 JSON：data[code]["day"][i] = [日期, 开, 收, 高, 低, 成交量(手)]
    （部分环境键名为 qfqday/hfqday，做三键兼容；不复权价与官方净值口径一致）
    """
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,,,{days},"
    data = json.loads(_http_get(url, "http://gu.qq.com/"))
    node = data.get("data", {}).get(code)
    if not node:
        raise RuntimeError(f"{code} K线无数据: {str(data)[:200]}")
    rows = node.get("day") or node.get("qfqday") or node.get("hfqday") or []
    out = []
    for r in rows:
        try:
            close = float(r[2])
            vol = float(r[5]) if len(r) > 5 else 0.0
        except (ValueError, TypeError, IndexError):
            continue  # 脏数据行跳过
        if close > 0:
            out.append((r[0], close, vol))
    return out


def _fetch_nav_map(code6: str) -> dict:
    """天天基金历史净值：解析 Data_netWorthTrend 为 {日期: 净值} 映射。

    x 为毫秒时间戳（东八区零点），y 为单位净值；约 4 年历史。
    """
    url = f"http://fund.eastmoney.com/pingzhongdata/{code6}.js"
    raw = _http_get(url, f"http://fund.eastmoney.com/{code6}.html")
    m = re.search(r"Data_netWorthTrend\s*=\s*(\[.*?\]);", raw)
    if not m:
        raise RuntimeError(f"{code6} 未匹配到 Data_netWorthTrend（返回前100字符: {raw[:100]}）")
    tz8 = timezone(timedelta(hours=8))
    nav_map = {}
    for item in json.loads(m.group(1)):
        try:
            d = datetime.fromtimestamp(item["x"] / 1000, tz8).strftime("%Y-%m-%d")
            nav = float(item["y"])
        except (KeyError, ValueError, TypeError, OverflowError):
            continue  # 异常条目跳过
        if nav > 0:
            nav_map[d] = nav
    return nav_map


def _t1_nav(nav_dates: list, nav_map: dict, d: str):
    """取严格早于交易日 d 的最近净值（T-1 官方净值，对应行情字段 [78] 口径）。

    用 bisect_left 而不是 bisect_right：现在天天基金已收录 NAV(d)（当晚才公布），
    若用 <=d 会取到当日净值，与实时系统"当日看 T-1"的口径不一致。
    """
    idx = bisect.bisect_left(nav_dates, d) - 1
    if idx < 0:
        return None  # 该 ETF 上市初期可能无净值历史
    return nav_map[nav_dates[idx]]


def backfill_one(code: str, name: str, days: int, today: str, dry_run: bool, conn) -> dict:
    """单只 ETF 回填：K线收盘价 × T-1 净值 → 溢价率，逐日写入 daily 表。返回统计信息。"""
    klines = _fetch_kline(code, days)
    nav_map = _fetch_nav_map(code[2:])  # 去掉 sz/sh 前缀即天天基金代码
    nav_dates = sorted(nav_map)
    written = 0
    skipped = 0
    samples = []  # (日期, 收盘, 净值, 溢价) 最近 5 条，用于抽查
    premiums = []  # 全部溢价率，用于打印区间抽查合理性
    for d, close, vol in klines:
        if d >= today:
            continue  # 今日及以后交给 history.py 实时记录，回填不覆盖
        nav = _t1_nav(nav_dates, nav_map, d)
        if nav is None:
            skipped += 1
            continue
        premium = (close - nav) / nav * 100  # 溢价率 %，与 history.py 公式一致
        turnover = close * vol / 100  # 成交额（万元）= 手×100股×价 ÷ 10000
        if not dry_run:
            conn.execute(
                "INSERT OR REPLACE INTO daily (date, code, name, price, nav, premium, turnover) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (d, code, name, close, nav, premium, turnover),
            )
        written += 1
        samples.append((d, close, nav, premium))
        premiums.append(premium)
    return {
        "code": code, "name": name,
        "kline": len(klines), "nav": len(nav_map),
        "written": written, "skipped": skipped,
        "first": samples[0][0] if samples else "—",
        "last": samples[-1][0] if samples else "—",
        "samples": samples[-5:],
        "pmin": min(premiums) if premiums else None,  # 溢价率区间（抽查合理性）
        "pmax": max(premiums) if premiums else None,
    }


def main():
    parser = argparse.ArgumentParser(description="ETF 历史数据回填（腾讯K线 × 天天基金净值）")
    parser.add_argument("--days", type=int, default=320, help="回填交易日数，默认 320（约1.5年）")
    parser.add_argument("--dry-run", action="store_true", help="只拉取核对，不写库")
    args = parser.parse_args()

    today = _beijing_now().strftime("%Y-%m-%d")
    codes = [c for c in config.ETF_CODES.split(",") if c]

    # 用实时行情接口拿 ETF 名称（与 history.py 同源，避免维护两套名称表）
    print(f"[1/3] 拉取实时行情获取名称（{len(codes)} 只）...")
    name_map = {e["code"]: e["name"] for e in parse_etf_data(fetch_etf_data())}
    missing = [c for c in codes if c not in name_map]
    if missing:
        raise RuntimeError(f"行情接口缺少以下代码的名称: {missing}")

    init_db()  # 幂等建表
    conn = None if args.dry_run else sqlite3.connect(DB_PATH)

    print(f"[2/3] 逐只回填（每只 {args.days} 个交易日，T-1 净值口径）...")
    results = []
    for code in codes:
        try:
            stat = backfill_one(code, name_map[code], args.days, today, args.dry_run, conn)
        except Exception as exc:
            print(f"  ❌ {code} {name_map.get(code, '')} 回填失败: {exc}")
            continue
        results.append(stat)
        latest = stat["samples"][-1] if stat["samples"] else None
        latest_str = f"最新 {latest[0]} 溢价 {latest[3]:+.2f}%" if latest else "无样本"
        range_str = f"溢价区间 {stat['pmin']:+.1f}~{stat['pmax']:+.1f}%" if stat["pmin"] is not None else "无样本"
        print(f"  ✅ {code} {stat['name']:<10} K线{stat['kline']:<3} 净值{stat['nav']:<4} "
              f"写入{stat['written']:<3} 跳过{stat['skipped']:<3} "
              f"{range_str}｜{latest_str}")

    if conn:
        conn.commit()
        conn.close()

    # [3/3] 口径核对：库内今日净值（行情字段[78]=NAV(昨日)）vs 回填取到的最近净值。
    # 注意：天天基金 QDII 净值上传滞后 1 个工作日（最新一条是前天交易日），
    # 两者相差一个净值日、差值为美股单日涨跌属预期；回填跳过今日，不受影响。
    print("[3/3] 口径核对（库内今日[78]净值 vs 天天基金最近净值；QDII 上传滞后 1 天，有差值属预期）...")
    check = sqlite3.connect(DB_PATH)
    for code in codes:
        stat = next((r for r in results if r["code"] == code), None)
        if stat is None:
            continue
        nav_map = _fetch_nav_map(code[2:])
        nav_dates = sorted(nav_map)
        backfill_nav = _t1_nav(nav_dates, nav_map, today)
        nav_date = nav_dates[bisect.bisect_left(nav_dates, today) - 1] if backfill_nav is not None else "—"
        row = check.execute("SELECT nav FROM daily WHERE code=? AND date=?", (code, today)).fetchone()
        if row and backfill_nav is not None:
            diff = abs(row[0] - backfill_nav)
            mark = "✓ 口径一致" if diff < 0.005 else f"△ 差 {diff:.4f}（美股单日波动，属预期）"
            print(f"  {code} 库内[78]={row[0]:.4f} vs 回填NAV({nav_date})={backfill_nav:.4f} {mark}")
        elif row:
            print(f"  {code} 库内[78]={row[0]:.4f}，天天基金无更早日净值")
        else:
            print(f"  {code} 今日记录不存在（15:05 复盘任务未运行或今日休市）")
    n_dates = check.execute("SELECT COUNT(DISTINCT date) FROM daily").fetchone()[0]
    n_rows = check.execute("SELECT COUNT(*) FROM daily").fetchone()[0]
    check.close()
    print(f"\n完成：历史库现有 {n_rows} 行 / {n_dates} 个交易日" + ("（dry-run 未写库）" if args.dry_run else ""))


if __name__ == "__main__":
    main()
