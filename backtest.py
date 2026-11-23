"""
ETF 定投监控 — 买入信号回测（方案C）
====================================
一次性分析脚本：回放历史库，统计历史上每次买入信号（一级/二级）出现后
5 / 10 / 20 个交易日的溢价率与价格变化，验证信号有效性。

用法：
  python -X utf8 backtest.py            # 全量回测并打印汇总
  python -X utf8 backtest.py --detail   # 追加打印每次信号明细（前 20 条）

口径与实盘完全一致（与 history.py 同一套定义）：
  一级：溢价率 < 类目阈值（标普500 2% / 纳指100 4% / 纳指科技 8%）
  二级：溢价率 ≥ 阈值 且 ≤ 溢价30日均（溢价稳定）且 现价 ≤ 价格30日均（价格低位）
  30日均窗口取信号日之前 30 个交易日（不含当日，与实盘"截至上一交易日"一致）

输出指标：
  溢价变动：信号日后 N 日溢价率 − 信号日溢价率（百分点，负=溢价回落=买入时机兑现）
  价格变动：信号日后 N 日收盘价相对信号日的涨跌幅（%）
  胜率：价格变动 > 0 的样本占比
"""
import argparse
import bisect
import sqlite3
from datetime import datetime, timedelta, timezone

import config  # 复用类目阈值
from history import DB_PATH  # 复用历史库路径
from monitor import fetch_index_kline, ETF_INDEX_MAP  # 指数K线 + ETF→指数映射（市场阶段分桶用）

# ── 回测参数 ─────────────────────────────────────────────────
HORIZONS = (5, 10, 20)      # 信号后的观察窗口（交易日）
MIN_SIGNAL_N = 3            # 某类信号总数低于此值不统计（样本无代表性）
MEAN_WINDOW = 30            # 均值窗口（信号日前 30 个交易日，与实盘一致）

# 类目名称（按阈值映射，与监控池分类一致）
CATEGORY_NAMES = {2.0: "标普500", 4.0: "纳指100", 8.0: "纳指科技"}


def _beijing_now() -> datetime:
    """当前北京时间。"""
    return datetime.now(timezone.utc) + timedelta(hours=8)


def load_series(conn: sqlite3.Connection) -> dict:
    """从历史库按 code 分组加载时间序列（日期升序）：
    {code: [(date, price, nav, premium), ...]}，price/nav ≤ 0 的脏行跳过。"""
    by_code = {}
    for code, date, price, nav, premium in conn.execute(
        "SELECT code, date, price, nav, premium FROM daily ORDER BY date"
    ):
        if price <= 0 or nav <= 0:
            continue
        by_code.setdefault(code, []).append((date, price, nav, premium))
    return by_code


def load_index_regime() -> dict:
    """信号日 → 指数当日距前 250 个交易日高点回撤（%），供按市场阶段分桶。
    返回 {指数代码: [(日期, 回撤%), ...]}（日期升序）。回撤口径与 fetch_market_watch
    一致：当日收盘 / 截至当日（含当日）近 250 个交易日最高收盘 - 1。"""
    series = {}
    for code in ("usINX", "usNDX"):
        rows = fetch_index_kline(code)   # [(日期, 收盘), ...] 旧→新
        pts = []
        closes = []
        for d, c in rows:
            closes.append(c)
            win = closes[-250:]          # 截至当日近 250 个交易日（含当日）
            if len(win) >= 2:
                pts.append((d, (c / max(win) - 1) * 100))
        series[code] = pts
    return series


def _dd_on(pts: list, date_str: str):
    """二分查找 ≤ 该日期的最近回撤值（信号日遇美股休市时取最近前一日）；无数据返回 None。"""
    dates = [p[0] for p in pts]
    i = bisect.bisect_right(dates, date_str) - 1
    return pts[i][1] if i >= 0 else None


def _signals_for_series(series: list, threshold: float) -> dict:
    """单只 ETF 时间序列 → 信号样本：{(tier, horizon): [(溢价变动, 价格变动%), ...]}。

    逐日回放：一级 = 溢价 < 阈值；二级 = 溢价稳定（≤30日均）+ 价格低位（≤30日均），
    均值窗口取当日之前 30 个交易日（不含当日，与实盘口径一致）。
    信号日之后 horizon 天内数据不足的样本丢弃。
    """
    outcomes = {(t, h): [] for t in (1, 2) for h in HORIZONS}
    detail = {(t, h): [] for t in (1, 2) for h in HORIZONS}
    for i, (d, price, _nav, premium) in enumerate(series):
        if i < 2:
            continue  # 均值样本不足，与实盘"n<2 不判断"一致
        hist = series[max(0, i - MEAN_WINDOW):i]  # 当日之前 30 个交易日
        prem_avg = sum(r[3] for r in hist) / len(hist)
        price_avg = sum(r[1] for r in hist) / len(hist)
        # ── 信号分级（与实盘 history.py 同规则）──
        if premium < threshold:
            tier = 1
        elif premium <= prem_avg and price <= price_avg:
            tier = 2
        else:
            continue
        # ── 观察窗口：信号日后 N 日的溢价与价格变动 ──
        for h in HORIZONS:
            if i + h >= len(series):
                continue
            d2, price2, _nav2, premium2 = series[i + h]
            prem_chg = premium2 - premium                 # 溢价变动（百分点）
            price_chg = (price2 / price - 1) * 100        # 价格变动（%）
            outcomes[(tier, h)].append((prem_chg, price_chg))
            detail[(tier, h)].append((d, premium, prem_chg, price_chg))
    return {"outcomes": outcomes, "detail": detail}


def _fmt_row(tier_name: str, h: int, samples: list) -> str:
    """汇总一行：样本数 / 溢价变动均值 / 价格变动均值 / 胜率。"""
    n = len(samples)
    if n < MIN_SIGNAL_N:
        return f"  后{h:>2}日  样本 {n}（不足 {MIN_SIGNAL_N}，不统计）"
    prem_chgs = [s[0] for s in samples]
    price_chgs = [s[1] for s in samples]
    win_rate = sum(1 for p in price_chgs if p > 0) / n * 100
    return (
        f"  后{h:>2}日  样本 {n:>4}｜溢价变动 {sum(prem_chgs) / n:+.2f} 个百分点｜"
        f"价格变动 {sum(price_chgs) / n:+.2f}%｜胜率 {win_rate:.0f}%"
    )


def main():
    parser = argparse.ArgumentParser(description="ETF 买入信号历史回测（方案C）")
    parser.add_argument("--detail", action="store_true", help="追加打印信号明细（前 20 条）")
    args = parser.parse_args()

    today = _beijing_now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    by_code = load_series(conn)
    conn.close()

    # ── 逐只回放，按 (tier, horizon) 聚合样本 ──
    agg = {(t, h): [] for t in (1, 2) for h in HORIZONS}
    agg_detail = {(t, h): [] for t in (1, 2) for h in HORIZONS}
    cat_agg = {}  # {类目: {(tier, horizon): [样本]}}
    for code, series in sorted(by_code.items()):
        threshold = config.get_etf_threshold(code)
        category = CATEGORY_NAMES.get(threshold, f"其他({threshold}%)")
        res = _signals_for_series(series, threshold)
        for key, samples in res["outcomes"].items():
            agg[key].extend(samples)
            agg_detail[key].extend(
                (code, d, prem, pc, pcg) for (d, prem, pc, pcg) in res["detail"][key]
            )
            cat_agg.setdefault(category, {}).setdefault(key, []).extend(samples)

    tier_names = {1: "一级（溢价<类目阈值）", 2: "二级（溢价稳·价低）"}
    print(f"╔══════════════════════════════════════════════════╗")
    print(f"║  ETF 买入信号历史回测（截至 {today}）              ║")
    print(f"╚══════════════════════════════════════════════════╝")
    print(f"  历史库：{len(by_code)} 只 ETF｜观察窗口 5/10/20 个交易日｜")
    print(f"  溢价变动<0 = 溢价回落（信号兑现）；价格变动>0 = 上涨\n")

    for tier in (1, 2):
        print(f"【{tier_names[tier]}】")
        for h in HORIZONS:
            print(_fmt_row(tier_names[tier], h, agg[(tier, h)]))
        print()

    # ── 分类目统计（标普500 / 纳指100 / 纳指科技）──
    print("── 分类目（后10日）──")
    for category in sorted(cat_agg):
        for tier in (1, 2):
            samples = cat_agg[category].get((tier, 10), [])
            if len(samples) >= MIN_SIGNAL_N:
                prem_avg = sum(s[0] for s in samples) / len(samples)
                price_avg = sum(s[1] for s in samples) / len(samples)
                win_rate = sum(1 for s in samples if s[1] > 0) / len(samples) * 100
                print(f"  {category} {tier_names[tier].split('（')[0]}：样本 {len(samples):>4}｜"
                      f"溢价变动 {prem_avg:+.2f}pp｜价格变动 {price_avg:+.2f}%｜胜率 {win_rate:.0f}%")

    # ── 按信号日市场阶段分桶（2026-11-23 新增）──
    # 验证"下跌段信号是否接飞刀"：信号日指数处深度回撤（熊市区）时，后10日收益是否显著更差；
    # 若更差，则支持实盘规则"熊市区仅定投、大额加仓需折价+企稳确认"。
    print("\n── 按信号日市场阶段分桶（后10日，回撤=信号日指数距前250日高点）──")
    regime_series = load_index_regime()
    buckets = {
        "deep": "🐻深度回撤(≤-20%)",
        "corr": "🔻回调(-20%~-10%)",
        "norm": "⬜正常/浅回调(>-10%)",
    }
    for tier in (1, 2):
        agg_by_regime = {k: [] for k in buckets}
        for code, d, _prem, _pc, pcg in agg_detail[(tier, 10)]:
            idx_code = ETF_INDEX_MAP.get(code)
            if not idx_code:
                continue
            dd = _dd_on(regime_series.get(idx_code, []), d)
            if dd is None:
                continue
            if dd <= config.REGIME_DRAWDOWN_DEEP:
                key = "deep"
            elif dd <= config.REGIME_DRAWDOWN_CORRECTION:
                key = "corr"
            else:
                key = "norm"
            agg_by_regime[key].append(pcg)
        print(f"  {tier_names[tier].split('（')[0]}：")
        for key, label in buckets.items():
            samples = agg_by_regime[key]
            if len(samples) < MIN_SIGNAL_N:
                print(f"    {label}：样本 {len(samples)}（不足 {MIN_SIGNAL_N}，不统计）")
                continue
            win_rate = sum(1 for p in samples if p > 0) / len(samples) * 100
            print(f"    {label}：样本 {len(samples):>4}｜后10日价格 {sum(samples) / len(samples):+.2f}%｜"
                  f"胜率 {win_rate:.0f}%")

    # ── 信号明细（可选）──
    if args.detail:
        print("\n── 信号明细（后10日，按信号日排序，前 20 条）──")
        rows = agg_detail[(1, 10)] + agg_detail[(2, 10)]
        rows.sort(key=lambda r: r[1])
        for code, d, prem, prem_chg, price_chg in rows[:20]:
            print(f"  {d} {code} 溢价 {prem:+.2f}% → 后10日 溢价 {prem_chg:+.2f}pp 价格 {price_chg:+.2f}%")


if __name__ == "__main__":
    # 安全：非 TTY 环境（计划任务）不崩溃
    try:
        import sys
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
