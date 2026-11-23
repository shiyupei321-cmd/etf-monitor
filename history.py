"""
ETF 定投监控 — 每日历史记录 + 均值复盘 + 盘中全量报告脚本
==========================================================
两个模式（本机计划任务触发，FC 云端无持久化存储，历史库只能放本机）：

  复盘模式（工作日 15:05 触发，默认）：
    1. 拉取腾讯行情，把全量 ETF 当日收盘溢价率写入本地 SQLite（幂等：同日重复运行覆盖当日记录）
    2. 计算每只 ETF 近 30 / 60 / 90 个交易日的溢价率均值
    3. 判断每只 ETF 现价、净值近 30 个交易日是上涨 / 下降 / 持平（今日 vs 30 个交易日前，±0.5% 容差）
    4. 生成每日复盘报告推送钉钉：全量 ETF 单表，按溢价率从低到高排列（最低在上），
       每只 ETF 后面跟现价 / 净值趋势判断

  盘中模式（工作日 10:00 / 14:00 / 14:30 触发，--intraday）：
    1. 不入库（历史库只存收盘价，避免盘中价污染均值）
    2. 生成盘中全量报告推送钉钉：全量单表按溢价率从低到高（最低在上），
       每只附 30 日均溢价率（截至上一交易日）+ 买入信号

用法：
  python history.py             # 复盘模式：记录 + 推送钉钉
  python history.py --dry-run   # 复盘模式：记录 + 打印报告，不推送钉钉
  python history.py --intraday  # 盘中模式：不入库，推送盘中全量报告（含30日均）

设计说明：
  - 阿里云 FC 函数无持久化存储（/tmp 冷启动即清空），历史库放在本机 SQLite
  - 交易日校验用行情接口时间戳字段 [30]（yyyyMMddHHmmss）：
    时间戳日期 != 今天 → 节假日/周末返回的是上一个交易日的过期行情 → 跳过记录，避免写入重复数据
"""
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import config  # 复用监控池与钉钉凭证
from monitor import (  # 复用主脚本能力：抓取 / 解析 / 推送 / 日志 / 信号
    fetch_etf_data,
    parse_etf_data,
    send_dingtalk,
    log_event,
    is_stale_data,
    market_status_text,
    compute_buy_signal,
    ROW_SEPARATOR,       # 每股之间的下划线隔离区
    fetch_market_watch,  # 美股大盘+汇率：点位 + 30/120/250 日趋势 + 近250日分位
    market_regime,             # 市场阶段分类（2026-11-23：距高点回撤 → 阶段+建议）
    regime_row_tag_for_etf,    # 可买入行附所属指数阶段标签
)

# ── 历史库路径（本机持久化，FC 云端无此能力）──────────────────
DB_PATH = Path(__file__).parent / "data" / "etf_history.db"

# ── 均值窗口（交易日口径）────────────────────────────────────
WINDOW_PREMIUM = (30, 60, 90)   # 溢价率均值窗口
WINDOW_TREND = 30               # 现价/净值短期趋势判断窗口（今日 vs 30 个交易日前）
WINDOW_TREND_LONG = 250         # 现价/净值长线趋势窗口（今日 vs 250 个交易日前，一年牛熊格局）

# ── 溢价率历史分位（方案B：溢价在近 90 个交易日中的贵贱位置）──
PREM_PCTILE_WINDOW = 90         # 分位窗口：近 90 个交易日
PREM_PCTILE_MIN_N = 10          # 样本下限，不足不显示分位
PREMIUM_HIGH_PCTILE = 90        # 分位 ≥ 90% → 高位警示（方案D）

# ── 回归风险量化（方案E）：买入后溢价收敛回30日均将亏多少 ────
REGRESSION_LOSS_MIN = 0.3       # 亏损超过 0.3% 才显示提醒

# ── 低于 2% 溢价 = 买入候选（全量表中 🟢 标记）────────────────
BUY_PREMIUM_LINE = 2.0

# ── 趋势判断容差：涨跌幅在 ±0.5% 内视为持平 ──────────────────
TREND_FLAT_TOLERANCE = 0.5

# ── 复盘报告标题（与实时报告的 DINGTALK_TITLE 区分）───────────
REVIEW_TITLE = "美股 ETF 定投每日复盘"


def _beijing_now() -> datetime:
    """当前北京时间。"""
    return datetime.now(timezone.utc) + timedelta(hours=8)


def _premium(etf: dict) -> Optional[float]:
    """计算单只 ETF 溢价率（%），价格或净值无效时返回 None。"""
    if etf["price"] > 0 and etf["iopv"] > 0:
        return (etf["price"] - etf["iopv"]) / etf["iopv"] * 100
    return None


def _quote_trade_date(raw_data: str) -> str:
    """从行情原始数据提取交易日（字段 [30] 时间戳前 8 位），如 '20261104'。
    返回空串表示提取失败。"""
    for line in raw_data.split(";"):
        if "=" not in line:
            continue
        _, _, data_part = line.partition("=")
        fields = data_part.split("~")
        if len(fields) > 30 and fields[30] and fields[30][:8].isdigit():
            return fields[30][:8]
    return ""


def init_db() -> None:
    """创建历史表（幂等）。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily (
            date     TEXT    NOT NULL,   -- 交易日 YYYY-MM-DD
            code     TEXT    NOT NULL,   -- 腾讯行情代码，如 sz159655
            name     TEXT    NOT NULL,   -- ETF 名称
            price    REAL    NOT NULL,   -- 当日收盘价
            nav      REAL    NOT NULL,   -- T-1 官方净值（溢价率基准）
            premium  REAL    NOT NULL,   -- 溢价率（%）
            turnover REAL    DEFAULT 0,  -- 成交额（万元）
            PRIMARY KEY (date, code)
        )
        """
    )
    conn.commit()
    conn.close()


def record_daily(etfs: list, trade_date: str) -> int:
    """把当日全量 ETF 记录写入历史库（INSERT OR REPLACE，同日重复运行覆盖当日）。"""
    conn = sqlite3.connect(DB_PATH)
    written = 0
    for etf in etfs:
        pct = _premium(etf)
        if pct is None:
            continue
        conn.execute(
            """
            INSERT OR REPLACE INTO daily (date, code, name, price, nav, premium, turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (trade_date, etf["code"], etf["name"], etf["price"],
             etf["iopv"], pct, etf.get("turnover", 0)),
        )
        written += 1
    conn.commit()
    conn.close()
    return written


def _history_rows(code: str) -> list:
    """取单只 ETF 最近 250 个交易日记录（新→旧），含价格与净值，
    供溢价均值窗口（30/60/90）、长线趋势（250 日）与溢价分位（90 日）判断。"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT date, price, nav, premium FROM daily WHERE code = ? ORDER BY date DESC LIMIT ?",
        # +1：统计时剔除今日记录后仍够窗口；取溢价均值/长线趋势所需的最大窗口
        (code, max(max(WINDOW_PREMIUM), WINDOW_TREND_LONG) + 1),
    ).fetchall()
    conn.close()
    return rows


def _total_sample_days() -> int:
    """历史库中已记录的不同交易日数量。"""
    conn = sqlite3.connect(DB_PATH)
    n = conn.execute("SELECT COUNT(DISTINCT date) FROM daily").fetchone()[0]
    conn.close()
    return n


def _avg(values: list) -> Optional[float]:
    """列表均值，空列表返回 None。"""
    return sum(values) / len(values) if values else None


def _fmt_avg(value: Optional[float], n: int, window: int) -> str:
    """均值显示：样本不足窗口时标注实际天数，如 '6.22(n=3)'。"""
    if value is None:
        return "—"
    return f"{value:.2f}(n={n})" if n < window else f"{value:.2f}"


def _price_vs_avg_str(price: float, s: dict) -> str:
    """现价相对价格30日均的高低符号：▲ 高于（偏贵）/ ▼ 低于（偏便宜）/ 无均值返回 ''。"""
    avg = s.get("price_30")
    if avg and avg > 0:
        if price > avg:
            return "▲"
        if price < avg:
            return "▼"
    return ""


def _signal_with_premium_stability(etf: dict, premium: float, threshold: float,
                                   market_ok: bool, is_near_close: bool,
                                   mins_to_close: int, s: dict) -> tuple:
    """本机报告买入信号（在云端 compute_buy_signal 基础上增加二级信号）。
    一级：溢价率 < 类目阈值 → 原信号（🔥🟢🟡✅，含日内时机），最优先。
    二级：溢价率 ≥ 阈值但溢价稳定（不高于溢价30日均，即溢价未扩张）且
          现价不高于价格30日均（价格处于低位）→ 稳定溢价下的低价买点。
    样本 n < 2 不足以判断时返回 '—'。"""
    signal, is_buy = compute_buy_signal(
        etf, premium, threshold, market_ok, is_near_close, mins_to_close
    )
    if is_buy or not market_ok:
        return signal, is_buy

    prem_avg = s.get("prem_30")
    price_avg = s.get("price_30")
    if (prem_avg is None or price_avg is None
            or min(s.get("prem_30_n", 0), s.get("price_30_n", 0)) < 2):
        return "—", False

    # 溢价稳定：当前溢价不高于近30交易日均值（溢价没比历史常态更贵）
    prem_stable = premium <= prem_avg
    # 价格低位：现价不高于价格30日均
    price_low = etf["price"] <= price_avg
    if prem_stable and price_low:
        return "✅ **溢价稳·价低**", True
    return "—", False


def _tier2_flag(s: dict) -> str:
    """复盘报告二级信号标记：溢价稳定（≤溢价30日均）且现价 ≤ 价格30日均。
    满足返回 ' ✅'（附在标的名后），否则返回 ''。样本 n < 2 不判断；一级（<2%）不重复标记。"""
    if s["premium"] < BUY_PREMIUM_LINE:
        return ""
    prem_avg = s.get("prem_30")
    price_avg = s.get("price_30")
    if (prem_avg is None or price_avg is None
            or min(s.get("prem_30_n", 0), s.get("price_30_n", 0)) < 2):
        return ""
    if s["premium"] <= prem_avg and s["price"] <= price_avg:
        return " ✅"
    return ""


def _trend_str(today_val: float, baseline: Optional[float], n: int,
               window: int = WINDOW_TREND) -> str:
    """趋势判断：今日值 vs 窗口内最早一天（默认近 30 交易日；长线传 window=250）。
    涨跌幅超过 ±0.5% 判上涨/下降，否则持平；样本不足 2 天返回 '—'；
    样本不足窗口时按实际天数标注（近n日）。"""
    if baseline is None or baseline <= 0 or n < 2:
        return "—"
    change = (today_val - baseline) / baseline * 100
    if change > TREND_FLAT_TOLERANCE:
        label = "📈 上涨"
    elif change < -TREND_FLAT_TOLERANCE:
        label = "📉 下降"
    else:
        label = "➖ 持平"
    span = f"（近{n}日）" if n < window else ""
    return f"{label} {change:+.1f}%{span}"


def _names_list(stat_list: list, limit: int = 3) -> str:
    """统计列表 → 名称摘要（最多 limit 个，超出加"等N只"），用于决策摘要区。"""
    names = [s["name"] for s in stat_list]
    if len(names) <= limit:
        return "、".join(names)
    return "、".join(names[:limit]) + f" 等{len(names)}只"


def dca_pacing_label(stats: list, tier1_n: int) -> str:
    """本期定投档位（2026-11-23 新增）：把"买不买"细化为"这期投多少"。
    定投不停是底线（微笑曲线靠下跌段摊低成本），档位只调节每期金额，优先级从高到低：
    🔥加倍 = 恐慌折价连2日+（历史大底标志）；🟢正常 = 有溢价低于类目阈值的标的；
    🟡减半 = 多数只溢价高于自身30日均（整体偏贵）；⏸缓投 = 多数只溢价处近90日高位（极端贵）。
    样本为空返回空串。"""
    n = len(stats)
    if n == 0:
        return ""
    # 1) 恐慌折价连 2 日+：极端恐慌 → 历史性买点区，定投可加倍
    panic = [s for s in stats if s.get("discount_days", 0) >= 2]
    if panic:
        return (f"🔥 **加倍投**：{len(panic)} 只恐慌折价连2日+（历史上接近大底），"
                f"本期可加倍，分 2-3 批买入")
    # 2) 有溢价低于类目阈值的一级信号：标的便宜，正常定投
    if tier1_n > 0:
        return f"🟢 **正常投**：{tier1_n} 只溢价低于类目阈值，按计划定投"
    # 3) 多数只溢价处近90日高位：极端贵，本期缓投（省下的份额等溢价回落补投）
    extreme = [s for s in stats
               if s.get("prem_pctile") is not None and s["prem_pctile"] >= PREMIUM_HIGH_PCTILE]
    if len(extreme) > n / 2:
        return (f"⏸ **本期缓投**：{len(extreme)}/{n} 只溢价处近90日高位（极端贵），"
                f"本期缓投，等溢价回落至30日均下方补投")
    # 4) 多数只溢价高于自身30日均：整体偏贵，本期减半
    rich = [s for s in stats
            if s.get("prem_30") is not None and s["premium"] > s["prem_30"]]
    if len(rich) > n / 2:
        return (f"🟡 **减半投**：{len(rich)}/{n} 只溢价高于自身30日均（整体偏贵），"
                f"本期减半，剩余留待溢价回落")
    return "🟢 **正常投**：整体溢价不高，按计划定投"


def build_stats(etfs: list) -> list:
    """每只 ETF 的 30/60/90 交易日溢价率均值 + 溢价近90日分位
    + 现价/净值近 30 交易日短期趋势与近 250 交易日长线趋势。"""
    stats = []
    for etf in etfs:
        pct = _premium(etf)
        if pct is None:
            continue
        rows = _history_rows(etf["code"])   # 新→旧
        # 剔除今日记录：均值与趋势基线用"截至上一交易日"口径，
        # 今日值来自实时行情（盘中模式本就不入库；复盘模式刚入库的今日收盘不参与历史比较）
        today_str = _beijing_now().strftime("%Y-%m-%d")
        hist_rows = [r for r in rows if r[0] != today_str]
        premiums = [r[3] for r in hist_rows]
        prices = [r[1] for r in hist_rows]
        navs = [r[2] for r in hist_rows]
        item = {
            "code": etf["code"],
            "name": etf["name"],
            "premium": pct,
            "price": etf["price"],
            "iopv": etf["iopv"],
        }
        # ── 连续折价天数（恐慌折价检测，2026-11-23）：今日溢价<0 + 历史连续折价天数；
        #    单日折价可能是 NAV(T-1) 滞后造成的假折价，连续 2 日+ 才视为有效恐慌信号 ──
        item["discount_days"] = 0
        if pct < config.PANIC_DISCOUNT_PREMIUM:
            days = 1
            for r in hist_rows:  # 新→旧（已剔除今日），从最近往前数连续折价
                if r[3] < config.PANIC_DISCOUNT_PREMIUM:
                    days += 1
                else:
                    break
            item["discount_days"] = days
        # ── 30/60/90 交易日溢价率均值 ──
        for w in WINDOW_PREMIUM:
            seg = premiums[:w]
            item["prem_%d" % w] = _avg(seg)
            item["prem_%d_n" % w] = len(seg)
        # ── 价格 30 日均（现价 vs 均价判断入场高低）──
        item["price_30"] = _avg(prices[:30])
        item["price_30_n"] = len(prices[:30])
        # ── 溢价近 90 交易日分位（方案B）：当前溢价在历史区间中的位置（0=最低，100=最高）；
        #    样本 < 10 天不显示（None），防止第一天就误报高位 ──
        win_pct = premiums[:PREM_PCTILE_WINDOW]
        item["prem_pctile"] = None
        item["prem_pctile_n"] = len(win_pct)
        if len(win_pct) >= PREM_PCTILE_MIN_N:
            below = sum(1 for p in win_pct if p < pct)
            # 当前溢价可能超出历史区间（创新高/新低），分位钳到 0~100
            item["prem_pctile"] = min(100, round(below / (len(win_pct) - 1) * 100))
        # ── 现价/净值近 30 交易日短期趋势：今日值 vs 窗口内最早一天 ──
        win_price = prices[:WINDOW_TREND]
        win_nav = navs[:WINDOW_TREND]
        base_price = win_price[-1] if len(win_price) > 1 else None
        base_nav = win_nav[-1] if len(win_nav) > 1 else None
        item["price_trend"] = _trend_str(etf["price"], base_price, len(win_price))
        item["nav_trend"] = _trend_str(etf["iopv"], base_nav, len(win_nav))
        # ── 现价/净值近 250 交易日长线趋势（一年牛熊格局，2026-11-04 新增）──
        win_price_250 = prices[:WINDOW_TREND_LONG]
        win_nav_250 = navs[:WINDOW_TREND_LONG]
        base_price_250 = win_price_250[-1] if len(win_price_250) > 1 else None
        base_nav_250 = win_nav_250[-1] if len(win_nav_250) > 1 else None
        item["price_trend_250"] = _trend_str(
            etf["price"], base_price_250, len(win_price_250), window=WINDOW_TREND_LONG)
        item["nav_trend_250"] = _trend_str(
            etf["iopv"], base_nav_250, len(win_nav_250), window=WINDOW_TREND_LONG)
        stats.append(item)
    return stats


def build_review_report(stats: list, trade_date: str, sample_days: int, market: list = None) -> str:
    """生成每日复盘报告 Markdown：决策摘要区（今日信号/整体贵贱/大盘位置）+ 美股大盘与汇率
    + 全量 ETF 列表（每股一行，手机端自动换行不横滑），按溢价率从低到高（最低在上），
    每只附溢价率 30/60/90 日均、溢价近90日分位、价格 30 日均（▲▼）、
    现价/净值近 30 日与 250 日趋势、高位警示、回归风险。"""
    lines = [f"### 📈 ETF每日复盘 ({trade_date})\n"]
    lines.append(
        f"> 全量 **{len(stats)}** 只｜历史样本 **{sample_days}** 个交易日｜溢价率从低到高（最低在上）\n"
    )

    # ── 决策摘要区（方案A）：先给结论，再给数据 ──
    tier1 = [s for s in stats if s["premium"] < BUY_PREMIUM_LINE]          # 一级（🟢 溢价<2%）
    tier2 = [s for s in stats if _tier2_flag(s)]                            # 二级（✅ 溢价稳·价低）
    above_mean = [s for s in stats
                  if s.get("prem_30") is not None and s["premium"] > s["prem_30"]]
    if tier1:
        lines.append(f"🔴 **今日信号**：一级 {len(tier1)} 只（{_names_list(tier1)}）— 优先关注，按日低/尾盘信号下单\n")
    elif tier2:
        lines.append(f"🟡 **今日信号**：无一级；二级 {len(tier2)} 只（{_names_list(tier2)}）— 溢价稳·价低，可小仓关注\n")
    else:
        lines.append("⚪ **今日信号**：无买入信号（无溢价<2%，也无溢价稳·价低标的）\n")
    lines.append(f"📋 **定投档位**：{dca_pacing_label(stats, len(tier1))}\n")
    n_rich = len(above_mean)
    if n_rich > len(stats) - n_rich:
        lines.append(f"💰 **整体贵贱**：{n_rich}/{len(stats)} 只溢价高于自身30日均，整体偏贵，追高需谨慎\n")
    elif n_rich < len(stats) - n_rich:
        lines.append(f"💰 **整体贵贱**：{n_rich}/{len(stats)} 只溢价高于自身30日均，整体偏便宜\n")
    else:
        lines.append(f"💰 **整体贵贱**：{n_rich}/{len(stats)} 只溢价高于自身30日均，多空均衡\n")
    if market:
        pos = [u for u in market if u["pctile"] is not None]
        if pos:
            seg = "｜".join(f"{u['name']} 分位 {u['pctile']}%" for u in pos)
            if any(u["pctile"] >= 70 for u in pos):
                advice = "⚠️ 大盘处近250日高位，新买入建议分批"
            elif any(u["pctile"] <= 30 for u in pos):
                advice = "✅ 大盘处近250日低位区，定投可正常执行"
            else:
                advice = "➖ 大盘处中位，按溢价信号执行"
            lines.append(f"🌡️ **大盘位置**：{seg}｜{advice}\n")
    # ── 市场阶段（2026-11-23 新增）：回撤定阶段，区分定投与大额抄底 ──
    idx_regime = [u for u in (market or [])
                  if u["code"] in ("usINX", "usNDX")
                  and u.get("drawdown") is not None
                  and u["drawdown"] <= config.REGIME_DRAWDOWN_SHALLOW]
    if idx_regime:
        segs = []
        deepest = None
        for u in idx_regime:
            label, advice = market_regime(u["drawdown"])
            segs.append(f"{u['name']} {label}({u['drawdown']:+.1f}%)")
            if deepest is None or u["drawdown"] < deepest[0]:
                deepest = (u["drawdown"], advice)
        lines.append(f"📊 **市场阶段**：{'｜'.join(segs)} → {deepest[1]}\n")
    # ── 恐慌折价（2026-11-23 新增）：溢价转负 = 极端恐慌标志，历史上接近大底 ──
    discounts = [s for s in stats if s.get("discount_days", 0) > 0]
    if discounts:
        names = "、".join(f"{s['name']}(连{s['discount_days']}日)" for s in discounts[:3])
        if len(discounts) > 3:
            names += f" 等{len(discounts)}只"
        lines.append(
            f"🧊 **恐慌折价**：{len(discounts)} 只溢价转负（{names}）— 极端恐慌标志，"
            f"2020-03 广发纳指盘中折价-12%、2022-10 折价-4.5% 均接近大底；"
            f"单日折价可能是 NAV 滞后假折价，连 2 日+ 才有效；折价可持多数周，"
            f"建议分批定投，勿一次性抄底\n"
        )

    items = []
    # 美股大盘与汇率段（每次报告必带大盘行情；抓取失败时显式占位，不静默缺席）
    if market:
        lines.append("🌐 **美股大盘与汇率（最新收盘）**\n")
        for u in market:
            pct_seg = f"｜分位 {u['pctile']}%" if u["pctile"] is not None else ""
            # 距高点回撤：仅在低于高点时显示（创新高时不显示，避免无意义噪音）
            dd_seg = f"｜距高点 {u['drawdown']:.1f}%" if u["drawdown"] < 0 else ""
            items.append(
                f"**{u['name']}** {u['point']:,.{u['decimals']}f}"
                f"｜30日 {u['t30']}｜120日 {u['t120']}｜250日 {u['t250']}{pct_seg}{dd_seg}"
            )
    else:
        lines.append("🌐 **美股大盘与汇率**：⚠️ 获取失败（网络异常），本次未附带\n")
    for idx, s in enumerate(sorted(stats, key=lambda x: x["premium"]), 1):
        # 🟢 = 一级（溢价率 < 2%）；✅ = 二级（溢价稳定 + 价格低位）
        flag = "🟢" if s["premium"] < BUY_PREMIUM_LINE else _tier2_flag(s)
        price_avg = _fmt_avg(s.get("price_30"), s.get("price_30_n", 0), 30)
        hl = _price_vs_avg_str(s["price"], s)  # 现价相对价格30日均高低符号
        hl_str = f" {hl}" if hl else ""
        # 溢价均 30/60/90：三个窗口值相同（含同为 —）时只显示一次，减少噪音
        m30 = _fmt_avg(s["prem_30"], s["prem_30_n"], 30)
        m60 = _fmt_avg(s["prem_60"], s["prem_60_n"], 60)
        m90 = _fmt_avg(s["prem_90"], s["prem_90_n"], 90)
        if s.get("prem_30") == s.get("prem_60") == s.get("prem_90"):
            means_str = m30
        else:
            means_str = f"{m30}/{m60}/{m90}"
        # 溢价分位（方案B）：近90日历史位置，样本 < 10 天不显示
        pctile_seg = ""
        if s.get("prem_pctile") is not None:
            pctile_seg = f"｜分位 {s['prem_pctile']}%"
        # 趋势段：样本 < 2 天无法判断时整段省略；30日=短线，250日=长线牛熊
        trend_str = ""
        if s["price_trend"] != "—" or s["nav_trend"] != "—":
            trend_str = f"｜现 {s['price_trend']}｜净 {s['nav_trend']}"
        trend250_str = ""
        if s["price_trend_250"] != "—" or s["nav_trend_250"] != "—":
            trend250_str = f"｜250日 现{s['price_trend_250']} 净{s['nav_trend_250']}"
        # 高位警示（方案D）：溢价分位 ≥ 90% → 历史高位，追高有回归风险
        warn_seg = ""
        if (s.get("prem_pctile") is not None
                and s["prem_pctile"] >= PREMIUM_HIGH_PCTILE):
            warn_seg = "｜⚠️溢价近90日高位"
        # 回归风险（方案E）：现溢价高于30日均时，收敛回均值将亏多少（亏损 > 0.3% 才提醒）
        reg_seg = ""
        prem_avg_val = s.get("prem_30")
        if prem_avg_val is not None and s["premium"] > prem_avg_val:
            loss = ((1 + prem_avg_val / 100) / (1 + s["premium"] / 100) - 1) * 100
            if loss <= -REGRESSION_LOSS_MIN:
                reg_seg = f"｜回归亏 {loss:.1f}%"
        # 均值/价均段：历史为空（样本不足）时整段省略，不显示 "—" 噪音
        means_seg = f" (均 {means_str})" if means_str != "—" else ""
        price_seg = f" (价均 {price_avg})" if price_avg != "—" else ""
        # 市场阶段标签（2026-11-23）：指数回调/深度回撤时附行尾（正常阶段为空串）
        regime_tag = regime_row_tag_for_etf(s["code"], market) if flag else ""
        items.append(
            f"**{idx}. {flag}{s['name']}**｜溢价 {s['premium']:+.2f}%{means_seg}{pctile_seg}｜"
            f"现价 {s['price']:.3f}{hl_str}{price_seg}{trend_str}{trend250_str}{warn_seg}{reg_seg}{regime_tag}"
        )
    # 每股之间：空一行 + 下划线隔离区（独占段落）；钉钉 markdown 单换行会被合并成一段
    lines.append(f"\n\n{ROW_SEPARATOR}\n\n".join(items))
    lines.append("")
    # 图例：与上面行同款下划线隔离区分块，每条图例单独成块，清爽可读
    legend = [
        "💡 🟢=溢价<2%｜✅=二级买入（溢价未高于30日均+现价不高于价均）",
        "(均)=溢价30/60/90交易日均值（截至上一交易日，三值相同合并显示）｜(价均)=价格30日均｜▲▼=现价高/低于价均",
        "现/净=近30日趋势，250日=近一年趋势（样本≥2天才显示）｜分位=溢价近90日历史位置，≥90%标⚠️高位｜回归亏=买入后溢价收敛回30日均的亏损",
        "样本不足标(n=)｜🌐=美股大盘与汇率最新收盘，30/120/250日=较N个交易日前涨跌，分位=点位近250日历史位置，距高点=较近2.5年高点回撤",
        "🐻/🔻=指数深度回撤/回调（定投照常；大额加仓分批或仅定投）｜🧊=恐慌折价（溢价<0，极端恐慌标志，连2日+有效）",
        "📋=本期定投档位（🔥加倍/🟢正常/🟡减半/⏸缓投，按整体溢价贵贱自动判定，只调节每期金额不停投）",
    ]
    lines.append(f"\n\n{ROW_SEPARATOR}\n\n".join(legend))
    return "\n".join(lines)


def build_intraday_report(etfs: list, stats: list, market: list = None) -> str:
    """生成盘中全量报告：决策摘要区（今日信号/整体贵贱/大盘位置）+ 美股大盘与汇率
    + 全量 ETF 列表（每股一行，手机端自动换行不横滑），按溢价率从低到高（最低在上），
    每只附溢价率30日均、溢价近90日分位、价格30日均（▲/▼ 高低标注）
    + 买入信号（一级：溢价<阈值 / 二级：溢价稳·价低）+ 高位警示 + 回归风险。"""
    mk = market_status_text()
    market_ok = mk["in_session"]
    is_near_close = False
    mins_to_close = 0
    if market_ok:
        now_cn = _beijing_now()
        mins_to_close = 15 * 60 - (now_cn.hour * 60 + now_cn.minute)
        if mins_to_close <= 30:
            is_near_close = True

    stat_map = {s["code"]: s for s in stats}

    # ── 买入窗口文案 ──
    if market_ok:
        if is_near_close:
            window = f"🛒 **买入窗口**：🟢 **交易中** — 距收盘仅 **{mins_to_close} 分钟**\n"
        else:
            h_left, m_left = divmod(mins_to_close, 60)
            window = f"🛒 **买入窗口**：🟢 **交易中** — 距收盘还有 **{h_left} 小时 {m_left} 分钟**\n"
    else:
        window = (
            f"🛒 **买入窗口**：{mk['status_emoji']} **{mk['status_label']}** — "
            f"下一窗口 **{mk['next_trading_day']}** 9:30–15:00\n"
        )

    # ── 逐只计算信号、日均、分位与风险段，按溢价率升序排列 ──
    rows = []
    for etf in etfs:
        if etf["price"] <= 0 or etf["iopv"] <= 0:
            continue
        premium = (etf["price"] - etf["iopv"]) / etf["iopv"] * 100
        threshold = config.get_etf_threshold(etf["code"])
        s = stat_map.get(etf["code"], {})
        signal, is_buy = _signal_with_premium_stability(
            etf, premium, threshold, market_ok, is_near_close, mins_to_close, s
        )
        prem_avg = _fmt_avg(s.get("prem_30"), s.get("prem_30_n", 0), 30)
        price_avg = _fmt_avg(s.get("price_30"), s.get("price_30_n", 0), 30)
        hl = _price_vs_avg_str(etf["price"], s)  # 现价相对价格30日均高低符号
        hl_str = f" {hl}" if hl else ""
        # 溢价分位（方案B）：近90日历史位置，样本 < 10 天不显示
        pctile = s.get("prem_pctile")
        # 高位警示（方案D）：溢价分位 ≥ 90% → 历史高位，追高有回归风险
        warn_seg = ""
        if pctile is not None and pctile >= PREMIUM_HIGH_PCTILE:
            warn_seg = "｜⚠️溢价近90日高位"
        # 回归风险（方案E）：现溢价高于30日均时，收敛回均值将亏多少（亏损 > 0.3% 才提醒）
        reg_seg = ""
        prem_avg_val = s.get("prem_30")
        if prem_avg_val is not None and premium > prem_avg_val:
            loss = ((1 + prem_avg_val / 100) / (1 + premium / 100) - 1) * 100
            if loss <= -REGRESSION_LOSS_MIN:
                reg_seg = f"｜回归亏 {loss:.1f}%"
        rows.append((premium, etf, prem_avg, price_avg, hl_str, signal, is_buy,
                     pctile, warn_seg, reg_seg))

    rows.sort(key=lambda item: item[0])

    lines = [
        f"### 📊 ETF盘中报告 ({_beijing_now().strftime('%m-%d %H:%M')})\n",
        f"> 全量 **{len(etfs)}** 只｜溢价率从低到高（最低在上）｜30日均=近30交易日均值（截至上一交易日）\n",
        window,
    ]

    # ── 决策摘要区（方案A，盘中版）：先给结论，再给数据 ──
    tier1 = [r for r in rows if r[6] and "稳·价低" not in r[5]]   # 一级：溢价<类目阈值
    tier2 = [r for r in rows if r[6] and "稳·价低" in r[5]]       # 二级：溢价稳·价低
    if tier1:
        names = _names_list([{"name": r[1]["name"]} for r in tier1])
        lines.append(f"🔴 **今日信号**：一级 {len(tier1)} 只（{names}）— 优先关注，按日低/尾盘信号下单\n")
    elif tier2:
        names = _names_list([{"name": r[1]["name"]} for r in tier2])
        lines.append(f"🟡 **今日信号**：无一级；二级 {len(tier2)} 只（{names}）— 溢价稳·价低，可小仓关注\n")
    else:
        lines.append("⚪ **今日信号**：无买入信号\n")
    lines.append(f"📋 **定投档位**：{dca_pacing_label(stats, len(tier1))}\n")
    above_mean = [s for s in stats
                  if s.get("prem_30") is not None and s["premium"] > s["prem_30"]]
    n_rich = len(above_mean)
    if stats:
        if n_rich > len(stats) - n_rich:
            lines.append(f"💰 **整体贵贱**：{n_rich}/{len(stats)} 只溢价高于自身30日均，整体偏贵，追高需谨慎\n")
        elif n_rich < len(stats) - n_rich:
            lines.append(f"💰 **整体贵贱**：{n_rich}/{len(stats)} 只溢价高于自身30日均，整体偏便宜\n")
        else:
            lines.append(f"💰 **整体贵贱**：{n_rich}/{len(stats)} 只溢价高于自身30日均，多空均衡\n")
    if market:
        pos = [u for u in market if u["pctile"] is not None]
        if pos:
            seg = "｜".join(f"{u['name']} 分位 {u['pctile']}%" for u in pos)
            if any(u["pctile"] >= 70 for u in pos):
                advice = "⚠️ 大盘处近250日高位，新买入建议分批"
            elif any(u["pctile"] <= 30 for u in pos):
                advice = "✅ 大盘处近250日低位区，定投可正常执行"
            else:
                advice = "➖ 大盘处中位，按溢价信号执行"
            lines.append(f"🌡️ **大盘位置**：{seg}｜{advice}\n")
    # ── 市场阶段（2026-11-23 新增）：回撤定阶段，区分定投与大额抄底 ──
    idx_regime = [u for u in (market or [])
                  if u["code"] in ("usINX", "usNDX")
                  and u.get("drawdown") is not None
                  and u["drawdown"] <= config.REGIME_DRAWDOWN_SHALLOW]
    if idx_regime:
        segs = []
        deepest = None
        for u in idx_regime:
            label, advice = market_regime(u["drawdown"])
            segs.append(f"{u['name']} {label}({u['drawdown']:+.1f}%)")
            if deepest is None or u["drawdown"] < deepest[0]:
                deepest = (u["drawdown"], advice)
        lines.append(f"📊 **市场阶段**：{'｜'.join(segs)} → {deepest[1]}\n")
    # ── 恐慌折价（2026-11-23 新增）：溢价转负 = 极端恐慌标志，历史上接近大底 ──
    discounts = [s for s in stats if s.get("discount_days", 0) > 0]
    if discounts:
        names = "、".join(f"{s['name']}(连{s['discount_days']}日)" for s in discounts[:3])
        if len(discounts) > 3:
            names += f" 等{len(discounts)}只"
        lines.append(
            f"🧊 **恐慌折价**：{len(discounts)} 只溢价转负（{names}）— 极端恐慌标志，"
            f"2020-03 广发纳指盘中折价-12%、2022-10 折价-4.5% 均接近大底；"
            f"单日折价可能是 NAV 滞后假折价，连 2 日+ 才有效；折价可持多数周，"
            f"建议分批定投，勿一次性抄底\n"
        )

    # 美股大盘与汇率段（每次报告必带大盘行情；抓取失败时显式占位，不静默缺席）
    idx_items = []
    if market:
        lines.append("🌐 **美股大盘与汇率（最新收盘）**\n")
        for u in market:
            pct_seg = f"｜分位 {u['pctile']}%" if u["pctile"] is not None else ""
            # 距高点回撤：仅在低于高点时显示（创新高时不显示，避免无意义噪音）
            dd_seg = f"｜距高点 {u['drawdown']:.1f}%" if u["drawdown"] < 0 else ""
            idx_items.append(
                f"**{u['name']}** {u['point']:,.{u['decimals']}f}"
                f"｜30日 {u['t30']}｜120日 {u['t120']}｜250日 {u['t250']}{pct_seg}{dd_seg}"
            )
    else:
        lines.append("🌐 **美股大盘与汇率**：⚠️ 获取失败（网络异常），本次未附带\n")

    # ── 列表格式（无表格）：每股一行，手机端自动换行不横滑；
    #    每股之间空一行 + 下划线隔离区（独占段落，钉钉 markdown 单换行会被合并）──
    items = []
    for idx, (premium, etf, prem_avg, price_avg, hl_str, signal, is_buy,
              pctile, warn_seg, reg_seg) in enumerate(rows, 1):
        # 信号后缀：可买入附信号；流动性差提示；其余不附
        if is_buy:
            suffix = f"｜{signal}"
        elif signal.startswith("⚠️"):
            suffix = f"｜{signal}"
        else:
            suffix = ""
        # 均值/价均段：历史为空（样本不足）时整段省略，不显示 "—" 噪音
        prem_seg = f" (均{prem_avg})" if prem_avg != "—" else ""
        price_seg = f" (价均{price_avg})" if price_avg != "—" else ""
        pctile_seg = f"｜分位 {pctile}%" if pctile is not None else ""
        # 市场阶段标签（2026-11-23）：可买入行附所属指数阶段提示（正常阶段为空串）
        regime_tag = regime_row_tag_for_etf(etf["code"], market) if is_buy else ""
        line = (
            f"**{idx}. {etf['name']}**｜溢价 {premium:+.2f}%{prem_seg}{pctile_seg}｜"
            f"现价 {etf['price']:.3f}{hl_str}{price_seg}{warn_seg}{reg_seg}{regime_tag}{suffix}"
        )
        if is_buy:
            line = f"<font color='#FF0000'>{line}</font>"  # 可买入行整行标红
        items.append(line)
    # 美股大盘行在前，ETF 行在后，统一用下划线隔离区连接
    lines.append(f"\n\n{ROW_SEPARATOR}\n\n".join(idx_items + items))

    lines.append("")
    # 图例：与上面行同款下划线隔离区分块，每条图例单独成块，清爽可读
    legend = [
        "💡 红色=可买入｜一级：溢价<阈值（🔥立即下手/🟢建议买入/🟡尽快决策/✅可买入）",
        "二级：✅溢价稳·价低=溢价未高于30日均+现价不高于价均",
        "▲▼=现价高/低于价均｜分位=溢价近90日历史位置，≥90%标⚠️高位｜回归亏=买入后溢价收敛回30日均的亏损",
        "样本不足标(n=)｜🌐=美股大盘与汇率最新收盘，30/120/250日=较N个交易日前涨跌，分位=点位近250日历史位置，距高点=较近2.5年高点回撤",
        "🐻/🔻=指数深度回撤/回调（定投照常；大额加仓分批或仅定投）｜🧊=恐慌折价（溢价<0，极端恐慌标志，连2日+有效）",
        "📋=本期定投档位（🔥加倍/🟢正常/🟡减半/⏸缓投，按整体溢价贵贱自动判定，只调节每期金额不停投）",
    ]
    lines.append(f"\n\n{ROW_SEPARATOR}\n\n".join(legend))
    return "\n".join(lines)


def main() -> dict:
    """主流程：抓取 → 交易日校验 → 入库 → 统计 → 报告 → 推送。"""
    dry_run = "--dry-run" in sys.argv
    run_id = datetime.now().strftime("%Y%m%d%H%M%S") + "-hist"
    log_event("info", run_id, "start", f"每日历史记录任务开始 dry_run={dry_run}")

    # 1. 抓取
    raw = fetch_etf_data(run_id=run_id)
    if not raw:
        log_event("error", run_id, "fetch", "行情数据为空，任务终止")
        return {"ok": False, "reason": "fetch_empty"}

    # 2. 交易日校验：行情时间戳日期 != 今天 → 节假日/周末过期数据，跳过（防止重复入库）
    now_cn = _beijing_now()
    today_str = now_cn.strftime("%Y-%m-%d")
    quote_date = _quote_trade_date(raw)
    if quote_date != now_cn.strftime("%Y%m%d"):
        log_event(
            "warning", run_id, "market",
            f"行情时间戳 {quote_date} != 今天 {now_cn.strftime('%Y%m%d')}，"
            f"节假日/周末过期数据，跳过记录与推送",
        )
        return {"ok": True, "reason": "stale_market_skip"}

    # 3. 解析 + 过期数据双保险
    etfs = parse_etf_data(raw)
    if not etfs:
        log_event("error", run_id, "parse", "无有效 ETF 数据，任务终止")
        return {"ok": False, "reason": "parse_empty"}
    if is_stale_data(etfs):
        log_event("warning", run_id, "market", "疑似休市过期数据（溢价率几乎全为 0），跳过")
        return {"ok": True, "reason": "stale_data_skip"}

    # 4. 入库（仅复盘模式；盘中模式不入库，历史库只存收盘价，避免盘中价污染均值）
    intraday = "--intraday" in sys.argv
    if intraday:
        log_event("info", run_id, "record", "盘中模式：跳过入库（历史库仅存收盘价）")
    else:
        init_db()
        written = record_daily(etfs, today_str)
        log_event("info", run_id, "record", f"已记录 {written} 只 ETF 到历史库（日期 {today_str}）")

    # 5. 统计 + 报告
    stats = build_stats(etfs)
    market = fetch_market_watch()  # 美股大盘+汇率：点位+30/120/250日趋势+分位（失败返回 []，报告跳过该段）
    if intraday:
        report = build_intraday_report(etfs, stats, market)
        push_title = "美股 ETF 定投盘中报告"
    else:
        sample_days = _total_sample_days()
        report = build_review_report(stats, today_str, sample_days, market)
        push_title = REVIEW_TITLE
    print(report)

    # 6. 推送（dry-run 仅打印）
    if dry_run:
        log_event("info", run_id, "notify", "dry-run 模式，跳过钉钉推送")
        return {"ok": True, "reason": "dry_run"}
    if not send_dingtalk(report, run_id=run_id, stage="notify-review", title=push_title):
        log_event("error", run_id, "notify", "钉钉报告推送失败")
        return {"ok": False, "reason": "notify_failed"}
    log_event("info", run_id, "finish", "报告已推送")
    return {"ok": True, "reason": "ok"}


if __name__ == "__main__":
    # 安全：非 TTY 环境（计划任务）不崩溃
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    result = main()
    sys.exit(0 if result.get("ok") else 1)
