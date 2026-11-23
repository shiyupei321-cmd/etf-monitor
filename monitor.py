"""
ETF 定投监控 — 主脚本
功能：拉取腾讯行情接口 → 解析 15 只美股 ETF → 计算溢价率 → 流动性风控 → 钉钉推送

用法：python monitor.py
"""
import json
import sys
import time
import hmac
import uuid
import logging

# 安全：非 TTY 环境（计划任务）不崩溃
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
import hashlib
import base64
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone

import config  # 配置文件（ETF 代码 / 阈值 / 钉钉凭证）


# ══════════════════════════════════════════════════════════════
# 0. 日志与运行态辅助
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def _short_text(text: str, limit: int = 120) -> str:
    """截断过长文本，避免日志与告警刷屏。"""
    text = str(text).strip().replace("\n", " ")
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def is_market_hours() -> bool:
    """判断当前北京时间是否在 A 股交易时段内（9:30-15:00，周一至周五）。"""
    now_beijing = datetime.now(timezone.utc) + timedelta(hours=8)
    # 周末不交易
    if now_beijing.weekday() >= 5:
        return False
    # 交易时段 9:30-15:00
    now_minutes = now_beijing.hour * 60 + now_beijing.minute
    return 9 * 60 + 30 <= now_minutes <= 15 * 60


def is_stale_data(etfs: list[dict]) -> bool:
    """检测行情数据是否为休市过期数据（几乎所有 ETF 溢价率 ≈ 0%）。
    真实在交易时段，15 只美股 ETF 不可能全部无溢价。"""
    zero_premium_count = 0
    valid_count = 0
    for etf in etfs:
        if etf["price"] > 0 and etf["iopv"] > 0:
            valid_count += 1
            premium = abs((etf["price"] - etf["iopv"]) / etf["iopv"] * 100)
            if premium < 0.01:  # 溢价率几乎为 0
                zero_premium_count += 1
    if valid_count == 0:
        return True  # 没有任何有效数据，判定为过期
    # 超过 80% 的 ETF 无溢价 → 休市过期数据
    return (zero_premium_count / valid_count) > 0.8


def market_status_text() -> dict:
    """返回当前市场状态信息，包含交易时段提示与下一交易日。
    用于在报告顶部展示行情数据可用时段 & 交易时间窗口。"""
    now_beijing = datetime.now(timezone.utc) + timedelta(hours=8)
    weekday = now_beijing.weekday()
    hour = now_beijing.hour
    minute = now_beijing.minute
    now_minutes = hour * 60 + minute

    # ── 判断当前状态 ──
    is_weekday = weekday < 5
    in_session = is_weekday and (9 * 60 + 30 <= now_minutes <= 15 * 60)
    before_session = is_weekday and now_minutes < 9 * 60 + 30
    after_session = is_weekday and now_minutes > 15 * 60

    # ── 状态标签 ──
    if in_session:
        status_emoji = "🟢"
        status_label = "交易中"
        detail = "行情实时更新，交易时段 NAV 净值持续刷新"
    elif before_session:
        status_emoji = "🟡"
        status_label = "盘前等待"
        detail = f"行情数据 9:30 起可用，距开盘还有约 {9 * 60 + 30 - now_minutes} 分钟"
    elif after_session:
        status_emoji = "🔴"
        status_label = "已收盘"
        detail = "以下为收盘数据，仅供复盘参考"
    else:
        status_emoji = "⚫"
        status_label = "周末/节假日休市"
        detail = "以下为最近交易日收盘数据，不构成买入信号"

    # ── 下一交易日 ──
    next_day = now_beijing
    for _ in range(14):  # 最多往后找 14 天，防止死循环
        next_day = next_day + timedelta(days=1)
        if next_day.weekday() < 5:
            break
    next_str = next_day.strftime("%m月%d日（周%u") \
        .replace("周1", "周一").replace("周2", "周二").replace("周3", "周三") \
        .replace("周4", "周四").replace("周5", "周五") + "）"

    return {
        "status_emoji": status_emoji,
        "status_label": status_label,
        "detail": detail,
        "in_session": in_session,
        "next_trading_day": next_str,
        "now_str": now_beijing.strftime("%Y-%m-%d %H:%M"),
    }


def log_event(level: str, run_id: str, stage: str, message: str) -> None:
    """统一输出关键运行日志，便于 FC / s logs 检索。"""
    logger = logging.getLogger("etf-monitor")
    text = f"run_id={run_id} stage={stage} {message}"
    getattr(logger, level.lower(), logger.info)(text)


def build_failure_report(run_id: str, stage: str, reason: str, source: str) -> str:
    """构造失败告警 Markdown，底层仍复用钉钉发送函数。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reason = _short_text(reason, 300)
    return "\n".join([
        f"### ❌ ETF 定投监控失败告警 ({datetime.now().strftime('%Y-%m-%d')})",
        "",
        f"- **时间**: {now}",
        f"- **运行 ID**: `{run_id}`",
        f"- **触发来源**: `{source}`",
        f"- **失败阶段**: `{stage}`",
        f"- **错误摘要**: {reason}",
        "",
        "> 请优先查看 FC 执行日志中的同 run_id 记录。",
    ])


def notify_failure(run_id: str, stage: str, reason: str, source: str) -> bool:
    """发送失败告警；即使发送失败，也要把原因留在日志里。"""
    markdown = build_failure_report(run_id, stage, reason, source)
    log_event("error", run_id, stage, f"触发失败告警: {_short_text(reason, 160)}")
    ok = send_dingtalk(markdown, run_id=run_id, stage=stage)
    if ok:
        log_event("info", run_id, stage, "失败告警发送成功")
    else:
        log_event("error", run_id, stage, "失败告警发送失败，请查看钉钉凭证或网络")
    return ok


def fail_with_report(run_id: str, stage: str, reason: str, source: str) -> dict:
    """统一处理失败：记录日志、发告警、返回失败状态。"""
    log_event("error", run_id, stage, reason)
    notify_failure(run_id, stage, reason, source)
    return {"ok": False, "stage": stage, "reason": reason, "run_id": run_id, "source": source}


def build_context(event=None, source: str = "manual") -> dict:
    """构造一次运行上下文，统一 run_id 与触发来源。"""
    if isinstance(event, dict):
        for key in ("run_id", "runId"):
            value = event.get(key)
            if value:
                return {"run_id": str(value), "source": source, "event": event}
    return {
        "run_id": datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6],
        "source": source,
        "event": event,
    }


# ══════════════════════════════════════════════════════════════
# 1. 数据抓取
# ══════════════════════════════════════════════════════════════

def fetch_etf_data(run_id: str = "-") -> str:
    """调用腾讯行情接口，返回原始文本数据。"""
    try:
        req = urllib.request.Request(config.API_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        })
        with urllib.request.urlopen(req, timeout=config.API_TIMEOUT) as resp:
            # 腾讯接口返回 GBK 编码
            raw = resp.read()
            log_event("info", run_id, "fetch", f"行情接口返回 {len(raw)} 字节")
            # 先尝试 GBK，失败则用 UTF-8 兜底
            try:
                return raw.decode("gbk")
            except UnicodeDecodeError:
                log_event("warning", run_id, "fetch", "GBK 解码失败，改用 UTF-8 兜底")
                return raw.decode("utf-8", errors="replace")
    except Exception as e:
        log_event("error", run_id, "fetch", f"行情接口请求失败: {e}")
        return ""


# ══════════════════════════════════════════════════════════════
# 2. 数据解析
# ══════════════════════════════════════════════════════════════

def parse_etf_data(raw_data: str) -> list[dict]:
    """
    解析腾讯行情接口返回的文本数据。
    格式：多行，每行以 "v_<code>=" 开头，字段用 "~" 分隔。

    字段索引（腾讯接口实测验证，2026-07-21）：
      [1]  = 名称
      [3]  = 最新价
      [4]  = 今开（当日开盘价）
      [33] = 最高（当日最高价）
      [34] = 最低（当日最低价）
      [37] = 成交额（万元）
      [78] = NAV 基金净值（T-1 日官方净值，跨境 ETF 溢价率的标准基准）
             注意：不要与 IOPV 混淆。QDII 跨境 ETF 的 IOPV 在 A 股时段基于
             隔夜美股收盘价估算，参考价值有限；NAV 才是公认的折溢价计算基准
    """
    results = []
    lines = raw_data.split(";")
    for line in lines:
        if "=" not in line:
            continue
        # 提取代码：从 "v_sz159655=" 中取 "sz159655"
        code_part, _, data_part = line.partition("=")
        code = code_part.strip().removeprefix("v_")  # "v_sz159655" → "sz159655"
        if not data_part:
            continue
        fields = data_part.split("~")
        if len(fields) < 79:
            continue  # 字段不全，跳过

        name = fields[1].strip() if fields[1] else ""
        if not name:
            continue

        # 最新价 [3]
        try:
            price = float(fields[3])
        except (ValueError, TypeError):
            price = 0.0

        # 今开 [4]
        try:
            open_price = float(fields[4])
        except (ValueError, TypeError):
            open_price = 0.0

        # 今日最高 [33]
        try:
            high = float(fields[33])
        except (ValueError, TypeError):
            high = 0.0

        # 今日最低 [34]
        try:
            low = float(fields[34])
        except (ValueError, TypeError):
            low = 0.0

        # 成交额（万元）
        try:
            turnover = float(fields[37])
        except (ValueError, TypeError):
            turnover = 0.0

        # NAV 基金净值（索引 78，T-1 日官方净值；不是 IOPV，见注释）
        iopv_raw = fields[78] if len(fields) > 78 else ""
        try:
            iopv = float(iopv_raw) if iopv_raw and iopv_raw.strip() else 0.0
        except (ValueError, TypeError):
            iopv = 0.0
        if iopv <= 0:
            iopv = price  # 兜底：无 IOPV 时用现价（溢价率 = 0）

        results.append({
            "code": code,
            "name": name,
            "price": price,
            "open": open_price,
            "high": high,
            "low": low,
            "turnover": turnover,
            "iopv": iopv,
        })
    return results


# ══════════════════════════════════════════════════════════════
# 3. 溢价率计算 + 流动性风控 + Markdown 表格生成
# ══════════════════════════════════════════════════════════════

def build_report(etfs: list[dict]) -> tuple[bool, str]:
    """
    生成买入报告：全量 ETF 溢价率排行表（按溢价率从低到高，最低在上），
    可购买行整行标红并附信号分级（🔥立即下手/🟢建议买入/🟡尽快决策/✅可买入）。
    返回 (是否有可购买标的, 报告文本)。
    """
    mk = market_status_text()

    # ── 尾盘紧迫度 ──
    # 距收盘 ≤ 30 分钟时触发"尽快下手"提示
    is_near_close = False
    mins_to_close = 0
    if mk["in_session"]:
        now_beijing = datetime.now(timezone.utc) + timedelta(hours=8)
        mins_to_close = 15 * 60 - (now_beijing.hour * 60 + now_beijing.minute)
        if mins_to_close <= 30:
            is_near_close = True

    # ── 全量溢价率排行表（可买入筛选 + 信号分级在 _build_full_premium_table 中完成）──
    buyable, table = _build_full_premium_table(etfs)
    if not buyable:
        return False, ""

    buy_count = len(buyable)

    # ── 购买窗口（醒目区块）──
    if mk["in_session"]:
        now_beijing = datetime.now(timezone.utc) + timedelta(hours=8)
        mins_left = 15 * 60 - (now_beijing.hour * 60 + now_beijing.minute)
        hours_left = mins_left // 60
        mins_left = mins_left % 60
        if is_near_close:
            buy_window = (
                f"🛒 **买入窗口**：{mk['status_emoji']} **尾盘冲刺** — "
                f"距收盘仅 **{mins_left} 分钟**，有标的请果断下手！\n"
            )
        else:
            buy_window = (
                f"🛒 **买入窗口**：{mk['status_emoji']} **交易中** — "
                f"距收盘还有 **{hours_left} 小时 {mins_left} 分钟**，请及时操作\n"
            )
    elif mk["status_label"] == "盘前等待":
        buy_window = (
            f"🛒 **买入窗口**：{mk['status_emoji']} **盘前等待** — "
            f"{mk['detail']}\n"
        )
    else:
        buy_window = (
            f"🛒 **买入窗口**：{mk['status_emoji']} **{mk['status_label']}** — "
            f"下一窗口：**{mk['next_trading_day']} 9:30–15:00**\n"
        )

    # ── 交易时段提示 ──
    trading_info = (
        f"📊 行情刷新：交易日 9:30 起 | ⏰ 可交易时段：**9:30–15:00**（周一至周五）\n"
        f"💡 信号说明：📉近低点=划算 | ⏰尾盘=紧迫 | 🔥双信号叠加=最佳时机\n"
        f"{buy_window}\n"
    )

    # ── 分类统计 ──
    cate_sp500 = [e for e in buyable if e["code"] in ("sz159655", "sh513500", "sz159612", "sh513650")]
    cate_nasdaq = [e for e in buyable if e["code"] not in ("sz159655", "sh513500", "sz159612", "sh513650", "sz159509")]
    cate_tech = [e for e in buyable if e["code"] == "sz159509"]
    cate_parts = []
    if cate_sp500:
        cate_parts.append(f"标普500 {len(cate_sp500)} 只（阈值 2%）")
    if cate_nasdaq:
        cate_parts.append(f"纳指 {len(cate_nasdaq)} 只（阈值 4%）")
    if cate_tech:
        cate_parts.append(f"纳指科技 {len(cate_tech)} 只（阈值 8%）")
    cate_str = "、".join(cate_parts)

    title_line = (
        f"### 🚨 美股 ETF 绿灯定投扫描报告 ({datetime.now().strftime('%Y-%m-%d')})\n\n"
        f"{trading_info}\n"
        f"**可购买 {buy_count} 只** | {cate_str} | 全量排行（最低在上）\n\n"
    )
    return True, title_line + table


# ══════════════════════════════════════════════════════════════
# 4. 钉钉群机器人推送（HMAC-SHA256 加签）
# ══════════════════════════════════════════════════════════════

def send_dingtalk(markdown_text: str, run_id: str = "-", stage: str = "notify", title: str = None) -> bool:
    """
    通过钉钉群机器人 Webhook 发送 Markdown 消息。
    使用 HMAC-SHA256 加签（与 JD 项目共用同一签名模板）。
    title 为空时使用 config.DINGTALK_TITLE，可传入自定义标题（如每日复盘报告）。
    """
    missing = config.validate_runtime_config(require_notify=True)
    if missing:
        log_event("error", run_id, stage, f"未配置钉钉凭证: {', '.join(missing)}")
        return False

    timestamp = str(int(time.time() * 1000))
    sign_str = f"{timestamp}\n{config.DINGTALK_SECRET}"
    sign = base64.b64encode(
        hmac.new(
            config.DINGTALK_SECRET.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")

    url = (
        f"https://oapi.dingtalk.com/robot/send"
        f"?access_token={config.DINGTALK_TOKEN}"
        f"&timestamp={timestamp}"
        f"&sign={urllib.parse.quote(sign)}"
    )

    body = json.dumps({
        "msgtype": "markdown",
        "markdown": {
            "title": title or config.DINGTALK_TITLE,
            "text": markdown_text,
        },
    }, ensure_ascii=False).encode("utf-8")

    try:
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json"},
        )
        resp = json.loads(urllib.request.urlopen(req, timeout=10).read().decode("utf-8"))
        ok = resp.get("errcode") == 0
        if ok:
            log_event("info", run_id, stage, "钉钉推送成功")
        else:
            log_event("error", run_id, stage, f"钉钉推送失败: {resp.get('errmsg', resp)}")
        return ok
    except Exception as e:
        log_event("error", run_id, stage, f"钉钉请求异常: {e}")
        return False


# ══════════════════════════════════════════════════════════════
# 4.5 买入信号共用函数 + 全量溢价率排行表
# ══════════════════════════════════════════════════════════════

def format_turnover(turnover: float) -> tuple:
    """成交额展示（亿/万）与流动性是否健康。"""
    if turnover >= config.TURNOVER_BILLION:
        return f"{turnover / 10000:.2f} 亿", True
    if turnover >= config.TURNOVER_HEALTHY:
        return f"{turnover:.0f} 万", True
    return f"{turnover:.0f} 万", False


def compute_buy_signal(etf: dict, premium: float, threshold: float,
                       market_ok: bool, is_near_close: bool, mins_to_close: int) -> tuple:
    """计算单只 ETF 的买入信号与可买状态（云端告警与本机盘中报告共用）。
    返回 (信号文字, 是否可买入)。可买入信号：🔥立即下手/🟢建议买入/🟡尽快决策/✅可买入；
    不可买入时返回 ⚠️ 流动性差（流动性差）或 —。"""
    turnover = etf.get("turnover", 0)
    _, liquidity_ok = format_turnover(turnover)
    is_buy = market_ok and premium < threshold
    if not is_buy:
        return ("⚠️ 流动性差" if not liquidity_ok else "—"), False

    signal_parts = []
    near_low = False
    low_p = etf.get("low", 0)
    price = etf["price"]
    # 1. 接近日低判断（现价距日低 ≤ 0.5%）
    if low_p > 0 and price > 0:
        dist_from_low = (price - low_p) / low_p * 100
        if dist_from_low <= 0.5:
            near_low = True
            signal_parts.append(f"📉 距日低仅 {dist_from_low:.2f}%")
    # 2. 尾盘紧迫判断
    if is_near_close:
        signal_parts.append(f"⏰ 距收盘 {mins_to_close} 分钟")
    # 3. 流动性枯竭告警
    if not liquidity_ok:
        signal_parts.append("⚠️ 流动性差")

    if near_low and is_near_close:
        signal = "🔥 **立即下手**"
    elif near_low:
        signal = "🟢 **建议买入**"
    elif is_near_close:
        signal = "🟡 **尽快决策**"
    elif signal_parts:
        signal = " ".join(signal_parts)
    else:
        signal = "✅ 可买入"
    return signal, True


# ── 每股之间的下划线隔离区（用户要求：每条单独隔开；须独占一段，空行包裹）──
ROW_SEPARATOR = "_" * 40

# ── 美股大盘与汇率（腾讯K线；us* 用美K线端点，wh* 用普通K线端点）──
# 周期体系（2026-11-04 用户定稿）：30日=短线入场时机 / 120日=半年趋势 / 250日=一年牛熊格局；
# 60/90 与 30 高度重叠已删；分位 = 最新收盘处近 250 个交易日区间位置（0=最低，100=最高）。
MARKET_WATCH = {
    "usINX": {"name": "标普500", "decimals": 2},
    "usNDX": {"name": "纳斯达克100", "decimals": 2},
    "whUSDCNY": {"name": "美元兑人民币", "decimals": 4},
}
MARKET_TREND_WINDOWS = (30, 120, 250)  # 短/中/长三档趋势
MARKET_KLINE_DAYS = 600                # 拉满可用深度（约2.5年），够 250 日趋势 + 全量回撤计算
MARKET_PCT_WINDOW = 250                # 分位窗口：近 250 个交易日（一年位置；
                                       # 更长分位在长牛里会长期100%导致报警疲劳，多年深度交给回撤指标）


def _index_trend_str(pct) -> str:
    """涨跌幅 → 方向符号 + 百分比文案；None 返回 —（样本不足）。
    与 ETF 趋势判断一致：±0.5% 内视为持平 ➖。"""
    if pct is None:
        return "—"
    if pct > 0.5:
        emoji = "📈"
    elif pct < -0.5:
        emoji = "📉"
    else:
        emoji = "➖"
    return f"{emoji} {pct:+.1f}%"


def fetch_index_kline(code: str) -> list:
    """拉取指数日 K 线 [(日期, 收盘价), ...]（旧→新），endpoint 按 us*/wh* 自动选择。
    供 fetch_market_watch 与回测（按信号日市场阶段分桶）共用；失败重试 1 次后返回 []。
    大盘行情每次报告必带，因此抓取做了重试兜底。"""
    endpoint = "usfqkline" if code.startswith("us") else "fqkline"
    url = (f"http://web.ifzq.gtimg.cn/appstock/app/{endpoint}/get?"
           f"param={code},day,,,{MARKET_KLINE_DAYS},qfq")
    for _attempt in (1, 2):  # 失败重试 1 次
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
            rows = (data.get("data", {}).get(code) or {}).get("day") or []
            out = []
            for r in rows:
                try:
                    d = str(r[0])   # 行格式: [日期, 开, 收, 高, 低, 量, ...]
                    c = float(r[2])
                except (ValueError, TypeError, IndexError):
                    continue
                if c > 0:
                    out.append((d, c))
            return out
        except Exception:
            continue
    return []


def fetch_market_watch() -> list:
    """拉取美股大盘指数 + 美元兑人民币汇率：最新收盘 + 近 30/120/250 个交易日涨跌趋势
    + 近 250 日分位（点位在历史区间中的高低位置，判断贵不贵）
    + 距高点回撤（近 2.5 年最高收盘回撤幅度，判断回调深度）。

    数据源：腾讯 K 线（us* 用 usfqkline，wh* 用 fqkline，与 ETF 行情同源）。
    kline 最新一条 = 最近一个已收盘交易日（北京白天美股未开盘时即上一交易日收盘）。
    返回 [{"code","name","point","decimals","t30","t120","t250","pctile","drawdown"}, ...]；
    某只失败只跳过该只，全部失败返回 []，调用方跳过该段，不影响主报告。
    """
    out = []
    for code, meta in MARKET_WATCH.items():
        try:
            closes = [c for _d, c in fetch_index_kline(code)]
            if not closes:
                continue
            point = closes[-1]  # 最新收盘点位
            item = {"code": code, "name": meta["name"], "point": point,
                    "decimals": meta["decimals"]}
            for n in MARKET_TREND_WINDOWS:
                if len(closes) > n:
                    pct = (point - closes[-n - 1]) / closes[-n - 1] * 100
                    item[f"t{n}"] = _index_trend_str(pct)
                else:
                    item[f"t{n}"] = "—"
            # 分位：最新收盘处近 250 个交易日区间位置（样本 ≥ 30 天才算）
            win = closes[-MARKET_PCT_WINDOW:]
            item["pctile"] = None
            if len(win) >= 30 and len(win) > 1:
                below = sum(1 for c in win if c < point)
                # 当前点位可能超出历史区间（创新高/新低），分位钳到 0~100
                item["pctile"] = min(100, round(below / (len(win) - 1) * 100))
            # 距高点回撤：最新收盘 vs 全量数据（约2.5年）最高收盘的幅度。
            # 经典"52周高点回撤"指标的长周期版——判断回调深度 vs 熊市（行业共识 >20% 为技术性熊市），
            # 定投在深度回调时更有价值；创出新高时回撤为 0
            item["drawdown"] = round((point / max(closes) - 1) * 100, 1)
            out.append(item)
        except Exception:
            continue  # 单只失败不影响整体
    return out


# ── 市场阶段（regime）体系（2026-11-23 新增）────────────────────
# 背景：大跌中"溢价<阈值 + 现价<价均"会天天满足（价格30日均在下跌里永远▼），
# 信号一路响一路接飞刀；定投理论（Vanguard 2012/2023 研究）结论：
# 一次性投入仅在暴跌市跑输定投——大跌时定投照常（微笑曲线靠下跌段摊低成本），
# 大额抄底需额外条件（回调分批 / 熊市区仅定投+折价企稳确认）。
# 阶段划分依据 fetch_market_watch 的 drawdown（距近2.5年高点回撤）。

# ── ETF → 跟踪指数映射（市场阶段标签用）──
# 标普500 系 → usINX；纳指100 与纳指科技系 → usNDX（纳指科技与纳指100 高度同向）
ETF_INDEX_MAP = {
    "sz159655": "usINX", "sh513500": "usINX", "sz159612": "usINX", "sh513650": "usINX",
    "sh513100": "usNDX", "sz159941": "usNDX", "sh513300": "usNDX", "sz159632": "usNDX",
    "sz159501": "usNDX", "sz159659": "usNDX", "sz159660": "usNDX", "sz159513": "usNDX",
    "sh513390": "usNDX", "sh513110": "usNDX", "sz159509": "usNDX",
}


def market_regime(drawdown) -> tuple:
    """距高点回撤 → (阶段标签, 操作建议)。drawdown 为 None 时返回空标签。
    阶段划分（行业共识）：≤-5% 浅回调 / ≤-10% 回调 / ≤-20% 技术性熊市（深度回撤）。"""
    if drawdown is None:
        return "", ""
    if drawdown <= config.REGIME_DRAWDOWN_DEEP:
        return "🐻 深度回撤", "定投照常（微笑曲线核心段）；大额加仓需折价+企稳确认，勿接飞刀"
    if drawdown <= config.REGIME_DRAWDOWN_CORRECTION:
        return "🔻 回调", "定投照常；大额买入分 2-3 批"
    if drawdown <= config.REGIME_DRAWDOWN_SHALLOW:
        return "⚠️ 浅回调", "按溢价信号正常买入"
    return "🐂 正常", "按溢价信号正常买入"


def regime_row_tag_for_etf(code: str, market: list) -> str:
    """ETF 所属指数的阶段标签（附在可买入行尾部，防止把"低溢价"误读为"可满仓"）。
    指数正常/浅回调/数据缺失返回 ''（不添噪音）。"""
    idx_code = ETF_INDEX_MAP.get(code)
    if not idx_code or not market:
        return ""
    u = next((x for x in market if x["code"] == idx_code), None)
    if not u or u.get("drawdown") is None:
        return ""
    dd = u["drawdown"]
    if dd <= config.REGIME_DRAWDOWN_DEEP:
        return "｜🐻深度回撤·仅定投"
    if dd <= config.REGIME_DRAWDOWN_CORRECTION:
        return "｜🔻回调·定投可·大额分批"
    return ""


def _build_full_premium_table(etfs: list[dict]) -> tuple[list, str]:
    """构建全量 ETF 溢价率排行列表：按溢价率从低到高排列（最低在上），
    每股一行（无表格，手机端自动换行不横滑），每股之间用下划线隔离区分隔
    （用户要求：每条单独隔开）。可买入行整行标红并附信号分级
    （🔥立即下手/🟢建议买入/🟡尽快决策/✅可买入）；非可买入行不附信号
    （流动性差的显示 ⚠️ 流动性差）。
    返回 (可买入 ETF 列表, 列表 Markdown)。"""
    mk = market_status_text()
    market_ok = mk["in_session"]  # 休市时不下买入信号（报告另附休市警告）

    # ── 尾盘紧迫度（距收盘 ≤ 30 分钟触发）──
    is_near_close = False
    mins_to_close = 0
    if market_ok:
        now_beijing = datetime.now(timezone.utc) + timedelta(hours=8)
        mins_to_close = 15 * 60 - (now_beijing.hour * 60 + now_beijing.minute)
        if mins_to_close <= 30:
            is_near_close = True

    buyable = []
    rows_data = []
    for etf in etfs:
        if etf["price"] <= 0 or etf["iopv"] <= 0:
            continue
        premium = (etf["price"] - etf["iopv"]) / etf["iopv"] * 100
        threshold = config.get_etf_threshold(etf["code"])
        open_p = etf.get("open", 0)
        turnover = etf.get("turnover", 0)

        # ── 今开 / 流动性展示 ──
        open_str = f"{open_p:.3f}" if open_p > 0 else "—"
        turnover_display, _liquidity = format_turnover(turnover)

        # ── 买入信号判断（逻辑见 compute_buy_signal，本机与云端共用）──
        signal, is_buy = compute_buy_signal(
            etf, premium, threshold, market_ok, is_near_close, mins_to_close
        )
        if is_buy:
            buyable.append({**etf, "premium": premium, "threshold": threshold})

        rows_data.append({
            "premium": premium, "etf": etf, "threshold": threshold,
            "buyable": is_buy, "signal": signal, "open_str": open_str,
            "turnover_display": turnover_display,
        })

    # ── 按溢价率从低到高排列（最低在上，用户要求）──
    rows_data.sort(key=lambda item: item["premium"])

    # ── 列表格式（无表格）：每股一行，手机端自动换行不横滑；
    #    每股之间空一行 + 下划线隔离区（独占段落，钉钉 markdown 单换行会被合并）──
    lines = []
    items = []
    market = fetch_market_watch()
    # ── 市场阶段提示（2026-11-23 新增）：指数处回调/熊市阶段时提醒定投照常、大额分批 ──
    if market:
        idx_regime = [u for u in market
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
    # 美股大盘与汇率段：点位 + 30/120/250 日趋势 + 近250日分位（失败返回 []，跳过该段）
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
        # 大盘行情每次报告必带：抓取失败也显式占位，不静默缺席
        lines.append("🌐 **美股大盘与汇率**：⚠️ 获取失败（网络异常），本次未附带\n")
    lines.append(f"**全量溢价率排行（{len(rows_data)} 只，最低在上）**：\n")
    for idx, item in enumerate(rows_data, 1):
        etf = item["etf"]
        # 信号后缀：可买入附信号；流动性差提示；其余不附
        if item["buyable"]:
            suffix = f"｜{item['signal']}"
        elif item["signal"].startswith("⚠️"):
            suffix = f"｜{item['signal']}"
        else:
            suffix = ""
        # 市场阶段标签（2026-11-23）：可买入行附所属指数阶段提示（正常阶段为空串）
        regime_tag = regime_row_tag_for_etf(etf["code"], market) if item["buyable"] else ""
        line = (
            f"**{idx}. {etf['name']}**｜溢价 {item['premium']:+.2f}%｜"
            f"现价 {etf['price']:.3f}｜今开 {item['open_str']}｜"
            f"成交额 {item['turnover_display']}{regime_tag}{suffix}"
        )
        if item["buyable"]:
            line = f"<font color='#FF0000'>{line}</font>"  # 整行标红（钉钉 Markdown 支持 font 标签）
        items.append(line)
    lines.append(f"\n\n{ROW_SEPARATOR}\n\n".join(items))
    table = "\n".join(lines)
    return buyable, table


# ══════════════════════════════════════════════════════════════
# 5. 主入口
# ══════════════════════════════════════════════════════════════

def main(run_context=None) -> dict:
    """主流程：抓取 → 解析 → 生成报告 → 推送。"""
    ctx = run_context or build_context(source="local")
    run_id = ctx["run_id"]
    source = ctx.get("source", "local")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_event("info", run_id, "start", f"任务开始 source={source} at={now}")
    config.emit_runtime_config_warning(require_notify=False)

    print(f"╔══════════════════════════════════════════════╗")
    print(f"║  ETF 定投监控  {now}  ║")
    print(f"╚══════════════════════════════════════════════╝")
    print(f"  监控池: {len(config.ETF_CODES.split(','))} 只 ETF")
    print(f"  分类阈值: 标普500<2% | 纳指<4% | 纳指科技<8%")
    print(f"  流动性: ≥ {config.TURNOVER_HEALTHY:.0f} 万健康 | ≥ {config.TURNOVER_BILLION:.0f} 万充沛")

    # ── 交易时段提示 ──
    mk = market_status_text()
    print(f"  行情: 交易日 9:30 起 | 交易窗口: 9:30–15:00")
    if mk["in_session"]:
        now_beijing = datetime.now(timezone.utc) + timedelta(hours=8)
        mins_left = 15 * 60 - (now_beijing.hour * 60 + now_beijing.minute)
        print(f"  🛒 买入窗口: 🟢 交易中 — 距收盘还有 {mins_left // 60}h{mins_left % 60}m")
    else:
        print(f"  🛒 买入窗口: {mk['status_emoji']} {mk['status_label']} — 下一窗口 {mk['next_trading_day']}")
    print()

    # 1. 抓取
    print("[1/3] 拉取行情数据...")
    log_event("info", run_id, "fetch", "开始拉取行情数据")
    raw = fetch_etf_data(run_id=run_id)
    if not raw:
        return fail_with_report(run_id, "fetch", "行情数据为空，请检查 API 或网络", source)
    print(f"  原始数据 {len(raw)} 字节")

    # 2. 解析
    print("[2/3] 解析数据...")
    etfs = parse_etf_data(raw)
    log_event("info", run_id, "parse", f"解析出 {len(etfs)} 只 ETF")
    print(f"  解析出 {len(etfs)} 只 ETF")
    if not etfs:
        return fail_with_report(run_id, "parse", "无有效 ETF 数据", source)

    # 打印全部行情概览（非交易时段不显示"可购买"误导信号）
    market_ok = mk["in_session"]  # 复用头部已计算的市场状态
    print(f"  {'代码':<12} {'名称':<16} {'现价':>8} {'今开':>8} {'日低':>8} {'日高':>8} {'溢价':>7} {'阈值':>6} {'状态'}")
    print(f"  {'─'*12} {'─'*16} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*7} {'─'*6} {'─'*14}")
    for etf in etfs:
        if etf["price"] > 0 and etf["iopv"] > 0:
            pct = (etf["price"] - etf["iopv"]) / etf["iopv"] * 100
            threshold = config.get_etf_threshold(etf["code"])
            open_p = etf.get("open", 0)
            low_p = etf.get("low", 0)
            high_p = etf.get("high", 0)
            open_str = f"{open_p:.3f}" if open_p > 0 else "—"
            low_str = f"{low_p:.3f}" if low_p > 0 else "—"
            high_str = f"{high_p:.3f}" if high_p > 0 else "—"
            # 距日低距离
            if low_p > 0 and etf["price"] > 0:
                dist_low = (etf["price"] - low_p) / low_p * 100
                low_hint = f"距低{dist_low:.1f}%" if dist_low <= 1.0 else ""
            else:
                low_hint = ""
            if market_ok and pct < threshold:
                status = f"✅ 可购买 {low_hint}"
            elif not market_ok:
                status = "—（休市）"
            else:
                status = "—"
            print(f"  {etf['code']:<12} {etf['name']:<16} {etf['price']:>8.3f} {open_str:>8} {low_str:>8} {high_str:>8} {pct:>6.2f}%  <{threshold}%  {status}")

    # 3. 报告 + 推送（含市场状态检测）
    print("\n[3/3] 生成报告 + 推送...")

    # ── 市场状态检测：避免休市时推送假的可购买信号 ──
    data_stale = is_stale_data(etfs)
    skip_buy_signal = (not market_ok) or data_stale

    if skip_buy_signal:
        if not market_ok:
            log_event("warning", run_id, "market", "当前不在 A 股交易时段，可能休市")
            print("  ⚠️ 当前不在交易时段（9:30-15:00 工作日），可能为节假日休市")
        if data_stale:
            log_event("warning", run_id, "market", "检测到疑似过期数据（几乎所有 ETF 无溢价），强制发观察报告")
            print("  ⚠️ 数据无溢价波动，疑似休市过期数据")

    if not skip_buy_signal:
        has_data, table = build_report(etfs)
    else:
        has_data, table = False, ""  # 跳过买入报告，强制走观察报告

    if has_data:
        log_event("info", run_id, "report", "命中买入报告，准备推送")
        print(table)
        if not send_dingtalk(table, run_id=run_id, stage="notify-report"):
            return fail_with_report(run_id, "notify-report", "买入报告推送失败", source)
        summary = "已推送买入报告"
    else:
        # 2026-11-04：云端改为仅推送买入告警；全量报告（含30日均值）由本机计划任务
        # 10:00/14:00/14:30/15:05 推送，避免重复推送
        log_event("info", run_id, "report", "无达标标的，跳过推送（全量报告由本机任务推送）")
        print("  无达标标的：云端仅保留买入告警，全量报告由本机任务推送，本次不推送")
        summary = "无达标标的，未推送"

    log_event("info", run_id, "finish", f"任务完成 status=SUCCESS summary={summary}")
    return {"ok": True, "stage": "finish", "reason": "", "run_id": run_id, "source": source, "summary": summary}


# ── 阿里云 FC 入口函数 ────────────────────────────────────
def handler(event, context):
    """阿里云函数计算标准入口，直接调用主流程。"""
    source = "fc-timer"
    if isinstance(event, dict) and event.get("source"):
        source = str(event.get("source"))
    ctx = build_context(event=event, source=source)
    run_id = ctx["run_id"]
    try:
        result = main(ctx)
        status_code = 200 if result.get("ok") else 500
        if not result.get("ok"):
            log_event("error", run_id, result.get("stage", "finish"), f"任务结束 status=FAILED reason={_short_text(result.get('reason', ''), 160)}")
        return {"statusCode": status_code, "body": json.dumps(result, ensure_ascii=False)}
    except Exception as e:
        reason = f"未捕获异常: {e}"
        log_event("error", run_id, "unexpected", reason)
        notify_failure(run_id, "unexpected", reason, source)
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "stage": "unexpected", "reason": reason, "run_id": run_id, "source": source}, ensure_ascii=False),
        }


# ── 本地测试入口 ──────────────────────────────────────────
if __name__ == "__main__":
    result = main(build_context(source="local-cli"))
    if not result.get("ok"):
        raise SystemExit(1)
