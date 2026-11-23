"""
ETF 定投监控 — 配置文件
美股核心宽基监控池（4 只标普 500 + 11 只纳指相关）+ API 参数
"""
import os
import sys
from pathlib import Path

# ── 加载 .env 配置 ──────────────────────────────────────────
_ENV_FILE = Path(__file__).parent / ".env"
if _ENV_FILE.exists():
    with open(_ENV_FILE, "r", encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                # 去掉行内注释（# 后面的部分）
                if "#" in _v:
                    _v = _v.split("#")[0].strip()
                # .env 文件优先于系统环境变量，避免其他项目 Token 混用
                if _k:
                    os.environ[_k] = _v

# ── 美股 ETF 监控池（腾讯行情接口代码格式）──────────────────
# sz159655 = 标普 500 ETF, sh513500 = 标普 500 ETF, 等等
# 2026-11-04：剔除 sz159687 亚太精选ETF南方（非标普500/纳指，用户要求移除）
ETF_CODES = (
    "sz159655,sh513500,sz159612,sh513650,"   # 4 只标普 500
    "sh513100,sz159941,sh513300,sz159632,"   # 纳指相关 1-4
    "sz159501,sz159509,sz159659,sz159660,"   # 纳指相关 5-8
    "sz159513,sh513390,sh513110"             # 纳指相关 9-11
)

# ── 腾讯行情 API ───────────────────────────────────────────
API_URL = f"http://qt.gtimg.cn/q={ETF_CODES}"
API_TIMEOUT = 15  # 请求超时（秒）

# ── 业务阈值（优先读环境变量，缺省用硬编码兜底）────────────
TARGET_PREMIUM = float(os.environ.get("TARGET_PREMIUM", "3.0"))
TURNOVER_BILLION = float(os.environ.get("TURNOVER_BILLION", "10000"))   # 亿元线
TURNOVER_HEALTHY = float(os.environ.get("TURNOVER_HEALTHY", "500"))     # 健康线

# ── 分 ETF 溢价率阈值（%）──────────────────────────────────
# 不同 ETF 的正常溢价区间差异巨大（标普500 2-3% vs 纳指科技 15%），
# 单一阈值无法有效筛选。未配置的 ETF 使用 TARGET_PREMIUM 兜底。
ETF_PREMIUM_THRESHOLDS = {
    # 标普500 — 溢价通常 2-4%，跌破 2% 即为低估
    "sz159655": 2.0,
    "sh513500": 2.0,
    "sz159612": 2.0,
    "sh513650": 2.0,
    # 纳指100 — 溢价通常 4-8%，跌破 4% 即为低估
    "sh513100": 4.0,
    "sz159941": 4.0,
    "sh513300": 4.0,
    "sz159632": 4.0,
    "sz159501": 4.0,
    "sz159659": 4.0,
    "sz159660": 4.0,
    "sz159513": 4.0,
    "sh513390": 4.0,
    "sh513110": 4.0,
    # 纳指科技 — 溢价极高（10-20%），设 8% 阈值
    "sz159509": 8.0,
}


def get_etf_threshold(code: str) -> float:
    """获取指定 ETF 的溢价率阈值，未配置则返回全局默认值。"""
    return ETF_PREMIUM_THRESHOLDS.get(code, TARGET_PREMIUM)


# ── 市场阶段（regime）与恐慌折价（2026-11-23 新增）────────────────
# 用"距近2.5年高点回撤"划分市场阶段（行业共识：-10% 回调、-20% 技术性熊市）。
# 大跌不等于停止定投：定投微笑曲线的收益核心恰来自下跌段摊低成本，
# 阶段标签只约束"大额抄底"（回调分批、熊市区仅定投），不约束"按计划定投"。
REGIME_DRAWDOWN_DEEP = -20.0        # 深度回撤线（≤ -20% = 技术性熊市区）
REGIME_DRAWDOWN_CORRECTION = -10.0  # 回调线（≤ -10% = 回调区）
REGIME_DRAWDOWN_SHALLOW = -5.0      # 浅回调线（≤ -5% = 浅回调区）
PANIC_DISCOUNT_PREMIUM = 0.0        # 恐慌折价线：溢价率 < 0% = 场内价低于净值（极端恐慌标志）
# 历史参照：2020-03 广发纳指盘中折价 -12%、2022-10 折价 -4.5% 持续3周、2025-04 易方达纳指盘中 -6.7%，
# 折价区历来接近大底；但单日折价可能是 NAV(T-1) 滞后造成的假折价，连续 2 日+ 才视为有效。


# ── 钉钉推送 ───────────────────────────────────────────────
DINGTALK_TOKEN = os.environ.get("DINGTALK_TOKEN", "")
DINGTALK_SECRET = os.environ.get("DINGTALK_SECRET", "")
DINGTALK_TITLE = "美股 ETF 定投雷达报告"  # 含"ETF"关键字以穿透钉钉拦截


# ── 配置校验与诊断 ─────────────────────────────────────────
def validate_runtime_config(require_notify: bool = False) -> list[str]:
    """校验运行期关键配置，返回缺失项列表。"""
    missing = []
    if require_notify:
        if not DINGTALK_TOKEN:
            missing.append("DINGTALK_TOKEN")
        if not DINGTALK_SECRET:
            missing.append("DINGTALK_SECRET")
    return missing


def emit_runtime_config_warning(require_notify: bool = False) -> list[str]:
    """输出配置缺失诊断日志，避免任务静默失败。"""
    missing = validate_runtime_config(require_notify=require_notify)
    if missing:
        print(
            "[配置] 缺少关键环境变量: "
            + ", ".join(missing)
            + "；请在运行环境中注入，勿在仓库中明文保存。",
            file=sys.stderr,
        )
    return missing
