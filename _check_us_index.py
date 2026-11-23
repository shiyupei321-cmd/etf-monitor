# 临时核对脚本：验证美股指数数据源（腾讯实时 + 腾讯美K线 + 东方财富K线），用完即删
import json
import urllib.request

UA = {"User-Agent": "Mozilla/5.0"}

# 1) 腾讯实时行情：测试各美股指数代码
print("=== 1) 腾讯实时 qt.gtimg.cn ===")
try:
    req = urllib.request.Request("http://qt.gtimg.cn/q=usDJI,usINX,usIXIC,usNDX", headers=UA)
    raw = urllib.request.urlopen(req, timeout=15).read().decode("gbk", errors="replace")
    for line in raw.split(";"):
        if "=" not in line:
            continue
        code, _, rest = line.partition("=")
        f = rest.strip('"').split("~")
        print(f"  {code}: 字段数={len(f)} 名称={f[1] if len(f) > 1 else '?'} 现价={f[3] if len(f) > 3 else '?'}")
except Exception as exc:
    print(f"  失败: {exc}")

# 2) 腾讯美股K线端点
print("=== 2) 腾讯美K线 usfqkline ===")
for code in ("usDJI", "usINX", "usIXIC", "usNDX"):
    try:
        url = f"http://web.ifzq.gtimg.cn/appstock/app/usfqkline/get?param={code},day,,,100,qfq"
        req = urllib.request.Request(url, headers=UA)
        data = json.loads(urllib.request.urlopen(req, timeout=15).read().decode("utf-8", errors="replace"))
        node = data.get("data", {}).get(code, {})
        keys = list(node.keys())
        print(f"  {code}: 键={keys}")
        for k in keys:
            rows = node[k]
            if isinstance(rows, list) and rows:
                print(f"    {k} 首条={rows[0][:2]} 末条={rows[-1][:2]}")
    except Exception as exc:
        print(f"  {code} 失败: {exc}")

# 3) 东方财富K线（带 ut 参数重试）
print("=== 3) 东方财富 push2his（带ut）===")
for secid, name in (("100.INX", "标普500"), ("100.NDX", "纳斯达克100"), ("100.DJIA", "道琼斯"), ("100.SPX", "标普SPX")):
    try:
        url = (f"http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
               f"&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5"
               f"&fields2=f51,f53&klt=101&fqt=1&end=20500101&lmt=10")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "http://quote.eastmoney.com/"})
        data = json.loads(urllib.request.urlopen(req, timeout=15).read().decode("utf-8"))
        d = data.get("data")
        print(f"  {secid} ({name}): data={'None' if d is None else '有'}")
        if d and d.get("klines"):
            print(f"    末条: {d['klines'][-1]}")
    except Exception as exc:
        print(f"  {secid} ({name}) 失败: {exc}")
