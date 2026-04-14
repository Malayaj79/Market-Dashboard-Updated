"""
Market Themes Momentum Dashboard — Data Fetcher v4
===================================================
RULES:
  - Every stock appears in exactly ONE theme (no cross-theme duplication)
  - 5-6 stocks per theme
  - All stocks have $10M+ average daily dollar volume
  - Each stock is a pure-play or primary revenue driver for that theme

SCORING:
  Composite Score = RetBlend×35% + RSBlend×30% + Resilience×20% + Breadth×15%
  Fully data-driven — no manual MA signal input
  RetBlend / RSBlend = 1D×20% + 1W×35% + 1M×45%
  Resilience = avg theme return on SPY red days minus avg SPY return (last 20 sessions)
  Breadth    = % of constituents above 20-day MA (0–10 scale)
  ADR weight = 1/ADR% so high-vol small caps don't dominate the signal

  1D = current price vs previous session close
  1W = today vs exactly 7 calendar days ago
  1M = today vs exactly 30 calendar days ago
"""

import json, datetime, time, urllib.request, os

# ── STOCK ASSIGNMENT RULES ────────────────────────────────────────────────────
# Each ticker lives in ONE theme only. Assignment is by primary revenue driver.
# NVDA → ai (biggest AI revenue driver)
# ARM  → semi (chip IP architecture)
# MRVL → memory (storage controllers)
# NET  → cyber (zero-trust + security, not CDN)
# GOOGL→ cloud (GCP is primary swing driver vs peers)
# MSFT → cloud (Azure is primary swing driver)
# AMZN → cloud (AWS is primary swing driver)
# TSLA → autonomouv (FSD/robo-taxi is primary catalyst now)
# ETN  → grid (transformers/power mgmt is primary)
# VRT  → datacntr (data center cooling/power is primary)
# ISRG → meddevice (surgical robot = medical device)
# ALB  → battery (lithium = battery input, not mineral)
# CCJ  → uranium (pure-play miner, not nuclear utility)
# PLTR → agenticai (AIP platform = agentic enterprise AI)
# META → arvr (Reality Labs + Quest = XR primary catalyst)
# WPM  → silver (streaming model, primary metal is silver)
# GLD  → gold (physical gold ETF)
# SNOW → aiinfra (data cloud = AI pipeline infra)
# PATH → robotics (RPA = automation)
# ONTO → semiequip (process control = equipment)
# CAT  → machinery (heavy equipment = primary)
# DE   → machinery (farm/construction equipment)
# NUE  → steel (largest US steelmaker)
# GTLB → saas (DevSecOps SaaS platform)
# FNV  → gold (royalty = gold exposure)
# PPLT → pgm (physical platinum)
# PALL → pgm (physical palladium)
# SLV  → silver (physical silver ETF)

THEMES = [
  # Format: (id, name, short, icon, sector, constituents, color)
  # All metrics computed purely from constituent stocks — no ETF proxy

  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — AI
  # ═══════════════════════════════════════════════════════════════════
  ("ai",        "Artificial Intelligence",     "AI",          "⚡","Technology",
   ["NVDA","PLTR","AI","MSFT","META","GOOGL"],                   "#00d4ff"),

  ("agenticai", "Agentic AI",                  "Agentic AI",  "🧠","Technology",
   ["CRM","ORCL","PEGA","GTLB","SOUN","BBAI"],                   "#818cf8"),

  ("edgecomp",  "Edge Computing",              "Edge Compute","🖥","Technology",
   ["AMBA","QCOM","AKAM","FSLY","OSS","ADI"],                   "#a78bfa"),

  ("aiinfra",   "AI Infrastructure Software",  "AI Infra SW", "🗄","Technology",
   ["DDOG","MDB","HASHI","NEWR","CFLT","NTNX"],                 "#60a5fa"),

  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — SEMICONDUCTORS
  # ═══════════════════════════════════════════════════════════════════
  ("semi",      "Semiconductors & Chips",      "Semis",       "💎","Technology",
   ["AMD","AVGO","ARM","TSM","ASML","LRCX"],                    "#6366f1"),

  ("semiequip", "Semiconductor Equipment",     "Semi Equip",  "🔭","Technology",
   ["AMAT","KLAC","ONTO","TER","UCTT","MKSI"],                  "#4f46e5"),

  ("memory",    "Memory & Data Storage",       "Memory",      "💾","Memory & Storage",
   ["SNDK","WDC","STX","MU","MRVL","NTAP"],                    "#38bdf8"),

  ("fiber",     "Fiber Optics & Optical Net.", "Fiber Optics","🔆","Fiber Optics",
   ["AAOI","LITE","COHR","CIEN","VIAV","FNSR"],                 "#bbf7d0"),

  ("datacntr",  "Data Centers & Infra",        "DataCtrs",    "🏭","Technology",
   ["EQIX","DLR","SMCI","VRT","DELL","NXDT"],                  "#fca5a5"),

  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — SOFTWARE
  # ═══════════════════════════════════════════════════════════════════
  ("cloud",     "Cloud Computing",             "Cloud",       "☁","Technology",
   ["AMZN","DOCN","SNOW","WDAY","PSTG","ESTC"],                "#93c5fd"),

  ("cyber",     "Cybersecurity",               "Cyber",       "🔐","Technology",
   ["CRWD","PANW","FTNT","S","OKTA","ZS"],                     "#22d3ee"),

  ("quantum",   "Quantum Computing",           "Quantum",     "⚛","Technology",
   ["IONQ","RGTI","QUBT","QBTS","HON","IBM"],                  "#e879f9"),

  ("robotics",  "Robotics & Automation",       "Robots",      "🤖","Technology",
   ["ISRG","ROK","PATH","BRZE","KUKA","FANUC"],                "#c084fc"),

  ("saas",      "Software as a Service",       "SaaS",        "🧩","Technology",
   ["NOW","HUBS","BILL","INTU","VEEV","ZM"],                   "#6ee7b7"),

  ("cdn",       "CDN & Edge Delivery",         "CDN/Edge",    "🌐","Technology",
   ["EGIO","FFIV","ZAYO","LUMN","CCOI","ATNI"],                "#67e8f9"),

  ("iot",       "Internet of Things",          "IoT",         "📡","Technology",
   ["TXN","SWKS","SLAB","SMTC","MCHP","NXPI"],                "#7dd3fc"),

  ("print3d",   "3D Printing",                 "3D Print",    "🖨","Technology",
   ["DDD","SSYS","XMTR","MTLS","MKFG","NNDM"],                "#7c3aed"),

  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — CONSUMER
  # ═══════════════════════════════════════════════════════════════════
  ("arvr",      "AR / VR & Spatial Computing", "AR/VR",       "🥽","Consumer Tech",
   ["RBLX","SNAP","IMMR","UNITY","U","AAPL"],                  "#f472b6"),

  ("autonomouv","Autonomous Vehicles",          "Auto Vehicles","🚗","Consumer Tech",
   ["TSLA","MBLY","LAZR","OUST","APTV","MOBILEYE"],            "#34d399"),

  # ═══════════════════════════════════════════════════════════════════
  # HEALTHCARE
  # ═══════════════════════════════════════════════════════════════════
  ("aidrug",    "AI Drug Discovery",           "AI Drug",     "🧬","Healthcare",
   ["RXRX","SDGR","ABCL","EXAI","INSM","CRVS"],               "#10b981"),

  ("meddevice", "Medical Devices",             "Med Devices", "🏥","Healthcare",
   ["EW","SYK","INSP","TNDM","NVCR","ALGN"],                  "#3b82f6"),

  ("glp1",      "GLP-1 & Obesity Drugs",       "GLP-1",       "💊","Healthcare",
   ["NVO","LLY","VKTX","HIMS","AMGN","ZFOX"],                 "#8b5cf6"),

  # ═══════════════════════════════════════════════════════════════════
  # FINTECH
  # ═══════════════════════════════════════════════════════════════════
  ("fintech",   "Digital Payments & Fintech",  "Fintech",     "💳","Fintech",
   ["SQ","AFRM","SOFI","NU","UPST","HOOD"],                   "#f97316"),

  # ═══════════════════════════════════════════════════════════════════
  # ENERGY
  # ═══════════════════════════════════════════════════════════════════
  ("nuclear",   "Nuclear Energy",              "Nuclear",     "☢","Energy",
   ["CEG","VST","TLN","BWXT","SMR","ETR"],                    "#fde68a"),

  ("uranium",   "Uranium Mining",              "Uranium",     "🪨","Energy",
   ["CCJ","NXE","DNN","UUUU","URG","UEC"],                    "#f59e0b"),

  ("grid",      "Power Grid Modernization",    "Grid",        "🔌","Energy",
   ["ETN","EMR","HUBB","PWR","GEV","AMPS"],                   "#86efac"),

  ("solar",     "Solar Energy",                "Solar",       "☀","Energy",
   ["ENPH","FSLR","SEDG","ARRY","CSIQ","MAXN"],               "#fbbf24"),

  ("clean",     "Wind & Renewable Energy",     "Wind/Renew",  "🌱","Energy",
   ["NEE","BEP","AES","CWEN","RUN","NOVA"],                   "#4ade80"),

  ("battery",   "Battery Technology",          "Battery",     "🔋","Energy",
   ["ALB","QS","LTHM","ENVX","FLNC","STEM"],                   "#34d399"),

  ("lng",       "LNG Export & Natural Gas",    "LNG",         "🔥","Energy",
   ["LNG","CQP","NFE","GLNG","AR","KNTK"],                    "#fb923c"),

  ("oilgas",    "Traditional Oil & Gas",       "Oil & Gas",   "🛢","Energy",
   ["XOM","CVX","COP","SLB","EOG","MPC"],                     "#d97706"),

  # ═══════════════════════════════════════════════════════════════════
  # MATERIALS
  # ═══════════════════════════════════════════════════════════════════
  ("minerals",  "Critical Minerals",           "Minerals",    "⛏","Materials",
   ["FCX","MP","VALE","RIO","SCCO","HBM"],                    "#fb923c"),

  ("steel",     "Steel & Aluminum",            "Steel/Al",    "🏗","Materials",
   ["NUE","STLD","CLF","CMC","AA","CENX"],                    "#78716c"),

  ("agri",      "Agriculture & Fertilizers",   "Agriculture", "🌾","Materials",
   ["MOS","NTR","CF","ICL","CTVA","SQM"],                    "#65a30d"),

  # ═══════════════════════════════════════════════════════════════════
  # PRECIOUS METALS
  # ═══════════════════════════════════════════════════════════════════
  ("gold",      "Gold & Gold Miners",          "Gold",        "🥇","Precious Metals",
   ["GLD","NEM","GOLD","AEM","WPM","FNV"],                    "#ffd700"),

  ("silver",    "Silver & Silver Miners",      "Silver",      "🥈","Precious Metals",
   ["SLV","PAAS","AG","HL","FSM","MAG"],                      "#e2e8f0"),

  ("jrgold",    "Junior Gold Miners",          "Jr. Gold",    "⛏","Precious Metals",
   ["GDXJ","ORLA","NGD","KGC","IAG","SAND"],                  "#fcd34d"),

  ("pgm",       "Platinum Group Metals",       "PGMs",        "⚗","Precious Metals",
   ["PPLT","PALL","SBSW","PLZL","PLAT","PAL"],                "#cbd5e1"),

  # ═══════════════════════════════════════════════════════════════════
  # UTILITIES
  # ═══════════════════════════════════════════════════════════════════
  ("water",     "Water Management",            "Water",       "💧","Utilities",
   ["AWK","XYL","WTRG","PNR","ARTNA","YORW"],                 "#0ea5e9"),

  # ═══════════════════════════════════════════════════════════════════
  # INDUSTRIALS
  # ═══════════════════════════════════════════════════════════════════
  ("defense",   "Defense & Military Tech",     "Defense",     "🛡","Industrials",
   ["LMT","RTX","NOC","GD","BA","HII"],                       "#ff6b35"),

  ("drones",    "Drones & Autonomous",         "Drones",      "🛸","Industrials",
   ["ACHR","JOBY","AVAV","KTOS","RCAT","ARK"],                "#f472b6"),

  ("space",     "Space Exploration",           "Space",       "🚀","Industrials",
   ["ASTS","LUNR","RKLB","PL","SPCE","SATL"],                 "#8b5cf6"),

  ("shipping",  "Shipping & Logistics",        "Shipping",    "🚢","Industrials",
   ["ZIM","GOGL","SBLK","MATX","STNG","FLNG"],                "#0369a1"),

  ("reshoring", "Supply Chain Reshoring",      "Reshoring",   "🏗","Industrials",
   ["URI","MLM","VMC","FAST","GWW","BLDR"],                   "#fdba74"),

  ("machinery", "Heavy Machinery & Infra",     "Machinery",   "🔧","Industrials",
   ["CAT","DE","CMI","PCAR","OSK","TEX"],                     "#94a3b8"),
]

# ── Verify no duplicates ──────────────────────────────────────────────────────
from collections import defaultdict
_seen = defaultdict(list)
for (tid, name, short, icon, sector, stocks, color) in THEMES:
    for s in stocks:
        _seen[s].append(tid)
_dupes = {s: ts for s, ts in _seen.items() if len(ts) > 1}
if _dupes:
    print(f"⚠ DUPLICATE STOCKS DETECTED:")
    for s, ts in _dupes.items():
        print(f"  {s} in {ts}")
else:
    print(f"✅ No duplicate stocks across {len(THEMES)} themes")

# ── Yahoo Finance fetcher ─────────────────────────────────────────────────────
def fetch(ticker, period="3mo"):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?range={period}&interval=1d")
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        res    = d["chart"]["result"][0]
        meta   = res["meta"]
        ts     = res["timestamp"]
        closes = res["indicators"]["quote"][0]["close"]
        highs  = res["indicators"]["quote"][0].get("high", [])
        lows   = res["indicators"]["quote"][0].get("low", [])
        vols   = res["indicators"]["quote"][0].get("volume", [])

        price = meta.get("regularMarketPrice") or meta.get("previousClose")

        prev_close = meta.get("regularMarketPreviousClose")
        if not prev_close:
            valid = [c for c in closes if c is not None]
            prev_close = valid[-2] if len(valid) >= 2 else (valid[-1] if valid else None)

        # ADR% — avg daily range as % of close, last 20 days
        adr_pct = None
        if highs and lows and closes:
            days = [(h, l, c) for h, l, c in zip(highs, lows, closes)
                    if h is not None and l is not None and c is not None and c > 0]
            days = days[-20:]
            if days:
                adr_pct = round(sum((h - l) / c * 100 for h, l, c in days) / len(days), 2)

        # Avg daily dollar volume (last 20 days) — used for liquidity check
        adv = None
        if vols and closes:
            pairs = [(v, c) for v, c in zip(vols, closes)
                     if v is not None and c is not None and v > 0][-20:]
            if pairs:
                adv = sum(v * c for v, c in pairs) / len(pairs)

        return {
            "ts": ts, "closes": closes, "price": price,
            "prev_close": prev_close, "adr_pct": adr_pct, "adv": adv
        }
    except Exception as e:
        print(f"    ERR {ticker}: {e}")
        return None

def price_on(ts, closes, target_ts):
    best = None
    for t, p in zip(ts, closes):
        if t <= target_ts and p is not None:
            best = p
    return best

def pct(cur, base):
    if cur and base and base != 0:
        return round((cur - base) / abs(base) * 100, 2)
    return None

def wavg(values, weights):
    pairs = [(v, w) for v, w in zip(values, weights)
             if v is not None and w is not None and w > 0]
    if not pairs:
        return None
    total_w = sum(w for _, w in pairs)
    return round(sum(v * w for v, w in pairs) / total_w, 2)

def avg(values):
    vals = [v for v in values if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else None

# ── Reference timestamps ──────────────────────────────────────────────────────
now       = datetime.datetime.utcnow()
one_week  = now - datetime.timedelta(days=7)
one_month = now - datetime.timedelta(days=30)

def mkets(dt):
    return int(datetime.datetime(dt.year, dt.month, dt.day, 21, 0).timestamp())

ts_week  = mkets(one_week)
ts_month = mkets(one_month)

print(f"Anchors: 1W={one_week.strftime('%b %d')}  1M={one_month.strftime('%b %d')}")

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache = {}
def fetch_cached(ticker):
    if ticker not in _cache:
        _cache[ticker] = fetch(ticker)
        time.sleep(0.3)
    return _cache[ticker]

# ── SPY benchmark ─────────────────────────────────────────────────────────────
print("\nFetching SPY benchmark...")
spy_raw = fetch("SPY")
spy = {"d": None, "w": None, "m": None, "daily_rets": []}

if spy_raw:
    c, pc   = spy_raw["price"], spy_raw["prev_close"]
    ts_list = spy_raw["ts"]
    cl_list = spy_raw["closes"]
    valid_cl = [(t, p) for t, p in zip(ts_list, cl_list) if p is not None]
    spy_daily = []
    for i in range(1, len(valid_cl)):
        prev, curr = valid_cl[i-1][1], valid_cl[i][1]
        spy_daily.append({"ts": valid_cl[i][0],
                          "ret": round((curr - prev) / prev * 100, 4)})
    spy_daily = spy_daily[-20:]
    spy = {
        "d": pct(c, pc),
        "w": pct(c, price_on(ts_list, cl_list, ts_week)),
        "m": pct(c, price_on(ts_list, cl_list, ts_month)),
        "daily_rets": spy_daily,
    }
    print(f"  SPY 1D={spy['d']}%  1W={spy['w']}%  1M={spy['m']}%")

spy_red_ts  = {s["ts"] for s in spy["daily_rets"] if s["ret"] < 0}
spy_red_avg = avg([s["ret"] for s in spy["daily_rets"] if s["ret"] < 0])

# ── Resilience ────────────────────────────────────────────────────────────────
def compute_resilience(raw):
    if not raw or not spy_red_ts:
        return None
    valid = [(t, p) for t, p in zip(raw["ts"], raw["closes"]) if p is not None]
    if len(valid) < 2:
        return None
    close_map = {t: p for t, p in valid}
    ts_sorted = sorted(close_map.keys())
    stock_reds = []
    for s in spy["daily_rets"]:
        if s["ret"] >= 0:
            continue
        t   = s["ts"]
        idx = next((i for i, st in enumerate(ts_sorted) if st >= t), None)
        if idx is None:
            idx = len(ts_sorted) - 1
        elif ts_sorted[idx] > t and idx > 0:
            idx -= 1
        if idx > 0:
            curr_p = close_map.get(ts_sorted[idx])
            prev_p = close_map.get(ts_sorted[idx - 1])
            if curr_p and prev_p:
                stock_reds.append((curr_p - prev_p) / prev_p * 100)
    if not stock_reds or spy_red_avg is None:
        return None
    return round(avg(stock_reds) - spy_red_avg, 2)

# ── Breadth ───────────────────────────────────────────────────────────────────
def compute_breadth(raw):
    if not raw:
        return None
    cl = [c for c in raw["closes"] if c is not None]
    if len(cl) < 20 or not raw["price"]:
        return None
    return 1 if raw["price"] > (sum(cl[-20:]) / 20) else 0

# ── Process themes ────────────────────────────────────────────────────────────
results = []
ADV_MIN = 10_000_000   # $10M minimum average daily dollar volume

for (tid, name, short, icon, sector, constituents, color) in THEMES:
    print(f"\n{name}")

    # No ETF proxy — sparkline and price computed from constituents directly
    spark5_raw = []   # filled below from constituent closes
    blended_price = None   # avg current price across constituents (display only)

    all_r1D, all_r1W, all_r1M = [], [], []
    all_res, all_brd, all_w   = [], [], []

    for ticker in constituents:
        raw = fetch_cached(ticker)
        if not raw:
            continue

        # Skip if below $10M average daily dollar volume
        adv = raw.get("adv")
        if adv is not None and adv < ADV_MIN:
            print(f"  {ticker:6s}  SKIP — ADV ${adv/1e6:.1f}M < $10M threshold")
            continue

        cur, pc = raw["price"], raw["prev_close"]
        r1D = pct(cur, pc)
        r1W = pct(cur, price_on(raw["ts"], raw["closes"], ts_week))
        r1M = pct(cur, price_on(raw["ts"], raw["closes"], ts_month))
        res = compute_resilience(raw)
        brd = compute_breadth(raw)

        adr = raw.get("adr_pct") or 3.0
        w   = 1.0 / max(adr, 0.5)

        adv_str = f"${adv/1e6:.0f}M" if adv else "—"
        print(f"  {ticker:6s}  1D={str(r1D):>7}%  1W={str(r1W):>7}%  1M={str(r1M):>7}%"
              f"  ADR={adr:.1f}%  ADV={adv_str}")

        all_r1D.append(r1D); all_r1W.append(r1W); all_r1M.append(r1M)
        all_res.append(res); all_brd.append(brd); all_w.append(w)

    r1D = wavg(all_r1D, all_w)
    r1W = wavg(all_r1W, all_w)
    r1M = wavg(all_r1M, all_w)

    # Build sparkline: equal-weight blend of last 5 daily closes across constituents
    # Normalize each stock to 100 at day -5 so different price scales don't distort
    spark5 = []
    valid_close_series = []
    for ticker in constituents:
        raw = _cache.get(ticker)
        if raw:
            cl = [c for c in raw["closes"] if c is not None]
            if len(cl) >= 5:
                valid_close_series.append(cl[-5:])
    if valid_close_series:
        for day_idx in range(5):
            day_vals = []
            for series in valid_close_series:
                base = series[0]
                if base and base > 0:
                    day_vals.append(series[day_idx] / base * 100)
            spark5.append(round(sum(day_vals)/len(day_vals), 2) if day_vals else None)
    spark5 = [v for v in spark5 if v is not None]

    resilience = avg([r for r in all_res if r is not None])
    breadth    = avg([b for b in all_brd if b is not None])

    rs1D = round(r1D - spy["d"], 2) if r1D is not None and spy["d"] is not None else None
    rs1W = round(r1W - spy["w"], 2) if r1W is not None and spy["w"] is not None else None
    rs1M = round(r1M - spy["m"], 2) if r1M is not None and spy["m"] is not None else None

    score = None
    if None not in (r1D, r1W, r1M, rs1D, rs1W, rs1M):
        ret_blend = r1D * 0.20 + r1W * 0.35 + r1M * 0.45
        rs_blend  = rs1D * 0.20 + rs1W * 0.35 + rs1M * 0.45
        res_score = resilience if resilience is not None else 0
        brd_score = (breadth * 10) if breadth is not None else 5
        # Score = RetBlend×35% + RSBlend×30% + Resilience×20% + Breadth×15%
        # MA crossover removed — fully data-driven, no manual inputs
        score = round(
            ret_blend  * 0.35 +
            rs_blend   * 0.30 +
            res_score  * 0.20 +
            brd_score  * 0.15,
            1
        )

    breadth_display = round(breadth * 10, 1) if breadth is not None else None
    n = sum(1 for r in all_r1M if r is not None)
    print(f"  → score={score}  res={resilience}  breadth={breadth_display}/10  (n={n})")

    # Blended display price: simple avg of constituent current prices (for reference only)
    constituent_prices = [_cache[t]["price"] for t in constituents
                         if t in _cache and _cache[t] and _cache[t].get("price")]
    blended_price = round(sum(constituent_prices)/len(constituent_prices), 2) if constituent_prices else None

    results.append({
        "id": tid, "name": name, "short": short, "icon": icon,
        "sector": sector, "stocks": constituents,
        "color": color, "price": blended_price,
        "ret1D": r1D, "ret1W": r1W, "ret1M": r1M,
        "rs1D": rs1D, "rs1W": rs1W, "rs1M": rs1M,
        "resilience": resilience,
        "breadth": breadth_display,
        "score": score,
        "spark5": spark5,
        "n_stocks": n,
    })

# ── Sort & write ──────────────────────────────────────────────────────────────
results.sort(key=lambda x: x["score"] if x["score"] is not None else -999, reverse=True)

output = {
    "updated":     now.strftime("%Y-%m-%d %H:%M UTC"),
    "methodology": "Pure constituent scoring · No ETF proxy · ADR-weighted · RetBlend×35%+RSBlend×30%+Resilience×20%+Breadth×15%",
    "spy":         {"d": spy["d"], "w": spy["w"], "m": spy["m"]},
    "themes":      results,
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅  Written data/market_data.json  ({len(results)} themes)")
print(f"    Top 3: {', '.join(r['name'] for r in results[:3])}")
