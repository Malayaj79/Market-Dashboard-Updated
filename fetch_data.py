"""
Market Themes Momentum Dashboard — Data Fetcher v3
===================================================
SCORING METHODOLOGY:

  Composite Score = RetBlend×30% + RSBlend×25% + Resilience×20% + Breadth×15% + MA×10pts

  RetBlend   = 1D×20% + 1W×35% + 1M×45%
  RSBlend    = same blend vs SPY
  Resilience = avg stock return on SPY red days minus avg SPY return on those days
               → positive = theme holds better when market sells off
  Breadth    = % of constituents above 20-day MA (0–10 scale)

  ADR ADJUSTMENT: returns are weighted inversely by each stock's average daily range %
  so high-volatility small caps (IONQ, RCAT) don't dominate over large liquid names.
  Weight = 1 / ADR%  (normalized across constituents)

  1D = current price vs previous session close
  1W = today vs last Friday close
  1M = today vs last month-end close

CONSTITUENT PHILOSOPHY:
  - Pure-play stocks that actually move with the theme
  - Mix of large liquid names (anchor the signal) + mid-cap pure-plays (capture the move)
  - No ETFs inside the constituent list (ETF is proxy only for sparkline/price display)
  - ARM, DOCN, FSLY and other missing names now included where relevant

TO UPDATE MA SIGNALS WEEKLY:
  Edit ma= in THEMES below (+1 bull, 0 neutral, -1 bear), then push.
"""

import json, datetime, time, urllib.request, os, math

THEMES = [
  # ── TECHNOLOGY: BROAD AI ────────────────────────────────────────────────────
  # Large-cap AI enablers — the infrastructure layer
  ("ai",        "Artificial Intelligence",      "AI",           "⚡", "Technology",       "AIQ",
   ["NVDA","MSFT","GOOGL","META","AMZN","PLTR"],                          "#00d4ff",  0),

  # ── TECHNOLOGY: AI SUB-THEMES ───────────────────────────────────────────────
  # Autonomous AI agents for enterprise — distinct from broad AI
  ("agenticai", "Agentic AI",                   "Agentic AI",   "🧠", "Technology",       "PLTR",
   ["PLTR","AI","BBAI","SOUN","GTLB","PATH"],                             "#818cf8",  1),

  # On-device inference — automotive, mobile, industrial
  ("edgeai",    "Edge AI & Inference Chips",    "Edge AI",      "🔩", "Technology",       "AMBA",
   ["AMBA","QCOM","MRVL","ADI","ARM","INTC"],                             "#a78bfa",  0),

  # Software layer for AI pipelines — observability, vector DB, streaming
  ("aiinfra",   "AI Infrastructure Software",   "AI Infra SW",  "🗄", "Technology",       "IGV",
   ["DDOG","MDB","SNOW","ESTC","CFLT","NET"],                             "#60a5fa",  0),

  # ── TECHNOLOGY: SEMICONDUCTORS ──────────────────────────────────────────────
  # Broad semis — GPU + logic + foundry
  ("semi",      "Semiconductors & Chips",       "Semis",        "💎", "Technology",       "SOXX",
   ["NVDA","AMD","AVGO","ARM","TSM","ASML"],                              "#6366f1", -1),

  # Equipment — the picks-and-shovels; leads the cycle
  ("semiequip", "Semiconductor Equipment",      "Semi Equip",   "🔭", "Technology",       "AMAT",
   ["AMAT","LRCX","KLAC","ONTO","UCTT","ACMR"],                          "#4f46e5",  0),

  # Memory — HBM, NAND, HDDs — distinct supply/demand cycle
  ("memory",    "Memory & Data Storage",        "Memory",       "💾", "Memory & Storage", "MU",
   ["SNDK","WDC","STX","MU","MRVL","NTAP"],                              "#38bdf8",  1),

  # Fiber/photonics — optical interconnects for AI data centers
  ("fiber",     "Fiber Optics & Optical Net.",  "Fiber Optics", "🔆", "Fiber Optics",     "CIEN",
   ["AAOI","LITE","COHR","CIEN","VIAV"],                                  "#bbf7d0",  1),

  # Data center real estate + power + servers
  ("datacntr",  "Data Centers & Infra",         "DataCtrs",     "🏭", "Technology",       "SRVR",
   ["EQIX","DLR","SMCI","VRT","ETN","DELL"],                             "#fca5a5",  1),

  # ── TECHNOLOGY: SOFTWARE ────────────────────────────────────────────────────
  # Cloud hyperscalers + pure-play cloud infra
  ("cloud",     "Cloud Computing",              "Cloud",        "☁",  "Technology",       "WCLD",
   ["AMZN","MSFT","GOOGL","SNOW","DOCN","NET"],                          "#93c5fd", -1),

  # Cybersecurity — endpoint + network + identity
  ("cyber",     "Cybersecurity",                "Cyber",        "🔐", "Technology",       "HACK",
   ["CRWD","PANW","ZS","FTNT","S","OKTA"],                               "#22d3ee",  1),

  # Quantum computing — pure speculative growth
  ("quantum",   "Quantum Computing",            "Quantum",      "⚛", "Technology",       "QTUM",
   ["IONQ","RGTI","QUBT","QMCO","IBM","GOOGL"],                          "#e879f9",  1),

  # Robotics — surgical + industrial + humanoid
  ("robotics",  "Robotics & Automation",        "Robots",       "🤖", "Technology",       "ROBO",
   ["ISRG","ROK","PATH","TSLA","FANUY","ONTO"],                          "#c084fc",  1),

  # SaaS — enterprise software on subscription
  ("saas",      "Software as a Service",        "SaaS",         "🧩", "Technology",       "IGV",
   ["CRM","NOW","WDAY","HUBS","BILL","GTLB"],                            "#6ee7b7",  0),

  # CDN + edge delivery — Fastly, Cloudflare, Akamai
  ("cdn",       "CDN & Edge Delivery",          "CDN/Edge",     "🌐", "Technology",       "NET",
   ["NET","FSLY","AKAM","EGIO","LLNW","BAND"],                           "#67e8f9",  0),

  # IoT — embedded chips + connectivity
  ("iot",       "Internet of Things",           "IoT",          "📡", "Technology",       "SNSR",
   ["CSCO","TXN","MCHP","PTC","SWKS","QRVO"],                           "#7dd3fc",  0),

  # 3D Printing — industrial + medical additive manufacturing
  ("print3d",   "3D Printing",                  "3D Print",     "🖨", "Technology",       "PRNT",
   ["DDD","SSYS","XMTR","NNDM","MTLS","TPVG"],                          "#7c3aed", -1),

  # ── TECHNOLOGY: CONSUMER ────────────────────────────────────────────────────
  # AR/VR — spatial computing + headsets
  ("arvr",      "AR / VR & Spatial Computing",  "AR/VR",        "🥽", "Consumer Tech",    "META",
   ["META","AAPL","IMMR","KOPN","VUZI","MVIS"],                          "#f472b6",  0),

  # Autonomous vehicles — LIDAR + self-driving software
  ("autonomouv","Autonomous Vehicles",           "Auto Vehicles","🚗", "Consumer Tech",    "TSLA",
   ["TSLA","GOOGL","MBLY","LAZR","INVZ","OUST"],                         "#34d399",  0),

  # ── HEALTHCARE ──────────────────────────────────────────────────────────────
  # AI drug discovery — ML-designed molecules
  ("aidrug",    "AI Drug Discovery",            "AI Drug",      "🧬", "Healthcare",       "ARKG",
   ["RXRX","EXAI","SDGR","ABCL","SANA","NUVB"],                         "#10b981",  1),

  # Medical devices — surgical robots + nerve stim + cardiac
  ("meddevice", "Medical Devices & Robotics",   "Med Devices",  "🏥", "Healthcare",       "IHI",
   ["ISRG","NVCR","SWAV","INSP","AXNX","TNDM"],                         "#3b82f6",  0),

  # GLP-1 obesity drugs — biggest pharma theme in a decade
  ("glp1",      "GLP-1 & Obesity Drugs",        "GLP-1",        "💊", "Healthcare",       "NVO",
   ["NVO","LLY","VKTX","HIMS","RDUS","ALT"],                            "#8b5cf6",  1),

  # ── FINTECH ─────────────────────────────────────────────────────────────────
  # Digital payments + neobanks + BNPL
  ("fintech",   "Digital Payments & Fintech",   "Fintech",      "💳", "Fintech",          "IPAY",
   ["SQ","AFRM","SOFI","NU","UPST","HOOD"],                             "#f97316",  0),

  # ── ENERGY ──────────────────────────────────────────────────────────────────
  # Nuclear power generation — utility operators
  ("nuclear",   "Nuclear Energy",               "Nuclear",      "☢", "Energy",            "NLR",
   ["CEG","VST","NRG","CCJ","BWXT","SMR"],                              "#fde68a",  1),

  # Uranium mining — pure-play supply side
  ("uranium",   "Uranium Mining",               "Uranium",      "🪨", "Energy",            "URNM",
   ["CCJ","NXE","DNN","UUUU","URG"],                                    "#f59e0b",  1),

  # Power grid modernization — transformers + switchgear + construction
  ("grid",      "Power Grid Modernization",     "Grid",         "🔌", "Energy",            "GRID",
   ["ETN","VRT","EMR","HUBB","PWR","GEV"],                              "#86efac",  1),

  # Clean energy — solar + wind + utility scale
  ("clean",     "Clean & Renewable Energy",     "Clean NRG",    "🌱", "Energy",            "ICLN",
   ["ENPH","FSLR","NEE","SEDG","RUN","BEP"],                            "#4ade80",  1),

  # Battery — EV batteries + grid storage + lithium
  ("battery",   "Battery Technology",           "Battery",      "🔋", "Energy",            "LIT",
   ["TSLA","QS","ALB","SQM","FLNC","STEM"],                             "#34d399",  0),

  # LNG — export terminals + shipping + US gas producers
  ("lng",       "LNG Export & Natural Gas",     "LNG",          "🔥", "Energy",            "FCG",
   ["LNG","CQP","TELL","NFE","GLNG","AR"],                              "#fb923c",  0),

  # Traditional O&G — integrated majors + E&P + services
  # Note: MA=-1 reflects current macro headwinds (tariffs, demand fears)
  # Update this when trend changes
  ("oilgas",    "Traditional Oil & Gas",        "Oil & Gas",    "🛢", "Energy",            "XLE",
   ["XOM","CVX","COP","SLB","EOG","MPC"],                               "#d97706", -1),

  # ── MATERIALS ───────────────────────────────────────────────────────────────
  # Critical minerals — copper + rare earth + nickel
  ("minerals",  "Critical Minerals",            "Minerals",     "⛏", "Materials",         "COPX",
   ["FCX","MP","ALB","VALE","RIO","TECK"],                              "#fb923c",  1),

  # Steel + aluminum — reshoring + infrastructure demand
  ("steel",     "Steel & Aluminum",             "Steel/Al",     "🏗", "Materials",         "SLX",
   ["NUE","STLD","CLF","CMC","AA","CENX"],                              "#78716c",  0),

  # Agriculture + fertilizers — food security + geopolitical supply
  ("agri",      "Agriculture & Fertilizers",    "Agriculture",  "🌾", "Materials",         "MOO",
   ["MOS","NTR","CF","ICL","IPI","CTVA"],                               "#65a30d",  0),

  # ── PRECIOUS METALS ─────────────────────────────────────────────────────────
  ("gold",      "Gold & Gold Miners",           "Gold",         "🥇", "Precious Metals",   "GLD",
   ["GLD","NEM","GOLD","AEM","WPM","FNV"],                              "#ffd700",  1),

  ("silver",    "Silver & Silver Miners",       "Silver",       "🥈", "Precious Metals",   "SLV",
   ["SLV","PAAS","AG","HL","WPM","FSM"],                                "#e2e8f0",  1),

  ("jrgold",    "Junior Gold Miners",           "Jr. Gold",     "⛏", "Precious Metals",   "GDXJ",
   ["GDXJ","ORLA","NGD","AUMN","EQX","AU"],                             "#fcd34d",  1),

  ("pgm",       "Platinum Group Metals",        "PGMs",         "⚗", "Precious Metals",   "PPLT",
   ["PPLT","PALL","SBSW","IMPUY","ANGPY"],                              "#cbd5e1",  0),

  ("broadmet",  "Broad Precious Metals",        "Prec. Metals", "🏅", "Precious Metals",   "GLTR",
   ["GLD","SLV","PPLT","PALL","WPM","FNV"],                             "#f0abfc",  1),

  # ── UTILITIES ───────────────────────────────────────────────────────────────
  ("water",     "Water Management",             "Water",        "💧", "Utilities",         "PHO",
   ["AWK","XYL","WTRG","PNR","MSEX","CWCO"],                            "#0ea5e9",  0),

  # ── INDUSTRIALS ─────────────────────────────────────────────────────────────
  ("defense",   "Defense & Military Tech",      "Defense",      "🛡", "Industrials",       "ITA",
   ["LMT","RTX","NOC","GD","BA","HII"],                                 "#ff6b35",  1),

  ("drones",    "Drones & Autonomous",          "Drones",       "🛸", "Industrials",       "ACHR",
   ["ACHR","JOBY","AVAV","KTOS","RCAT","UAVS"],                         "#f472b6",  1),

  ("space",     "Space Exploration",            "Space",        "🚀", "Industrials",       "UFO",
   ["ASTS","LUNR","RKLB","RDW","SPCE","BWXT"],                          "#8b5cf6",  1),

  ("shipping",  "Shipping & Logistics",         "Shipping",     "🚢", "Industrials",       "BDRY",
   ["ZIM","DAC","GOGL","SBLK","MATX","ATSG"],                           "#0369a1",  0),

  ("reshoring", "Supply Chain Reshoring",       "Reshoring",    "🏗", "Industrials",       "PAVE",
   ["CAT","DE","URI","MLM","VMC","NUE"],                                 "#fdba74",  0),

  ("machinery", "Heavy Machinery & Infra",      "Machinery",    "🔧", "Industrials",       "XLI",
   ["CAT","DE","CMI","PCAR","TEX","WNC"],                               "#94a3b8",  0),
]

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

        price = meta.get("regularMarketPrice") or meta.get("previousClose")

        prev_close = meta.get("regularMarketPreviousClose")
        if not prev_close:
            valid = [c for c in closes if c is not None]
            prev_close = valid[-2] if len(valid) >= 2 else (valid[-1] if valid else None)

        # ADR% — average daily range as % of close, last 20 days
        # Used for volatility-adjusted weighting
        adr_pct = None
        if highs and lows and closes:
            days = [(h, l, c) for h, l, c in zip(highs, lows, closes)
                    if h is not None and l is not None and c is not None and c > 0]
            days = days[-20:]
            if days:
                adr_pct = round(sum((h - l) / c * 100 for h, l, c in days) / len(days), 2)

        return {
            "ts": ts, "closes": closes, "price": price,
            "prev_close": prev_close, "adr_pct": adr_pct
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
    """Weighted average — weights are raw (will be normalized internally)."""
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
last_fri  = now - datetime.timedelta(days=max((now.weekday() - 4) % 7, 1))
month_end = now.replace(day=1) - datetime.timedelta(days=1)

def mkets(dt):
    return int(datetime.datetime(dt.year, dt.month, dt.day, 21, 0).timestamp())

ts_fri   = mkets(last_fri)
ts_month = mkets(month_end)

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache = {}
def fetch_cached(ticker):
    if ticker not in _cache:
        _cache[ticker] = fetch(ticker)
        time.sleep(0.3)
    return _cache[ticker]

# ── SPY benchmark ─────────────────────────────────────────────────────────────
print("Fetching SPY benchmark...")
spy_raw = fetch("SPY")
spy = {"d": None, "w": None, "m": None, "daily_rets": []}

if spy_raw:
    c, pc    = spy_raw["price"], spy_raw["prev_close"]
    ts_list  = spy_raw["ts"]
    cl_list  = spy_raw["closes"]
    valid_cl = [(t, p) for t, p in zip(ts_list, cl_list) if p is not None]
    spy_daily = []
    for i in range(1, len(valid_cl)):
        prev, curr = valid_cl[i-1][1], valid_cl[i][1]
        spy_daily.append({"ts": valid_cl[i][0],
                          "ret": round((curr - prev) / prev * 100, 4)})
    spy_daily = spy_daily[-20:]
    spy = {
        "d": pct(c, pc),
        "w": pct(c, price_on(ts_list, cl_list, ts_fri)),
        "m": pct(c, price_on(ts_list, cl_list, ts_month)),
        "daily_rets": spy_daily,
    }
    print(f"  SPY 1D={spy['d']}%  1W={spy['w']}%  1M={spy['m']}%")

spy_red_ts  = {s["ts"] for s in spy["daily_rets"] if s["ret"] < 0}
spy_red_avg = avg([s["ret"] for s in spy["daily_rets"] if s["ret"] < 0])

# ── Resilience helper ─────────────────────────────────────────────────────────
def compute_resilience(raw):
    """Stock's avg return on SPY red days minus SPY's avg on those days.
    Positive = held up better than market when it sold off."""
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

# ── Breadth helper ────────────────────────────────────────────────────────────
def compute_breadth(raw):
    """1 if stock is above its 20-day MA, 0 if below."""
    if not raw:
        return None
    cl = [c for c in raw["closes"] if c is not None]
    if len(cl) < 20 or not raw["price"]:
        return None
    return 1 if raw["price"] > (sum(cl[-20:]) / 20) else 0

# ── Process themes ────────────────────────────────────────────────────────────
results = []

for (tid, name, short, icon, sector, proxy_etf, constituents, color, ma) in THEMES:
    print(f"\n{name}")

    proxy_raw   = fetch_cached(proxy_etf)
    proxy_price = round(proxy_raw["price"], 2) if proxy_raw and proxy_raw["price"] else None
    spark5      = [round(x, 2) for x in proxy_raw["closes"] if x is not None][-5:] if proxy_raw else []

    all_r1D, all_r1W, all_r1M = [], [], []
    all_res, all_brd, all_w   = [], [], []

    for ticker in constituents:
        raw = fetch_cached(ticker)
        if not raw:
            continue
        cur, pc = raw["price"], raw["prev_close"]

        r1D = pct(cur, pc)
        r1W = pct(cur, price_on(raw["ts"], raw["closes"], ts_fri))
        r1M = pct(cur, price_on(raw["ts"], raw["closes"], ts_month))
        res = compute_resilience(raw)
        brd = compute_breadth(raw)

        # ADR-based weight: inverse of volatility
        # Low-ADR stocks (large caps) get higher weight — they anchor the signal
        # High-ADR stocks (small caps) get lower weight — reduce distortion
        adr = raw.get("adr_pct") or 3.0   # fallback to 3% if unavailable
        w   = 1.0 / max(adr, 0.5)         # floor at 0.5% to avoid extreme weights

        print(f"  {ticker:6s}  1D={str(r1D):>7}%  1W={str(r1W):>7}%  1M={str(r1M):>7}%"
              f"  ADR={adr:.1f}%  w={w:.2f}")

        all_r1D.append(r1D); all_r1W.append(r1W); all_r1M.append(r1M)
        all_res.append(res); all_brd.append(brd); all_w.append(w)

    # ADR-weighted blends
    r1D = wavg(all_r1D, all_w)
    r1W = wavg(all_r1W, all_w)
    r1M = wavg(all_r1M, all_w)

    # Resilience & Breadth — simple avg (these are already normalized metrics)
    resilience = avg([r for r in all_res if r is not None])
    breadth    = avg([b for b in all_brd if b is not None])  # 0–1, multiply by 10 for display

    # RS vs SPY
    rs1D = round(r1D - spy["d"], 2) if r1D is not None and spy["d"] is not None else None
    rs1W = round(r1W - spy["w"], 2) if r1W is not None and spy["w"] is not None else None
    rs1M = round(r1M - spy["m"], 2) if r1M is not None and spy["m"] is not None else None

    # Composite Score
    score = None
    if None not in (r1D, r1W, r1M, rs1D, rs1W, rs1M):
        ret_blend = r1D * 0.20 + r1W * 0.35 + r1M * 0.45
        rs_blend  = rs1D * 0.20 + rs1W * 0.35 + rs1M * 0.45
        res_score = resilience if resilience is not None else 0
        brd_score = (breadth * 10) if breadth is not None else 5  # 0–10
        score = round(
            ret_blend  * 0.30 +
            rs_blend   * 0.25 +
            res_score  * 0.20 +
            brd_score  * 0.15 +
            ma         * 10,
            1
        )

    # Breadth display: convert 0–1 avg to 0–10
    breadth_display = round(breadth * 10, 1) if breadth is not None else None

    print(f"  → score={score}  res={resilience}  breadth={breadth_display}/10  "
          f"(n={sum(1 for r in all_r1M if r is not None)})")

    results.append({
        "id": tid, "name": name, "short": short, "icon": icon,
        "sector": sector, "etf": proxy_etf, "stocks": constituents,
        "color": color, "ma": ma, "price": proxy_price,
        "ret1D": r1D, "ret1W": r1W, "ret1M": r1M,
        "rs1D": rs1D, "rs1W": rs1W, "rs1M": rs1M,
        "resilience": resilience,
        "breadth": breadth_display,
        "score": score,
        "spark5": spark5,
        "n_stocks": sum(1 for r in all_r1M if r is not None),
    })

# ── Sort & write ──────────────────────────────────────────────────────────────
results.sort(key=lambda x: x["score"] if x["score"] is not None else -999, reverse=True)

output = {
    "updated":     now.strftime("%Y-%m-%d %H:%M UTC"),
    "methodology": "ADR-weighted: RetBlend×30%+RSBlend×25%+Resilience×20%+Breadth×15%+MA×10pts",
    "spy":         {"d": spy["d"], "w": spy["w"], "m": spy["m"]},
    "themes":      results,
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅  Written data/market_data.json  ({len(results)} themes)")
print(f"    Top 3: {', '.join(r['name'] for r in results[:3])}")
