"""
Market Themes Momentum Dashboard — Data Fetcher v2
===================================================
SCORING METHODOLOGY:

  Composite Score = RetBlend×30% + RSBlend×25% + Resilience×20% + Breadth×15% + MA×10pts

  RetBlend   = 1D×20% + 1W×35% + 1M×45%   (magnitude of move)
  RSBlend    = same blend but vs SPY         (relative outperformance)
  Resilience = avg theme return on SPY red days (last 20 sessions)
               minus avg SPY return on those same days
               → positive = holds better than market on down days
  Breadth    = % of constituents above their 20-day MA
               scaled 0–10 (10 = all stocks above 20MA)
               → full participation vs narrow leadership

  1D = today's move: current price vs previous session close
  1W = today vs last Friday close
  1M = today vs last month-end close

TO UPDATE MA SIGNALS WEEKLY:
  Edit ma= in THEMES below (+1 bull, 0 neutral, -1 bear), then push.
"""

import json, datetime, time, urllib.request, os

# ── Theme definitions: (id, name, short, icon, sector, proxy_etf, constituents, color, ma)
THEMES = [
  # ── TECHNOLOGY: AI ──────────────────────────────────────────────────────────
  ("ai",        "Artificial Intelligence",      "AI",           "⚡", "Technology",       "AIQ",
   ["NVDA","MSFT","GOOGL","META","AMZN","PLTR"],                          "#00d4ff",  0),
  ("agenticai", "Agentic AI",                   "Agentic AI",   "🧠", "Technology",       "PLTR",
   ["PLTR","AI","BBAI","SOUN","GTLB","AMBA"],                             "#818cf8",  1),
  ("edgeai",    "Edge AI & Inference Chips",    "Edge AI",      "🔩", "Technology",       "AMBA",
   ["AMBA","QCOM","MRVL","ADI","SWKS","INTC"],                            "#a78bfa",  0),
  ("aiinfra",   "AI Infrastructure Software",   "AI Infra SW",  "🗄", "Technology",       "WCLD",
   ["DDOG","MDB","SNOW","GTLB","ESTC","CFLT"],                            "#60a5fa",  0),
  ("semi",      "Semiconductors & Chips",       "Semis",        "💎", "Technology",       "SOXX",
   ["NVDA","AMD","AVGO","ASML","TSM","AMAT"],                             "#6366f1", -1),
  ("semiequip", "Semiconductor Equipment",      "Semi Equip",   "🔬", "Technology",       "AMAT",
   ["AMAT","LRCX","KLAC","ONTO","UCTT","ACMR"],                           "#4f46e5",  0),
  ("memory",    "Memory & Data Storage",        "Memory",       "💾", "Memory & Storage", "MU",
   ["SNDK","WDC","STX","MU","MRVL","NTAP"],                               "#38bdf8",  1),
  ("fiber",     "Fiber Optics & Optical Net.",  "Fiber Optics", "🔆", "Fiber Optics",     "CIEN",
   ["AAOI","LITE","COHR","CIEN","VIAV"],                                  "#bbf7d0",  1),
  ("datacntr",  "Data Centers & Infra",         "DataCtrs",     "🏭", "Technology",       "SRVR",
   ["EQIX","DLR","NVDA","SMCI","VRT","ETN"],                              "#fca5a5",  1),
  ("cloud",     "Cloud Computing",              "Cloud",        "☁",  "Technology",       "WCLD",
   ["AMZN","MSFT","GOOGL","SNOW","NET","DDOG"],                           "#93c5fd", -1),
  ("cyber",     "Cybersecurity",                "Cyber",        "🔐", "Technology",       "HACK",
   ["CRWD","PANW","ZS","FTNT","S","OKTA"],                                "#22d3ee",  1),
  ("quantum",   "Quantum Computing",            "Quantum",      "⚛", "Technology",       "QTUM",
   ["IONQ","RGTI","QUBT","IBM","GOOGL","MSFT"],                           "#e879f9",  1),
  ("robotics",  "Robotics & Automation",        "Robots",       "🤖", "Technology",       "ROBO",
   ["ISRG","ROK","PATH","TSLA","FANUY","ONTO"],                           "#c084fc",  1),
  ("saas",      "Software as a Service",        "SaaS",         "🧩", "Technology",       "IGV",
   ["CRM","NOW","WDAY","HUBS","ZM","BILL"],                               "#6ee7b7",  0),
  ("iot",       "Internet of Things",           "IoT",          "📡", "Technology",       "SNSR",
   ["CSCO","TXN","MCHP","PTC","SWKS","QRVO"],                            "#7dd3fc",  0),
  ("print3d",   "3D Printing",                  "3D Print",     "🖨", "Technology",       "PRNT",
   ["DDD","SSYS","XMTR","NNDM","MTLS","VELO"],                           "#7c3aed", -1),
  # ── TECHNOLOGY: CONSUMER ────────────────────────────────────────────────────
  ("arvr",      "AR / VR & Spatial Computing",  "AR/VR",        "🥽", "Consumer Tech",    "META",
   ["META","AAPL","IMMR","KOPN","VUZI","MVIS"],                           "#f472b6",  0),
  ("autonomouv","Autonomous Vehicles",           "Auto Vehicles","🚗", "Consumer Tech",    "TSLA",
   ["TSLA","GOOGL","MBLY","LAZR","INVZ","OUST"],                          "#34d399",  0),
  # ── HEALTHCARE ──────────────────────────────────────────────────────────────
  ("aidrug",    "AI Drug Discovery",            "AI Drug",      "🧬", "Healthcare",       "ARKG",
   ["RXRX","EXAI","SDGR","ABCL","SANA","NUVB"],                          "#10b981",  1),
  ("meddevice", "Medical Devices & Robotics",   "Med Devices",  "🏥", "Healthcare",       "IHI",
   ["ISRG","NVCR","SWAV","INSP","AXNX","RELY"],                          "#3b82f6",  0),
  ("glp1",      "GLP-1 & Obesity Drugs",        "GLP-1",        "💊", "Healthcare",       "NVO",
   ["NVO","LLY","VKTX","RDUS","HIMS","ALT"],                             "#8b5cf6",  1),
  # ── FINTECH ─────────────────────────────────────────────────────────────────
  ("fintech",   "Digital Payments & Fintech",   "Fintech",      "💳", "Fintech",          "IPAY",
   ["SQ","AFRM","SOFI","NU","UPST","HOOD"],                              "#f97316",  0),
  # ── ENERGY ──────────────────────────────────────────────────────────────────
  ("nuclear",   "Nuclear Energy",               "Nuclear",      "☢", "Energy",            "NLR",
   ["CEG","VST","NRG","CCJ","BWXT","SMR"],                                "#fde68a",  1),
  ("uranium",   "Uranium Mining",               "Uranium",      "🪨", "Energy",            "URNM",
   ["CCJ","NXE","DNN","UUUU","URG"],                                      "#f59e0b",  1),
  ("grid",      "Power Grid Modernization",     "Grid",         "🔌", "Energy",            "GRID",
   ["ETN","VRT","EMR","HUBB","PWR","GEV"],                                "#86efac",  1),
  ("clean",     "Clean & Renewable Energy",     "Clean NRG",    "🌱", "Energy",            "ICLN",
   ["ENPH","FSLR","NEE","SEDG","RUN","BEP"],                              "#4ade80",  1),
  ("battery",   "Battery Technology",           "Battery",      "🔋", "Energy",            "LIT",
   ["TSLA","QS","ALB","SQM","FLNC","STEM"],                               "#34d399",  0),
  ("lng",       "LNG Export & Natural Gas",     "LNG",          "🔥", "Energy",            "FCG",
   ["LNG","CQP","TELL","NFE","GLNG","AR"],                                "#fb923c",  0),
  ("oilgas",    "Traditional Oil & Gas",        "Oil & Gas",    "🛢", "Energy",            "XLE",
   ["XOM","CVX","COP","SLB","EOG","MPC"],                                 "#d97706", -1),
  # ── MATERIALS ───────────────────────────────────────────────────────────────
  ("minerals",  "Critical Minerals",            "Minerals",     "⛏", "Materials",         "COPX",
   ["FCX","MP","ALB","VALE","RIO","TECK"],                                "#fb923c",  1),
  ("steel",     "Steel & Aluminum",             "Steel/Al",     "🏗", "Materials",         "SLX",
   ["NUE","STLD","CLF","CMC","AA","CENX"],                                "#78716c",  0),
  ("agri",      "Agriculture & Fertilizers",    "Agriculture",  "🌾", "Materials",         "MOO",
   ["MOS","NTR","CF","ICL","IPI","CTVA"],                                 "#65a30d",  0),
  # ── PRECIOUS METALS ─────────────────────────────────────────────────────────
  ("gold",      "Gold & Gold Miners",           "Gold",         "🥇", "Precious Metals",   "GLD",
   ["GLD","NEM","GOLD","AEM","WPM","FNV"],                                "#ffd700",  1),
  ("silver",    "Silver & Silver Miners",       "Silver",       "🥈", "Precious Metals",   "SLV",
   ["SLV","PAAS","AG","HL","WPM","FSM"],                                  "#e2e8f0",  1),
  ("jrgold",    "Junior Gold Miners",           "Jr. Gold",     "⛏", "Precious Metals",   "GDXJ",
   ["GDXJ","ORLA","NGD","AUMN","EQX","AU"],                               "#fcd34d",  1),
  ("pgm",       "Platinum Group Metals",        "PGMs",         "⚗", "Precious Metals",   "PPLT",
   ["PPLT","PALL","SBSW","IMPUY","ANGPY"],                                "#cbd5e1",  0),
  ("broadmet",  "Broad Precious Metals",        "Prec. Metals", "🏅", "Precious Metals",   "GLTR",
   ["GLD","SLV","PPLT","PALL","WPM","FNV"],                               "#f0abfc",  1),
  # ── UTILITIES ───────────────────────────────────────────────────────────────
  ("water",     "Water Management",             "Water",        "💧", "Utilities",         "PHO",
   ["AWK","XYL","WTRG","PNR","MSEX","CWCO"],                              "#0ea5e9",  0),
  # ── INDUSTRIALS ─────────────────────────────────────────────────────────────
  ("defense",   "Defense & Military Tech",      "Defense",      "🛡", "Industrials",       "ITA",
   ["LMT","RTX","NOC","GD","BA","HII"],                                   "#ff6b35",  1),
  ("drones",    "Drones & Autonomous",          "Drones",       "🛸", "Industrials",       "ACHR",
   ["ACHR","JOBY","AVAV","KTOS","RCAT","UAVS"],                           "#f472b6",  1),
  ("space",     "Space Exploration",            "Space",        "🚀", "Industrials",       "UFO",
   ["ASTS","LUNR","RKLB","RDW","SPCE","BWXT"],                            "#8b5cf6",  1),
  ("shipping",  "Shipping & Logistics",         "Shipping",     "🚢", "Industrials",       "BDRY",
   ["ZIM","DAC","GOGL","SBLK","MATX","ATSG"],                             "#0369a1",  0),
  ("reshoring", "Supply Chain Reshoring",       "Reshoring",    "🏗", "Industrials",       "PAVE",
   ["CAT","DE","URI","MLM","VMC","NUE"],                                   "#fdba74",  0),
  ("machinery", "Heavy Machinery & Infra",      "Machinery",    "🔧", "Industrials",       "XLI",
   ["CAT","DE","CMI","PCAR","TEX","WNC"],                                  "#94a3b8",  0),
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

        price = meta.get("regularMarketPrice") or meta.get("previousClose")

        # prev_close: meta field first, fallback to second-to-last historical close
        prev_close = meta.get("regularMarketPreviousClose")
        if not prev_close:
            valid = [c for c in closes if c is not None]
            prev_close = valid[-2] if len(valid) >= 2 else (valid[-1] if valid else None)

        return {"ts": ts, "closes": closes, "price": price, "prev_close": prev_close}
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

# ── Cache to avoid re-fetching shared tickers ─────────────────────────────────
_cache = {}
def fetch_cached(ticker):
    if ticker not in _cache:
        _cache[ticker] = fetch(ticker)
        time.sleep(0.3)
    return _cache[ticker]

# ── Fetch SPY — needed for RS and Resilience ──────────────────────────────────
print("Fetching SPY benchmark...")
spy_raw = fetch("SPY")
spy = {"d": None, "w": None, "m": None, "daily_rets": []}

if spy_raw:
    c  = spy_raw["price"]
    pc = spy_raw["prev_close"]
    ts_list = spy_raw["ts"]
    cl_list = spy_raw["closes"]

    # Build daily return series for SPY (last 20 trading sessions)
    # Used for Resilience calculation
    valid_closes = [(t, p) for t, p in zip(ts_list, cl_list) if p is not None]
    spy_daily = []
    for i in range(1, len(valid_closes)):
        prev = valid_closes[i-1][1]
        curr = valid_closes[i][1]
        spy_daily.append({
            "ts":  valid_closes[i][0],
            "ret": round((curr - prev) / prev * 100, 4)
        })
    spy_daily = spy_daily[-20:]  # last 20 sessions

    spy = {
        "d":          pct(c, pc),
        "w":          pct(c, price_on(ts_list, cl_list, ts_fri)),
        "m":          pct(c, price_on(ts_list, cl_list, ts_month)),
        "daily_rets": spy_daily,
    }
    print(f"  SPY → 1D={spy['d']}%  1W={spy['w']}%  1M={spy['m']}%  ({len(spy_daily)} daily sessions)")

# Red-day timestamps: sessions where SPY was negative
spy_red_days = {s["ts"] for s in spy["daily_rets"] if s["ret"] < 0}
spy_red_avg  = avg([s["ret"] for s in spy["daily_rets"] if s["ret"] < 0])

# ── Helper: compute resilience for a ticker ───────────────────────────────────
def compute_resilience(raw):
    """
    On each day SPY was negative (last 20 sessions), compute the stock's
    return on that day. Return the average stock return on red days minus
    the average SPY return on those same red days.
    Positive = stock held up better than SPY when market sold off.
    """
    if not raw or not spy_red_days:
        return None
    ts_list = raw["ts"]
    cl_list = raw["closes"]
    valid   = [(t, p) for t, p in zip(ts_list, cl_list) if p is not None]
    if len(valid) < 2:
        return None

    # Build a ts→close lookup for fast access
    close_map = {t: p for t, p in valid}
    ts_sorted = sorted(close_map.keys())

    stock_reds = []
    for spy_session in spy["daily_rets"]:
        if spy_session["ret"] >= 0:
            continue
        # Find the stock close on that date and the prior date
        t = spy_session["ts"]
        idx = None
        for i, st in enumerate(ts_sorted):
            if st == t:
                idx = i
                break
            elif st > t:
                # use previous available close
                if i > 0:
                    idx = i - 1
                break
        if idx is not None and idx > 0:
            curr_p = close_map.get(ts_sorted[idx])
            prev_p = close_map.get(ts_sorted[idx - 1])
            if curr_p and prev_p:
                stock_reds.append(round((curr_p - prev_p) / prev_p * 100, 4))

    if not stock_reds or spy_red_avg is None:
        return None
    return round(avg(stock_reds) - spy_red_avg, 2)

# ── Helper: compute breadth (% above 20-day MA) ───────────────────────────────
def compute_breadth(raw):
    """
    Returns 0–10 score: (stocks above their 20-day simple MA / total stocks) × 10
    """
    if not raw:
        return None
    cl_list = [c for c in raw["closes"] if c is not None]
    if len(cl_list) < 20:
        return None
    ma20     = sum(cl_list[-20:]) / 20
    price    = raw["price"]
    if price is None:
        return None
    return 10 if price > ma20 else 0   # per-stock: 10 if above, 0 if below

# ── Process each theme ────────────────────────────────────────────────────────
results = []

for (tid, name, short, icon, sector, proxy_etf, constituents, color, ma) in THEMES:
    print(f"\n{name}")

    # Proxy ETF → sparkline & display price
    proxy_raw   = fetch_cached(proxy_etf)
    proxy_price = round(proxy_raw["price"], 2) if proxy_raw and proxy_raw["price"] else None
    spark5      = [round(x, 2) for x in proxy_raw["closes"] if x is not None][-5:] if proxy_raw else []

    # Per-constituent metrics
    all_r1D, all_r1W, all_r1M = [], [], []
    all_resilience, all_breadth = [], []

    for ticker in constituents:
        raw = fetch_cached(ticker)
        if not raw:
            continue
        cur = raw["price"]
        pc  = raw["prev_close"]
        r1D = pct(cur, pc)
        r1W = pct(cur, price_on(raw["ts"], raw["closes"], ts_fri))
        r1M = pct(cur, price_on(raw["ts"], raw["closes"], ts_month))

        # Resilience & Breadth per stock
        res = compute_resilience(raw)
        brd = compute_breadth(raw)

        print(f"  {ticker:8s}  1D={str(r1D):>7}%  1W={str(r1W):>7}%  1M={str(r1M):>7}%"
              f"  res={str(res):>6}  brd={brd}")

        if r1D is not None: all_r1D.append(r1D)
        if r1W is not None: all_r1W.append(r1W)
        if r1M is not None: all_r1M.append(r1M)
        if res is not None: all_resilience.append(res)
        if brd is not None: all_breadth.append(brd)

    # Equal-weight blends
    r1D = avg(all_r1D)
    r1W = avg(all_r1W)
    r1M = avg(all_r1M)

    # Resilience: avg stock return on SPY red days minus avg SPY return on those days
    # Positive = theme held up better than market when market sold off
    resilience = avg(all_resilience)

    # Breadth: avg 0–10 score across constituents
    # 10 = all stocks above 20MA, 0 = none above 20MA
    breadth = avg(all_breadth)

    # Relative Strength vs SPY
    rs1D = round(r1D - spy["d"], 2) if r1D is not None and spy["d"] is not None else None
    rs1W = round(r1W - spy["w"], 2) if r1W is not None and spy["w"] is not None else None
    rs1M = round(r1M - spy["m"], 2) if r1M is not None and spy["m"] is not None else None

    # ── Composite Score ───────────────────────────────────────────────────────
    # RetBlend×30% + RSBlend×25% + Resilience×20% + Breadth×15% + MA×10pts
    score = None
    if None not in (r1D, r1W, r1M, rs1D, rs1W, rs1M):
        ret_blend = r1D * 0.20 + r1W * 0.35 + r1M * 0.45
        rs_blend  = rs1D * 0.20 + rs1W * 0.35 + rs1M * 0.45
        res_score = resilience if resilience is not None else 0
        brd_score = (breadth / 10 * 10) if breadth is not None else 0  # already 0–10
        score = round(
            ret_blend  * 0.30 +
            rs_blend   * 0.25 +
            res_score  * 0.20 +
            brd_score  * 0.15 +
            ma         * 10,
            1
        )

    print(f"  → BLENDED  score={score}  res={resilience}  breadth={breadth}/10  (n={len(all_r1M)})")

    results.append({
        "id": tid, "name": name, "short": short, "icon": icon,
        "sector": sector, "etf": proxy_etf, "stocks": constituents,
        "color": color, "ma": ma, "price": proxy_price,
        "ret1D": r1D, "ret1W": r1W, "ret1M": r1M,
        "rs1D": rs1D, "rs1W": rs1W, "rs1M": rs1M,
        "resilience": resilience,
        "breadth": breadth,
        "score": score,
        "spark5": spark5,
        "n_stocks": len(all_r1M),
    })

# ── Sort & write ──────────────────────────────────────────────────────────────
results.sort(key=lambda x: x["score"] if x["score"] is not None else -999, reverse=True)

output = {
    "updated":     now.strftime("%Y-%m-%d %H:%M UTC"),
    "methodology": "RetBlend×30%+RSBlend×25%+Resilience×20%+Breadth×15%+MA×10pts",
    "spy":         {"d": spy["d"], "w": spy["w"], "m": spy["m"]},
    "themes":      results,
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅  Written data/market_data.json  ({len(results)} themes)")
print(f"    Top 3: {', '.join(r['name'] for r in results[:3])}")
