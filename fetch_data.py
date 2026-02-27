"""
Market Themes Momentum Dashboard — Data Fetcher
================================================
METHODOLOGY:
  - Fetches Market Cap for all constituent stocks to calculate Cap-Weighted Averages.
  - Computes 1D / 1W / 1M returns for every stock individually.
  - Weights each stock's return by its Market Cap relative to the theme's total Cap.
  - Relative Strength = theme return minus SPY return (same anchor)
"""

import json, datetime, time, urllib.request

THEMES = [
  ("ai",        "Artificial Intelligence",      "AI",           "⚡", "Technology",       "AIQ",
   ["NVDA","MSFT","GOOGL","META","AMZN","PLTR"],                          "#00d4ff",  0),
  ("semi",      "Semiconductors & Chips",       "Semis",        "💎", "Technology",       "SOXX",
   ["NVDA","AMD","AVGO","ASML","TSM","AMAT"],                             "#818cf8", -1),
  ("cloud",     "Cloud Computing",              "Cloud",        "☁",  "Technology",       "WCLD",
   ["AMZN","MSFT","GOOGL","SNOW","NET","DDOG"],                           "#60a5fa", -1),
  ("cyber",     "Cybersecurity",                "Cyber",        "🔐", "Technology",       "HACK",
   ["CRWD","PANW","ZS","FTNT","S","OKTA"],                                "#22d3ee",  1),
  ("quantum",   "Quantum Computing",            "Quantum",      "🔬", "Technology",       "QTUM",
   ["IONQ","RGTI","QUBT","IBM","GOOGL","MSFT"],                           "#a78bfa",  1),
  ("robotics",  "Robotics & Automation",        "Robots",       "🤖", "Technology",       "ROBO",
   ["ISRG","ABB","PATH","TSLA","FANUY","BRZE"],                           "#c084fc",  1),
  ("iot",       "Internet of Things",           "IoT",          "📡", "Technology",       "SNSR",
   ["CSCO","TXN","MCHP","PTC","SWKS","QRVO"],                            "#38bdf8",  0),
  ("print3d",   "3D Printing",                  "3D Print",     "🖨", "Technology",       "PRNT",
   ["DDD","SSYS","MKFG","XMTR","NNDM","MTLS"],                           "#7c3aed", -1),
  ("saas",      "Software as a Service",        "SaaS",         "🧩", "Technology",       "IGV",
   ["CRM","NOW","WDAY","HUBS","ZM","BILL"],                               "#6ee7b7",  0),
  ("datacntr",  "Data Centers & Infra",         "DataCtrs",     "🏭", "Technology",       "SRVR",
   ["EQIX","DLR","NVDA","SMCI","VRT","ETN"],                              "#fca5a5",  1),
  ("memory",    "Memory & Data Storage",        "Memory",       "💾", "Memory & Storage", "MU",
   ["SNDK","WDC","STX","MU","MRVL","NTAP"],                               "#38bdf8",  1),
  ("fiber",     "Fiber Optics & Optical Net.",  "Fiber Optics", "🔆", "Fiber Optics",     "CIEN",
   ["AAOI","LITE","COHR","CIEN","VIAV"],                                  "#bbf7d0",  1),
  ("nuclear",   "Nuclear Energy",               "Nuclear",      "⚛", "Energy",            "NLR",
   ["CEG","VST","NRG","CCJ","BWXT","SMR"],                                "#fde68a",  1),
  ("uranium",   "Uranium Mining",               "Uranium",      "☢", "Energy",            "URNM",
   ["CCJ","NXE","DNN","UUUU","URG"],                                      "#f59e0b",  1),
  ("clean",     "Clean & Renewable Energy",     "Clean NRG",    "🌱", "Energy",            "ICLN",
   ["ENPH","FSLR","NEE","SEDG","RUN","BEP"],                              "#4ade80",  1),
  ("battery",   "Battery Technology",           "Battery",      "🔋", "Energy",            "LIT",
   ["TSLA","QS","ALB","SQM","FLNC","STEM"],                               "#34d399",  0),
  ("grid",      "Power Grid Modernization",     "Grid",         "🔌", "Energy",            "GRID",
   ["ETN","VRT","EMR","HUBB","PWR","GEV"],                                "#86efac",  1),
  ("oilgas",    "Traditional Oil & Gas",        "Oil & Gas",    "🛢", "Energy",            "XLE",
   ["XOM","CVX","COP","SLB","EOG","MPC"],                                 "#d97706", -1),
  ("minerals",  "Critical Minerals",            "Minerals",     "⛏", "Materials",         "COPX",
   ["FCX","MP","ALB","VALE","RIO","LTHM"],                                "#fb923c",  1),
  # ETFs removed and replaced with relevant individual stocks below:
  ("gold",      "Gold & Gold Miners",           "Gold",         "🥇", "Precious Metals",   "GLD",
   ["KGC","NEM","GOLD","AEM","WPM","FNV"],                                "#ffd700",  1),
  ("silver",    "Silver & Silver Miners",       "Silver",       "🥈", "Precious Metals",   "SLV",
   ["CDE","PAAS","AG","HL","MAG","WPM"],                                  "#e2e8f0",  1),
  ("jrgold",    "Junior Gold Miners",           "Jr. Gold",     "⛏", "Precious Metals",   "GDXJ",
   ["EGO","ORLA","KNT","MAI","NGD","AUMN"],                               "#fcd34d",  1),
  ("pgm",       "Platinum Group Metals",        "PGMs",         "⚗", "Precious Metals",   "PPLT",
   ["PLG","VALE","SBSW","IMPUY","ANGPY"],                                 "#cbd5e1",  0),
  ("broadmet",  "Broad Precious Metals",        "Prec. Metals", "🏅", "Precious Metals",   "GLTR",
   ["NEM","GOLD","AEM","PAAS","WPM","FNV"],                               "#f0abfc",  1),
  # ----------------------------------------------------------------------
  ("water",     "Water Management",             "Water",        "💧", "Utilities",         "PHO",
   ["AWK","XYL","WTRG","PNR","MSEX","CWCO"],                              "#0ea5e9",  0),
  ("space",     "Space Exploration",            "Space",        "🚀", "Industrials",       "UFO",
   ["ASTS","LUNR","RKLB","RDW","ASTR","SPCE"],                            "#8b5cf6",  1),
  ("defense",   "Defense & Military Tech",      "Defense",      "🛡", "Industrials",       "ITA",
   ["LMT","RTX","NOC","GD","BA","HII"],                                   "#ff6b35",  1),
  ("drones",    "Drones & Autonomous",          "Drones",       "🛸", "Industrials",       "IFLY",
   ["ACHR","JOBY","AVAV","KTOS","RCAT","UAVS"],                           "#f472b6",  1),
  ("reshoring", "Supply Chain Reshoring",       "Reshoring",    "🏗", "Industrials",       "PAVE",
   ["CAT","DE","URI","MLM","VMC","NUE"],                                   "#fdba74",  0),
  ("machinery", "Heavy Machinery & Infra",      "Machinery",    "🔧", "Industrials",       "XLI",
   ["CAT","DE","CMI","PCAR","TEX","WNC"],                                  "#94a3b8",  0),
]

# ── 1. Batch Fetch Market Caps ────────────────────────────────────────────────
print("Fetching Market Caps for weighting...")
mkt_caps = {}
all_tickers = list(set([t for theme in THEMES for t in theme[6]]))

# Chunk tickers into groups of 40 to avoid Yahoo API URL limits
for i in range(0, len(all_tickers), 40):
    chunk = ",".join(all_tickers[i:i+40])
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={chunk}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            for res in data["quoteResponse"]["result"]:
                # If Market Cap is missing, default to 1 so the math doesn't break
                mkt_caps[res["symbol"]] = res.get("marketCap", 1) 
    except Exception as e:
        print(f"  ERR fetching caps for {chunk[:20]}... : {e}")

# ── 2. Yahoo Finance Chart Fetcher ────────────────────────────────────────────
def fetch(ticker, period="2mo"):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?range={period}&interval=1d")
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        res = d["chart"]["result"][0]
        meta   = res["meta"]
        closes = res["indicators"]["quote"][0]["close"]
        
        valid_closes = [c for c in closes if c is not None]
        if not valid_closes:
            return None
        
        price = meta.get("regularMarketPrice") or valid_closes[-1]
        prev_close = meta.get("previousClose")
        
        if not prev_close or prev_close == 0:
            prev_close = valid_closes[-2] if len(valid_closes) > 1 else price
            
        return {"ts": res["timestamp"], "closes": closes, "price": price, "prev_close": prev_close}
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

def cap_weighted_avg(returns, caps):
    """Calculates the market-cap weighted average of a list of returns."""
    valid_pairs = [(r, c) for r, c in zip(returns, caps) if r is not None and c is not None and c > 0]
    if not valid_pairs:
        return None
    
    total_cap = sum([c for r, c in valid_pairs])
    weighted_sum = sum([r * c for r, c in valid_pairs])
    
    return round(weighted_sum / total_cap, 2)

# ── Reference timestamps ──────────────────────────────────────────────────────
now = datetime.datetime.utcnow()

days_to_fri = (now.weekday() - 4) % 7
last_fri = now - datetime.timedelta(days=7 if days_to_fri == 0 else days_to_fri)
month_end = now.replace(day=1) - datetime.timedelta(days=1)

def mkets(dt):
    return int(datetime.datetime(dt.year, dt.month, dt.day, 21, 0).timestamp())

ts_fri   = mkets(last_fri)
ts_month = mkets(month_end)

# ── Fetch SPY benchmark ───────────────────────────────────────────────────────
print("\nFetching SPY benchmark...")
spy_raw = fetch("SPY")
spy = {"d": None, "w": None, "m": None}
if spy_raw:
    c  = spy_raw["price"]
    pc = spy_raw["prev_close"]
    spy = {
        "d": pct(c, pc),
        "w": pct(c, price_on(spy_raw["ts"], spy_raw["closes"], ts_fri)),
        "m": pct(c, price_on(spy_raw["ts"], spy_raw["closes"], ts_month)),
    }
    print(f"  SPY → 1D={spy['d']}%  1W={spy['w']}%  1M={spy['m']}%")

_cache = {}
def fetch_cached(ticker):
    if ticker not in _cache:
        _cache[ticker] = fetch(ticker)
        time.sleep(0.3)
    return _cache[ticker]

# ── Process each theme ────────────────────────────────────────────────────────
results = []

for (tid, name, short, icon, sector, proxy_etf, constituents, color, ma) in THEMES:
    print(f"\n{name}")

    proxy_raw   = fetch_cached(proxy_etf)
    proxy_price = round(proxy_raw["price"], 2) if proxy_raw and proxy_raw["price"] else None
    spark5      = [round(x, 2) for x in proxy_raw["closes"] if x is not None][-5:] if proxy_raw else []

    all_r1D, all_r1W, all_r1M, theme_caps = [], [], [], []
    
    for ticker in constituents:
        raw = fetch_cached(ticker)
        cap = mkt_caps.get(ticker, 1) # Get the market cap we fetched earlier
        
        if not raw:
            continue
            
        cur = raw["price"]
        pc  = raw["prev_close"]
        
        r1D = pct(cur, pc)
        r1W = pct(cur, price_on(raw["ts"], raw["closes"], ts_fri))
        r1M = pct(cur, price_on(raw["ts"], raw["closes"], ts_month))
        
        print(f"  {ticker:8s}  Cap: {cap/1e9:>6.1f}B | 1D={str(r1D):>6}%  1W={str(r1W):>6}%  1M={str(r1M):>6}%")
        
        all_r1D.append(r1D)
        all_r1W.append(r1W)
        all_r1M.append(r1M)
        theme_caps.append(cap)

    # Use Cap-Weighted Math instead of Simple Equal Weighting
    r1D = cap_weighted_avg(all_r1D, theme_caps)
    r1W = cap_weighted_avg(all_r1W, theme_caps)
    r1M = cap_weighted_avg(all_r1M, theme_caps)

    rs1D = round(r1D - spy["d"], 2) if r1D is not None and spy["d"] is not None else None
    rs1W = round(r1W - spy["w"], 2) if r1W is not None and spy["w"] is not None else None
    rs1M = round(r1M - spy["m"], 2) if r1M is not None and spy["m"] is not None else None

    score = None
    if None not in (r1D, r1W, r1M, rs1D, rs1W, rs1M):
        ret_blend = r1D * 0.20 + r1W * 0.35 + r1M * 0.45
        rs_blend  = rs1D * 0.20 + rs1W * 0.35 + rs1M * 0.45
        score     = round(ret_blend * 0.45 + rs_blend * 0.35 + ma * 7, 1)

    print(f"  → WEIGHTED 1D={r1D}%  1W={r1W}%  1M={r1M}%  score={score}")

    results.append({
        "id": tid, "name": name, "short": short, "icon": icon,
        "sector": sector, "etf": proxy_etf, "stocks": constituents,
        "color": color, "ma": ma, "price": proxy_price,
        "ret1D": r1D, "ret1W": r1W, "ret1M": r1M,
        "rs1D": rs1D, "rs1W": rs1W, "rs1M": rs1M,
        "score": score, "spark5": spark5, "n_stocks": len(all_r1M),
    })

results.sort(key=lambda x: x["score"] if x["score"] is not None else -999, reverse=True)

output = {
    "updated":     now.strftime("%Y-%m-%d %H:%M UTC"),
    "methodology": "Market-Cap Weighted avg of constituent stocks · 1D = vs official prev close · 1W = vs last Friday · 1M = vs last month-end",
    "spy":         spy,
    "themes":      results,
}

with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅  Written data/market_data.json")
