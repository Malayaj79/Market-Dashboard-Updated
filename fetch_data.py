"""
Market Themes Momentum Dashboard — Data Fetcher
================================================
METHODOLOGY (matches original dashboard):
  - Fetches ALL constituent stocks for each theme (not just one proxy ETF)
  - Computes 1D / 1W / 1M returns for every stock individually:
      1D = regularMarketPrice vs regularMarketPreviousClose
           → true today's intraday move (matches what you see on any brokerage)
      1W = today vs last Friday's closing price
      1M = today vs last month-end closing price
  - Equal-weight averages all valid constituent returns → theme return
  - Relative Strength = theme return minus SPY return (same anchor)
  - Momentum Score = (RetBlend × 45%) + (RSBlend × 35%) + (MA × 7pts)
      where RetBlend = 1D×20% + 1W×35% + 1M×45%

TO UPDATE MA SIGNALS WEEKLY:
  Edit ma= in THEMES below (+1 bull, 0 neutral, -1 bear), then push.
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
  ("gold",      "Gold & Gold Miners",           "Gold",         "🥇", "Precious Metals",   "GLD",
   ["GLD","NEM","GOLD","AEM","WPM","FNV"],                                "#ffd700",  1),
  ("silver",    "Silver & Silver Miners",       "Silver",       "🥈", "Precious Metals",   "SLV",
   ["SLV","PAAS","AG","HL","MAG","WPM"],                                  "#e2e8f0",  1),
  ("jrgold",    "Junior Gold Miners",           "Jr. Gold",     "⛏", "Precious Metals",   "GDXJ",
   ["GDXJ","ORLA","KNT","MAI","NGD","AUMN"],                              "#fcd34d",  1),
  ("pgm",       "Platinum Group Metals",        "PGMs",         "⚗", "Precious Metals",   "PPLT",
   ["PPLT","PALL","SBSW","IMPUY","ANGPY"],                                "#cbd5e1",  0),
  ("broadmet",  "Broad Precious Metals",        "Prec. Metals", "🏅", "Precious Metals",   "GLTR",
   ["GLD","SLV","PPLT","PALL","WPM","FNV"],                               "#f0abfc",  1),
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

# ── Yahoo Finance fetcher ─────────────────────────────────────────────────────
def fetch(ticker, period="3mo"):
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
        # regularMarketPrice = live intraday price (or last trade if market closed)
        # regularMarketPreviousClose = official prior session close
        # Using these two gives true today's move (what you see on any brokerage)
        price      = meta.get("regularMarketPrice") or meta.get("previousClose")
        prev_close = meta.get("regularMarketPreviousClose") or meta.get("previousClose")
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

# ── Fetch SPY benchmark ───────────────────────────────────────────────────────
print("Fetching SPY benchmark...")
spy_raw = fetch("SPY")
spy = {"d": None, "w": None, "m": None}
if spy_raw:
    c  = spy_raw["price"]
    pc = spy_raw["prev_close"]   # official prior session close → true today's move
    spy = {
        "d": pct(c, pc),
        "w": pct(c, price_on(spy_raw["ts"], spy_raw["closes"], ts_fri)),
        "m": pct(c, price_on(spy_raw["ts"], spy_raw["closes"], ts_month)),
    }
    print(f"  SPY → 1D={spy['d']}% (vs prev close ${pc})  1W={spy['w']}%  1M={spy['m']}%")

# Cache to avoid re-fetching tickers shared across themes (e.g. NVDA, WPM)
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

    # Proxy ETF → sparkline & display price only
    proxy_raw   = fetch_cached(proxy_etf)
    proxy_price = round(proxy_raw["price"], 2) if proxy_raw and proxy_raw["price"] else None
    spark5      = [round(x, 2) for x in proxy_raw["closes"] if x is not None][-5:] if proxy_raw else []

    # Fetch every constituent and collect individual returns
    all_r1D, all_r1W, all_r1M = [], [], []
    for ticker in constituents:
        raw = fetch_cached(ticker)
        if not raw:
            continue
        cur = raw["price"]
        pc  = raw["prev_close"]   # official prior session close → true today's move
        r1D = pct(cur, pc)
        r1W = pct(cur, price_on(raw["ts"], raw["closes"], ts_fri))
        r1M = pct(cur, price_on(raw["ts"], raw["closes"], ts_month))
        print(f"  {ticker:8s}  1D={str(r1D):>7}%  1W={str(r1W):>7}%  1M={str(r1M):>7}%")
        if r1D is not None: all_r1D.append(r1D)
        if r1W is not None: all_r1W.append(r1W)
        if r1M is not None: all_r1M.append(r1M)

    # Equal-weight blend
    r1D = avg(all_r1D)
    r1W = avg(all_r1W)
    r1M = avg(all_r1M)

    # Relative Strength vs SPY
    rs1D = round(r1D - spy["d"], 2) if r1D is not None and spy["d"] is not None else None
    rs1W = round(r1W - spy["w"], 2) if r1W is not None and spy["w"] is not None else None
    rs1M = round(r1M - spy["m"], 2) if r1M is not None and spy["m"] is not None else None

    # Momentum Score
    score = None
    if None not in (r1D, r1W, r1M, rs1D, rs1W, rs1M):
        ret_blend = r1D * 0.20 + r1W * 0.35 + r1M * 0.45
        rs_blend  = rs1D * 0.20 + rs1W * 0.35 + rs1M * 0.45
        score     = round(ret_blend * 0.45 + rs_blend * 0.35 + ma * 7, 1)

    print(f"  → BLENDED  1D={r1D}%  1W={r1W}%  1M={r1M}%  score={score}  (n={len(all_r1M)} stocks)")

    results.append({
        "id": tid, "name": name, "short": short, "icon": icon,
        "sector": sector, "etf": proxy_etf, "stocks": constituents,
        "color": color, "ma": ma, "price": proxy_price,
        "ret1D": r1D, "ret1W": r1W, "ret1M": r1M,
        "rs1D": rs1D, "rs1W": rs1W, "rs1M": rs1M,
        "score": score, "spark5": spark5, "n_stocks": len(all_r1M),
    })

# ── Sort & write JSON ─────────────────────────────────────────────────────────
results.sort(key=lambda x: x["score"] if x["score"] is not None else -999, reverse=True)

output = {
    "updated":     now.strftime("%Y-%m-%d %H:%M UTC"),
    "methodology": "Equal-weight avg of all constituent stocks · 1D/1W/1M anchored to today's price",
    "spy":         spy,
    "themes":      results,
}

with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅  Written data/market_data.json  ({len(results)} themes)")
print(f"    Top 3: {', '.join(r['name'] for r in results[:3])}")
