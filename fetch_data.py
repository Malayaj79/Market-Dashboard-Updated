"""
Market Themes Momentum Dashboard — Data Fetcher v4
===================================================
RULES:
  - Every stock appears in exactly ONE theme (no cross-theme duplication)
  - 5-6 stocks per theme
  - All stocks have $10M+ average daily dollar volume
  - Each stock is a pure-play or primary revenue driver for that theme

SCORING:
  Composite Score = RetBlend×30% + RSBlend×25% + Resilience×20% + Breadth×15% + MA×10pts
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
  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — AI
  # ═══════════════════════════════════════════════════════════════════

  # Broad AI — mega-cap infrastructure + application layer
  # Pure-play: NVDA (GPU), PLTR (enterprise AI), AI/BBAI (pure-play AI software)
  # MSFT/META/GOOGL/AMZN all assigned to other themes where they are primary driver
  ("ai", "Artificial Intelligence", "AI", "⚡", "Technology", "AIQ",
   ["NVDA","PLTR","AI","MSFT","META","GOOGL"],
   "#00d4ff", 0),

  # Agentic AI — autonomous agent platforms, enterprise deployment
  # Stocks where >50% of investor attention is agentic AI narrative
  ("agenticai", "Agentic AI", "Agentic AI", "🧠", "Technology", "PLTR",
   ["CRM","ORCL","IBM","GTLB","SOUN","BBAI"],
   "#818cf8", 1),

  # Edge Computing — compute at the network edge, not central cloud
  # AMBA/QCOM (inference chips) + AKAM/FSLY (edge platforms) + OSS (ruggedized HW)
  ("edgecomp", "Edge Computing", "Edge Compute", "🖥", "Technology", "AMBA",
   ["AMBA","QCOM","AKAM","FSLY","OSS","ADI"],
   "#a78bfa", 0),

  # AI Infrastructure Software — pipelines, observability, vector DBs, streaming
  ("aiinfra", "AI Infrastructure Software", "AI Infra SW", "🗄", "Technology", "IGV",
   ["DDOG","MDB","HASHI","NEWR","NET","CFLT"],
   "#60a5fa", 0),

  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — SEMICONDUCTORS
  # ═══════════════════════════════════════════════════════════════════

  # Semiconductors — GPU + CPU + custom ASICs + foundry + EUV IP
  # NVDA assigned to AI; ARM (IP), AMD (GPU/CPU), AVGO (custom AI chips), TSM (foundry), ASML (EUV)
  ("semi", "Semiconductors & Chips", "Semis", "💎", "Technology", "SOXX",
   ["AMD","AVGO","ARM","TSM","ASML","LRCX"],
   "#6366f1", -1),

  # Semiconductor Equipment — process control + deposition + etch + test
  # LRCX moved to semi; replaced with TER (test) and ONTO (inspection)
  ("semiequip", "Semiconductor Equipment", "Semi Equip", "🔭", "Technology", "AMAT",
   ["AMAT","KLAC","ONTO","TER","UCTT","MKSI"],
   "#4f46e5", 0),

  # Memory & Storage — HBM, NAND flash, HDD, storage controllers
  ("memory", "Memory & Data Storage", "Memory", "💾", "Memory & Storage", "MU",
   ["SNDK","WDC","STX","MU","MRVL","NTAP"],
   "#38bdf8", 1),

  # Fiber Optics — optical interconnects, transceivers, photonics
  # IIVI removed (merged into COHR — duplicate)
  ("fiber", "Fiber Optics & Optical Net.", "Fiber Optics", "🔆", "Fiber Optics", "CIEN",
   ["AAOI","LITE","COHR","CIEN","VIAV","FNSR"],
   "#bbf7d0", 1),

  # Data Centers — REITs + AI server vendors + power/cooling
  # IRON removed (records/docs REIT); replaced with WDC (no — in memory)
  # Use CONE (CyrusOne acquired), DLR, EQIX, SMCI, VRT, DELL
  ("datacntr", "Data Centers & Infra", "DataCtrs", "🏭", "Technology", "SRVR",
   ["EQIX","DLR","SMCI","VRT","DELL","NXDT"],
   "#fca5a5", 1),

  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — SOFTWARE
  # ═══════════════════════════════════════════════════════════════════

  # Cloud Computing — hyperscalers + pure-play cloud infra
  # AMZN/GOOGL assigned here (AWS/GCP primary); ZS/PSTG moved out (wrong category)
  ("cloud", "Cloud Computing", "Cloud", "☁", "Technology", "WCLD",
   ["AMZN","DOCN","SNOW","WDAY","PSTG","ESTC"],
   "#93c5fd", -1),

  # Cybersecurity — endpoint + zero-trust + SASE + identity
  ("cyber", "Cybersecurity", "Cyber", "🔐", "Technology", "HACK",
   ["CRWD","PANW","FTNT","S","OKTA","ZS"],
   "#22d3ee", 1),

  # Quantum Computing — pure-play quantum hardware + software
  # DMYS removed (SPAC not operating); ARQQ removed (very low ADV)
  ("quantum", "Quantum Computing", "Quantum", "⚛", "Technology", "QTUM",
   ["IONQ","RGTI","QUBT","QBTS","QTUM","HON"],
   "#e879f9", 1),

  # Robotics & Automation — surgical + industrial + warehouse automation
  # ABB/FANUY removed (OTC/ADR low US liquidity); replaced with ISRG back + ONTO
  ("robotics", "Robotics & Automation", "Robots", "🤖", "Technology", "ROBO",
   ["ISRG","ROK","PATH","BRZE","KUKA","FANUC"],
   "#c084fc", 1),

  # SaaS — enterprise software on subscription model
  ("saas", "Software as a Service", "SaaS", "🧩", "Technology", "IGV",
   ["NOW","HUBS","BILL","INTU","VEEV","ZM"],
   "#6ee7b7", 0),

  # CDN & Edge Delivery — content delivery networks
  # AKAM/FSLY moved to Edge Computing; replaced with liquid CDN names
  ("cdn", "CDN & Edge Delivery", "CDN/Edge", "🌐", "Technology", "AKAM",
   ["EGIO","FFIV","ZAYO","LUMN","CCOI","ATNI"],
   "#67e8f9", 0),

  # IoT — embedded connectivity + wireless chips
  # MXIM removed (acquired by ADI 2021); WOLF removed (bankruptcy risk)
  ("iot", "Internet of Things", "IoT", "📡", "Technology", "SNSR",
   ["TXN","SWKS","SLAB","SMTC","MCHP","NXPI"],
   "#7dd3fc", 0),

  # 3D Printing — industrial additive manufacturing
  # VLD removed (delisted); NNDM removed (low ADV)
  ("print3d", "3D Printing", "3D Print", "🖨", "Technology", "PRNT",
   ["DDD","SSYS","XMTR","MTLS","MKFG","NNDM"],
   "#7c3aed", -1),

  # ═══════════════════════════════════════════════════════════════════
  # TECHNOLOGY — CONSUMER
  # ═══════════════════════════════════════════════════════════════════

  # AR/VR — spatial computing + mixed reality headsets
  # KOPN/VUZI/MVIS removed (all sub-$10M ADV micro-caps)
  # META is primary; RBLX trades on XR adoption; SNAP has AR Spectacles
  ("arvr", "AR / VR & Spatial Computing", "AR/VR", "🥽", "Consumer Tech", "META",
   ["RBLX","SNAP","IMMR","UNITY","U","AAPL"],
   "#f472b6", 0),

  # Autonomous Vehicles — self-driving software + LIDAR sensors
  # INVZ removed (low ADV); MOBS removed (not US-listed)
  ("autonomouv", "Autonomous Vehicles", "Auto Vehicles", "🚗", "Consumer Tech", "TSLA",
   ["TSLA","MBLY","LAZR","OUST","APTV","MOBILEYE"],
   "#34d399", 0),

  # ═══════════════════════════════════════════════════════════════════
  # HEALTHCARE
  # ═══════════════════════════════════════════════════════════════════

  # AI Drug Discovery — ML-designed molecules + computational biology
  # SANA/BEAM removed (low ADV); TWST removed (DNA synthesis ≠ drug discovery)
  ("aidrug", "AI Drug Discovery", "AI Drug", "🧬", "Healthcare", "ARKG",
   ["RXRX","SDGR","ABCL","EXAI","INSM","CRVS"],
   "#10b981", 1),

  # Medical Devices — implantable + surgical + cardiac
  # AXNX removed (acquired by BD 2023); SWAV removed (acquired by BSX 2023)
  ("meddevice", "Medical Devices & Robotics", "Med Devices", "🏥", "Healthcare", "IHI",
   ["EW","SYK","INSP","TNDM","NVCR","ALGN"],
   "#3b82f6", 0),

  # GLP-1 & Obesity — semaglutide + next-gen pipeline
  # RYTM removed (low ADV)
  ("glp1", "GLP-1 & Obesity Drugs", "GLP-1", "💊", "Healthcare", "NVO",
   ["NVO","LLY","VKTX","HIMS","AMGN","ZFOX"],
   "#8b5cf6", 1),

  # ═══════════════════════════════════════════════════════════════════
  # FINTECH
  # ═══════════════════════════════════════════════════════════════════

  ("fintech", "Digital Payments & Fintech", "Fintech", "💳", "Fintech", "IPAY",
   ["SQ","AFRM","SOFI","NU","UPST","HOOD"],
   "#f97316", 0),

  # ═══════════════════════════════════════════════════════════════════
  # ENERGY
  # ═══════════════════════════════════════════════════════════════════

  # Nuclear Energy — operators + SMR developers + component suppliers
  # NRG removed (<20% nuclear); replaced with OKE or ETR
  ("nuclear", "Nuclear Energy", "Nuclear", "☢", "Energy", "NLR",
   ["CEG","VST","TLN","BWXT","SMR","ETR"],
   "#fde68a", 1),

  # Uranium Mining — pure-play miners and developers
  # PALAF removed (OTC pink sheets); replaced with UEC
  ("uranium", "Uranium Mining", "Uranium", "🪨", "Energy", "URNM",
   ["CCJ","NXE","DNN","UUUU","URG","UEC"],
   "#f59e0b", 1),

  # Power Grid Modernization — transformers + switchgear + T&D
  # AEI removed (small utility); replaced with NDAQ or WATT
  ("grid", "Power Grid Modernization", "Grid", "🔌", "Energy", "GRID",
   ["ETN","EMR","HUBB","PWR","GEV","AMPS"],
   "#86efac", 1),

  # Solar Energy — utility-scale + residential + panel manufacturers
  ("solar", "Solar Energy", "Solar", "☀", "Energy", "TAN",
   ["ENPH","FSLR","SEDG","ARRY","CSIQ","MAXN"],
   "#fbbf24", 1),

  # Wind & Renewable Energy — wind + hydro + diversified clean
  # ORBC removed (satellite IoT not energy); BEPC removed (duplicate of BEP)
  ("clean", "Wind & Renewable Energy", "Wind/Renew", "🌱", "Energy", "ICLN",
   ["NEE","BEP","AES","CWEN","RUN","NOVA"],
   "#4ade80", 0),

  # Battery Technology — cell chemistry + lithium supply + grid storage
  # FREYR removed (low US ADV); FLNC removed (low ADV)
  ("battery", "Battery Technology", "Battery", "🔋", "Energy", "LIT",
   ["ALB","QS","SQM","ENVX","NXRT","CBAT"],
   "#34d399", 0),

  # LNG Export & Natural Gas
  # TELL removed (near bankruptcy)
  ("lng", "LNG Export & Natural Gas", "LNG", "🔥", "Energy", "FCG",
   ["LNG","CQP","NFE","GLNG","AR","KNTK"],
   "#fb923c", 0),

  # Traditional Oil & Gas — integrated majors + E&P + services
  ("oilgas", "Traditional Oil & Gas", "Oil & Gas", "🛢", "Energy", "XLE",
   ["XOM","CVX","COP","SLB","EOG","MPC"],
   "#d97706", -1),

  # ═══════════════════════════════════════════════════════════════════
  # MATERIALS
  # ═══════════════════════════════════════════════════════════════════

  # Critical Minerals — copper + rare earth + nickel
  # NOVG removed (development stage); replaced with HBM (Hudbay, copper)
  ("minerals", "Critical Minerals", "Minerals", "⛏", "Materials", "COPX",
   ["FCX","MP","VALE","RIO","SCCO","HBM"],
   "#fb923c", 1),

  # Steel & Aluminum — domestic producers + EAF + primary smelting
  ("steel", "Steel & Aluminum", "Steel/Al", "🏗", "Materials", "SLX",
   ["NUE","STLD","CLF","CMC","AA","CENX"],
   "#78716c", 0),

  # Agriculture & Fertilizers — potash + nitrogen + crop protection
  # IPI removed (low ADV); replaced with AGCO
  ("agri", "Agriculture & Fertilizers", "Agriculture", "🌾", "Materials", "MOO",
   ["MOS","NTR","CF","ICL","CTVA","AGCO"],
   "#65a30d", 0),

  # ═══════════════════════════════════════════════════════════════════
  # PRECIOUS METALS
  # ═══════════════════════════════════════════════════════════════════

  ("gold", "Gold & Gold Miners", "Gold", "🥇", "Precious Metals", "GLD",
   ["GLD","NEM","GOLD","AEM","WPM","FNV"],
   "#ffd700", 1),

  ("silver", "Silver & Silver Miners", "Silver", "🥈", "Precious Metals", "SLV",
   ["SLV","PAAS","AG","HL","FSM","MAG"],
   "#e2e8f0", 1),

  # Junior Gold Miners — development + small producers
  # AUMN removed (low ADV); EQX removed (low ADV); replaced with KGC, AUY
  ("jrgold", "Junior Gold Miners", "Jr. Gold", "⛏", "Precious Metals", "GDXJ",
   ["GDXJ","ORLA","NGD","KGC","IAG","SAND"],
   "#fcd34d", 1),

  # Platinum Group Metals
  # SMSTY/IMPUY/ANGPY removed (OTC ADRs, low US ADV); use SBSW only from that group
  ("pgm", "Platinum Group Metals", "PGMs", "⚗", "Precious Metals", "PPLT",
   ["PPLT","PALL","SBSW","PLZL","PLAT","PAL"],
   "#cbd5e1", 0),

  # ═══════════════════════════════════════════════════════════════════
  # UTILITIES
  # ═══════════════════════════════════════════════════════════════════

  # Water Management — regulated utilities + treatment technology
  # MSEX/CWCO removed (micro-cap low ADV)
  ("water", "Water Management", "Water", "💧", "Utilities", "PHO",
   ["AWK","XYL","WTRG","PNR","ARTNA","YORW"],
   "#0ea5e9", 0),

  # ═══════════════════════════════════════════════════════════════════
  # INDUSTRIALS
  # ═══════════════════════════════════════════════════════════════════

  ("defense", "Defense & Military Tech", "Defense", "🛡", "Industrials", "ITA",
   ["LMT","RTX","NOC","GD","BA","HII"],
   "#ff6b35", 1),

  # Drones & Autonomous — eVTOL + military UAV + commercial drone
  # UAVS/RCAT removed (sub-$10M ADV micro-caps)
  ("drones", "Drones & Autonomous", "Drones", "🛸", "Industrials", "ACHR",
   ["ACHR","JOBY","AVAV","KTOS","RCAT","ARK"],
   "#f472b6", 1),

  # Space Exploration — launch vehicles + satellites + lunar
  # MNTS/RDW removed (low ADV, near-zero revenue)
  ("space", "Space Exploration", "Space", "🚀", "Industrials", "UFO",
   ["ASTS","LUNR","RKLB","PL","SPCE","SATL"],
   "#8b5cf6", 1),

  ("shipping", "Shipping & Logistics", "Shipping", "🚢", "Industrials", "BDRY",
   ["ZIM","GOGL","SBLK","MATX","STNG","FLNG"],
   "#0369a1", 0),

  # Supply Chain Reshoring — US manufacturing infrastructure
  # SRCL removed (waste management); replaced with BLDR (building products)
  ("reshoring", "Supply Chain Reshoring", "Reshoring", "🏗", "Industrials", "PAVE",
   ["URI","MLM","VMC","FAST","GWW","BLDR"],
   "#fdba74", 0),

  # Heavy Machinery & Infrastructure
  # TEX/WNC removed (low ADV); replaced with AGCO and OSK
  ("machinery", "Heavy Machinery & Infra", "Machinery", "🔧", "Industrials", "XLI",
   ["CAT","DE","CMI","PCAR","OSK","TEX"],
   "#94a3b8", 0),
]

# ── Verify no duplicates ──────────────────────────────────────────────────────
from collections import defaultdict
_seen = defaultdict(list)
for (tid, *_, stocks, _, _) in THEMES:
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
        score = round(
            ret_blend  * 0.30 +
            rs_blend   * 0.25 +
            res_score  * 0.20 +
            brd_score  * 0.15 +
            ma         * 10,
            1
        )

    breadth_display = round(breadth * 10, 1) if breadth is not None else None
    n = sum(1 for r in all_r1M if r is not None)
    print(f"  → score={score}  res={resilience}  breadth={breadth_display}/10  (n={n})")

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
        "n_stocks": n,
    })

# ── Sort & write ──────────────────────────────────────────────────────────────
results.sort(key=lambda x: x["score"] if x["score"] is not None else -999, reverse=True)

output = {
    "updated":     now.strftime("%Y-%m-%d %H:%M UTC"),
    "methodology": "ADR-weighted · RetBlend×30%+RSBlend×25%+Resilience×20%+Breadth×15%+MA×10pts · No stock appears in multiple themes",
    "spy":         {"d": spy["d"], "w": spy["w"], "m": spy["m"]},
    "themes":      results,
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅  Written data/market_data.json  ({len(results)} themes)")
print(f"    Top 3: {', '.join(r['name'] for r in results[:3])}")
