"""
Market Themes — MA Breadth Dashboard — Data Fetcher
====================================================
No composite scoring, no resilience/emerging metrics. This does exactly one
thing: for each theme, what % of its constituents are trading above their
10/20/50-day moving average (plus raw "x/y" counts), and the same check for
SPY / QQQ / RSP as a reference bar (RSP included since it's equal-weight,
matching this dashboard's own constituent-averaging approach).

RULES:
  - Some tickers intentionally appear in more than one theme (dual-tag)
    where the company genuinely straddles two catalysts (e.g. RMBS in both
    AI Memory and Semiconductor IP). Expected, not a bug.
  - Stocks below $10M average daily dollar volume are skipped at fetch time
    (liquidity floor) — logged, not treated as an error.

Runs via GitHub Actions daily at 22:00 UTC (6pm ET, after US market close).
Writes data/market_data.json, which index.html reads.
"""

import json, datetime, time, urllib.request, os

THEMES = [
  # Format: (id, name, short, icon, sector, constituents, color)
  # Finalized theme taxonomy — catalyst-driven, cyclical/generic sectors removed.
  # Some tickers intentionally appear in more than one theme (dual-tag) where
  # the company genuinely straddles two catalysts (e.g. RMBS in both Memory
  # and Semiconductor IP). That's expected, not a bug.

  # ═══════════════════════════════════════════════════════════════════
  # AI — SOFTWARE / COMPUTE
  # ═══════════════════════════════════════════════════════════════════
  ("agenticai", "Agentic AI",                   "Agentic AI",  "🧠","AI Software",
   ["PATH","SOUN","FIVN","PEGA","RNG","DOCN","ZETA","FSLY"],      "#818cf8"),

  ("edgeai",    "Edge AI",                      "Edge AI",     "📱","AI Software",
   ["OUST","AMBA","QCOM","LSCC","SYNA"],                         "#a78bfa"),

  ("enterpriseaisoftware","Enterprise AI Software","Ent. AI SW","🗄","AI Software",
   ["SNOW","SAP","MDB","NOW","CRM"],                              "#60a5fa"),

  ("aihealthcare","AI Healthcare",              "AI Health",   "🧬","AI Software",
   ["RXRX","SDGR","CERT","ABSI","TEM","HTFL"],                    "#10b981"),

  ("aicompute", "AI Compute",                   "AI Compute",  "⚡","AI Hardware",
   ["CBRS","SMCI","DELL","HPE","NBIS","CRWV","APLD"],              "#00d4ff"),

  ("ai",        "Artificial Intelligence (Broad)","AI Broad",  "🤖","AI Software",
   ["AI","BBAI","PLTR","SOUN","UPST"],                             "#00b8d4"),

  # ═══════════════════════════════════════════════════════════════════
  # AI — SEMICONDUCTORS & HARDWARE SUPPLY CHAIN
  # ═══════════════════════════════════════════════════════════════════
  ("datacenterchips","Data Center Chips",       "DC Chips",    "💎","Semiconductors",
   ["NVDA","AMD","AVGO","MRVL","ALAB"],                            "#6366f1"),

  ("aimemory",  "AI Memory",                    "AI Memory",   "💾","Semiconductors",
   ["MU","RMBS","PENG","SNDK","SIMO","DRAM","STX","WDC"],          "#38bdf8"),

  ("semiconductorip","Semiconductor IP",        "Semi IP",     "📐","Semiconductors",
   ["ARM","CEVA","SNPS","CDNS","RMBS"],                            "#4f46e5"),

  ("ainetworking","AI Networking",              "AI Network",  "🔌","Semiconductors",
   ["ANET","CIEN","CRDO","EXTR","FFIV"],                           "#22d3ee"),

  ("photonics", "Photonics",                    "Photonics",   "🔆","Semiconductors",
   ["AXTI","COHR","LITE","AAOI","POET","FN"],                      "#bbf7d0"),

  ("aipackaging","AI Packaging",                "AI Package",  "📦","Semiconductors",
   ["AMKR","ASX","KLIC","VECO","ONTO"],                            "#f0abfc"),

  ("foundryservices","Foundry Services",        "Foundry",     "🏭","Semiconductors",
   ["TSM","GFS","UMC","TSEM","INTC"],                               "#94a3b8"),

  ("waferfabequipment","Wafer Fab Equipment",   "Wafer Equip", "🔭","Semiconductors",
   ["AMAT","LRCX","KLAC","ASML","ACLS","UCTT"],                     "#7c83fd"),

  ("smallcapsemis","Small Cap Semis",           "Small Semis", "🔬","Semiconductors",
   ["ALMU","POWI","NVTS","ON","MXL","TTMI","AOSL","AMBQ"],          "#c084fc"),

  ("semitest",  "Semiconductor Test",           "Semi Test",   "🧪","Semiconductors",
   ["FORM","AEHR","COHU","TER","KEYS"],                             "#67e8f9"),

  ("lidarlaser","Lidar / Laser",                "Lidar/Laser", "📡","Semiconductors",
   ["LASR"],                                                        "#f59e0b"),

  ("datastorage","Data Storage",                "Data Storage","💽","Semiconductors",
   ["QMCO","MRAM","P","BLZE"],                                      "#93c5fd"),

  ("fiberoptics","Fiber Optics (Legacy Telecom)","Fiber Optics","🌐","Semiconductors",
   ["APH","GLW","AME","IPGP","LUMN","OPTX","PLAB"],                 "#7dd3fc"),

  # ═══════════════════════════════════════════════════════════════════
  # AI — POWER, ENERGY, LAND & CONSTRUCTION (data center buildout)
  # ═══════════════════════════════════════════════════════════════════
  ("aipower",   "AI Power",                     "AI Power",    "🔋","AI Infrastructure",
   ["VRT","MOD","AEIS","NVT","POWL"],                               "#86efac"),

  ("aienergy",  "AI Energy",                    "AI Energy",   "⚡","AI Infrastructure",
   ["VST","CEG","NRG","GEV","FCEL","TLN","BE"],                     "#4ade80"),

  ("ailand",    "AI Land (Data Center REITs)",  "AI Land",     "🏗","AI Infrastructure",
   ["DLR","EQIX","IRM","APLD","SRVR"],                              "#fca5a5"),

  ("aiconstruction","AI Construction",          "AI Constr.",  "🏗","AI Infrastructure",
   ["EME","MTZ","PWR","FIX","STRL","AGX"],                          "#fdba74"),

  ("cooling",   "Data Center Cooling",          "Cooling",     "❄","AI Infrastructure",
   ["JCI","INV","CARR","AAON"],                                     "#0ea5e9"),

  ("datacenters","Data Centers (Crypto→AI Hosting)","DC Hosting","🖥","AI Infrastructure",
   ["SHAZ","WYFI","BRUN","WGMI","HUT","FRMI","CIFR","INOD","CORZ","IREN","KEEL","BTDR","NUAI","WULF"],
   "#f87171"),

  # ═══════════════════════════════════════════════════════════════════
  # ENERGY (non-AI-specific)
  # ═══════════════════════════════════════════════════════════════════
  ("nuclearenergy","Nuclear Energy",            "Nuclear",     "☢","Energy",
   ["SMR","OKLO","CCJ","LEU","BWXT"],                                "#fde68a"),

  ("nuclear",   "Uranium & Nuclear Fuel",       "Uranium",     "🪨","Energy",
   ["NLR","NNE","XE","URNM","DNN","UEC","UUUU"],                     "#f59e0b"),

  ("solar",     "Solar Energy",                 "Solar",       "☀","Energy",
   ["FSLR","ENLT","ENPH","RUN","SEDG","ARRY","TAN","NXT"],           "#fbbf24"),

  ("batteries", "Battery Technology",           "Battery",     "🔋","Energy",
   ["ABAT","HYLN","TE","AMPX","ENVX","FLNC"],                        "#34d399"),

  ("lithium",   "Lithium & Battery Materials",  "Lithium",     "⛏","Energy",
   ["ALB","LAC","LAR","LIT","SGML","SLI"],                           "#65a30d"),

  # ═══════════════════════════════════════════════════════════════════
  # DEFENSE / SPACE
  # ═══════════════════════════════════════════════════════════════════
  ("defense",   "Defense & Military Tech",      "Defense",     "🛡","Defense",
   ["LMT","RTX","NOC","LHX","GD","KTOS","AVAV"],                     "#ff6b35"),

  ("drones",    "Drones & Autonomous Systems",  "Drones",      "🛸","Defense",
   ["RCAT","ONDS","UMAC","DPRO","SWMR","FLY"],                       "#f472b6"),

  ("space",     "Space Exploration",            "Space",       "🚀","Defense",
   ["RKLB","ASTS","RDW","LUNR","VOYG","UFO","BKSY","PL","SATL","SIDU","YSS","FJET"],
   "#8b5cf6"),

  ("aerospace", "Aerospace",                     "Aerospace",   "✈","Defense",
   ["AXON","FTAI","HWM","BA"],                                       "#94a3b8"),

  ("evtol",     "eVTOL / Air Mobility",         "eVTOL",       "🚁","Defense",
   ["ACHR","JOBY"],                                                  "#c084fc"),

  ("satellites","Satellites",                   "Satellites",  "🛰","Defense",
   ["VIAV","SPIR","GSAT","IRDM","ECHO","VSAT"],                      "#7c3aed"),

  # ═══════════════════════════════════════════════════════════════════
  # CYBERSECURITY
  # ═══════════════════════════════════════════════════════════════════
  ("cybersecurity","Cybersecurity",              "Cyber",       "🔐","Software",
   ["CRWD","PANW","FTNT","OKTA","NET","ZS","AKAM"],                  "#22d3ee"),

  # ═══════════════════════════════════════════════════════════════════
  # CRYPTO / BLOCKCHAIN / FINTECH
  # ═══════════════════════════════════════════════════════════════════
  ("crypto",    "Crypto (Exchanges & Miners)",  "Crypto",      "₿","Fintech",
   ["COIN","MSTR","MARA","RIOT","CLSK"],                             "#f97316"),

  ("blockchain","Blockchain Infrastructure",    "Blockchain",  "🔗","Fintech",
   ["PURR","CRCL","BMNR","FIGR","COIN","SBET"],                      "#fb923c"),

  ("paymentprocessing","Payment Processing",    "Payments",    "💳","Fintech",
   ["CHYM","AFRM","PYPL","SEZL","XYZ","KLAR","UPST"],                 "#fdba74"),

  # ═══════════════════════════════════════════════════════════════════
  # EVs / AUTONOMY / ROBOTICS
  # ═══════════════════════════════════════════════════════════════════
  ("evs",       "Electric Vehicles",             "EVs",         "🚗","Consumer Tech",
   ["TSLA","RIVN","LCID","QS","PSNY","LI","NIO","XPEV"],              "#34d399"),

  ("selfdriving","Self-Driving / Autonomy",      "Self-Driving","🚘","Consumer Tech",
   ["AUR","PONY"],                                                   "#5eead4"),

  ("robotics",  "Robotics & Automation",         "Robots",      "🦾","Consumer Tech",
   ["SYM","SERV","ISRG","NOVT","CGNX","BOT","RR","PDYN"],             "#c084fc"),

  ("quantumcomputing","Quantum Computing",       "Quantum",     "⚛","Consumer Tech",
   ["IONQ","RGTI","QBTS","QUBT","ARQQ","INFQ","QSI"],                 "#e879f9"),

  ("print3d",   "3D Printing",                   "3D Print",    "🖨","Consumer Tech",
   ["DDD","VELO"],                                                    "#7c3aed"),

  ("genomics",  "Genomics & Gene Editing",       "Genomics",    "🧬","Healthcare",
   ["ARKG","BEAM","CRSP"],                                            "#3b82f6"),

  ("digitalhealth","Digital Health",             "Digital Hlth","🏥","Healthcare",
   ["HIMS","DOCS","GH","OSCR","PHR"],                                 "#f472b6"),

  # ═══════════════════════════════════════════════════════════════════
  # MISC — GEOGRAPHY, DELIVERY, LEGALIZED VICES
  # ═══════════════════════════════════════════════════════════════════
  ("chinanames","China Names",                   "China",       "🇨🇳","Geography",
   ["BABA","BIDU","FUTU","FXI","JD","PDD"],                           "#dc2626"),

  ("delivery",  "Delivery & Rideshare",          "Delivery",    "🛵","Consumer",
   ["LYFT","CART","UBER"],                                            "#fb7185"),

  ("sportsbetting","Sports Betting",             "SportsBet",   "🎰","Consumer",
   ["BETZ","DKNG","FUBO","PENN"],                                     "#eab308"),

  ("weed",      "Cannabis",                      "Weed",        "🌿","Consumer",
   ["ACB","CGC","CURA","MJ","MSOS","TLRY"],                           "#4d7c0f"),

  # ═══════════════════════════════════════════════════════════════════
  # MEGA CAP REFERENCE (not a theme per se — kept for context)
  # ═══════════════════════════════════════════════════════════════════
  ("mag7",      "Mag 7 Stocks",                  "Mag 7",       "⭐","Mega Cap",
   ["MSFT","AMZN","GOOGL","META","AAPL","NFLX"],                      "#facc15"),
]
# ── Note dual-tagged stocks (intentional, not an error) ────────────────────
from collections import defaultdict
_seen = defaultdict(list)
for (tid, name, short, icon, sector, stocks, color) in THEMES:
    for s in stocks:
        _seen[s].append(tid)
_dupes = {s: ts for s, ts in _seen.items() if len(ts) > 1}
if _dupes:
    print(f"ℹ Dual-tagged stocks (intentional — company straddles two catalysts):")
    for s, ts in _dupes.items():
        print(f"  {s} in {ts}")
else:
    print(f"✅ No dual-tagged stocks across {len(THEMES)} themes")

# ── Yahoo Finance fetcher ────────────────────────────────────────────────────
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
        closes = res["indicators"]["quote"][0]["close"]
        vols   = res["indicators"]["quote"][0].get("volume", [])

        price = meta.get("regularMarketPrice") or meta.get("previousClose")

        # Avg daily dollar volume (last 20 days) — liquidity floor
        adv = None
        if vols and closes:
            pairs = [(v, c) for v, c in zip(vols, closes)
                     if v is not None and c is not None and v > 0][-20:]
            if pairs:
                adv = sum(v * c for v, c in pairs) / len(pairs)

        return {"closes": closes, "price": price, "adv": adv}
    except Exception as e:
        print(f"    ERR {ticker}: {e}")
        return None

# ── MA Breadth (10/20/50) ────────────────────────────────────────────────────
def above_ma(raw, period):
    """1 if current price is above its N-day SMA, 0 if below, None if
    insufficient data."""
    if not raw or not raw.get("price"):
        return None
    cl = [c for c in raw["closes"] if c is not None]
    if len(cl) < period:
        return None
    sma = sum(cl[-period:]) / period
    return 1 if raw["price"] > sma else 0

def ma_breadth_bundle(raw):
    return {p: above_ma(raw, p) for p in (10, 20, 50)}

def avg(values):
    vals = [v for v in values if v is not None]
    return round(sum(vals) / len(vals), 1) if vals else None

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache = {}
def fetch_cached(ticker):
    if ticker not in _cache:
        _cache[ticker] = fetch(ticker)
        time.sleep(0.3)
    return _cache[ticker]

# ── SPY / QQQ / RSP benchmarks (pinned reference bar) ────────────────────────
# RSP is the fairer breadth benchmark since it's equal-weight, matching this
# dashboard's own equal-weight constituent blending. QQQ is kept alongside
# since most themes here skew tech/AI.
print("Fetching SPY / QQQ / RSP benchmarks...")
benchmarks = {}
for bname in ("SPY", "QQQ", "RSP"):
    braw = fetch(bname)
    if braw:
        mb = ma_breadth_bundle(braw)
        benchmarks[bname] = {"price": braw["price"], "ma10": mb[10], "ma20": mb[20], "ma50": mb[50]}
        print(f"  {bname}  price={braw['price']}  above10MA={mb[10]}  above20MA={mb[20]}  above50MA={mb[50]}")
    else:
        benchmarks[bname] = {"price": None, "ma10": None, "ma20": None, "ma50": None}

# ── Process themes ────────────────────────────────────────────────────────────
results = []
ADV_MIN = 10_000_000   # $10M minimum average daily dollar volume

for (tid, name, short, icon, sector, constituents, color) in THEMES:
    print(f"\n{name}")

    all_ma10, all_ma20, all_ma50 = [], [], []
    all_tickers_used = []

    for ticker in constituents:
        raw = fetch_cached(ticker)
        if not raw:
            continue

        adv = raw.get("adv")
        if adv is not None and adv < ADV_MIN:
            print(f"  {ticker:6s}  SKIP — ADV ${adv/1e6:.1f}M < $10M threshold")
            continue

        mab = ma_breadth_bundle(raw)
        print(f"  {ticker:6s}  10MA={mab[10]}  20MA={mab[20]}  50MA={mab[50]}")

        all_ma10.append(mab[10]); all_ma20.append(mab[20]); all_ma50.append(mab[50])
        all_tickers_used.append(ticker)

    def _ma_pct_and_count(flags):
        valid = [f for f in flags if f is not None]
        if not valid:
            return None, None
        pct_val = round(sum(valid) / len(valid) * 100, 1)
        cnt_str = f"{sum(valid)}/{len(valid)}"
        return pct_val, cnt_str

    ma10_pct, ma10_cnt = _ma_pct_and_count(all_ma10)
    ma20_pct, ma20_cnt = _ma_pct_and_count(all_ma20)
    ma50_pct, ma50_cnt = _ma_pct_and_count(all_ma50)

    n = len(all_tickers_used)
    print(f"  → 10MA={ma10_pct}%  20MA={ma20_pct}%  50MA={ma50_pct}%  (n={n})")

    results.append({
        "id": tid, "name": name, "short": short, "icon": icon,
        "sector": sector, "stocks": constituents,
        "color": color,
        "ma10_pct": ma10_pct, "ma10_cnt": ma10_cnt,
        "ma20_pct": ma20_pct, "ma20_cnt": ma20_cnt,
        "ma50_pct": ma50_pct, "ma50_cnt": ma50_cnt,
        "ma_detail": [
            {"ticker": t, "ma10": m10, "ma20": m20, "ma50": m50}
            for t, m10, m20, m50 in zip(all_tickers_used, all_ma10, all_ma20, all_ma50)
        ],
        "n_stocks": n,
    })

# ── Write ──────────────────────────────────────────────────────────────────────
now = datetime.datetime.now(datetime.timezone.utc)

output = {
    "updated":     now.strftime("%Y-%m-%d %H:%M UTC"),
    "methodology": "MA breadth only — % of constituents above their 10/20/50-day MA. No composite scoring.",
    "benchmarks":  benchmarks,
    "themes":      results,
}

os.makedirs("data", exist_ok=True)
with open("data/market_data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\n✅  Written data/market_data.json  ({len(results)} themes)")
