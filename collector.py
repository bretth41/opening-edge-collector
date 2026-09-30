import asyncio, os, re, sqlite3, time
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
import httpx

ET=ZoneInfo("America/New_York")
DATA_DIR=Path(os.getenv("EDGE_DATA_DIR","/data" if Path("/data").exists() else "."))
DATA_DIR.mkdir(parents=True,exist_ok=True)
DB_PATH=Path(os.getenv("EDGE_DB",str(DATA_DIR/"opening_edge_v2.db")))
UW_TOKEN=os.environ.get("UW_TOKEN")

# Existing broad aggregate collection (all expiries combined).
TICKERS=[x.strip() for x in os.getenv("EDGE_TICKERS","SPY,SPXW").split(",") if x.strip()]

# NEW: expiry-isolated structure collection.
# Default to SPY because this is the direct analogue to the Quant SPY interval maps
# and keeps API usage modest. Set EDGE_EXPIRY_TICKERS=SPY,SPXW later if desired.
EXPIRY_TICKERS=[x.strip() for x in os.getenv("EDGE_EXPIRY_TICKERS","SPY").split(",") if x.strip()]

# Collect the first N listed expiries beginning with today's expiry when available.
# With 2, this normally means 0DTE + 1DTE. Around weekends/holidays the second
# listed expiry may have calendar_dte > 1; we store both expiry_rank and calendar_dte
# so this is explicit rather than silently mislabelled.
EXPIRY_SLOTS=int(os.getenv("EDGE_EXPIRY_SLOTS","2"))

POLL_SECONDS=int(os.getenv("EDGE_POLL_SECONDS","60"))
UW_BASE="https://api.unusualwhales.com"
EXPOSURE_URL=UW_BASE+"/api/stock/{ticker}/spot-exposures/strike"
MARKET_START,MARKET_END=dtime(9,25),dtime(16,0)
OPTION_RE=re.compile(r"^(?P<root>[A-Z]+)(?P<date>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")

# Cache listed expiries once per ticker/trading date.
_EXPIRY_CACHE={}

SCHEMA="""
CREATE TABLE IF NOT EXISTS strike_exposure (
 observed_at_et TEXT NOT NULL, session_window TEXT NOT NULL, source_time TEXT, source_date TEXT,
 ticker TEXT NOT NULL, strike REAL NOT NULL, underlying_price REAL,
 call_delta_oi REAL,call_delta_vol REAL,call_delta_ask REAL,call_delta_bid REAL,
 put_delta_oi REAL,put_delta_vol REAL,put_delta_ask REAL,put_delta_bid REAL,
 call_gamma_oi REAL,call_gamma_vol REAL,call_gamma_ask REAL,call_gamma_bid REAL,
 put_gamma_oi REAL,put_gamma_vol REAL,put_gamma_ask REAL,put_gamma_bid REAL,
 call_vanna_oi REAL,call_vanna_vol REAL,call_vanna_ask REAL,call_vanna_bid REAL,
 put_vanna_oi REAL,put_vanna_vol REAL,put_vanna_ask REAL,put_vanna_bid REAL,
 call_charm_oi REAL,call_charm_vol REAL,call_charm_ask REAL,call_charm_bid REAL,
 put_charm_oi REAL,put_charm_vol REAL,put_charm_ask REAL,put_charm_bid REAL,
 PRIMARY KEY(observed_at_et,ticker,strike));
CREATE INDEX IF NOT EXISTS ix_exposure_ticker_time ON strike_exposure(ticker,observed_at_et);

-- NEW: expiry-isolated strike structure.
-- Stores raw Greeks from UW plus per-side OI/volume/ask/bid volume from the
-- corresponding expiry's option contracts. This preserves the source inputs so
-- we can derive/test GEX/DEX/CHEX/VEX conventions later without baking assumptions
-- into collection.
CREATE TABLE IF NOT EXISTS expiry_strike_structure (
 observed_at_et TEXT NOT NULL,
 session_window TEXT NOT NULL,
 ticker TEXT NOT NULL,
 expiry TEXT NOT NULL,
 expiry_rank INTEGER NOT NULL,
 calendar_dte INTEGER NOT NULL,
 strike REAL NOT NULL,
 underlying_price REAL,

 call_delta REAL, put_delta REAL,
 call_gamma REAL, put_gamma REAL,
 call_charm REAL, put_charm REAL,
 call_vanna REAL, put_vanna REAL,
 call_vega REAL, put_vega REAL,
 call_theta REAL, put_theta REAL,
 call_rho REAL, put_rho REAL,
 call_volatility REAL, put_volatility REAL,

 call_open_interest REAL, put_open_interest REAL,
 call_volume REAL, put_volume REAL,
 call_ask_volume REAL, put_ask_volume REAL,
 call_bid_volume REAL, put_bid_volume REAL,
 call_mid_volume REAL, put_mid_volume REAL,

 PRIMARY KEY(observed_at_et,ticker,expiry,strike)
);
CREATE INDEX IF NOT EXISTS ix_expiry_structure_time
 ON expiry_strike_structure(ticker,expiry,observed_at_et);
CREATE INDEX IF NOT EXISTS ix_expiry_structure_rank
 ON expiry_strike_structure(ticker,expiry_rank,observed_at_et,strike);

CREATE TABLE IF NOT EXISTS underlying_snapshot(
 observed_at_et TEXT NOT NULL,session_window TEXT NOT NULL,ticker TEXT NOT NULL,price REAL,
 PRIMARY KEY(observed_at_et,ticker));

CREATE TABLE IF NOT EXISTS option_snapshot(
 observed_at_et TEXT NOT NULL,session_window TEXT NOT NULL,underlying TEXT NOT NULL,
 option_symbol TEXT NOT NULL,expiry TEXT NOT NULL,option_type TEXT NOT NULL,strike REAL NOT NULL,
 bid REAL,ask REAL,mid REAL,last_price REAL,implied_volatility REAL,
 delta REAL,gamma REAL,theta REAL,vega REAL,rho REAL,open_interest REAL,volume REAL,
 nbbo_bid_size REAL,nbbo_ask_size REAL,PRIMARY KEY(observed_at_et,option_symbol));
CREATE INDEX IF NOT EXISTS ix_option_snapshot_time ON option_snapshot(observed_at_et,underlying,strike);

CREATE TABLE IF NOT EXISTS option_trade(
 executed_at TEXT NOT NULL,observed_at_et TEXT NOT NULL,session_window TEXT NOT NULL,
 option_symbol TEXT NOT NULL,expiry TEXT,option_type TEXT,strike REAL,price REAL,premium REAL,size REAL,
 nbbo_bid REAL,nbbo_ask REAL,nbbo_bid_size REAL,nbbo_ask_size REAL,implied_volatility REAL,
 delta REAL,gamma REAL,theta REAL,vega REAL,rho REAL,underlying_price REAL,
 PRIMARY KEY(executed_at,option_symbol,price,size));
CREATE INDEX IF NOT EXISTS ix_option_trade_time ON option_trade(observed_at_et,option_symbol);

CREATE TABLE IF NOT EXISTS collector_events(observed_at_et TEXT NOT NULL,event_type TEXT NOT NULL,detail TEXT);
"""

FIELDS=['call_delta_oi','call_delta_vol','call_delta_ask','call_delta_bid','put_delta_oi','put_delta_vol','put_delta_ask','put_delta_bid',
'call_gamma_oi','call_gamma_vol','call_gamma_ask','call_gamma_bid','put_gamma_oi','put_gamma_vol','put_gamma_ask','put_gamma_bid',
'call_vanna_oi','call_vanna_vol','call_vanna_ask','call_vanna_bid','put_vanna_oi','put_vanna_vol','put_vanna_ask','put_vanna_bid',
'call_charm_oi','call_charm_vol','call_charm_ask','call_charm_bid','put_charm_oi','put_charm_vol','put_charm_ask','put_charm_bid']

EXPIRY_GREEK_FIELDS=[
    'call_delta','put_delta','call_gamma','put_gamma',
    'call_charm','put_charm','call_vanna','put_vanna',
    'call_vega','put_vega','call_theta','put_theta',
    'call_rho','put_rho','call_volatility','put_volatility'
]

def f(v):
    try:return float(v)
    except (TypeError,ValueError):return None

def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.executescript(SCHEMA)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")

def active_window(now):
    if now.weekday()>=5:return None
    t=now.time().replace(tzinfo=None)
    if not MARKET_START<=t<=MARKET_END:return None
    if t<=dtime(10,30):return "OPEN"
    if t>=dtime(15,40):return "CLOSE"
    return "MIDDAY"

def event(kind,detail):
    now=datetime.now(ET).isoformat()
    try:
        with sqlite3.connect(DB_PATH) as con:con.execute("INSERT INTO collector_events VALUES(?,?,?)",(now,kind,detail))
    except Exception:pass
    print(f"{now} {kind}: {detail}",flush=True)

def parse_contract(sym):
    m=OPTION_RE.match(sym or "")
    if not m:return None
    d=m.group("date")
    return f"20{d[:2]}-{d[2:4]}-{d[4:6]}",("call" if m.group("cp")=="C" else "put"),int(m.group("strike"))/1000.0

async def uw_get(client,path,params=None,retries=3):
    last=None
    for attempt in range(retries):
        try:
            r=await client.get(UW_BASE+path,params=params or {},timeout=30)
            if r.status_code in (401,403):return r.status_code,None
            r.raise_for_status();return r.status_code,r.json()
        except Exception as e:
            last=e
            if attempt+1<retries:await asyncio.sleep(1.5*(2**attempt))
    raise last

async def fetch_exposure(client,ticker):
    rows=[];page=0
    while True:
        r=await client.get(EXPOSURE_URL.format(ticker=ticker),params={"page":page,"limit":500},timeout=30)
        if r.status_code in (401,403):raise RuntimeError(f"UW rejected {ticker} request ({r.status_code})")
        r.raise_for_status();data=r.json().get("data",[])
        if not data:break
        rows.extend(data)
        if len(data)<500:break
        page+=1
    return rows

def save_exposure(observed,label,ticker,rows):
    cols=['observed_at_et','session_window','source_time','source_date','ticker','strike','underlying_price']+FIELDS
    sql=f"INSERT OR REPLACE INTO strike_exposure({','.join(cols)}) VALUES({','.join('?' for _ in cols)})"
    vals=[];prices=[]
    for r in rows:
        strike=f(r.get("strike"));px=f(r.get("price"))
        if strike is None:continue
        if px is not None:prices.append(px)
        vals.append([observed.isoformat(),label,r.get("time"),r.get("date"),ticker,strike,px]+[f(r.get(k)) for k in FIELDS])
    spot=sorted(prices)[len(prices)//2] if prices else None
    with sqlite3.connect(DB_PATH) as con:
        con.executemany(sql,vals)
        if spot is not None:con.execute("INSERT OR REPLACE INTO underlying_snapshot VALUES(?,?,?,?)",(observed.isoformat(),label,ticker,spot))
    return len(vals),spot

def payload_rows(p):
    return p.get("data",[]) if isinstance(p,dict) and isinstance(p.get("data"),list) else []

# ---------------- NEW: expiry-isolated collection ----------------

async def listed_expiries(client,ticker,observed):
    """
    Returns listed expiries >= observed trading date, cached once per ticker/date.
    We use exchange-listed expiries rather than naïvely adding 1 calendar day so
    weekends/holidays are handled correctly.
    """
    day=observed.date().isoformat()
    cache_key=(ticker,day)
    if cache_key in _EXPIRY_CACHE:
        return _EXPIRY_CACHE[cache_key]

    status,payload=await uw_get(client,f"/api/stock/{ticker}/expiry-breakdown")
    if status in (401,403) or not payload:
        raise RuntimeError(f"UW expiry-breakdown unavailable for {ticker} (HTTP {status})")

    expiries=sorted({
        str(r.get("expires"))
        for r in payload_rows(payload)
        if r.get("expires") and str(r.get("expires"))>=day
    })
    _EXPIRY_CACHE[cache_key]=expiries
    return expiries

async def fetch_greeks_for_expiry(client,ticker,expiry):
    status,payload=await uw_get(client,f"/api/stock/{ticker}/greeks",{"expiry":expiry})
    if status in (401,403) or not payload:
        raise RuntimeError(f"UW greeks unavailable for {ticker} {expiry} (HTTP {status})")
    return payload_rows(payload)

async def fetch_contracts_for_expiry(client,ticker,expiry):
    """
    Pulls all contracts for one expiry. The endpoint is capped at 500 rows, so
    page until exhausted. These rows give OI plus cumulative intraday side volume.
    """
    rows=[];page=0
    while True:
        status,payload=await uw_get(
            client,
            f"/api/stock/{ticker}/option-contracts",
            {"expiry":expiry,"page":page,"limit":500}
        )
        if status in (401,403) or not payload:
            raise RuntimeError(f"UW option-contracts unavailable for {ticker} {expiry} (HTTP {status})")
        data=payload_rows(payload)
        if not data:break
        rows.extend(data)
        if len(data)<500:break
        page+=1
    return rows

def contract_side_map(contract_rows,expiry):
    """
    Converts contract rows into (strike, side) -> OI/volume metadata.
    We aggregate defensively in case UW returns duplicate/variant rows.
    """
    out={}
    for r in contract_rows:
        p=parse_contract(r.get("option_symbol"))
        if not p:continue
        exp,side,strike=p
        if exp!=expiry:continue
        key=(strike,side)
        cur=out.setdefault(key,{
            "open_interest":0.0,"volume":0.0,"ask_volume":0.0,
            "bid_volume":0.0,"mid_volume":0.0
        })
        for k in list(cur):
            v=f(r.get(k))
            if v is not None:cur[k]+=v
    return out

def save_expiry_structure(observed,label,ticker,expiry,rank,spot,greek_rows,contract_rows):
    calendar_dte=(datetime.fromisoformat(expiry).date()-observed.date()).days
    contracts=contract_side_map(contract_rows,expiry)

    cols=[
        'observed_at_et','session_window','ticker','expiry','expiry_rank','calendar_dte',
        'strike','underlying_price'
    ]+EXPIRY_GREEK_FIELDS+[
        'call_open_interest','put_open_interest',
        'call_volume','put_volume',
        'call_ask_volume','put_ask_volume',
        'call_bid_volume','put_bid_volume',
        'call_mid_volume','put_mid_volume'
    ]
    sql=f"INSERT OR REPLACE INTO expiry_strike_structure({','.join(cols)}) VALUES({','.join('?' for _ in cols)})"

    vals=[]
    for r in greek_rows:
        strike=f(r.get("strike"))
        if strike is None:continue

        c=contracts.get((strike,"call"),{})
        p=contracts.get((strike,"put"),{})

        vals.append([
            observed.isoformat(),label,ticker,expiry,rank,calendar_dte,strike,spot,
            *[f(r.get(k)) for k in EXPIRY_GREEK_FIELDS],
            f(c.get("open_interest")),f(p.get("open_interest")),
            f(c.get("volume")),f(p.get("volume")),
            f(c.get("ask_volume")),f(p.get("ask_volume")),
            f(c.get("bid_volume")),f(p.get("bid_volume")),
            f(c.get("mid_volume")),f(p.get("mid_volume"))
        ])

    with sqlite3.connect(DB_PATH) as con:
        con.executemany(sql,vals)
    return len(vals),calendar_dte

async def collect_expiry_structure(client,label,observed,ticker,spot):
    expiries=await listed_expiries(client,ticker,observed)
    selected=expiries[:max(0,EXPIRY_SLOTS)]
    if not selected:
        event("EXPIRY_NO_DATA",f"{ticker}: no listed expiries found")
        return 0

    total=0
    summaries=[]
    for rank,expiry in enumerate(selected):
        try:
            greek_rows,contract_rows=await asyncio.gather(
                fetch_greeks_for_expiry(client,ticker,expiry),
                fetch_contracts_for_expiry(client,ticker,expiry)
            )
            n,calendar_dte=save_expiry_structure(
                observed,label,ticker,expiry,rank,spot,greek_rows,contract_rows
            )
            total+=n
            slot="0DTE" if calendar_dte==0 else (f"{calendar_dte}DTE")
            summaries.append(f"{slot}/{expiry}:{n}")
        except Exception as e:
            event("EXPIRY_ERROR",f"{ticker} {expiry}: {type(e).__name__}: {e}")

    event("EXPIRY_SNAPSHOT",f"{label} {ticker}: "+", ".join(summaries))
    return total

# ---------------- Existing option-path collection ----------------

def choose_options(rows,spot):
    if spot is None:return []
    parsed=[]
    for r in rows:
        p=parse_contract(r.get("option_symbol")) if isinstance(r,dict) else None
        if p:parsed.append((r,*p))
    atm=round(spot/5.0)*5.0
    wanted={("call",atm),("call",atm+5),("call",atm+10),("put",atm),("put",atm-5),("put",atm-10)}
    return [x for x in parsed if (x[2],x[3]) in wanted]

def save_options(observed,label,selected):
    vals=[]
    for r,expiry,typ,strike in selected:
        bid=f(r.get("nbbo_bid"));ask=f(r.get("nbbo_ask"));mid=(bid+ask)/2 if bid is not None and ask is not None else None
        vals.append((observed.isoformat(),label,"SPX",r["option_symbol"],expiry,typ,strike,bid,ask,mid,
        f(r.get("last_price")),f(r.get("implied_volatility")),f(r.get("delta")),f(r.get("gamma")),f(r.get("theta")),
        f(r.get("vega")),f(r.get("rho")),f(r.get("open_interest")),f(r.get("volume")),f(r.get("nbbo_bid_size")),f(r.get("nbbo_ask_size"))))
    with sqlite3.connect(DB_PATH) as con:
        con.executemany("INSERT OR REPLACE INTO option_snapshot VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",vals)
    return len(vals)

def save_trades(observed,label,rows,symbols):
    vals=[]
    for r in rows:
        if not isinstance(r,dict):continue
        sym=r.get("option_chain_id") or r.get("option_symbol")
        if sym not in symbols:continue
        p=parse_contract(sym)
        expiry,typ,strike=p if p else (r.get("expiry"),r.get("option_type"),f(r.get("strike")))
        vals.append((r.get("executed_at") or observed.isoformat(),observed.isoformat(),label,sym,expiry,typ,strike,
        f(r.get("price")),f(r.get("premium")),f(r.get("size")),f(r.get("nbbo_bid")),f(r.get("nbbo_ask")),
        f(r.get("nbbo_bid_size")),f(r.get("nbbo_ask_size")),f(r.get("implied_volatility")),f(r.get("delta")),
        f(r.get("gamma")),f(r.get("theta")),f(r.get("vega")),f(r.get("rho")),f(r.get("underlying_price"))))
    with sqlite3.connect(DB_PATH) as con:
        con.executemany("INSERT OR IGNORE INTO option_trade VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",vals)
    return len(vals)

async def collect_options(client,label,observed,spot):
    today=observed.date().isoformat()
    status,payload=await uw_get(client,"/api/stock/SPX/option-contracts",{"expiry":today,"limit":500})
    if status in (401,403) or not payload:
        event("OPTIONS_NO_ACCESS",f"HTTP {status}");return
    selected=choose_options(payload_rows(payload),spot)
    nq=save_options(observed,label,selected)
    symbols={x[0]["option_symbol"] for x in selected}
    nt=0
    try:
        status,tp=await uw_get(client,"/api/option-trades",{"ticker_symbol":"SPX","expiry_dates[]":today,"limit":500,"intraday_only":"true"})
        if status not in (401,403) and tp:nt=save_trades(observed,label,payload_rows(tp),symbols)
    except Exception as e:event("OPTION_TRADES_ERROR",f"{type(e).__name__}: {e}")
    event("OPTIONS",f"{label}: spot={spot}; saved {nq} symmetric ATM/near-ATM quotes and {nt} relevant trades")

def storage_health():
    try:
        db_mb=DB_PATH.stat().st_size/1024/1024 if DB_PATH.exists() else 0
        s=os.statvfs(DATA_DIR);free_mb=s.f_bavail*s.f_frsize/1024/1024
        print(f"STORAGE db={db_mb:.1f}MB free={free_mb:.1f}MB",flush=True)
        if free_mb<40:event("STORAGE_WARNING",f"Only {free_mb:.1f}MB free")
    except Exception as e:print(f"STORAGE_CHECK_ERROR {e}",flush=True)

async def collect_once(client,label):
    observed=datetime.now(ET).replace(second=0,microsecond=0)
    total=0
    spots={}

    # Preserve the existing all-expiry aggregate collector unchanged.
    for ticker in TICKERS:
        try:
            rows=await fetch_exposure(client,ticker)
            n,px=save_exposure(observed,label,ticker,rows)
            total+=n
            spots[ticker]=px
        except Exception as e:
            event("EXPOSURE_ERROR",f"{ticker}: {type(e).__name__}: {e}")

    # NEW: collect expiry-isolated SPY structure (0DTE + next listed expiry by default).
    expiry_total=0
    for ticker in EXPIRY_TICKERS:
        try:
            expiry_total+=await collect_expiry_structure(
                client,label,observed,ticker,spots.get(ticker)
            )
        except Exception as e:
            event("EXPIRY_ERROR",f"{ticker}: {type(e).__name__}: {e}")

    # Existing SPX near-ATM option path collection.
    try:
        await collect_options(client,label,observed,spots.get("SPXW"))
    except Exception as e:
        event("OPTIONS_ERROR",f"{type(e).__name__}: {e}")

    if observed.minute%15==0:storage_health()
    event("SNAPSHOT",f"{label}: saved {total} broad structural rows + {expiry_total} expiry-isolated rows")

async def main():
    if not UW_TOKEN:raise SystemExit("UW_TOKEN is missing.")
    init_db()
    event(
        "START",
        f"DATA_ONLY_V3; DB={DB_PATH}; NO trading triggers/signals/rules; "
        f"broad aggregate structure + expiry-isolated structure "
        f"(tickers={EXPIRY_TICKERS}, slots={EXPIRY_SLOTS}) + symmetric ATM option paths"
    )
    headers={"Accept":"application/json","Authorization":UW_TOKEN}
    async with httpx.AsyncClient(headers=headers) as client:
        rows=await fetch_exposure(client,"SPY")
        event("UW_TEST_OK",f"SPY endpoint returned {len(rows)} rows")
        last=None
        while True:
            now=datetime.now(ET)
            label=active_window(now)
            key=now.strftime("%Y-%m-%d %H:%M")
            if label and key!=last:
                started=time.monotonic()
                try:await collect_once(client,label)
                except Exception as e:event("ERROR",f"{type(e).__name__}: {e}")
                last=key
                await asyncio.sleep(max(1,POLL_SECONDS-(time.monotonic()-started)))
            else:
                await asyncio.sleep(10)

if __name__=="__main__":
    asyncio.run(main())
