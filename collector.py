import asyncio, os, sqlite3, time, json
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
import httpx

ET = ZoneInfo("America/New_York")
DATA_DIR = Path(os.getenv("EDGE_DATA_DIR", "/data" if Path("/data").exists() else "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(os.getenv("EDGE_DB", str(DATA_DIR / "opening_edge.db")))
UW_TOKEN = os.environ.get("UW_TOKEN")
TICKERS = [x.strip() for x in os.getenv("EDGE_TICKERS", "SPY,SPXW").split(",") if x.strip()]
POLL_SECONDS = int(os.getenv("EDGE_POLL_SECONDS", "60"))
BASE = "https://api.unusualwhales.com/api/stock/{ticker}/spot-exposures/strike"
UW_BASE = "https://api.unusualwhales.com"
MARKET_START, MARKET_END = dtime(9,25), dtime(16,0)

SCHEMA = """
CREATE TABLE IF NOT EXISTS strike_exposure (
  observed_at_et TEXT NOT NULL, session_window TEXT NOT NULL,
  source_time TEXT, source_date TEXT, ticker TEXT NOT NULL, strike REAL NOT NULL,
  underlying_price REAL,
  call_delta_oi REAL, call_delta_vol REAL, call_delta_ask REAL, call_delta_bid REAL,
  put_delta_oi REAL, put_delta_vol REAL, put_delta_ask REAL, put_delta_bid REAL,
  call_gamma_oi REAL, call_gamma_vol REAL, call_gamma_ask REAL, call_gamma_bid REAL,
  put_gamma_oi REAL, put_gamma_vol REAL, put_gamma_ask REAL, put_gamma_bid REAL,
  call_vanna_oi REAL, call_vanna_vol REAL, call_vanna_ask REAL, call_vanna_bid REAL,
  put_vanna_oi REAL, put_vanna_vol REAL, put_vanna_ask REAL, put_vanna_bid REAL,
  call_charm_oi REAL, call_charm_vol REAL, call_charm_ask REAL, call_charm_bid REAL,
  put_charm_oi REAL, put_charm_vol REAL, put_charm_ask REAL, put_charm_bid REAL,
  PRIMARY KEY (observed_at_et, ticker, strike)
);
CREATE INDEX IF NOT EXISTS ix_exposure_ticker_time ON strike_exposure(ticker, observed_at_et);
CREATE TABLE IF NOT EXISTS collector_events (
  observed_at_et TEXT NOT NULL, event_type TEXT NOT NULL, detail TEXT
);
CREATE TABLE IF NOT EXISTS phase2_raw (
  observed_at_et TEXT NOT NULL, session_window TEXT NOT NULL, endpoint TEXT NOT NULL,
  ticker TEXT, contract TEXT, payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_phase2_time ON phase2_raw(observed_at_et, endpoint);
CREATE TABLE IF NOT EXISTS option_snapshot (
  observed_at_et TEXT NOT NULL, session_window TEXT NOT NULL, underlying TEXT NOT NULL,
  option_symbol TEXT NOT NULL, expiry TEXT, option_type TEXT, strike REAL,
  bid REAL, ask REAL, mid REAL, implied_volatility REAL,
  delta REAL, gamma REAL, theta REAL, vega REAL, rho REAL,
  open_interest REAL, volume REAL,
  PRIMARY KEY (observed_at_et, option_symbol)
);
CREATE INDEX IF NOT EXISTS ix_option_snapshot_time
ON option_snapshot(observed_at_et, underlying, strike);
"""

FIELDS = [
 'call_delta_oi','call_delta_vol','call_delta_ask','call_delta_bid','put_delta_oi','put_delta_vol','put_delta_ask','put_delta_bid',
 'call_gamma_oi','call_gamma_vol','call_gamma_ask','call_gamma_bid','put_gamma_oi','put_gamma_vol','put_gamma_ask','put_gamma_bid',
 'call_vanna_oi','call_vanna_vol','call_vanna_ask','call_vanna_bid','put_vanna_oi','put_vanna_vol','put_vanna_ask','put_vanna_bid',
 'call_charm_oi','call_charm_vol','call_charm_ask','call_charm_bid','put_charm_oi','put_charm_vol','put_charm_ask','put_charm_bid'
]

def f(v):
    try: return float(v)
    except (TypeError, ValueError): return None

def init_db():
    with sqlite3.connect(DB_PATH) as con: con.executescript(SCHEMA)

def active_window(now):
    if now.weekday() >= 5: return None
    t = now.time().replace(tzinfo=None)
    if not MARKET_START <= t <= MARKET_END: return None
    if t <= dtime(10,30): return "OPEN"
    if t >= dtime(15,40): return "CLOSE"
    return "MIDDAY"

def event(kind, detail):
    now=datetime.now(ET).isoformat()
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT INTO collector_events VALUES (?,?,?)",(now,kind,detail)); con.commit()
    print(f"{now} {kind}: {detail}", flush=True)

async def uw_get(client, path, params=None, retries=3):
    last=None
    for attempt in range(retries):
        try:
            r=await client.get(UW_BASE+path, params=params or {}, timeout=30)
            if r.status_code in (401,403): return r.status_code, None
            r.raise_for_status()
            return r.status_code, r.json()
        except Exception as e:
            last=e
            if attempt+1 < retries: await asyncio.sleep(1.5*(2**attempt))
    raise last

async def fetch_ticker(client,ticker):
    rows=[]; page=0
    while True:
        r=await client.get(BASE.format(ticker=ticker),params={'page':page,'limit':500},timeout=30)
        if r.status_code in (401,403):
            raise RuntimeError(f"UW rejected {ticker} request ({r.status_code}). Check UW_TOKEN/API access.")
        r.raise_for_status(); data=r.json().get('data',[])
        if not data: break
        rows.extend(data)
        if len(data)<500: break
        page += 1
    return rows

def save_snapshot(observed,label,ticker,rows):
    cols=['observed_at_et','session_window','source_time','source_date','ticker','strike','underlying_price']+FIELDS
    sql=f"INSERT OR REPLACE INTO strike_exposure ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})"
    vals=[]
    for r in rows:
        strike=f(r.get('strike'))
        if strike is None: continue
        vals.append([observed.isoformat(),label,r.get('time'),r.get('date'),ticker,strike,f(r.get('price'))]+[f(r.get(k)) for k in FIELDS])
    with sqlite3.connect(DB_PATH) as con:
        con.executemany(sql,vals); con.commit()
    return len(vals)

def save_raw(observed,label,endpoint,ticker,payload):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT INTO phase2_raw VALUES (?,?,?,?,?,?)",
                    (observed.isoformat(),label,endpoint,ticker,None,json.dumps(payload,separators=(",",":"))))
        con.commit()

def payload_rows(payload):
    if isinstance(payload,dict) and isinstance(payload.get("data"),list): return payload["data"]
    return []

def save_option_snapshot(observed,label,rows):
    vals=[]
    for r in rows:
        if not isinstance(r,dict): continue
        sym=r.get("option_symbol") or r.get("id") or r.get("symbol")
        if not sym: continue
        bid=f(r.get("bid") if r.get("bid") is not None else r.get("nbbo_bid"))
        ask=f(r.get("ask") if r.get("ask") is not None else r.get("nbbo_ask"))
        mid=(bid+ask)/2 if bid is not None and ask is not None else None
        iv=r.get("implied_volatility")
        if iv is None: iv=r.get("iv")
        oi=r.get("open_interest")
        if oi is None: oi=r.get("oi")
        vals.append((
            observed.isoformat(),label,"SPX",sym,r.get("expiry"),
            r.get("option_type") or r.get("type"),f(r.get("strike")),bid,ask,mid,f(iv),
            f(r.get("delta")),f(r.get("gamma")),f(r.get("theta")),f(r.get("vega")),f(r.get("rho")),
            f(oi),f(r.get("volume"))
        ))
    with sqlite3.connect(DB_PATH) as con:
        con.executemany("INSERT OR REPLACE INTO option_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",vals)
        con.commit()
    return len(vals)

async def collect_phase4(client,label,observed):
    today=observed.date().isoformat()
    try:
        status,payload=await uw_get(client,"/api/stock/SPX/option-contracts",{"expiry":today,"limit":500})
        if status in (401,403):
            event("PHASE4_CONTRACTS_NO_ACCESS",f"HTTP {status}")
        elif payload:
            save_raw(observed,label,"spx_0dte_contracts","SPX",payload)
            n=save_option_snapshot(observed,label,payload_rows(payload))
            event("PHASE4_OPTIONS",f"{label}: saved {n} SPX 0DTE quote/IV/Greek rows")
    except Exception as e:
        event("PHASE4_OPTIONS_ERROR",f"{type(e).__name__}: {e}")
    try:
        status,payload=await uw_get(client,"/api/option-trades",
            {"ticker_symbol":"SPX","expiry_dates[]":today,"limit":500,"intraday_only":"true"})
        if status not in (401,403) and payload:
            save_raw(observed,label,"spx_0dte_trades","SPX",payload)
    except Exception as e:
        event("PHASE4_TRADES_ERROR",f"{type(e).__name__}: {e}")

async def collect_once(client,label):
    observed=datetime.now(ET).replace(second=0,microsecond=0); total=0
    for ticker in TICKERS:
        try:
            rows=await fetch_ticker(client,ticker)
            total += save_snapshot(observed,label,ticker,rows)
        except Exception as e:
            event("EXPOSURE_ERROR",f"{ticker}: {type(e).__name__}: {e}")
    await collect_phase4(client,label,observed)
    event("SNAPSHOT",f"{label}: saved {total} strike rows for {','.join(TICKERS)}")

async def main():
    if not UW_TOKEN: raise SystemExit("UW_TOKEN is missing. Add it in Railway > service > Variables.")
    init_db()
    event("START",f"PHASE4; DB={DB_PATH}; tickers={TICKERS}; collection=09:25-16:00 ET; labels=OPEN/MIDDAY/CLOSE")
    headers={'Accept':'application/json','Authorization':UW_TOKEN}
    async with httpx.AsyncClient(headers=headers) as client:
        rows=await fetch_ticker(client,"SPY")
        event("UW_TEST_OK",f"SPY endpoint returned {len(rows)} rows")
        last_key=None
        while True:
            now=datetime.now(ET); label=active_window(now); minute_key=now.strftime('%Y-%m-%d %H:%M')
            if label and minute_key != last_key:
                started=time.monotonic()
                try: await collect_once(client,label)
                except Exception as e: event("ERROR",f"{type(e).__name__}: {e}")
                last_key=minute_key
                await asyncio.sleep(max(1,POLL_SECONDS-(time.monotonic()-started)))
            else:
                await asyncio.sleep(10)

if __name__=='__main__': asyncio.run(main())
