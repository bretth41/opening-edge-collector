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
WINDOWS = [(dtime(9,25), dtime(10,30), "OPEN"), (dtime(15,30), dtime(16,0), "CLOSE")]

SCHEMA = '''
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
'''
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
    t=now.time().replace(tzinfo=None)
    for start,end,label in WINDOWS:
        if start <= t <= end: return label
    return None

def event(kind, detail):
    now=datetime.now(ET).isoformat()
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT INTO collector_events VALUES (?,?,?)",(now,kind,detail)); con.commit()
    print(f"{now} {kind}: {detail}", flush=True)

async def fetch_ticker(client,ticker):
    rows=[]; page=0
    while True:
        r=await client.get(BASE.format(ticker=ticker),params={'page':page,'limit':500},timeout=25)
        if r.status_code in (401,403):
            raise RuntimeError(f"UW rejected {ticker} request ({r.status_code}). Check UW_TOKEN or Basic-plan endpoint access.")
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

async def collect_once(client,label):
    observed=datetime.now(ET).replace(second=0,microsecond=0); total=0
    for ticker in TICKERS:
        rows=await fetch_ticker(client,ticker); total += save_snapshot(observed,label,ticker,rows)
    event("SNAPSHOT",f"{label}: saved {total} strike rows for {','.join(TICKERS)}")


PHASE2_BASE = "https://api.unusualwhales.com"

async def phase2_get(client, path, params=None):
    r = await client.get(PHASE2_BASE + path, params=params or {}, timeout=25)
    if r.status_code in (401,403):
        return r.status_code, None
    r.raise_for_status()
    return r.status_code, r.json()

def save_phase2(observed, label, endpoint, ticker, contract, payload):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO phase2_raw VALUES (?,?,?,?,?,?)",
            (observed.isoformat(), label, endpoint, ticker, contract, json.dumps(payload, separators=(",",":")))
        )
        con.commit()

async def phase2_probe(client):
    # Probe documented REST endpoints using today's SPX 0DTE expiry. No WebSockets required.
    today = datetime.now(ET).date().isoformat()
    probes = [
        ("CONTRACTS", "/api/stock/SPX/option-contracts", {"expiry": today, "limit": 10}),
        ("GREEKS", "/api/stock/SPX/greeks", {"expiry": today}),
        ("TRADES", "/api/option-trades", {"ticker_symbol": "SPX", "expiry_dates[]": today, "limit": 10, "intraday_only": "true"}),
    ]
    first_contract = None
    for name, path, params in probes:
        try:
            status, payload = await phase2_get(client, path, params)
            if status in (401,403):
                event(f"PHASE2_{name}_NO_ACCESS", f"HTTP {status}")
                continue
            data = payload.get("data", []) if isinstance(payload, dict) else []
            event(f"PHASE2_{name}_OK", f"returned {len(data)} rows")
            if name == "CONTRACTS" and data:
                for row in data:
                    if isinstance(row, dict):
                        first_contract = row.get("option_symbol") or row.get("id") or row.get("symbol")
                        if first_contract: break
        except Exception as e:
            event(f"PHASE2_{name}_FAILED", f"{type(e).__name__}: {e}")
    if first_contract:
        try:
            status, payload = await phase2_get(client, f"/api/option-contract/{first_contract}/intraday", {"date": today})
            if status in (401,403): event("PHASE2_INTRADAY_NO_ACCESS", f"HTTP {status}")
            else:
                data = payload.get("data", []) if isinstance(payload, dict) else []
                event("PHASE2_INTRADAY_OK", f"{first_contract} returned {len(data)} minute rows")
        except Exception as e:
            event("PHASE2_INTRADAY_FAILED", f"{type(e).__name__}: {e}")

async def collect_phase2_once(client, label, observed):
    # Preserve raw SPX 0DTE chain/Greek/trade responses each minute. Raw storage prevents
    # us from accidentally throwing away a field before we know which ones predict edge.
    today = observed.date().isoformat()
    requests = [
        ("spx_0dte_contracts", "/api/stock/SPX/option-contracts", {"expiry": today, "limit": 500}),
        ("spx_0dte_greeks", "/api/stock/SPX/greeks", {"expiry": today}),
        ("spx_0dte_trades", "/api/option-trades", {"ticker_symbol": "SPX", "expiry_dates[]": today, "limit": 500, "intraday_only": "true"}),
    ]
    saved=0
    for name,path,params in requests:
        try:
            status,payload=await phase2_get(client,path,params)
            if status in (401,403): continue
            save_phase2(observed,label,name,"SPX",None,payload); saved += 1
        except Exception as e:
            event("PHASE2_ERROR", f"{name}: {type(e).__name__}: {e}")
    if saved: event("PHASE2_SNAPSHOT", f"{label}: saved {saved} raw SPX 0DTE datasets")

async def main():
    if not UW_TOKEN: raise SystemExit("UW_TOKEN is missing. Add it in Railway > service > Variables.")
    init_db(); event("START",f"DB={DB_PATH}; tickers={TICKERS}; windows=09:25-10:30 & 15:30-16:00 ET")
    headers={'Accept':'application/json','Authorization':UW_TOKEN}
    async with httpx.AsyncClient(headers=headers) as client:
        # Immediate entitlement/authentication test on startup.
        try:
            rows=await fetch_ticker(client,"SPY")
            event("UW_TEST_OK",f"SPY endpoint returned {len(rows)} rows")
            await phase2_probe(client)
        except Exception as e:
            event("UW_TEST_FAILED",str(e)); raise
        last_key=None
        while True:
            now=datetime.now(ET); label=active_window(now)
            minute_key=now.strftime('%Y-%m-%d %H:%M')
            if label and minute_key != last_key:
                started=time.monotonic()
                try:
                    await collect_once(client,label)
                    await collect_phase2_once(client,label,datetime.now(ET).replace(second=0,microsecond=0))
                except Exception as e: event("ERROR",f"{type(e).__name__}: {e}")
                last_key=minute_key
                await asyncio.sleep(max(1,POLL_SECONDS-(time.monotonic()-started)))
            else:
                await asyncio.sleep(10)
if __name__=='__main__': asyncio.run(main())
