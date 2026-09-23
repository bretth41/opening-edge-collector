import os, sqlite3, secrets
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import Depends

DB = Path(os.getenv("EDGE_DB", "/data/opening_edge.db"))
DASH_PASSWORD = os.getenv("EDGE_DASH_PASSWORD", "")
RESEARCH_KEY = os.getenv("EDGE_RESEARCH_KEY", "")
security = HTTPBasic(auto_error=False)
app = FastAPI(title="Opening Edge Research", docs_url=None, redoc_url=None)

def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def q(sql, params=()):
    if not DB.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB) as c:
        return pd.read_sql_query(sql, c, params=params)

def check_browser(creds: Optional[HTTPBasicCredentials] = Depends(security)):
    if not DASH_PASSWORD:
        raise HTTPException(503, "Set EDGE_DASH_PASSWORD in Railway Variables.")
    ok = creds and secrets.compare_digest(creds.password, DASH_PASSWORD)
    if not ok:
        raise HTTPException(401, "Authentication required", headers={"WWW-Authenticate":"Basic"})
    return True

def check_api(key: Optional[str]):
    if not RESEARCH_KEY:
        raise HTTPException(503, "Set EDGE_RESEARCH_KEY in Railway Variables.")
    if not key or not secrets.compare_digest(key, RESEARCH_KEY):
        raise HTTPException(401, "Invalid research key")

def exposure(day, window="CLOSE", ticker="SPY"):
    return q("""SELECT * FROM strike_exposure
                WHERE substr(observed_at_et,1,10)=? AND session_window=? AND ticker=?
                ORDER BY observed_at_et,strike""",(day,window,ticker))

def derive(day, window="CLOSE", ticker="SPY", radius=8):
    x=exposure(day,window,ticker)
    if x.empty: return {"error":"no data","date":day,"window":window,"ticker":ticker}
    x["net_gamma_vol"]=x["call_gamma_vol"].fillna(0)+x["put_gamma_vol"].fillna(0)
    x["net_gamma_oi"]=x["call_gamma_oi"].fillna(0)+x["put_gamma_oi"].fillna(0)
    x["net_charm_vol"]=x["call_charm_vol"].fillna(0)+x["put_charm_vol"].fillna(0)
    x["net_vanna_vol"]=x["call_vanna_vol"].fillna(0)+x["put_vanna_vol"].fillna(0)
    times=sorted(x.observed_at_et.unique())
    px=(x.groupby("observed_at_et",as_index=False)["underlying_price"].median().dropna())
    last_spot=float(px.iloc[-1].underlying_price) if len(px) else None
    center=float(px.underlying_price.median()) if len(px) else float(x.strike.median())
    near=x[(x.strike>=center-radius)&(x.strike<=center+radius)].copy()

    piv=near.pivot_table(index="observed_at_et",columns="strike",values="net_gamma_vol",aggfunc="sum").sort_index()
    first=piv.iloc[0]; last=piv.iloc[-1]
    change=(last-first)
    pct=[]
    for strike in piv.columns:
        f=float(first[strike]); l=float(last[strike]); ch=float(change[strike])
        pct_change=(ch/abs(f)*100) if f else None
        pct.append({"strike":float(strike),"first":f,"last":l,"change":ch,"pct_change_vs_abs_first":pct_change})
    pct=sorted(pct,key=lambda r:abs(r["change"]),reverse=True)

    # Per-strike 1m/3m/5m delta and rank at the latest snapshot.
    latest_rows=[]
    for strike in piv.columns:
        s=piv[strike].dropna()
        if not len(s): continue
        cur=float(s.iloc[-1])
        def delta(n):
            return float(cur-s.iloc[-1-n]) if len(s)>n else None
        latest_rows.append({
            "strike":float(strike),"net_gamma_vol":cur,
            "delta_1m":delta(1),"delta_3m":delta(3),"delta_5m":delta(5)
        })
    # Rank by absolute gamma magnitude; 1 = dominant node.
    for r in latest_rows:
        r["abs_rank"]=1+sum(abs(o["net_gamma_vol"])>abs(r["net_gamma_vol"]) for o in latest_rows)
    latest_rows=sorted(latest_rows,key=lambda r:r["abs_rank"])

    # Detect largest minute impulse in each nearby strike.
    impulses=[]
    for strike in piv.columns:
        s=piv[strike].dropna()
        ds=s.diff().dropna()
        if len(ds):
            t=ds.abs().idxmax()
            impulses.append({"strike":float(strike),"time":t,"delta_1m":float(ds.loc[t]),"gamma_after":float(s.loc[t])})
    impulses=sorted(impulses,key=lambda r:abs(r["delta_1m"]),reverse=True)

    # Price series, rounded only for transport/readability.
    price_series=[{"time":str(r.observed_at_et),"price":round(float(r.underlying_price),4)} for _,r in px.iterrows()]
    return {
      "date":day,"window":window,"ticker":ticker,
      "summary":{"snapshots":len(times),"strike_rows":len(x),"first_snapshot":times[0],"last_snapshot":times[-1],
                 "latest_spot":last_spot,"center_spot":center},
      "largest_first_to_last_changes":pct[:20],
      "latest_node_state":latest_rows[:30],
      "largest_1m_impulses":impulses[:20],
      "price_series":price_series
    }

@app.get("/health")
def health():
    return {"ok":DB.exists(),"db":str(DB)}

@app.get("/api/session")
def api_session(date:str, window:str="CLOSE", ticker:str="SPY", radius:int=8, key:Optional[str]=Query(None)):
    check_api(key)
    return JSONResponse(derive(date,window.upper(),ticker.upper(),radius))

@app.get("/api/latest")
def api_latest(window:str="CLOSE", ticker:str="SPY", radius:int=8, key:Optional[str]=Query(None)):
    check_api(key)
    d=q("SELECT max(substr(observed_at_et,1,10)) d FROM strike_exposure WHERE session_window=? AND ticker=?",
        (window.upper(),ticker.upper()))
    if d.empty or not d.iloc[0].d: raise HTTPException(404,"No sessions")
    return JSONResponse(derive(str(d.iloc[0].d),window.upper(),ticker.upper(),radius))

@app.get("/", response_class=HTMLResponse)
def dashboard(_:bool=Depends(check_browser)):
    dates=q("SELECT DISTINCT substr(observed_at_et,1,10) d FROM strike_exposure ORDER BY d DESC")
    if dates.empty: return "<h2>No data yet.</h2>"
    day=str(dates.iloc[0].d)
    data=derive(day,"CLOSE","SPY",8)
    if "error" in data: data=derive(day,"OPEN","SPY",8)
    s=data["summary"]
    changes=data["largest_first_to_last_changes"][:10]
    nodes=data["latest_node_state"][:10]
    prices=data["price_series"]
    def fmtpct(v): return "" if v is None else f"{v:+.1f}%"
    def fmtb(v): return "" if v is None else f"{v/1e9:+.1f}B"
    rows="".join(f"<tr><td>{r['strike']:.0f}</td><td>{r['first']/1e9:.1f}B</td><td>{r['last']/1e9:.1f}B</td><td>{r['change']/1e9:+.1f}B</td><td>{fmtpct(r['pct_change_vs_abs_first'])}</td></tr>" for r in changes)
    nrows="".join(f"<tr><td>{r['abs_rank']}</td><td>{r['strike']:.0f}</td><td>{r['net_gamma_vol']/1e9:.1f}B</td><td>{fmtb(r['delta_1m'])}</td><td>{fmtb(r['delta_3m'])}</td><td>{fmtb(r['delta_5m'])}</td></tr>" for r in nodes)
    labels=[p["time"][11:16] for p in prices]; vals=[p["price"] for p in prices]
    import json
    return f"""<!doctype html><html><head><meta name=viewport content="width=device-width,initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;max-width:1200px;margin:30px auto;padding:0 18px;background:#0d1117;color:#e6edf3}}
    .cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}} .card{{background:#161b22;padding:16px;border-radius:12px}}
    table{{width:100%;border-collapse:collapse;background:#161b22}}th,td{{padding:9px;border-bottom:1px solid #30363d;text-align:right}}th:first-child,td:first-child{{text-align:left}}
    h1,h2{{margin-top:28px}} canvas{{background:#fff;border-radius:12px;padding:8px}} @media(max-width:800px){{.cards{{grid-template-columns:repeat(2,1fr)}}}}</style></head>
    <body><h1>Opening Edge Research</h1><p>{data['date']} · {data['window']} · {data['ticker']}</p>
    <div class=cards><div class=card><small>Snapshots</small><h2>{s['snapshots']}</h2></div><div class=card><small>Strike rows</small><h2>{s['strike_rows']:,}</h2></div>
    <div class=card><small>Latest spot</small><h2>{s['latest_spot']:.2f}</h2></div><div class=card><small>Last snapshot</small><h2>{s['last_snapshot'][11:16]}</h2></div></div>
    <h2>Underlying price</h2><canvas id=p></canvas>
    <h2>Largest gamma changes — first → last</h2><table><tr><th>Strike</th><th>First</th><th>Last</th><th>Δ</th><th>Δ %</th></tr>{rows}</table>
    <h2>Latest node velocity</h2><table><tr><th>Rank</th><th>Strike</th><th>Gamma</th><th>Δ1m</th><th>Δ3m</th><th>Δ5m</th></tr>{nrows}</table>
    <script>new Chart(document.getElementById('p'),{{type:'line',data:{{labels:{json.dumps(labels)},datasets:[{{label:'SPY',data:{json.dumps(vals)},pointRadius:1}}]}},options:{{scales:{{y:{{beginAtZero:false}}}}}}}});</script>
    </body></html>"""
