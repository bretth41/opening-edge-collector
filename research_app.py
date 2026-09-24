import os,secrets,sqlite3,asyncio,json
from pathlib import Path
from typing import Optional
import pandas as pd
from fastapi import FastAPI,HTTPException,Query,Depends
from fastapi.security import HTTPBasic,HTTPBasicCredentials
from fastapi.responses import JSONResponse,HTMLResponse
from signal_engine import warnings,outcomes

DB=Path(os.getenv("EDGE_DB","/data/opening_edge.db"));PW=os.getenv("EDGE_DASH_PASSWORD","");KEY=os.getenv("EDGE_RESEARCH_KEY","")
sec=HTTPBasic(auto_error=False);app=FastAPI(title="Opening Edge Research API",docs_url=None,redoc_url=None)

def q(sql,p=()):
    with sqlite3.connect(DB) as c:return pd.read_sql_query(sql,c,params=p)

def auth(k):
    if not KEY or not k or not secrets.compare_digest(k,KEY):raise HTTPException(401,"Invalid research key")

def browser(c:Optional[HTTPBasicCredentials]=Depends(sec)):
    if not PW or not c or not secrets.compare_digest(c.password,PW):raise HTTPException(401,"Authentication required",headers={"WWW-Authenticate":"Basic"})

def norm_window(w):
    # Collector stores the morning session as OPEN. Keep MORNING as the human/API term.
    return "OPEN" if w.upper()=="MORNING" else w.upper()

def latest(w,t):
    d=q("SELECT max(substr(observed_at_et,1,10)) d FROM strike_exposure WHERE session_window=? AND ticker=?",(norm_window(w),t))
    return None if d.empty else d.iloc[0].d

def latest_snapshot(window,ticker):
    storage_window=norm_window(window)
    d=q("SELECT max(observed_at_et) t FROM strike_exposure WHERE session_window=? AND ticker=?",(storage_window,ticker))
    if d.empty or pd.isna(d.iloc[0].t): return None
    return str(d.iloc[0].t)

def emit_signal_snapshot(window,ticker="SPY",radius=8):
    storage_window=norm_window(window)
    t=latest_snapshot(storage_window,ticker)
    if not t:return None
    day=t[:10]
    w=warnings(DB,day,storage_window,ticker,radius)
    current=[e for e in w.get("warnings",[]) if str(e.get("time"))==t]
    payload={
        "version":"3.3-imm-bridge",
        "causal":True,
        "window":"MORNING" if storage_window=="OPEN" else storage_window,
        "storage_window":storage_window,
        "ticker":ticker,
        "snapshot_time":t,
        "warning_count":len(current),
        "warnings":current
    }
    print("OPENING_EDGE_SIGNAL "+json.dumps(payload,separators=(",",":")),flush=True)
    return t

async def signal_log_loop():
    seen={}
    while True:
        try:
            # IMPORTANT: collector labels the morning rows OPEN, not MORNING.
            for storage_window in ("OPEN","CLOSE"):
                t=latest_snapshot(storage_window,"SPY")
                if t and seen.get(storage_window)!=t:
                    emit_signal_snapshot(storage_window,"SPY",8)
                    seen[storage_window]=t
        except Exception as e:
            print("OPENING_EDGE_BRIDGE_ERROR "+json.dumps({"error":type(e).__name__,"message":str(e)[:240]}),flush=True)
        await asyncio.sleep(15)

@app.on_event("startup")
async def start_signal_bridge():
    asyncio.create_task(signal_log_loop())

@app.get("/health")
def health():return {"ok":DB.exists(),"version":"3.3-imm"}

@app.get("/api/research/warnings")
def aw(date:str,window:str="CLOSE",ticker:str="SPY",radius:int=8,key:Optional[str]=Query(None)):
    auth(key);return JSONResponse(warnings(DB,date,norm_window(window),ticker.upper(),radius))

@app.get("/api/research/outcomes")
def ao(date:str,window:str="CLOSE",ticker:str="SPY",radius:int=8,key:Optional[str]=Query(None)):
    auth(key);return JSONResponse(outcomes(DB,date,norm_window(window),ticker.upper(),radius))

@app.get("/api/research/latest")
def al(window:str="CLOSE",ticker:str="SPY",radius:int=8,key:Optional[str]=Query(None)):
    auth(key);w=norm_window(window);d=latest(w,ticker.upper())
    if not d:raise HTTPException(404,"No data")
    return JSONResponse({"warnings":warnings(DB,str(d),w,ticker.upper(),radius),
                         "outcomes":outcomes(DB,str(d),w,ticker.upper(),radius)})

@app.get("/",response_class=HTMLResponse)
def home(_:bool=Depends(browser)):
    return """<html><head><meta name=viewport content="width=device-width,initial-scale=1"><style>body{font-family:-apple-system,sans-serif;max-width:900px;margin:40px auto;padding:20px;background:#0d1117;color:#e6edf3}.c{background:#161b22;padding:20px;border-radius:12px}</style></head><body><h1>Opening Edge Research</h1><div class=c><h2>Phase 3.3 — IMM</h2><p>The morning bridge now reads the collector's OPEN session correctly while exposing it externally as MORNING.</p><p>Research objective: Immediately Monetizable Migration — causal setups intended to identify substantial 2–10 minute migration while option convexity remains monetizable. Outcome analysis remains separate from signal generation.</p></div></body></html>"""
