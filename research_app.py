import os,secrets,sqlite3
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
def norm_window(w):return "OPEN" if w.upper()=="MORNING" else w.upper()

@app.get("/health")
def health():return {"ok":DB.exists(),"version":"4.0"}
@app.get("/api/research/warnings")
def aw(date:str,window:str="MIDDAY",ticker:str="SPY",radius:int=8,key:Optional[str]=Query(None)):
    auth(key);return JSONResponse(warnings(DB,date,norm_window(window),ticker.upper(),radius))
@app.get("/api/research/outcomes")
def ao(date:str,window:str="MIDDAY",ticker:str="SPY",radius:int=8,key:Optional[str]=Query(None)):
    auth(key);return JSONResponse(outcomes(DB,date,norm_window(window),ticker.upper(),radius))
@app.get("/",response_class=HTMLResponse)
def home(_:bool=Depends(browser)):
    return """<html><head><meta name=viewport content="width=device-width,initial-scale=1"></head><body style="font-family:-apple-system;background:#0d1117;color:#e6edf3;max-width:900px;margin:40px auto;padding:20px"><h1>Opening Edge Research</h1><h2>Phase 4 — IMM + ISR</h2><p>All-day causal data foundation: 09:25–16:00 ET with OPEN, MIDDAY and CLOSE segmentation plus prospective SPX 0DTE option price, IV and Greek snapshots.</p></body></html>"""
