import os,zipfile,tempfile,csv,io,json,secrets
from datetime import date,timedelta
from fastapi import FastAPI,Depends,HTTPException
from fastapi.security import HTTPBasic,HTTPBasicCredentials
from fastapi.responses import FileResponse,HTMLResponse
from starlette.background import BackgroundTask
from imm_store import DB,connect
app=FastAPI(docs_url=None,redoc_url=None,title="IMM Collector v4.0");sec=HTTPBasic(auto_error=False)
def auth(c:HTTPBasicCredentials=Depends(sec)):
 if not os.getenv("EDGE_DASH_PASSWORD") or not c or not secrets.compare_digest(c.password,os.getenv("EDGE_DASH_PASSWORD")):raise HTTPException(401,headers={"WWW-Authenticate":"Basic"})
@app.get("/health")
def health():
 try:
  with connect() as c:n=c.execute("SELECT COUNT(*) FROM master").fetchone()[0];latest=c.execute("SELECT MAX(interval_end_utc) FROM master").fetchone()[0]
  return {"ok":True,"version":"imm-v4.0","master_rows":n,"last_master_row":latest,"db":DB.name}
 except Exception:return {"ok":False,"version":"imm-v4.0"}
@app.get("/status",dependencies=[Depends(auth)])
def status():
 with connect() as c:return {t:c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ["master","strike_history","price_history","price_continuation","option_tape","tape_checkpoint","es_event","audit"]}
@app.get("/",response_class=HTMLResponse,dependencies=[Depends(auth)])
def home():return "<h2>IMM v4 research collector</h2><a href='/status'>Collection status</a>"
