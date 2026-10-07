import os,zipfile,tempfile,csv,io,json,secrets,shutil
from datetime import date,timedelta
from fastapi import FastAPI,Depends,HTTPException
from fastapi.security import HTTPBasic,HTTPBasicCredentials
from fastapi.responses import FileResponse,HTMLResponse
from starlette.background import BackgroundTask
from imm_store import DB,connect
app=FastAPI(docs_url=None,redoc_url=None,title="IMM Collector v4.2");sec=HTTPBasic(auto_error=False)
def auth(c:HTTPBasicCredentials=Depends(sec)):
 if not os.getenv("EDGE_DASH_PASSWORD") or not c or not secrets.compare_digest(c.password,os.getenv("EDGE_DASH_PASSWORD")):raise HTTPException(401,headers={"WWW-Authenticate":"Basic"})
def dates(s,e):
 try:a,b=date.fromisoformat(s),date.fromisoformat(e)
 except ValueError:raise HTTPException(400,"Dates must be YYYY-MM-DD")
 if b<a or (b-a).days>31:raise HTTPException(400,"Invalid range (max 32 days)")
 return a,b
@app.get("/health")
def health():
 try:
  with connect() as c:n=c.execute("SELECT COUNT(*) FROM master").fetchone()[0];latest=c.execute("SELECT MAX(interval_end_utc) FROM master").fetchone()[0]
  return {"ok":True,"version":"imm-v4.2","master_rows":n,"last_master_row":latest}
 except Exception:return {"ok":False,"version":"imm-v4.2"}
@app.get("/status",dependencies=[Depends(auth)])
def status():
 with connect() as c:
  result={"master_rows":c.execute("SELECT COUNT(*) FROM master").fetchone()[0],"by_session":{r[0]:r[1] for r in c.execute("SELECT session_date_et,COUNT(*) FROM master GROUP BY session_date_et ORDER BY session_date_et DESC LIMIT 10")}}
  result["disk_free_mb"]=shutil.disk_usage(DB.parent).free//1048576
  result["db_mb"]=DB.stat().st_size//1048576 if DB.exists() else 0
  result["wal_mb"]=DB.with_name(DB.name+"-wal").stat().st_size//1048576 if DB.with_name(DB.name+"-wal").exists() else 0
  return result
@app.get("/export",dependencies=[Depends(auth)])
def export(start:str,end:str):
 a,b=dates(start,end);lo=a.isoformat();hi=(b+timedelta(days=1)).isoformat()
 fd,path=tempfile.mkstemp(suffix=".zip",prefix="imm_");os.close(fd)
 manifest={"version":"imm-v4.2","start":lo,"end":b.isoformat(),"timezone":"America/New_York","five_second_master_expected_per_full_session":4680,"strike_window":"nearest 5pt strike +/-35 SPX points, 15 strikes per expiry","expiry_columns":["greeks_0dte_json","greeks_1dte_json"],"greek_snapshot_timestamp_column":"uw_snapshot_at","missing_greek_strikes":"null values, never zero-filled","rolling_windows":{"spx_change_30s":"exact trailing 30 seconds","spx_change_60s":"exact trailing 60 seconds","spx_change_180s":"exact trailing 3 minutes","spx_change_300s":"exact trailing 5 minutes","es_interval_activity":"preceding 5 seconds","greek_change_1_3_5_10min":"derive from distinct UW snapshots using uw_snapshot_at; not computed in collector"},"tables":{}}
 with connect() as c,zipfile.ZipFile(path,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
  cur=c.execute("SELECT * FROM master WHERE session_date_et>=? AND session_date_et<? ORDER BY session_date_et,interval_end_utc",(lo,hi));count=0
  with z.open("master.csv","w") as f:
   txt=io.TextIOWrapper(f,encoding="utf-8",newline="");w=csv.writer(txt);w.writerow([x[0] for x in cur.description])
   while batch:=cur.fetchmany(1000):w.writerows(batch);count+=len(batch)
   txt.flush()
  manifest["tables"]["master"]=count
  z.writestr("manifest.json",json.dumps(manifest,indent=2))
 return FileResponse(path,filename=f"imm_{lo}_to_{b}.zip",background=BackgroundTask(lambda:os.unlink(path)))
@app.get("/",response_class=HTMLResponse,dependencies=[Depends(auth)])
def home():return '<h2>IMM v4.2 master export</h2><form action="/export"><input type="date" name="start" required><input type="date" name="end" required><button>Download ZIP</button></form><a href="/status">Collection status</a>'
