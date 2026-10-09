import os,zipfile,tempfile,csv,io,json,secrets,shutil
from datetime import date,timedelta,datetime,timezone,time as dtime
from zoneinfo import ZoneInfo
from fastapi import FastAPI,Depends,HTTPException
from fastapi.security import HTTPBasic,HTTPBasicCredentials
from fastapi.responses import FileResponse,HTMLResponse
from starlette.background import BackgroundTask
from imm_store import DB,connect
app=FastAPI(docs_url=None,redoc_url=None,title="IMM Collector v4.3");sec=HTTPBasic(auto_error=False)
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
  from imm_uw import UW_STATE
  from imm_massive import SPX
  live=live_diagnostics()
  return {"ok":True,"version":"imm-v4.3","master_rows":n,"last_master_row":latest,"market_open":live['market_open'],"collection_current":live['collection_current'],"greeks_ready":live['greeks_ready'],'greek_reasons':live['greek_reasons'],'uw_state':UW_STATE.quality.get('state')}
 except Exception:return {"ok":False,"version":"imm-v4.3"}
def live_diagnostics():
 now=datetime.now(timezone.utc);local=now.astimezone(ZoneInfo('America/New_York'))
 market_open=local.weekday()<5 and dtime(9,30)<=local.time().replace(tzinfo=None)<dtime(16)
 with connect() as c:
  row=c.execute('SELECT * FROM master ORDER BY interval_end_utc DESC LIMIT 1').fetchone()
 current=False;quality={};reasons=[]
 if row:
  try:
   t=datetime.fromisoformat(row['interval_end_utc']);age=(now-t).total_seconds()
   current=row['session_date_et']==local.date().isoformat() and -2<=age<=15
  except (ValueError,TypeError):reasons.append('invalid_master_timestamp')
  quality=json.loads(row['uw_window_quality_json'] or '{}')
  reasons.extend(quality.get('reasons',[]))
 else:reasons.append('no_master_rows')
 if not current:reasons.append('no_current_master_row')
 if not market_open:reasons.append('outside_cash_session_live_validation_pending')
 from imm_uw import UW_STATE
 if UW_STATE.quality.get('state')!='received':reasons.append(UW_STATE.quality.get('state','starting'))
 return {'market_open':market_open,'collection_current':current,'greeks_ready':current and market_open and quality.get('ready_for_structural_analysis',False) and UW_STATE.quality.get('state')=='received','greek_reasons':list(dict.fromkeys(reasons)),'window':quality}

@app.get('/diagnostics',dependencies=[Depends(auth)])
def diagnostics():
 from imm_uw import UW_STATE
 from imm_massive import SPX
 from imm_es import BOOK
 result=live_diagnostics();result['uw']=UW_STATE.quality;result['massive']=SPX.quality()
 result['es']=BOOK.quality(datetime.now(timezone.utc))
 result['price_and_pressure_ready']=bool(result['collection_current'] and result['market_open'] and SPX.connected and SPX.authenticated and SPX.subscribed and result['es']['valid_and_fresh'])
 result['version']='imm-v4.3';result['configuration']={'uw_token_present':bool(os.getenv('UW_TOKEN')),'massive_key_present':bool(os.getenv('MASSIVE_API_KEY')),'es_key_present':bool(os.getenv('DATABENTO_API_KEY'))}
 result['limitations']=['ES MBP-1 is L1; cancellation and absorption are unavailable','Directional exposure is a model estimate, not observed dealer hedge trades','IV is not delivered by the spot-exposure endpoint; IV capture is not included','Weekday cash-session schedule; no exchange holiday/early-close calendar']
 return result

@app.get("/status",dependencies=[Depends(auth)])
def status():
 with connect() as c:
  result={"master_rows":c.execute("SELECT COUNT(*) FROM master").fetchone()[0],"by_session":{r[0]:r[1] for r in c.execute("SELECT session_date_et,COUNT(*) FROM master GROUP BY session_date_et ORDER BY session_date_et DESC LIMIT 10")}}
  result["disk_free_mb"]=shutil.disk_usage(DB.parent).free//1048576
  result["db_mb"]=DB.stat().st_size//1048576 if DB.exists() else 0
  result["wal_mb"]=DB.with_name(DB.name+"-wal").stat().st_size//1048576 if DB.with_name(DB.name+"-wal").exists() else 0
  result["diagnostics"]=live_diagnostics()
  return result
@app.get("/export",dependencies=[Depends(auth)])
def export(start:str,end:str):
 a,b=dates(start,end);lo=a.isoformat();hi=(b+timedelta(days=1)).isoformat()
 fd,path=tempfile.mkstemp(suffix=".zip",prefix="imm_");os.close(fd)
 manifest={"version":"imm-v4.3","start":lo,"end":b.isoformat(),"timezone":"America/New_York","five_second_master_expected_per_full_session":4680,"strike_window":"5pt strike at/below SPX +/-35 points, 15 strikes per expiry","expiry_columns":["greeks_0dte_json","greeks_1dte_json"],"greek_snapshot_timestamp_column":"uw_snapshot_at (newest source time only; use each node source_time for analysis)","collector_version_column":"collector_version","exposure_endpoint":"/api/stock/SPX/spot-exposures/expiry-strike","exposure_bases":["oi","vol","directional (bid+ask, call+put)"],"units":"UW spot exposure units; v4.2 units are incompatible; never combine unlabelled","receipt_timestamp_column":"uw_received_at","quality_columns":["uw_analysis_ready","uw_window_quality_json"],"limitations":["IV not captured","Directional exposure is a proxy, not direct hedging","Absorption and withdrawal columns are null in v4.3"],"missing_greek_strikes":"null values, never zero-filled","rolling_windows":{"spx_change_30s":"exact trailing 30 seconds","spx_change_60s":"exact trailing 60 seconds","spx_change_180s":"exact trailing 3 minutes","spx_change_300s":"exact trailing 5 minutes","es_interval_activity":"preceding 5 seconds","greek_change_1_3_5_10min":"inside each node changes: per source-time, fixed strike/expiry/basis; baseline at or before target, max lag 60s; null when unavailable; do not count repeated 5s rows as observations"},"tables":{}}
 with connect() as c,zipfile.ZipFile(path,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
  cur=c.execute("SELECT * FROM master WHERE session_date_et>=? AND session_date_et<? ORDER BY session_date_et,interval_end_utc",(lo,hi));count=0
  with z.open("master.csv","w") as f:
   txt=io.TextIOWrapper(f,encoding="utf-8",newline="");w=csv.writer(txt);w.writerow([x[0] for x in cur.description])
   while batch:=cur.fetchmany(1000):w.writerows(batch);count+=len(batch)
   txt.flush()
  manifest["tables"]["master"]=count
  manifest['quality_summary']=[dict(r) for r in c.execute("SELECT session_date_et,COUNT(*) AS rows,MIN(interval_end_utc) AS first_row,MAX(interval_end_utc) AS last_row,SUM(CASE WHEN collector_version='imm-v4.3' THEN 1 ELSE 0 END) AS v43_rows,SUM(CASE WHEN uw_analysis_ready=1 THEN 1 ELSE 0 END) AS greek_ready_rows FROM master WHERE session_date_et>=? AND session_date_et<? GROUP BY session_date_et",(lo,hi))]
  z.writestr("manifest.json",json.dumps(manifest,indent=2))
 return FileResponse(path,filename=f"imm_{lo}_to_{b}.zip",background=BackgroundTask(lambda:os.unlink(path)))
@app.get("/",response_class=HTMLResponse,dependencies=[Depends(auth)])
def home():return '<h2>IMM v4.3 master export</h2><form action="/export"><input type="date" name="start" required><input type="date" name="end" required><button>Download ZIP</button></form><a href="/status">Collection status</a> | <a href="/diagnostics">Greek diagnostics</a>'

