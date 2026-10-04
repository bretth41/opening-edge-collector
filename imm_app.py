import os,sqlite3,zipfile,tempfile,csv,io,json,secrets
from datetime import date,timedelta
from fastapi import FastAPI,Depends,HTTPException
from fastapi.security import HTTPBasic,HTTPBasicCredentials
from fastapi.responses import FileResponse,HTMLResponse
from starlette.background import BackgroundTask
from imm_store import DB,connect
app=FastAPI(docs_url=None,redoc_url=None,title='IMM Collector v3')
sec=HTTPBasic(auto_error=False)
def auth(c:HTTPBasicCredentials=Depends(sec)):
 if not os.getenv('EDGE_DASH_PASSWORD') or not c or not secrets.compare_digest(c.password,os.getenv('EDGE_DASH_PASSWORD')):raise HTTPException(401,headers={'WWW-Authenticate':'Basic'})
def dates(s,e):
 try:a,b=date.fromisoformat(s),date.fromisoformat(e)
 except ValueError:raise HTTPException(400,'Dates must be YYYY-MM-DD')
 if b<a or (b-a).days>31:raise HTTPException(400,'Invalid range (max 32 days)')
 return a,b
@app.get('/health')
def health():
 try:
  with connect() as c:
   n=c.execute('SELECT COUNT(*) FROM master').fetchone()[0];latest=c.execute('SELECT MAX(interval_end_utc) FROM master').fetchone()[0]
  return {'ok':True,'version':'imm-v3','master_rows':n,'last_master_row':latest,'db':DB.name}
 except Exception:return {'ok':False,'version':'imm-v3'}
@app.get('/status',dependencies=[Depends(auth)])
def status():
 with connect() as c:
  return {t:c.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in ['master','strike_history','price_history','price_continuation','option_tape','tape_checkpoint','es_event','audit']}
@app.get('/export',dependencies=[Depends(auth)])
def export(start:str,end:str):
 a,b=dates(start,end);lo=a.isoformat();hi=(b+timedelta(days=1)).isoformat();fd,path=tempfile.mkstemp(suffix='.zip',prefix='imm_');os.close(fd)
 tables={'master':'session_date_et','strike_history':'session_date_et','price_history':'session_date_et','option_tape':'received_at','audit':'at','price_continuation':'session_date_et','tape_checkpoint':'session_date_et'}
 manifest={'version':'imm-v3','start':lo,'end':b.isoformat(),'timezone':'America/New_York','five_second_master_expected':1440,'spx_price_cadence':'observed only; inspect source age','tables':{}}
 with connect() as c,zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
  for table,col in tables.items():
   if table in ('audit','option_tape'):
    from datetime import datetime,timezone
    begin=datetime.combine(a,datetime.min.time()).replace(tzinfo=timezone.utc).isoformat();finish=datetime.combine(b+timedelta(days=1),datetime.min.time()).replace(tzinfo=timezone.utc).isoformat()
   else:begin,finish=lo,hi
   cur=c.execute(f'SELECT * FROM {table} WHERE {col}>=? AND {col}<? ORDER BY {col}',(begin,finish));count=0
   with z.open(table+'.csv','w') as f:
    txt=io.TextIOWrapper(f,encoding='utf-8',newline='');w=csv.writer(txt);w.writerow([x[0] for x in cur.description])
    while rows:=cur.fetchmany(3000):w.writerows(rows);count+=len(rows)
    txt.flush()
   manifest['tables'][table]=count
  manifest['quality_note']='Review /status and source age; zero rows do not imply healthy feed.'
  z.writestr('manifest.json',json.dumps(manifest,indent=2))
 return FileResponse(path,filename=f'imm_{lo}_to_{b}.zip',background=BackgroundTask(lambda:os.unlink(path)))
@app.get('/',response_class=HTMLResponse,dependencies=[Depends(auth)])
def home():return '<h2>IMM v3 research export</h2><form action="/export"><input type="date" name="start" required><input type="date" name="end" required><button>Download ZIP</button></form><a href="/status">Collection status</a>'
