import asyncio,hashlib,os
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import httpx
from imm_store import many,write,audit,js
from imm_uw import parse
ET=ZoneInfo("America/New_York")
def rr(p):return p.get("data",[]) if isinstance(p,dict) else p if isinstance(p,list) else []
async def collect(c,day,watermark,max_pages=100):
 params={"ticker_symbol":"SPX","limit":500,"intraday_only":"true","expiry_dates[]":day};seen=set();newest=watermark;old=None;total=0;complete=False
 if watermark:params["newer_than"]=watermark
 for _ in range(max_pages):
  r=await c.get("https://api.unusualwhales.com/api/option-trades",params=params);r.raise_for_status();xs=rr(r.json())
  if not xs:complete=True;break
  batch=[];reached=False
  for x in xs:
   t=x.get("executed_at");sym=x.get("option_chain_id") or x.get("option_symbol") or "";p=parse(sym)
   if watermark and t and str(t)<=str(watermark):reached=True
   if not p or p[0]!=day or not t:continue
   k=str(x.get("id") or hashlib.sha256(js(x).encode()).hexdigest())
   if k in seen:continue
   seen.add(k);newest=max(str(t),str(newest)) if newest else str(t);batch.append((k,datetime.now(timezone.utc).isoformat(),t,sym,p[0],p[2],p[1],x.get("price"),x.get("size"),x.get("nbbo_bid"),x.get("nbbo_ask"),x.get("upstream_condition_detail"),js(x)))
  many("INSERT OR IGNORE INTO option_tape VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",batch);total+=len(batch);oldest=min((x.get("executed_at") for x in xs if x.get("executed_at")),default=None)
  if reached or len(xs)<500:complete=True;break
  if not oldest or oldest==old:break
  old=oldest;params["older_than"]=oldest
 return newest,complete,total
async def run():
 day=watermark=None
 async with httpx.AsyncClient(headers={"Authorization":os.environ["UW_TOKEN"],"Accept":"application/json"},timeout=35) as c:
  while True:
   try:
    n=datetime.now(ET);today=n.date().isoformat()
    if day!=today:day=today;watermark=None
    if n.weekday()<5 and (n.hour==9 and n.minute>=25 or 10<=n.hour<12):
     newest,ok,count=await collect(c,day,watermark,int(os.getenv("IMM_TAPE_MAX_PAGES","100")))
     if ok:watermark=newest;write("INSERT OR REPLACE INTO tape_checkpoint VALUES(?,?,?,?,?)",(day,watermark,datetime.now(timezone.utc).isoformat(),0,f"rows={count}"))
     else:write("INSERT OR REPLACE INTO tape_checkpoint VALUES(?,?,?,?,?)",(day,watermark,None,1,f"rows={count}"));audit("TAPE","WARN","pagination incomplete")
   except Exception as e:audit("TAPE","ERROR",f"{type(e).__name__}: {str(e)[:250]}")
   await asyncio.sleep(int(os.getenv("IMM_TAPE_POLL_SECONDS","20")))
