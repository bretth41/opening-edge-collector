import asyncio,os,re
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import httpx
from imm_store import many,audit,js
ET=ZoneInfo("America/New_York");ROOT="https://api.unusualwhales.com";OPTION=re.compile(r"^(?:SPXW|SPX)(\d{6})([CP])(\d{8})$")
def num(x):
 try:return float(x)
 except (ValueError,TypeError):return None
def parse(s):
 m=OPTION.match(str(s or ""));return (f"20{m[1][:2]}-{m[1][2:4]}-{m[1][4:]}",m[2],int(m[3])/1000) if m else None
def rows(v):
 if isinstance(v,list):return v
 if isinstance(v,dict):
  x=v.get("data",v.get("results",[]));return x if isinstance(x,list) else []
 return []
class UW:
 def __init__(self):self.lock=asyncio.Lock();self.updated=None;self.rows={};self.expiries=[];self.quality={}
 async def get(self,c,path,params=None):
  r=await c.get(ROOT+path,params=params,timeout=30)
  if r.status_code in (401,403):raise RuntimeError(f"UW auth/entitlement HTTP {r.status_code} {path}")
  r.raise_for_status();return rows(r.json())
 async def symbols(self,c,day):
  data=await self.get(c,"/api/stock/SPX/option-chains",{"date":day});out=[]
  for x in data:
   if isinstance(x,str):out.append(x)
   elif isinstance(x,dict):
    s=x.get("option_symbol") or x.get("option_chain_id")
    if s:out.append(s)
  return out
 async def contracts(self,c,expiry):
  out=[]
  for page in range(50):
   x=await self.get(c,"/api/stock/SPX/option-contracts",{"expiry":expiry,"page":page,"limit":500});out+=x
   if len(x)<500:return out
  raise RuntimeError("UW contract pagination safety limit exceeded")
 async def once(self,c):
  now=datetime.now(timezone.utc);recv=now.isoformat();day=now.astimezone(ET).date().isoformat();syms=await self.symbols(c,day)
  expiries=sorted({p[0] for s in syms if (p:=parse(s)) and p[0]>=day})[:2]
  if not expiries or expiries[0]!=day:
   self.expiries=[];self.rows={};self.quality={"last_attempt":recv,"zero_dte_confirmed":False,"expiry_discovery":"SPX option-chains symbols","symbol_count":len(syms),"available_expiries":expiries};audit("UW","WARN",f"option-chains did not confirm 0DTE {day}; symbols={len(syms)} expiries={expiries}");return
  updated={};total=0;ng=0
  for rank,expiry in enumerate(expiries):
   gs=await self.get(c,"/api/stock/SPX/greeks",{"date":day,"expiry":expiry});cs=await self.contracts(c,expiry);bysym={x.get("option_symbol"):x for x in cs if isinstance(x,dict) and x.get("option_symbol")};byg={num(g.get("strike")):g for g in gs if num(g.get("strike")) is not None}
   strikes=sorted({p[2] for s in syms if (p:=parse(s)) and p[0]==expiry});batch=[]
   for strike in strikes:
    g=byg.get(strike,{});cp={}
    for side,typ in (("call","C"),("put","P")):
     sym=next((s for s in syms if (p:=parse(s)) and p[0]==expiry and p[1]==typ and p[2]==strike),None);cp[side]=bysym.get(sym,{})
    source=str(g.get("time") or g.get("timestamp") or "") or None;updated[(expiry,strike)]={"expiry":expiry,"rank":rank,"strike":strike,"greeks":g,"contracts":cp,"source":source,"received":recv};batch.append((recv,source,day,expiry,rank,strike,None,js(g),js(cp),None))
   many("INSERT OR REPLACE INTO strike_history VALUES(?,?,?,?,?,?,?,?,?,?)",batch);total+=len(batch);ng+=len(gs)
  async with self.lock:self.rows=updated;self.expiries=expiries;self.updated=recv
  self.quality={"last_success":recv,"zero_dte_confirmed":True,"expiry_discovery":"SPX option-chains symbols","symbol_count":len(syms),"strike_count":total,"greek_rows":ng,"available_expiries":expiries,"uw_is_spx_price_source":False};audit("UW","INFO",f"SPX 0DTE confirmed {expiries[0]}; next={expiries[1] if len(expiries)>1 else None}; strikes={total}; greek_rows={ng}")
 async def poll(self):
  async with httpx.AsyncClient(headers={"Authorization":os.environ["UW_TOKEN"],"Accept":"application/json"}) as c:
   while True:
    try:await self.once(c)
    except Exception as e:audit("UW","ERROR",f"{type(e).__name__}: {str(e)[:300]}")
    await asyncio.sleep(max(15,int(os.getenv("IMM_UW_POLL_SECONDS","60"))))
UW_STATE=UW()
