import asyncio,os,re
from datetime import datetime,timezone,time as dtime
from zoneinfo import ZoneInfo
import httpx
from imm_store import audit
ET=ZoneInfo("America/New_York");ROOT="https://api.unusualwhales.com"
OPTION=re.compile(r"^(?:SPXW|SPX)(\d{6})([CP])(\d{8})$")
def num(x):
 try:return float(x)
 except (ValueError,TypeError):return None
def parse(s):
 m=OPTION.match(str(s or ""))
 return (f"20{m[1][:2]}-{m[1][2:4]}-{m[1][4:]}",m[2],int(m[3])/1000) if m else None
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
 async def exposures(self,c,day,expiry):
  return await self.get(c,"/api/stock/SPX/greek-exposure/strike-expiry",{"date":day,"expiry":expiry})
 async def once(self,c):
  now=datetime.now(timezone.utc);recv=now.isoformat();local=now.astimezone(ET);day=local.date().isoformat()
  syms=await self.symbols(c,day)
  expiries=sorted({v[0] for s in syms if (v:=parse(s)) and v[0]>=day})[:2]
  if not expiries or expiries[0]!=day:
   self.quality={"last_attempt":recv,"zero_dte_confirmed":False,"symbol_count":len(syms)}
   audit("UW","WARN",f"0DTE unconfirmed for {day}");return
  updated={};exposure_count=0
  for rank,expiry in enumerate(expiries):
   es=await self.exposures(c,day,expiry)
   bystrike={num(x.get("strike")):x for x in es if isinstance(x,dict) and num(x.get("strike")) is not None}
   strikes={v[2] for s in syms if (v:=parse(s)) and v[0]==expiry}
   for strike in strikes:
    e=bystrike.get(strike)
    if e is None:continue  # missing exposure is missing, never zero
    g={"call_gamma_oi":e.get("call_gex"),"put_gamma_oi":e.get("put_gex"),"call_delta_oi":e.get("call_delta"),"put_delta_oi":e.get("put_delta"),"call_charm_oi":e.get("call_charm"),"put_charm_oi":e.get("put_charm"),"call_vanna_oi":e.get("call_vanna"),"put_vanna_oi":e.get("put_vanna")}
    updated[(expiry,strike)]={"greeks":g,"received":recv}
   exposure_count+=len(es)
  # Publish only when both expiries have been fetched; readers never see a partial update.
  async with self.lock:
   self.rows=updated;self.expiries=expiries;self.updated=recv
   self.quality={"last_success":recv,"zero_dte_confirmed":True,"strike_count":len(updated),"exposure_rows":exposure_count,"available_expiries":expiries,"exposure_source":"SPX greek-exposure/strike-expiry"}
  audit("UW","INFO",f"0DTE {expiries[0]}; next={expiries[1] if len(expiries)>1 else None}; strikes={len(updated)}")
 async def poll(self):
  async with httpx.AsyncClient(headers={"Authorization":os.environ["UW_TOKEN"],"Accept":"application/json"}) as c:
   while True:
    local=datetime.now(ET)
    if local.weekday()<5 and dtime(9,20)<=local.time().replace(tzinfo=None)<dtime(16,5):
     try:await self.once(c)
     except Exception as e:audit("UW","ERROR",f"{type(e).__name__}: {str(e)[:300]}")
    await asyncio.sleep(max(15,int(os.getenv("IMM_UW_POLL_SECONDS","60"))))
UW_STATE=UW()
