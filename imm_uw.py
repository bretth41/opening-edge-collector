"""SPX intraday spot exposure; no fallback to daily exposure."""
import asyncio, hashlib, math, os, re
from datetime import datetime, timezone, time as dtime
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
import httpx
from imm_store import audit, js
ET=ZoneInfo('America/New_York'); ROOT='https://api.unusualwhales.com'
ENDPOINT='/api/stock/SPX/spot-exposures/expiry-strike'
OPTION=re.compile(r'^(?:SPXW|SPX)(\d{6})([CP])(\d{8})$')
FIELDS=tuple(f'{s}_{g}_{b}' for s in ('call','put') for g in ('gamma','delta','charm','vanna') for b in ('oi','vol','bid','ask'))
CORE=tuple(f'{s}_{g}_oi' for s in ('call','put') for g in ('gamma','delta','charm','vanna'))
def num(x):
 try:
  v=float(x);return v if math.isfinite(v) else None
 except (ValueError,TypeError):return None
def parse(s):
 m=OPTION.match(str(s or ''))
 return (f'20{m[1][:2]}-{m[1][2:4]}-{m[1][4:]}',m[2],int(m[3])/1000) if m else None
def timestamp(v):
 try:
  t=datetime.fromisoformat(str(v).replace('Z','+00:00'))
  return t.astimezone(timezone.utc) if t.tzinfo else None
 except (ValueError,TypeError):return None
def rows(v):
 data=v if isinstance(v,list) else v.get('data') if isinstance(v,dict) else None
 if not isinstance(data,list):raise ValueError('UW schema: expected data array')
 return data
def token_header():
 token=os.environ['UW_TOKEN'].strip()
 return token if token.lower().startswith('bearer ') else 'Bearer '+token
def retry_seconds(v,now):
 try:return max(0,float(v))
 except (ValueError,TypeError):
  try:return max(0,(parsedate_to_datetime(v)-now).total_seconds())
  except (ValueError,TypeError,OverflowError):return 2
class UWError(RuntimeError):
 def __init__(self,status,path,reason=''):
  self.status=status;super().__init__(f'UW HTTP {status} {path} {reason}')
class UW:
 def __init__(self):
  self.lock=asyncio.Lock();self.session=None
  self.updated=self.received=self.last_change=None
  self.rows={};self.expiries=[];self.quality={'state':'starting','endpoint':ENDPOINT}
  self.revision=self.oi_changes=0;self.source_advances=0;self.changed_expiries=set();self.last_warning=None
 def reset_day(self,day):
  if self.session!=day:
   self.session=day;self.rows={};self.expiries=[]
   self.updated=self.received=self.last_change=None
   self.revision=self.oi_changes=0;self.source_advances=0;self.changed_expiries=set()
   self.quality={'state':'awaiting_current_session','session':day,'endpoint':ENDPOINT}
 async def get(self,c,path,params=None):
  for attempt in range(4):
   r=await c.get(ROOT+path,params=params,timeout=30);status=r.status_code
   if status in (429,503) or status>=500:
    delay=retry_seconds(r.headers.get('Retry-After'),datetime.now(timezone.utc))
    if attempt==3 or delay>30:raise UWError(status,path,'retry later; old source times retained')
    await asyncio.sleep(delay);continue
   if status!=200:raise UWError(status,path,'auth/entitlement' if status in (401,403) else 'request rejected')
   return rows(r.json())
  raise RuntimeError('unreachable')
 async def symbols(self,c,day):
  data=await self.get(c,'/api/stock/SPX/option-chains',{'date':day})
  return [x if isinstance(x,str) else x.get('option_symbol') or x.get('option_chain_id') for x in data if isinstance(x,(str,dict))]
 async def exposures(self,c,day,expiry):
  result=[];signatures=set()
  for page in range(20):
   data=await self.get(c,ENDPOINT,{'date':day,'expirations[]':expiry,'limit':500,'page':page})
   sig=hashlib.sha256(js(data).encode()).hexdigest()
   if data and sig in signatures:raise ValueError('UW repeated pagination page; snapshot not published')
   signatures.add(sig);result.extend(data)
   if len(data)<500:return result
  raise ValueError('UW pagination exceeded 20 pages; snapshot not published')
 def normalize(self,data,expiry,day,receipt):
  out={};missing_time=missing_fields=rejected=inferred=0
  for x in data:
   if not isinstance(x,dict):rejected+=1;continue
   strike=num(x.get('strike'));explicit=str(x.get('expiry') or '')[:10]
   # Current schema example omits expiry. One filtered expiry per request.
   # Explicit mismatches are rejected, never merged across expiries.
   if explicit and explicit!=expiry or strike is None:rejected+=1;continue
   src=timestamp(x.get('time'))
   if src and (src.astimezone(ET).date().isoformat()!=day or (src-receipt).total_seconds()>2):rejected+=1;continue
   if not src:missing_time+=1
   if not explicit:inferred+=1
   values={f:num(x.get(f)) for f in FIELDS};missing_fields+=sum(values[f] is None for f in CORE)
   key=(expiry,strike)
   if key in out:raise ValueError('UW duplicate strike/expiry; ambiguous response not published')
   out[key]={'greeks':values,'source_time':src.isoformat() if src else None,'received':receipt.isoformat(),'source_price':num(x.get('price')),'expiry_origin':'response' if explicit else 'single_expiry_request','raw':x}
  return out,{'missing_source_times':missing_time,'missing_core_fields':missing_fields,'rejected_rows':rejected,'expiry_from_request_rows':inferred}
 async def once(self,c,now=None):
  started=now or datetime.now(timezone.utc);day=started.astimezone(ET).date().isoformat();self.reset_day(day)
  self.quality={**self.quality,'last_attempt':started.isoformat()}
  syms=await self.symbols(c,day)
  expiries=sorted({v[0] for s in syms if (v:=parse(s)) and v[0]>=day})[:2]
  if len(expiries)!=2 or expiries[0]!=day:raise ValueError(f'UW needs confirmed same-day and next listed expiry for {day}')
  data={}
  for expiry in expiries:data[expiry]=await self.exposures(c,day,expiry)
  if data[expiries[0]] and data[expiries[0]]==data[expiries[1]]:
   raise ValueError('UW returned identical payloads for distinct expiries; verify expiry filter')
  receipt=now or datetime.now(timezone.utc)
  if receipt.astimezone(ET).date().isoformat()!=day:raise ValueError('UW session changed during request; discarded')
  updated={};details={};changed=advanced=regressed=0;same_time_changes=0;changed_expiries=set()
  for expiry in expiries:
   normalized,details[expiry]=self.normalize(data[expiry],expiry,day,receipt)
   if not normalized:raise ValueError(f'UW no valid spot exposures for {expiry}')
   for key,item in normalized.items():
    prior=self.rows.get(key);old=timestamp(prior['source_time']) if prior else None;new=timestamp(item['source_time'])
    if old and (new is None or new<old):updated[key]=prior;regressed+=1;continue
    if prior:
     # First discovery of a strike is not evidence of change.
     numeric_change=any(item['greeks'][f] is not None and prior['greeks'][f] is not None and item['greeks'][f]!=prior['greeks'][f] for f in CORE)
     if numeric_change and old and new and new>old:changed+=1;changed_expiries.add(expiry)
     elif numeric_change:same_time_changes+=1
     if old and new and new>old:advanced+=1
    updated[key]=item
  sig=hashlib.sha256(js([(k,v['source_time'],v['greeks']) for k,v in sorted(updated.items())]).encode()).hexdigest()
  revision=self.revision+int(sig!=self.quality.get('snapshot_id'))
  sources=[v['source_time'] for v in updated.values() if v['source_time']]
  async with self.lock:
   self.rows=updated;self.expiries=expiries;self.received=receipt.isoformat();self.updated=max(sources) if sources else None
   self.revision=revision;self.oi_changes+=changed;self.source_advances+=advanced;self.changed_expiries.update(changed_expiries)
   if changed:self.last_change=self.received
   self.quality={'state':'received','session':day,'last_success':self.received,'endpoint':ENDPOINT,'exposure_source':'SPX intraday spot exposure','zero_dte_confirmed':True,'available_expiries':expiries,'strike_count':len(updated),'exposure_rows':sum(map(len,data.values())),'snapshot_id':sig,'revision':self.revision,'oi_changed_nodes_this_poll':changed,'oi_changed_nodes_session':self.oi_changes,'changed_expiries':sorted(self.changed_expiries),'source_times_advanced_this_poll':advanced,'source_time_regressions':regressed,'numeric_changes_without_source_advance':same_time_changes,'source_times_advanced_session':self.source_advances,'last_oi_change_received_at':self.last_change,'schema':details,'units':'UW spot exposure; not v4.2 Greek*OI*100 units','source_price_is_index_atm_estimate':True}
 async def poll(self):
  async with httpx.AsyncClient(headers={'Authorization':token_header(),'Accept':'application/json'}) as c:
   probed_day=None
   while True:
    local=datetime.now(ET);day=local.date().isoformat();self.reset_day(day)
    # Once after deployment even off hours: proves access, never live validation.
    if probed_day!=day or local.weekday()<5 and dtime(9,20)<=local.time().replace(tzinfo=None)<dtime(16,5):
     probed_day=day
     try:await self.once(c);self.last_warning=None
     except Exception as e:
      kind='access_denied' if isinstance(e,UWError) and e.status in (401,403) else 'fetch_error'
      self.quality={**self.quality,'state':kind,'error':str(e)[:300],'last_error_at':datetime.now(timezone.utc).isoformat()}
      if str(e)!=self.last_warning:audit('UW','ERROR',str(e)[:300]);self.last_warning=str(e)
    await asyncio.sleep(max(15,int(os.getenv('IMM_UW_POLL_SECONDS','30'))))
UW_STATE=UW()
