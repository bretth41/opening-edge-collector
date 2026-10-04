import asyncio,os,re,time
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import httpx
from imm_store import write,many,audit,js
ET=ZoneInfo('America/New_York');ROOT='https://api.unusualwhales.com';OPTION=re.compile(r'^(?:SPXW|SPX)(\d{6})([CP])(\d{8})$')
def num(x):
 try:return float(x)
 except (ValueError,TypeError):return None
def parse(s):
 m=OPTION.match(str(s or ''))
 return (f'20{m[1][:2]}-{m[1][2:4]}-{m[1][4:]}',m[2],int(m[3])/1000) if m else None
class UW:
 def __init__(self):self.lock=asyncio.Lock();self.spot=None;self.spot_source=None;self.spot_recv=None;self.updated=None;self.rows={};self.expiries=[];self.last_price=None;self.quality={}
 def update_spot(self,price,source,received):
  if price is None:return
  with_time=source or received
  if self.spot_source and with_time<self.spot_source:return
  self.spot=price;self.spot_source=with_time;self.spot_recv=received
  if self.last_price!=(with_time,price):
   write('INSERT OR IGNORE INTO price_history VALUES(?,?,?,?,?)',(received,with_time,datetime.now(ET).date().isoformat(),price,'UW'))
   self.last_price=(with_time,price)
 async def get(self,client,path,params=None):
  r=await client.get(ROOT+path,params=params,timeout=25);r.raise_for_status();v=r.json();return v.get('data',[]) if isinstance(v,dict) else []
 async def contracts(self,c,expiry):
  out=[]
  for page in range(0,50):
   rows=await self.get(c,'/api/stock/SPX/option-contracts',{'expiry':expiry,'page':page,'limit':500})
   out+=rows
   if len(rows)<500:return out
  raise RuntimeError('contract pagination safety limit exceeded')
 async def once(self,c):
  now=datetime.now(timezone.utc);recv=now.isoformat();day=now.astimezone(ET).date().isoformat()
  exp=await self.get(c,'/api/stock/SPX/expiry-breakdown')
  dates=sorted({str(x.get('expires')) for x in exp if x.get('expires') and str(x.get('expires'))>=day})[:2]
  if not dates or dates[0]!=day:
   audit('UW','WARN',f'No confirmed SPX 0DTE expiry for {day}: {dates}');return
  self.expiries=dates;total=0;updated={}
  for rank,expiry in enumerate(dates):
   greeks,contracts=await asyncio.gather(self.get(c,'/api/stock/SPX/greeks',{'expiry':expiry}),self.contracts(c,expiry))
   bysym={r.get('option_symbol'):r for r in contracts if r.get('option_symbol')}
   rows=[]
   for g in greeks:
    strike=num(g.get('strike'))
    if strike is None:continue
    cp={}
    for side in ('call','put'):
     sym=g.get(f'{side}_option_symbol')
     if not sym:
      matches=[x for x in contracts if (p:=parse(x.get('option_symbol'))) and p[0]==expiry and p[2]==strike and p[1]==('C' if side=='call' else 'P')]
      sym=matches[0]['option_symbol'] if matches else None
     cp[side]=bysym.get(sym,{})
    source=g.get('time') or g.get('timestamp') or None
    source=str(source) if source else None
    price=num(g.get('price'))
    if price is not None:self.update_spot(price,source,recv)
    record={'expiry':expiry,'rank':rank,'strike':strike,'greeks':g,'contracts':cp,'source':source,'received':recv}
    updated[(expiry,strike)]=record
    rows.append((recv,source,day,expiry,rank,strike,price,js(g),js(cp),None))
   many('INSERT OR REPLACE INTO strike_history VALUES(?,?,?,?,?,?,?,?,?,?)',rows)
   total+=len(rows)
  async with self.lock:self.rows=updated;self.updated=recv
  self.quality={'last_success':recv,'strike_count':total,'expiry_count':len(dates)}
 async def stream_price(self):
  """SPX websocket price from UW gex_strike_expiry; validates actual cadence live."""
  import websockets,json
  token=os.environ['UW_TOKEN']
  while True:
   try:
    async with websockets.connect('wss://api.unusualwhales.com/socket?token='+token,ping_interval=20) as ws:
     await ws.send(json.dumps({'msg_type':'join','channel':'gex_strike_expiry:SPX'}))
     audit('UW_WS','INFO','SPX exposure channel join sent')
     async for raw in ws:
      msg=json.loads(raw)
      if not isinstance(msg,list) or len(msg)<2 or not isinstance(msg[1],dict):continue
      p=msg[1];px=num(p.get('price'));ms=p.get('timestamp')
      if px is None or not ms:continue
      src=datetime.fromtimestamp(int(ms)/1000,timezone.utc).isoformat();recv=datetime.now(timezone.utc).isoformat()
      self.update_spot(px,src,recv)
   except Exception as e:
    audit('UW_WS','ERROR',f'{type(e).__name__}: {str(e)[:200]}');await asyncio.sleep(10)
 async def poll(self):
  headers={'Authorization':os.environ['UW_TOKEN'],'Accept':'application/json'}
  async with httpx.AsyncClient(headers=headers) as c:
   while True:
    try:await self.once(c)
    except Exception as e:audit('UW','ERROR',f'{type(e).__name__}: {str(e)[:250]}')
    await asyncio.sleep(int(os.getenv('IMM_UW_POLL_SECONDS','60')))
UW_STATE=UW()
