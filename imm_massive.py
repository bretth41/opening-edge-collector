import asyncio,json,os
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import websockets
from imm_store import write,audit
ET=ZoneInfo("America/New_York"); URL="wss://socket.massive.com/indices"; SUB="A.I:SPX"
def iso_ms(v):
 try:return datetime.fromtimestamp(int(v)/1000,timezone.utc).isoformat()
 except Exception:return None
def num(v):
 try:return float(v)
 except (TypeError,ValueError):return None
class SPXState:
 def __init__(self):
  self.price=self.source_time=self.received_at=None;self.connected=self.authenticated=self.subscribed=False;self.last_status=None;self.reconnects=0
 def update(self,m):
  if m.get("ev")!="A" or m.get("sym")!="I:SPX":return
  px=num(m.get("c"));src=iso_ms(m.get("e"))
  if px is None or src is None or (self.source_time and src<=self.source_time):return
  recv=datetime.now(timezone.utc).isoformat();self.price=px;self.source_time=src;self.received_at=recv
  day=datetime.fromisoformat(src).astimezone(ET).date().isoformat()
  write("INSERT OR IGNORE INTO price_history VALUES(?,?,?,?,?)",(recv,src,day,px,"MASSIVE:I:SPX:A1S"))
 def quality(self):return {"connected":self.connected,"authenticated":self.authenticated,"subscribed":self.subscribed,"channel":SUB,"reconnects":self.reconnects,"last_status":self.last_status}
SPX=SPXState()
async def run():
 key=os.getenv("MASSIVE_API_KEY")
 if not key:raise RuntimeError("MASSIVE_API_KEY missing")
 while True:
  try:
   SPX.connected=SPX.authenticated=SPX.subscribed=False
   async with websockets.connect(URL,ping_interval=20,ping_timeout=20,max_queue=2048) as ws:
    SPX.connected=True;await ws.send(json.dumps({"action":"auth","params":key}))
    async for raw in ws:
     msgs=json.loads(raw);msgs=msgs if isinstance(msgs,list) else [msgs]
     for m in msgs:
      if not isinstance(m,dict):continue
      if m.get("ev")=="status":
       SPX.last_status=m;status=m.get("status")
       if status=="auth_success" and not SPX.authenticated:
        SPX.authenticated=True;await ws.send(json.dumps({"action":"subscribe","params":SUB}));continue
       if status=="success" and "subscribed" in str(m.get("message","")).lower():
        if not SPX.subscribed:SPX.subscribed=True;audit("MASSIVE","INFO","real-time SPX subscribed A.I:SPX")
        continue
       if status in ("auth_failed","not_authorized","max_connections","error"):raise RuntimeError(f"Massive status: {m}")
      else:SPX.update(m)
  except Exception as e:
   SPX.reconnects+=1;SPX.connected=SPX.authenticated=SPX.subscribed=False;audit("MASSIVE","ERROR",f"{type(e).__name__}: {str(e)[:300]}; reconnect in 5s");await asyncio.sleep(5)
