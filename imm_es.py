"""Databento MBO adapter. Native event order retained; no inferred SPX values."""
import os,asyncio,threading,time
from datetime import datetime,timezone
from collections import defaultdict
from imm_store import audit,write,iso,js
class ESBook:
 def __init__(self):
  self.lock=threading.RLock();self.orders={};self.bid=defaultdict(int);self.ask=defaultdict(int);self.valid=False;self.gap=0;self.contract=None;self.instrument=None;self.last=None;self.counters=defaultdict(int);self.seq=0
 def reset(self):
  self.orders.clear();self.bid.clear();self.ask.clear();self.valid=False;self.gap+=1
 def update(self,r,receipt):
  action=str(getattr(r,'action','')).upper();side=str(getattr(r,'side','')).upper();flags=int(getattr(r,'flags',0));oid=str(getattr(r,'order_id',''));iid=getattr(r,'instrument_id',None)
  px_raw=getattr(r,'price',None);px=None if px_raw is None else float(px_raw)/1e9
  qty=int(getattr(r,'size',0) or 0);event=iso(getattr(r,'ts_event',None));
  with self.lock:
   self.seq+=1;self.instrument=iid;self.last=receipt
   # Snapshot completion is flagged by Databento LAST (0x80); never treat partial snapshot as complete.
   if action=='R':self.reset()
   if action in ('A','M','C','F','T') and oid:
    old=self.orders.pop(oid,None)
    if old:
     s,p,q=old;book=self.bid if s=='B' else self.ask;book[p]-=q
     if book[p]<=0:book.pop(p,None)
    if action in ('A','M') and side in ('B','A') and px is not None and qty>0:
     self.orders[oid]=(side,px,qty);(self.bid if side=='B' else self.ask)[px]+=qty
    if action=='A':self.counters['es_add_'+('bid' if side=='B' else 'ask')]+=qty
    if action=='C':self.counters['es_cancel_'+('bid' if side=='B' else 'ask')]+=qty
    if action=='M':self.counters['es_modify_'+('bid' if side=='B' else 'ask')]+=qty
   if action=='T':self.counters['es_trade_'+('sell' if side=='B' else 'buy')]+=qty
   self.counters['es_events']+=1
   # A snapshot LAST flag alone does not prove a coherent order book.
   if flags&0x80 and self.bid and self.ask:self.valid=True
  if os.getenv('IMM_RAW_ES','1')=='1':
   # Never log credentials; retain raw order events for limited audit/replay.
   write('INSERT OR IGNORE INTO es_event VALUES(?,?,?,?,?,?,?,?,?,?,?)',(f'{receipt}:{self.seq}',receipt,event,iid,action,side,px,qty,oid,flags,js({'ts_recv':iso(getattr(r,'ts_recv',None))})))
 def snapshot(self,now):
  with self.lock:
   bids=sorted(((p,q) for p,q in self.bid.items() if q>0),reverse=True)[:5];asks=sorted(((p,q) for p,q in self.ask.items() if q>0))[:5]
   v=self.valid and bool(bids) and bool(asks) and bids[0][0]<asks[0][0]
   out=dict(self.counters);self.counters.clear()
   out.update(es_contract=self.contract,es_instrument_id=self.instrument,es_bid=bids[0][0] if v else None,es_ask=asks[0][0] if v else None,es_spread=asks[0][0]-bids[0][0] if v else None,es_bid_depth_1=bids[0][1] if v else None,es_ask_depth_1=asks[0][1] if v else None,es_bid_depth_5=sum(q for _,q in bids) if v else None,es_ask_depth_5=sum(q for _,q in asks) if v else None,es_last_event_at=self.last,es_age_seconds=(now-datetime.fromisoformat(self.last)).total_seconds() if self.last else None,es_book_valid=int(v),es_feed_gap=self.gap)
   return out
BOOK=ESBook()
def _run_live():
 import databento as db
 key=os.getenv('DATABENTO_API_KEY')
 if not key:raise RuntimeError('DATABENTO_API_KEY missing')
 while True:
  try:
   client=db.Live(key=key)
   client.subscribe(dataset='GLBX.MDP3',schema='mbo',stype_in='continuous',symbols='ES.c.0',snapshot=True)
   audit('ES','INFO','MBO subscription opened ES.c.0 snapshot requested')
   for record in client:
    if not hasattr(record,'action'):continue
    BOOK.update(record,datetime.now(timezone.utc).isoformat())
  except Exception as e:
   with BOOK.lock:BOOK.reset()
   audit('ES','ERROR',f'{type(e).__name__}: {str(e)[:300]}; reconnect in 10s')
   time.sleep(10)
async def run():await asyncio.to_thread(_run_live)
