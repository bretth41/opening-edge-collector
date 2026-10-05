"""Databento ES MBP-1 adapter for Standard-plan live data.
Captures every BBO-changing event, trades, top-of-book depth/order counts, and neutral
microstructure observables. No dealer-hedging or absorption conclusion is inferred here.
"""
import os,asyncio,threading,time,math
from datetime import datetime,timezone
from collections import defaultdict
from imm_store import audit,write,js

TICK=0.25
UNDEF_PRICE=9223372036854775807

def _px(v):
 try:
  if v is None:return None
  x=int(v)
  if x in (UNDEF_PRICE,-UNDEF_PRICE):return None
  return x/1e9
 except Exception:return None

def _ts(v):
 try:
  if v is None:return None
  if isinstance(v,datetime):return v.astimezone(timezone.utc).isoformat()
  return datetime.fromtimestamp(int(v)/1e9,timezone.utc).isoformat()
 except Exception:return None

def _f(v):
 try:return float(v)
 except Exception:return None

class ESBook:
 def __init__(self):
  self.lock=threading.RLock();self.valid=False;self.gap=0;self.contract=None;self.instrument=None;self.last=None;self.seq=0
  self.bid=None;self.ask=None;self.bid_sz=None;self.ask_sz=None;self.bid_ct=None;self.ask_ct=None
  self.counters=defaultdict(float);self.window={}
 def reset(self):
  self.valid=False;self.gap+=1;self.bid=self.ask=self.bid_sz=self.ask_sz=self.bid_ct=self.ask_ct=None
  self.counters.clear();self.window={}
 def _begin_window_if_needed(self,mid):
  if not self.window:
   self.window={'mid_start':mid,'mid_high':mid,'mid_low':mid,'bid_start':self.bid_sz,'ask_start':self.ask_sz}
  elif mid is not None:
   if self.window.get('mid_high') is None or mid>self.window['mid_high']:self.window['mid_high']=mid
   if self.window.get('mid_low') is None or mid<self.window['mid_low']:self.window['mid_low']=mid
 def update(self,r,receipt):
  action=str(getattr(r,'action','')).upper();side=str(getattr(r,'side','')).upper();iid=getattr(r,'instrument_id',None)
  event_px=_px(getattr(r,'price',None));qty=int(getattr(r,'size',0) or 0);event=_ts(getattr(r,'ts_event',None));flags=int(getattr(r,'flags',0) or 0)
  try: level=r.levels[0]
  except Exception:return
  nbid=_px(getattr(level,'bid_px',None));nask=_px(getattr(level,'ask_px',None))
  nbs=int(getattr(level,'bid_sz',0) or 0);nas=int(getattr(level,'ask_sz',0) or 0)
  nbc=int(getattr(level,'bid_ct',0) or 0);nac=int(getattr(level,'ask_ct',0) or 0)
  coherent=nbid is not None and nask is not None and nbid<nask
  with self.lock:
   self.seq+=1;self.instrument=iid;self.last=receipt
   pb,pa,pbs,pas=self.bid,self.ask,self.bid_sz,self.ask_sz
   mid=(nbid+nask)/2 if coherent else None
   self._begin_window_if_needed(mid)
   self.counters['es_events']+=1
   if action=='T':
    # Databento: B = buy aggressor, A = sell aggressor.
    if side=='B':self.counters['es_trade_buy']+=qty;self.counters['es_trade_buy_count']+=1
    elif side=='A':self.counters['es_trade_sell']+=qty;self.counters['es_trade_sell_count']+=1
   elif action in ('A','C','M'):
    suffix='bid' if side=='B' else 'ask' if side=='A' else None
    if suffix:self.counters[f'es_{ {"A":"add","C":"cancel","M":"modify"}[action] }_{suffix}']+=qty
   # Same-price BBO size changes: directly observable MBP-1 liquidity changes.
   if pb is not None and nbid==pb and pbs is not None:
    d=nbs-pbs
    if d>0:self.counters['es_bid_replenish_proxy']+=d
    elif d<0:
     self.counters['es_bid_deplete_proxy']+=-d
     if action!='T':self.counters['es_bid_withdraw_proxy']+=-d
   if pa is not None and nask==pa and pas is not None:
    d=nas-pas
    if d>0:self.counters['es_ask_replenish_proxy']+=d
    elif d<0:
     self.counters['es_ask_deplete_proxy']+=-d
     if action!='T':self.counters['es_ask_withdraw_proxy']+=-d
   # Trade volume not matched by visible same-price depth loss is a neutral reload/absorption proxy.
   if action=='T' and side=='A' and qty>0 and pb is not None and nbid==pb and pbs is not None:
    visible=max(0,pbs-nbs);self.counters['es_bid_absorption_proxy']+=max(0,qty-visible)
   if action=='T' and side=='B' and qty>0 and pa is not None and nask==pa and pas is not None:
    visible=max(0,pas-nas);self.counters['es_ask_absorption_proxy']+=max(0,qty-visible)
   self.bid,self.ask,self.bid_sz,self.ask_sz,self.bid_ct,self.ask_ct=nbid,nask,nbs,nas,nbc,nac
   self.valid=coherent
   if coherent:
    mid=(nbid+nask)/2
    if self.window.get('mid_high') is None or mid>self.window['mid_high']:self.window['mid_high']=mid
    if self.window.get('mid_low') is None or mid<self.window['mid_low']:self.window['mid_low']=mid
  if os.getenv('IMM_RAW_ES','1')=='1':
   raw={'ts_recv':_ts(getattr(r,'ts_recv',None)),'sequence':getattr(r,'sequence',None),'publisher_id':getattr(r,'publisher_id',None),'depth':getattr(r,'depth',None),'bid':nbid,'ask':nask,'bid_sz':nbs,'ask_sz':nas,'bid_ct':nbc,'ask_ct':nac}
   write('INSERT OR IGNORE INTO es_event VALUES(?,?,?,?,?,?,?,?,?,?,?)',(f'{receipt}:{self.seq}',receipt,event,iid,action,side,event_px,qty,None,flags,js(raw)))
 def mapping(self,r):
  try:
   if str(getattr(r,'stype_in_symbol',''))=='ES.c.0':
    with self.lock:self.contract=str(getattr(r,'stype_out_symbol','') or '') or self.contract;self.instrument=getattr(r,'instrument_id',self.instrument)
  except Exception:pass
 def snapshot(self,now):
  with self.lock:
   v=self.valid and self.bid is not None and self.ask is not None and self.bid<self.ask
   c=dict(self.counters);self.counters.clear();w=self.window;self.window={}
   mid=(self.bid+self.ask)/2 if v else None
   total=(c.get('es_trade_buy',0)+c.get('es_trade_sell',0))
   signed=c.get('es_trade_buy',0)-c.get('es_trade_sell',0)
   mid_start=w.get('mid_start') if w else None
   mid_change_ticks=(mid-mid_start)/TICK if mid is not None and mid_start is not None else None
   range_ticks=(w.get('mid_high')-w.get('mid_low'))/TICK if w and w.get('mid_high') is not None and w.get('mid_low') is not None else None
   displacement=(mid_change_ticks*100/total) if mid_change_ticks is not None and total>0 else None
   denom=(self.bid_sz or 0)+(self.ask_sz or 0) if v else 0
   depth_imb=((self.bid_sz or 0)-(self.ask_sz or 0))/denom if denom>0 else None
   out={
    'es_contract':self.contract,'es_instrument_id':self.instrument,'es_bid':self.bid if v else None,'es_ask':self.ask if v else None,
    'es_spread':self.ask-self.bid if v else None,'es_bid_depth_1':self.bid_sz if v else None,'es_ask_depth_1':self.ask_sz if v else None,
    'es_bid_depth_5':None,'es_ask_depth_5':None,'es_bid_order_count':self.bid_ct if v else None,'es_ask_order_count':self.ask_ct if v else None,
    'es_mid_start':mid_start,'es_mid_end':mid,'es_mid_high':w.get('mid_high') if w else None,'es_mid_low':w.get('mid_low') if w else None,
    'es_mid_change_ticks':mid_change_ticks,'es_range_ticks':range_ticks,'es_trade_imbalance':signed,'es_depth_imbalance_end':depth_imb,
    'es_displacement_ticks_per_100_contracts':displacement,'es_last_event_at':self.last,
    'es_age_seconds':(now-datetime.fromisoformat(self.last)).total_seconds() if self.last else None,'es_book_valid':int(v),'es_feed_gap':self.gap,
   }
   out.update(c)
   return out
BOOK=ESBook()

def _run_live():
 import databento as db
 key=os.getenv('DATABENTO_API_KEY')
 if not key:raise RuntimeError('DATABENTO_API_KEY missing')
 while True:
  try:
   client=db.Live(key=key)
   client.subscribe(dataset='GLBX.MDP3',schema='mbp-1',stype_in='continuous',symbols='ES.c.0')
   audit('ES','INFO','MBP-1 subscription opened ES.c.0 (Standard-plan L1)')
   for record in client:
    if hasattr(record,'stype_in_symbol'):BOOK.mapping(record);continue
    if not hasattr(record,'action'):continue
    BOOK.update(record,datetime.now(timezone.utc).isoformat())
  except Exception as e:
   with BOOK.lock:BOOK.reset()
   audit('ES','ERROR',f'{type(e).__name__}: {str(e)[:300]}; reconnect in 10s')
   time.sleep(10)
async def run():await asyncio.to_thread(_run_live)
