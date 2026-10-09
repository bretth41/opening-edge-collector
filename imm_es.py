import os,asyncio,threading,time
from datetime import datetime,timezone
from collections import defaultdict
from imm_store import audit,write,js
TICK=.25;UNDEF=9223372036854775807
def px(v):
 try:
  x=int(v);return None if x in (UNDEF,-UNDEF) else x/1e9
 except Exception:return None
def ts(v):
 try:return datetime.fromtimestamp(int(v)/1e9,timezone.utc).isoformat()
 except Exception:return None
class ESBook:
 def __init__(self):self.lock=threading.RLock();self.valid=False;self.gap=0;self.contract=None;self.instrument=None;self.last=None;self.seq=0;self.bid=self.ask=self.bid_sz=self.ask_sz=self.bid_ct=self.ask_ct=None;self.counters=defaultdict(float);self.window={}
 def reset(self):self.valid=False;self.gap+=1;self.counters.clear();self.window={}
 def update(self,r,receipt):
  try:l=r.levels[0]
  except Exception:return
  action=str(getattr(r,"action","")).upper();side=str(getattr(r,"side","")).upper();q=int(getattr(r,"size",0) or 0);nb=px(l.bid_px);na=px(l.ask_px);nbs=int(l.bid_sz or 0);nas=int(l.ask_sz or 0);nbc=int(l.bid_ct or 0);nac=int(l.ask_ct or 0);ok=nb is not None and na is not None and nb<na
  with self.lock:
   self.seq+=1;self.instrument=getattr(r,"instrument_id",None);self.last=receipt;pb,pa,pbs,pas=self.bid,self.ask,self.bid_sz,self.ask_sz;mid=(nb+na)/2 if ok else None
   if not self.window:self.window={"mid_start":mid,"mid_high":mid,"mid_low":mid}
   self.counters["es_events"]+=1
   if action=="T":
    if side=="B":self.counters["es_trade_buy"]+=q;self.counters["es_trade_buy_count"]+=1
    elif side=="A":self.counters["es_trade_sell"]+=q;self.counters["es_trade_sell_count"]+=1
   if pb is not None and nb==pb and pbs is not None:
    d=nbs-pbs
    if d>0:self.counters["es_bid_replenish_proxy"]+=d
    elif d<0:self.counters["es_bid_deplete_proxy"]+=-d;self.counters["es_bid_withdraw_proxy"]+=(-d if action!="T" else 0)
   if pa is not None and na==pa and pas is not None:
    d=nas-pas
    if d>0:self.counters["es_ask_replenish_proxy"]+=d
    elif d<0:self.counters["es_ask_deplete_proxy"]+=-d;self.counters["es_ask_withdraw_proxy"]+=(-d if action!="T" else 0)
   if action=="T" and side=="A" and pb==nb and pbs is not None:self.counters["es_bid_absorption_proxy"]+=max(0,q-max(0,pbs-nbs))
   if action=="T" and side=="B" and pa==na and pas is not None:self.counters["es_ask_absorption_proxy"]+=max(0,q-max(0,pas-nas))
   self.bid,self.ask,self.bid_sz,self.ask_sz,self.bid_ct,self.ask_ct=nb,na,nbs,nas,nbc,nac;self.valid=ok
   if ok:self.window["mid_high"]=max(x for x in (self.window.get("mid_high"),mid) if x is not None);self.window["mid_low"]=min(x for x in (self.window.get("mid_low"),mid) if x is not None)
  if os.getenv("IMM_RAW_ES","1")=="1":write("INSERT OR IGNORE INTO es_event VALUES(?,?,?,?,?,?,?,?,?,?,?)",(f"{receipt}:{self.seq}",receipt,ts(getattr(r,"ts_event",None)),self.instrument,action,side,px(getattr(r,"price",None)),q,None,int(getattr(r,"flags",0) or 0),js({"bid":nb,"ask":na,"bid_sz":nbs,"ask_sz":nas,"bid_ct":nbc,"ask_ct":nac})))
 def mapping(self,r):
  if str(getattr(r,"stype_in_symbol",""))=="ES.c.0":self.contract=str(getattr(r,"stype_out_symbol","") or "") or self.contract
 def quality(self,now):
  with self.lock:
   age=(now-datetime.fromisoformat(self.last)).total_seconds() if self.last else None
   return {'schema':'mbp-1','depth_levels':1,'contract':self.contract,'valid_and_fresh':bool(self.valid and age is not None and -2<=age<=2),'receipt_age_seconds':max(0,age) if age is not None else None,'feed_resets':self.gap,'absorption_available':False,'withdrawal_available':False}
 def snapshot(self,now):
  with self.lock:
   age=(now-datetime.fromisoformat(self.last)).total_seconds() if self.last else None;v=self.valid and self.bid is not None and self.ask is not None and self.bid<self.ask and age is not None and -2<=age<=2;c=dict(self.counters);self.counters.clear();w=self.window;self.window={};mid=(self.bid+self.ask)/2 if v else None;start=w.get("mid_start") if w else None;chg=(mid-start)/TICK if mid is not None and start is not None else None;rng=(w.get("mid_high")-w.get("mid_low"))/TICK if w and w.get("mid_high") is not None and w.get("mid_low") is not None else None;tot=c.get("es_trade_buy",0)+c.get("es_trade_sell",0);den=(self.bid_sz or 0)+(self.ask_sz or 0)
   out={"es_contract":self.contract,"es_instrument_id":self.instrument,"es_bid":self.bid if v else None,"es_ask":self.ask if v else None,"es_spread":self.ask-self.bid if v else None,"es_bid_depth_1":self.bid_sz if v else None,"es_ask_depth_1":self.ask_sz if v else None,"es_bid_depth_5":None,"es_ask_depth_5":None,"es_bid_order_count":self.bid_ct if v else None,"es_ask_order_count":self.ask_ct if v else None,"es_mid_start":start,"es_mid_end":mid,"es_mid_high":w.get("mid_high") if w else None,"es_mid_low":w.get("mid_low") if w else None,"es_mid_change_ticks":chg,"es_range_ticks":rng,"es_trade_imbalance":c.get("es_trade_buy",0)-c.get("es_trade_sell",0),"es_depth_imbalance_end":((self.bid_sz or 0)-(self.ask_sz or 0))/den if den else None,"es_displacement_ticks_per_100_contracts":chg*100/tot if chg is not None and tot else None,"es_last_event_at":self.last,"es_age_seconds":(now-datetime.fromisoformat(self.last)).total_seconds() if self.last else None,"es_book_valid":int(v),"es_feed_gap":self.gap};out.update(c)
   # MBP-1 book snapshots/trades do not identify cancellations or true absorption.
   # Historical columns stay for compatibility; never label traded volume as absorption.
   for key in ('es_bid_absorption_proxy','es_ask_absorption_proxy','es_bid_withdraw_proxy','es_ask_withdraw_proxy'):out[key]=None
   if not v:
    for key in ('es_depth_imbalance_end','es_displacement_ticks_per_100_contracts','es_mid_start','es_mid_end','es_mid_high','es_mid_low','es_mid_change_ticks','es_range_ticks','es_trade_imbalance'):out[key]=None
   for key in ('es_trade_buy','es_trade_sell','es_trade_buy_count','es_trade_sell_count','es_events','es_bid_replenish_proxy','es_ask_replenish_proxy','es_bid_deplete_proxy','es_ask_deplete_proxy'):
    out[key]=c.get(key,0) if v and c.get('es_events',0)>0 else None
   out['es_age_seconds']=max(0,age) if age is not None else None
   return out
BOOK=ESBook()
def live():
 import databento as db
 while True:
  try:
   c=db.Live(key=os.environ["DATABENTO_API_KEY"]);c.subscribe(dataset="GLBX.MDP3",schema="mbp-1",stype_in="continuous",symbols="ES.c.0");audit("ES","INFO","MBP-1 subscription opened ES.c.0 (Standard-plan L1)")
   for r in c:
    if hasattr(r,"stype_in_symbol"):BOOK.mapping(r);continue
    if hasattr(r,"action"):BOOK.update(r,datetime.now(timezone.utc).isoformat())
  except Exception as e:BOOK.reset();audit("ES","ERROR",f"{type(e).__name__}: {str(e)[:300]}; reconnect in 10s");time.sleep(10)
async def run():await asyncio.to_thread(live)

