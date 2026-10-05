import asyncio,shutil
from datetime import datetime,timezone,timedelta,time as dtime
from zoneinfo import ZoneInfo
from imm_store import write,audit,js,DB
from imm_uw import UW_STATE
from imm_es import BOOK
from imm_massive import SPX
ET=ZoneInfo("America/New_York")
COLUMNS=["session_date_et","interval_end_utc","interval_start_utc","spx","spx_source_time","spx_received_at","spx_age_seconds","spx_carried","uw_age_seconds","uw_rows","uw_0dte_expiry","uw_next_expiry","es_contract","es_instrument_id","es_bid","es_ask","es_spread","es_bid_depth_1","es_ask_depth_1","es_bid_depth_5","es_ask_depth_5","es_add_bid","es_add_ask","es_cancel_bid","es_cancel_ask","es_modify_bid","es_modify_ask","es_trade_buy","es_trade_sell","es_events","es_last_event_at","es_age_seconds","es_book_valid","es_feed_gap","nearest_above_json","nearest_below_json","source_status_json","es_bid_order_count","es_ask_order_count","es_trade_buy_count","es_trade_sell_count","es_mid_start","es_mid_end","es_mid_high","es_mid_low","es_mid_change_ticks","es_range_ticks","es_trade_imbalance","es_depth_imbalance_end","es_displacement_ticks_per_100_contracts","es_bid_replenish_proxy","es_ask_replenish_proxy","es_bid_deplete_proxy","es_ask_deplete_proxy","es_bid_withdraw_proxy","es_ask_withdraw_proxy","es_bid_absorption_proxy","es_ask_absorption_proxy"]
def age(t,n):
 try:return max(0,(n-datetime.fromisoformat(t.replace("Z","+00:00"))).total_seconds()) if t else None
 except Exception:return None
def levels(rows,spot,expiry):
 if spot is None:return None,None
 data=[]
 for (ex,s),v in rows.items():
  if ex!=expiry:continue
  g=v["greeks"];data.append({"strike":s,"distance":round(s-spot,3),"call_gamma_oi":g.get("call_gamma_oi"),"put_gamma_oi":g.get("put_gamma_oi"),"call_charm_oi":g.get("call_charm_oi"),"put_charm_oi":g.get("put_charm_oi"),"call_vanna_oi":g.get("call_vanna_oi"),"put_vanna_oi":g.get("put_vanna_oi"),"source_time":v["source"]})
 return sorted((x for x in data if x["distance"]>0),key=lambda x:x["distance"])[:3],sorted((x for x in data if x["distance"]<0),key=lambda x:-x["distance"])[:3]
def make(n):
 d=n.astimezone(ET).date().isoformat();exp=UW_STATE.expiries;a=age(SPX.source_time,n);spot=SPX.price if a is not None and a<=5 else None;above,below=levels(UW_STATE.rows,spot,exp[0] if exp else None);es=BOOK.snapshot(n);r=dict.fromkeys(COLUMNS)
 r.update(session_date_et=d,interval_start_utc=(n-timedelta(seconds=5)).isoformat(),interval_end_utc=n.isoformat(),spx=spot,spx_source_time=SPX.source_time,spx_received_at=SPX.received_at,spx_age_seconds=a,spx_carried=int(a>1.5) if spot is not None else None,uw_age_seconds=age(UW_STATE.updated,n),uw_rows=len(UW_STATE.rows),uw_0dte_expiry=exp[0] if exp else None,uw_next_expiry=exp[1] if len(exp)>1 else None,nearest_above_json=js(above),nearest_below_json=js(below),source_status_json=js({"spx_source":"Massive I:SPX A1S","massive":SPX.quality(),"uw":UW_STATE.quality,"es_schema":"mbp-1","es_valid":bool(es.get("es_book_valid")),"es_depth_levels":1,"microstructure_metrics_are_proxies":True}));r.update(es);return r
async def run():
 last=None
 while True:
  n=datetime.now(timezone.utc);await asyncio.sleep(max(.01,5-n.timestamp()%5+.08));n=datetime.now(timezone.utc);end=datetime.fromtimestamp(int(n.timestamp()//5)*5,timezone.utc);local=end.astimezone(ET)
  if local.weekday()>4 or not dtime(9,30)<=local.time().replace(tzinfo=None)<dtime(12):continue
  if end==last:continue
  last=end
  if local.time().replace(tzinfo=None)>=dtime(11,30):
   a=age(SPX.source_time,end)
   if SPX.price is not None and a is not None and a<=5:write("INSERT OR IGNORE INTO price_continuation VALUES(?,?,?,?,?)",(end.isoformat(),local.date().isoformat(),SPX.price,SPX.source_time,a))
   continue
  r=make(end);cols=",".join(COLUMNS);write(f'INSERT OR REPLACE INTO master ({cols}) VALUES ({",".join("?" for _ in COLUMNS)})',tuple(r.get(k) for k in COLUMNS))
  if end.minute%15==0 and end.second==0:audit("MASTER","INFO",f'v4 rows live; disk_free_mb={shutil.disk_usage(DB.parent).free//1048576}; spx_age={r["spx_age_seconds"]}; uw_rows={r["uw_rows"]}; es_valid={r["es_book_valid"]}')
