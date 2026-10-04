import asyncio,os,shutil
from datetime import datetime,timezone,timedelta,time as dtime
from zoneinfo import ZoneInfo
from imm_store import write,audit,js,DB
from imm_uw import UW_STATE
from imm_es import BOOK
ET=ZoneInfo('America/New_York')
COLUMNS=['session_date_et','interval_end_utc','interval_start_utc','spx','spx_source_time','spx_received_at','spx_age_seconds','spx_carried','uw_age_seconds','uw_rows','uw_0dte_expiry','uw_next_expiry','es_contract','es_instrument_id','es_bid','es_ask','es_spread','es_bid_depth_1','es_ask_depth_1','es_bid_depth_5','es_ask_depth_5','es_add_bid','es_add_ask','es_cancel_bid','es_cancel_ask','es_modify_bid','es_modify_ask','es_trade_buy','es_trade_sell','es_events','es_last_event_at','es_age_seconds','es_book_valid','es_feed_gap','nearest_above_json','nearest_below_json','source_status_json']
def age(t,now):
 try:return max(0,(now-datetime.fromisoformat(t.replace('Z','+00:00'))).total_seconds()) if t else None
 except (ValueError,TypeError):return None
def levels(rows,spot,expiry):
 if spot is None:return None,None
 data=[]
 for (ex,strike),v in rows.items():
  if ex!=expiry:continue
  g=v['greeks'];strength=abs(float(g.get('call_gamma_oi') or 0))+abs(float(g.get('put_gamma_oi') or 0))
  data.append({'strike':strike,'distance':round(strike-spot,3),'gamma_oi_abs':strength,'call_gamma_oi':g.get('call_gamma_oi'),'put_gamma_oi':g.get('put_gamma_oi'),'call_charm_oi':g.get('call_charm_oi'),'put_charm_oi':g.get('put_charm_oi'),'call_vanna_oi':g.get('call_vanna_oi'),'put_vanna_oi':g.get('put_vanna_oi'),'source_time':v['source']})
 # Neutral provisional proximity ranking: all available strikes, nearest three each side.
 return sorted((x for x in data if x['distance']>0),key=lambda x:x['distance'])[:3],sorted((x for x in data if x['distance']<0),key=lambda x:-x['distance'])[:3]
def make_row(now):
 date=now.astimezone(ET).date().isoformat();start=now-timedelta(seconds=5);exp=UW_STATE.expiries
 spot=UW_STATE.spot if age(UW_STATE.spot_source,now) is not None and age(UW_STATE.spot_source,now)<=60 else None;above,below=levels(UW_STATE.rows,spot,exp[0] if exp else None)
 es=BOOK.snapshot(now)
 row=dict.fromkeys(COLUMNS)
 row.update(session_date_et=date,interval_start_utc=start.isoformat(),interval_end_utc=now.isoformat(),spx=spot,spx_source_time=UW_STATE.spot_source,spx_received_at=UW_STATE.spot_recv,spx_age_seconds=age(UW_STATE.spot_source,now),spx_carried=int(age(UW_STATE.spot_source,now)>5) if spot is not None and age(UW_STATE.spot_source,now) is not None else None,uw_age_seconds=age(UW_STATE.updated,now),uw_rows=len(UW_STATE.rows),uw_0dte_expiry=exp[0] if exp else None,uw_next_expiry=exp[1] if len(exp)>1 else None,nearest_above_json=js(above),nearest_below_json=js(below),source_status_json=js({'spx_price_verified_five_second':False,'uw':UW_STATE.quality,'es_valid':bool(es.get('es_book_valid'))}))
 row.update(es)
 return row
async def run():
 last=None
 while True:
  now=datetime.now(timezone.utc);secs=now.timestamp();await asyncio.sleep(max(.01,5-secs%5+.08))
  now=datetime.now(timezone.utc);end=datetime.fromtimestamp(int(now.timestamp()//5)*5,timezone.utc);local=end.astimezone(ET)
  if local.weekday()>4 or not dtime(9,30)<=local.time().replace(tzinfo=None)<dtime(12,0):continue
  if end==last:continue
  last=end
  # 09:30–11:30: full master; 11:30–12:00: price-only continuation.
  if local.time().replace(tzinfo=None)>=dtime(11,30):
   # Price-only continuation: never invent a new price or timestamp.
   if UW_STATE.spot is not None and UW_STATE.spot_source and age(UW_STATE.spot_source,end) is not None:
    write('INSERT OR IGNORE INTO price_continuation VALUES(?,?,?,?,?)',(end.isoformat(),local.date().isoformat(),UW_STATE.spot,UW_STATE.spot_source,age(UW_STATE.spot_source,end)))
   continue
  row=make_row(end);cols=','.join(COLUMNS)
  write(f'INSERT OR REPLACE INTO master ({cols}) VALUES ({",".join("?" for _ in COLUMNS)})',tuple(row.get(k) for k in COLUMNS))
  if end.minute%15==0 and end.second==0:
   usage=shutil.disk_usage(DB.parent);audit('MASTER','INFO',f'rows live; disk_free_mb={usage.free//1048576}; spx_age={row["spx_age_seconds"]}; es_valid={row["es_book_valid"]}')
