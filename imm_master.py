import asyncio,shutil,math,json
from collections import deque
from datetime import datetime,timezone,timedelta,time as dtime
from zoneinfo import ZoneInfo
from imm_store import write,audit,js,DB
from imm_uw import UW_STATE
from imm_features import HISTORY,structure as node_structure,assess
from imm_es import BOOK
from imm_massive import SPX
ET=ZoneInfo("America/New_York")
BASE=["session_date_et","interval_end_utc","interval_start_utc","spx","spx_source_time","spx_received_at","spx_age_seconds","spx_carried","uw_age_seconds","uw_rows","uw_0dte_expiry","uw_next_expiry","es_contract","es_instrument_id","es_bid","es_ask","es_spread","es_bid_depth_1","es_ask_depth_1","es_bid_depth_5","es_ask_depth_5","es_add_bid","es_add_ask","es_cancel_bid","es_cancel_ask","es_modify_bid","es_modify_ask","es_trade_buy","es_trade_sell","es_events","es_last_event_at","es_age_seconds","es_book_valid","es_feed_gap","nearest_above_json","nearest_below_json","source_status_json","es_bid_order_count","es_ask_order_count","es_trade_buy_count","es_trade_sell_count","es_mid_start","es_mid_end","es_mid_high","es_mid_low","es_mid_change_ticks","es_range_ticks","es_trade_imbalance","es_depth_imbalance_end","es_displacement_ticks_per_100_contracts","es_bid_replenish_proxy","es_ask_replenish_proxy","es_bid_deplete_proxy","es_ask_deplete_proxy","es_bid_withdraw_proxy","es_ask_withdraw_proxy","es_bid_absorption_proxy","es_ask_absorption_proxy"]
ADDED=["greeks_0dte_json","greeks_1dte_json","strike_center","strike_low","strike_high","strike_expected_per_expiry","strike_present_0dte","strike_present_1dte","spx_change_30s","spx_change_60s","spx_change_180s","spx_change_300s","spx_window_quality_json","uw_snapshot_at"]
ADDED += ["collector_version","uw_received_at","uw_snapshot_id","uw_revision","uw_window_quality_json","uw_analysis_ready","uw_oldest_window_age_seconds"]
COLUMNS=BASE+ADDED
LAST_DIAGNOSTICS={}
LAST_WARNING=None
PRICE_HISTORY=deque(maxlen=75)
def age(t,n):
 try:return max(0,(n-datetime.fromisoformat(t.replace("Z","+00:00"))).total_seconds()) if t else None
 except Exception:return None
def strike_bounds(spot):
 # Anchor at the 5-point strike at/below spot. +/- 35 gives 15 strikes per expiry.
 center=5*math.floor(spot/5)
 return center,center-35,center+35
def rolling_price(n,spot):
 # Strict exact-interval endpoints, no interpolated or stale carry-forward values.
 day=n.astimezone(ET).date()
 if PRICE_HISTORY and PRICE_HISTORY[-1][0].astimezone(ET).date()!=day:PRICE_HISTORY.clear()
 PRICE_HISTORY.append((n,spot))
 seen={t:p for t,p in PRICE_HISTORY}
 out={};q={}
 for seconds in (30,60,180,300):
  prior=seen.get(n-timedelta(seconds=seconds))
  valid=spot is not None and prior is not None
  out[f"spx_change_{seconds}s"]=round(spot-prior,4) if valid else None
  q[f"{seconds}s_complete"]=valid
 return out,q
def make(n):
 d=n.astimezone(ET).date().isoformat()
 a=age(SPX.source_time,n);spot=SPX.price if a is not None and a<=5 else None
 # UW updates are published as one atomic replacement between awaits.
 rows=UW_STATE.rows;exp=UW_STATE.expiries;updated=UW_STATE.updated;quality=UW_STATE.quality
 center,lo,hi=strike_bounds(spot) if spot is not None else (None,None,None)
 if UW_STATE.session!=d:rows={};exp=[];updated=None
 HISTORY.observe(d,UW_STATE.revision,rows)
 zero,q0=node_structure(rows,exp[0] if exp else None,lo,hi,n)
 next_,q1=node_structure(rows,exp[1] if len(exp)>1 else None,lo,hi,n)
 c0=q0['present'];c1=q1['present']
 greek_quality=assess(UW_STATE,spot,n,q0,q1)
 global LAST_DIAGNOSTICS
 LAST_DIAGNOSTICS={'interval_end_utc':n.isoformat(),'greeks':greek_quality,'uw':quality}
 ages=q0['ages']+q1['ages']
 es=BOOK.snapshot(n);r=dict.fromkeys(COLUMNS)
 momentum,mq=rolling_price(n,spot)
 r.update(session_date_et=d,interval_start_utc=(n-timedelta(seconds=5)).isoformat(),interval_end_utc=n.isoformat(),spx=spot,spx_source_time=SPX.source_time,spx_received_at=SPX.received_at,spx_age_seconds=a,spx_carried=int(a>1.5) if spot is not None else None,uw_age_seconds=age(updated,n),uw_snapshot_at=updated,uw_rows=len(rows),uw_0dte_expiry=exp[0] if exp else None,uw_next_expiry=exp[1] if len(exp)>1 else None,greeks_0dte_json=js(zero) if zero is not None else None,greeks_1dte_json=js(next_) if next_ is not None else None,strike_center=center,strike_low=lo,strike_high=hi,strike_expected_per_expiry=15 if spot is not None else None,strike_present_0dte=c0,strike_present_1dte=c1,nearest_above_json=None,nearest_below_json=None,spx_window_quality_json=js(mq),source_status_json=js({"spx_source":"Massive I:SPX A1S","massive":SPX.quality(),"uw":quality,"es_schema":"mbp-1","es_valid":bool(es.get("es_book_valid")),"es_depth_levels":1,"es_interval_seconds":5,"es_microstructure_metrics_are_proxies":True,"uw_repeated_until_refresh":True,"greeks_rolling_changes":"derive from consecutive distinct uw_snapshot_at values; 1/3/5/10-minute windows; no interpolation"}))
 r.update(collector_version='imm-v4.3',uw_received_at=UW_STATE.received,uw_snapshot_id=quality.get('snapshot_id'),uw_revision=UW_STATE.revision,uw_window_quality_json=js(greek_quality),uw_analysis_ready=int(greek_quality['ready_for_structural_analysis']),uw_oldest_window_age_seconds=max(ages) if ages else None)
 # Ordered nearby nodes, NOT forecast targets or whole-chain totals.
 r['nearest_above_json']=js([x for x in (zero or []) if spot is not None and x['strike']>spot][:3])
 r['nearest_below_json']=js(list(reversed([x for x in (zero or []) if spot is not None and x['strike']<=spot]))[:3])
 source=json.loads(r['source_status_json']);source['uw_window']=greek_quality
 source['greeks_rolling_changes']='per fixed strike/expiry/basis, source-time baselines, 60s maximum baseline lag; no interpolation'
 source['es_absorption_available']=False;source['es_withdrawal_available']=False
 r['source_status_json']=js(source)
 r.update(momentum);r.update(es);return r
async def run():
 last=None
 while True:
  n=datetime.now(timezone.utc);await asyncio.sleep(max(.01,5-n.timestamp()%5+.08))
  n=datetime.now(timezone.utc);end=datetime.fromtimestamp(int(n.timestamp()//5)*5,timezone.utc)
  local=end.astimezone(ET);lt=local.time().replace(tzinfo=None)
  if local.weekday()>4 or not dtime(9,30)<=(lt)<dtime(16):continue
  if end==last:continue
  last=end;r=make(end)
  global LAST_WARNING
  warning=tuple(json.loads(r['uw_window_quality_json'])['reasons'])
  if warning!=LAST_WARNING:
   audit('QUALITY','WARN' if warning else 'INFO','Greek readiness: '+(', '.join(warning) if warning else 'READY'))
   LAST_WARNING=warning
  cols=",".join(COLUMNS);placeholders=",".join("?" for _ in COLUMNS)
  write(f"INSERT OR REPLACE INTO master ({cols}) VALUES ({placeholders})",tuple(r.get(k) for k in COLUMNS))
  if end.minute%15==0 and end.second==0:
   audit("MASTER","INFO",f'v4.3 rows live; disk_free_mb={shutil.disk_usage(DB.parent).free//1048576}; spx_age={r["spx_age_seconds"]}; uw_age={r["uw_age_seconds"]}; strikes={r["strike_present_0dte"]}/{r["strike_present_1dte"]}; es_valid={r["es_book_valid"]}')

