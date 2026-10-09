"""Per-node freshness and trailing changes; no interpolation or dealer-position claim."""
import os
from collections import deque
from datetime import timedelta
from imm_uw import timestamp, CORE, ET
FRESH_SECONDS=int(os.getenv('IMM_UW_FRESH_SECONDS','180'))
BASELINE_TOLERANCE=60
WINDOWS=(60,180,300,600)
class GreekHistory:
 def __init__(self):self.day=None;self.nodes={};self.last_revision=None
 def observe(self,day,revision,rows):
  if self.day!=day:self.day=day;self.nodes={};self.last_revision=None
  if revision==self.last_revision:return
  self.last_revision=revision
  for key,item in rows.items():
   t=timestamp(item['source_time'])
   if not t:continue
   series=self.nodes.setdefault(key,deque())
   values=item['greeks']
   if series and t==series[-1][0]:
    # Provider rewrote values for the same source time; do not fabricate a new time.
    series[-1]=(t,values)
   elif not series or t>series[-1][0]:series.append((t,values))
   while series and (t-series[0][0]).total_seconds()>1200:series.popleft()
 def changes(self,key,item,now):
  t=timestamp(item['source_time']);result={}
  if not t or (now-t).total_seconds()>FRESH_SECONDS:return result
  for seconds in WINDOWS:
   target=t-timedelta(seconds=seconds)
   baseline=next((x for x in reversed(self.nodes.get(key,())) if x[0]<=target),None)
   valid=baseline is not None and (target-baseline[0]).total_seconds()<=BASELINE_TOLERANCE
   for basis in ('oi','vol','directional'):
    values={}
    for greek in ('gamma','delta','charm','vanna'):
     current=net(item['greeks'],greek,basis)
     prior=net(baseline[1],greek,basis) if valid else None
     values[greek]=current-prior if current is not None and prior is not None else None
    result[f'{seconds//60}min_{basis}']={'baseline_source_time':baseline[0].isoformat() if valid else None,'delta':values}
  return result
HISTORY=GreekHistory()
def net(values,greek,basis):
 fields=[f'{s}_{greek}_{b}' for s in ('call','put') for b in (('bid','ask') if basis=='directional' else (basis,))]
 xs=[values.get(f) for f in fields]
 return sum(xs) if all(x is not None for x in xs) else None

def structure(rows,expiry,lo,hi,now,history=HISTORY):
 if expiry is None or lo is None:return None,{'present':0,'fresh':0,'complete':0,'ages':[]}
 out=[];stats={'present':0,'fresh':0,'complete':0,'ages':[],'nonzero_delta_nodes':0}
 for strike in range(int(lo),int(hi)+1,5):
  key=(expiry,float(strike));item=rows.get(key);g=item['greeks'] if item else {}
  t=timestamp(item['source_time']) if item else None
  age=(now-t).total_seconds() if t else None
  same_day=t is not None and t.astimezone(ET).date()==now.astimezone(ET).date()
  fresh=age is not None and -2<=age<=FRESH_SECONDS and same_day
  complete=bool(item) and all(g.get(f) is not None for f in CORE)
  stats['nonzero_delta_nodes']+=int(any(g.get(f'{side}_delta_oi') not in (None,0) for side in ('call','put')))
  stats['present']+=int(bool(item));stats['fresh']+=int(fresh and complete);stats['complete']+=int(complete)
  if age is not None:stats['ages'].append(max(0,age))
  node={'strike':strike,'expiry':expiry,'source_time':t.isoformat() if t else None,
        'source_age_seconds':max(0,age) if age is not None else None,'fresh':fresh,'core_complete':complete,
        'received_at':item['received'] if item else None,'source_price':item['source_price'] if item else None,
        'expiry_origin':item['expiry_origin'] if item else None}
  # Existing names mean OI basis only. Explicit full fields preserve all bases.
  node.update({f'{s}_{greek}':g.get(f'{s}_{greek}_oi') for s in ('call','put') for greek in ('gamma','delta','charm','vanna')})
  node['exposures']=dict(g) if item else None
  node['net']={basis:{greek:net(g,greek,basis) for greek in ('gamma','delta','charm','vanna')} for basis in ('oi','vol','directional')}
  node['changes']=history.changes(key,item,now) if item and fresh and complete else {}
  out.append(node)
 return out,stats

def assess(state,spot,now,stats0,stats1):
 session=now.astimezone(ET).date().isoformat()
 q=state.quality;reasons=[]
 if state.session!=session:reasons.append('wrong_session')
 if spot is None:reasons.append('spx_missing_or_stale')
 if q.get('state')!='received':reasons.append(q.get('state','starting'))
 poll_time=timestamp(state.received)
 poll_age=(now-poll_time).total_seconds() if poll_time else None
 if poll_age is None or poll_age>FRESH_SECONDS:reasons.append('poll_missing_or_stale')
 for rank,stats in enumerate((stats0,stats1)):
  if stats['present']!=15:reasons.append(f'expiry_{rank}_missing_strikes')
  if stats['complete']!=15:reasons.append(f'expiry_{rank}_missing_greek_fields')
  if stats['fresh']!=15:reasons.append(f'expiry_{rank}_stale_or_missing_source_times')
  if stats.get('nonzero_delta_nodes',1)==0:reasons.append(f'expiry_{rank}_delta_all_zero_unverified')
 changed=q.get('changed_expiries',[])
 if not state.expiries or state.expiries[0] not in changed:reasons.append('0dte_live_change_not_yet_observed')
 # Next-expiry change evidence is reported separately; OI changes can be slower.
 return {'ready_for_structural_analysis':not reasons,'reasons':reasons,
  'threshold_seconds':FRESH_SECONDS,'poll_age_seconds':max(0,poll_age) if poll_age is not None else None,
  'source_times_advanced_session':q.get('source_times_advanced_session',0),
  '0dte':{k:v for k,v in stats0.items() if k!='ages'},'next_expiry':{k:v for k,v in stats1.items() if k!='ages'},
  'changed_expiries':changed,'oi_changed_nodes_session':q.get('oi_changed_nodes_session',0),
  'snapshot_id':q.get('snapshot_id'),'revision':state.revision,'endpoint':q.get('endpoint'),
  'iv_available':False,'net_hedging_directly_observed':False,
  'directional_exposure_is_estimate':True,'scope':'15 strikes per expiry around SPX; not whole-chain totals'}
