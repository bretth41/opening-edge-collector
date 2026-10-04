"""UW Basic REST SPX 0DTE tape. Cursor pagination; never claim complete when capped."""
import asyncio,hashlib,os
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import httpx
from imm_store import many,write,audit,js
from imm_uw import parse
ET=ZoneInfo('America/New_York')

def _date():return datetime.now(ET).date().isoformat()
def _rows(payload):return payload.get('data',[]) if isinstance(payload,dict) else payload if isinstance(payload,list) else []
def _cursor(rows):
 times=[x.get('executed_at') for x in rows if x.get('executed_at')]
 return min(times) if times else None

async def collect(c,day,watermark,max_pages=100):
 """Walk newest-first pages until prior completed watermark is reached.
 Return (newest_seen, complete, count). No watermark advance on gaps.
 """
 params={'ticker_symbol':'SPX','limit':500,'intraday_only':'true','expiry_dates[]':day}
 if watermark:params['newer_than']=watermark
 seen=set();newest=watermark;previous_oldest=None;total=0;complete=False
 for page in range(max_pages):
  r=await c.get('https://api.unusualwhales.com/api/option-trades',params=params)
  if r.status_code in (401,403):raise RuntimeError('UW REST tape entitlement HTTP '+str(r.status_code))
  r.raise_for_status();rows=_rows(r.json())
  if not rows:complete=True;break
  batch=[];reached=False
  for x in rows:
   t=x.get('executed_at');sym=x.get('option_chain_id') or x.get('option_symbol') or '';contract=parse(sym)
   if watermark and t and str(t)<=str(watermark):reached=True
   if not contract or contract[0]!=day or not t or (watermark and str(t)<str(watermark)):continue
   key=str(x.get('id') or hashlib.sha256(js(x).encode()).hexdigest())
   if key in seen:continue
   seen.add(key)
   if newest is None or str(t)>str(newest):newest=str(t)
   batch.append((key,datetime.now(timezone.utc).isoformat(),t,sym,contract[0],contract[2],contract[1],x.get('price'),x.get('size'),x.get('nbbo_bid'),x.get('nbbo_ask'),x.get('upstream_condition_detail'),js(x)))
  many('INSERT OR IGNORE INTO option_tape VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',batch);total+=len(batch)
  oldest=_cursor(rows)
  if reached or len(rows)<500:complete=True;break
  if not oldest or oldest==previous_oldest:break
  previous_oldest=oldest;params['older_than']=oldest
 return newest,complete,total

async def run():
 headers={'Authorization':os.environ['UW_TOKEN'],'Accept':'application/json'}
 day=None;watermark=None
 async with httpx.AsyncClient(headers=headers,timeout=35) as c:
  while True:
   try:
    now=datetime.now(ET);today=now.date().isoformat()
    if day!=today:day=today;watermark=None
    # Do not churn through entire previous-day tape on weekends or outside study window.
    if now.weekday()<5 and (now.hour==9 and now.minute>=25 or 10<=now.hour<12):
     newest,complete,count=await collect(c,day,watermark,int(os.getenv('IMM_TAPE_MAX_PAGES','100')))
     if complete:
      watermark=newest
      write('INSERT OR REPLACE INTO tape_checkpoint VALUES(?,?,?,?,?)',(day,watermark,datetime.now(timezone.utc).isoformat(),0,f'rows={count}'))
      if count:audit('TAPE','INFO',f'rows={count}; complete_window=1')
     else:
      write('INSERT OR REPLACE INTO tape_checkpoint VALUES(?,?,?,?,?)',(day,watermark,None,1,f'pagination cap or stalled; rows={count}'))
      audit('TAPE','WARN',f'incomplete pagination rows={count}; watermark not advanced')
   except Exception as e:audit('TAPE','ERROR',f'{type(e).__name__}: {str(e)[:250]}')
   await asyncio.sleep(int(os.getenv('IMM_TAPE_POLL_SECONDS','20')))
