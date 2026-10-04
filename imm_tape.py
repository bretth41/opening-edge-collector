"""Best-effort, cursor-based UW SPX options tape; all failures audited."""
import asyncio,hashlib,json,os
from datetime import datetime,timezone
import httpx
from imm_store import many,audit,js
from imm_uw import parse

async def run():
 headers={'Authorization':os.environ['UW_TOKEN'],'Accept':'application/json'}
 cursor=None;day=None
 async with httpx.AsyncClient(headers=headers,timeout=35) as c:
  while True:
   try:
    now=datetime.now(timezone.utc);today=now.date().isoformat()
    if day!=today:cursor=None;day=today
    params={'ticker_symbol':'SPX','limit':500,'intraday_only':'true'}
    if cursor:params['newer_than']=cursor
    # UW newest-first; paging backward only within the current poll window.
    seen=set();newest=cursor;pages=0;total=0
    while pages<30:
     r=await c.get('https://api.unusualwhales.com/api/option-trades',params=params)
     if r.status_code in (401,403):raise RuntimeError('UW tape entitlement HTTP '+str(r.status_code))
     r.raise_for_status();payload=r.json();rows=payload.get('data',[]) if isinstance(payload,dict) else []
     if not rows:break
     out=[]
     for x in rows:
      sym=x.get('option_chain_id') or x.get('option_symbol') or ''
      p=parse(sym)
      if not p or p[0]!=datetime.now(timezone.utc).date().isoformat():continue
      t=x.get('executed_at');key=str(x.get('id') or hashlib.sha256(js(x).encode()).hexdigest())
      if key in seen:continue
      seen.add(key)
      if t and (newest is None or str(t)>str(newest)):newest=str(t)
      out.append((key,datetime.now(timezone.utc).isoformat(),t,sym,p[0],p[2],p[1],x.get('price'),x.get('size'),x.get('nbbo_bid'),x.get('nbbo_ask'),x.get('upstream_condition_detail'),js(x)))
     many('INSERT OR IGNORE INTO option_tape VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',out);total+=len(out)
     pages+=1
     if len(rows)<500:break
     oldest=min((str(x['executed_at']) for x in rows if x.get('executed_at')),default=None)
     if not oldest or oldest==params.get('older_than'):break
     params['older_than']=oldest
    if pages>=30:audit('TAPE','WARN','30-page cap reached; possible trade gap')
    if newest:cursor=newest
    if total:audit('TAPE','INFO',f'new SPX trade rows received={total}')
   except Exception as e:audit('TAPE','ERROR',f'{type(e).__name__}: {str(e)[:250]}')
   await asyncio.sleep(10)
