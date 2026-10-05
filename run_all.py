import asyncio,os
import uvicorn
from imm_store import init,audit
from imm_uw import UW_STATE
from imm_es import run as run_es
from imm_master import run as run_master
from imm_app import app
from imm_tape import run as run_tape
async def web():
 s=uvicorn.Server(uvicorn.Config(app,host='0.0.0.0',port=int(os.getenv('PORT','8080')),access_log=False));await s.serve()
async def main():
 init()
 if not os.getenv('UW_TOKEN'):
  audit('START','ERROR','Missing UW_TOKEN');raise SystemExit('Missing data credentials')
 audit('START','INFO','IMM v3.2: UW REST + ES MBP-1 collection; no signals')
 tasks=[asyncio.create_task(f()) for f in (web,UW_STATE.poll,run_master,run_tape)]
 if os.getenv('DATABENTO_API_KEY'):tasks.append(asyncio.create_task(run_es()))
 else:audit('ES','WARN','No Databento key; ES fields will remain missing')
 done,pending=await asyncio.wait(tasks,return_when=asyncio.FIRST_EXCEPTION)
 for t in pending:t.cancel()
 for t in done:
  if t.exception():raise t.exception()
if __name__=='__main__':asyncio.run(main())
