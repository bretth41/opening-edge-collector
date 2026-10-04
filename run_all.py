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
 if not os.getenv('UW_TOKEN') or not os.getenv('DATABENTO_API_KEY'):
  audit('START','ERROR','Missing UW_TOKEN or DATABENTO_API_KEY');raise SystemExit('Missing data credentials')
 audit('START','INFO','IMM v3: collection only, no signals; new isolated database')
 tasks=[asyncio.create_task(f()) for f in (web,UW_STATE.poll,UW_STATE.stream_price,run_es,run_master,run_tape)]
 done,pending=await asyncio.wait(tasks,return_when=asyncio.FIRST_EXCEPTION)
 for t in pending:t.cancel()
 for t in done:
  if t.exception():raise t.exception()
if __name__=='__main__':asyncio.run(main())
