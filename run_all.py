import asyncio,os,uvicorn
from imm_store import init,audit
from imm_uw import UW_STATE
from imm_massive import run as run_massive
from imm_es import run as run_es
from imm_master import run as run_master
from imm_app import app
async def web():await uvicorn.Server(uvicorn.Config(app,host="0.0.0.0",port=int(os.getenv("PORT","8080")),access_log=False)).serve()
async def main():
 init();missing=[x for x in ("UW_TOKEN","MASSIVE_API_KEY") if not os.getenv(x)]
 if missing:audit("START","ERROR","Missing: "+",".join(missing));raise SystemExit("Missing credentials")
 audit("START","INFO","IMM v4.2: master-only 5-second rows, two-expiry 35pt strike window")
 tasks=[asyncio.create_task(f()) for f in (web,UW_STATE.poll,run_massive,run_master)]
 if os.getenv("DATABENTO_API_KEY"):tasks.append(asyncio.create_task(run_es()))
 else:audit("ES","WARN","No Databento key")
 done,pending=await asyncio.wait(tasks,return_when=asyncio.FIRST_EXCEPTION)
 for t in pending:t.cancel()
 for t in done:
  if t.exception():raise t.exception()
if __name__=="__main__":asyncio.run(main())
