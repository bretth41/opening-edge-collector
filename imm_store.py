import os,sqlite3,json,threading
from pathlib import Path
from datetime import datetime,timezone
DB=Path(os.getenv("IMM_DB","/data/imm_v4.db" if Path("/data").exists() else "imm_v4.db"));DB.parent.mkdir(parents=True,exist_ok=True);LOCK=threading.RLock()
SCHEMA="""PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS master(session_date_et TEXT NOT NULL,interval_end_utc TEXT NOT NULL,interval_start_utc TEXT NOT NULL,spx REAL,spx_source_time TEXT,spx_received_at TEXT,spx_age_seconds REAL,spx_carried INTEGER,uw_age_seconds REAL,uw_rows INTEGER,uw_0dte_expiry TEXT,uw_next_expiry TEXT,es_contract TEXT,es_instrument_id INTEGER,es_bid REAL,es_ask REAL,es_spread REAL,es_bid_depth_1 REAL,es_ask_depth_1 REAL,es_bid_depth_5 REAL,es_ask_depth_5 REAL,es_add_bid INTEGER,es_add_ask INTEGER,es_cancel_bid INTEGER,es_cancel_ask INTEGER,es_modify_bid INTEGER,es_modify_ask INTEGER,es_trade_buy INTEGER,es_trade_sell INTEGER,es_events INTEGER,es_last_event_at TEXT,es_age_seconds REAL,es_book_valid INTEGER,es_feed_gap INTEGER,nearest_above_json TEXT,nearest_below_json TEXT,source_status_json TEXT,PRIMARY KEY(session_date_et,interval_end_utc));
CREATE TABLE IF NOT EXISTS strike_history(received_at TEXT NOT NULL,source_time TEXT,session_date_et TEXT,expiry TEXT,expiry_rank INTEGER,strike REAL,underlying_price REAL,greeks_json TEXT,contracts_json TEXT,exposures_json TEXT,PRIMARY KEY(received_at,expiry,strike));
CREATE INDEX IF NOT EXISTS idx_strike_day ON strike_history(session_date_et,expiry,strike);
CREATE TABLE IF NOT EXISTS option_tape(trade_key TEXT PRIMARY KEY,received_at TEXT,executed_at TEXT,option_symbol TEXT,expiry TEXT,strike REAL,side TEXT,price REAL,size REAL,bid REAL,ask REAL,condition TEXT,raw_json TEXT);
CREATE TABLE IF NOT EXISTS es_event(event_key TEXT PRIMARY KEY,received_at TEXT,event_at TEXT,instrument_id INTEGER,action TEXT,side TEXT,price REAL,size INTEGER,order_id TEXT,flags INTEGER,raw_json TEXT);
CREATE TABLE IF NOT EXISTS price_history(received_at TEXT PRIMARY KEY,source_time TEXT,session_date_et TEXT,spx REAL,source TEXT);
CREATE TABLE IF NOT EXISTS price_continuation(interval_end_utc TEXT PRIMARY KEY,session_date_et TEXT,spx REAL,spx_source_time TEXT,spx_age_seconds REAL);
CREATE TABLE IF NOT EXISTS tape_checkpoint(session_date_et TEXT PRIMARY KEY,newest_seen TEXT,last_complete_at TEXT,incomplete INTEGER,detail TEXT);
CREATE TABLE IF NOT EXISTS audit(at TEXT,source TEXT,level TEXT,detail TEXT);"""
EXTRA={"es_bid_order_count":"INTEGER","es_ask_order_count":"INTEGER","es_trade_buy_count":"INTEGER","es_trade_sell_count":"INTEGER","es_mid_start":"REAL","es_mid_end":"REAL","es_mid_high":"REAL","es_mid_low":"REAL","es_mid_change_ticks":"REAL","es_range_ticks":"REAL","es_trade_imbalance":"REAL","es_depth_imbalance_end":"REAL","es_displacement_ticks_per_100_contracts":"REAL","es_bid_replenish_proxy":"REAL","es_ask_replenish_proxy":"REAL","es_bid_deplete_proxy":"REAL","es_ask_deplete_proxy":"REAL","es_bid_withdraw_proxy":"REAL","es_ask_withdraw_proxy":"REAL","es_bid_absorption_proxy":"REAL","es_ask_absorption_proxy":"REAL"}
def connect():
 c=sqlite3.connect(DB,timeout=30);c.row_factory=sqlite3.Row;c.execute("PRAGMA busy_timeout=30000");return c
def init():
 with LOCK,connect() as c:
  c.executescript(SCHEMA);have={r[1] for r in c.execute("PRAGMA table_info(master)")}
  for n,t in EXTRA.items():
   if n not in have:c.execute(f"ALTER TABLE master ADD COLUMN {n} {t}")
def write(sql,args):
 with LOCK,connect() as c:c.execute(sql,args)
def many(sql,args):
 if args:
  with LOCK,connect() as c:c.executemany(sql,args)
def audit(source,level,detail):print(f"{source} {level} {detail}",flush=True);write("INSERT INTO audit VALUES(?,?,?,?)",(datetime.now(timezone.utc).isoformat(),source,level,str(detail)[:1200]))
def js(x):return json.dumps(x,default=str,separators=(",",":"))
