import os, sqlite3, json, threading
from pathlib import Path
from datetime import datetime, timezone
DB=Path(os.getenv("IMM_DB", "/data/imm_v4.db" if Path("/data").exists() else "imm_v4.db"))
DB.parent.mkdir(parents=True,exist_ok=True)
LOCK=threading.RLock()
SCHEMA="""PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS master(session_date_et TEXT NOT NULL,interval_end_utc TEXT NOT NULL,interval_start_utc TEXT NOT NULL,spx REAL,spx_source_time TEXT,spx_received_at TEXT,spx_age_seconds REAL,spx_carried INTEGER,uw_age_seconds REAL,uw_rows INTEGER,uw_0dte_expiry TEXT,uw_next_expiry TEXT,es_contract TEXT,es_instrument_id INTEGER,es_bid REAL,es_ask REAL,es_spread REAL,es_bid_depth_1 REAL,es_ask_depth_1 REAL,es_bid_depth_5 REAL,es_ask_depth_5 REAL,es_add_bid INTEGER,es_add_ask INTEGER,es_cancel_bid INTEGER,es_cancel_ask INTEGER,es_modify_bid INTEGER,es_trade_buy INTEGER,es_trade_sell INTEGER,es_events INTEGER,es_last_event_at TEXT,es_age_seconds REAL,es_book_valid INTEGER,es_feed_gap INTEGER,nearest_above_json TEXT,nearest_below_json TEXT,source_status_json TEXT,PRIMARY KEY(session_date_et,interval_end_utc));
CREATE TABLE IF NOT EXISTS audit(at TEXT,source TEXT,level TEXT,detail TEXT);"""
EXTRA={"es_bid_order_count":"INTEGER","es_ask_order_count":"INTEGER","es_trade_buy_count":"INTEGER","es_trade_sell_count":"INTEGER","es_mid_start":"REAL","es_mid_end":"REAL","es_mid_high":"REAL","es_mid_low":"REAL","es_mid_change_ticks":"REAL","es_range_ticks":"REAL","es_trade_imbalance":"REAL","es_depth_imbalance_end":"REAL","es_displacement_ticks_per_100_contracts":"REAL","es_bid_replenish_proxy":"REAL","es_ask_replenish_proxy":"REAL","es_bid_deplete_proxy":"REAL","es_ask_deplete_proxy":"REAL","es_bid_withdraw_proxy":"REAL","es_ask_withdraw_proxy":"REAL","es_bid_absorption_proxy":"REAL","es_ask_absorption_proxy":"REAL","greeks_0dte_json":"TEXT","greeks_1dte_json":"TEXT","strike_center":"REAL","strike_low":"REAL","strike_high":"REAL","strike_expected_per_expiry":"INTEGER","strike_present_0dte":"INTEGER","strike_present_1dte":"INTEGER","spx_change_30s":"REAL","spx_change_60s":"REAL","spx_change_180s":"REAL","spx_change_300s":"REAL","spx_window_quality_json":"TEXT","uw_snapshot_at":"TEXT"}
def connect():
 c=sqlite3.connect(DB,timeout=30);c.row_factory=sqlite3.Row;c.execute("PRAGMA busy_timeout=30000");return c
def init():
 with LOCK,connect() as c:
  c.executescript(SCHEMA)
  have={r[1] for r in c.execute("PRAGMA table_info(master)")}
  for n,t in EXTRA.items():
   if n not in have:c.execute(f"ALTER TABLE master ADD COLUMN {n} {t}")
def write(sql,args):
 # Strict persistence allowlist: legacy modules cannot refill historical tables.
 q=" ".join(sql.lower().split())
 if not (q.startswith("insert into master ") or q.startswith("insert or replace into master ") or q.startswith("insert into audit ")):return
 with LOCK,connect() as c:c.execute(sql,args)
def audit(source,level,detail):
 print(f"{source} {level} {detail}",flush=True)
 # Keep only errors and startup info, not every UW poll in SQLite.
 if level in ("ERROR","WARN") or source=="START":
  write("INSERT INTO audit VALUES(?,?,?,?)",(datetime.now(timezone.utc).isoformat(),source,level,str(detail)[:1200]))
def js(x):return json.dumps(x,default=str,separators=(",",":"))
