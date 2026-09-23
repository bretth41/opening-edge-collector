import sqlite3, os
from pathlib import Path
import pandas as pd

DB=Path(os.getenv('EDGE_DB','opening_edge.db'))
TICKER=os.getenv('EDGE_ANALYZE_TICKER','SPY')

with sqlite3.connect(DB) as con:
    df=pd.read_sql_query('SELECT * FROM strike_exposure WHERE ticker=? ORDER BY observed_at_et,strike',con,params=[TICKER])
if df.empty:
    raise SystemExit('No data yet.')

# Core research fields. We intentionally preserve raw inputs and derive signals later.
df['net_gamma_oi']=df['call_gamma_oi'].fillna(0)+df['put_gamma_oi'].fillna(0)
df['net_gamma_vol']=df['call_gamma_vol'].fillna(0)+df['put_gamma_vol'].fillna(0)
df['net_charm_vol']=df['call_charm_vol'].fillna(0)+df['put_charm_vol'].fillna(0)
df['net_vanna_vol']=df['call_vanna_vol'].fillna(0)+df['put_vanna_vol'].fillna(0)
df['dgamma_vol_1m']=df.groupby(['ticker','strike'])['net_gamma_vol'].diff()
df['dgamma_oi_1m']=df.groupby(['ticker','strike'])['net_gamma_oi'].diff()

# Keep the output deliberately descriptive; don't bake entry/exit weights in before evidence exists.
latest=df['observed_at_et'].max()
x=df[df.observed_at_et==latest].copy()
x['abs_gamma_vol']=x.net_gamma_vol.abs()
print(f'Latest snapshot: {latest} | {TICKER} | spot≈{x.underlying_price.median():.2f}')
print('\nLargest current volume-gamma nodes:')
print(x.nlargest(12,'abs_gamma_vol')[['strike','net_gamma_vol','dgamma_vol_1m']].to_string(index=False))
print('\nFastest 1-minute gamma changes:')
print(x.assign(abs_d=x.dgamma_vol_1m.abs()).nlargest(12,'abs_d')[['strike','net_gamma_vol','dgamma_vol_1m']].to_string(index=False))
