import sqlite3
import pandas as pd

def q(db,sql,p=()):
    with sqlite3.connect(db) as c:return pd.read_sql_query(sql,c,params=p)

def exposures(db,day,window,ticker):
    x=q(db,"SELECT * FROM strike_exposure WHERE substr(observed_at_et,1,10)=? AND session_window=? AND ticker=? ORDER BY observed_at_et,strike",(day,window,ticker))
    if not x.empty:x["g"]=x["call_gamma_vol"].fillna(0)+x["put_gamma_vol"].fillna(0)
    return x

def warnings(db,day,window="MIDDAY",ticker="SPY",radius=8):
    x=exposures(db,day,window,ticker)
    return {"date":day,"window":window,"ticker":ticker,"causal":True,"warnings":[],
            "note":"Phase 4 does not pre-impose new IMM/ISR predictors. Candidate variables are collected prospectively and evaluated afterward."}

def outcomes(db,day,window="MIDDAY",ticker="SPY",radius=8):
    try:
        o=q(db,"SELECT * FROM option_snapshot WHERE substr(observed_at_et,1,10)=? AND session_window=? ORDER BY observed_at_et,option_symbol",(day,window))
        rows=len(o); contracts=int(o.option_symbol.nunique()) if not o.empty else 0
    except Exception:
        rows=contracts=0
    return {"date":day,"window":window,"ticker":ticker,
            "objective":"IMM / ISR — fewest and least complex variables; high probability, accuracy and significant profitability",
            "option_snapshot_rows":rows,"unique_contracts":contracts,
            "note":"Stored option snapshots support causal entry price/IV/Greek analysis and subsequent 2/3/5/10-minute profitability scoring."}
