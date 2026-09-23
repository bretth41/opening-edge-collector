import sqlite3, json, re, math
import pandas as pd
from datetime import datetime

def q(db,sql,p=()):
    with sqlite3.connect(db) as c:return pd.read_sql_query(sql,c,params=p)

def exposures(db,day,window,ticker):
    x=q(db,"""SELECT * FROM strike_exposure WHERE substr(observed_at_et,1,10)=?
    AND session_window=? AND ticker=? ORDER BY observed_at_et,strike""",(day,window,ticker))
    if not x.empty:x["g"]=x["call_gamma_vol"].fillna(0)+x["put_gamma_vol"].fillna(0)
    return x

def warnings(db,day,window="CLOSE",ticker="SPY",radius=8):
    x=exposures(db,day,window,ticker)
    if x.empty:return {"date":day,"warnings":[]}
    times=sorted(x.observed_at_et.unique()); px=x.groupby("observed_at_et")["underlying_price"].median().reindex(times)
    events=[]
    for i,t in enumerate(times):
        if i<3 or pd.isna(px.loc[t]):continue
        spot=float(px.loc[t]); known=times[:i+1]; hist=x[x.observed_at_et.isin(known)]
        cur=hist[hist.observed_at_et==t].copy()
        cur=cur[(cur.strike>=spot-radius)&(cur.strike<=spot+radius)]
        if cur.empty:continue
        cur["abs_g"]=cur.g.abs();cur=cur.sort_values("abs_g",ascending=False);total=max(float(cur.abs_g.sum()),1)
        nodes=[]
        for _,r in cur.head(12).iterrows():
            k=float(r.strike);s=hist[hist.strike==k].groupby("observed_at_et")["g"].sum().reindex(known).dropna()
            if len(s)<2:continue
            d=lambda n:float(s.iloc[-1]-s.iloc[-1-n]) if len(s)>n else None
            nodes.append({"strike":k,"gamma":float(r.g),"share":abs(float(r.g))/total,"distance":k-spot,
                          "d1":d(1),"d3":d(3),"d5":d(5)})
        neg=[n for n in nodes if n["strike"]<=spot and n["gamma"]<0]
        pos=[n for n in nodes if n["strike"]>=spot and n["gamma"]>0]
        flags=[]
        dn=[n for n in neg if n["d1"] is not None and n["d3"] is not None and n["d1"]<0 and n["d3"]<0]
        if dn:
            n=max(dn,key=lambda z:abs(z["gamma"]))
            flags.append({"type":"BEARISH_NODE_BUILD","direction":"DOWN","node":n})
        weak=[n for n in neg if n["d1"] and n["d3"] and n["d1"]>0 and n["d3"]>0]
        up=[n for n in pos if n["d1"] and n["d3"] and n["d1"]>0 and n["d3"]>0]
        if weak and up:
            flags.append({"type":"BULLISH_MIGRATION","direction":"UP","ahead":max(up,key=lambda z:abs(z["gamma"])),
                          "behind":max(weak,key=lambda z:abs(z["gamma"]))})
        if flags:events.append({"time":t,"spot":spot,"p1":spot-float(px.iloc[i-1]),"p3":spot-float(px.iloc[i-3]),"flags":flags})
    return {"date":day,"window":window,"ticker":ticker,"causal":True,"warnings":events}

def raw_rows(db,day,window):
    try:return q(db,"SELECT observed_at_et,endpoint,ticker,contract,payload_json FROM phase2_raw WHERE substr(observed_at_et,1,10)=? AND session_window=? ORDER BY observed_at_et",(day,window))
    except:return pd.DataFrame()

def option_context(db,day,window,signal_time,spot,direction):
    """Best-effort extraction from raw Phase2 payloads. Never chooses using future payloads."""
    r=raw_rows(db,day,window)
    if r.empty:return {"status":"no_phase2_raw"}
    r=r[r.observed_at_et<=signal_time]
    if r.empty:return {"status":"no_payload_at_or_before_signal"}
    # Return provenance and candidate contracts seen by signal time. Contract freezing remains causal.
    contracts=[]
    for _,row in r.tail(100).iterrows():
        c=row.get("contract")
        if isinstance(c,str) and c and c not in contracts:contracts.append(c)
    return {"status":"raw_available","signal_time":signal_time,"direction":direction,
            "spot":spot,"candidate_contracts_seen_by_signal":contracts[:20],
            "note":"Contract selection/premium parsing is frozen to payloads available at or before signal time; unsupported payload fields are not invented."}

def outcomes(db,day,window="CLOSE",ticker="SPY",radius=8):
    w=warnings(db,day,window,ticker,radius); x=exposures(db,day,window,ticker)
    if x.empty:return w
    px=x.groupby("observed_at_et")["underlying_price"].median().sort_index()
    labeled=[]
    for ev in w["warnings"]:
        t=ev["time"]; future=px[px.index>=t]; entry=float(ev["spot"])
        for f in ev["flags"]:
            direction=f["direction"]
            if direction=="DOWN":
                favorable=entry-future.min(); adverse=future.max()-entry
            else:
                favorable=future.max()-entry; adverse=entry-future.min()
            ctx=option_context(db,day,window,t,entry,direction)
            labeled.append({"signal_time":t,"type":f["type"],"direction":direction,"entry_spot":entry,
                            "underlying_mfe":float(favorable),"underlying_mae":float(adverse),
                            "remaining_window_minutes":len(future)-1,"option_context":ctx})
    return {"date":day,"window":window,"ticker":ticker,"events":labeled,
            "note":"Outcome labels may use future data; signal generation never does. Premium outcomes are only populated when the stored Phase2 payload exposes parseable contract data."}
