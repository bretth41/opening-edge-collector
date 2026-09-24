import sqlite3, json
import pandas as pd

def q(db,sql,p=()):
    with sqlite3.connect(db) as c:return pd.read_sql_query(sql,c,params=p)

def exposures(db,day,window,ticker):
    x=q(db,"""SELECT * FROM strike_exposure WHERE substr(observed_at_et,1,10)=?
    AND session_window=? AND ticker=? ORDER BY observed_at_et,strike""",(day,window,ticker))
    if not x.empty:x["g"]=x["call_gamma_vol"].fillna(0)+x["put_gamma_vol"].fillna(0)
    return x

def warnings(db,day,window="CLOSE",ticker="SPY",radius=8):
    """Causal structure warnings. Uses only snapshots available at or before each event time."""
    x=exposures(db,day,window,ticker)
    if x.empty:return {"date":day,"warnings":[]}
    times=sorted(x.observed_at_et.unique());px=x.groupby("observed_at_et")["underlying_price"].median().reindex(times)
    events=[]
    for i,t in enumerate(times):
        if i<3 or pd.isna(px.loc[t]):continue
        spot=float(px.loc[t]);known=times[:i+1];hist=x[x.observed_at_et.isin(known)]
        cur=hist[hist.observed_at_et==t].copy();cur=cur[(cur.strike>=spot-radius)&(cur.strike<=spot+radius)]
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
        weak=[n for n in neg if n["d1"] is not None and n["d3"] is not None and n["d1"]>0 and n["d3"]>0]
        up=[n for n in pos if n["d1"] is not None and n["d3"] is not None and n["d1"]>0 and n["d3"]>0]
        if weak and up:
            flags.append({"type":"BULLISH_MIGRATION","direction":"UP",
                          "ahead":max(up,key=lambda z:abs(z["gamma"])),
                          "behind":max(weak,key=lambda z:abs(z["gamma"]))})
        if flags:events.append({"time":t,"spot":spot,"p1":spot-float(px.iloc[i-1]),"p3":spot-float(px.iloc[i-3]),"flags":flags})
    return {"date":day,"window":window,"ticker":ticker,"causal":True,"warnings":events}

def raw_rows(db,day,window):
    try:return q(db,"SELECT observed_at_et,endpoint,ticker,contract,payload_json FROM phase2_raw WHERE substr(observed_at_et,1,10)=? AND session_window=? ORDER BY observed_at_et",(day,window))
    except:return pd.DataFrame()

def _contract_symbols(payload):
    """Extract only explicit contract identifiers from stored JSON; never infer symbols."""
    out=[]
    try:
        obj=json.loads(payload) if isinstance(payload,str) else payload
    except Exception:return out
    rows=obj.get("data",[]) if isinstance(obj,dict) else []
    if not isinstance(rows,list):return out
    for row in rows:
        if not isinstance(row,dict):continue
        c=row.get("option_symbol") or row.get("id") or row.get("symbol")
        if isinstance(c,str) and c and c not in out:out.append(c)
    return out

def option_context(db,day,window,signal_time,spot,direction):
    """Causal Phase2 provenance plus contract identifiers actually visible by signal time."""
    r=raw_rows(db,day,window)
    if r.empty:return {"status":"no_phase2_raw"}
    r=r[r.observed_at_et<=signal_time]
    if r.empty:return {"status":"no_payload_at_or_before_signal"}
    contracts=[]
    for _,row in r.tail(150).iterrows():
        c=row.get("contract")
        if isinstance(c,str) and c and c not in contracts:contracts.append(c)
        if row.get("endpoint")=="spx_0dte_contracts":
            for sym in _contract_symbols(row.get("payload_json")):
                if sym not in contracts:contracts.append(sym)
    return {"status":"raw_available","signal_time":signal_time,"direction":direction,"spot":spot,
            "candidate_contracts_seen_by_signal":contracts[:40],
            "note":"Contract identifiers are extracted only from payloads stored at or before signal time. No contract or premium field is inferred."}

def _horizon_stats(px,t,entry,direction,minutes):
    future=px[px.index>=t].iloc[:minutes+1]
    if future.empty:return None
    if direction=="DOWN":fav=entry-float(future.min());adv=float(future.max())-entry;end=entry-float(future.iloc[-1])
    else:fav=float(future.max())-entry;adv=entry-float(future.min());end=float(future.iloc[-1])-entry
    return {"minutes":minutes,"mfe":float(fav),"mae":float(adv),"net_at_horizon":float(end),"observations":int(len(future))}

def outcomes(db,day,window="CLOSE",ticker="SPY",radius=8):
    """IMM outcomes: score immediate 2/3/5/10 minute migration, not eventual session direction."""
    w=warnings(db,day,window,ticker,radius);x=exposures(db,day,window,ticker)
    if x.empty:return w
    px=x.groupby("observed_at_et")["underlying_price"].median().sort_index();labeled=[]
    for ev in w["warnings"]:
        t=ev["time"];entry=float(ev["spot"])
        for f in ev["flags"]:
            direction=f["direction"];ctx=option_context(db,day,window,t,entry,direction)
            horizons={str(m):_horizon_stats(px,t,entry,direction,m) for m in (2,3,5,10)}
            labeled.append({"signal_time":t,"type":f["type"],"direction":direction,"entry_spot":entry,
                            "imm_horizons":horizons,"option_context":ctx})
    return {"date":day,"window":window,"ticker":ticker,"objective":"Immediately Monetizable Migration (IMM)",
            "events":labeled,
            "note":"IMM scores the next 2/3/5/10 minutes. A later reversal does not erase a successful immediate migration. Signal generation remains strictly causal. Actual option-return outcomes require stored parseable contract price data."}
