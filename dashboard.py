import os, sqlite3, json
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Opening Edge Research", page_icon="📈", layout="wide")
DB = Path(os.getenv("EDGE_DB", "/data/opening_edge.db"))
PASSWORD = os.getenv("EDGE_DASH_PASSWORD", "")

def auth():
    if not PASSWORD:
        st.error("Dashboard locked: set EDGE_DASH_PASSWORD in Railway Variables.")
        st.stop()
    if st.session_state.get("authed"):
        return
    st.title("Opening Edge Research")
    entered = st.text_input("Dashboard password", type="password")
    if st.button("Open dashboard", type="primary"):
        if entered == PASSWORD:
            st.session_state["authed"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

auth()

@st.cache_data(ttl=15)
def q(sql, params=()):
    if not DB.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB) as con:
        return pd.read_sql_query(sql, con, params=params)

st.title("Opening Edge Research")
st.caption(f"Persistent database: {DB}")

events = q("SELECT observed_at_et,event_type,detail FROM collector_events ORDER BY observed_at_et DESC LIMIT 500")
if events.empty:
    st.warning("No collector data yet.")
    st.stop()

dates = q("SELECT DISTINCT substr(observed_at_et,1,10) d FROM strike_exposure ORDER BY d DESC")["d"].tolist()
c1,c2,c3 = st.columns([1,1,2])
with c1:
    day = st.selectbox("Session date", dates)
with c2:
    window = st.selectbox("Window", ["CLOSE","OPEN"])
with c3:
    ticker = st.selectbox("Underlying", ["SPY","SPXW"])

exp = q("""SELECT * FROM strike_exposure
           WHERE substr(observed_at_et,1,10)=? AND session_window=? AND ticker=?
           ORDER BY observed_at_et,strike""",(day,window,ticker))

tab1,tab2,tab3,tab4 = st.tabs(["Session","Gamma migration","Phase 2 raw","Collector health"])

with tab1:
    if exp.empty:
        st.info("No strike-exposure snapshots for this selection.")
    else:
        times = sorted(exp.observed_at_et.unique())
        first,last = times[0],times[-1]
        latest = exp[exp.observed_at_et==last].copy()
        latest["net_gamma_vol"] = latest["call_gamma_vol"].fillna(0)+latest["put_gamma_vol"].fillna(0)
        latest["net_charm_vol"] = latest["call_charm_vol"].fillna(0)+latest["put_charm_vol"].fillna(0)
        latest["net_vanna_vol"] = latest["call_vanna_vol"].fillna(0)+latest["put_vanna_vol"].fillna(0)
        spot = latest["underlying_price"].dropna()
        spot = float(spot.iloc[0]) if len(spot) else None
        a,b,c,d = st.columns(4)
        a.metric("Snapshots", len(times))
        b.metric("Strike rows", f"{len(exp):,}")
        c.metric("Latest spot", f"{spot:.2f}" if spot is not None else "—")
        d.metric("Last snapshot", last[11:16] if len(last)>=16 else last)

        st.subheader("Latest gamma profile")
        lo,hi = (spot-8,spot+8) if spot is not None else (latest.strike.quantile(.35),latest.strike.quantile(.65))
        prof=latest[(latest.strike>=lo)&(latest.strike<=hi)][["strike","net_gamma_vol"]].set_index("strike")
        st.bar_chart(prof)

        st.subheader("Underlying price recorded with snapshots")
        px=(exp.groupby("observed_at_et",as_index=False)["underlying_price"].median()
              .dropna().set_index("observed_at_et"))
        st.line_chart(px)

with tab2:
    if exp.empty:
        st.info("No data.")
    else:
        x=exp.copy()
        x["net_gamma_vol"]=x["call_gamma_vol"].fillna(0)+x["put_gamma_vol"].fillna(0)
        x["net_gamma_oi"]=x["call_gamma_oi"].fillna(0)+x["put_gamma_oi"].fillna(0)
        spot=x["underlying_price"].dropna().median()
        radius=st.slider("Strikes around spot",3,20,8)
        near=x[(x.strike>=spot-radius)&(x.strike<=spot+radius)]
        piv=near.pivot_table(index="observed_at_et",columns="strike",values="net_gamma_vol",aggfunc="sum")
        st.subheader("Net directionalized-volume gamma by strike over time")
        st.line_chart(piv)

        first_t=near.observed_at_et.min(); last_t=near.observed_at_et.max()
        f=near[near.observed_at_et==first_t].set_index("strike")["net_gamma_vol"]
        l=near[near.observed_at_et==last_t].set_index("strike")["net_gamma_vol"]
        delta=pd.concat([f.rename("first"),l.rename("last")],axis=1).fillna(0)
        delta["change"]=delta["last"]-delta["first"]
        delta=delta.sort_values("change",key=lambda s:s.abs(),ascending=False)
        st.subheader("Largest gamma changes: first → last snapshot")
        st.dataframe(delta.head(20),use_container_width=True)

with tab3:
    raw=q("""SELECT observed_at_et,endpoint,ticker,contract,length(payload_json) payload_bytes
             FROM phase2_raw WHERE substr(observed_at_et,1,10)=? AND session_window=?
             ORDER BY observed_at_et DESC""",(day,window))
    st.metric("Raw Phase 2 payloads",len(raw))
    st.dataframe(raw,use_container_width=True,height=420)
    st.caption("Raw JSON remains stored in SQLite; this view shows metadata so the browser stays fast.")

with tab4:
    ev=events[events.observed_at_et.str.startswith(day)]
    st.dataframe(ev,use_container_width=True,height=520)
    errs=ev[ev.event_type.str.contains("ERROR|FAILED",regex=True,na=False)]
    st.metric("Errors / failed events",len(errs))
