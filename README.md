# IMM v3 candidate — connection-validation build

Follows IMM Requirements & Architecture v2.1, collection first. **Not production-validated**. No scores, alerts, labels or option-P&L logic.

## Before upload
This candidate has important outstanding gates: actual UW SPX websocket and REST entitlement; actual Databento MBO record/snapshot interpretation and ES rollover; SPX five-second cadence. Do not replace the running production collector until these gates pass. Existing `/data/opening_edge_v2.db` is preserved; v3 uses `/data/imm_v3.db`.

## Source architecture
- Databento live `GLBX.MDP3`, `mbo`, `ES.c.0`, snapshot requested. Native events retained temporarily in `es_event`, with book-derived five-second aggregates. ES is never labeled SPX.
- UW `gex_strike_expiry:SPX` websocket provides provisional SPX observed price; `SPX` expiry-specific Greeks and fully paginated contracts polled once per minute. Timestamp/freshness preserved. No fabricated five-second SPX prices.
- Full primary 0DTE + secondary next listed expiry strike history. Neutral proximity top-three levels per side; all available strikes retained. SPX options tape uses UW cursor-based REST polling; endpoint entitlement, pagination completeness and ES snapshot integrity require live validation.
- 09:30–11:30 ET master five-second rows. 11:30–12:00 ET SPX price history continues via websocket; stream subscription runs beyond noon.

## Safe installation
Recommended: use a **separate Railway service and volume** for parallel validation, or create a GitHub staging branch without touching production main. Keep secrets in Railway Variables (`UW_TOKEN`, `DATABENTO_API_KEY`, `EDGE_DASH_PASSWORD`), set `IMM_DB=/data/imm_v3.db` and leave `EDGE_DB` untouched. Set Railway service start command `python run_all.py`.

## Acceptance gates before production promotion
1. Prove both vendor entitlements with actual live data; confirm exact UW SPX vs SPXW semantics, Greek units and strike timestamps.
2. Prove ES snapshot completion, feed gap recovery, correct trade/cancel/modify semantics, front-month rollover and reproducible replay.
3. Validate SPX 0DTE option trade tape completeness and multi-leg identification (multi-leg classification remains pending).
4. Observe SPX price source age, at least one complete morning, 1440 master rows, complete 0DTE strike coverage, no lookahead, source freshness, compressed export.
5. Define measured raw ES retention/storage cap and off-volume backup. Raw event deletion disabled by default.

## Download
The existing research service is untouched in staging. New authenticated `/export?start=YYYY-MM-DD&end=YYYY-MM-DD` creates a ZIP; `/status` shows row counts.
