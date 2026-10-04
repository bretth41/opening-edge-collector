# IMM v3.1 — production REST fallback and quality patch

Matches the collection-first architecture in IMM Requirements & Architecture v2.1. No signal scores, rules or selected outcomes. Existing `/data/opening_edge_v2.db` remains untouched. New research DB defaults to `/data/imm_v3.db`.

## Deployment
Upload the ten files in this ZIP to the repository root, replacing same-name files. Do **not** delete other files, Railway volume or database. Railway starts `python run_all.py`. Keep `UW_TOKEN`, `DATABENTO_API_KEY`, `EDGE_DASH_PASSWORD` as Railway variables; never commit secrets. `/health` is public; `/status` and `/export?start=YYYY-MM-DD&end=YYYY-MM-DD` use HTTP Basic with `EDGE_DASH_PASSWORD`.

## Current capture and explicit limits
- UW Basic REST polls SPX expiry breakdown, primary 0DTE and next listed expiry full contract pages plus expiry-specific Greeks every 60s; missing 0DTE explicitly flagged. Source timestamp required before a price counts as an observation. **SPX price entitlement and frequency unverified**. No UW WebSocket dependency.
- UW SPX 0DTE options tape uses `newer_than` high watermark plus `older_than` backwards pagination; if max pages reached, watermark is not advanced and a tape checkpoint flags incomplete capture. Trade data is raw vendor records, not guaranteed complete until live verification. Tune `IMM_TAPE_MAX_PAGES` (default 100) and `IMM_TAPE_POLL_SECONDS` (default 20) after measuring volume. Historical Sunday backfills are deliberately disabled.
- ES live Databento MBO remains configured but **currently unauthorized**. No invented liquidity values. ES book reconstruction, contract mapping, snapshot and native replay are provisional and require live validation before research use.
- Five-second master from 09:30–11:30 ET; nearest 3 strikes above/below plus full-strike companion. From 11:30–12:00 ET a separate `price_continuation` table records as-of source price and age; carried prices are not new observations.
- `tape_checkpoint`, `audit`, `source_status_json`, `/status` and `/export` provide explicit missing-data diagnostics. Export excludes potentially enormous raw ES events by default; raw ES remains on the Railway volume. **A Railway volume is not an independent backup.**

## Tomorrow's live acceptance checks
Verify 0DTE and next-expiry contract/Greek coverage and timestamp alignment, actual SPX price availability/cadence, options-tape completeness and pagination costs, 1,440 five-second master rows, noon continuation and disk usage. Obtain Databento live MBO entitlement and UW SPX price entitlement as needed. **Do not use provisional ES reconstruction for trading conclusions before native-event replay tests.**
