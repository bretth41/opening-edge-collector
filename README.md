# IMM collector v4.3 — complete runtime package

## Upload from your iPad

1. Unzip `IMM_v4.3_complete.zip`.
2. In the existing `bretth41/opening-edge-collector` GitHub repository, upload/replace **every root file in this package**, including the new `imm_features.py`, `requirements.txt` and `.python-version`. Keep the original filenames. Upload the `tests` folder and `VALIDATION.md` too if convenient; these are not required to run the collector.
3. Commit to `main`. Railway already starts `python run_all.py`. Keep the existing environment variables and `/data` volume. Do not delete the database or service. Existing legacy files may remain; this entry point does not run them.
4. Check `/health`. Version must be `imm-v4.3`. Then sign in to `/diagnostics` using the same dashboard password.

All runtime files are included; this is not a one-file patch. Deployment is not performed by this ZIP. Existing rows remain intact. The schema upgrades automatically, and new writes use the existing `/data/imm_v4.db` unless `IMM_DB` is explicitly configured.

## What this release fixes

- Uses `/api/stock/SPX/spot-exposures/expiry-strike`, the documented intraday exposure endpoint. No fallback to the static exposure endpoint.
- Requests one explicitly filtered expiry at a time: confirmed same-day SPX expiry and next **listed** expiry. The latter is called next-expiry, because Friday-to-Monday is not one calendar day.
- Reads every pagination page, starting at page 0. Retries 429/503/transient server errors; a failed second expiry never partially replaces the first.
- Validates numeric values, source times, session dates, expiry identities, duplicate rows and pagination loops. Missing/invalid values remain null.
- Separates source calculation time, HTTP receipt time, snapshot fingerprint and revision. A successful poll alone cannot qualify as a new market observation.
- Preserves OI, volume and bid/ask directional exposure separately for Gamma, Delta, Charm and Vanna. Existing `call_gamma`, etc. aliases refer only to OI exposure. All 32 explicit input fields are in each node's `exposures` object.
- Preserves each strike's calculation time, freshness and provider price. UW's index `price` is an ATM strike estimate, not the Massive SPX spot price.
- Records changes over 1/3/5/10 minutes at a fixed strike/expiry/basis. Baselines are selected at or before the target SOURCE time with at most 60 seconds of additional lag. No interpolation, no fake baseline for a newly entering strike, no cross-session comparison. Warm-up/baseline failures yield null changes.
- Retains the 5-second master table and minute-to-minute SPX momentum fields, with 15 strikes per expiry anchored at the 5-point strike at/below SPX plus/minus 35 points.
- Stops filling absorption/withdrawal columns with misleading values. MBP-1 supplies useful trades and top-of-book liquidity, but does not independently establish those quantities. These columns are null in new rows. Replenishment/depletion remain labelled proxies.
- Rejects stale ES top-of-book state. A valid active interval with no trades records zero; absent/stale feed intervals remain null.
- Compresses large JSON fields inside SQLite and transparently decodes them on reads and CSV export. Existing plain JSON rows stay readable. This reduces volume growth; it does not delete history. Direct external SQLite readers must decode `gz1:` values as base64+gzip. The delivered CSV remains ordinary JSON text.
- Pins the exact dependency versions tested under Python 3.12.

## What tomorrow's validation means

`ok: true` on `/health` means database/application liveness only. It does **not** certify market data.

- `collection_current: true`: a same-session master row is no more than 15 seconds old.
- `/diagnostics` -> `price_and_pressure_ready: true`: recording is current during the cash session, the Massive socket is connected/authenticated/subscribed and ES book receipt is current.
- `greeks_ready: true`: the latest row has all 15 strikes for BOTH expiries, complete OI Gamma/Delta/Charm/Vanna inputs, source times no older than 180 seconds, recent successful polling, nonzero Delta somewhere in each window, and an actual numeric OI change at an existing 0DTE strike with an advancing source time has been observed this session.
- `uw.changed_expiries` reports whether real OI changes were observed in next-expiry too. The 0DTE test is mandatory; slower next-expiry OI change is reported separately.
- `uw.numeric_changes_without_source_advance` exposes a provider rewrite at the same time; it is not accepted as proof of a new observation.

Before/after market hours, `greeks_ready` is deliberately false. Deployment attempts one access probe even off hours, but cannot prove cash-session freshness. At 9:30 ET, Greek rolling changes may need 1/3/5/10 minutes to warm up. SPX momentum needs its own initial 30/60/180/300-second warm-up. A transient request error retains last good data with original source times and marks quality down. Price/ES recording continues even if UW access fails.

Read `greek_reasons` and `/diagnostics` for the exact failure: access denied, wrong/missing expiry, missing inputs, stale strikes, frozen OI, etc. A three-minute threshold is an explicit conservative analysis-quality setting; do not relax it merely to turn the status green. Individual fresh nodes remain available even when the entire window fails readiness.

## Limits that remain explicit

- The authenticated SPX endpoint and its live completeness/freshness must be validated during market hours. Offline tests cannot prove UW token entitlements or provider behavior.
- OI exposure is a model based on daily OI with updated Greeks; it is not directly observed dealer inventory. Directional volume uses the provider's bid/ask assumptions and is an estimate. Neither is a measurement of actual dealer hedge executions.
- The spot-exposure feed does not deliver IV. IV collection is **not** added by this release. No unsupported feed is silently substituted.
- ES is a futures proxy for pressure/liquidity; SPX remains the price and option-structure instrument. There is no SPY option substitution.
- No trading signals, destination forecasts or entry rules are introduced. `nearest_above_json` / `nearest_below_json` are ordered nearby 0DTE nodes, not predictions.
- Schedule is weekday 9:30–16:00 ET; no holiday/early-close calendar is added.
- The new exposure endpoint uses different units from v4.2 (`Greek * OI * 100`). Do not splice old/new levels into one continuous feature series. Historical `collector_version` null means pre-v4.3. No historical frozen data is reconstructed.
- Only the 70-point window is exported for structure; node sums are not whole-chain exposure totals. The collector fetches both expiry chains to maintain rolling fixed-node history as the window moves.

## Research references checked 2026-10-08

- Current official OpenAPI: https://api.unusualwhales.com/api/openapi
- Replacement endpoint and parameters: https://api.unusualwhales.com/docs/operations/PublicApi.TickerController.spot_exposures_by_strike_expiry_v2
- Source recalculation times, differences across strikes, directional exposure conventions and historical reconstruction caveats: https://api.unusualwhales.com/docs/operations/PublicApi.TickerController.spot_exposures_by_strike
- Prior endpoint: https://api.unusualwhales.com/docs/operations/PublicApi.TickerController.greek_exposure_by_strike_expiry
- Official streaming example (reference only; no WebSocket upgrade required): https://github.com/unusual-whales/api-examples/tree/main/examples/ws-stream-spot-greeks-by-strike-by-expiry
- Databento MBP-1, aggressor sides and top-of-book scope: https://databento.com/docs/schemas-and-data-formats/mbp-1

The replacement's schema example omits expiry even though the endpoint returns strike/expiry rows. Each request therefore asks for exactly ONE expiry; explicit expiry mismatches and identical cross-expiry payloads are rejected. When an expiry is missing from a row, `expiry_origin=single_expiry_request` makes the inference visible. A material live schema mismatch is surfaced rather than converted into made-up values.

## Download and testing

Use the same `/export?start=YYYY-MM-DD&end=YYYY-MM-DD` page. ZIP contains only `master.csv` and `manifest.json`; the manifest reports session row counts, version-labelled rows and Greek-ready rows. The master CSV contains source quality, exposure bases and per-node trailing changes.

Run tests in a disposable Python environment after installing requirements:

```bash
python -m unittest discover -s tests -v
python tests/replay_full_session.py
```

Tests redirect every write into a temporary DB, never `/data`. The full-session replay uses synthetic data and does not contact providers. See `VALIDATION.md` for actual results and the remaining live acceptance checks.
