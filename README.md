# IMM v3.2 — ES MBP-1 + liquidity/absorption observables

Collection-first research build. No signal scores, trading rules, dealer-intent labels, or selected outcomes. Existing `/data/opening_edge_v2.db` remains untouched; research DB remains `/data/imm_v3.db` and is migrated in place by adding new nullable master columns.

## What changed
- Databento ES source changed from unauthorized `mbo` to Standard-plan `mbp-1` for `ES.c.0`.
- MBP-1 stores each BBO-changing event in `es_event`, including native event/action/side/price/size and the resulting best bid/ask, size, order count, sequence and timestamps in `raw_json`.
- Five-second master now includes directly observed or neutral proxy variables for liquidity and absorption research:
  - aggressive buy/sell contract volume and trade counts
  - inside bid/ask size and order count
  - same-price inside depth gain/loss (replenishment/depletion proxies)
  - non-trade same-price depth loss (withdrawal proxy)
  - trade quantity not matched by visible same-price depth loss (absorption/reload proxy)
  - trade imbalance, end depth imbalance
  - mid-price start/end/high/low, 5-second range and displacement per 100 traded contracts
- Depth-5 fields are intentionally `NULL`; MBP-1 only supplies the top level. Never interpret them as zero.
- `source_status_json` explicitly states `es_schema=mbp-1`, one depth level, and that derived microstructure fields are proxies.

## Interpretation limits
MBP-1 provides every event that changes the BBO, every trade, BBO size and BBO order count. It does **not** provide full-depth queues or order IDs, so true hidden liquidity, queue position, cancellations away from the inside, and full-book replenishment cannot be measured. The proxy fields are retained to test relationships, not to assert intent.

The existing UW REST collector continues to retain full strike snapshots, Greeks/contracts and 0DTE option tape. Those data can later be aligned with the ES observables to test whether changes in option structure precede, coincide with, or lag futures aggression/liquidity changes. No hedging causality is assumed in collection.

## Deployment
Upload all ten files in this ZIP to the repository root, replacing same-name files. Do not delete the Railway volume or database. Keep the existing Railway variables. Railway starts `python run_all.py` and `/health` should report `imm-v3.2` after deployment.

## Monday acceptance checks
1. Log shows `MBP-1 subscription opened ES.c.0 (Standard-plan L1)` with no schema authorization error.
2. `es_event` count rises and master rows show fresh ES bid/ask, size/order counts and non-null event metrics.
3. Confirm trade-side semantics against Databento (`B` buy aggressor, `A` sell aggressor).
4. Validate 5-second aggregation against a raw-event replay sample before using proxy variables in research.
5. Verify UW 0DTE/next-expiry coverage, SPX price freshness, option-tape completeness and noon continuation as already planned.
