# v4.3 validation — 2026-10-08

## Observed results

- **29 automated tests passed** under Python 3.12, with the exact pinned dependencies in requirements.txt.
- All included Python source files compiled successfully.
- **Full synthetic session replay passed:** 4,680 master rows, five-second spacing, zero gaps; expected five-minute SPX warm-up checked.
- Synthetic Greek readiness was false for the first six rows until the first simulated refresh, then true for 4,674 rows. This is a test of gating logic, not a forecast of tomorrow's completeness.
- All 4,680 rows were exported, decompressed, parsed as CSV, and reconciled to the manifest.
- Replay DB size: **55.74 MiB**, vs **591.42 MiB** before JSON compression in the same synthetic test. Compressed CSV ZIP: **9.18 MiB**. Real-day size will vary.
- Full replay runtime: 45.18 seconds on the test machine. This includes the entire synthetic day plus export; no wall-clock market waiting was simulated.

## Cases exercised by the automated suite

1. Correct intraday endpoint, explicit expiry query and Friday-to-Monday next-expiry selection.
2. Repeated payloads do not advance source time, revision, or numeric-change evidence.
3. Actual numeric change with an advancing source time counts as live change at the same strike.
4. Numerical rewrites at an unchanged source time are flagged and do not establish live change.
5. A denied second expiry cannot partially publish a first-expiry update.
6. Pagination, retrying the same 503 page, 429 handling and repeated-page rejection.
7. Credentials are not echoed in provider-error text.
8. New-session clearing even when the first new-day request fails.
9. Regressed source times retain previous values with original timestamps.
10. Identical payloads returned for two distinct expiry requests are rejected.
11. Missing and nonfinite fields stay null; genuine numerical zero stays zero.
12. Wrong-day, future-time and wrong-expiry rows are rejected.
13. Source freshness expires regardless of the last HTTP receipt time.
14. OI and directional sums remain separate; incomplete sums remain null.
15. Trailing changes use a fixed strike/expiry and source-time baseline; missing/new/stale baselines stay null.
16. Readiness requires both complete fresh windows and actual 0DTE change; all-zero delta windows fail validation.
17. Automatic migration from a genuine prior table schema is idempotent and preserves old rows.
18. Full master-row construction, persistence, JSON round trip and added-column coverage.
19. Exact SPX rolling endpoints, missing intervals and session reset.
20. Stale ES quotes become unavailable; misleading absorption/withdrawal fields are null.
21. Out-of-order SPX messages cannot overwrite newer prices.
22. Authentication, health/diagnostics responses, private-credential omission, export content and invalid date handling.
23. Transparent compressed JSON write/read round trip alongside old plain JSON rows.

## Live acceptance checks — still required

Offline tests are not evidence of authenticated UW entitlement or live provider correctness. No live API token was exposed or included in this package. No production deployment was changed during its creation.

After uploading, confirm `imm-v4.3` on /health and the startup log's verified database migration. Then during the regular cash session:

- Master rows advance every five seconds, with current Massive SPX and ES data.
- Replacement endpoint returns HTTP 200 for both explicit expiry requests, with legitimate fixed-strike Gamma/Delta/Charm/Vanna inputs and source times.
- `uw.changed_expiries` includes the same-day expiry after values and source times genuinely advance at existing strikes.
- Node freshness and completeness support the whole-window readiness check. Stale far strikes are reported individually; a successful HTTP poll does not override them.
- One-, three-, five- and ten-minute changes appear only after valid baseline history exists.
- Exported data confirms the same source times, changes, bases and quality seen in diagnostics.

## Explicit scope

This release repairs intraday exposure collection, provenance, change calculation, monitoring, persistence and export. It does not claim direct dealer inventory/hedge execution measurement, true MBP-1 absorption/cancellations, IV capture, whole-chain totals, trading-signal validation or historical recovery of the frozen v4.2 series. These limits are stated in the export metadata and README.
