# IMM v4.0
Sources: Massive real-time I:SPX A.I:SPX; Unusual Whales SPX/SPXW structure+tape; Databento ES.c.0 MBP-1.
New isolated DB defaults to /data/imm_v4.db; prior databases are untouched.
Required Railway variables: UW_TOKEN, MASSIVE_API_KEY, DATABENTO_API_KEY, EDGE_DASH_PASSWORD.
Acceptance logs: MASSIVE real-time SPX subscribed A.I:SPX; UW SPX 0DTE confirmed with nonzero greek_rows; ES MBP-1 subscription opened.
Five-second master rejects SPX older than 5 seconds rather than silently carrying stale values.
No signal rules, scores, dealer-intent labels, or lookahead.
