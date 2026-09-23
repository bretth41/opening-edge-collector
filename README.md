# Opening + Closing Edge Collector — Railway V2

## What it does
A small always-on Python worker that uses the Unusual Whales REST API once per minute during two Eastern-time windows:

- Opening collection: 9:25–10:30 AM ET (primary study: 9:30–10:15)
- Closing collection: 3:30–4:00 PM ET (primary study: 3:40–4:00; 3:30–3:40 is control)

It stores strike-level SPY and SPXW delta/gamma/charm/vanna snapshots in SQLite. On every startup it first makes a SPY test request. If the Basic API plan does not permit the endpoint, the Railway log will say `UW_TEST_FAILED`; if it works, it says `UW_TEST_OK`.

## Security
Never place the real UW API token in this folder or GitHub. Add it only in Railway's Variables screen as `UW_TOKEN`.

## Railway settings
- Start command is already supplied by `railway.toml`: `python collector.py`
- Add a persistent Volume mounted at `/data`
- Add variable `UW_TOKEN` with the real token
- Optional variables are shown in `.env.example`
- This is a persistent background service, not a Railway Cron Job. The code itself understands Eastern Time and daylight-saving changes and sleeps outside collection windows.

## What a healthy deployment looks like
In Railway Logs after deployment, expect:

`START: DB=/data/opening_edge.db ...`

followed by either:

`UW_TEST_OK: SPY endpoint returned ... rows`

or a clear `UW_TEST_FAILED` message.

During a collection window, each minute should produce a `SNAPSHOT` line.

## Why V2 stops here
This version proves reliable minute-by-minute gamma migration capture first. The next layer will add 0DTE option premium/IV capture after we verify which option endpoints the Basic subscription permits. That prevents us from building around an unavailable endpoint.
