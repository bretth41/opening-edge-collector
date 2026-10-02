import csv, io, os, secrets, sqlite3, tempfile, zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from starlette.background import BackgroundTask

from signal_engine import warnings, outcomes

DB = Path(os.getenv("EDGE_DB", "/data/opening_edge_v2.db"))
ARCHIVE_DB = DB.with_name("opening_edge.db")
PW = os.getenv("EDGE_DASH_PASSWORD", "")
KEY = os.getenv("EDGE_RESEARCH_KEY", "")

sec = HTTPBasic(auto_error=False)
app = FastAPI(title="Opening Edge Research API", docs_url=None, redoc_url=None)

EXPORT_TABLES = {
    "strike_exposure": "observed_at_et",
    "expiry_strike_structure": "observed_at_et",
    "underlying_snapshot": "observed_at_et",
    "option_snapshot": "observed_at_et",
    "option_trade": "observed_at_et",
    "collector_events": "observed_at_et",
}

def q(sql, p=()):
    with sqlite3.connect(DB) as c:
        return pd.read_sql_query(sql, c, params=p)

def auth(k):
    if not KEY or not k or not secrets.compare_digest(k, KEY):
        raise HTTPException(401, "Invalid research key")

def browser(c: Optional[HTTPBasicCredentials] = Depends(sec)):
    if not PW or not c or not secrets.compare_digest(c.password, PW):
        raise HTTPException(
            401,
            "Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

def norm_window(w):
    return "OPEN" if w.upper() == "MORNING" else w.upper()

@app.get("/health")
def health():
    return {
        "ok": DB.exists(),
        "version": "4.1",
        "db": DB.name,
        "archive": ARCHIVE_DB.exists(),
    }

@app.get("/api/research/warnings")
def aw(
    date: str,
    window: str = "MIDDAY",
    ticker: str = "SPY",
    radius: int = 8,
    key: Optional[str] = Query(None),
):
    auth(key)
    return JSONResponse(warnings(DB, date, norm_window(window), ticker.upper(), radius))

@app.get("/api/research/outcomes")
def ao(
    date: str,
    window: str = "MIDDAY",
    ticker: str = "SPY",
    radius: int = 8,
    key: Optional[str] = Query(None),
):
    auth(key)
    return JSONResponse(outcomes(DB, date, norm_window(window), ticker.upper(), radius))

def parse_date(v: str) -> date:
    try:
        return date.fromisoformat(v)
    except Exception:
        raise HTTPException(400, "Dates must be YYYY-MM-DD")

def remove_file(path: str):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass

@app.get("/export")
def export_data(
    start: str,
    end: str,
    source: str = "current",
    _: bool = Depends(browser),
):
    start_d, end_d = parse_date(start), parse_date(end)

    if end_d < start_d:
        raise HTTPException(400, "End date must be on or after start date")
    if (end_d - start_d).days > 31:
        raise HTTPException(400, "Maximum export range is 32 days")
    if source not in ("current", "archive"):
        raise HTTPException(400, "Source must be current or archive")

    db = DB if source == "current" else ARCHIVE_DB
    if not db.exists():
        raise HTTPException(404, f"{source.title()} database not found")

    lo = start_d.isoformat() + "T00:00:00"
    hi = (end_d + timedelta(days=1)).isoformat() + "T00:00:00"

    fd, tmp = tempfile.mkstemp(prefix="opening_edge_export_", suffix=".zip")
    os.close(fd)

    try:
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as con, \
             zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:

            existing = {
                r[0]
                for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }

            manifest = []

            for table, time_col in EXPORT_TABLES.items():
                if table not in existing:
                    continue

                cur = con.execute(
                    f'SELECT * FROM "{table}" '
                    f'WHERE "{time_col}">=? AND "{time_col}"<? '
                    f'ORDER BY "{time_col}"',
                    (lo, hi),
                )

                cols = [d[0] for d in cur.description]
                count = 0

                with zf.open(f"{table}.csv", "w") as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
                    writer = csv.writer(text)
                    writer.writerow(cols)

                    while True:
                        rows = cur.fetchmany(5000)
                        if not rows:
                            break
                        writer.writerows(rows)
                        count += len(rows)

                    text.flush()

                manifest.append((table, count))

            with zf.open("manifest.csv", "w") as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
                writer = csv.writer(text)
                writer.writerow(
                    ["source", "database", "start", "end", "table", "rows"]
                )
                for table, count in manifest:
                    writer.writerow(
                        [
                            source,
                            db.name,
                            start_d.isoformat(),
                            end_d.isoformat(),
                            table,
                            count,
                        ]
                    )
                text.flush()

        filename = (
            f"opening_edge_{source}_{start_d.isoformat()}_to_{end_d.isoformat()}.zip"
        )
        return FileResponse(
            tmp,
            media_type="application/zip",
            filename=filename,
            background=BackgroundTask(remove_file, tmp),
        )

    except Exception:
        remove_file(tmp)
        raise

@app.get("/", response_class=HTMLResponse)
def home(_: bool = Depends(browser)):
    return """<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Opening Edge Research</title>
<style>
body{font-family:-apple-system;background:#0d1117;color:#e6edf3;max-width:760px;margin:30px auto;padding:20px}
input,select,button{font-size:18px;padding:12px;margin:6px 0;width:100%;box-sizing:border-box}
button{font-weight:700}
label{display:block;margin-top:14px;color:#9da7b3}
.card{background:#161b22;padding:20px;border-radius:12px}
</style>
</head>
<body>
<h1>Opening Edge Research</h1>
<div class="card">
<h2>Download research data</h2>
<p>Creates a compressed ZIP of CSV files for the selected trading dates. The database is opened read-only and is not modified.</p>
<form action="/export" method="get">
<label>Start date</label>
<input type="date" name="start" required>
<label>End date</label>
<input type="date" name="end" required>
<label>Database</label>
<select name="source">
<option value="current">Current — opening_edge_v2.db</option>
<option value="archive">Archive — opening_edge.db</option>
</select>
<button type="submit">Download ZIP</button>
</form>
</div>
<p>Phase 4 — IMM + ISR research foundation.</p>
</body>
</html>"""
