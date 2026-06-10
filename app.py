"""
skysync_combined.py
────────────────────────────────────────────────────────────────
Domestic + International non-stop flight fare scraper → Google Sheets
DATE-RANGE edition with SECTOR SELECTION

Features:
  • Tab-based UI: Domestic | UAE | KSA
  • Sector selection: checkboxes (predefined) + manual entry (type any route)
  • Date range picker (start → end, every day scraped)
  • Worksheet name input
  • Non-stop only, cheapest per airline, all fare classes
  • Domestic always PKR; Ex-UAE → AED; Ex-KSA → SAR; Ex-PAK → PKR
  • Parallel workers
  • Single spreadsheet for all regions (one spreadsheet_id in secrets)
  • CONFIG sheet per region: "Route | Airline | Fare Name | Date | Fare Cell"
    where Date is YYYY-MM-DD
"""

import asyncio
import json
import os
import pathlib
import random
import re
import subprocess
import sys
import threading
import traceback
import urllib.parse
import warnings
from datetime import datetime, timedelta, date
from queue import Queue

import gspread
import streamlit as st
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

warnings.filterwarnings("ignore", category=DeprecationWarning)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ─────────────────────────────────────────────────────────────────────────────
# Playwright browser setup
# ─────────────────────────────────────────────────────────────────────────────
# Reuse the existing Chromium install from the sibling "International Fare
# Comparison" folder if present — saves a 200MB re-download. Falls back to
# the local .playwright-browsers/ dir otherwise.
_SIBLING_BROWSERS = (
    pathlib.Path(__file__).resolve().parent.parent
    / "International Fare Comparison" / ".playwright-browsers"
)
if _SIBLING_BROWSERS.is_dir() and (_SIBLING_BROWSERS / "chromium-1217").is_dir():
    _browsers_dir = _SIBLING_BROWSERS
else:
    _browsers_dir = pathlib.Path(__file__).resolve().parent / ".playwright-browsers"
_browsers_dir.mkdir(parents=True, exist_ok=True)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(_browsers_dir)
_browser_install_lock = threading.Lock()


def _chromium_installed() -> bool:
    if _browsers_dir.is_dir():
        for name in ("chrome-headless-shell", "chrome", "chrome.exe",
                     "chrome-headless-shell.exe"):
            if any(_browsers_dir.rglob(name)):
                return True
    default_cache = pathlib.Path.home() / "AppData" / "Local" / "ms-playwright"
    if default_cache.is_dir():
        for name in ("chrome-headless-shell.exe", "chrome.exe"):
            if any(default_cache.rglob(name)):
                return True
    return False


def ensure_playwright_browsers() -> None:
    if _chromium_installed():
        return
    with _browser_install_lock:
        if _chromium_installed():
            return
        print("⏳ Downloading Playwright Chromium (first run ~1-2 min)...")
        proc = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            env={**os.environ, "PLAYWRIGHT_BROWSERS_PATH": str(_browsers_dir)},
            capture_output=True, text=True, timeout=600,
        )
        if proc.stdout:
            print(proc.stdout.strip())
        if proc.returncode != 0:
            raise RuntimeError(f"playwright install failed:\n{proc.stderr}\n{proc.stdout}")
        if not _chromium_installed():
            raise RuntimeError("Chromium still missing after install.")
        print("✅ Playwright Chromium ready.\n")


# ─────────────────────────────────────────────────────────────────────────────
# Persistent queues (one set per region tab, keyed by region)
# ─────────────────────────────────────────────────────────────────────────────
for _rk in ("DOM", "UAE", "KSA"):
    if f"lq_{_rk}" not in st.session_state:
        st.session_state[f"lq_{_rk}"] = Queue()
    if f"dq_{_rk}" not in st.session_state:
        st.session_state[f"dq_{_rk}"] = Queue()

original_print = print


def make_log_print(region: str):
    lq = st.session_state[f"lq_{region}"]
    def _log(*args, **kwargs):
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        msg = sep.join(str(a) for a in args) + end
        lq.put(msg)
        original_print(msg, file=sys.__stdout__)
    return _log


# ─────────────────────────────────────────────────────────────────────────────
# Sector definitions
# ─────────────────────────────────────────────────────────────────────────────
AIRPORT_NAMES = {
    "ISB": "Islamabad", "KHI": "Karachi",   "LHE": "Lahore",
    "KDU": "Skardu",    "MUX": "Multan",    "PEW": "Peshawar",
    "DXB": "Dubai",     "AUH": "Abu Dhabi", "SHJ": "Sharjah",
    "RUH": "Riyadh",    "JED": "Jeddah",    "DMM": "Dammam", "MED": "Medina",
}

# (origin, dest, currency)
DOMESTIC_SECTORS = [
    ("KHI", "LHE", "PKR"), ("LHE", "KHI", "PKR"),
    ("ISB", "KHI", "PKR"), ("KHI", "ISB", "PKR"),
    ("KDU", "ISB", "PKR"), ("ISB", "KDU", "PKR"),
    ("KDU", "LHE", "PKR"), ("LHE", "KDU", "PKR"),
    ("KHI", "KDU", "PKR"), ("KDU", "KHI", "PKR"),
]

UAE_SECTORS = [
    ("ISB", "DXB", "PKR"), ("LHE", "DXB", "PKR"), ("KHI", "DXB", "PKR"), ("MUX", "DXB", "PKR"),
    ("ISB", "AUH", "PKR"), ("LHE", "AUH", "PKR"),
    ("ISB", "SHJ", "PKR"), ("LHE", "SHJ", "PKR"), ("MUX", "SHJ", "PKR"),
    ("DXB", "ISB", "AED"), ("DXB", "LHE", "AED"), ("DXB", "KHI", "AED"), ("DXB", "MUX", "AED"),
    ("AUH", "ISB", "AED"), ("AUH", "LHE", "AED"),
    ("SHJ", "ISB", "AED"), ("SHJ", "LHE", "AED"), ("SHJ", "MUX", "AED"),
]

KSA_SECTORS = [
    ("ISB", "RUH", "PKR"), ("LHE", "RUH", "PKR"),
    ("ISB", "JED", "PKR"), ("LHE", "JED", "PKR"), ("KHI", "JED", "PKR"),
    ("MUX", "JED", "PKR"),
    ("RUH", "ISB", "SAR"), ("RUH", "LHE", "SAR"),
    ("JED", "ISB", "SAR"), ("JED", "LHE", "SAR"), ("JED", "KHI", "SAR"),
    ("JED", "MUX", "SAR"),
]

REGION_SECTORS = {"DOM": DOMESTIC_SECTORS, "UAE": UAE_SECTORS, "KSA": KSA_SECTORS}

STALL_SECONDS  = 120
STABLE_SECONDS = 5
RESTART_DELAY  = 10

# ─────────────────────────────────────────────────────────────────────────────
# Browser / JS hooks
# ─────────────────────────────────────────────────────────────────────────────
HOOK_JS = """
window.__allFlightBatches = [];
const _orig = JSON.parse;
JSON.parse = function(...args) {
    const result = _orig.apply(this, args);
    try {
        const s = JSON.stringify(result);
        if (s.includes('"flights"') && s.includes('"flight_number"'))
            window.__allFlightBatches.push(result);
    } catch(e) {}
    return result;
};
"""

COUNT_JS = """
() => {
    let total = 0;
    for (const batch of window.__allFlightBatches) {
        const lists = (batch.data && batch.data.flights)
            ? batch.data.flights : (batch.flights || []);
        for (const fl of lists)
            total += Array.isArray(fl) ? fl.length : 1;
    }
    return total;
}
"""


async def open_browser(p, worker_id: int = 0, region: str = "DOM"):
    context = await p.chromium.launch_persistent_context(
        user_data_dir=f"/tmp/chrome_profile_{region}_w{worker_id}",
        headless=st.secrets.get("HEADLESS", True),
        no_viewport=True,
        args=["--disable-blink-features=AutomationControlled", "--start-maximized"],
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    page = context.pages[0] if context.pages else await context.new_page()
    await page.add_init_script(HOOK_JS)
    return context, page


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def build_url(origin: str, dest: str, date_str: str, currency: str) -> str:
    cabin = urllib.parse.quote(json.dumps({"code": "Y", "label": "Economy"}))
    legs  = urllib.parse.quote(json.dumps([{
        "departureDate":   date_str,
        "origin":          origin,
        "destination":     dest,
        "originName":      AIRPORT_NAMES.get(origin, origin),
        "destinationName": AIRPORT_NAMES.get(dest, dest),
    }]))[3:-3]
    pax = urllib.parse.quote(json.dumps({"numAdult": 1, "numChild": 0, "numInfant": 0}))
    url = (
        f"https://www.sastaticket.pk/air/search"
        f"?cabinClass={cabin}&legs[]={legs}&routeType=ONEWAY"
        f"&travelerCount={pax}&sortBy=cheapest"
    )
    if currency.upper() != "PKR":
        url += f"&currency={currency.upper()}"
    return url


def is_nonstop(flight: dict) -> bool:
    legs = flight.get("legs") or []
    if len(legs) != 1:
        return False
    segs = legs[0].get("segments") or []
    if len(segs) != 1:
        return False
    leg = legs[0]
    for field in ("stops", "stop_count", "number_of_stops", "num_stops"):
        for obj in (leg, flight):
            if field not in obj:
                continue
            val = obj[field]
            if val not in (0, "0", None, False, ""):
                return False
    label = str(
        leg.get("stop_label") or leg.get("stops_text") or leg.get("stop_info") or ""
    ).lower()
    if label and any(w in label for w in ("stop", "layover", "connect", "via ")):
        if not any(w in label for w in ("non-stop", "nonstop", "direct", "0 stop")):
            return False
    return True


def dep_time(flight: dict) -> str:
    try:
        seg = flight["legs"][0]["segments"][0]
        raw = (
            seg.get("departure_datetime") or seg.get("departure_time")
            or seg.get("dep_time") or seg.get("departure") or ""
        )
        if not raw:
            return "N/A"
        m = re.search(r"T(\d{2}:\d{2})", str(raw))
        if m:
            h, mn = map(int, m.group(1).split(":"))
            return f"{h % 12 or 12}:{mn:02d} {'AM' if h < 12 else 'PM'}"
        return str(raw)
    except Exception:
        return "N/A"


def airline_code(flight: dict) -> str:
    try:
        segment = flight["legs"][0]["segments"][0]
        airline_code = segment["operating_airline"]["code"].upper()
        flight_number = segment.get("flight_number", "")

        # Unwrap if API returns a list or dict instead of a plain string
        if isinstance(flight_number, list):
            flight_number = flight_number[0] if flight_number else ""
        if isinstance(flight_number, dict):
            flight_number = flight_number.get("number") or flight_number.get("flight_number") or ""
        flight_number = str(flight_number).strip().replace("[", "").replace("]", "").replace("'", "").replace('"', '')

        if flight_number:
            # Avoid double prefix like PA-PA-201 when API already includes it
            if flight_number.upper().startswith(airline_code + "-"):
                return flight_number
            return f"{airline_code}-{flight_number}"
        return airline_code
    except Exception:
        return "??"


def date_range(start: date, end: date) -> list[str]:
    return [
        (start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end - start).days + 1)
    ]


def parse_manual_sectors(text: str, default_currency: str = "PKR") -> list[tuple]:
    """
    Parse manually entered routes like:
      KHI-ISB, ISB-LHE, LHE-KHI
    Each gets default_currency. For intl manual entries with currency:
      KHI-DXB:AED  or  DXB-KHI:PKR
    Returns list of (origin, dest, currency).
    """
    sectors = []
    for token in re.split(r"[,\s\n]+", text.strip().upper()):
        token = token.strip()
        if not token:
            continue
        # Support optional :CURRENCY suffix
        if ":" in token:
            route, cur = token.split(":", 1)
        else:
            route, cur = token, default_currency
        parts = route.split("-")
        if len(parts) == 2 and all(len(p) == 3 for p in parts):
            sectors.append((parts[0], parts[1], cur))
    return sectors


# ─────────────────────────────────────────────────────────────────────────────
# Core scrape
# ─────────────────────────────────────────────────────────────────────────────
async def scrape_one(page, origin: str, dest: str, date_str: str,
                     currency: str, _print) -> list | None:
    await page.evaluate("window.__allFlightBatches = []")
    await page.goto(build_url(origin, dest, date_str, currency), wait_until="commit")
    await page.wait_for_timeout(3000)

    try:
        btn = page.locator('button:has-text("Stay on the web")')
        if await btn.is_visible():
            await btn.click()
    except Exception:
        pass

    last_count = -1
    stable_ticks = 0
    zero_ticks   = 0

    for _ in range(STALL_SECONDS + STABLE_SECONDS + 10):
        await asyncio.sleep(1)
        count = await page.evaluate(COUNT_JS)

        if count == 0:
            zero_ticks += 1
            if zero_ticks >= STALL_SECONDS:
                _print(f"      ⚠️  Stalled — no flights for {STALL_SECONDS}s")
                return None
        else:
            zero_ticks = 0

        if count > 0 and count == last_count:
            stable_ticks += 1
            if stable_ticks >= STABLE_SECONDS:
                nb = await page.evaluate("window.__allFlightBatches.length")
                _print(f"      ✅ Stable — {count} flights / {nb} batch(es)")
                break
        else:
            if count != last_count:
                nb = await page.evaluate("window.__allFlightBatches.length")
                _print(f"      📦 {count} flights / {nb} batch(es)...")
            stable_ticks = 0
            last_count   = count

    all_batches = await page.evaluate("window.__allFlightBatches")
    if not all_batches:
        return []

    all_flights, seen = [], set()
    for batch in all_batches:
        lists = batch.get("data", {}).get("flights") or batch.get("flights") or []
        for item in lists:
            for fl in (item if isinstance(item, list) else [item]):
                h = fl.get("hash") or json.dumps(fl, sort_keys=True)[:80]
                if h not in seen:
                    seen.add(h)
                    all_flights.append(fl)

    _print(f"      ✈️  {len(all_flights)} unique flights — filtering non-stop...")

    nonstop_flights = []
    skipped = 0

    for fl in all_flights:
        if not is_nonstop(fl):
            skipped += 1
            continue

        al = airline_code(fl)
        t = dep_time(fl)

        # Exchange rate handling (same as before)
        exchange_rates = fl.get("meta", {}).get("exchange_rate", {})
        rate = 1.0
        if currency != "PKR":
            rate = exchange_rates.get(currency)
            if not rate or rate <= 0:
                if currency == "SAR":
                    provider_meta = fl.get("fare_options", [{}])[0].get("price", {}).get("meta", {})
                    sar_rate = provider_meta.get("sar_to_pkr_rate")
                    if sar_rate and sar_rate > 0:
                        rate = 1.0 / sar_rate
                if not rate or rate <= 0:
                    _print(f"      ⚠️  Exchange rate for {currency} not found, keeping PKR")
                    rate = 1.0

        fares = {}
        for fo in fl.get("fare_options", []):
            fname = (fo.get("fare_name") or "").strip()
            p = (
                fo.get("price", {}).get("selling_fare")
                or fo.get("selling_fare")
                or fo.get("price") or 0
            )
            if fname and isinstance(p, (int, float)) and p > 0:
                converted = round(p * rate)
                if fname not in fares or converted < fares[fname]:
                    fares[fname] = converted

        if fares:
            nonstop_flights.append({
                "airline": al,
                "departure_time": t,
                "fares": fares,
            })
            _print(f"        → {al}  {t}  fares={list(fares.keys())}")

    if skipped:
        _print(f"      ⏭️  Skipped {skipped} connecting flight(s)")

    return nonstop_flights


# ─────────────────────────────────────────────────────────────────────────────
# Parallel scraping
# ─────────────────────────────────────────────────────────────────────────────
async def scrape_worker(
    worker_id: int,
    task_list: list,
    shared_results: dict,
    results_lock: asyncio.Lock,
    total_tasks: int,
    completed_counter: list,
    region: str,
    data_queue: Queue,
    _print,
    p,                          # shared Playwright instance passed in from scrape_all_parallel
) -> None:
    tag = f"[W{worker_id}]"

    context, page = await open_browser(p, worker_id, region)
    retries = 0

    try:
        for (origin, dest, currency, date_str) in task_list:
            completed_counter[0] += 1
            route_key = f"{origin}-{dest}"
            _print(f"\n  {tag} [{completed_counter[0]}/{total_tasks}] "
                   f"{route_key} – {date_str}  [{currency}]")

            while True:
                try:
                    data = await scrape_one(page, origin, dest, date_str, currency, _print)
                except Exception as e:
                    _print(f"      {tag} ❌ Exception: {e}")
                    data = []

                if data is None:
                    retries += 1
                    if retries >= 3:
                        _print(f"      {tag} ❌ Max retries — skipping {route_key} {date_str}")
                        data = []
                        retries = 0
                        break
                    _print(f"      {tag} 🔁 Retry {retries}/3 — restarting browser...")
                    try:
                        await context.close()
                    except Exception:
                        pass
                    await asyncio.sleep(RESTART_DELAY)
                    context, page = await open_browser(p, worker_id, region)
                    continue
                else:
                    retries = 0
                    break

            async with results_lock:
                shared_results[(origin, dest, date_str)] = data

            for flight in data:
                al = flight["airline"]
                dep_time_val = flight["departure_time"]
                for fare_name, price in flight["fares"].items():
                    data_queue.put({
                        "route":    route_key,
                        "airline":  al,
                        "fare":     fare_name,
                        "date":     date_str,
                        "price":    price,
                        "currency": currency,
                        "time":     dep_time_val,
                        "worker":   worker_id,
                    })

            remaining = total_tasks - completed_counter[0]
            if remaining > 0:
                delay = random.uniform(20, 40)
                _print(f"      {tag} 💤 Sleeping {delay:.1f}s...")
                await asyncio.sleep(delay)

    finally:
        try:
            await context.close()
        except Exception:
            pass


async def scrape_all_parallel(sectors: list, dates: list[str],
                               n_workers: int, region: str,
                               data_queue: Queue, _print) -> dict:
    task_list = [
        (origin, dest, currency, date_str)
        for (origin, dest, currency) in sectors
        for date_str in dates
    ]

    n_workers = max(1, min(n_workers, len(task_list)))
    chunks: list[list] = [[] for _ in range(n_workers)]
    for i, task in enumerate(task_list):
        chunks[i % n_workers].append(task)

    total_tasks       = len(task_list)
    shared_results: dict = {}
    results_lock      = asyncio.Lock()
    completed_counter = [0]

    _print(f"  🚀 Launching {n_workers} parallel browser(s) — "
           f"{len(sectors)} sectors × {len(dates)} dates = {total_tasks} tasks")
    for i, chunk in enumerate(chunks):
        if chunk:
            summary = ", ".join(f"{o}-{d}" for o, d, _, __ in chunk[:4])
            ellipsis = "…" if len(chunk) > 4 else ""
            _print(f"     W{i}: {len(chunk)} task(s) → {summary}{ellipsis}")
    _print("")

    # One shared Playwright instance — workers each get their own browser context
    # but share the same playwright process, which is what allows true parallelism.
    async with Stealth().use_async(async_playwright()) as p:
        await asyncio.gather(*[
            scrape_worker(
                worker_id=i,
                task_list=chunk,
                shared_results=shared_results,
                results_lock=results_lock,
                total_tasks=total_tasks,
                completed_counter=completed_counter,
                region=region,
                data_queue=data_queue,
                _print=_print,
                p=p,
            )
            for i, chunk in enumerate(chunks)
            if chunk
        ])

    return shared_results


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets push
# ─────────────────────────────────────────────────────────────────────────────
def push_to_sheets(results: dict, worksheet_name: str,
                   spreadsheet_id: str, _print) -> dict:
    creds_json = st.secrets.get("gcp_service_account", "")
    if not creds_json:
        _print("  ❌ GCP service account secrets missing. Cannot push to sheets.")
        return {"pasted": [], "unmapped": []}

    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(spreadsheet_id)

    try:
        target_ws = spreadsheet.worksheet(worksheet_name)
        _print(f"  📄 Worksheet '{worksheet_name}' found, clearing it...")
        target_ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        target_ws = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=200)
        _print(f"  📄 Created worksheet '{worksheet_name}'")

    if not results:
        _print("  ⚠️  No fare data to write.")
        return {"pasted": [], "unmapped": []}

    # ── Colour palette (RGB 0-1) ──
    AIRLINE_STYLES = {
        "PA":  {"name": "AIRBLUE",   "color": {"red": 0.12, "green": 0.27, "blue": 0.55}},
        "ER":  {"name": "AIRBLUE",   "color": {"red": 0.12, "green": 0.27, "blue": 0.55}},
        "9P":  {"name": "FLYJINNAH", "color": {"red": 0.90, "green": 0.15, "blue": 0.15}},
        "PF":  {"name": "AIRSIAL",   "color": {"red": 0.85, "green": 0.65, "blue": 0.10}},
        "PK":  {"name": "PIA",       "color": {"red": 0.10, "green": 0.50, "blue": 0.25}},
    }
    DEFAULT_STYLE = {"name": "OTHER", "color": {"red": 0.40, "green": 0.40, "blue": 0.40}}
    WHITE_TEXT = {"red": 1.0, "green": 1.0, "blue": 1.0}
    DARK_TEXT  = {"red": 0.1, "green": 0.1, "blue": 0.1}

    # Group results by route
    routes_data: dict[str, dict[str, list]] = {}
    for (origin, dest, date_str), flights in results.items():
        route = f"{origin}-{dest}"
        routes_data.setdefault(route, {})[date_str] = flights

    all_rows: list[list] = []
    all_merges: list[tuple] = []          # (sr, sc, er, ec)  1-indexed
    all_formats: list[tuple] = []         # (sr, sc, er, ec, format_dict)
    route_data_ranges: list[tuple] = []   # (sr, er, sc, ec)
    current_row = 1
    pasted: list[dict] = []

    for route in sorted(routes_data.keys()):
        by_date = routes_data[route]
        dates = sorted(by_date.keys())

        # Collect cheapest fare per flight per date
        flights_info: dict[str, dict] = {}
        for d in dates:
            for fl in by_date[d]:
                key = fl["airline"]
                prefix = key.split("-")[0] if "-" in key else key[:2]
                if key not in flights_info:
                    flights_info[key] = {
                        "prefix": prefix,
                        "time": fl.get("departure_time") or "LOADS",
                        "fares": {},
                    }
                if fl.get("fares"):
                    best = min(fl["fares"].values())
                    old = flights_info[key]["fares"].get(d)
                    flights_info[key]["fares"][d] = best if old is None else min(old, best)

        # Order airlines
        prefix_order = sorted(
            {f["prefix"] for f in flights_info.values()},
            key=lambda p: (
                0 if p in ("PA", "ER") else
                1 if p == "9P" else
                2 if p == "PF" else
                3 if p == "PK" else
                4
            )
        )
        airlines = {p: sorted([k for k, v in flights_info.items() if v["prefix"] == p])
                    for p in prefix_order}

        n_flights = sum(len(v) for v in airlines.values())
        total_cols = 2 + n_flights
        if n_flights == 0:
            continue

        # Row 1 : Route name
        all_rows.append([route.replace("-", " ")] + [""] * (total_cols - 1))
        all_merges.append((current_row, 3, current_row, total_cols))
        all_formats.append((current_row, 3, current_row, total_cols, {
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "textFormat": {"bold": True, "fontSize": 13, "foregroundColor": DARK_TEXT},
        }))
        current_row += 1

        # Row 2 : Airline names merged over their flights
        row2 = ["", ""]
        c = 3
        for p in prefix_order:
            if p not in airlines or not airlines[p]:
                continue
            style = AIRLINE_STYLES.get(p, DEFAULT_STYLE)
            span = len(airlines[p])
            row2.extend([style["name"]] + [""] * (span - 1))
            if span > 1:
                all_merges.append((current_row, c, current_row, c + span - 1))
            all_formats.append((current_row, c, current_row, c + span - 1, {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"bold": True, "foregroundColor": WHITE_TEXT},
                "backgroundColor": style["color"],
            }))
            c += span
        all_rows.append(row2)
        current_row += 1

        # Row 3 : Flight numbers
        row3 = ["", ""]
        c = 3
        for p in prefix_order:
            if p not in airlines or not airlines[p]:
                continue
            style = AIRLINE_STYLES.get(p, DEFAULT_STYLE)
            for fk in airlines[p]:
                clean_fk = fk.replace("[", "").replace("]", "").replace("'", "").replace('"', '')
                row3.append(clean_fk)
                all_formats.append((current_row, c, current_row, c, {
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "textFormat": {"bold": True, "foregroundColor": WHITE_TEXT},
                    "backgroundColor": style["color"],
                }))
                c += 1
        all_rows.append(row3)
        current_row += 1

        # Row 4 : Departure times
        row4 = ["", ""]
        c = 3
        for p in prefix_order:
            if p not in airlines or not airlines[p]:
                continue
            style = AIRLINE_STYLES.get(p, DEFAULT_STYLE)
            for fk in airlines[p]:
                row4.append(flights_info[fk]["time"])
                all_formats.append((current_row, c, current_row, c, {
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "textFormat": {"bold": True, "foregroundColor": WHITE_TEXT},
                    "backgroundColor": style["color"],
                }))
                c += 1
        all_rows.append(row4)
        current_row += 1

        # Row 5 : COUNT OF DAYS | Date header
        all_rows.append(["COUNT OF DAYS", "Date"] + [""] * n_flights)
        all_formats.append((current_row, 1, current_row, 2, {
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "textFormat": {"bold": True, "foregroundColor": DARK_TEXT},
            "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
        }))
        current_row += 1

        # Data rows
        data_start_r = current_row
        for i, d in enumerate(dates):
            label = f"{(i + 1) * 24}H"
            disp = datetime.strptime(d, "%Y-%m-%d").strftime("%d-%b")
            row = [label, disp]
            for p in prefix_order:
                if p not in airlines:
                    continue
                for fk in airlines[p]:
                    price = flights_info[fk]["fares"].get(d)
                    row.append(price if price is not None else "")
                    if price is not None:
                        pasted.append({
                            "date": d, "route": route, "airline": fk,
                            "price": price, "day_label": label,
                        })
            all_rows.append(row)
            current_row += 1
        route_data_ranges.append((data_start_r, current_row - 1, 3, total_cols))

        # Blank separator
        all_rows.append([""] * total_cols)
        current_row += 1

    # ── Resize & write values ──
    if not all_rows:
        _print("  ⚠️  No rows to write.")
        return {"pasted": [], "unmapped": []}

    max_cols = max(len(r) for r in all_rows)
    if current_row > target_ws.row_count or max_cols > target_ws.col_count:
        target_ws.resize(rows=max(current_row + 10, target_ws.row_count),
                         cols=max(max_cols + 5, target_ws.col_count))

    target_ws.update(all_rows, value_input_option='USER_ENTERED')
    _print(f"  ✅ Data written ({len(all_rows)} rows).")

    # ── Batch all formatting into ONE API call ──
    requests: list[dict] = []
    sheet_id = target_ws.id

    # 1) Merge cells
    for sr, sc, er, ec in all_merges:
        requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": sr - 1,
                    "endRowIndex": er,
                    "startColumnIndex": sc - 1,
                    "endColumnIndex": ec,
                },
                "mergeType": "MERGE_ALL",
            }
        })

    # 2) Formatting (colours, alignment, bold)
    for sr, sc, er, ec, fmt in all_formats:
        user_fmt: dict = {}
        fields: list[str] = []
        if "backgroundColor" in fmt:
            user_fmt["backgroundColor"] = fmt["backgroundColor"]
            fields.append("backgroundColor")
        if "horizontalAlignment" in fmt:
            user_fmt["horizontalAlignment"] = fmt["horizontalAlignment"]
            fields.append("horizontalAlignment")
        if "verticalAlignment" in fmt:
            user_fmt["verticalAlignment"] = fmt["verticalAlignment"]
            fields.append("verticalAlignment")
        if "textFormat" in fmt:
            user_fmt["textFormat"] = fmt["textFormat"]
            fields.append("textFormat")

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": sr - 1,
                    "endRowIndex": er,
                    "startColumnIndex": sc - 1,
                    "endColumnIndex": ec,
                },
                "cell": {"userEnteredFormat": user_fmt},
                "fields": f"userEnteredFormat({','.join(fields)})",
            }
        })

    # 3) Centre-align data body
    for ds, de, sc, ec in route_data_ranges:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": ds - 1,
                    "endRowIndex": de,
                    "startColumnIndex": sc - 1,
                    "endColumnIndex": ec,
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)",
            }
        })

    # 4) Auto-resize columns A & B
    requests.append({
        "autoResizeDimensions": {
            "dimensions": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": 0,
                "endIndex": 2,
            }
        }
    })

    # Send everything in one batch
    try:
        spreadsheet.batch_update({"requests": requests})
        _print(f"  ✅ Formatting applied ({len(requests)} operations batched).")
    except Exception as e:
        _print(f"  ⚠️  Batch format failed: {e}")

    _print(f"  ✅ Wrote {len(pasted)} fares in matrix layout to '{worksheet_name}'.")
    return {"pasted": pasted, "unmapped": []}


def run_scrape_thread(region: str, sectors: list, worksheet_name: str,
                      spreadsheet_id: str, dates: list[str], n_workers: int,
                      lq: "Queue", dq: "Queue"):
    def _print(*args, **kwargs):
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        msg = sep.join(str(a) for a in args) + end
        lq.put(msg)
        original_print(msg, file=sys.__stdout__)

    try:
        _print("═" * 60)
        _print(f"  SkySync Pro — {region} Scraper")
        _print("═" * 60)
        _print(f"  Region   : {region}")
        _print(f"  Sectors  : {len(sectors)}")
        _print(f"  Workers  : {n_workers}")
        _print(f"  Dates    : {dates[0]} → {dates[-1]} ({len(dates)} day(s))")
        _print(f"  Sheet    : {worksheet_name}")
        _print("═" * 60 + "\n")

        ensure_playwright_browsers()
        results = asyncio.run(
            scrape_all_parallel(sectors, dates, n_workers, region, dq, _print)
        )

        if not results:
            _print("\n⚠️  No data collected.")
            THREAD_RESULTS[region] = {"pasted": [], "unmapped": []}
        else:
            total_fares = sum(
                len(flight["fares"])
                for flights in results.values()
                for flight in flights
        )
            _print(f"\n📋 Pushing {total_fares} fare values to '{worksheet_name}'...")
            sheet_result = push_to_sheets(
            results, worksheet_name, spreadsheet_id, _print
            )
            # Write to module-level dict — the fragment will sync this into
            # session_state on the next rerun.
            THREAD_RESULTS[region] = {
                "pasted":   sheet_result["pasted"],
                "unmapped": sheet_result["unmapped"],
            }
            _print("\n🏁 All done!")

    except Exception:
        _print("=" * 60)
        _print("❌ EXCEPTION IN SCRAPER")
        _print("=" * 60)
        tb = traceback.format_exc()
        lq.put(tb)
        original_print(tb, file=sys.__stderr__)
        THREAD_RESULTS[region] = {"pasted": [], "unmapped": [], "error": tb}
    finally:
        # Signal completion via the log queue with a sentinel marker.
        lq.put("__SCRAPE_DONE__\n")


# ─────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────────────────────────
_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500&family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,300&display=swap');

/* ── Base ── */
.stApp {
    background: #f5f7fa;
    color: #1a202c;
    font-family: 'DM Sans', sans-serif;
}
.block-container { padding-top: 0 !important; max-width: 1440px !important; padding-left: 2rem !important; padding-right: 2rem !important; }

/* hide streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid #e2e8f0 !important;
    gap: 0 !important;
    padding: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border: none !important;
    border-bottom: 2px solid transparent !important;
    color: #a0aec0 !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    padding: 0.9rem 1.6rem !important;
    transition: color 0.15s, border-color 0.15s !important;
    margin-bottom: -1px !important;
}
.stTabs [aria-selected="true"] {
    color: #1a202c !important;
    border-bottom-color: #2563eb !important;
    background: transparent !important;
}
.stTabs [data-baseweb="tab-panel"] { padding-top: 1.8rem !important; }

/* ── Hero ── */
.ss-hero {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 2rem 0 1.2rem;
    border-bottom: 1px solid #e2e8f0;
    margin-bottom: 0;
}
.ss-hero-left { display: flex; align-items: baseline; gap: 1rem; }
.ss-hero h1 {
    font-family: 'Syne', sans-serif;
    font-size: 1.55rem;
    font-weight: 800;
    letter-spacing: -0.01em;
    color: #0f172a;
    margin: 0;
    line-height: 1;
}
.ss-hero h1 span { color: #2563eb; }
.ss-hero-sub {
    font-size: 0.73rem;
    color: #94a3b8;
    font-family: 'IBM Plex Mono', monospace;
    letter-spacing: 0.03em;
}

/* ── Status pill ── */
.status-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem;
    font-weight: 500;
    letter-spacing: 0.12em;
    padding: 0.28rem 0.85rem;
    border-radius: 3px;
    text-transform: uppercase;
}
.status-pill::before {
    content: '';
    width: 6px;
    height: 6px;
    border-radius: 50%;
    display: inline-block;
}
.status-idle  { color: #94a3b8; background: #f8fafc; border: 1px solid #e2e8f0; }
.status-idle::before  { background: #94a3b8; }
.status-scan  { color: #2563eb; background: #eff6ff; border: 1px solid #bfdbfe; animation: blink-dot 1.2s ease-in-out infinite; }
.status-scan::before  { background: #2563eb; }
.status-done  { color: #16a34a; background: #f0fdf4; border: 1px solid #bbf7d0; }
.status-done::before  { background: #16a34a; }
@keyframes blink-dot {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.35; }
}

/* ── Section label ── */
.ss-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.6rem;
    font-weight: 500;
    letter-spacing: 0.2em;
    color: #2563eb;
    text-transform: uppercase;
    margin: 1.6rem 0 0.65rem;
    display: flex;
    align-items: center;
    gap: 0.6rem;
}
.ss-label::after {
    content: '';
    flex: 1;
    height: 1px;
    background: #e2e8f0;
}

/* ── Stat cards ── */
.ss-stat-row { display: grid; grid-template-columns: repeat(4,1fr); gap: 1px; background: #e2e8f0; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; margin-top: 1.4rem; }
.ss-stat {
    background: #ffffff;
    padding: 1rem 1.2rem;
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
}
.ss-stat-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.58rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #94a3b8;
}
.ss-stat-val {
    font-family: 'Syne', sans-serif;
    font-size: 2rem;
    font-weight: 700;
    color: #0f172a;
    line-height: 1;
    letter-spacing: -0.03em;
}
.ss-stat-sub { font-size: 0.68rem; color: #cbd5e1; }

/* ── Terminal ── */
.ss-terminal {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    line-height: 1.7;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 1rem 1.2rem;
    color: #475569;
    max-height: 340px;
    overflow-y: auto;
    white-space: pre-wrap;
    word-break: break-word;
    scroll-behavior: smooth;
}
.ss-terminal::-webkit-scrollbar { width: 4px; }
.ss-terminal::-webkit-scrollbar-track { background: transparent; }
.ss-terminal::-webkit-scrollbar-thumb { background: #e2e8f0; border-radius: 2px; }

/* ── Banners ── */
.ss-banner-ok {
    background: #f0fdf4;
    border: 1px solid #bbf7d0;
    border-left: 3px solid #16a34a;
    border-radius: 4px;
    padding: 0.75rem 1rem;
    color: #16a34a;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.73rem;
    letter-spacing: 0.05em;
    margin: 1rem 0;
}
.ss-banner-wait {
    background: #ffffff;
    border: 1px dashed #e2e8f0;
    border-radius: 6px;
    padding: 2rem;
    text-align: center;
    color: #cbd5e1;
    font-size: 0.8rem;
    font-family: 'IBM Plex Mono', monospace;
}

/* ── Streamlit widget overrides ── */

/* Inputs */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 4px !important;
    color: #1a202c !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.8rem !important;
    caret-color: #2563eb !important;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: #2563eb !important;
    box-shadow: 0 0 0 2px rgba(37,99,235,0.1) !important;
}
.stTextInput label, .stTextArea label {
    color: #64748b !important;
    font-size: 0.72rem !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 500 !important;
    letter-spacing: 0.06em !important;
}

/* Date inputs */
.stDateInput > div > div > input {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 4px !important;
    color: #1a202c !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.78rem !important;
}
.stDateInput label {
    color: #64748b !important;
    font-size: 0.72rem !important;
    font-family: 'IBM Plex Mono', monospace !important;
}

/* Slider */
.stSlider > div > div > div > div { background: #2563eb !important; }
.stSlider label {
    color: #64748b !important;
    font-size: 0.72rem !important;
    font-family: 'IBM Plex Mono', monospace !important;
}

/* Checkboxes */
.stCheckbox label {
    color: #475569 !important;
    font-size: 0.78rem !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 400 !important;
    gap: 0.4rem !important;
}
.stCheckbox label:hover { color: #1a202c !important; }

/* Captions */
.stCaption, [data-testid="stCaptionContainer"] {
    color: #94a3b8 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.65rem !important;
}

/* Buttons */
.stButton > button {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 4px !important;
    color: #64748b !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    padding: 0.5rem 1rem !important;
    transition: all 0.15s !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
}
.stButton > button:hover:not(:disabled) {
    border-color: #2563eb !important;
    color: #2563eb !important;
    background: #eff6ff !important;
    box-shadow: 0 1px 4px rgba(37,99,235,0.12) !important;
}
.stButton > button:disabled {
    opacity: 0.4 !important;
    cursor: not-allowed !important;
}

/* Progress bar */
div[data-testid="stProgress"] > div > div {
    background: #2563eb !important;
    border-radius: 0 !important;
}
div[data-testid="stProgress"] > div {
    background: #e2e8f0 !important;
    border-radius: 0 !important;
    height: 2px !important;
}

/* Dataframe */
div[data-testid="stDataFrame"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 6px !important;
    overflow: hidden !important;
    background: white !important;
}

/* Error / Warning */
.stAlert { border-radius: 4px !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.73rem !important; }
"""

st.set_page_config(
    page_title="SkySync Pro",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(f"<style>{_CSS}</style>", unsafe_allow_html=True)
st.markdown(
    '<div class="ss-hero">'
    '<div class="ss-hero-left">'
    '<h1>✦ <span>SKYSYNC</span> PRO</h1>'
    '</div>'
    '<div class="ss-hero-sub">PK · UAE · KSA &nbsp;/&nbsp; non-stop fares &nbsp;/&nbsp; date range &nbsp;/&nbsp; → sheets</div>'
    '</div>',
    unsafe_allow_html=True,
)

# Per-region session state defaults
for _r in ("DOM", "UAE", "KSA"):
    for _k, _v in [
        (f"started_{_r}", False), (f"done_{_r}", False),
        (f"log_{_r}", ""),        (f"rows_{_r}", []),
        (f"pasted_{_r}", []),     (f"unmapped_{_r}", []),
    ]:
        if _k not in st.session_state:
            st.session_state[_k] = _v

# Module-level results dict — written by the background thread, read by the
# Streamlit fragment. This avoids any st.session_state access from a thread,
# which is unsafe on Python 3.14 + recent Streamlit (raises KeyError because
# the ScriptRunContext can't be attached to worker threads cleanly).
THREAD_RESULTS: dict[str, dict] = {}

try:
    spreadsheet_id = st.secrets.get("spreadsheet_id", "")
except Exception:
    spreadsheet_id = ""
if not spreadsheet_id:
    st.warning("Add `spreadsheet_id` to your `.streamlit/secrets.toml`.", icon="⚠️")

# ─────────────────────────────────────────────────────────────────────────────
# Reusable region panel
# ─────────────────────────────────────────────────────────────────────────────
def region_panel(region: str, predefined_sectors: list, default_currency: str = "PKR"):
    """Renders the full UI panel for one region tab."""

    is_running = st.session_state[f"started_{region}"]
    is_done    = st.session_state[f"done_{region}"]

    # Status pill
    if is_done:
        pill = '<span class="status-pill status-done">COMPLETE</span>'
    elif is_running:
        pill = '<span class="status-pill status-scan">SCANNING</span>'
    else:
        pill = '<span class="status-pill status-idle">STANDBY</span>'
    st.markdown(f'<div style="margin-bottom:1rem;">{pill}</div>', unsafe_allow_html=True)

    # ── Sector selection ──────────────────────────────────────────────────────
    st.markdown('<p class="ss-label">Select sectors</p>', unsafe_allow_html=True)

    # Quick-select buttons
    qc1, qc2, _ = st.columns([1, 1, 6])
    with qc1:
        select_all = st.button("✔ All", key=f"all_{region}", disabled=is_running)
    with qc2:
        clear_all  = st.button("✖ None", key=f"none_{region}", disabled=is_running)

    # Checkbox state keys
    cb_key = f"cb_{region}"
    if cb_key not in st.session_state:
        st.session_state[cb_key] = {f"{o}-{d}": False for o, d, _ in predefined_sectors}

    if select_all:
        for k in st.session_state[cb_key]:
            st.session_state[cb_key][k] = True
            st.session_state[f"chk_{region}_{k}"] = True
        st.rerun()
    if clear_all:
        for k in st.session_state[cb_key]:
            st.session_state[cb_key][k] = False
            st.session_state[f"chk_{region}_{k}"] = False
        st.rerun()

    # Render checkboxes in a responsive grid (5 columns)
    n_cols = 5
    cols   = st.columns(n_cols)
    sector_map = {f"{o}-{d}": (o, d, c) for o, d, c in predefined_sectors}

    for idx, (label, (o, d, c)) in enumerate(sector_map.items()):
        with cols[idx % n_cols]:
            checked = st.checkbox(
                label,
                value=st.session_state[cb_key].get(label, False),
                key=f"chk_{region}_{label}",
                disabled=is_running,
            )
            st.session_state[cb_key][label] = checked

    # Manual entry
    st.markdown('<p class="ss-label">Custom routes</p>', unsafe_allow_html=True)

    manual_hint = (
        "e.g.  KHI-MUL, LHE-SKT"
        if region == "DOM"
        else "e.g.  KHI-DXB, LHE-AUH   (add :AED or :SAR to override currency)"
    )
    manual_text = st.text_area(
        "Additional routes (comma or newline separated)",
        placeholder=manual_hint,
        height=80,
        key=f"manual_{region}",
        disabled=is_running,
        label_visibility="collapsed",
    )

    # Build final sector list
    selected_sectors = [
        sector_map[lbl]
        for lbl, checked in st.session_state[cb_key].items()
        if checked
    ]
    manual_sectors = parse_manual_sectors(manual_text, default_currency)
    # Deduplicate: manual routes override checkbox currency for the same route
    seen_keys: set[str] = set()
    final_sectors: list = []
    for s in selected_sectors:
        key = f"{s[0]}-{s[1]}"
        if key not in seen_keys:
            final_sectors.append(s)
            seen_keys.add(key)
    for s in manual_sectors:
        key = f"{s[0]}-{s[1]}"
        if key in seen_keys:
            # Replace existing (preserves manual currency override)
            final_sectors = [s if f"{x[0]}-{x[1]}" != key else s for x in final_sectors]
        else:
            final_sectors.append(s)
            seen_keys.add(key)
    selected_sectors = final_sectors

    st.caption(
        f"**{len(selected_sectors)} sector(s) selected** — "
        + ", ".join(f"{o}-{d}" for o, d, _ in selected_sectors[:10])
        + ("…" if len(selected_sectors) > 10 else "")
    )

    # ── Date range + sheet name + workers ────────────────────────────────────
    st.markdown('<p class="ss-label">Mission control</p>', unsafe_allow_html=True)

    col_dates, col_tab, col_workers, col_btn = st.columns([2, 1.5, 1, 1], gap="large")

    with col_dates:
        today = date.today()

        date_mode = st.radio(
            "Date mode",
            ["Date range", "Specific dates"],
            key=f"date_mode_{region}",
            horizontal=True,
            disabled=is_running,
            label_visibility="collapsed",
        )

        if date_mode == "Date range":
            cs, ce = st.columns(2)
            with cs:
                start_date = st.date_input("Start date", value=today + timedelta(days=1),
                                           min_value=today, key=f"sd_{region}",
                                           disabled=is_running)
            with ce:
                end_date   = st.date_input("End date",   value=today + timedelta(days=7),
                                           min_value=today, key=f"ed_{region}",
                                           disabled=is_running)
            if end_date < start_date:
                st.error("End date must be on or after start date.")
                dates_ok = False
                selected_dates = []
            else:
                selected_dates = date_range(start_date, end_date)
                dates_ok = True
                st.caption(f"📅 {len(selected_dates)} day(s): {selected_dates[0]} → {selected_dates[-1]}")

        else:  # Specific dates
            # Multi date picker via text input (comma-separated)
            specific_key = f"specific_dates_{region}"
            raw_dates = st.text_input(
                "Enter dates (DD-MM-YYYY, comma separated)",
                placeholder="e.g. 10-06-2026, 12-06-2026, 24-06-2026",
                key=specific_key,
                disabled=is_running,
            )
            parsed_specific = []
            parse_errors = []
            if raw_dates.strip():
                for part in raw_dates.replace("\n", ",").split(","):
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        parsed_specific.append(datetime.strptime(part, "%d-%m-%Y").date())
                    except ValueError:
                        parse_errors.append(part)
            if parse_errors:
                st.error(f"Could not parse: {', '.join(parse_errors)} — use DD-MM-YYYY format")
                dates_ok = False
                selected_dates = []
            elif not parsed_specific:
                st.caption("Enter at least one date above.")
                dates_ok = False
                selected_dates = []
            else:
                selected_dates = [d.strftime("%Y-%m-%d") for d in sorted(set(parsed_specific))]
                dates_ok = True
                st.caption(f"📅 {len(selected_dates)} date(s): {', '.join(selected_dates)}")

    with col_tab:
        config_sheet_key = f"config_{region.lower()}_sheet"
        config_sheet     = st.secrets.get(config_sheet_key, f"CONFIG_{region}")
        worksheet_name   = st.text_input(
            "Worksheet tab",
            placeholder="e.g. Jun-2025",
            key=f"ws_{region}",
            disabled=is_running,
        )
        st.caption(f"Config: `{config_sheet}`")

    with col_workers:
        # Slider bounds must be STABLE across reruns — recomputing from
        # `len(selected_sectors)` causes a crash when the user moves the slider
        # 1-by-1 and the checkboxes briefly reduce the sector count.
        # Pin the bounds to the *predefined* sector list size instead.
        _n_predef = max(len(predefined_sectors), 1)
        _max_w = min(_n_predef, 8)
        _wk_key = f"wk_{region}"

        # Defensive clamp: if the stored slider value is now > new max,
        # reset it BEFORE the slider is created.
        if _wk_key in st.session_state and st.session_state[_wk_key] > _max_w:
            st.session_state[_wk_key] = max(1, _max_w)
        if _wk_key not in st.session_state:
            st.session_state[_wk_key] = min(4, _max_w)

        n_workers = st.slider(
            "Workers", min_value=1, max_value=_max_w,
            value=st.session_state[_wk_key], step=1,
            key=_wk_key, disabled=is_running,
        )

    with col_btn:
        st.markdown("<div style='height:1.9rem'></div>", unsafe_allow_html=True)
        can_start = (
            bool(worksheet_name)
            and bool(spreadsheet_id)
            and dates_ok
            and len(selected_sectors) > 0
            and not is_running
        )
        start_btn = st.button("▶ Start scan", key=f"btn_{region}",
                              disabled=not can_start, use_container_width=True)

    # Stats cards
    n_fares = len(st.session_state[f"rows_{region}"])
    st.markdown(
        '<div class="ss-stat-row">'
        + "".join(
            f'<div class="ss-stat"><div class="ss-stat-label">{title}</div>'
            f'<div class="ss-stat-val">{val}</div>'
            f'<div class="ss-stat-sub">{sub}</div></div>'
            for title, val, sub in [
                ("Sectors",  str(len(selected_sectors)), "selected"),
                ("Workers",  str(n_workers),             "parallel"),
                ("Days",     str(len(selected_dates)),   "in range"),
                ("Fares",    str(n_fares),               "captured"),
            ]
        )
        + '</div>',
        unsafe_allow_html=True,
    )

    # ── Start ─────────────────────────────────────────────────────────────────
    if start_btn and not is_running:
        st.session_state[f"started_{region}"] = True
        st.session_state[f"done_{region}"]    = False
        lq = st.session_state[f"lq_{region}"]
        dq = st.session_state[f"dq_{region}"]
        while not lq.empty():
            lq.get()
        while not dq.empty():
            dq.get()
        st.session_state[f"log_{region}"]      = ""
        st.session_state[f"rows_{region}"]     = []
        st.session_state[f"pasted_{region}"]   = []
        st.session_state[f"unmapped_{region}"] = []
        # Clear any stale thread results for this region
        THREAD_RESULTS.pop(region, None)

        thread = threading.Thread(
    target=run_scrape_thread,
    args=(region, selected_sectors, worksheet_name,
          spreadsheet_id, selected_dates, n_workers,
          lq, dq),
    daemon=True,
)
        thread.start()
        st.rerun()

    # ── Live panel ────────────────────────────────────────────────────────────
    @st.fragment(run_every=0.5)
    def _live(region=region):
        lq = st.session_state[f"lq_{region}"]
        dq = st.session_state[f"dq_{region}"]

        if st.session_state[f"started_{region}"]:
            lines = []
            while not lq.empty():
                lines.append(lq.get())
            if lines:
                joined = "".join(lines)
                # Detect the completion sentinel the thread sends in `finally`.
                if "__SCRAPE_DONE__" in joined:
                    joined = joined.replace("__SCRAPE_DONE__\n", "")
                    st.session_state[f"done_{region}"] = True
                    # Sync results from thread -> session_state (only on main thread!)
                    tr = THREAD_RESULTS.get(region)
                    if tr:
                        st.session_state[f"pasted_{region}"]   = tr.get("pasted", [])
                        st.session_state[f"unmapped_{region}"] = tr.get("unmapped", [])
                st.session_state[f"log_{region}"] += joined

            while not dq.empty():
                st.session_state[f"rows_{region}"].append(dq.get())

            if not st.session_state[f"done_{region}"]:
                _dates = selected_dates
                total = len(selected_sectors) * len(_dates)
                done  = len(set(
                    (r["route"], r["date"])
                    for r in st.session_state[f"rows_{region}"]
                ))
                st.progress(
                    min(1.0, done / max(total, 1)),
                    text=f"Scanning… {done}/{total} route-date combinations",
                )

        st.markdown('<p class="ss-label">Telemetry</p>', unsafe_allow_html=True)
        safe = (
            st.session_state[f"log_{region}"]
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        ) or "— ready. configure sectors, date range and worksheet tab, then start scan."
        st.markdown(
            f'<div class="ss-terminal" id="term-{region}">{safe}</div>'
            f"<script>(function(){{var e=document.getElementById('term-{region}');"
            f"if(e)e.scrollTop=e.scrollHeight;}})();</script>",
            unsafe_allow_html=True,
        )

        st.markdown('<p class="ss-label">Live fare matrix</p>', unsafe_allow_html=True)
        rows = st.session_state[f"rows_{region}"]
        if rows:
            st.dataframe(rows, use_container_width=True, hide_index=True,
                column_config={
                    "worker":   st.column_config.NumberColumn("W#",      width="small", format="%d"),
                    "route":    st.column_config.TextColumn("Route",    width="small"),
                    "airline":  st.column_config.TextColumn("Flight",   width="medium"),
                    "fare":     st.column_config.TextColumn("Fare Class"),
                    "date":     st.column_config.TextColumn("Date",     width="medium"),
                    "price":    st.column_config.NumberColumn("Price",  format="%d"),
                    "currency": st.column_config.TextColumn("Cur",      width="small"),
                    "time":     st.column_config.TextColumn("Departure"),
                })
        elif st.session_state[f"started_{region}"] and not st.session_state[f"done_{region}"]:
            st.markdown('<div class="ss-banner-wait">◌ parsing fares…</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="ss-banner-wait">no data yet — configure and start scan.</div>', unsafe_allow_html=True)

        if st.session_state[f"done_{region}"]:
            st.markdown('<div class="ss-banner-ok">✓ COMPLETE — GOOGLE SHEET UPDATED</div>', unsafe_allow_html=True)
            pasted   = st.session_state.get(f"pasted_{region}") or []
            unmapped = st.session_state.get(f"unmapped_{region}") or []
            if pasted:
                st.markdown('<p class="ss-label">Pasted to sheet</p>', unsafe_allow_html=True)
                st.dataframe(pasted, use_container_width=True, hide_index=True)
            if unmapped:
                st.markdown('<p class="ss-label">Not pasted — missing from config</p>', unsafe_allow_html=True)
                st.dataframe(unmapped, use_container_width=True, hide_index=True)
                st.caption(
                    "Add these to your CONFIG sheet: "
                    "Route | Airline | Fare Name | Date (YYYY-MM-DD) | Fare Cell"
                )

    _live()


# ─────────────────────────────────────────────────────────────────────────────
# Main tabs
# ─────────────────────────────────────────────────────────────────────────────
tab_dom, tab_uae, tab_ksa = st.tabs(["🇵🇰  Domestic", "🇦🇪  UAE", "🇸🇦  KSA"])

with tab_dom:
    region_panel("DOM", DOMESTIC_SECTORS, default_currency="PKR")

with tab_uae:
    region_panel("UAE", UAE_SECTORS, default_currency="PKR")

with tab_ksa:
    region_panel("KSA", KSA_SECTORS, default_currency="PKR")
