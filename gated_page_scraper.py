# gated_page_scraper.py
# =============================================================================
# 【Portfolio-safe / Anonymous Example】
# - Login to a gated member site with a visible browser (human-in-the-loop)
# - Hand off the authenticated session (cookies) to Playwright for polite crawling
# - No real brand/site names; URLs/XPath are placeholders
# - Compliance-by-default: explicit opt-in, robots.txt enforcement, origin allowlist,
#   QPS throttling, minimal logs with PII redaction, no secrets persisted
# =============================================================================

import os
import re
import sys
import time
import sqlite3
import argparse
import datetime
from typing import Dict, Any, List, Tuple, Optional
from contextlib import asynccontextmanager
from urllib.parse import urlparse

# --- Selenium: visible browser for human login --------------------------------
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Playwright: efficient post-login fetching --------------------------------
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PwTimeout, Page

# --- robots.txt (compliance) ---------------------------------------------------
import urllib.robotparser as robotparser

# ====== Environment (explicit opt-in, all anonymous) ===========================
ALLOW_AUTOMATION = os.getenv("ALLOW_AUTOMATION", "0") in ("1","true","TRUE","yes","YES")
ALLOW_AUTOFILL   = os.getenv("ALLOW_AUTOFILL", "0") in ("1","true","TRUE","yes","YES")
ROBOTS_ENFORCE   = os.getenv("ROBOTS_ENFORCE", "1") not in ("0","false","FALSE","no","NO")

USER_ID   = os.getenv("GATED_USER_ID", "")
PASSWORD  = os.getenv("GATED_PASSWORD", "")

LOGIN_URL   = os.getenv("GATED_LOGIN_URL",   "https://example.com/login")
BASE_ORIGIN = os.getenv("GATED_BASE_ORIGIN", "https://example.com")  # for relative normalization

DB_PATH = os.getenv("GATED_DB_PATH", os.path.abspath("./portfolio_demo.db"))

DEFAULT_CONCURRENCY = int(os.getenv("GATED_CONCURRENCY", "2"))
DEFAULT_QPS         = float(os.getenv("GATED_QPS", "0.6"))
DEFAULT_BATCH       = int(os.getenv("GATED_BATCH", "100"))
NAV_TIMEOUT_MS      = int(os.getenv("GATED_NAV_TIMEOUT_MS", "25000"))
SEL_NAV_TIMEOUT     = int(os.getenv("GATED_SEL_NAV_TIMEOUT", "25"))
RETRIES             = int(os.getenv("GATED_RETRIES", "3"))
BASE_DELAY          = float(os.getenv("GATED_BASE_DELAY", "0.8"))
DEFAULT_LOGIN_WAIT  = int(os.getenv("GATED_LOGIN_WAIT", "60"))

ALLOWED_ORIGINS = [o.strip() for o in os.getenv("GATED_ALLOWED_ORIGINS", BASE_ORIGIN).split(",") if o.strip()]

# ====== Dummy XPaths: only to convey intent ===================================
XPATH_MAP: Dict[str, str] = {
    # KPI examples
    "sales_growth":     "string(//*[@id='metrics']//table//tr[1]/td[2])",
    "op_profit_growth": "string(//*[@id='metrics']//table//tr[2]/td[2])",
    "op_margin":        "string(//*[@id='metrics']//table//tr[3]/td[2])",
    "roe":              "string(//*[@id='metrics']//table//tr[4]/td[2])",
    "roa":              "string(//*[@id='metrics']//table//tr[5]/td[2])",
    "equity_ratio":     "string(//*[@id='metrics']//table//tr[6]/td[2])",
    "dividend_payout":  "string(//*[@id='metrics']//table//tr[7]/td[2])",
    # Text blocks
    "overview_text":    "string(//*[@id='report']//div[@data-block='overview'])",
    "topics_text":      "string(//*[@id='report']//div[@data-block='topics'])",
    "risks_text":       "string(//*[@id='report']//div[@data-block='risks'])",
}

# ====== Utils =================================================================
def pct(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    return s if s.endswith("%") else (s + "%")

def squeeze_ws(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s.replace("\u3000", " ")).strip()

def redact(s: str) -> str:
    if not s: return ""
    s = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "[redacted@email]", s)
    s = re.sub(r"\b\d{10,}\b", "[redacted-number]", s)
    return s

def is_allowed_origin(url: str) -> bool:
    return any(url.startswith(origin) for origin in ALLOWED_ORIGINS)

# ---- robots.txt ---------------------------------------------------------------
_robots_cache: Dict[str, robotparser.RobotFileParser] = {}

def robots_allowed(url: str, user_agent: str) -> bool:
    try:
        parts = urlparse(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        rp = _robots_cache.get(origin)
        if not rp:
            rp = robotparser.RobotFileParser()
            rp.set_url(origin + "/robots.txt")
            rp.read()  # network call; portfolio demo is fine
            _robots_cache[origin] = rp
        return rp.can_fetch(user_agent or "*", url)
    except Exception:
        # Safe default: disallow if robots cannot be fetched and enforcement is on
        return not ROBOTS_ENFORCE

class TokenBucket:
    def __init__(self, qps: float):
        self.interval = 1.0 / max(qps, 0.0001)
        self._lock = asyncio.Lock()
        self._next = time.monotonic()
    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self.interval

# ====== DB ====================================================================
def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()

    # Links to visit (generic schema)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS consensus_links (
            target_date TEXT,
            code        TEXT,
            name        TEXT,
            member_url  TEXT,
            PRIMARY KEY (target_date, code)
        )
    """)

    # Snapshots (KPI + texts)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gated_snapshots (
            target_date TEXT,
            code        TEXT,
            sales_growth TEXT,
            op_profit_growth TEXT,
            op_margin TEXT,
            roe TEXT,
            roa TEXT,
            equity_ratio TEXT,
            dividend_payout TEXT,
            overview_text TEXT,
            topics_text   TEXT,
            risks_text    TEXT,
            PRIMARY KEY (target_date, code)
        )
    """)
    conn.commit()

def resolve_target_date(conn: sqlite3.Connection, explicit: Optional[str]) -> Optional[str]:
    if explicit:
        datetime.datetime.strptime(explicit, "%Y%m%d")
        return explicit
    cur = conn.cursor()
    cur.execute("SELECT MAX(target_date) FROM consensus_links")
    row = cur.fetchone()
    td = row[0] if row else None
    if td:
        datetime.datetime.strptime(td, "%Y%m%d")
        return td
    return None

def load_targets(conn: sqlite3.Connection, target_date: str, mode: str, user_agent: str) -> List[Tuple[str, str]]:
    cur = conn.cursor()
    if mode == "all":
        cur.execute("SELECT code, member_url FROM consensus_links WHERE target_date = ?", (target_date,))
        rows = cur.fetchall()
    else:
        cur.execute("""
            SELECT code FROM consensus_links WHERE target_date = ?
            EXCEPT
            SELECT code FROM gated_snapshots WHERE target_date = ?
        """, (target_date, target_date))
        codes = [r[0] for r in cur.fetchall()]
        if not codes: return []
        ph = ",".join(["?"] * len(codes))
        cur.execute(f"""
            SELECT code, member_url FROM consensus_links
            WHERE target_date = ? AND code IN ({ph})
        """, [target_date] + codes)
        rows = cur.fetchall()

    out: List[Tuple[str, str]] = []
    for c, u in rows:
        if not u: continue
        if not is_allowed_origin(u): 
            continue
        if ROBOTS_ENFORCE and not robots_allowed(u, user_agent):
            print(f"🤖 robots.txt により取得対象外: {u}")
            continue
        out.append((c, u))
    return out

# ====== Selenium: human login =================================================
def build_selenium() -> Tuple[webdriver.Chrome, str]:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("detach", True)  # keep window open for transparency
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = "eager"
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"user-agent={ua}")
    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_page_load_timeout(SEL_NAV_TIMEOUT)
    return driver, ua

def site_login(driver: webdriver.Chrome, wait_seconds: int):
    driver.get(LOGIN_URL)
    print("🌐 Opened login page. Please complete authentication in the visible browser.")

    if ALLOW_AUTOMATION and ALLOW_AUTOFILL and USER_ID and PASSWORD:
        try:
            uid = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username'], input[name='user_id']"))
            )
            pwd = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
            )
            uid.clear(); uid.send_keys(USER_ID)
            pwd.clear(); pwd.send_keys(PASSWORD)
            btns = driver.find_elements(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            if btns:
                try:
                    btns[0].click()
                    print("🔐 Submitted credentials (ALLOW_AUTOFILL). Complete any MFA in the browser.")
                except Exception:
                    pass
        except Exception:
            pass

    if wait_seconds > 0:
        print(f"⏳ Waiting ~{wait_seconds}s for manual auth…")
        time.sleep(wait_seconds)

    input("⏸ Press Enter once the member pages are accessible… ")

def export_cookies_for_playwright(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    cookies: List[Dict[str, Any]] = []
    for c in driver.get_cookies():
        cookies.append({
            "name": c.get("name"),
            "value": c.get("value"),
            "domain": c.get("domain"),
            "path": c.get("path", "/"),
            "expires": c.get("expiry", -1),
            "httpOnly": bool(c.get("httpOnly", False)),
            "secure": bool(c.get("secure", True)),
            "sameSite": "Lax",
        })
    return cookies

# ====== Playwright workers =====================================================
@asynccontextmanager
async def playwright_context(play, user_agent: str, seed_cookies: List[Dict[str, Any]], headful: bool):
    browser = await play.chromium.launch(
        headless=not headful,
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
    )
    context = await browser.new_context(
        user_agent=user_agent,
        java_script_enabled=True,
        bypass_csp=True,
        viewport={"width": 1366, "height": 768}
    )

    async def route_handler(route, request):
        if request.resource_type in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", route_handler)

    if seed_cookies:
        try:
            await context.add_cookies([c for c in seed_cookies if c.get("domain")])
        except Exception:
            pass

    try:
        yield context
    finally:
        await context.close()
        await browser.close()

async def fetch_one(page: Page, code: str, url: str) -> Tuple[str, Dict[str, Any]]:
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_selector("xpath=//*[@id='metrics']", timeout=NAV_TIMEOUT_MS)
    data = await page.evaluate(
        """(xps)=>{
            const out = {};
            const get = (xp) => {
              try { return document.evaluate(xp, document, null, XPathResult.STRING_TYPE, null).stringValue.trim(); }
              catch(e){ return ""; }
            };
            for (const [k, xp] of Object.entries(xps)) out[k] = get(xp);
            return out;
        }""",
        XPATH_MAP
    )
    for k, v in list(data.items()):
        if k in ("sales_growth","op_profit_growth","op_margin","roe","roa","equity_ratio","dividend_payout"):
            data[k] = pct(v)
        else:
            data[k] = squeeze_ws(v)
    return code, data

async def worker(ctx, jobs: asyncio.Queue, bucket: TokenBucket, results: asyncio.Queue):
    page = await ctx.new_page()
    try:
        while True:
            item = await jobs.get()
            if item is None: break
            code, url = item
            await bucket.acquire()
            delay = BASE_DELAY
            last_err = None
            for attempt in range(RETRIES):
                try:
                    c, d = await fetch_one(page, code, url)
                    await results.put((c, d, None))
                    break
                except (PwTimeout, Exception) as e:
                    last_err = e
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(delay); delay *= 1.8
                    else:
                        await results.put((code, None, last_err))
            jobs.task_done()
    finally:
        await page.close()

async def run_scrape(targets: List[Tuple[str, str]], ua: str, cookies: List[Dict[str, Any]],
                     target_date: str, qps: float, concurrency: int, batch: int, headful: bool):
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    cur = conn.cursor()

    jobs: asyncio.Queue = asyncio.Queue()
    results: asyncio.Queue = asyncio.Queue()
    for t in targets: await jobs.put(t)

    total = len(targets)
    done = ok = ng = 0
    buf: List[Tuple] = []
    bucket = TokenBucket(qps)

    async with async_playwright() as play:
        async with playwright_context(play, ua, cookies, headful=headful) as ctx:
            workers = [asyncio.create_task(worker(ctx, jobs, bucket, results))
                       for _ in range(max(1, min(concurrency, 4)))]

            async def stop_workers():
                for _ in workers: await jobs.put(None)

            try:
                while done < total:
                    code, data, err = await results.get()
                    done += 1
                    if err is None and data:
                        row = (
                            target_date, code,
                            data.get("sales_growth",""), data.get("op_profit_growth",""),
                            data.get("op_margin",""), data.get("roe",""), data.get("roa",""),
                            data.get("equity_ratio",""), data.get("dividend_payout",""),
                            data.get("overview_text",""), data.get("topics_text",""), data.get("risks_text",""),
                        )
                        buf.append(row); ok += 1
                        if len(buf) >= batch:
                            cur.executemany("""
                                INSERT OR REPLACE INTO gated_snapshots (
                                    target_date, code,
                                    sales_growth, op_profit_growth, op_margin, roe, roa, equity_ratio, dividend_payout,
                                    overview_text, topics_text, risks_text
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, buf)
                            conn.commit(); buf.clear()
                    else:
                        ng += 1
                        err_msg = redact(str(err)) if err else ""
                        print(f"  ✖ {code}: {err_msg}")
                    if done % 50 == 0 or done == total:
                        print(f"📦 {done}/{total}  OK:{ok}  NG:{ng}")
            finally:
                if buf:
                    cur.executemany("""
                        INSERT OR REPLACE INTO gated_snapshots (
                            target_date, code,
                            sales_growth, op_profit_growth, op_margin, roe, roa, equity_ratio, dividend_payout,
                            overview_text, topics_text, risks_text
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, buf)
                    conn.commit()
                await stop_workers()
                await asyncio.gather(*workers, return_exceptions=True)
                conn.close()
                print(f"🏁 Done / OK:{ok} NG:{ng} / Total:{total}")

# ====== Main ==================================================================
def print_policy_banner():
    print("\n" + "="*78)
    print("  Policy & Safety")
    print("- Use only on sites you are authorized to access and within ToS & law.")
    print("- Human verifies login in a visible browser; autofill only with explicit opt-in.")
    print("- No credentials or cookies are persisted to disk by this script.")
    print("- Throttling & conservative retries to avoid undue load.")
    print("- robots.txt is enforced by default; disallowed URLs are skipped.")
    print("="*78 + "\n")

def main():
    print_policy_banner()

    p = argparse.ArgumentParser(
        description="Login to a gated site (human-in-the-loop) and take polite snapshots of member pages (anonymized demo)."
    )
    p.add_argument("-a", "--target_date", help="YYYYMMDD (defaults to latest in consensus_links)")
    p.add_argument("--mode", choices=["all", "missing"], default="missing",
                   help="all: all codes on the date / missing: only those not in gated_snapshots")
    p.add_argument("--qps", type=float, default=DEFAULT_QPS, help="global QPS (0.5–0.9 recommended)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Playwright parallelism (1–4)")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="rows per DB commit")
    p.add_argument("--login-wait", type=int, default=DEFAULT_LOGIN_WAIT, help="seconds to wait for manual auth")
    p.add_argument("--headful", action="store_true", help="show Playwright window (demo/debug)")
    args = p.parse_args()

    if not ALLOW_AUTOMATION:
        print("⚠️ Automation disabled. Set ALLOW_AUTOMATION=1 to explicitly allow.")
        sys.exit(0)

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    # Visible browser login (Selenium)
    print("🌐 Launching visible browser for human login…")
    driver, ua = build_selenium()
    try:
        site_login(driver, wait_seconds=args.login_wait)
        cookies = export_cookies_for_playwright(driver)
        if not cookies:
            print("⚠️ No cookies exported; you may not be authenticated (continuing, but requests may fail).")
    finally:
        # keep the window open for transparency; do not quit here
        pass

    # Resolve targets after we know UA (for robots)
    target_date = resolve_target_date(conn, args.target_date)
    if not target_date:
        print("❌ Could not resolve target_date. Provide -a YYYYMMDD or seed consensus_links.")
        conn.close(); sys.exit(1)
    targets = load_targets(conn, target_date, args.mode, ua)
    if not targets:
        msg = "no missing URLs" if args.mode == "missing" else "no URLs found"
        print(f"ℹ️ {target_date} {msg} (after origin & robots filtering)")
        conn.close(); return
    conn.close()

    # Polite crawl with Playwright
    print(f"▶ Starting: mode={args.mode} date={target_date} / concurrency={args.concurrency} qps={args.qps}")
    asyncio.run(run_scrape(
        targets=targets,
        ua=ua,
        cookies=cookies,
        target_date=target_date,
        qps=args.qps,
        concurrency=args.concurrency,
        batch=args.batch,
        headful=args.headful
    ))

    print("\n🧹 The visible login window remains open for transparency. Close it manually when done.")
    print("   (This script does not persist credentials/cookies/PII to disk.)")

if __name__ == "__main__":
    main()
