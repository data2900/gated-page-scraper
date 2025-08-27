import os
import sys
import time
import random
import sqlite3
import argparse
import datetime
from typing import Dict, Any, List, Tuple, Optional
from contextlib import asynccontextmanager

# --- Selenium (GUI) for login ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Playwright for scraping ---
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PwTimeout, Page

# ====== 認証（環境変数）======
USER_ID = os.getenv("GATED_USER_ID")
PASSWORD = os.getenv("GATED_PASSWORD")
LOGIN_URL = os.getenv("GATED_LOGIN_URL", "https://example.com/login")

if not USER_ID or not PASSWORD:
    raise RuntimeError("環境変数 GATED_USER_ID / GATED_PASSWORD を設定してください。")

# ====== パス ======
DB_PATH = os.getenv("GATED_DB_PATH", os.path.abspath("./market_data.db"))

# ====== 収集挙動 ======
DEFAULT_CONCURRENCY = 2
DEFAULT_QPS = 0.7
DEFAULT_BATCH = 100
NAV_TIMEOUT_MS = 25000
SEL_NAV_TIMEOUT = 25
RETRIES = 3
BASE_DELAY = 0.8
DEFAULT_LOGIN_WAIT = 60

# ====== 取得対象（サンプルの一般的なXPath）=====
# ※ 実サイト依存ではなく、目的が伝わる最小構成
XPATH_MAP: Dict[str, str] = {
    "sales_growth":     "string(//*[@id='metrics']//table//tr[1]/td[2])",
    "op_profit_growth": "string(//*[@id='metrics']//table//tr[2]/td[2])",
    "op_margin":        "string(//*[@id='metrics']//table//tr[3]/td[2])",
    "roe":              "string(//*[@id='metrics']//table//tr[4]/td[2])",
    "roa":              "string(//*[@id='metrics']//table//tr[5]/td[2])",
    "equity_ratio":     "string(//*[@id='metrics']//table//tr[6]/td[2])",
    "dividend_payout":  "string(//*[@id='metrics']//table//tr[7]/td[2])",
}

# ====== ユーティリティ ======
def pct(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "N/A"
    return s if s.endswith("%") else (s + "%")

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

# ====== DB ======
def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    # 会員ページの指標スナップショット
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sample_reports (
            target_date TEXT,
            code        TEXT,
            sales_growth TEXT,
            op_profit_growth TEXT,
            op_margin TEXT,
            roe TEXT,
            roa TEXT,
            equity_ratio TEXT,
            dividend_payout TEXT,
            PRIMARY KEY (target_date, code)
        )
    """)
    conn.commit()

def resolve_target_date(conn: sqlite3.Connection, explicit: Optional[str]) -> Optional[str]:
    if explicit:
        datetime.datetime.strptime(explicit, "%Y%m%d")
        return explicit
    cur = conn.cursor()
    cur.execute("SELECT MAX(target_date) FROM consensus_url")
    row = cur.fetchone()
    td = row[0] if row else None
    if td:
        datetime.datetime.strptime(td, "%Y%m%d")
        return td
    return None

def load_targets(conn: sqlite3.Connection, target_date: str, mode: str) -> List[Tuple[str, str]]:
    """
    consensus_url(target_date, code, name, link_a, link_b, link_c) を前提に、
    会員ページに相当するURLを link_c として参照するサンプル実装。
    """
    cur = conn.cursor()
    if mode == "all":
        cur.execute("SELECT code, link_c FROM consensus_url WHERE target_date = ?", (target_date,))
        return [(c, u) for c, u in cur.fetchall() if u]

    # missing: 未保存のみ
    cur.execute("""
        SELECT code FROM consensus_url WHERE target_date = ?
        EXCEPT
        SELECT code FROM sample_reports WHERE target_date = ?
    """, (target_date, target_date))
    codes = [r[0] for r in cur.fetchall()]
    if not codes:
        return []
    ph = ",".join(["?"] * len(codes))
    cur.execute(f"""
        SELECT code, link_c FROM consensus_url
        WHERE target_date = ? AND code IN ({ph})
    """, [target_date] + codes)
    return [(c, u) for c, u in cur.fetchall() if u]

# ====== Selenium (GUI) ログイン ======
def build_selenium() -> Tuple[webdriver.Chrome, str]:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("detach", False)  # Enter後に自動クローズ
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = "eager"
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"user-agent={ua}")
    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_page_load_timeout(SEL_NAV_TIMEOUT)
    return driver, ua

def site_login_auto(driver: webdriver.Chrome, wait_seconds: int):
    driver.get(LOGIN_URL)
    try:
        # 例：ID/PW の一般的な入力欄（サンプルのためID/名前はダミー）
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "user_id"))).clear()
        driver.find_element(By.NAME, "user_id").send_keys(USER_ID)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "password"))).clear()
        driver.find_element(By.NAME, "password").send_keys(PASSWORD)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type=submit]"))).click()
        print("🔐 ログイン送信済み。必要な追加認証があれば人手で実施してください。")
    except Exception as e:
        print(f"❌ ログイン操作失敗: {e}")
        return
    if wait_seconds > 0:
        time.sleep(wait_seconds)

def export_cookies_for_playwright(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    raw = driver.get_cookies()
    cookies: List[Dict[str, Any]] = []
    for c in raw:
        cookies.append({
            "name": c.get("name"),
            "value": c.get("value"),
            "domain": c.get("domain") or ".example.com",
            "path": c.get("path", "/"),
            "expires": c.get("expiry", -1),
            "httpOnly": bool(c.get("httpOnly", False)),
            "secure": bool(c.get("secure", True)),
            "sameSite": "Lax",
        })
    return cookies

# ====== Playwright ======
@asynccontextmanager
async def playwright_context(play, user_agent: str, seed_cookies: List[Dict[str, Any]]):
    browser = await play.chromium.launch(
        headless=True,
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
    )
    context = await browser.new_context(
        user_agent=user_agent,
        java_script_enabled=True,
        bypass_csp=True,
        viewport={"width": 1366, "height": 768}
    )
    # 軽量化: 画像/フォント/メディア遮断
    async def route_handler(route, request):
        if request.resource_type in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", route_handler)

    if seed_cookies:
        try:
            await context.add_cookies(seed_cookies)
        except Exception:
            # ドメイン不一致などは破棄（学習用のため厳密対応は割愛）
            pass

    try:
        yield context
    finally:
        await context.close()
        await browser.close()

async def fetch_one(page: Page, code: str, url: str) -> Tuple[str, Dict[str, Any]]:
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    # 指標の親要素が描画されるまで待つ（ダミー）
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
    for k in list(data.keys()):
        data[k] = pct(data.get(k, ""))
    return code, data

async def worker(ctx, jobs: asyncio.Queue, bucket: TokenBucket, results: asyncio.Queue):
    page = await ctx.new_page()
    try:
        while True:
            item = await jobs.get()
            if item is None:
                break
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
                     target_date: str, qps: float, concurrency: int, batch: int):
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    cur = conn.cursor()

    jobs: asyncio.Queue = asyncio.Queue()
    results: asyncio.Queue = asyncio.Queue()
    for t in targets:
        await jobs.put(t)
    total = len(targets)
    done = ok = ng = 0
    buf: List[Tuple] = []
    bucket = TokenBucket(qps)

    async with async_playwright() as play:
        async with playwright_context(play, ua, cookies) as ctx:
            workers = [asyncio.create_task(worker(ctx, jobs, bucket, results))
                       for _ in range(max(1, min(concurrency, 6)))]

            async def stop_workers():
                for _ in workers:
                    await jobs.put(None)

            try:
                while done < total:
                    code, data, err = await results.get()
                    done += 1
                    if err is None and data:
                        row = (
                            target_date, code,
                            data.get("sales_growth",""), data.get("op_profit_growth",""),
                            data.get("op_margin",""), data.get("roe",""), data.get("roa",""),
                            data.get("equity_ratio",""), data.get("dividend_payout","")
                        )
                        buf.append(row); ok += 1
                        if len(buf) >= batch:
                            cur.executemany("""
                                INSERT OR REPLACE INTO sample_reports (
                                    target_date, code, sales_growth, op_profit_growth,
                                    op_margin, roe, roa, equity_ratio, dividend_payout
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, buf)
                            conn.commit(); buf.clear()
                    else:
                        ng += 1
                    if done % 50 == 0 or done == total:
                        print(f"📦 {done}/{total}  OK:{ok}  NG:{ng}")
            finally:
                if buf:
                    cur.executemany("""
                        INSERT OR REPLACE INTO sample_reports (
                            target_date, code, sales_growth, op_profit_growth,
                            op_margin, roe, roa, equity_ratio, dividend_payout
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, buf)
                    conn.commit()
                await stop_workers()
                await asyncio.gather(*workers, return_exceptions=True)
                conn.close()
                print(f"🏁 完了 / OK:{ok} NG:{ng} / 対象:{total}")

# ====== メイン ======
def main():
    p = argparse.ArgumentParser()
    p.add_argument("-a", "--target_date", help="YYYYMMDD（未指定なら consensus_url の最新日付）")
    p.add_argument("--mode", choices=["all", "missing"], default="missing",
                   help="all: consensus_url 全件 / missing: 未取得のみ")
    p.add_argument("--qps", type=float, default=DEFAULT_QPS, help="全体QPS上限（0.6〜0.9推奨）")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Playwright並列（2〜3推奨）")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="DBコミット間隔")
    p.add_argument("--login-wait", type=int, default=DEFAULT_LOGIN_WAIT, help="自動送信後に待つ秒数（人手認証の目安）")
    p.add_argument("--headful", action="store_true", help="（デバッグ）Playwrightも表示")
    args = p.parse_args()

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    target_date = resolve_target_date(conn, args.target_date)
    if not target_date:
        print("❌ target_date を決定できません（-a YYYYMMDD を指定するか、consensus_url にデータが必要）")
        conn.close(); sys.exit(1)

    cur = conn.cursor()
    targets = load_targets(conn, target_date, args.mode)
    if not targets:
        msg = "未取得はありません（mode=missing）" if args.mode == "missing" else "対象URLが見つかりません（mode=all）"
        print(f"ℹ️ {target_date} {msg}")
        conn.close(); return
    conn.close()

    # 1) Selenium(GUI) ログイン → Enter で自動クローズ
    print("🌐 ブラウザ(GUI)を起動してログインします。")
    driver, ua = build_selenium()
    try:
        site_login_auto(driver, wait_seconds=args.login_wait)
        input("⏸ 認証が完了し会員ページが開ける状態になったら Enter を押してください… ")
        cookies = export_cookies_for_playwright(driver)
    finally:
        try: driver.quit()
        except Exception: pass

    if not cookies:
        print("⚠️ Cookie を取得できませんでした。認証未完了の可能性があります。続行は可能ですが失敗する場合があります。")

    # 2) Playwright で収集
    print(f"▶ 取得開始: mode={args.mode} date={target_date} / concurrency={args.concurrency} qps={args.qps}")
    asyncio.run(run_scrape(
        targets=targets,
        ua=ua,
        cookies=cookies,
        target_date=target_date,
        qps=args.qps,
        concurrency=args.concurrency,
        batch=args.batch
    ))

if __name__ == "__main__":
    main()
