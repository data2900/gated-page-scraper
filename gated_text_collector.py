import os
import sys
import time
import re
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

# --- Playwright ---
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PwTimeout, Page

# ====== 認証（環境変数で注入：直書き禁止）======
USER_ID = os.getenv("GATED_USER_ID")
PASSWORD = os.getenv("GATED_PASSWORD")
LOGIN_URL = os.getenv("GATED_LOGIN_URL", "https://example.com/login")
if not USER_ID or not PASSWORD:
    raise RuntimeError("環境変数 GATED_USER_ID / GATED_PASSWORD を設定してください。")

# ====== パス ======
DB_PATH = os.getenv("GATED_DB_PATH", os.path.abspath("./market_data.db"))

# ====== 収集挙動 ======
DEFAULT_CONCURRENCY = 3
DEFAULT_QPS = 0.7
DEFAULT_BATCH = 100
NAV_TIMEOUT_MS = 25000
SEL_NAV_TIMEOUT = 25
RETRIES = 3
BASE_DELAY = 0.8
DEFAULT_LOGIN_WAIT = 60

# ====== URLビルダー（サンプルの汎用パス）======
def url_overview(code: str) -> str:
    return f"https://example.com/members/stock/overview?code={code}"

def url_report(code: str) -> str:
    return f"https://example.com/members/stock/report?code={code}"

def url_profile(code: str) -> str:
    return f"https://example.com/members/stock/profile?code={code}"

def url_analysis(code: str) -> str:
    return f"https://example.com/members/stock/analysis?code={code}"

def url_extra(code: str) -> str:
    return f"https://example.net/partners/extra?code={code}"

# ====== XPATH（サンプル想定：目的が伝わる一般的構造）======
XP_OVERVIEW = {
    "company_name": "string(//h1[@data-role='company-name'])",
    "code_label":   "string(//span[@data-role='company-code'])",
    "price_now":    "string(//*[@id='price-area']//span[@data-role='price'])",
}

XP_REPORT = {
    "rating_text":      "string(//*[@id='rating']//p[@data-role='text'])",
    "company_overview": "string(//*[@id='report']//table//tr[1]/td)",
    "performance":      "string(//*[@id='report']//table//tr[2]/td)",
    "topics_title":     "string(//*[@id='topics']//thead//th)",
    "topics_body":      "string(//*[@id='topics']//tbody//tr/td)",
    "risk_title":       "string(//*[@id='risk']//thead//th)",
    "risk_body":        "string(//*[@id='risk']//tbody//tr/td)",
    "investment_view":  "string(//*[@id='insight']//tbody//tr/td)",
}

XP_PROFILE = {
    "shikiho_gaiyo": "string(//*[@id='profile']//table[1]/tbody)",
    "top_holders":   "string(//*[@id='profile']//table[2]//tbody/tr/td[1]/div)",
    "executives":    "string(//*[@id='profile']//table[2]//tbody/tr/td[2]/div)",
}

XP_ANALYSIS = {
    "score_total":        "string(//*[@id='analysis']//table[1]/tbody/tr/td[2]/span[1])",
    "score_total_avg":    "string(//*[@id='analysis']//table[1]/tbody/tr/td[2]/span[2])",
    "score_fin_health":   "string(//*[@id='analysis']//table[2]/tbody/tr[1]/td[2]/div[2])",
    "score_fin_health_s": "string(//*[@id='analysis']//table[2]/tbody/tr[1]/td[2]/div[3])",
    "score_profit":       "string(//*[@id='analysis']//table[2]/tbody/tr[2]/td[2]/div[2])",
    "score_profit_s":     "string(//*[@id='analysis']//table[2]/tbody/tr[2]/td[2]/div[3])",
    "score_cheap":        "string(//*[@id='analysis']//table[2]/tbody/tr[3]/td[2]/div[2])",
    "score_cheap_s":      "string(//*[@id='analysis']//table[2]/tbody/tr[3]/td[2]/div[3])",
    "score_stable":       "string(//*[@id='analysis']//table[2]/tbody/tr[4]/td[2]/div[2])",
    "score_stable_s":     "string(//*[@id='analysis']//table[2]/tbody/tr[4]/td[2]/div[3])",
    "score_momentum":     "string(//*[@id='analysis']//table[2]/tbody/tr[5]/td[2]/div[2])",
    "score_momentum_s":   "string(//*[@id='analysis']//table[2]/tbody/tr[5]/td[2]/div[3])",
    "target_price":       "string(//*[@id='analysis']//table[3]/tbody/tr[1]/td[1]/span[1])",
    "deviation":          "string(//*[@id='analysis']//table[3]/tbody/tr[1]/td[3])",
}

XP_EXTRA = {
    "analyst_comment": "string(//*[@id='extra']//table[1]/tbody/tr/td)",
    "rating_comment":  "string(//*[@id='extra']//table[3]/tbody/tr/td)",
    "tp_consensus":    "string(//*[@id='extra']//table[@data-role='tp']//tr[2]/td[1]/span[1])",
    "tp_wow":          "string(//*[@id='extra']//table[@data-role='tp']//tr[2]/td[2]/span[1])",
    "tp_deviation":    "string(//*[@id='extra']//table[@data-role='tp']//tr[2]/td[3]/span[1])",
    "rating_now":      "string(//*[@id='extra']//table[@data-role='rating']//tr[2]/td[1]/span)",
    "rating_w1":       "string(//*[@id='extra']//table[@data-role='rating']//tr[2]/td[2])",
    "rating_m1":       "string(//*[@id='extra']//table[@data-role='rating']//tr[2]/td[3])",
    "rating_m3":       "string(//*[@id='extra']//table[@data-role='rating']//tr[2]/td[4])",
    "bull":            "string(//*[@id='extra']//table[@data-role='vote']//tr[1]/td[1]/span[1])",
    "slightly_bull":   "string(//*[@id='extra']//table[@data-role='vote']//tr[2]/td[1]/span[1])",
    "neutral":         "string(//*[@id='extra']//table[@data-role='vote']//tr[3]/td[1]/span[1])",
    "slightly_bear":   "string(//*[@id='extra']//table[@data-role='vote']//tr[4]/td[1]/span[1])",
    "bear":            "string(//*[@id='extra']//table[@data-role='vote']//tr[5]/td[1]/span[1])",
}

# ====== 保存カラム順（target_date, code 除く）======
FIELD_ORDER = [
    # 価格/概要
    "company_name","code_label","price_now",
    # レポート
    "rating_text","company_overview","performance","topics_title","topics_body",
    "risk_title","risk_body","investment_view",
    # プロフィール
    "shikiho_gaiyo","top_holders","executives",
    # 分析
    "score_total","score_total_avg","score_fin_health","score_fin_health_s",
    "score_profit","score_profit_s","score_cheap","score_cheap_s",
    "score_stable","score_stable_s","score_momentum","score_momentum_s",
    "target_price","deviation",
    # 追加情報
    "analyst_comment","tp_consensus","tp_wow","tp_deviation",
    "rating_now","rating_w1","rating_m1","rating_m3",
    "bull","slightly_bull","neutral","slightly_bear","bear",
    "rating_comment",
]

# ========= ユーティリティ =========
def _norm_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u3000", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n", s)
    return s.strip()

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

# ========= DB =========
def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sample_text_reports (
            target_date TEXT,
            code        TEXT,

            company_name     TEXT,
            code_label       TEXT,
            price_now        TEXT,

            rating_text      TEXT,
            company_overview TEXT,
            performance      TEXT,
            topics_title     TEXT,
            topics_body      TEXT,
            risk_title       TEXT,
            risk_body        TEXT,
            investment_view  TEXT,

            shikiho_gaiyo    TEXT,
            top_holders      TEXT,
            executives       TEXT,

            score_total      TEXT,
            score_total_avg  TEXT,
            score_fin_health TEXT,
            score_fin_health_s TEXT,
            score_profit     TEXT,
            score_profit_s   TEXT,
            score_cheap      TEXT,
            score_cheap_s    TEXT,
            score_stable     TEXT,
            score_stable_s   TEXT,
            score_momentum   TEXT,
            score_momentum_s TEXT,
            target_price     TEXT,
            deviation        TEXT,

            analyst_comment  TEXT,
            tp_consensus     TEXT,
            tp_wow           TEXT,
            tp_deviation     TEXT,

            rating_now       TEXT,
            rating_w1        TEXT,
            rating_m1        TEXT,
            rating_m3        TEXT,

            bull             TEXT,
            slightly_bull    TEXT,
            neutral          TEXT,
            slightly_bear    TEXT,
            bear             TEXT,

            rating_comment   TEXT,

            PRIMARY KEY (target_date, code)
        )
    """)
    conn.commit()
    # 列の不足は自動ADD
    cur.execute("PRAGMA table_info(sample_text_reports)")
    existing = {row[1] for row in cur.fetchall()}
    to_add = [c for c in FIELD_ORDER if c not in existing]
    for col in to_add:
        cur.execute(f"ALTER TABLE sample_text_reports ADD COLUMN {col} TEXT")
    if to_add:
        conn.commit()

def build_insert_sql() -> str:
    cols = ["target_date", "code"] + FIELD_ORDER
    placeholders = ",".join(["?"] * len(cols))
    return f"INSERT OR REPLACE INTO sample_text_reports ({','.join(cols)}) VALUES ({placeholders})"

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

def load_targets(conn: sqlite3.Connection, target_date: str, mode: str) -> List[str]:
    cur = conn.cursor()
    if mode == "all":
        cur.execute("SELECT code FROM consensus_url WHERE target_date = ?", (target_date,))
        return [r[0] for r in cur.fetchall()]
    cur.execute("""
        SELECT code FROM sample_reports WHERE target_date = ?
        EXCEPT
        SELECT code FROM sample_text_reports WHERE target_date = ?
    """, (target_date, target_date))
    return [r[0] for r in cur.fetchall()]

# ========= Selenium (GUI) =========
def build_selenium():
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("detach", False)
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
        # サンプルの一般的なフィールド名
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "user_id"))).clear()
        driver.find_element(By.NAME, "user_id").send_keys(USER_ID)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "password"))).clear()
        driver.find_element(By.NAME, "password").send_keys(PASSWORD)
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
        ).click()
        print("🔐 ログイン送信済み。必要に応じて人手の追加認証を実施してください。")
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

# ========= Playwright =========
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
            # ドメイン不一致などは破棄（学習用のため厳密対応はしない）
            pass

    try:
        yield context
    finally:
        await context.close()
        await browser.close()

# ------- 汎用 XPath -> テキスト抽出 -------
async def xstr(page: Page, xp: str, timeout: int = 10000) -> str:
    try:
        await page.wait_for_selector(f"xpath={xp}", timeout=timeout)
    except Exception:
        return ""
    await asyncio.sleep(0.15)  # レイアウト安定化
    try:
        val = await page.evaluate("""(xp)=>{
          try{
            const res = document.evaluate(xp, document, null, XPathResult.STRING_TYPE, null);
            return (res && res.stringValue) ? res.stringValue : "";
          }catch(e){ return ""; }
        }""", xp)
        return _norm_text(val)
    except Exception:
        return ""

# ------- 各タブ取得 -------
async def fetch_overview(page: Page, code: str) -> Dict[str, str]:
    await page.goto(url_overview(code), wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await asyncio.sleep(0.2)
    return {
        "company_name": await xstr(page, XP_OVERVIEW["company_name"]),
        "code_label":   await xstr(page, XP_OVERVIEW["code_label"]),
        "price_now":    await xstr(page, XP_OVERVIEW["price_now"]),
    }

async def fetch_report(page: Page, code: str) -> Dict[str, str]:
    await page.goto(url_report(code), wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await asyncio.sleep(0.2)
    return {
        "rating_text":      await xstr(page, XP_REPORT["rating_text"]),
        "company_overview": await xstr(page, XP_REPORT["company_overview"]),
        "performance":      await xstr(page, XP_REPORT["performance"]),
        "topics_title":     await xstr(page, XP_REPORT["topics_title"]),
        "topics_body":      await xstr(page, XP_REPORT["topics_body"]),
        "risk_title":       await xstr(page, XP_REPORT["risk_title"]),
        "risk_body":        await xstr(page, XP_REPORT["risk_body"]),
        "investment_view":  await xstr(page, XP_REPORT["investment_view"]),
    }

async def fetch_profile(page: Page, code: str) -> Dict[str, str]:
    await page.goto(url_profile(code), wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await asyncio.sleep(0.2)
    return {
        "shikiho_gaiyo": await xstr(page, XP_PROFILE["shikiho_gaiyo"]),
        "top_holders":   await xstr(page, XP_PROFILE["top_holders"]),
        "executives":    await xstr(page, XP_PROFILE["executives"]),
    }

async def fetch_analysis(page: Page, code: str) -> Dict[str, str]:
    await page.goto(url_analysis(code), wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await asyncio.sleep(0.25)
    return {
        "score_total":        await xstr(page, XP_ANALYSIS["score_total"]),
        "score_total_avg":    await xstr(page, XP_ANALYSIS["score_total_avg"]),
        "score_fin_health":   await xstr(page, XP_ANALYSIS["score_fin_health"]),
        "score_fin_health_s": await xstr(page, XP_ANALYSIS["score_fin_health_s"]),
        "score_profit":       await xstr(page, XP_ANALYSIS["score_profit"]),
        "score_profit_s":     await xstr(page, XP_ANALYSIS["score_profit_s"]),
        "score_cheap":        await xstr(page, XP_ANALYSIS["score_cheap"]),
        "score_cheap_s":      await xstr(page, XP_ANALYSIS["score_cheap_s"]),
        "score_stable":       await xstr(page, XP_ANALYSIS["score_stable"]),
        "score_stable_s":     await xstr(page, XP_ANALYSIS["score_stable_s"]),
        "score_momentum":     await xstr(page, XP_ANALYSIS["score_momentum"]),
        "score_momentum_s":   await xstr(page, XP_ANALYSIS["score_momentum_s"]),
        "target_price":       await xstr(page, XP_ANALYSIS["target_price"]),
        "deviation":          await xstr(page, XP_ANALYSIS["deviation"]),
    }

async def fetch_extra(page: Page, code: str) -> Dict[str, str]:
    await page.goto(url_extra(code), wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await asyncio.sleep(0.25)
    return {
        "analyst_comment": await xstr(page, XP_EXTRA["analyst_comment"]),
        "tp_consensus":    await xstr(page, XP_EXTRA["tp_consensus"]),
        "tp_wow":          await xstr(page, XP_EXTRA["tp_wow"]),
        "tp_deviation":    await xstr(page, XP_EXTRA["tp_deviation"]),
        "rating_now":      await xstr(page, XP_EXTRA["rating_now"]),
        "rating_w1":       await xstr(page, XP_EXTRA["rating_w1"]),
        "rating_m1":       await xstr(page, XP_EXTRA["rating_m1"]),
        "rating_m3":       await xstr(page, XP_EXTRA["rating_m3"]),
        "bull":            await xstr(page, XP_EXTRA["bull"]),
        "slightly_bull":   await xstr(page, XP_EXTRA["slightly_bull"]),
        "neutral":         await xstr(page, XP_EXTRA["neutral"]),
        "slightly_bear":   await xstr(page, XP_EXTRA["slightly_bear"]),
        "bear":            await xstr(page, XP_EXTRA["bear"]),
        "rating_comment":  await xstr(page, XP_EXTRA["rating_comment"]),
    }

# ------- 1コード処理 -------
async def fetch_all_for_code(page: Page, code: str) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for fn in (fetch_overview, fetch_report, fetch_profile, fetch_analysis, fetch_extra):
        delay = BASE_DELAY
        for attempt in range(RETRIES):
            try:
                chunk = await fn(page, code)
                data.update(chunk)
                break
            except (PwTimeout, Exception):
                if attempt < RETRIES - 1:
                    await asyncio.sleep(delay); delay *= 1.8
    return data

# ------- ワーカー -------
async def worker(ctx, jobs: asyncio.Queue, bucket: TokenBucket, results: asyncio.Queue):
    page = await ctx.new_page()
    try:
        while True:
            item = await jobs.get()
            if item is None:
                break
            target_date, code = item
            await bucket.acquire()
            try:
                d = await fetch_all_for_code(page, code)
                await results.put((target_date, code, d, None))
            except Exception as e:
                await results.put((target_date, code, None, e))
            jobs.task_done()
    finally:
        await page.close()

# ------- 実行メイン -------
async def run(td: str, codes: List[str], qps: float, concurrency: int, batch: int):
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    cur = conn.cursor()

    jobs: asyncio.Queue = asyncio.Queue()
    results: asyncio.Queue = asyncio.Queue()
    total = len(codes)
    for c in codes:
        await jobs.put((td, c))

    done = ok = ng = 0
    buf: List[Tuple] = []
    bucket = TokenBucket(qps)

    # Selenium でログイン→Cookie抽出
    print("🌐 Chrome(GUI) を起動してログインします。")
    driver, ua = build_selenium()
    try:
        site_login_auto(driver, wait_seconds=DEFAULT_LOGIN_WAIT)
        input("⏸ 認証が完了し会員ページが開ける状態になったら Enter を押してください… ")
        cookies = export_cookies_for_playwright(driver)
    finally:
        try: driver.quit()
        except Exception: pass

    if not cookies:
        print("⚠️ Cookie を取得できませんでした。認証未完了の可能性があります。続行は可能ですが失敗する場合があります。")

    insert_sql = build_insert_sql()

    async with async_playwright() as play:
        async with playwright_context(play, ua, cookies) as ctx:
            workers = [asyncio.create_task(worker(ctx, jobs, bucket, results))
                       for _ in range(max(1, min(concurrency, 8)))]

            async def stop_workers():
                for _ in workers:
                    await jobs.put(None)

            try:
                while done < total:
                    td0, code, data, err = await results.get()
                    done += 1
                    if err is None and data is not None:
                        row = [td0, code] + [data.get(k, "") for k in FIELD_ORDER]
                        buf.append(tuple(row)); ok += 1
                        if len(buf) >= batch:
                            cur.executemany(insert_sql, buf); conn.commit(); buf.clear()
                    else:
                        ng += 1

                    if done % 20 == 0 or done == total:
                        print(f"📦 {done}/{total}  OK:{ok}  NG:{ng}")

            finally:
                if buf:
                    cur.executemany(insert_sql, buf); conn.commit()
                conn.close()
                print(f"🏁 完了 / OK:{ok} NG:{ng} / 対象:{total} / date={td}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("-a", "--target_date", help="YYYYMMDD（未指定なら consensus_url の最新日付）")
    p.add_argument("--mode", choices=["all", "missing"], default="all")
    p.add_argument("--qps", type=float, default=DEFAULT_QPS)
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    p.add_argument("--code", help="単体テスト用：このコードだけ取得（例: 1234）")
    args = p.parse_args()

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    td = resolve_target_date(conn, args.target_date)
    if not td:
        print("❌ target_date を決定できません（-a YYYYMMDD を指定するか、consensus_url にデータが必要）")
        conn.close(); sys.exit(1)

    if args.code:
        codes = [args.code]
    else:
        codes = load_targets(conn, td, args.mode)
        if not codes:
            if args.mode == "missing":
                print(f"✅ {td} 未取得はありません（mode=missing）")
            else:
                print(f"⚠️ {td} 対象コードが見つかりません（mode=all）")
            conn.close(); return
    conn.close()

    print(f"▶ 取得開始: date={td} / codes={codes if len(codes)<=5 else codes[:5]+['...']} / concurrency={args.concurrency} / qps={args.qps}")
    asyncio.run(run(td, codes, args.qps, args.concurrency, args.batch))

if __name__ == "__main__":
    main()
