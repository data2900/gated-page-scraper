# gated_page_scraper.py
# =============================================================================
# 【ポートフォリオ提出用・匿名化済みサンプル】
# - 会員制サイト（自分が利用権限を持つサイト）に可視ブラウザでログイン
# - セッションCookieをPlaywrightへ受け渡し、節度あるQPSでメンバーページを巡回
# - 実サイト・社名・銘柄・URL・XPathはすべてダミー（特定不可）
# - 規約順守のための安全装置（明示オプトイン、許可オリジン、スロットリング、最小ログ等）
#
# ⚠ 重要:
# - 自動化や取得はサイトの利用規約・法令・robots等に従い、権限がある範囲でのみ実施してください。
# - 本コードは「やりたいこと・目的」を示すための雛形です。実運用前に必ず法務/コンプラ確認を。
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

# --- Selenium（人手ログイン用の可視ブラウザ） -----------------------------------
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Playwright（ログイン後の効率的な取得） ---------------------------------------
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PwTimeout, Page

# ====== 環境変数（明示オプトイン & 匿名） =========================================
# 自動化の明示許諾（未設定なら安全側で停止）
ALLOW_AUTOMATION = os.getenv("ALLOW_AUTOMATION", "0") in ("1", "true", "TRUE", "yes", "YES")
# ID/PWの自動入力を許可（任意。未許可なら人手で入力）
ALLOW_AUTOFILL   = os.getenv("ALLOW_AUTOFILL", "0") in ("1", "true", "TRUE", "yes", "YES")

# 認証情報（任意・ダミー）。ALLOW_AUTOFILL が True の時だけ使用。
USER_ID   = os.getenv("GATED_USER_ID", "")
PASSWORD  = os.getenv("GATED_PASSWORD", "")

# 対象サイト（ダミー）。具体名は出さない。
LOGIN_URL   = os.getenv("GATED_LOGIN_URL",   "https://example.com/login")
BASE_ORIGIN = os.getenv("GATED_BASE_ORIGIN", "https://example.com")  # 相対URLの正規化用

# DBパス（ローカルファイル）
DB_PATH = os.getenv("GATED_DB_PATH", os.path.abspath("./portfolio_demo.db"))

# 実行パラメータ（保守的な既定値）
DEFAULT_CONCURRENCY = int(os.getenv("GATED_CONCURRENCY", "2"))
DEFAULT_QPS         = float(os.getenv("GATED_QPS", "0.6"))          # 0.6 req/sec
DEFAULT_BATCH       = int(os.getenv("GATED_BATCH", "100"))
NAV_TIMEOUT_MS      = int(os.getenv("GATED_NAV_TIMEOUT_MS", "25000"))
SEL_NAV_TIMEOUT     = int(os.getenv("GATED_SEL_NAV_TIMEOUT", "25"))
RETRIES             = int(os.getenv("GATED_RETRIES", "3"))
BASE_DELAY          = float(os.getenv("GATED_BASE_DELAY", "0.8"))
DEFAULT_LOGIN_WAIT  = int(os.getenv("GATED_LOGIN_WAIT", "60"))

# 許可オリジン（カンマ区切り）。一致しないURLはスキップ（安全側）。
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("GATED_ALLOWED_ORIGINS", BASE_ORIGIN).split(",") if o.strip()]

# ====== 取得対象（ダミーXPath：目的共有のための最小構成） ===========================
# 会員ページ上で、KPIやテキストブロックを拾う例。実DOMに合わせて差し替え前提。
XPATH_MAP: Dict[str, str] = {
    # KPI例
    "sales_growth":     "string(//*[@id='metrics']//table//tr[1]/td[2])",
    "op_profit_growth": "string(//*[@id='metrics']//table//tr[2]/td[2])",
    "op_margin":        "string(//*[@id='metrics']//table//tr[3]/td[2])",
    "roe":              "string(//*[@id='metrics']//table//tr[4]/td[2])",
    "roa":              "string(//*[@id='metrics']//table//tr[5]/td[2])",
    "equity_ratio":     "string(//*[@id='metrics']//table//tr[6]/td[2])",
    "dividend_payout":  "string(//*[@id='metrics']//table//tr[7]/td[2])",
    # テキストブロック例
    "overview_text":    "string(//*[@id='report']//div[@data-block='overview'])",
    "topics_text":      "string(//*[@id='report']//div[@data-block='topics'])",
    "risks_text":       "string(//*[@id='report']//div[@data-block='risks'])",
}

# ====== ユーティリティ ==============================================================
def pct(s: str) -> str:
    """末尾%が無ければ付与（例示的整形）"""
    s = (s or "").strip()
    if not s:
        return ""
    return s if s.endswith("%") else (s + "%")

def squeeze_ws(s: str) -> str:
    """全角空白含む連続空白を1スペースへ圧縮"""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.replace("\u3000", " ")).strip()

def is_allowed(url: str) -> bool:
    """許可オリジン以外は取得対象外（安全側）"""
    return any(url.startswith(origin) for origin in ALLOWED_ORIGINS)

def redact(s: str) -> str:
    """ログ用の簡易マスキング（PIIや長数字を伏せる）"""
    if not s:
        return ""
    s = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "[redacted@email]", s)
    s = re.sub(r"\b\d{10,}\b", "[redacted-number]", s)
    return s

class TokenBucket:
    """グローバルQPS制御（素朴なトークンバケット）"""
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

# ====== DB =========================================================================
def ensure_tables(conn: sqlite3.Connection):
    """スキーマ作成（匿名の一般名）"""
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()

    # 取得対象リンク（例：date, code, name, member_url）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS consensus_links (
            target_date TEXT,
            code        TEXT,
            name        TEXT,
            member_url  TEXT,
            PRIMARY KEY (target_date, code)
        )
    """)

    # スナップショット（KPI＋テキスト）
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
    """指定なければ consensus_links の最新日付を使用"""
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

def load_targets(conn: sqlite3.Connection, target_date: str, mode: str) -> List[Tuple[str, str]]:
    """
    consensus_links(target_date, code, member_url) 前提。
      - mode='all'     : 全件
      - mode='missing' : まだ gated_snapshots に無いもの
    許可オリジンでフィルタ。
    """
    cur = conn.cursor()
    if mode == "all":
        cur.execute("SELECT code, member_url FROM consensus_links WHERE target_date = ?", (target_date,))
        return [(c, u) for c, u in cur.fetchall() if u and is_allowed(u)]

    cur.execute("""
        SELECT code FROM consensus_links WHERE target_date = ?
        EXCEPT
        SELECT code FROM gated_snapshots WHERE target_date = ?
    """, (target_date, target_date))
    codes = [r[0] for r in cur.fetchall()]
    if not codes:
        return []
    ph = ",".join(["?"] * len(codes))
    cur.execute(f"""
        SELECT code, member_url FROM consensus_links
        WHERE target_date = ? AND code IN ({ph})
    """, [target_date] + codes)
    return [(c, u) for c, u in cur.fetchall() if u and is_allowed(u)]

# ====== Selenium（人手ログイン） ====================================================
def build_selenium() -> Tuple[webdriver.Chrome, str]:
    """可視ブラウザ起動（detach=True で終了後も画面を残す）"""
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("detach", True)  # ユーザー確認のため開いたまま
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.page_load_strategy = "eager"
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"user-agent={ua}")
    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_page_load_timeout(SEL_NAV_TIMEOUT)
    return driver, ua

def site_login(driver: webdriver.Chrome, wait_seconds: int):
    """
    人手ログインが前提。ALLOW_AUTOFILL と認証情報があれば最小限補助。
    具体サイト名は出さない。要素セレクタは一般的な例。
    """
    driver.get(LOGIN_URL)
    print("🌐 ログインページを開きました。ブラウザ上で認証を完了してください。")

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
                    print("🔐 認証情報を送信しました（ALLOW_AUTOFILL）。追加認証があれば人手で実施してください。")
                except Exception:
                    pass
        except Exception:
            pass

    if wait_seconds > 0:
        print(f"⏳ 手動認証のため {wait_seconds} 秒ほど待機します。")
        time.sleep(wait_seconds)

    input("⏸ 会員ページへ遷移できる状態になったら Enter を押してください… ")

def export_cookies_for_playwright(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    """SeleniumのCookieをPlaywrightへ受け渡し（ドメイン/パスは拡張しない）"""
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

# ====== Playwright（取得ワーカー） ==================================================
@asynccontextmanager
async def playwright_context(play, user_agent: str, seed_cookies: List[Dict[str, Any]], headful: bool):
    """Cookieを投入したコンテキストを生成。画像/フォントは遮断して軽量化。"""
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
            # クロスサイト属性等は本デモでは無視
            pass

    try:
        yield context
    finally:
        await context.close()
        await browser.close()

async def fetch_one(page: Page, code: str, url: str) -> Tuple[str, Dict[str, Any]]:
    """1件取得：ダミーXPathでKPIとテキストを採取"""
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_selector("xpath=//*[@id='metrics']", timeout=NAV_TIMEOUT_MS)  # 親要素のレンダ待ち
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
    # 整形（%付与・空白圧縮）
    for k, v in list(data.items()):
        if k in ("sales_growth","op_profit_growth","op_margin","roe","roa","equity_ratio","dividend_payout"):
            data[k] = pct(v)
        else:
            data[k] = squeeze_ws(v)
    return code, data

async def worker(ctx, jobs: asyncio.Queue, bucket: TokenBucket, results: asyncio.Queue):
    """並列ワーカー（保守的リトライ＋QPS制御）"""
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
                     target_date: str, qps: float, concurrency: int, batch: int, headful: bool):
    """Playwrightで対象を巡回してDBへ蓄積"""
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
        async with playwright_context(play, ua, cookies, headful=headful) as ctx:
            workers = [asyncio.create_task(worker(ctx, jobs, bucket, results))
                       for _ in range(max(1, min(concurrency, 4)))]

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
                print(f"🏁 完了 / OK:{ok} NG:{ng} / 対象:{total}")

# ====== メイン ======================================================================
def print_policy_banner():
    print("\n" + "="*78)
    print("  【ポリシー/安全バナー】")
    print("- 権限があるサイトのみで利用（第三者サイトの規約・法令・robots等を順守）。")
    print("- ログインは可視ブラウザで人手確認。自動入力は明示オプトイン時のみ。")
    print("- 本スクリプトは認証情報/PII/Cookieをディスク保存しません。")
    print("- スロットリング・保守的なリトライで過負荷を回避します。")
    print("="*78 + "\n")

def main():
    print_policy_banner()

    p = argparse.ArgumentParser(
        description="会員サイトにログイン→節度あるQPSでメンバーページを巡回しスナップショット保存（匿名・規約配慮の雛形）"
    )
    p.add_argument("-a", "--target_date", help="YYYYMMDD（未指定は consensus_links の最新日付）")
    p.add_argument("--mode", choices=["all", "missing"], default="missing",
                   help="all: 該当日の全コード / missing: 未取得のみ")
    p.add_argument("--qps", type=float, default=DEFAULT_QPS, help="全体QPS（0.5〜0.9推奨）")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Playwright並列（1〜4推奨）")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="DBコミット間隔")
    p.add_argument("--login-wait", type=int, default=DEFAULT_LOGIN_WAIT, help="初回ログイン後に待機する秒数（人手認証用の目安）")
    p.add_argument("--headful", action="store_true", help="Playwrightも可視化（デモ/デバッグ）")
    args = p.parse_args()

    if not ALLOW_AUTOMATION:
        print("⚠️ 自動化は無効です。環境変数 ALLOW_AUTOMATION=1 を設定して明示的に許可してください。")
        sys.exit(0)

    # DBとターゲット解決
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    target_date = resolve_target_date(conn, args.target_date)
    if not target_date:
        print("❌ target_date を決定できません。-a YYYYMMDD を指定するか、consensus_links を事前に投入してください。")
        conn.close(); sys.exit(1)

    targets = load_targets(conn, target_date, args.mode)
    if not targets:
        msg = "未取得はありません（mode=missing）" if args.mode == "missing" else "対象URLが見つかりません（mode=all）"
        print(f"ℹ️ {target_date} {msg}")
        conn.close(); return
    conn.close()

    # 1) Seleniumで人手ログイン（ブラウザは開いたまま）
    print("🌐 Seleniumで可視ブラウザを起動してログインします。")
    driver, ua = build_selenium()
    try:
        site_login(driver, wait_seconds=args.login_wait)
        cookies = export_cookies_for_playwright(driver)
        if not cookies:
            print("⚠️ Cookieを取得できませんでした。未認証の可能性があります（続行可だが失敗する場合あり）。")
    finally:
        # デモのためブラウザは開いたまま（透明性確保）。ここでは quit しない。
        pass

    # 2) Playwrightで節度ある巡回取得
    print(f"▶ 取得開始: mode={args.mode} date={target_date} / concurrency={args.concurrency} qps={args.qps}")
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

    print("\n🧹 Seleniumのログインウィンドウは可視のままです。作業完了後に手動で閉じてください。")
    print("   ※ 本スクリプトは認証情報・Cookie・個人情報をディスクへ保存しません。")

if __name__ == "__main__":
    main()
