# gated_page_scraper.py
# =============================================================================
# 【ポートフォリオ提出用・匿名化サンプル / Seleniumのみ】
# - “自分が利用権限を持つ”会員サイトに可視ブラウザでログイン（人手）
# - ログイン後、節度あるQPSで会員ページを巡回し、KPI/テキストをSQLiteへ保存
# - 実サービス名・実URL・実XPathはダミー（特定不可）。やりたいことの骨子を示すための雛形。
#
# ⚠ 重要（法務/コンプラ）:
# - 自動取得やログイン自動化は、多くの媒体/金融サイトで禁止・制限されています。
# - 本スクリプトは学習/ポートフォリオ用途のデモです。第三者サイトでの実運用は禁止。
# - 使う場合は必ず自社/サイトの規約・法令・robots.txt・社内規程の範囲内で。
# - 認証情報やCookieはディスク保存しません。Allowed Originsの外にはアクセスしません。
# =============================================================================

import os
import re
import sys
import time
import sqlite3
import argparse
import datetime
from getpass import getpass
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urljoin

# --- Selenium ---------------------------------------------------------------
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ====== 環境変数（安全スイッチ／匿名化パラメータ） ================================
# 明示オプトイン（未設定なら安全側で停止）
ALLOW_AUTOMATION = os.getenv("ALLOW_AUTOMATION", "0") in ("1", "true", "TRUE", "yes", "YES")
# ID/PW 自動入力を許可（任意。未許可なら人手で入力）
ALLOW_AUTOFILL   = os.getenv("ALLOW_AUTOFILL", "0") in ("1", "true", "TRUE", "yes", "YES")

# 認証情報（任意・デモ用）。ALLOW_AUTOFILL が True の時だけ使用。
USER_ID   = os.getenv("GATED_USER_ID", "")
PASSWORD  = os.getenv("GATED_PASSWORD", "")

# 匿名化したログインURLと基点（相対URL正規化用）
LOGIN_URL   = os.getenv("GATED_LOGIN_URL",   "https://example.com/login")
BASE_ORIGIN = os.getenv("GATED_BASE_ORIGIN", "https://example.com")

# 許可オリジン（カンマ区切り）。一致しないURLは自動スキップ（安全側）。
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("GATED_ALLOWED_ORIGINS", BASE_ORIGIN).split(",") if o.strip()]

# DBパス（ローカルファイル）
DB_PATH = os.getenv("GATED_DB_PATH", os.path.abspath("./gated_data.db"))

# タイミング・スロットリング
SEL_WAIT = int(os.getenv("GATED_SEL_WAIT", "25"))
SHORT_WAIT = int(os.getenv("GATED_SHORT_WAIT", "8"))
TINY_WAIT = int(os.getenv("GATED_TINY_WAIT", "2"))
DEFAULT_QPS = float(os.getenv("GATED_QPS", "0.7"))
DEFAULT_LOGIN_WAIT = int(os.getenv("GATED_LOGIN_WAIT", "60"))

# ====== 画面上の“例示用”XPath（ダミー：目的を伝えるための最小構成） ============
XP: Dict[str, str] = {
    # ナビゲーション例
    "go_detail": '//*[@data-nav="detail"]',
    "go_report": '//*[@data-nav="report"]',
    "go_analysis": '//*[@data-nav="analysis"]',
    "go_perf_popup": '//*[@data-nav="popup"]',
    "go_profile": '//*[@data-nav="profile"]',

    # KPI（テーブルの例：1〜7行目の2列目）※実DOMに合わせて差し替え前提
    "sales_growth":      'string(//*[@id="metrics"]//table//tr[1]/td[2])',
    "op_profit_growth":  'string(//*[@id="metrics"]//table//tr[2]/td[2])',
    "op_margin":         'string(//*[@id="metrics"]//table//tr[3]/td[2])',
    "roe":               'string(//*[@id="metrics"]//table//tr[4]/td[2])',
    "roa":               'string(//*[@id="metrics"]//table//tr[5]/td[2])',
    "equity_ratio":      'string(//*[@id="metrics"]//table//tr[6]/td[2])',
    "dividend_payout":   'string(//*[@id="metrics"]//table//tr[7]/td[2])',

    # 予想KPI（例）
    "sales_growth_f":      'string(//*[@id="metrics"]//table//tr[1]/td[3])',
    "op_profit_growth_f":  'string(//*[@id="metrics"]//table//tr[2]/td[3])',
    "op_margin_f":         'string(//*[@id="metrics"]//table//tr[3]/td[3])',
    "roe_f":               'string(//*[@id="metrics"]//table//tr[4]/td[3])',
    "roa_f":               'string(//*[@id="metrics"]//table//tr[5]/td[3])',
    "equity_ratio_f":      'string(//*[@id="metrics"]//table//tr[6]/td[3])',
    "dividend_payout_f":   'string(//*[@id="metrics"]//table//tr[7]/td[3])',

    # 分析/レーティング（例）
    "score_total":              'string(//*[@id="analysis"]//div[@data-kpi="total"])',
    "score_total_avg":          'string(//*[@id="analysis"]//div[@data-kpi="total_avg"])',
    "score_fin_health":         'string(//*[@id="analysis"]//div[@data-kpi="fin_health"])',
    "score_fin_health_avg":     'string(//*[@id="analysis"]//div[@data-kpi="fin_health_avg"])',
    "score_profit":             'string(//*[@id="analysis"]//div[@data-kpi="profit"])',
    "score_profit_avg":         'string(//*[@id="analysis"]//div[@data-kpi="profit_avg"])',
    "score_cheap":              'string(//*[@id="analysis"]//div[@data-kpi="cheap"])',
    "score_cheap_avg":          'string(//*[@id="analysis"]//div[@data-kpi="cheap_avg"])',
    "score_stable":             'string(//*[@id="analysis"]//div[@data-kpi="stable"])',
    "score_stable_avg":         'string(//*[@id="analysis"]//div[@data-kpi="stable_avg"])',
    "score_momentum":           'string(//*[@id="analysis"]//div[@data-kpi="momentum"])',
    "score_momentum_avg":       'string(//*[@id="analysis"]//div[@data-kpi="momentum_avg"])',
    "target_price":             'string(//*[@id="analysis"]//div[@data-kpi="target_price"])',
    "deviation_value":          'string(//*[@id="analysis"]//div[@data-kpi="dev_value"])',
    "deviation_change":         'string(//*[@id="analysis"]//div[@data-kpi="dev_change"])',

    # 速報/分布（例）
    "flash_weather":            'string(//*[@id="flash"]//span[@data-k="weather"])',
    "profit_progress_period":   'string(//*[@id="flash"]//span[@data-k="period"])',
    "profit_progress_rate":     'string(//*[@id="flash"]//span[@data-k="rate"])',
    "tp_consensus_latest":      'string(//*[@id="consensus"]//span[@data-k="latest"])',
    "tp_consensus_wow":         'string(//*[@id="consensus"]//span[@data-k="wow"])',
    "tp_consensus_deviation":   'string(//*[@id="consensus"]//span[@data-k="deviation"])',
    "rating_dist_latest":       'string(//*[@id="rating"]//span[@data-k="dist_latest"])',

    # レポート本文（テキストの例：任意）
    "company_overview":   '//*[@id="report"]//section[@data-block="overview"]//text()',
    "performance":        'string(//*[@id="report"]//h3[@data-block="performance"])',
    "topics_title":       'string(//*[@id="report"]//h3[@data-block="topics"])',
    "topics_body":        '//*[@id="report"]//div[@data-block="topics"]//text()',
    "risk_title":         'string(//*[@id="report"]//h3[@data-block="risks"])',
    "risk_body":          '//*[@id="report"]//div[@data-block="risks"]//text()',
    "investment_view":    '//*[@id="report"]//div[@data-block="view"]//text()',
}

# ====== 保存する主要フィールド（例示） ===========================================
FIELDS = [
    # 詳細（実績＋予想）
    "sales_growth","op_profit_growth","op_margin","roe","roa","equity_ratio","dividend_payout",
    "sales_growth_f","op_profit_growth_f","op_margin_f","roe_f","roa_f","equity_ratio_f","dividend_payout_f",
    # 分析
    "score_total","score_total_avg",
    "score_fin_health","score_fin_health_avg",
    "score_profit","score_profit_avg",
    "score_cheap","score_cheap_avg",
    "score_stable","score_stable_avg",
    "score_momentum","score_momentum_avg",
    "target_price","deviation_value","deviation_change",
    # 速報/要約
    "flash_weather","profit_progress_period","profit_progress_rate",
    "tp_consensus_latest","tp_consensus_wow","tp_consensus_deviation","rating_dist_latest",
]

# ====== ユーティリティ ==============================================================
def is_allowed(url: str) -> bool:
    return any(url.startswith(o) for o in ALLOWED_ORIGINS)

def normalize_url(u: str) -> str:
    if not u: return ""
    return u if u.startswith("http") else urljoin(BASE_ORIGIN, u)

def pctify(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    return s if s.endswith("%") else f"{s}%"

def polite_sleep(qps: float):
    if qps > 0:
        time.sleep(max(0.05, 1.0 / qps))

def base_for_wait(xp: str) -> str:
    return xp.split("/text()")[0] if "/text()" in xp else xp

def squeeze(s: str) -> str:
    if not s: return ""
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# 画面外へ移動（前面に来ても見えにくくする）/ 代替で最小化
def hide_window(driver):
    try:
        driver.set_window_rect(x=-2000, y=0, width=1100, height=900)
    except Exception:
        try:
            driver.minimize_window()
        except Exception:
            pass

# 短時間の存在チェック
def exists_xpath(driver: webdriver.Chrome, xp: str, t: int = TINY_WAIT) -> bool:
    try:
        WebDriverWait(driver, t).until(EC.presence_of_element_located((By.XPATH, base_for_wait(xp))))
        return True
    except Exception:
        return False

def exists_css(driver: webdriver.Chrome, sel: str, t: int = TINY_WAIT) -> bool:
    try:
        WebDriverWait(driver, t).until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
        return True
    except Exception:
        return False

# 非提供メッセージ検出（例）
UNAVAILABLE_SNIPPETS = [
    "現在提供しておりません","該当するデータはありません","表示できません",
    "not available","no data","temporarily unavailable"
]
def page_has_unavailable_notice(driver: webdriver.Chrome) -> bool:
    try:
        txt = driver.execute_script("return (document.body && (document.body.innerText||'')) || '';") or ""
        txt = txt.replace("\u3000", " ")
        return any(s in txt for s in UNAVAILABLE_SNIPPETS)
    except Exception:
        return False

# ====== Seleniumビルド／ログイン ====================================================
def build_selenium() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    opts.page_load_strategy = "eager"
    # バックグラウンド抑制を極力無効化（非アクティブでも処理が止まりにくくする）
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    # （必要に応じて）ユーザーエージェントを明示
    opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari")
    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_page_load_timeout(SEL_WAIT)
    return driver

def wait_dom(driver: webdriver.Chrome, t: int = SEL_WAIT):
    WebDriverWait(driver, t).until(lambda d: d.execute_script("return document.readyState") in ("interactive","complete"))
    time.sleep(0.2)

def safe_click(driver: webdriver.Chrome, xp: str, pause: float = 0.35, t: int = SHORT_WAIT):
    el = WebDriverWait(driver, t).until(EC.presence_of_element_located((By.XPATH, base_for_wait(xp))))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    try:
        WebDriverWait(driver, t).until(EC.element_to_be_clickable((By.XPATH, base_for_wait(xp))))
        el.click()
    except Exception:
        driver.execute_script("arguments[0].click();", el)
    time.sleep(pause)

def open_popup_and_switch(driver: webdriver.Chrome, xp: str, t: int = SHORT_WAIT) -> Tuple[str, str]:
    parent = driver.current_window_handle
    before = set(driver.window_handles)
    safe_click(driver, xp, pause=0.2, t=t)
    WebDriverWait(driver, t).until(lambda d: len(d.window_handles) > len(before))
    new_h = (set(driver.window_handles) - before).pop()
    driver.switch_to.window(new_h)
    hide_window(driver)
    wait_dom(driver, t)
    return parent, new_h

def close_other_windows(driver: webdriver.Chrome, keep: str):
    for h in list(driver.window_handles):
        if h != keep:
            try:
                driver.switch_to.window(h)
                driver.close()
            except Exception:
                pass
    driver.switch_to.window(keep)

def xstring(driver: webdriver.Chrome, xp: str, t: int = SHORT_WAIT) -> str:
    try:
        WebDriverWait(driver, t).until(EC.presence_of_element_located((By.XPATH, base_for_wait(xp))))
    except Exception:
        return ""
    try:
        return (driver.execute_script("""
            const xp = arguments[0];
            try{
              const r = document.evaluate(xp, document, null, XPathResult.STRING_TYPE, null);
              return (r && r.stringValue) ? r.stringValue.trim() : "";
            }catch(e){ return ""; }
        """, xp) or "").strip()
    except Exception:
        return ""

def xstrings_join(driver: webdriver.Chrome, xp: str, sep: str = " ", t: int = SHORT_WAIT) -> str:
    try:
        WebDriverWait(driver, t).until(EC.presence_of_element_located((By.XPATH, base_for_wait(xp))))
    except Exception:
        return ""
    try:
        arr = driver.execute_script("""
            const xp = arguments[0];
            const SNAP = XPathResult.ORDERED_NODE_SNAPSHOT_TYPE;
            try{
              const res = document.evaluate(xp, document, null, SNAP, null);
              const out = [];
              for(let i=0;i<res.snapshotLength;i++){
                const n = res.snapshotItem(i);
                out.push((n.textContent||"").trim());
              }
              return out;
            }catch(e){ return []; }
        """, xp) or []
        return sep.join([s for s in arr if s])
    except Exception:
        return ""

def qtext(driver: webdriver.Chrome, selectors: List[str]) -> str:
    for sel in selectors:
        try:
            val = driver.execute_script("""
                const sel = arguments[0];
                try{
                  const el = document.querySelector(sel);
                  if(!el) return "";
                  return (el.innerText || el.textContent || "").trim();
                }catch(e){ return ""; }
            """, sel)
            if val:
                return val.strip()
        except Exception:
            pass
    return ""

def qattr(driver: webdriver.Chrome, selectors: List[str], attr: str) -> str:
    for sel in selectors:
        try:
            val = driver.execute_script("""
                const sel = arguments[0], attr = arguments[1];
                try{
                  const el = document.querySelector(sel);
                  if(!el) return "";
                  return el.getAttribute(attr) || "";
                }catch(e){ return ""; }
            """, sel, attr)
            if val:
                return val
        except Exception:
            pass
    return ""

# ====== データ取得ロジック（例） ====================================================
def fetch_details(driver: webdriver.Chrome) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        if exists_xpath(driver, XP["go_detail"], t=TINY_WAIT):
            safe_click(driver, XP["go_detail"], 0.4)
    except Exception:
        pass

    keys = [
        "sales_growth","op_profit_growth","op_margin","roe","roa","equity_ratio","dividend_payout",
        "sales_growth_f","op_profit_growth_f","op_margin_f","roe_f","roa_f","equity_ratio_f","dividend_payout_f",
    ]
    for k in keys:
        v = xstring(driver, XP[k], t=SHORT_WAIT)
        out[k] = pctify(v) if k.endswith(("growth","growth_f","margin","margin_f","roe","roe_f","roa","roa_f","equity_ratio","equity_ratio_f","dividend_payout","dividend_payout_f")) else v
    return out

def fetch_analysis(driver: webdriver.Chrome) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not exists_xpath(driver, XP["go_analysis"], t=TINY_WAIT):
        return out
    try:
        safe_click(driver, XP["go_analysis"], 0.5)
    except Exception:
        return out

    # 例示：単純にXPathから値を拾う
    for k in [
        "score_total","score_total_avg",
        "score_fin_health","score_fin_health_avg",
        "score_profit","score_profit_avg",
        "score_cheap","score_cheap_avg",
        "score_stable","score_stable_avg",
        "score_momentum","score_momentum_avg",
        "target_price","deviation_value","deviation_change",
    ]:
        out[k] = xstring(driver, XP[k], t=SHORT_WAIT)
    if out.get("deviation_change"):
        out["deviation_change"] = pctify(out["deviation_change"])
    return out

def fetch_perf_popup(driver: webdriver.Chrome) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not exists_xpath(driver, XP["go_perf_popup"], t=TINY_WAIT):
        return out
    try:
        parent, child = open_popup_and_switch(driver, XP["go_perf_popup"], t=SHORT_WAIT)
    except Exception:
        return out

    try:
        if page_has_unavailable_notice(driver):
            return out
        for k in ["flash_weather","profit_progress_period","profit_progress_rate",
                  "tp_consensus_latest","tp_consensus_wow","tp_consensus_deviation","rating_dist_latest"]:
            out[k] = xstring(driver, XP[k], t=SHORT_WAIT)
        if out.get("profit_progress_rate"):
            out["profit_progress_rate"] = pctify(out["profit_progress_rate"])
        if out.get("tp_consensus_wow"):
            out["tp_consensus_wow"] = pctify(out["tp_consensus_wow"])
        if out.get("tp_consensus_deviation"):
            out["tp_consensus_deviation"] = pctify(out["tp_consensus_deviation"])
    finally:
        try:
            driver.close()
        except Exception:
            pass
        driver.switch_to.window(parent)
        time.sleep(0.2)
    return out

# ====== DB（匿名スキーマ） ==========================================================
def ensure_tables(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    cur = conn.cursor()

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

    # 取得結果（主要KPI＋速報）
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS gated_detailed_reports (
            target_date TEXT,
            code        TEXT,
            {", ".join(f"{c} TEXT" for c in FIELDS)},
            PRIMARY KEY (target_date, code)
        )
    """)

    # 既存テーブルに不足列があれば追加（将来拡張用）
    cur.execute("PRAGMA table_info(gated_detailed_reports)")
    existing = {row[1] for row in cur.fetchall()}
    for col in FIELDS:
        if col not in existing:
            for i in range(6):
                try:
                    cur.execute(f"ALTER TABLE gated_detailed_reports ADD COLUMN {col} TEXT")
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() or "busy" in str(e).lower():
                        time.sleep(0.5 + i * 0.3)
                        continue
                    raise
    conn.commit()

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone() is not None

def load_targets(conn: sqlite3.Connection, target_date: str, codes: List[str]) -> List[Tuple[str, str]]:
    if not table_exists(conn, "consensus_links"):
        return []
    ph = ",".join(["?"] * len(codes))
    rows = conn.execute(
        f"SELECT code, member_url FROM consensus_links WHERE target_date = ? AND code IN ({ph})",
        [target_date] + codes
    ).fetchall()
    out = []
    for c, u in rows:
        if c and u:
            u2 = normalize_url(str(u))
            if is_allowed(u2):
                out.append((str(c), u2))
    return out

# ====== メイン ======================================================================
def print_policy_banner():
    print("\n" + "="*78)
    print("  【ポリシー/安全バナー】")
    print("- このスクリプトは学習/ポートフォリオ用の雛形です。第三者サイトでの実運用は禁止。")
    print("- 規約・法令・robots.txt・社内規程を順守し、権限のある範囲でのみ使用。")
    print("- 認証情報/Cookieはディスク保存しません。Allowed Origins外は自動スキップ。")
    print("="*78 + "\n")

def main():
    print_policy_banner()

    if not ALLOW_AUTOMATION:
        print("⚠️ 自動化は無効です。環境変数 ALLOW_AUTOMATION=1 を設定して明示的に許可してください。")
        sys.exit(0)

    p = argparse.ArgumentParser(
        description="会員サイトにログイン（人手）→ 節度あるQPSで巡回 → SQLite保存（匿名・規約配慮の雛形）"
    )
    p.add_argument("-a","--target_date", required=True, help="YYYYMMDD")
    p.add_argument("--codes", required=True, help="例: 1234,5678")
    p.add_argument("--qps", type=float, default=DEFAULT_QPS)
    p.add_argument("--login-wait", type=int, default=DEFAULT_LOGIN_WAIT)
    args = p.parse_args()

    # ログインID/パスは基本“人手入力”。（任意で環境変数から自動補助）
    user_id = USER_ID
    password = PASSWORD
    if not (ALLOW_AUTOFILL and USER_ID and PASSWORD):
        print("👤 ログインIDとパスワードを入力してください（パスワードは非表示・保存しません）")
        user_id = input("User ID: ").strip()
        password = getpass("Password: ").strip()
    if not user_id or not password:
        print("❌ ID/パスワードが未入力です。"); sys.exit(1)

    # DB準備・対象解決
    conn = sqlite3.connect(DB_PATH, timeout=60)
    try:
        conn.execute("PRAGMA busy_timeout=60000;")
        ensure_tables(conn)
    except Exception as e:
        print(f"❌ DB初期化に失敗: {e}")
        conn.close(); sys.exit(1)

    # 日付形式チェック
    try:
        datetime.datetime.strptime(args.target_date, "%Y%m%d")
    except ValueError:
        print("❌ target_date は YYYYMMDD 形式で指定してください"); conn.close(); sys.exit(1)

    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    targets = load_targets(conn, args.target_date, codes)
    if not targets:
        print("⚠️ consensus_links から member_url を解決できませんでした（date/codes/ALLOWED_ORIGINS を確認）")
        conn.close(); sys.exit(0)

    # ブラウザ起動＆ログイン
    driver = build_selenium()
    print(f"▶ 取得開始: date={args.target_date} / targets={len(targets)} / mode=sequential / qps={args.qps}")
    print("🌐 ブラウザを起動しログインします（追加認証は手動）")
    try:
        driver.get(LOGIN_URL); wait_dom(driver)
        w = WebDriverWait(driver, SEL_WAIT)

        # 一般的なフォーム要素（例）。サイトにより差し替え必要。
        try:
            uid = w.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username'], input[name='user_id']")))
            pwd = w.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']")))
            uid.clear(); uid.send_keys(user_id)
            pwd.clear(); pwd.send_keys(password)
            # 送信ボタンの例
            btn = w.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")))
            driver.execute_script("arguments[0].click();", btn)
            print("🔐 ログイン送信済み。必要に応じて2段階認証等を完了してください。")
        except Exception:
            print("ℹ️ フォーム自動入力に失敗したため、画面上で手動ログインを続けてください。")

        if args.login_wait > 0:
            print(f"⏳ 認証の完了を待機中…（目安 {args.login_wait} 秒）")
            time.sleep(args.login_wait)

        input("⏸ 会員ページが閲覧可能になったら Enter を押してください… ")
        hide_window(driver)  # 以降の遷移で前面に出にくくする

        cur = conn.cursor()
        insert_cols = ["target_date","code"] + FIELDS
        placeholders = ",".join("?" for _ in insert_cols)
        insert_sql = f"INSERT OR REPLACE INTO gated_detailed_reports ({','.join(insert_cols)}) VALUES ({placeholders})"

        t_all0 = time.perf_counter(); ok = ng = 0
        parent_main = driver.current_window_handle

        for idx, (code, start_url) in enumerate(targets, 1):
            t0 = time.perf_counter()
            print(f"{code} 処理中…")
            try:
                close_other_windows(driver, keep=parent_main)
                driver.get(start_url); wait_dom(driver)

                data: Dict[str, str] = {}
                try: data.update(fetch_details(driver))
                except Exception: pass
                try: data.update(fetch_analysis(driver))
                except Exception: pass
                try: data.update(fetch_perf_popup(driver))
                except Exception: pass

                row = [args.target_date, code] + [data.get(k,"") for k in FIELDS]
                for i in range(6):
                    try:
                        cur.execute(insert_sql, row)
                        conn.commit()
                        break
                    except sqlite3.OperationalError as e:
                        if "locked" in str(e).lower() or "busy" in str(e).lower():
                            time.sleep(0.5 + i * 0.3); continue
                        raise

                dt = time.perf_counter() - t0
                print(f"  ✓ {code} 成功 ({dt:.2f}s) / {idx}/{len(targets)}  ウィンドウ:{len(driver.window_handles)}")
                ok += 1
            except Exception as e:
                dt = time.perf_counter() - t0
                print(f"  ✖ {code} 失敗 ({dt:.2f}s) : {e}")
                ng += 1
            polite_sleep(args.qps)

        dt_all = time.perf_counter() - t_all0
        print(f"🏁 全体完了 / OK:{ok} NG:{ng} / 対象:{len(targets)} / date={args.target_date} / 所要 {dt_all:.2f}s")
        print("👀 ブラウザは開いたままにしています（検証用）。必要なら手動で閉じてください。")

    finally:
        conn.close()
        # 透明性のため、実行後もしばらく画面は残します。driver.quit() は呼ばない。
        pass

if __name__ == "__main__":
    main()
