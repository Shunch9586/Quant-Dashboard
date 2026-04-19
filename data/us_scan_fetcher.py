"""
US 全市場掃描器

資料來源：
  - 股票清單：NASDAQ FTP（免費，NYSE / NASDAQ / AMEX 全覆蓋）
  - 價格歷史：yfinance 批次下載（免費，無需 API Key）
  - 產業分類：Tiingo metadata（有 TIINGO_API_KEY 時啟用，本地快取）

輸出：/tmp/us_scan_latest.parquet（固定檔名，附日期 metadata）
有效期：US_SCAN_MAX_AGE_DAYS 天內不重下（週末 / 假日自動沿用）
"""

import logging
import time
from datetime import date, timedelta
from io import StringIO
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd
import requests

import config
from data.features import compute_features

logger = logging.getLogger(__name__)

# ── 常數 ─────────────────────────────────────────────────
US_SCAN_LOCAL     = Path("/tmp/us_scan_latest.parquet")      # 固定檔名（不含日期）
US_SCAN_DATE_FILE = Path("/tmp/us_scan_latest_date.txt")     # 記錄上次掃描日期
US_IND_CACHE      = Path("/tmp/us_industry_cache.parquet")   # 産業快取（跨日保存）
US_SCAN_MAX_AGE_DAYS = 3   # 允許最多 3 天不重下（涵蓋週末 + 假日）

TIINGO_TICKERS_URL = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
TIINGO_META_URL    = "https://api.tiingo.com/tiingo/daily/{ticker}"
SEC_TICKERS_URL    = "https://www.sec.gov/files/company_tickers.json"
NASDAQ_LISTED_URL  = "https://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
OTHER_LISTED_URL   = "https://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"

BATCH_SIZE        = 100      # yfinance 每批張數（100 降低單批 timeout 機率）
MAX_SYMBOLS       = 7000     # 上限（避免記憶體不足）
MIN_PRICE         = 1.0      # 過濾低價（放寬至 $1，避免誤殺正常股）
MIN_AVG_VOLUME    = 50_000   # 過濾低流動性（放寬至 5 萬，涵蓋中小型股）
MAX_IND_FETCH     = 2000     # Tiingo 每次最多補充多少産業資訊

# yfinance 抓取品質最佳的主要美國交易所
MAJOR_US_EXCHANGES = {"NASDAQ", "NYSE", "NYSE MKT", "AMEX", "NYSE ARCA"}


# ════════════════════════════════════════════════════════
# 公開介面
# ════════════════════════════════════════════════════════

def us_scan_is_fresh() -> bool:
    """
    US scan 是否在有效期內（US_SCAN_MAX_AGE_DAYS 天）。
    週末 / 假日自動沿用最近一次掃描結果，不強制每日重下。
    """
    if not US_SCAN_LOCAL.exists() or not US_SCAN_DATE_FILE.exists():
        return False
    try:
        scan_date = date.fromisoformat(US_SCAN_DATE_FILE.read_text().strip())
        return (date.today() - scan_date).days <= US_SCAN_MAX_AGE_DAYS
    except Exception:
        return False


def get_us_scan_date() -> Optional[str]:
    """回傳上次掃描日期字串，供 UI 顯示用"""
    if US_SCAN_DATE_FILE.exists():
        try:
            return US_SCAN_DATE_FILE.read_text().strip()
        except Exception:
            pass
    return None


def run_us_daily_update(progress_cb: Optional[Callable] = None) -> tuple[bool, str]:
    """
    快速每日更新：用 Tiingo IEX 批次取最新報價，更新 close / daily_change。
    前提：US_SCAN_LOCAL 已存在（全市場掃描跑過至少一次）。
    通常 < 30 秒，不需重跑 yfinance。
    """
    def _p(msg: str) -> None:
        logger.info(msg)
        if progress_cb:
            progress_cb(msg)

    if not US_SCAN_LOCAL.exists():
        return False, "尚無完整掃描資料，請先執行「全市場掃描」"

    api_key = config.fresh("TIINGO_API_KEY")
    if not api_key:
        return False, "TIINGO_API_KEY 未設定"

    try:
        _p("📂 載入現有掃描資料...")
        scan_df = pd.read_parquet(str(US_SCAN_LOCAL))
        symbols = scan_df["symbol"].tolist()
        n_batches = (len(symbols) + 499) // 500
        _p(f"   {len(symbols)} 支（{n_batches} 個 IEX 請求）")

        _p("⚡ Tiingo IEX 批次取最新報價...")
        from data.tiingo_utils import iex_batch_prices
        prices = iex_batch_prices(symbols, api_key)
        _p(f"   取得 {len(prices)} 支最新報價")

        if not prices:
            return False, "IEX 回傳空資料，請確認 TIINGO_API_KEY 與 Power plan 狀態"

        # 更新 close / daily_change / date
        _p("🔄 更新報價與漲跌幅...")
        updated = 0
        daily_changes = []

        for idx in range(len(scan_df)):
            sym  = scan_df.at[idx, "symbol"]
            info = prices.get(sym)
            if info is None:
                daily_changes.append(None)
                continue

            new_price  = info["lastPrice"]
            prev_close = info.get("prevClose") or float(scan_df.at[idx, "close"])
            daily_chg  = (new_price - prev_close) / prev_close if prev_close > 0 else 0.0

            scan_df.at[idx, "close"] = round(new_price, 2)
            scan_df.at[idx, "date"]  = date.today()
            if info.get("volume"):
                scan_df.at[idx, "volume"] = info["volume"]

            daily_changes.append(round(daily_chg * 100, 2))   # 轉為 %
            updated += 1

        scan_df["daily_change_pct"] = daily_changes

        scan_df.to_parquet(str(US_SCAN_LOCAL), index=False, engine="pyarrow")
        US_SCAN_DATE_FILE.write_text(date.today().isoformat())
        _p(f"💾 更新完成（{updated} 支有新報價）")

        return True, f"今日報價更新完成：{updated}/{len(symbols)} 支"

    except Exception as e:
        logger.exception("US daily update 失敗")
        return False, f"發生錯誤：{e}"


def run_us_scan(fs=None, progress_cb: Optional[Callable] = None) -> tuple[bool, str]:
    """
    執行 US 全市場掃描，結果存至 /tmp/us_scan_{today}.parquet。
    fs    : s3fs FileSystem（供未來 S3 讀取用，目前可為 None）
    progress_cb: 進度回調，簽名 (message: str) -> None
    回傳 (成功?, 訊息)
    """
    def _p(msg: str) -> None:
        logger.info(msg)
        if progress_cb:
            progress_cb(msg)

    try:
        # ① API key 診斷
        tiingo_key = config.fresh("TIINGO_API_KEY")
        if tiingo_key:
            _p("🔑 Tiingo API key 已設定（將使用 Tiingo 歷史資料）")
        else:
            _p("⚠️  Tiingo API key 未找到，將改用 yfinance（Streamlit Cloud 可能受限）")

        # ② 股票清單
        _p("📋 取得全市場股票清單（Tiingo → SEC → NASDAQ FTP）...")
        symbols = _get_us_symbols()
        _p(f"   取得 {len(symbols)} 支（過濾 ETF / 低流動性後）")

        if not symbols:
            return False, "無法取得美股清單（Tiingo / SEC / NASDAQ FTP 均失敗）"

        # ③ 下載 + 即時計算特徵（streaming pipeline，不存原始 price data）
        src = "Tiingo" if tiingo_key else "yfinance"
        _p(f"📈 下載 + 特徵計算 {len(symbols)} 支（{src}，低記憶體模式）...")
        scan_rows = _download_and_scan(symbols, _p, tiingo_key)
        _p(f"   完成：{len(scan_rows)} 支有效資料")

        if not scan_rows:
            return False, f"{src} 回傳空資料，請確認 API key 並稍後再試"

        scan_df = pd.DataFrame(scan_rows)

        # ④ 補充産業資訊
        _p("🏷️  補充産業分類（Tiingo，優先高分股）...")
        scan_df = _enrich_industry(scan_df, max_fetch=MAX_IND_FETCH, progress_cb=_p)

        # ⑤ 儲存（固定路徑 + 日期記錄）
        scan_df.to_parquet(str(US_SCAN_LOCAL), index=False, engine="pyarrow")
        US_SCAN_DATE_FILE.write_text(date.today().isoformat())
        _p(f"💾 儲存至本地快取（{len(scan_df)} 支，有效期 {US_SCAN_MAX_AGE_DAYS} 天）")

        return True, f"美股掃描完成：{len(scan_df)} 支（全市場覆蓋）"

    except Exception as e:
        logger.exception("US scan 失敗")
        return False, f"發生錯誤：{e}"


# ════════════════════════════════════════════════════════
# ① 股票清單（三層 fallback）
# ════════════════════════════════════════════════════════

def _get_us_symbols() -> list[str]:
    """
    取得全市場股票清單（只取主要美國交易所，確保 yfinance 覆蓋率接近 100%）。
    依序嘗試三個來源，若 Tiingo < 3000 支則補充 SEC EDGAR。
    """
    symbols: set[str] = set()

    # 1. Tiingo ZIP（最乾淨，有 exchange + endDate 可過濾）
    tiingo = _try_tiingo_tickers()
    symbols.update(tiingo)
    logger.info(f"Tiingo 貢獻：{len(tiingo)} 支")

    # 若 Tiingo 不足，補充 SEC EDGAR
    if len(symbols) < 3000:
        sec = _try_sec_tickers()
        before = len(symbols)
        symbols.update(sec)
        logger.info(f"SEC EDGAR 補充：{len(symbols) - before} 支")

    # 若還是不足，再嘗試 NASDAQ FTP
    if len(symbols) < 3000:
        ftp = _try_nasdaq_ftp()
        before = len(symbols)
        symbols.update(ftp)
        logger.info(f"NASDAQ FTP 補充：{len(symbols) - before} 支")

    if not symbols:
        logger.error("所有股票清單來源均失敗")
        return []

    result = sorted(symbols)[:MAX_SYMBOLS]
    logger.info(f"最終股票清單：{len(result)} 支")
    return result


def _try_tiingo_tickers() -> list[str]:
    """
    Tiingo supported_tickers.zip。
    只取主要美國交易所 + 在架（無 endDate）的普通股，
    確保 yfinance 對這批股票有近 100% 覆蓋率。
    """
    import zipfile
    import io as _io

    try:
        resp = requests.get(TIINGO_TICKERS_URL, timeout=60)
        resp.raise_for_status()

        with zipfile.ZipFile(_io.BytesIO(resp.content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(csv_name) as f:
                df = pd.read_csv(f)

        # 正規化欄位名稱（防止大小寫差異）
        df.columns = [c.strip() for c in df.columns]

        # endDate：Tiingo 對在架股票可能填 NaN / "" / "2100-01-01" 等未來日期
        # 只排除已確定下市（過去日期）的股票
        today_ts = pd.Timestamp.now().normalize()
        end_dates = pd.to_datetime(df["endDate"], errors="coerce")
        still_trading = end_dates.isna() | (end_dates > today_ts)

        mask = (
            (df["assetType"] == "Stock") &                          # 只要普通股
            (df["priceCurrency"] == "USD") &                        # 美元計價
            (df["exchange"].isin(MAJOR_US_EXCHANGES)) &             # 主要美國交易所
            (df["ticker"].str.match(r"^[A-Z]{1,5}$", na=False)) &  # 純英文 1-5 碼
            still_trading                                           # 未下市
        )
        syms = df[mask]["ticker"].dropna().unique().tolist()
        logger.info(
            f"Tiingo ticker filter: {len(df)} total → {mask.sum()} passed "
            f"(exchange={df['exchange'].isin(MAJOR_US_EXCHANGES).sum()}, "
            f"active={still_trading.sum()}, stock={( df['assetType']=='Stock').sum()})"
        )
        return sorted(syms)

    except Exception as e:
        logger.warning(f"Tiingo supported_tickers 失敗：{e}")
        return []


def _try_sec_tickers() -> list[str]:
    """
    SEC EDGAR company_tickers.json：所有 SEC 登記的上市公司。
    免費、穩定，但不含 exchange 資訊。
    """
    try:
        resp = requests.get(
            SEC_TICKERS_URL,
            headers={"User-Agent": "quant-dashboard research@example.com"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        syms = [
            v["ticker"].upper()
            for v in data.values()
            if str(v.get("ticker", "")).strip()
        ]
        # 過濾純英文字母 1–5 碼
        syms = [s for s in syms if __import__("re").match(r"^[A-Z]{1,5}$", s)]
        return sorted(set(syms))[:MAX_SYMBOLS]

    except Exception as e:
        logger.warning(f"SEC EDGAR 失敗：{e}")
        return []


def _try_nasdaq_ftp() -> list[str]:
    """NASDAQ FTP（備用，Streamlit Cloud 可能被 IP 封鎖）"""
    symbols: set[str] = set()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}

    for url, sym_col, exchange_col, exchange_vals in [
        (NASDAQ_LISTED_URL, "Symbol",     None,       None),
        (OTHER_LISTED_URL,  "ACT Symbol", "Exchange", ["A", "N", "P", "Q"]),
    ]:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text), sep="|")
            if sym_col not in df.columns:
                continue
            mask = (
                (df.get("ETF",        pd.Series(["N"] * len(df))) != "Y") &
                (df.get("Test Issue", pd.Series(["N"] * len(df))) != "Y") &
                (df[sym_col].str.match(r"^[A-Z]{1,5}$", na=False))
            )
            if exchange_col and exchange_vals:
                mask &= df.get(exchange_col, pd.Series(["N"] * len(df))).isin(exchange_vals)
            symbols.update(df[mask][sym_col].dropna().tolist())
        except Exception as e:
            logger.warning(f"NASDAQ FTP ({url}) 失敗：{e}")

    return sorted(symbols)[:MAX_SYMBOLS]


# ════════════════════════════════════════════════════════
# ② 批次下載價格（yfinance）
# ════════════════════════════════════════════════════════

# yfinance 不同版本 Close 欄位名稱不一（0.2.50+ 改用 "Price"）
_CLOSE_KEYS = ("Close", "Price", "Adj Close")


def _pick_close(frame) -> Optional[pd.Series]:
    """從 DataFrame 或 dict-like 中找到第一個非空的 Close 欄位"""
    for key in _CLOSE_KEYS:
        try:
            val = frame.get(key)
            if val is not None:
                return val
        except Exception:
            pass
    return None


def _extract_close_vol(
    raw: pd.DataFrame,
    sym: str,
    batch_len: int,
) -> tuple[Optional[pd.Series], Optional[pd.Series]]:
    """
    從 yfinance 下載結果取出單一 ticker 的 Close / Volume Series。

    yfinance 不同版本可能回傳：
      A. flat DataFrame — 單 ticker 或批量中只剩 1 支有資料
      B. MultiIndex (field, ticker) — group_by="column"（預設，舊版 field="Close"）
      C. MultiIndex (field, ticker) — 新版 field 改名為 "Price"
      D. MultiIndex (ticker, field) — group_by="ticker"
    所有路徑都以 _CLOSE_KEYS 嘗試，不寫死 "Close"。
    """
    cols = raw.columns

    # ── A. 無 MultiIndex（flat）──────────────────────────
    if not isinstance(cols, pd.MultiIndex):
        if batch_len == 1:
            close = _pick_close(raw)
            vol   = raw.get("Volume")
            return (close, vol) if close is not None else (None, None)
        # 批量回傳 flat → 只剩 1 支有資料但無法判斷是哪支，跳過
        return None, None

    lvl0_set = set(cols.get_level_values(0))

    # ── B/C. (field, ticker)：某個 close key 在 level-0 ──
    close_key = next((k for k in _CLOSE_KEYS if k in lvl0_set), None)
    if close_key:
        close_frame = raw[close_key]   # DataFrame，columns = tickers
        vol_frame   = raw.get("Volume")
        if isinstance(close_frame, pd.Series):
            # 整個 batch 只有 1 支有資料
            return (close_frame, vol_frame) if batch_len == 1 else (None, None)
        close = close_frame.get(sym)
        vol   = (
            vol_frame.get(sym)
            if vol_frame is not None and isinstance(vol_frame, pd.DataFrame)
            else None
        )
        return close, vol

    # ── D. (ticker, field)：ticker 在 level-0 ────────────
    if sym in lvl0_set:
        sym_df = raw[sym]
        close  = _pick_close(sym_df)
        vol    = sym_df.get("Volume")
        return close, vol

    # ── E. 保底：level-1 掃描 ────────────────────────────
    if sym in set(cols.get_level_values(1)):
        sym_df = raw.xs(sym, axis=1, level=1)
        close  = _pick_close(sym_df)
        vol    = sym_df.get("Volume")
        return close, vol

    return None, None


def _build_scan_row(sym: str, df: pd.DataFrame) -> Optional[dict]:
    """
    從單支股票的 price DataFrame 計算特徵，回傳 scan row dict。
    計算完畢後 df 即可被 GC 回收（streaming pipeline 的關鍵）。
    """
    try:
        feats     = compute_features(df)
        last      = df.iloc[-1]
        close     = float(last["adj_close"])
        vol       = float(last["volume"])
        avg_vol   = feats["avg_volume_20d"]
        vol_ratio = vol / avg_vol if avg_vol > 0 else 0.0
        ma50      = feats["ma50"]
        ma200     = feats["ma200"]
        above_ma50  = close > ma50  if ma50  > 0 else False
        above_ma200 = close > ma200 if ma200 > 0 else False
        ma50_above  = ma50  > ma200 if ma50  > 0 and ma200 > 0 else False
        trend = (
            "bull"  if (above_ma50  and above_ma200 and ma50_above) else
            "bear"  if (not above_ma50 and not above_ma200 and not ma50_above) else
            "mixed"
        )
        prices   = df["adj_close"].values
        high_52w = float(prices[-252:].max()) if len(prices) >= 252 else float(prices.max())
        dist_52w = (close - high_52w) / high_52w if high_52w > 0 else 0.0
        row_date = last["date"]
        if hasattr(row_date, "date"):
            row_date = row_date.date()
        return {
            "symbol":             sym,
            "name":               sym,       # 後續 Tiingo 補充
            "market":             "US",
            "industry":           "未分類",  # 後續 Tiingo 補充
            "date":               row_date,
            "close":              round(close, 2),
            "volume":             round(vol),
            "avg_volume_20d":     round(avg_vol, 0),
            "ma50":               round(ma50, 2),
            "ma200":              round(ma200, 2),
            "score":              round(feats["current_score"], 1),
            "score_delta":        round(feats["score_delta"], 1),
            "vol_ratio_20d":      round(vol_ratio, 2),
            "dist_to_52w_high":   round(dist_52w, 4),
            "adj_close_to_ma50":  round(feats["adj_close_to_ma50_ratio"], 4),
            "adj_close_to_ma200": round(close / ma200 - 1, 4) if ma200 > 0 else 0.0,
            "momentum_20d":       round(feats["momentum_raw"], 4),
            "trend_state":        trend,
            "above_ma50":         above_ma50,
            "above_ma200":        above_ma200,
            "ma50_above_ma200":   ma50_above,
            "near_52w_high":      dist_52w > -0.05,
            "high_volume":        vol_ratio > 2.0,
            "vcp_flag":           False,
            "breakout_flag":      False,
            "reversal_flag":      False,
        }
    except Exception as e:
        logger.debug(f"{sym} _build_scan_row 失敗：{e}")
        return None


def _download_and_scan(
    symbols: list[str],
    progress_cb: Callable,
    api_key: Optional[str] = None,
) -> list[dict]:
    """
    Streaming pipeline：下載完一支立即計算特徵並丟棄 price data。
    記憶體峰值 ≈ _TIINGO_WORKERS 支股票的 price data（~幾 MB），
    而非全部 5000 支（~500MB）。
    """
    if api_key:
        ok, probe_msg = _probe_tiingo_eod(api_key)
        progress_cb(f"   Tiingo EOD probe（key 前 4 碼：{api_key[:4]}****）：{probe_msg}")
        if ok:
            return _tiingo_scan(symbols, progress_cb, api_key)
        progress_cb("   ⚠️  Tiingo EOD 不可用，切換至 yfinance fallback...")
    return _yfinance_scan(symbols, progress_cb)


def _batch_download(
    symbols: list[str],
    progress_cb: Callable,
) -> dict[str, pd.DataFrame]:
    """
    【已廢棄，保留供外部相容】優先用 Tiingo 逐支抓歷史價格（平行 10 thread）；
    無 API Key 時 fallback 到 yfinance 批次下載。

    Tiingo Power Plan 限制 20,000 req/hr，10 thread 並行即可在 3-5 分鐘內
    完成 5000+ 支，且對 Streamlit Cloud IP 無封鎖問題。
    """
    api_key = config.fresh("TIINGO_API_KEY")
    if api_key:
        return _tiingo_historical_download(symbols, progress_cb, api_key)
    else:
        logger.warning("TIINGO_API_KEY 未設定，改用 yfinance（Streamlit Cloud 可能受限）")
        return _yfinance_batch_download(symbols, progress_cb)


# ── Tiingo 歷史資料（主力）─────────────────────────────────

TIINGO_HIST_URL = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"
_TIINGO_WORKERS = 10   # 並行 thread 數；10 × ~300ms ≈ 33 req/s，在 20k/hr 限制內


def _tiingo_fetch_one(
    sym: str,
    start_date: str,
    api_key: str,
) -> Optional[pd.DataFrame]:
    """
    單支股票：呼叫 Tiingo daily prices，回傳 {date, adj_close, volume}。

    注意：resampleFreq 只接受 monthly/weekly/quarterly/annually，
    daily endpoint 預設即為日線，不傳此參數。
    """
    url = TIINGO_HIST_URL.format(ticker=sym.lower())
    headers = {"Authorization": f"Token {api_key}"}    # 與 tiingo_utils.py 一致
    params  = {"startDate": start_date}                 # 不傳 resampleFreq，預設日線

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=12)
            if resp.status_code == 429:          # rate limit → 指數退避
                time.sleep(2 ** attempt)
                continue
            if resp.status_code in (404, 400):   # 找不到 / 不支援
                return None
            if resp.status_code == 403:
                logger.warning(f"Tiingo 403 Forbidden（{sym}）— 可能需要升級方案或 key 無效")
                return None
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None

            df = pd.DataFrame(data)
            df["date"]      = pd.to_datetime(df["date"], utc=True).dt.tz_convert(None)
            # 優先取 adjClose；若無則取 close
            close_col = "adjClose" if "adjClose" in df.columns else "close"
            vol_col   = "adjVolume" if "adjVolume" in df.columns else "volume"
            df = df.rename(columns={close_col: "adj_close", vol_col: "volume"})
            df = df[["date", "adj_close", "volume"]].copy()
            df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
            df["volume"]    = pd.to_numeric(df["volume"],    errors="coerce").fillna(0.0)
            df = df.dropna(subset=["adj_close"]).reset_index(drop=True)
            return df

        except Exception as e:
            if attempt < 2:
                time.sleep(0.5)
            else:
                logger.debug(f"{sym} Tiingo hist 失敗（attempt {attempt}）：{e}")
    return None


def _probe_tiingo_eod(api_key: str) -> tuple[bool, str]:
    """
    用 AAPL 快速測試 Tiingo EOD endpoint 是否可用。
    回傳 (可用?, 診斷訊息)
    """
    url = TIINGO_HIST_URL.format(ticker="aapl")
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Token {api_key}"},
            params={"startDate": (date.today() - timedelta(days=7)).isoformat()},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return True, f"✅ AAPL 連線成功，取得 {len(data)} 筆"
        # 嘗試解析錯誤訊息
        try:
            detail = resp.json().get("detail", resp.text[:120])
        except Exception:
            detail = resp.text[:120]
        hints = {
            403: f"HTTP 403 — {detail}（key 無效或方案不含 EOD 資料）",
            401: f"HTTP 401 — 認證失敗，請確認 key 正確",
            404: f"HTTP 404 — endpoint 路徑錯誤",
            402: f"HTTP 402 — 需要付費升級方案",
        }
        msg = hints.get(resp.status_code, f"HTTP {resp.status_code} — {detail}")
        return False, msg
    except Exception as e:
        return False, f"連線異常：{e}"


def _tiingo_scan(
    symbols: list[str],
    progress_cb: Callable,
    api_key: str,
) -> list[dict]:
    """
    Tiingo streaming scan：下載一支 → 立即計算特徵 → 丟棄 price data。
    記憶體峰值 ≈ _TIINGO_WORKERS 支股票的 price data，不累積整個 dict。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    start_date = (date.today() - timedelta(days=430)).isoformat()
    rows:  list[dict] = []
    total = len(symbols)

    # ── 主執行緒預測試（診斷用）──────────────────────────────
    progress_cb(f"   [診斷] AAPL 全期資料測試（startDate={start_date}）...")
    try:
        _test_df = _tiingo_fetch_one("AAPL", start_date, api_key)
        if _test_df is None:
            progress_cb("   [診斷] ⚠️  AAPL 全期請求回傳 None → API 或方案問題")
        elif len(_test_df) < 50:
            progress_cb(f"   [診斷] ⚠️  AAPL 只有 {len(_test_df)} 筆（需 ≥50）→ 方案可能限制歷史深度")
        else:
            last_p  = float(_test_df["adj_close"].iloc[-1])
            avg_v   = float(_test_df["volume"].tail(20).mean())
            test_row = _build_scan_row("AAPL", _test_df)
            progress_cb(
                f"   [診斷] ✅ AAPL OK：{len(_test_df)} 筆，"
                f"price=${last_p:.2f}，avgVol={avg_v:,.0f}，"
                f"row={'OK' if test_row else 'None（_build_scan_row 失敗）'}"
            )
    except Exception as ex:
        progress_cb(f"   [診斷] ❌ AAPL 例外：{ex}")

    # ── 失敗類型計數（輔助診斷） ─────────────────────────────
    _cnt: dict[str, int] = {
        "api_none": 0, "too_short": 0,
        "low_price": 0, "low_vol": 0,
        "build_fail": 0, "exception": 0,
    }

    def _process(sym: str) -> Optional[dict]:
        """回傳 (tag, row_or_None)；tag 供診斷計數用"""
        try:
            df = _tiingo_fetch_one(sym, start_date, api_key)
            if df is None:
                return ("api_none", None)
            if len(df) < 50:
                return ("too_short", None)
            last_price = float(df["adj_close"].iloc[-1])
            avg_vol    = float(df["volume"].tail(20).mean())
            if last_price < MIN_PRICE:
                return ("low_price", None)
            if avg_vol < MIN_AVG_VOLUME:
                return ("low_vol", None)
            row = _build_scan_row(sym, df)
            if row is None:
                return ("build_fail", None)
            return ("ok", row)
        except Exception as e:
            logger.debug(f"{sym} _process 例外：{e}")
            return ("exception", None)

    with ThreadPoolExecutor(max_workers=_TIINGO_WORKERS) as ex:
        futures = {ex.submit(_process, s): s for s in symbols}
        done = 0
        for fut in as_completed(futures):
            try:
                tag, row = fut.result()
                if tag == "ok":
                    rows.append(row)
                else:
                    _cnt[tag] = _cnt.get(tag, 0) + 1
            except Exception as e:
                logger.debug(f"future exception: {e}")
                _cnt["exception"] += 1
            done += 1
            if done % 500 == 0 or done >= total:
                progress_cb(
                    f"   Tiingo 進度：{done}/{total} 支"
                    f"（有效：{len(rows)}｜"
                    f"API無資料：{_cnt['api_none']}｜"
                    f"資料不足：{_cnt['too_short']}｜"
                    f"低量：{_cnt['low_vol']}｜"
                    f"低價：{_cnt['low_price']}）"
                )

    # 最終統計
    progress_cb(
        f"   [診斷] 失敗分類 — "
        f"API無資料:{_cnt['api_none']} "
        f"資料不足:{_cnt['too_short']} "
        f"低量:{_cnt['low_vol']} "
        f"低價:{_cnt['low_price']} "
        f"build失敗:{_cnt['build_fail']} "
        f"例外:{_cnt['exception']}"
    )
    return rows


def _yfinance_scan(
    symbols: list[str],
    progress_cb: Callable,
) -> list[dict]:
    """
    yfinance fallback streaming scan：每批下載後立即計算特徵、丟棄原始 DataFrame。
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("請先安裝 yfinance：pip install yfinance>=0.2.0")

    rows:  list[dict] = []
    total = len(symbols)

    for batch_start in range(0, total, BATCH_SIZE):
        batch = symbols[batch_start : batch_start + BATCH_SIZE]
        done  = min(batch_start + BATCH_SIZE, total)

        try:
            raw = yf.download(
                tickers=" ".join(batch),
                period="14mo",
                group_by="column",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if not raw.empty:
                for sym in batch:
                    try:
                        close_s, vol_s = _extract_close_vol(raw, sym, len(batch))
                        if close_s is None:
                            continue
                        close_s = pd.to_numeric(close_s, errors="coerce")
                        vol_s   = (
                            pd.to_numeric(vol_s, errors="coerce")
                            if vol_s is not None
                            else pd.Series(0.0, index=close_s.index)
                        )
                        df = pd.DataFrame({"adj_close": close_s, "volume": vol_s})
                        df.index.name = "date"
                        df = df.reset_index().dropna(subset=["adj_close"])
                        df["volume"] = df["volume"].fillna(0.0)
                        if len(df) < 50:
                            continue
                        last_price = float(df["adj_close"].iloc[-1])
                        avg_vol    = float(df["volume"].tail(20).mean())
                        if last_price < MIN_PRICE or avg_vol < MIN_AVG_VOLUME:
                            continue
                        row = _build_scan_row(sym, df)   # 計算完立即丟棄 df
                        if row is not None:
                            rows.append(row)
                    except Exception:
                        continue

        except Exception as e:
            logger.warning(f"yfinance 批次 {batch_start // BATCH_SIZE + 1} 失敗：{e}")

        if done % (BATCH_SIZE * 5) == 0 or done >= total:
            progress_cb(f"   yfinance 進度：{done}/{total}（有效：{len(rows)} 支）")

    return rows


# ════════════════════════════════════════════════════════
# ③ 計算技術特徵
# ════════════════════════════════════════════════════════

def _compute_scan(
    price_data: dict[str, pd.DataFrame],
    progress_cb: Callable,
) -> pd.DataFrame:
    """對每支股票呼叫 compute_features，回傳 scan DataFrame"""
    rows = []
    total = len(price_data)

    for idx, (symbol, df) in enumerate(price_data.items()):
        try:
            feats = compute_features(df)
        except Exception as e:
            logger.debug(f"{symbol} 特徵計算失敗：{e}")
            continue

        last      = df.iloc[-1]
        close     = float(last["adj_close"])
        vol       = float(last["volume"])
        avg_vol   = feats["avg_volume_20d"]
        vol_ratio = vol / avg_vol if avg_vol > 0 else 0.0

        ma50  = feats["ma50"]
        ma200 = feats["ma200"]
        above_ma50  = close > ma50  if ma50  > 0 else False
        above_ma200 = close > ma200 if ma200 > 0 else False
        ma50_above  = ma50  > ma200 if ma50  > 0 and ma200 > 0 else False

        trend = (
            "bull"  if (above_ma50  and above_ma200 and ma50_above) else
            "bear"  if (not above_ma50 and not above_ma200 and not ma50_above) else
            "mixed"
        )

        prices   = df["adj_close"].values
        high_52w = float(prices[-252:].max()) if len(prices) >= 252 else float(prices.max())
        dist_52w = (close - high_52w) / high_52w if high_52w > 0 else 0.0

        row_date = last["date"]
        if hasattr(row_date, "date"):
            row_date = row_date.date()

        rows.append({
            "symbol":             symbol,
            "name":               symbol,   # 後續 Tiingo 補充
            "market":             "US",
            "industry":           "未分類",  # 後續 Tiingo 補充
            "date":               row_date,
            "close":              round(close, 2),
            "volume":             round(vol),
            "avg_volume_20d":     round(avg_vol, 0),
            "ma50":               round(ma50, 2),
            "ma200":              round(ma200, 2),
            "score":              round(feats["current_score"], 1),
            "score_delta":        round(feats["score_delta"], 1),
            "vol_ratio_20d":      round(vol_ratio, 2),
            "dist_to_52w_high":   round(dist_52w, 4),
            "adj_close_to_ma50":  round(feats["adj_close_to_ma50_ratio"], 4),
            "adj_close_to_ma200": round(close / ma200 - 1, 4) if ma200 > 0 else 0.0,
            "momentum_20d":       round(feats["momentum_raw"], 4),
            "trend_state":        trend,
            "above_ma50":         above_ma50,
            "above_ma200":        above_ma200,
            "ma50_above_ma200":   ma50_above,
            "near_52w_high":      dist_52w > -0.05,
            "high_volume":        vol_ratio > 2.0,
            "vcp_flag":           False,
            "breakout_flag":      False,
            "reversal_flag":      False,
        })

        if idx % 500 == 0 and idx > 0:
            progress_cb(f"   特徵計算進度：{idx}/{total}")

    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════
# ④ 産業分類（Tiingo，本地快取）
# ════════════════════════════════════════════════════════

def _enrich_industry(
    scan_df: pd.DataFrame,
    max_fetch: int = MAX_IND_FETCH,
    progress_cb: Optional[Callable] = None,
) -> pd.DataFrame:
    """
    用 Tiingo metadata 補充名稱 + 産業分類。
    - 優先從本地快取讀取（跨次復用）
    - 對無資料的股票逐一呼叫 Tiingo /tiingo/daily/{ticker}
    - 用 DataFrame.loc 直接更新（保留欄位型別，避免 apply 型別污染）
    - sector 作為 industry 的 fallback（Tiingo industry 欄位常為 null）
    """
    api_key = config.fresh("TIINGO_API_KEY")
    if not api_key:
        logger.warning("TIINGO_API_KEY 未設定，跳過産業補充")
        return scan_df

    def _p(msg: str) -> None:
        logger.info(msg)
        if progress_cb:
            progress_cb(msg)

    # ── 載入本地快取 ──────────────────────────────────────
    cache_map: dict[str, dict] = {}
    if US_IND_CACHE.exists():
        try:
            cached = pd.read_parquet(str(US_IND_CACHE))
            for r in cached.to_dict("records"):  # to_dict 比 iterrows 快 10x+
                cache_map[str(r["symbol"])] = {
                    "name":     str(r.get("name", "") or ""),
                    "industry": str(r.get("industry", "") or ""),
                    "sector":   str(r.get("sector", "") or ""),
                }
            _p(f"   産業快取載入：{len(cache_map)} 支")
        except Exception as e:
            logger.warning(f"産業快取讀取失敗：{e}")

    # ── 套用快取（直接 loc 更新，保留型別） ──────────────
    scan_df = scan_df.copy()
    scan_df["sector"] = ""    # 確保 sector 欄存在

    for idx in range(len(scan_df)):
        sym  = scan_df.at[idx, "symbol"]
        info = cache_map.get(sym)
        if not info:
            continue
        if info.get("name"):
            scan_df.at[idx, "name"] = info["name"]
        ind = info.get("industry") or info.get("sector") or ""
        if ind and ind not in ("nan", "None", "未分類"):
            scan_df.at[idx, "industry"] = ind
        if info.get("sector"):
            scan_df.at[idx, "sector"] = info["sector"]

    # ── 找出需要補充的 symbol ──────────────────────────────
    need = (
        scan_df[scan_df["industry"] == "未分類"]
        .sort_values("score", ascending=False)["symbol"]
        .tolist()[:max_fetch]
    )
    _p(f"   需補充産業：{len(need)} 支（上限 {max_fetch}）")

    if not need:
        return scan_df

    # ── 逐一呼叫 Tiingo metadata ──────────────────────────
    new_cache: list[dict] = []
    success = 0

    for i, sym in enumerate(need):
        try:
            resp = requests.get(
                TIINGO_META_URL.format(ticker=sym.lower()),
                headers={"Authorization": f"Token {api_key}"},
                timeout=8,
            )
            if resp.status_code == 200:
                d    = resp.json()
                name = d.get("name") or sym
                sec  = d.get("sector") or ""
                ind  = d.get("industry") or sec or "未分類"

                # 更新 scan_df
                rows = scan_df.index[scan_df["symbol"] == sym].tolist()
                for row_idx in rows:
                    if name:
                        scan_df.at[row_idx, "name"] = name
                    if ind and ind != "未分類":
                        scan_df.at[row_idx, "industry"] = ind
                    if sec:
                        scan_df.at[row_idx, "sector"] = sec

                cache_map[sym] = {"name": name, "industry": ind, "sector": sec}
                new_cache.append({"symbol": sym, "name": name, "sector": sec, "industry": ind})
                success += 1

        except Exception as e:
            logger.debug(f"{sym} Tiingo metadata 失敗：{e}")

        if i > 0 and i % 200 == 0:
            time.sleep(0.3)
            _p(f"   産業補充進度：{i}/{len(need)}（成功 {success} 支）")

    _p(f"   産業補充完成：{success}/{len(need)} 支取得資料")

    # ── 更新本地快取 ───────────────────────────────────────
    if new_cache:
        _update_industry_cache(new_cache)

    return scan_df


def _update_industry_cache(new_rows: list[dict]) -> None:
    """把新取得的 industry 資料合併入本地快取"""
    try:
        new_df = pd.DataFrame(new_rows)[["symbol", "name", "sector", "industry"]]
        if US_IND_CACHE.exists():
            old_df = pd.read_parquet(str(US_IND_CACHE))
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates("symbol", keep="last")
        else:
            combined = new_df
        combined.to_parquet(str(US_IND_CACHE), index=False, engine="pyarrow")
    except Exception as e:
        logger.warning(f"産業快取更新失敗：{e}")
