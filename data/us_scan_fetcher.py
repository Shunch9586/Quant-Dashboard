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

BATCH_SIZE        = 200      # yfinance 每批張數
MAX_SYMBOLS       = 7000     # 上限（避免記憶體不足）
MIN_PRICE         = 2.0      # 過濾低價垃圾股
MIN_AVG_VOLUME    = 100_000  # 過濾低流動性
MAX_IND_FETCH     = 800      # Tiingo 每次最多補充多少産業資訊


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
        # ① 股票清單
        _p("📋 取得全市場股票清單（Tiingo → SEC → NASDAQ FTP）...")
        symbols = _get_us_symbols()
        _p(f"   取得 {len(symbols)} 支（過濾 ETF / 低流動性後）")

        if not symbols:
            return False, "無法取得美股清單（Tiingo / SEC / NASDAQ FTP 均失敗）"

        # ② 批次下載價格
        _p(f"📈 批次下載 {len(symbols)} 支近 14 個月價格（yfinance）...")
        price_data = _batch_download(symbols, _p)
        _p(f"   成功下載 {len(price_data)} 支有效資料")

        if not price_data:
            return False, "yfinance 回傳空資料，請稍後再試"

        # ③ 計算技術特徵
        _p("⚙️  計算技術特徵（Score / MA / 動能）...")
        scan_df = _compute_scan(price_data, _p)
        _p(f"   特徵計算完成：{len(scan_df)} 支")

        # ④ 補充産業資訊
        _p("🏷️  補充産業分類（Tiingo，優先高分股）...")
        scan_df = _enrich_industry(scan_df, max_fetch=MAX_IND_FETCH)

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
    取得全市場股票清單，依序嘗試三個來源：
    1. Tiingo supported_tickers.zip（最完整，免費公開 ZIP）
    2. SEC EDGAR company_tickers.json（備用，約 1 萬家上市公司）
    3. NASDAQ FTP（第三選擇，Streamlit Cloud 可能被封鎖）
    過濾：排除 ETF，只保留 1–5 個大寫英文字母的純股票代號
    """
    symbols = _try_tiingo_tickers()
    if symbols:
        logger.info(f"股票清單來源：Tiingo supported_tickers（{len(symbols)} 支）")
        return symbols

    symbols = _try_sec_tickers()
    if symbols:
        logger.info(f"股票清單來源：SEC EDGAR（{len(symbols)} 支）")
        return symbols

    symbols = _try_nasdaq_ftp()
    if symbols:
        logger.info(f"股票清單來源：NASDAQ FTP（{len(symbols)} 支）")
        return symbols

    logger.error("三個股票清單來源均失敗")
    return []


def _try_tiingo_tickers() -> list[str]:
    """
    Tiingo supported_tickers.zip：免費公開 ZIP，包含所有 Tiingo 支援的代號。
    CSV 欄位：ticker, exchange, assetType, priceCurrency, startDate, endDate
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

        # 過濾：只要 Stock（非 ETF / Mutual Fund），且主要交易所
        valid_exchanges = {"NYSE", "NASDAQ", "NYSE MKT", "NYSE ARCA", "BATS", "IEX"}
        mask = (
            (df["assetType"].str.upper().isin(["STOCK", "COMMON STOCK"])) &
            (df["exchange"].str.upper().isin({e.upper() for e in valid_exchanges})) &
            (df["ticker"].str.match(r"^[A-Z]{1,5}$", na=False)) &
            (df["priceCurrency"].str.upper() == "USD")
        )
        syms = df[mask]["ticker"].dropna().unique().tolist()
        return sorted(syms)[:MAX_SYMBOLS]

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

def _batch_download(
    symbols: list[str],
    progress_cb: Callable,
) -> dict[str, pd.DataFrame]:
    """
    用 yfinance 批次下載調整後收盤 + 成交量。
    每批 BATCH_SIZE 支，回傳 {symbol: price_df}。
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("請先安裝 yfinance：pip install yfinance>=0.2.0")

    result: dict[str, pd.DataFrame] = {}
    total = len(symbols)

    for batch_start in range(0, total, BATCH_SIZE):
        batch = symbols[batch_start : batch_start + BATCH_SIZE]
        batch_str = " ".join(batch)

        try:
            raw = yf.download(
                tickers=batch_str,
                period="14mo",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            if raw.empty:
                continue

            for sym in batch:
                try:
                    # yfinance 多 ticker 回傳 MultiIndex (field, ticker)
                    if len(batch) == 1:
                        ticker_df = raw.copy()
                    elif sym in raw.columns.get_level_values(0):
                        ticker_df = raw[sym].copy()
                    else:
                        continue

                    if ticker_df.empty or len(ticker_df) < 50:
                        continue

                    ticker_df = ticker_df.dropna(subset=["Close", "Volume"])
                    ticker_df = ticker_df.rename(columns={
                        "Close":  "adj_close",
                        "Volume": "volume",
                    })
                    ticker_df.index.name = "date"
                    ticker_df = ticker_df.reset_index()[["date", "adj_close", "volume"]]
                    ticker_df["adj_close"] = ticker_df["adj_close"].astype(float)
                    ticker_df["volume"]    = ticker_df["volume"].astype(float)

                    # 過濾低價 / 低量
                    last_price = float(ticker_df["adj_close"].iloc[-1])
                    avg_vol    = float(ticker_df["volume"].tail(20).mean())
                    if last_price < MIN_PRICE or avg_vol < MIN_AVG_VOLUME:
                        continue

                    result[sym] = ticker_df

                except Exception:
                    continue

        except Exception as e:
            logger.warning(f"批次 {batch_start // BATCH_SIZE + 1} 下載失敗：{e}")

        done = min(batch_start + BATCH_SIZE, total)
        if done % (BATCH_SIZE * 5) == 0 or done >= total:
            progress_cb(
                f"   下載進度：{done}/{total} 支 "
                f"（有效：{len(result)} 支）"
            )

    return result


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

def _enrich_industry(scan_df: pd.DataFrame, max_fetch: int = MAX_IND_FETCH) -> pd.DataFrame:
    """
    用 Tiingo 補充名稱 + 産業分類（限制 API 呼叫）。
    優先補充 score 最高的股票。
    快取儲存於 /tmp/us_industry_cache.parquet（跨次呼叫復用）。
    """
    api_key = config.fresh("TIINGO_API_KEY")
    if not api_key:
        return scan_df

    # 載入本地 industry 快取
    cache_map: dict[str, dict] = {}
    if US_IND_CACHE.exists():
        try:
            cached = pd.read_parquet(str(US_IND_CACHE))
            for _, row in cached.iterrows():
                cache_map[str(row["symbol"])] = {
                    "name":     str(row.get("name", "")),
                    "industry": str(row.get("industry", "未分類")),
                }
        except Exception:
            pass

    # 套用已有快取
    def _apply(row: pd.Series) -> pd.Series:
        info = cache_map.get(row["symbol"])
        if info:
            if info.get("name"):
                row["name"] = info["name"]
            if info.get("industry") and info["industry"] not in ("", "nan", "None", "未分類"):
                row["industry"] = info["industry"]
        return row

    scan_df = scan_df.apply(_apply, axis=1)

    # 找出尚缺産業的 symbol（按 score 排序）
    need_industry = (
        scan_df[scan_df["industry"] == "未分類"]
        .sort_values("score", ascending=False)["symbol"]
        .tolist()[:max_fetch]
    )

    if not need_industry:
        return scan_df

    logger.info(f"Tiingo 補充 {len(need_industry)} 支産業資訊...")
    new_cache: list[dict] = []

    for i, sym in enumerate(need_industry):
        try:
            url  = TIINGO_META_URL.format(ticker=sym.lower())
            resp = requests.get(
                url,
                headers={"Authorization": f"Token {api_key}"},
                timeout=8,
            )
            if resp.status_code == 200:
                d = resp.json()
                info = {
                    "symbol":   sym,
                    "name":     d.get("name", sym),
                    "sector":   d.get("sector", ""),
                    "industry": d.get("industry") or d.get("sector") or "未分類",
                }
                cache_map[sym] = {"name": info["name"], "industry": info["industry"]}
                new_cache.append(info)
        except Exception:
            pass

        if i > 0 and i % 100 == 0:
            time.sleep(0.5)   # 避免觸發 Tiingo rate limit

    # 更新 scan_df
    if new_cache:
        new_map = {r["symbol"]: r for r in new_cache}

        def _apply_new(row: pd.Series) -> pd.Series:
            info = new_map.get(row["symbol"])
            if info:
                if info.get("name"):
                    row["name"] = info["name"]
                if info.get("industry") not in ("", "nan", "None", "未分類", None):
                    row["industry"] = info["industry"]
            return row

        scan_df = scan_df.apply(_apply_new, axis=1)

        # 更新本地快取（合併舊有 + 新增）
        _update_industry_cache(new_cache)
        logger.info(f"産業快取已更新（新增 {len(new_cache)} 支）")

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
