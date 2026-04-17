"""
市場掃描資料抓取器
Dashboard 直接呼叫 API，計算特徵後存至 S3，不依賴 ETL pipeline。

TW  → FinMind API (TaiwanStockPriceAdj + TaiwanStockInfo)
US  → 讀 ETL 提供的 S3 Parquet，補充 Tiingo 的 sector/industry metadata

執行策略：
  1. 檢查 S3 是否已有今日資料 → 有就跳過
  2. 沒有 → 呼叫 API → 計算特徵 → 存 S3
  每次只在需要時執行，每天最多跑一次
"""

import io
import logging
import time
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import requests

import config
from data.features import compute_features

logger = logging.getLogger(__name__)

BUCKET            = config.S3_BUCKET_NAME
FINMIND_URL       = "https://api.finmindtrade.com/api/v4/data"
TIINGO_META_URL   = "https://api.tiingo.com/tiingo/daily/{ticker}"
FETCH_DAYS        = 300       # MA200 需要 200+ 天
TW_LATEST_S3      = f"data_lake/market_scan/market=TW/latest/scan.parquet"
US_INDUSTRY_S3    = f"data_lake/market_scan/us_industry_cache.parquet"


# ════════════════════════════════════════════════════════
# 公開介面
# ════════════════════════════════════════════════════════

def tw_scan_is_fresh(fs) -> bool:
    """S3 的 TW scan 是否已是今天的資料"""
    return _check_date(fs, TW_LATEST_S3)


def run_tw_scan(fs) -> tuple[bool, str]:
    """
    執行 TW 市場掃描，結果存至 S3。
    回傳 (成功?, 訊息)
    需要 FINMIND_API_TOKEN 已設定。
    """
    if not config.FINMIND_API_TOKEN:
        return False, "FINMIND_API_TOKEN 未設定，請在 Streamlit Cloud Secrets 加入"

    try:
        logger.info("開始 TW 市場掃描（FinMind）...")

        # 1. 取得股票清單 + 產業別
        info_df = _fetch_tw_info()
        logger.info(f"TaiwanStockInfo：{len(info_df)} 支股票")

        # 2. 取得近 300 天價格（全市場，支援分頁）
        price_df = _fetch_tw_prices()
        if price_df.empty:
            return False, "FinMind 回傳空的價格資料"
        logger.info(f"TaiwanStockPriceAdj：{len(price_df)} 筆 / {price_df['stock_id'].nunique()} 支股票")

        # 3. 計算特徵
        scan_df = _compute_tw_scan(price_df, info_df)
        logger.info(f"特徵計算完成：{len(scan_df)} 支有效股票")

        # 4. 存至 S3
        _save_to_s3(fs, scan_df, TW_LATEST_S3)

        return True, f"台股掃描完成，共 {len(scan_df)} 支"

    except requests.exceptions.Timeout:
        return False, "FinMind API 逾時，請稍後再試"
    except requests.exceptions.HTTPError as e:
        return False, f"FinMind API 錯誤：{e}"
    except Exception as e:
        logger.exception("TW scan 失敗")
        return False, f"發生錯誤：{e}"


def enrich_us_industry(fs, symbols: list[str]) -> pd.DataFrame:
    """
    用 Tiingo 補充 US 股票的 industry / sector。
    回傳 DataFrame: symbol, name, sector, industry
    自動快取結果至 S3，只呼叫沒有快取的 symbol。
    """
    if not config.TIINGO_API_KEY:
        return pd.DataFrame(columns=["symbol", "name", "sector", "industry"])

    # 讀現有快取
    cached = _load_us_industry_cache(fs)
    cached_symbols = set(cached["symbol"].tolist()) if not cached.empty else set()
    missing = [s for s in symbols if s not in cached_symbols]

    if not missing:
        return cached

    logger.info(f"Tiingo 補充 {len(missing)} 支 US 股票的 industry...")
    new_rows = []
    for i, sym in enumerate(missing):
        row = _fetch_tiingo_meta(sym)
        if row:
            new_rows.append(row)
        if i > 0 and i % 50 == 0:
            logger.info(f"  進度 {i}/{len(missing)}")
            time.sleep(0.5)   # 避免 rate limit

    if new_rows:
        new_df  = pd.DataFrame(new_rows)
        combined = pd.concat([cached, new_df], ignore_index=True)
        combined = combined.drop_duplicates("symbol", keep="last")
        _save_to_s3(fs, combined, US_INDUSTRY_S3)
        return combined

    return cached


# ════════════════════════════════════════════════════════
# FinMind — TW
# ════════════════════════════════════════════════════════

def _fetch_tw_info() -> pd.DataFrame:
    """TaiwanStockInfo：股票名稱 + 產業別"""
    resp = _finmind_get("TaiwanStockInfo", {})
    if resp is None or resp.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name", "industry_category"])
    # 保留普通股 + ETF
    keep_types = {"twse", "tpex", "ETF", "stock", "上市", "上櫃"}
    if "type" in resp.columns:
        resp = resp[resp["type"].apply(lambda t: str(t).lower() in {x.lower() for x in keep_types})]
    return resp[["stock_id", "stock_name", "industry_category"]].drop_duplicates("stock_id")


def _fetch_tw_prices() -> pd.DataFrame:
    """TaiwanStockPriceAdj：近 FETCH_DAYS 天全市場調整後收盤（支援 FinMind 分頁）"""
    start = (date.today() - timedelta(days=FETCH_DAYS)).isoformat()
    end   = date.today().isoformat()

    frames = []
    page   = 1
    while True:
        chunk = _finmind_get("TaiwanStockPriceAdj", {
            "start_date": start,
            "end_date":   end,
            "page":       page,
        })
        if chunk is None or chunk.empty:
            break
        frames.append(chunk)
        logger.info(f"  FinMind page {page}：{len(chunk)} 筆")
        # FinMind 每頁上限通常 30,000 筆，小於 1,000 代表已是最後頁
        if len(chunk) < 1000:
            break
        page += 1
        time.sleep(0.3)   # 禮貌性等待

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    # FinMind 的 TaiwanStockPriceAdj close 就是權息調整後收盤
    df = df.rename(columns={"close": "adj_close"})
    return df.sort_values(["stock_id", "date"]).reset_index(drop=True)


def _finmind_get(dataset: str, extra_params: dict) -> Optional[pd.DataFrame]:
    """統一的 FinMind GET，回傳 DataFrame 或 None"""
    params = {
        "dataset": dataset,
        "token":   config.FINMIND_API_TOKEN,
        **extra_params,
    }
    try:
        resp = requests.get(FINMIND_URL, params=params, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != 200:
            logger.warning(f"FinMind {dataset} status={body.get('status')}: {body.get('msg')}")
            return None
        data = body.get("data", [])
        return pd.DataFrame(data) if data else pd.DataFrame()
    except requests.exceptions.RequestException as e:
        logger.error(f"FinMind {dataset} 請求失敗：{e}")
        return None


# ════════════════════════════════════════════════════════
# 特徵計算 — TW
# ════════════════════════════════════════════════════════

def _compute_tw_scan(price_df: pd.DataFrame, info_df: pd.DataFrame) -> pd.DataFrame:
    """對每支股票計算技術特徵，回傳 scan DataFrame"""
    # stock_id → {name, industry} lookup
    info_map: dict[str, dict] = {}
    if not info_df.empty:
        for _, r in info_df.iterrows():
            sid = str(r["stock_id"])
            info_map[sid] = {
                "name":     str(r.get("stock_name", sid)),
                "industry": str(r.get("industry_category", "") or "未分類").strip() or "未分類",
            }

    rows = []
    for stock_id, grp in price_df.groupby("stock_id"):
        sid  = str(stock_id)
        grp  = grp.sort_values("date").reset_index(drop=True)

        if len(grp) < 20:
            continue

        # compute_features 需要 adj_close 欄位
        try:
            feats = compute_features(grp)
        except Exception as e:
            logger.debug(f"{sid} 特徵計算失敗：{e}")
            continue

        last    = grp.iloc[-1]
        close   = float(last["adj_close"])
        vol     = float(last["volume"]) if "volume" in grp.columns else 0.0
        info    = info_map.get(sid, {"name": sid, "industry": "未分類"})

        ma50    = feats["ma50"]
        ma200   = feats["ma200"]
        avg_vol = feats["avg_volume_20d"]

        above_ma50  = close > ma50  if ma50  > 0 else False
        above_ma200 = close > ma200 if ma200 > 0 else False
        ma50_above  = ma50 > ma200  if ma50  > 0 and ma200 > 0 else False

        trend = ("bull" if (above_ma50 and above_ma200 and ma50_above)
                 else "bear" if (not above_ma50 and not above_ma200 and not ma50_above)
                 else "mixed")

        high_52w  = grp["adj_close"].tail(252).max()
        dist_52w  = (close - high_52w) / high_52w if high_52w > 0 else 0.0
        vol_ratio = vol / avg_vol if avg_vol > 0 else 0.0

        row_date = last["date"]
        if hasattr(row_date, "date"):
            row_date = row_date.date()

        rows.append({
            "symbol":             sid,
            "name":               info["name"],
            "market":             "TW",
            "industry":           info["industry"],
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

    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════
# Tiingo — US industry metadata
# ════════════════════════════════════════════════════════

def _fetch_tiingo_meta(symbol: str) -> Optional[dict]:
    """取得單一美股的 name / sector / industry"""
    try:
        url  = TIINGO_META_URL.format(ticker=symbol.lower())
        resp = requests.get(
            url,
            headers={"Authorization": f"Token {config.TIINGO_API_KEY}"},
            timeout=10,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        d = resp.json()
        return {
            "symbol":   symbol.upper(),
            "name":     d.get("name", symbol),
            "sector":   d.get("sector", ""),
            "industry": d.get("industry", d.get("sector", "未分類")) or "未分類",
        }
    except Exception:
        return None


def _load_us_industry_cache(fs) -> pd.DataFrame:
    """讀取 US industry 快取"""
    try:
        path = f"s3://{BUCKET}/{US_INDUSTRY_S3}"
        with fs.open(path, "rb") as f:
            return pd.read_parquet(f)
    except Exception:
        return pd.DataFrame(columns=["symbol", "name", "sector", "industry"])


# ════════════════════════════════════════════════════════
# S3 工具
# ════════════════════════════════════════════════════════

def _check_date(fs, s3_key: str) -> bool:
    """S3 檔案的 date 欄位最大值是否是今天"""
    try:
        with fs.open(f"s3://{BUCKET}/{s3_key}", "rb") as f:
            df = pd.read_parquet(f, columns=["date"])
        if not df.empty:
            latest = pd.to_datetime(df["date"]).max().date()
            return latest >= date.today()
    except Exception:
        pass
    return False


def _save_to_s3(fs, df: pd.DataFrame, s3_key: str) -> None:
    """儲存 DataFrame 至 S3（parquet 格式）"""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    path = f"s3://{BUCKET}/{s3_key}"
    with fs.open(path, "wb") as f:
        f.write(buf.read())
    logger.info(f"已存至 {path}（{len(df)} 列）")
