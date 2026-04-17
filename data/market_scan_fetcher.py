"""
市場掃描資料抓取器

TW 主要資料來源：tw_market.db（S3 SQLite，ETL 每日更新）
  - 一個大型 SQL query 取全市場 300 天價格 + 股票名稱 + 產業別
  - 計算特徵後存至 S3 作為每日快取

TW FinMind API（選配）：
  - 若設定 FINMIND_API_TOKEN，改用 FinMind 抓每支股票歷史資料
  - FinMind 的 TaiwanStockPriceAdj 需逐一指定 stock_id（不支援全市場一次抓）
  - 目前預設用 SQLite，FinMind 預留但不主動使用

US：由 ETL pipeline 提供，Tiingo 補充 industry metadata
"""

import io
import logging
import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

import config
from data.features import compute_features

logger = logging.getLogger(__name__)

BUCKET          = config.S3_BUCKET_NAME
FETCH_DAYS      = 310        # 多取一點確保 MA200 夠用
TW_LATEST_S3    = "data_lake/market_scan/market=TW/latest/scan.parquet"
US_INDUSTRY_S3  = "data_lake/market_scan/us_industry_cache.parquet"
FINMIND_URL     = "https://api.finmindtrade.com/api/v4/data"
TIINGO_META_URL = "https://api.tiingo.com/tiingo/daily/{ticker}"

# 複用 loader.py 的 SQLite 快取路徑（兩邊共用同一份檔案）
TW_DB_LOCAL     = Path("/tmp/tw_market_cache.db")
TW_DB_DATE_FILE = Path("/tmp/tw_market_cache_date.txt")
TW_DB_S3_KEY    = "db/latest/tw_market.db"

# TW scan 結果存 /tmp（固定檔名，附日期 metadata）
TW_SCAN_LOCAL     = Path("/tmp/tw_scan_latest.parquet")
TW_SCAN_DATE_FILE = Path("/tmp/tw_scan_latest_date.txt")
TW_SCAN_MAX_AGE_DAYS = 3   # 週末 / 假日沿用最近 3 天內的資料


# ════════════════════════════════════════════════════════
# 公開介面
# ════════════════════════════════════════════════════════

def tw_scan_is_fresh(fs=None) -> bool:
    """TW scan 是否在有效期內（優先查 /tmp，再查 S3）"""
    # 先查本地 /tmp
    if TW_SCAN_LOCAL.exists() and TW_SCAN_DATE_FILE.exists():
        try:
            scan_date = date.fromisoformat(TW_SCAN_DATE_FILE.read_text().strip())
            if (date.today() - scan_date).days <= TW_SCAN_MAX_AGE_DAYS:
                return True
        except Exception:
            pass
    # 再查 S3
    if fs is not None:
        return _check_s3_date(fs, TW_LATEST_S3)
    return False


def get_tw_scan_date() -> Optional[str]:
    """回傳上次掃描日期字串，供 UI 顯示用"""
    if TW_SCAN_DATE_FILE.exists():
        try:
            return TW_SCAN_DATE_FILE.read_text().strip()
        except Exception:
            pass
    return None


def run_tw_scan(fs) -> tuple[bool, str]:
    """
    執行 TW 全市場掃描，結果存至 S3。
    主要資料來源：S3 SQLite (tw_market.db)
    回傳 (成功?, 訊息)
    """
    try:
        logger.info("開始 TW 市場掃描（SQLite）...")

        # 1. 確保 SQLite 已下載（每日一次）
        _ensure_tw_db(fs)
        if not TW_DB_LOCAL.exists():
            return False, "tw_market.db 不存在，請確認 ETL pipeline 已上傳"

        # 2. 一次 SQL 撈全市場 FETCH_DAYS 天價格 + 股票資訊
        price_df, info_df = _query_tw_db()
        if price_df.empty:
            return False, "tw_market.db 中找不到價格資料"
        logger.info(f"SQLite 讀取：{len(price_df)} 筆 / {price_df['stock_id'].nunique()} 支股票")

        # 3. 計算技術特徵
        scan_df = _compute_tw_scan(price_df, info_df)
        if scan_df.empty:
            return False, "特徵計算結果為空（資料可能不足）"
        logger.info(f"特徵計算完成：{len(scan_df)} 支有效股票")

        # 4. 存至 /tmp（固定路徑 + 日期記錄）
        _save_to_local(scan_df, TW_SCAN_LOCAL)
        TW_SCAN_DATE_FILE.write_text(date.today().isoformat())
        return True, f"台股掃描完成：{len(scan_df)} 支（有效期 {TW_SCAN_MAX_AGE_DAYS} 天）"

    except Exception as e:
        logger.exception("TW scan 失敗")
        return False, f"發生錯誤：{e}"


def enrich_us_industry(fs, symbols: list[str]) -> pd.DataFrame:
    """
    用 Tiingo 補充 US 股票的 industry / sector。
    自動快取至 S3，只呼叫尚未快取的 symbol。
    """
    if not _tiingo_key():
        return pd.DataFrame(columns=["symbol", "name", "sector", "industry"])

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
            time.sleep(0.5)

    if new_rows:
        combined = pd.concat([cached, pd.DataFrame(new_rows)], ignore_index=True)
        combined = combined.drop_duplicates("symbol", keep="last")
        _save_to_s3(fs, combined, US_INDUSTRY_S3)
        return combined

    return cached


# ════════════════════════════════════════════════════════
# SQLite 資料讀取
# ════════════════════════════════════════════════════════

def _ensure_tw_db(fs) -> None:
    """
    確保本地 SQLite 快取是今天的版本。
    與 loader.py 的 _ensure_tw_db 邏輯相同，但接受 fs 參數。
    """
    today_str = date.today().isoformat()

    if TW_DB_LOCAL.exists() and TW_DB_DATE_FILE.exists():
        if TW_DB_DATE_FILE.read_text().strip() == today_str:
            logger.info("tw_market.db 已是今日版本，跳過下載")
            return

    logger.info("下載 tw_market.db（約 370MB）...")
    import boto3
    import botocore
    kwargs = {"region_name": config.AWS_REGION}
    if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"]     = config.AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = config.AWS_SECRET_ACCESS_KEY
    s3 = boto3.client("s3", **kwargs)
    s3.download_file(BUCKET, TW_DB_S3_KEY, str(TW_DB_LOCAL))
    TW_DB_DATE_FILE.write_text(today_str)
    logger.info("tw_market.db 下載完成")


def _query_tw_db() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    一次 SQL query 取：
    - price_df: 全市場近 FETCH_DAYS 天價格（stock_id, date, adj_close, volume）
    - info_df:  股票清單（stock_id, stock_name, industry_category）
    """
    start_date = (date.today() - timedelta(days=FETCH_DAYS)).isoformat()
    conn = sqlite3.connect(str(TW_DB_LOCAL))
    try:
        price_df = pd.read_sql(
            """
            SELECT stock_id, date, adj_close, volume
            FROM   price
            WHERE  date >= ?
            ORDER  BY stock_id, date
            """,
            conn,
            params=(start_date,),
        )
        info_df = pd.read_sql(
            "SELECT stock_id, stock_name, industry_category FROM stock_info",
            conn,
        )
    finally:
        conn.close()

    price_df["date"] = pd.to_datetime(price_df["date"])
    return price_df, info_df


# ════════════════════════════════════════════════════════
# 技術特徵計算（TW）
# ════════════════════════════════════════════════════════

def _compute_tw_scan(price_df: pd.DataFrame, info_df: pd.DataFrame) -> pd.DataFrame:
    """
    對每支股票計算技術特徵，回傳 scan DataFrame。
    price_df 必須有欄位：stock_id, date, adj_close, volume
    info_df  必須有欄位：stock_id, stock_name, industry_category
    """
    # stock_id → info lookup
    info_map: dict[str, dict] = {}
    if not info_df.empty:
        for _, r in info_df.iterrows():
            sid = str(r["stock_id"])
            industry = str(r.get("industry_category") or "未分類").strip() or "未分類"
            info_map[sid] = {
                "name":     str(r.get("stock_name") or sid),
                "industry": industry,
            }

    rows = []
    groups = price_df.groupby("stock_id", sort=False)
    total  = len(groups)

    for idx, (stock_id, grp) in enumerate(groups):
        sid = str(stock_id)
        grp = grp.sort_values("date").reset_index(drop=True)

        if len(grp) < 20:
            continue

        try:
            feats = compute_features(grp)
        except Exception as e:
            logger.debug(f"{sid} 特徵計算失敗：{e}")
            continue

        last     = grp.iloc[-1]
        close    = float(last["adj_close"])
        vol      = float(last["volume"]) if "volume" in grp.columns else 0.0
        info     = info_map.get(sid, {"name": sid, "industry": "未分類"})
        avg_vol  = feats["avg_volume_20d"]
        ma50     = feats["ma50"]
        ma200    = feats["ma200"]

        above_ma50  = close > ma50  if ma50  > 0 else False
        above_ma200 = close > ma200 if ma200 > 0 else False
        ma50_above  = ma50 > ma200  if ma50  > 0 and ma200 > 0 else False

        trend = (
            "bull"  if (above_ma50 and above_ma200 and ma50_above) else
            "bear"  if (not above_ma50 and not above_ma200 and not ma50_above) else
            "mixed"
        )

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

        if idx % 200 == 0:
            logger.info(f"  特徵計算進度：{idx}/{total}")

    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════
# Tiingo — US industry metadata
# ════════════════════════════════════════════════════════

def _tiingo_key() -> str:
    return config.fresh("TIINGO_API_KEY")


def _finmind_token() -> str:
    return config.fresh("FINMIND_API_TOKEN") or config.fresh("FINMIND_API_KEY")


def _fetch_tiingo_meta(symbol: str) -> Optional[dict]:
    try:
        url  = TIINGO_META_URL.format(ticker=symbol.lower())
        resp = requests.get(
            url,
            headers={"Authorization": f"Token {_tiingo_key()}"},
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
            "industry": d.get("industry") or d.get("sector") or "未分類",
        }
    except Exception:
        return None


def _load_us_industry_cache(fs) -> pd.DataFrame:
    try:
        with fs.open(f"s3://{BUCKET}/{US_INDUSTRY_S3}", "rb") as f:
            return pd.read_parquet(f)
    except Exception:
        return pd.DataFrame(columns=["symbol", "name", "sector", "industry"])


# ════════════════════════════════════════════════════════
# S3 工具
# ════════════════════════════════════════════════════════

def _check_s3_date(fs, s3_key: str) -> bool:
    """S3 檔案的 date 欄位最大值是否 >= 今天"""
    try:
        with fs.open(f"s3://{BUCKET}/{s3_key}", "rb") as f:
            df = pd.read_parquet(f, columns=["date"])
        if not df.empty:
            return pd.to_datetime(df["date"]).max().date() >= date.today()
    except Exception:
        pass
    return False


def _save_to_s3(fs, df: pd.DataFrame, s3_key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    with fs.open(f"s3://{BUCKET}/{s3_key}", "wb") as f:
        f.write(buf.read())
    logger.info(f"已存至 s3://{BUCKET}/{s3_key}（{len(df)} 列）")


def _save_to_local(df: pd.DataFrame, path: Path) -> None:
    """存至本地 /tmp（無需 S3 寫入權限）"""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(path), index=False, engine="pyarrow")
    logger.info(f"已存至本地 {path}（{len(df)} 列）")
