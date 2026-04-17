"""
資料載入器（真實版）
- USE_MOCK_DATA=true  → mock_data.py 假資料（開發用）
- USE_MOCK_DATA=false → 從 S3 + GSheet 讀取真實資料

真實資料流：
  1. GSheet（公開 CSV）→ 持倉清單
  2. TW 價格 → S3 SQLite (tw_market.db)
  3. US 價格 → S3 Hive Parquet (data_lake/price/market=US/...)
  4. features.py → 計算 MA50/MA200/Score
  5. decision.py → 計算 should_exit / exit_grade
"""

import io
import os
import time
import sqlite3
import urllib.request
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa
import s3fs

import config
from data.models import DecisionRecord, PortfolioSummaryData, HistoryPoint
from data import mock_data
from data.features import compute_features, compute_history_scores
from data.decision import compute_decision

logger = logging.getLogger(__name__)

# ── GSheet 設定 ───────────────────────────────────────────
GSHEET_ID  = "1WKXBFLddqlQeel5Et0vl2-7KvuWo4LJ21m3rK5ThUkE"
GSHEET_GID = "1030323616"

# ── S3 設定 ───────────────────────────────────────────────
BUCKET         = "hanetic-quant-data-2026"
TW_DB_S3_KEY   = "db/latest/tw_market.db"
US_PRICE_S3_PREFIX = f"s3://{BUCKET}/data_lake/price/market=US/"
TW_DB_LOCAL    = Path("/tmp/tw_market_cache.db")
TW_DB_DATE_FILE = Path("/tmp/tw_market_cache_date.txt")

# 歷史天數（計算 MA200 需要 200+ 天）
HISTORY_DAYS = 250


# ════════════════════════════════════════════════════════
# 公開介面（views 層只呼叫這三個函數）
# ════════════════════════════════════════════════════════

def load_positions() -> list[DecisionRecord]:
    if config.USE_MOCK_DATA:
        return mock_data.get_mock_positions()
    return _load_positions_real()


def load_portfolio_summary(records: list[DecisionRecord]) -> PortfolioSummaryData:
    return mock_data.get_mock_portfolio_summary(records)


def load_history(symbol: str, market: str = "TW", days: int = 120,
                 entry_price: float = 0.0) -> list[HistoryPoint]:
    if config.USE_MOCK_DATA:
        return mock_data.get_mock_history(symbol, days)
    return _load_history_real(symbol, market, days, entry_price)


# ════════════════════════════════════════════════════════
# 真實資料載入
# ════════════════════════════════════════════════════════

def _load_positions_real() -> list[DecisionRecord]:
    """
    主流程：GSheet → 價格 → Features → Decision → DecisionRecord
    """
    inventory = _read_gsheet()
    if inventory.empty:
        logger.warning("GSheet 讀取失敗，改用 mock 資料")
        return mock_data.get_mock_positions()

    # 確保 TW SQLite 已快取
    _ensure_tw_db()

    records = []
    for _, row in inventory.iterrows():
        symbol       = str(row["symbol"]).strip()
        market       = str(row["market"]).strip().upper()
        entry_price  = float(row["entry_price"])
        position_size = float(row["position_size"])
        stop_price   = row.get("stop_price")
        entry_date_raw = str(row["entry_date"])
        position_type  = str(row.get("position_type", "manual"))

        # 解析日期
        try:
            entry_date = _parse_date(entry_date_raw)
        except Exception:
            logger.warning(f"{symbol}: 日期格式無法解析 ({entry_date_raw})，略過")
            continue

        # 讀取價格歷史
        try:
            if market == "TW":
                price_df = _read_tw_prices(symbol, HISTORY_DAYS)
                sector   = _get_tw_sector(symbol)
            else:
                price_df = _read_us_prices(symbol, HISTORY_DAYS)
                sector   = "US"
        except Exception as e:
            logger.warning(f"{symbol}: 價格讀取失敗 ({e})，略過")
            continue

        if price_df is None or len(price_df) < 5:
            logger.warning(f"{symbol}: 歷史資料不足，略過")
            continue

        curr_price = float(price_df.sort_values("date").iloc[-1]["adj_close"])

        # 計算 Features
        feats = compute_features(price_df)

        # 計算 Decision
        dec = compute_decision(curr_price, stop_price, feats)

        # 衍生欄位
        roi           = (curr_price - entry_price) / entry_price
        position_value = curr_price * position_size
        unrealized_pnl = (curr_price - entry_price) * position_size

        records.append(DecisionRecord(
            symbol=symbol,
            market=market,
            position_type=position_type,
            entry_price=entry_price,
            entry_date=entry_date,
            shares=position_size,
            stop_price=float(stop_price) if _is_valid_number(stop_price) else 0.0,
            entry_score=0.0,               # GSheet 目前無此欄位
            curr_price=curr_price,
            roi=round(roi, 4),
            position_value=round(position_value, 2),
            unrealized_pnl=round(unrealized_pnl, 2),
            ma50=feats["ma50"],
            ma200=feats["ma200"],
            adj_close_to_ma50_ratio=feats["adj_close_to_ma50_ratio"],
            momentum_raw=feats["momentum_raw"],
            current_score=feats["current_score"],
            score_delta=feats["score_delta"],
            sector=sector,
            avg_volume_20d=feats["avg_volume_20d"],
            should_exit=dec["should_exit"],
            exit_grade=dec["exit_grade"],
            exit_reason_code=dec["exit_reason_code"],
            exit_reason_detail=dec["exit_reason_detail"],
            exit_price=dec["exit_price"],
            distance_to_stop=dec["distance_to_stop"],
            distance_to_ma50=dec["distance_to_ma50"],
        ))

    return records


def _load_history_real(symbol: str, market: str, days: int,
                       entry_price: float = 0.0) -> list[HistoryPoint]:
    """載入某個 symbol 的歷史時間序列"""
    _ensure_tw_db()

    try:
        if market == "TW":
            price_df = _read_tw_prices(symbol, days + 50)
        else:
            price_df = _read_us_prices(symbol, days + 50)
    except Exception as e:
        logger.warning(f"History load failed for {symbol}: {e}")
        return mock_data.get_mock_history(symbol, days)

    if price_df is None or price_df.empty:
        return mock_data.get_mock_history(symbol, days)

    df = compute_history_scores(price_df).tail(days)

    result = []
    for _, row in df.iterrows():
        curr_price = float(row["adj_close"])
        # 用進場價計算當日 ROI（進場價未知時顯示 0）
        roi = (curr_price - entry_price) / entry_price if entry_price > 0 else 0.0
        result.append(HistoryPoint(
            date=_parse_date(str(row["date"])),
            roi=round(roi, 4),
            curr_price=curr_price,
            score=float(row.get("score", 50.0)),
            score_delta=float(row.get("score_delta", 0.0)),
            should_exit=False,   # 歷史 Decision 紀錄尚未建立
            exit_grade="",
        ))
    return result


# ════════════════════════════════════════════════════════
# GSheet 讀取
# ════════════════════════════════════════════════════════

def _read_gsheet() -> pd.DataFrame:
    """從公開 GSheet 讀取持倉清單（CSV export，不需要 API 金鑰）"""
    url = f"https://docs.google.com/spreadsheets/d/{GSHEET_ID}/export?format=csv&gid={GSHEET_GID}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            df = pd.read_csv(io.BytesIO(resp.read()))

        # 只保留有效欄位
        keep = ["status", "symbol", "market", "entry_price",
                "position_size", "entry_date", "stop_price", "position_type"]
        df = df[[c for c in keep if c in df.columns]]

        # 過濾 status = open
        df = df[df["status"].str.lower() == "open"].reset_index(drop=True)
        return df

    except Exception as e:
        logger.error(f"GSheet 讀取失敗: {e}")
        return pd.DataFrame()


# ════════════════════════════════════════════════════════
# TW 價格（SQLite）
# ════════════════════════════════════════════════════════

def _get_s3_client():
    """建立 S3 client（優先使用 config 裡的金鑰，其次 IAM Role）"""
    kwargs = {"region_name": config.AWS_REGION}
    if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"]     = config.AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = config.AWS_SECRET_ACCESS_KEY
    return boto3.client("s3", **kwargs)


def _get_s3fs():
    """建立 s3fs FileSystem（供 pyarrow dataset 使用）"""
    if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
        return s3fs.S3FileSystem(
            key=config.AWS_ACCESS_KEY_ID,
            secret=config.AWS_SECRET_ACCESS_KEY,
        )
    return s3fs.S3FileSystem()   # 使用 IAM Role / 本機 ~/.aws


def _ensure_tw_db() -> None:
    """確保本地 SQLite 快取是今天的版本，否則重新下載"""
    today_str = date.today().isoformat()

    if TW_DB_LOCAL.exists() and TW_DB_DATE_FILE.exists():
        cached_date = TW_DB_DATE_FILE.read_text().strip()
        if cached_date == today_str:
            return   # 已是今日版本，不重新下載

    logger.info("下載 tw_market.db（約 370MB，首次或每日一次）...")
    s3 = _get_s3_client()
    s3.download_file(BUCKET, TW_DB_S3_KEY, str(TW_DB_LOCAL))
    TW_DB_DATE_FILE.write_text(today_str)
    logger.info("tw_market.db 下載完成")


def _read_tw_prices(symbol: str, days: int) -> pd.DataFrame | None:
    """從本地快取 SQLite 讀取台股歷史價格"""
    if not TW_DB_LOCAL.exists():
        _ensure_tw_db()

    conn = sqlite3.connect(str(TW_DB_LOCAL))
    try:
        # 先確認 symbol 存在
        check = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM price WHERE stock_id = ?",
            conn, params=(symbol,)
        )
        if check.iloc[0]["cnt"] == 0:
            logger.warning(f"TW symbol {symbol} 在 SQLite 中找不到")
            return None

        df = pd.read_sql(
            """
            SELECT date, stock_id as symbol, adj_close, volume
            FROM price
            WHERE stock_id = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            conn,
            params=(symbol, days),
        )
        df = df.sort_values("date").reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"TW price read failed for {symbol}: {e}")
        return None
    finally:
        conn.close()


def _get_tw_sector(symbol: str) -> str:
    """從 SQLite 取得台股產業別"""
    if not TW_DB_LOCAL.exists():
        return "TW"
    try:
        conn = sqlite3.connect(str(TW_DB_LOCAL))
        df = pd.read_sql(
            "SELECT industry_category FROM stock_info WHERE stock_id = ?",
            conn, params=(symbol,)
        )
        conn.close()
        if not df.empty and df.iloc[0]["industry_category"]:
            return str(df.iloc[0]["industry_category"])
    except Exception:
        pass
    return "TW"


# ════════════════════════════════════════════════════════
# US 價格（Hive Parquet）
# ════════════════════════════════════════════════════════

def _read_us_prices(symbol: str, days: int) -> pd.DataFrame | None:
    """從 S3 Hive Parquet 讀取美股歷史價格，只取指定 symbol 的資料列"""
    try:
        fs = _get_s3fs()
        dataset = ds.dataset(
            f"{BUCKET}/data_lake/price/market=US/",
            filesystem=fs,
            format="parquet",
            partitioning=ds.partitioning(
                pa.schema([
                    ("year",  pa.int32()),
                    ("month", pa.int32()),
                    ("day",   pa.int32()),
                ]),
                flavor="hive",
            ),
        )

        table = dataset.to_table(
            filter=ds.field("symbol") == symbol,
            columns=["date", "symbol", "adj_close", "volume"],
        )
        df = table.to_pandas().sort_values("date").tail(days).reset_index(drop=True)

        if df.empty:
            logger.warning(f"US symbol {symbol} 在 Parquet 中找不到")
            return None

        return df

    except Exception as e:
        logger.error(f"US price read failed for {symbol}: {e}")
        return None


# ════════════════════════════════════════════════════════
# 輔助函數
# ════════════════════════════════════════════════════════

def _parse_date(s: str) -> date:
    """解析多種日期格式 → date 物件"""
    s = s.strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"無法解析日期：{s}")


def _is_valid_number(v) -> bool:
    """判斷是否為有效數字（排除 NaN / None）"""
    if v is None:
        return False
    try:
        f = float(v)
        return not (f != f)   # NaN check
    except (TypeError, ValueError):
        return False
