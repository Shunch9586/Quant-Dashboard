"""
市場掃描資料載入器

TW：由 market_scan_fetcher.py 呼叫 FinMind API 產生，存於 S3
US：由 ETL pipeline 產生，存於 S3（約 3,000 支 pre-filtered）

S3 路徑嘗試順序（每個 market 各自）：
  1. data_lake/market_scan/market=TW|US/latest/scan.parquet  ← 主路徑
  2. data_lake/market_scan/market=TW|US/scan.parquet         ← 備用
  3. data_lake/market_scan/latest/scan.parquet               ← ETL 合併 fallback

US industry 由 Tiingo API 補充，快取於：
  data_lake/market_scan/us_industry_cache.parquet
"""

import logging
import random
from datetime import date, timedelta
from typing import Optional

import pandas as pd

import config

logger = logging.getLogger(__name__)

BUCKET = config.S3_BUCKET_NAME

# ── S3 路徑定義 ─────────────────────────────────────────
# 每個 market 各試兩個路徑；最後還有一個合併路徑兜底
_S3_PATHS_TW = [
    f"s3://{BUCKET}/data_lake/market_scan/market=TW/latest/scan.parquet",
    f"s3://{BUCKET}/data_lake/market_scan/market=TW/scan.parquet",
]
_S3_PATHS_US = [
    f"s3://{BUCKET}/data_lake/market_scan/market=US/latest/scan.parquet",
    f"s3://{BUCKET}/data_lake/market_scan/market=US/scan.parquet",
]
_S3_PATH_COMBINED = f"s3://{BUCKET}/data_lake/market_scan/latest/scan.parquet"

# ── 欄位型別保證 ─────────────────────────────────────────
_DTYPE_MAP = {
    "symbol":             "str",
    "name":               "str",
    "market":             "str",
    "industry":           "str",
    "close":              "float64",
    "volume":             "float64",
    "avg_volume_20d":     "float64",
    "ma50":               "float64",
    "ma200":              "float64",
    "score":              "float64",
    "score_delta":        "float64",
    "vol_ratio_20d":      "float64",
    "dist_to_52w_high":   "float64",
    "adj_close_to_ma50":  "float64",
    "adj_close_to_ma200": "float64",
    "momentum_20d":       "float64",
    "trend_state":        "str",
    "above_ma50":         "bool",
    "above_ma200":        "bool",
    "ma50_above_ma200":   "bool",
    "near_52w_high":      "bool",
    "high_volume":        "bool",
    "vcp_flag":           "bool",
    "breakout_flag":      "bool",
    "reversal_flag":      "bool",
}


# ════════════════════════════════════════════════════════
# 公開介面
# ════════════════════════════════════════════════════════

def load_market_scan() -> pd.DataFrame:
    """
    載入全市場掃描結果（TW + US 合併）。
    - TW 和 US 各自從獨立路徑讀取，找不到才 fallback 到合併路徑
    - 任一市場有資料就算成功，兩個都有就合併
    - 完全找不到才回傳空 DataFrame
    """
    if config.USE_MOCK_DATA:
        return _mock_scan()
    return _load_from_s3()


# ════════════════════════════════════════════════════════
# S3 讀取（多路徑策略）
# ════════════════════════════════════════════════════════

def _load_from_s3() -> pd.DataFrame:
    import s3fs
    fs = s3fs.S3FileSystem(
        key=config.AWS_ACCESS_KEY_ID or None,
        secret=config.AWS_SECRET_ACCESS_KEY or None,
    )

    parts = []

    # 1. 嘗試讀取 TW 分開路徑
    tw_df = _try_paths(fs, _S3_PATHS_TW, market_tag="TW")
    if tw_df is not None:
        parts.append(tw_df)
        logger.info(f"TW 市場掃描載入：{len(tw_df)} 支")

    # 2. 嘗試讀取 US 分開路徑
    us_df = _try_paths(fs, _S3_PATHS_US, market_tag="US")
    if us_df is not None:
        parts.append(us_df)
        logger.info(f"US 市場掃描載入：{len(us_df)} 支（ETL pre-filtered）")

    # 3. 若兩個分開路徑都找不到，嘗試合併路徑
    if not parts:
        combined = _try_paths(fs, [_S3_PATH_COMBINED], market_tag=None)
        if combined is not None:
            parts.append(combined)
            logger.info(f"合併市場掃描載入：{len(combined)} 支")

    if not parts:
        logger.warning("所有 S3 路徑均找不到 scan.parquet，請確認 ETL pipeline 已執行")
        return pd.DataFrame()

    # 4. 合併 + 去重（以 symbol 為主鍵，同一支股票以後出現的覆蓋前面）
    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)

    df = _normalize(df)
    logger.info(f"市場掃描合併完成：TW {len(df[df['market']=='TW'])} + US {len(df[df['market']=='US'])} = {len(df)} 支")
    return df


def _try_paths(fs, paths: list[str], market_tag: Optional[str]) -> Optional[pd.DataFrame]:
    """依序嘗試 paths，第一個成功的就回傳；全部失敗回傳 None"""
    for path in paths:
        try:
            with fs.open(path, "rb") as f:
                df = pd.read_parquet(f)
            # 若 market 欄位不存在，用 market_tag 補填
            if market_tag and ("market" not in df.columns or df["market"].isnull().all()):
                df["market"] = market_tag
            return df
        except FileNotFoundError:
            logger.debug(f"路徑不存在（跳過）：{path}")
        except Exception as e:
            logger.warning(f"讀取失敗 {path}：{e}")
    return None


# ════════════════════════════════════════════════════════
# 正規化（型別 + 空值處理）
# ════════════════════════════════════════════════════════

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """確保欄位存在且型別正確"""
    for col, dtype in _DTYPE_MAP.items():
        if col not in df.columns:
            df[col] = False if dtype == "bool" else ("" if dtype == "str" else 0.0)
        else:
            try:
                df[col] = df[col].astype(dtype)
            except Exception:
                pass

    # 空白 / NaN 產業別 → 「未分類」
    if "industry" in df.columns:
        df["industry"] = df["industry"].fillna("未分類")
        df.loc[df["industry"].str.strip().isin(["", "nan", "None"]), "industry"] = "未分類"

    # 空白 name → 用 symbol 代替
    if "name" in df.columns and "symbol" in df.columns:
        mask = df["name"].fillna("").str.strip() == ""
        df.loc[mask, "name"] = df.loc[mask, "symbol"]

    # market 大寫統一
    if "market" in df.columns:
        df["market"] = df["market"].str.upper().str.strip()

    return df


# ════════════════════════════════════════════════════════
# Mock 資料（開發用，USE_MOCK_DATA=true）
# ════════════════════════════════════════════════════════

_TW_INDUSTRIES = [
    "半導體", "ETF", "電子零組件", "光電", "電腦及週邊",
    "通信網路", "金融保險", "生技醫療", "塑膠", "鋼鐵",
    "航運", "低軌衛星", "電動車", "綠能", "食品",
]

_TW_SYMBOLS = [
    ("2330", "台積電", "半導體"), ("2317", "鴻海", "電子零組件"),
    ("2454", "聯發科", "半導體"), ("2308", "台達電", "電子零組件"),
    ("2382", "廣達", "電腦及週邊"), ("3008", "大立光", "光電"),
    ("2412", "中華電", "通信網路"), ("2881", "富邦金", "金融保險"),
    ("0050", "元大台灣50", "ETF"), ("0056", "元大高股息", "ETF"),
    ("6669", "緯穎", "電腦及週邊"), ("2379", "瑞昱", "半導體"),
    ("3034", "聯詠", "半導體"), ("2303", "聯電", "半導體"),
    ("4938", "和碩", "電子零組件"), ("2395", "研華", "電腦及週邊"),
    ("2337", "旺宏", "半導體"), ("2408", "南亞科", "半導體"),
    ("2353", "宏碁", "電腦及週邊"), ("6488", "環球晶", "半導體"),
    ("3231", "緯創", "電腦及週邊"), ("5347", "世界先進", "半導體"),
    ("2357", "華碩", "電腦及週邊"), ("2376", "技嘉", "電腦及週邊"),
    ("3045", "台灣大", "通信網路"), ("4904", "遠傳", "通信網路"),
    ("2002", "中鋼", "鋼鐵"), ("2603", "長榮", "航運"),
    ("2609", "陽明", "航運"), ("6523", "達運精密", "低軌衛星"),
    ("4916", "亞太電", "低軌衛星"), ("3673", "TPK-KY", "光電"),
    ("1301", "台塑", "塑膠"), ("1303", "南亞", "塑膠"),
    ("2886", "兆豐金", "金融保險"), ("2891", "中信金", "金融保險"),
    ("2884", "玉山金", "金融保險"), ("4763", "材料-KY", "生技醫療"),
    ("6550", "北極星藥業", "生技醫療"), ("1216", "統一", "食品"),
]

_US_SYMBOLS = [
    ("NVDA", "NVIDIA", "Semiconductors"), ("AAPL", "Apple", "Technology"),
    ("MSFT", "Microsoft", "Technology"), ("GOOGL", "Alphabet", "Technology"),
    ("AMZN", "Amazon", "Consumer Discretionary"), ("META", "Meta", "Technology"),
    ("TSLA", "Tesla", "Consumer Discretionary"), ("AMD", "AMD", "Semiconductors"),
    ("AVGO", "Broadcom", "Semiconductors"), ("TSM", "TSMC ADR", "Semiconductors"),
    ("ASML", "ASML", "Semiconductors"), ("QCOM", "Qualcomm", "Semiconductors"),
    ("AMAT", "Applied Materials", "Semiconductors"), ("LRCX", "Lam Research", "Semiconductors"),
    ("PANW", "Palo Alto", "Cybersecurity"), ("CRWD", "CrowdStrike", "Cybersecurity"),
    ("NET", "Cloudflare", "Cloud"), ("SNOW", "Snowflake", "Cloud"),
    ("PLTR", "Palantir", "AI/Data"), ("SMCI", "Super Micro", "Servers"),
]


def _mock_scan() -> pd.DataFrame:
    random.seed(42)
    rows = []

    for symbol, name, industry in _TW_SYMBOLS:
        rows.append(_make_mock_row(symbol, name, industry, "TW"))

    for symbol, name, industry in _US_SYMBOLS:
        rows.append(_make_mock_row(symbol, name, industry, "US"))

    return pd.DataFrame(rows)


def _make_mock_row(symbol: str, name: str, industry: str, market: str) -> dict:
    close = random.uniform(50, 1200) if market == "TW" else random.uniform(10, 900)
    ma50  = close * random.uniform(0.88, 1.08)
    ma200 = ma50  * random.uniform(0.85, 1.10)
    score = random.uniform(20, 95)
    score_delta = random.uniform(-12, 12)
    vol_ratio   = random.uniform(0.3, 4.5)
    dist_52w    = random.uniform(-0.40, 0.02)

    above_ma50  = close > ma50
    above_ma200 = close > ma200
    ma50_above  = ma50 > ma200

    if above_ma50 and above_ma200 and ma50_above:
        trend = "bull"
    elif not above_ma50 and not above_ma200 and not ma50_above:
        trend = "bear"
    else:
        trend = "mixed"

    return {
        "symbol":             symbol,
        "name":               name,
        "market":             market,
        "industry":           industry,
        "date":               date.today(),
        "close":              round(close, 2),
        "volume":             round(random.uniform(1e5, 5e7)),
        "avg_volume_20d":     round(random.uniform(5e5, 2e7)),
        "ma50":               round(ma50, 2),
        "ma200":              round(ma200, 2),
        "score":              round(score, 1),
        "score_delta":        round(score_delta, 1),
        "vol_ratio_20d":      round(vol_ratio, 2),
        "dist_to_52w_high":   round(dist_52w, 4),
        "adj_close_to_ma50":  round(close / ma50 - 1, 4),
        "adj_close_to_ma200": round(close / ma200 - 1, 4),
        "momentum_20d":       round(random.uniform(-0.15, 0.20), 4),
        "trend_state":        trend,
        "above_ma50":         above_ma50,
        "above_ma200":        above_ma200,
        "ma50_above_ma200":   ma50_above,
        "near_52w_high":      dist_52w > -0.05,
        "high_volume":        vol_ratio > 2.0,
        "vcp_flag":           random.random() < 0.08,
        "breakout_flag":      random.random() < 0.10,
        "reversal_flag":      random.random() < 0.05,
    }
