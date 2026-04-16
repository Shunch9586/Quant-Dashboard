"""
市場掃描資料載入器
- USE_MOCK_DATA=true  → 產生假資料（開發用）
- USE_MOCK_DATA=false → 從 S3 讀取 ETL pipeline 輸出的 scan.parquet

S3 路徑：s3://hanetic-quant-data-2026/data_lake/market_scan/latest/scan.parquet
Schema 由 ETL pipeline 的 full_market_scan.py 決定，此處只做讀取與型別確保。
"""

import logging
import random
from datetime import date, timedelta

import pandas as pd

import config

logger = logging.getLogger(__name__)

S3_SCAN_PATH = f"s3://{config.S3_BUCKET_NAME}/data_lake/market_scan/latest/scan.parquet"

# 欄位型別保證（ETL 輸出不一定嚴格，這裡做防禦）
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


def load_market_scan() -> pd.DataFrame:
    """
    載入全市場掃描結果。
    回傳 DataFrame，每列一支股票。
    如果 S3 尚無資料（ETL 尚未產出），回傳空 DataFrame + 顯示提示。
    """
    if config.USE_MOCK_DATA:
        return _mock_scan()
    return _load_from_s3()


# ════════════════════════════════════════════════════════
# 真實資料（S3）
# ════════════════════════════════════════════════════════

def _load_from_s3() -> pd.DataFrame:
    try:
        import s3fs
        fs = s3fs.S3FileSystem(
            key=config.AWS_ACCESS_KEY_ID or None,
            secret=config.AWS_SECRET_ACCESS_KEY or None,
        )
        with fs.open(S3_SCAN_PATH, "rb") as f:
            df = pd.read_parquet(f)
        df = _normalize(df)
        logger.info(f"市場掃描載入完成：{len(df)} 支股票")
        return df
    except FileNotFoundError:
        logger.warning("market_scan/latest/scan.parquet 尚不存在，ETL pipeline 尚未產出")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"市場掃描 S3 讀取失敗：{e}")
        return pd.DataFrame()


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """確保欄位存在且型別正確"""
    for col, dtype in _DTYPE_MAP.items():
        if col not in df.columns:
            if dtype == "bool":
                df[col] = False
            elif dtype == "str":
                df[col] = ""
            else:
                df[col] = 0.0
        else:
            try:
                df[col] = df[col].astype(dtype)
            except Exception:
                pass
    return df


# ════════════════════════════════════════════════════════
# Mock 資料（開發用）
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
    ("3231", "緯創", "電腦及週邊"), ("5347", "世界", "半導體"),
    ("2357", "華碩", "電腦及週邊"), ("2376", "技嘉", "電腦及週邊"),
    ("3045", "台灣大", "通信網路"), ("4904", "遠傳", "通信網路"),
    ("2002", "中鋼", "鋼鐵"), ("2603", "長榮", "航運"),
    ("2609", "陽明", "航運"), ("6523", "達運", "低軌衛星"),
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

    for symbol, name, industry in _TW_SYMBOLS + _US_SYMBOLS:
        market = "TW" if len(symbol) == 4 and symbol.isdigit() or symbol.startswith("0") else "US"
        # 也處理 ETF 代號
        if symbol.startswith("0"):
            market = "TW"

        close = random.uniform(50, 1200) if market == "TW" else random.uniform(10, 900)
        ma50  = close * random.uniform(0.88, 1.08)
        ma200 = ma50  * random.uniform(0.85, 1.10)
        score = random.uniform(20, 95)
        score_delta = random.uniform(-12, 12)
        vol_ratio   = random.uniform(0.3, 4.5)
        dist_52w    = random.uniform(-0.40, 0.02)
        momentum_20d = random.uniform(-0.15, 0.20)

        above_ma50   = close > ma50
        above_ma200  = close > ma200
        ma50_above   = ma50 > ma200

        if above_ma50 and above_ma200 and ma50_above:
            trend = "bull"
        elif not above_ma50 and not above_ma200 and not ma50_above:
            trend = "bear"
        else:
            trend = "mixed"

        rows.append({
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
            "momentum_20d":       round(momentum_20d, 4),
            "trend_state":        trend,
            "above_ma50":         above_ma50,
            "above_ma200":        above_ma200,
            "ma50_above_ma200":   ma50_above,
            "near_52w_high":      dist_52w > -0.05,
            "high_volume":        vol_ratio > 2.0,
            "vcp_flag":           random.random() < 0.08,
            "breakout_flag":      random.random() < 0.10,
            "reversal_flag":      random.random() < 0.05,
        })

    return pd.DataFrame(rows)
