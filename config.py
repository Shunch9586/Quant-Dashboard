import os
from dotenv import load_dotenv

load_dotenv()

# ── S3 設定 ──────────────────────────────────────────────
S3_BUCKET          = os.getenv("S3_BUCKET_NAME", "your-bucket-name")
S3_PRICE_PREFIX    = os.getenv("S3_PRICE_PREFIX", "price/")
S3_FEATURE_PREFIX  = os.getenv("S3_FEATURE_PREFIX", "feature/")
S3_DECISION_PREFIX = os.getenv("S3_DECISION_PREFIX", "decision/")
AWS_REGION         = os.getenv("AWS_REGION", "ap-northeast-1")

# ── 資料來源切換 ─────────────────────────────────────────
# True  = 使用 mock 假資料（開發 / 展示用，不需要連 S3）
# False = 使用真實 S3 資料
USE_MOCK_DATA = os.getenv("USE_MOCK_DATA", "true").lower() == "true"

# ── 即時報價 API ─────────────────────────────────────────
TIINGO_API_KEY  = os.getenv("TIINGO_API_KEY", "")
FINMIND_API_KEY = os.getenv("FINMIND_API_KEY", "")

# ── Dashboard 設定 ───────────────────────────────────────
DRAWDOWN_ALERT_THRESHOLD = float(os.getenv("DRAWDOWN_ALERT_THRESHOLD", "-0.15"))  # -15%
LIVE_PRICE_CACHE_TTL     = int(os.getenv("LIVE_PRICE_CACHE_TTL", "300"))          # 5 分鐘
