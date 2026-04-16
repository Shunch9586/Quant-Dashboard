"""
設定檔
優先順序：Streamlit secrets → 環境變數 → 預設值

本機開發：值來自 .streamlit/secrets.toml（不推上 GitHub）
Streamlit Cloud：值來自 App Settings → Secrets
"""

import os

def _get(key: str, default: str = "") -> str:
    """先讀 Streamlit secrets，再讀環境變數，最後用預設值"""
    try:
        import streamlit as st
        val = st.secrets.get(key)
        if val:
            return str(val)
    except Exception:
        pass
    return os.getenv(key, default)


# ── AWS S3 ───────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = _get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = _get("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = _get("AWS_REGION", "ap-northeast-1")
S3_BUCKET_NAME        = _get("S3_BUCKET_NAME", "hanetic-quant-data-2026")

# ── 資料來源切換 ─────────────────────────────────────────
# "true"  = mock 假資料（不需 S3，開發展示用）
# "false" = 真實 S3 資料
USE_MOCK_DATA = _get("USE_MOCK_DATA", "true").lower() == "true"

# ── Dashboard 設定 ───────────────────────────────────────
DRAWDOWN_ALERT_THRESHOLD = float(_get("DRAWDOWN_ALERT_THRESHOLD", "-0.15"))
LIVE_PRICE_CACHE_TTL     = int(_get("LIVE_PRICE_CACHE_TTL", "300"))

# ── 即時報價 API（選配）──────────────────────────────────
TIINGO_API_KEY  = _get("TIINGO_API_KEY")
FINMIND_API_KEY = _get("FINMIND_API_KEY")
