"""
Tiingo API 工具函式（Power Plan 專用）

提供：
  iex_batch_prices()  — IEX 批次取最新報價（一次多支，極快）
  fetch_stock_news()  — 個股 / 多股新聞（News API）
"""

import logging
from typing import Optional

import requests

import config

logger = logging.getLogger(__name__)

IEX_URL   = "https://api.tiingo.com/iex"
NEWS_URL  = "https://api.tiingo.com/tiingo/news"
IEX_BATCH = 500   # 每個 IEX 請求最多幾支（URL 長度上限）


# ════════════════════════════════════════════════════════
# IEX 批次報價
# ════════════════════════════════════════════════════════

def iex_batch_prices(
    symbols: list[str],
    api_key: Optional[str] = None,
    batch_size: int = IEX_BATCH,
) -> dict[str, dict]:
    """
    用 Tiingo IEX REST 端點批次取最新報價。
    回傳 {SYMBOL: {lastPrice, prevClose, volume, timestamp}}。
    一次最多 batch_size 支（預設 500），自動分批。
    """
    api_key = api_key or config.fresh("TIINGO_API_KEY")
    if not api_key:
        logger.warning("TIINGO_API_KEY 未設定，跳過 IEX 更新")
        return {}

    result: dict[str, dict] = {}

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        try:
            resp = requests.get(
                IEX_URL,
                params={"tickers": ",".join(batch), "token": api_key},
                timeout=30,
            )
            resp.raise_for_status()
            for item in resp.json():
                ticker = str(item.get("ticker", "")).upper()
                if not ticker:
                    continue
                # IEX 可能回傳 lastSalePrice 或 lastPrice
                price = item.get("lastSalePrice") or item.get("lastPrice")
                prev  = item.get("prevClose")
                vol   = item.get("lastVolume") or item.get("volume")
                ts    = item.get("lastSaleTimestamp") or item.get("timestamp")
                if price is not None:
                    result[ticker] = {
                        "lastPrice": float(price),
                        "prevClose": float(prev) if prev is not None else None,
                        "volume":    int(vol)   if vol   is not None else None,
                        "timestamp": ts,
                    }
        except Exception as e:
            logger.warning(f"IEX 批次 {i // batch_size + 1} 失敗：{e}")

    return result


# ════════════════════════════════════════════════════════
# 個股新聞
# ════════════════════════════════════════════════════════

def fetch_stock_news(
    symbols: list[str],
    api_key: Optional[str] = None,
    limit: int = 8,
) -> list[dict]:
    """
    用 Tiingo News API 取多支股票的最新新聞。
    回傳 list[{id, publishedDate, title, url, source, tickers, tags}]。
    """
    api_key = api_key or config.fresh("TIINGO_API_KEY")
    if not api_key:
        return []

    try:
        resp = requests.get(
            NEWS_URL,
            params={
                "tickers": ",".join([s.lower() for s in symbols]),
                "limit":   limit,
                "token":   api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json() or []
    except Exception as e:
        logger.warning(f"News API 失敗：{e}")
        return []
