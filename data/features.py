"""
技術指標計算模組
輸入：某個 symbol 的歷史價格 DataFrame
輸出：ma50, ma200, adj_close_to_ma50_ratio, momentum_raw, current_score, score_delta
"""

import pandas as pd
import numpy as np


def compute_features(price_df: pd.DataFrame) -> dict:
    """
    計算單一 symbol 今日的技術指標。

    Args:
        price_df: 欄位需包含 [date, adj_close, volume]，按日期升序排列

    Returns:
        dict 包含所有 Feature Parquet 對應欄位
    """
    if price_df is None or len(price_df) < 5:
        return _empty_features()

    df = price_df.sort_values("date").reset_index(drop=True)
    prices  = df["adj_close"].values
    volumes = df["volume"].values if "volume" in df.columns else np.ones(len(df))

    curr_price = float(prices[-1])

    # ── 移動平均 ──────────────────────────────────────────
    ma50  = float(prices[-50:].mean())  if len(prices) >= 50  else float(prices.mean())
    ma200 = float(prices[-200:].mean()) if len(prices) >= 200 else float(prices.mean())

    # ── 比率 & 動能 ───────────────────────────────────────
    ma50_ratio = curr_price / ma50 if ma50 > 0 else 1.0
    momentum_raw = float((prices[-1] / prices[-21]) - 1) if len(prices) >= 21 else 0.0
    avg_volume_20d = float(volumes[-20:].mean()) if len(volumes) >= 20 else float(volumes.mean())

    # ── Score 計算（0–100） ───────────────────────────────
    today_score = _score(prices)

    # ── Score Delta（今日 vs 昨日） ───────────────────────
    if len(prices) >= 2:
        yesterday_score = _score(prices[:-1])
        score_delta = round(today_score - yesterday_score, 1)
    else:
        score_delta = 0.0

    return {
        "ma50":                    round(ma50, 4),
        "ma200":                   round(ma200, 4),
        "adj_close_to_ma50_ratio": round(ma50_ratio, 4),
        "momentum_raw":            round(momentum_raw, 4),
        "current_score":           round(today_score, 1),
        "score_delta":             score_delta,
        "avg_volume_20d":          round(avg_volume_20d, 0),
    }


def compute_history_scores(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    計算整段歷史的每日 score 和 score_delta（給 Zone E HistoryView 使用）。

    Args:
        price_df: 欄位需包含 [date, adj_close, volume]

    Returns:
        DataFrame 新增 score, score_delta 欄位
    """
    if price_df is None or len(price_df) < 5:
        return price_df

    df = price_df.sort_values("date").reset_index(drop=True)
    prices = df["adj_close"].values

    scores = []
    for i in range(len(prices)):
        s = _score(prices[:i+1]) if i >= 4 else 50.0
        scores.append(round(s, 1))

    score_arr = pd.Series(scores)
    df["score"]       = score_arr.values
    df["score_delta"] = score_arr.diff().fillna(0).round(1).values

    return df


# ── 內部計算 ──────────────────────────────────────────────

def _score(prices: np.ndarray) -> float:
    """
    基於價格序列計算一個 0–100 的健康分數。

    權重分配：
    - MA50 站上程度   35 分（價格相對 MA50 的位置）
    - 20日動能        35 分（近期漲跌幅）
    - MA200 站上程度  30 分（中長期趨勢確認）
    """
    if len(prices) < 5:
        return 50.0

    curr = prices[-1]

    # MA50 分數 (0–35)
    ma50  = prices[-50:].mean()  if len(prices) >= 50  else prices.mean()
    ratio = curr / ma50 if ma50 > 0 else 1.0
    # ratio 1.10 → 35分, 1.00 → 20分, 0.88 → 0分
    ma_score = max(0.0, min(35.0, (ratio - 0.88) / 0.22 * 35.0))

    # 動能分數 (0–35)
    mom = float((prices[-1] / prices[-21]) - 1) if len(prices) >= 21 else 0.0
    # +15% → 35分, 0% → 17.5分, -15% → 0分
    mom_score = max(0.0, min(35.0, (mom + 0.15) / 0.30 * 35.0))

    # MA200 分數 (0–30)
    ma200 = prices[-200:].mean() if len(prices) >= 200 else prices.mean()
    ratio200 = curr / ma200 if ma200 > 0 else 1.0
    ma200_score = max(0.0, min(30.0, (ratio200 - 0.88) / 0.22 * 30.0))

    return round(ma_score + mom_score + ma200_score, 1)


def _empty_features() -> dict:
    return {
        "ma50": 0.0, "ma200": 0.0,
        "adj_close_to_ma50_ratio": 1.0,
        "momentum_raw": 0.0,
        "current_score": 50.0,
        "score_delta": 0.0,
        "avg_volume_20d": 0.0,
    }
