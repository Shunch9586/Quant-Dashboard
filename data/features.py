"""
技術指標計算模組 v2（分析師建議升級版）

Score 架構（總分 100）：
  ① 趨勢結構（25）— MA 多頭排列 + MA200 斜率
  ② 多週期動能（25）— 5D / 20D / 60D 加權動能
  ③ 位置優勢（15）— 距 52 週高點
  ④ 量價結構（20）— 成交量確認突破真偽
  ⑤ 穩定性   （15）— 60D 回撤 + 日報酬波動度修正

RSI(14) 過熱懲罰：
  RSI > 80 → 總分 × 0.85（追高警示）
  RSI < 30 → 總分 × 0.90（空頭慣性尚未脫離）

Score Delta：today_score - mean(last 3 days)（過濾單日雜訊）

待實作（需市場指數資料）：
  RS 相對強度（個股漲幅 vs SPY/0050）
"""

import pandas as pd
import numpy as np


# ════════════════════════════════════════════════════════
# 公開介面
# ════════════════════════════════════════════════════════

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

    df      = price_df.sort_values("date").reset_index(drop=True)
    prices  = df["adj_close"].values.astype(float)
    volumes = df["volume"].values.astype(float) if "volume" in df.columns else np.ones(len(df))

    curr_price = float(prices[-1])

    # ── 移動平均（供外部使用） ────────────────────────────
    ma50  = float(prices[-50:].mean())  if len(prices) >= 50  else float(prices.mean())
    ma200 = float(prices[-200:].mean()) if len(prices) >= 200 else float(prices.mean())

    # ── 比率 & 動能（供外部使用） ─────────────────────────
    ma50_ratio     = curr_price / ma50 if ma50 > 0 else 1.0
    momentum_raw   = float((prices[-1] / prices[-21]) - 1) if len(prices) >= 21 else 0.0
    avg_volume_20d = float(volumes[-20:].mean()) if len(volumes) >= 20 else float(volumes.mean())

    # ── Score 計算 ─────────────────────────────────────
    today_score = _score(prices, volumes)

    # ── Score Delta（3 日均值差，過濾單日雜訊） ───────────
    n = len(prices)
    if n >= 8:
        s1 = _score(prices[:-1], volumes[:-1])
        s2 = _score(prices[:-2], volumes[:-2])
        s3 = _score(prices[:-3], volumes[:-3])
        score_delta = round(today_score - float(np.mean([s1, s2, s3])), 1)
    elif n >= 2:
        score_delta = round(today_score - _score(prices[:-1], volumes[:-1]), 1)
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

    df      = price_df.sort_values("date").reset_index(drop=True)
    prices  = df["adj_close"].values.astype(float)
    volumes = df["volume"].values.astype(float) if "volume" in df.columns else np.ones(len(df))

    scores = []
    for i in range(len(prices)):
        s = _score(prices[:i+1], volumes[:i+1]) if i >= 4 else 50.0
        scores.append(round(s, 1))

    score_arr = pd.Series(scores)

    # Score Delta：今日 - 最近 3 日均值（min_periods=1 避免開頭 NaN）
    rolling_mean = score_arr.shift(1).rolling(3, min_periods=1).mean()
    delta_arr    = (score_arr - rolling_mean).fillna(0).round(1)

    df["score"]       = score_arr.values
    df["score_delta"] = delta_arr.values

    return df


# ════════════════════════════════════════════════════════
# 核心評分函數
# ════════════════════════════════════════════════════════

def _score(prices: np.ndarray, volumes: np.ndarray | None = None) -> float:
    """
    計算 0–100 的健康分數（v2 升級版）。

    五大模組 + RSI 懲罰因子：
    ① 趨勢結構  25
    ② 多週期動能 25
    ③ 位置優勢  15
    ④ 量價結構  20
    ⑤ 穩定性   15
    """
    n = len(prices)
    if n < 5:
        return 50.0

    curr = float(prices[-1])

    # ──────────────────────────────────────────────────
    # ① 趨勢結構（25 分）
    #   close > MA50          +8
    #   MA50 > MA150          +8
    #   MA150 > MA200         +9
    #   MA200 斜率向上        +5（今日 MA200 > 20 日前 MA200）
    # ──────────────────────────────────────────────────
    ma50  = float(prices[-50:].mean())  if n >= 50  else float(prices.mean())
    ma150 = float(prices[-150:].mean()) if n >= 150 else float(prices.mean())
    ma200 = float(prices[-200:].mean()) if n >= 200 else float(prices.mean())

    trend_score = 0.0
    if curr  > ma50:  trend_score += 8.0
    if ma50  > ma150: trend_score += 8.0
    if ma150 > ma200: trend_score += 9.0

    # MA200 斜率（需 220 天；不足則以 MA50 斜率代替）
    if n >= 220:
        ma200_20d_ago = float(prices[-220:-20].mean())
        if ma200 > ma200_20d_ago:
            trend_score += 5.0
    elif n >= 60:
        ma50_10d_ago = float(prices[-60:-10].mean())
        if ma50 > ma50_10d_ago:
            trend_score += 5.0

    trend_score = min(trend_score, 25.0)

    # ──────────────────────────────────────────────────
    # ② 多週期動能（25 分）
    #   5D  → 8 分   clamp range: -5% ~ +5%
    #   20D → 9 分   clamp range: -15% ~ +15%
    #   60D → 8 分   clamp range: -30% ~ +30%
    # ──────────────────────────────────────────────────
    mom_5  = float(curr / prices[-6]  - 1) if n >= 6  else 0.0
    mom_20 = float(curr / prices[-21] - 1) if n >= 21 else 0.0
    mom_60 = float(curr / prices[-61] - 1) if n >= 61 else mom_20 * 2.5

    s_mom5  = max(0.0, min(8.0, (mom_5  + 0.05) / 0.10 * 8.0))
    s_mom20 = max(0.0, min(9.0, (mom_20 + 0.15) / 0.30 * 9.0))
    s_mom60 = max(0.0, min(8.0, (mom_60 + 0.30) / 0.60 * 8.0))
    score_mom = s_mom5 + s_mom20 + s_mom60

    # ──────────────────────────────────────────────────
    # ③ 位置優勢（15 分）
    #   dist_52w：當前價格離 252 日高點的距離
    #   -30% 以下 → 0 分；在高點附近 → 15 分
    # ──────────────────────────────────────────────────
    high_252 = float(prices[-252:].max()) if n >= 252 else float(prices.max())
    dist_52w = float(curr / high_252 - 1) if high_252 > 0 else 0.0
    score_pos = max(0.0, min(15.0, (dist_52w + 0.30) / 0.30 * 15.0))

    # ──────────────────────────────────────────────────
    # ④ 量價結構（20 分）
    #   基礎量比得分  0–10 分
    #   量增價漲加分  +5
    #   量縮價漲扣分  -3（虛漲）
    # ──────────────────────────────────────────────────
    if volumes is not None and len(volumes) >= 20:
        vol       = float(volumes[-1])
        avg_vol   = float(volumes[-20:].mean())
        vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0

        score_vol = max(0.0, min(10.0, (vol_ratio - 1.0) / 2.0 * 10.0))
        price_up  = (curr > float(prices[-2])) if n >= 2 else True

        if price_up and vol_ratio > 1.2:
            score_vol += 5.0
        elif price_up and vol_ratio < 0.8:
            score_vol -= 3.0

        score_vol = max(0.0, min(20.0, score_vol))
    else:
        score_vol = 10.0   # 無量能資料，給中間值

    # ──────────────────────────────────────────────────
    # ⑤ 穩定性（15 分）
    #   60 日最大回撤     0–10 分
    #   20 日報酬波動度   0–5 分（替代 ATR，不需 OHLC）
    # ──────────────────────────────────────────────────
    high_60d  = float(prices[-60:].max()) if n >= 60 else float(prices.max())
    drawdown  = float(curr / high_60d - 1) if high_60d > 0 else 0.0
    score_dd  = max(0.0, min(10.0, (drawdown + 0.30) / 0.30 * 10.0))

    if n >= 21:
        rets     = np.diff(prices[-21:]) / prices[-21:-1]
        vol_std  = float(np.std(rets))
    else:
        vol_std = 0.02
    # 日波動 < 1% → 5 分；> 4% → 0 分
    score_vol_stab = max(0.0, min(5.0, (0.04 - vol_std) / 0.04 * 5.0))

    score_stability = score_dd + score_vol_stab

    # ──────────────────────────────────────────────────
    # RSI(14) 懲罰因子（分析師1建議）
    #   RSI > 80 → × 0.85（過熱追高警示）
    #   RSI < 30 → × 0.90（空頭慣性）
    #   40–70   → × 1.00（健康強勢區）
    # ──────────────────────────────────────────────────
    rsi = _rsi14(prices)
    if rsi > 80:
        rsi_factor = 0.85
    elif rsi < 30:
        rsi_factor = 0.90
    else:
        rsi_factor = 1.0

    total = (trend_score + score_mom + score_pos + score_vol + score_stability) * rsi_factor
    return round(max(0.0, min(100.0, total)), 1)


# ════════════════════════════════════════════════════════
# 輔助函數
# ════════════════════════════════════════════════════════

def _rsi14(prices: np.ndarray, period: int = 14) -> float:
    """計算 RSI(14)"""
    if len(prices) < period + 1:
        return 50.0

    deltas    = np.diff(prices[-(period + 1):])
    gains     = np.where(deltas > 0, deltas,  0.0)
    losses    = np.where(deltas < 0, -deltas, 0.0)
    avg_gain  = float(gains.mean())
    avg_loss  = float(losses.mean())

    if avg_loss == 0:
        return 100.0
    return round(100.0 - 100.0 / (1.0 + avg_gain / avg_loss), 1)


def _empty_features() -> dict:
    return {
        "ma50":                    0.0,
        "ma200":                   0.0,
        "adj_close_to_ma50_ratio": 1.0,
        "momentum_raw":            0.0,
        "current_score":           50.0,
        "score_delta":             0.0,
        "avg_volume_20d":          0.0,
    }
