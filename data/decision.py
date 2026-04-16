"""
簡化版 Decision Layer
在你的 portfolio_manager 建好之前，用規則引擎計算 should_exit / exit_grade。

規則優先順序：
  1. STOP_BREACH      → hard exit（價格跌破止損價）
  2. MA50_BREAKDOWN   → hard exit（價格低於 MA50 超過 7%）
  3. SCORE_DECLINE    → advisory exit（分數低 + 快速下滑）
  4. 無訊號           → Hold
"""

import pandas as pd


def compute_decision(
    curr_price:  float,
    stop_price,            # float 或 NaN
    features:    dict,
) -> dict:
    """
    Args:
        curr_price:  今日收盤價
        stop_price:  止損價（可能是 NaN）
        features:    來自 features.compute_features() 的 dict

    Returns:
        dict 包含 Decision Layer 所有欄位
    """
    ma50            = features.get("ma50", curr_price)
    ma50_ratio      = features.get("adj_close_to_ma50_ratio", 1.0)
    score           = features.get("current_score", 50.0)
    score_delta     = features.get("score_delta", 0.0)
    dist_to_ma50    = ma50_ratio - 1.0

    # 止損距離（有止損價才計算）
    has_stop = stop_price is not None and not (isinstance(stop_price, float) and pd.isna(stop_price))
    dist_to_stop = (curr_price - stop_price) / stop_price if has_stop else 0.0

    # ── Rule 1: Stop Price 跌破 ───────────────────────────
    if has_stop and curr_price < stop_price:
        return _decision(
            should_exit=True,
            exit_grade="hard",
            code="STOP_BREACH",
            detail=f"價格 {curr_price:.2f} 跌破止損 {stop_price:.2f}（距離 {dist_to_stop:.1%}）",
            exit_price=curr_price,
            dist_to_stop=dist_to_stop,
            dist_to_ma50=dist_to_ma50,
        )

    # ── Rule 2: MA50 嚴重跌破 ─────────────────────────────
    if ma50_ratio < 0.93:
        return _decision(
            should_exit=True,
            exit_grade="hard",
            code="MA50_BREAKDOWN",
            detail=f"價格低於 MA50 達 {dist_to_ma50:.1%}，趨勢持續惡化。",
            exit_price=curr_price,
            dist_to_stop=dist_to_stop,
            dist_to_ma50=dist_to_ma50,
        )

    # ── Rule 3: Score 快速下滑 ────────────────────────────
    if score < 38 and score_delta < -8:
        return _decision(
            should_exit=True,
            exit_grade="advisory",
            code="SCORE_DECLINE",
            detail=f"Score {score:.0f}/100，Delta {score_delta:+.1f}，技術面持續惡化。",
            exit_price=curr_price,
            dist_to_stop=dist_to_stop,
            dist_to_ma50=dist_to_ma50,
        )

    # ── 無出場訊號 ────────────────────────────────────────
    return _decision(
        should_exit=False,
        exit_grade="",
        code="",
        detail="",
        exit_price=0.0,
        dist_to_stop=dist_to_stop,
        dist_to_ma50=dist_to_ma50,
    )


def _decision(should_exit, exit_grade, code, detail, exit_price, dist_to_stop, dist_to_ma50) -> dict:
    return {
        "should_exit":        should_exit,
        "exit_grade":         exit_grade,
        "exit_reason_code":   code,
        "exit_reason_detail": detail,
        "exit_price":         exit_price,
        "distance_to_stop":   round(dist_to_stop, 4),
        "distance_to_ma50":   round(dist_to_ma50, 4),
    }
