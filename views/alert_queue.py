"""
Zone B — AlertQueueView
今日需要處理的出場清單，按緊急程度排序。
回答問題：今天要對哪些持倉做決策？
"""

import streamlit as st
import pandas as pd
from data.models import DecisionRecord
from utils.formatters import fmt_pct, fmt_pnl, grade_label, grade_color


def render(records: list[DecisionRecord]) -> str | None:
    """
    顯示 Alert Queue。
    回傳被選中的 symbol（點擊 row 時），沒有選中則回傳 None。
    """
    st.markdown("### 🚨 Alert Queue")

    # 篩選 should_exit=True 的持倉
    flagged = [r for r in records if r.should_exit]

    # ── Hard Only 篩選 toggle ─────────────────────────────
    col1, col2 = st.columns([3, 1])
    with col2:
        hard_only = st.toggle("Hard Only", key="hard_only_toggle")

    if hard_only:
        display = [r for r in flagged if r.exit_grade == "hard"]
        hidden_count = len(flagged) - len(display)
    else:
        display = flagged
        hidden_count = 0

    # ── 排序：hard 優先 → roi 虧損優先 ───────────────────
    grade_order = {"hard": 0, "advisory": 1, "": 2}
    display.sort(key=lambda r: (grade_order.get(r.exit_grade, 2), r.roi))

    # ── 空清單狀態 ────────────────────────────────────────
    if not display:
        if hard_only and flagged:
            st.info(f"無 Hard exit 訊號（隱藏了 {len(flagged)} 個 Advisory）")
        else:
            st.success("✅ No exit signals today — 今日無出場訊號")
        return None

    if hidden_count > 0:
        st.caption(f"(+{hidden_count} advisory hidden)")

    # ── 建立 DataFrame 顯示 ───────────────────────────────
    rows = []
    for r in display:
        rows.append({
            "Symbol":     r.symbol,
            "Market":     r.market,
            "Grade":      grade_label(r.exit_grade),
            "ROI":        fmt_pct(r.roi),
            "Unr. PnL":   fmt_pnl(r.unrealized_pnl),
            "Reason":     r.exit_reason_code,
            "_roi_raw":   r.roi,
            "_grade_raw": r.exit_grade,
            "_symbol":    r.symbol,
        })

    df = pd.DataFrame(rows)

    # ── 顯示互動表格 ──────────────────────────────────────
    selected_symbol = None

    for _, row in df.iterrows():
        border_color = grade_color(row["_grade_raw"])
        roi_color = "#2ecc71" if row["_roi_raw"] >= 0 else "#e74c3c"

        col_sym, col_grade, col_roi, col_pnl, col_reason, col_btn = st.columns([1.2, 1.2, 1, 1.2, 2, 0.8])

        with col_sym:
            st.markdown(
                f"<span style='border-left: 4px solid {border_color}; padding-left: 8px; font-weight: bold;'>"
                f"{row['Symbol']} <span style='font-size:0.75em; color:#888;'>{row['Market']}</span>"
                f"</span>",
                unsafe_allow_html=True
            )
        with col_grade:
            st.markdown(row["Grade"])
        with col_roi:
            st.markdown(
                f"<span style='color:{roi_color}; font-weight:bold;'>{row['ROI']}</span>",
                unsafe_allow_html=True
            )
        with col_pnl:
            st.markdown(
                f"<span style='color:{roi_color};'>{row['Unr. PnL']}</span>",
                unsafe_allow_html=True
            )
        with col_reason:
            st.caption(row["Reason"])
        with col_btn:
            if st.button("詳細", key=f"alert_btn_{row['_symbol']}"):
                selected_symbol = row["_symbol"]

    return selected_symbol
