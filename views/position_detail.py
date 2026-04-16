"""
Zone C — PositionDetailView
單一持倉的完整狀態卡片（由 Zone B 或 Zone D 點擊觸發）。
回答問題：這個持倉現在的狀況是什麼？系統建議我怎麼做？
"""

import streamlit as st
from data.models import DecisionRecord
from utils.formatters import (
    fmt_pct, fmt_price, fmt_pnl, fmt_score_delta,
    grade_label, grade_color, roi_color, score_color, score_delta_color
)


def render(record: DecisionRecord | None) -> None:
    st.markdown("### 🔍 Position Detail")

    if record is None:
        st.info("← 點擊左側 Alert Queue 或 Technical Health 的任一持倉查看詳情")
        return

    # ── 標題列 ────────────────────────────────────────────
    grade_clr = grade_color(record.exit_grade)
    st.markdown(
        f"<h3 style='margin-bottom:0;'>{record.symbol} "
        f"<span style='font-size:0.6em; color:#888;'>{record.market} · {record.position_type.upper()}</span>"
        f"</h3>",
        unsafe_allow_html=True
    )
    st.caption(f"📅 持有 {record.holding_days} 天 ｜ 進場價 {fmt_price(record.entry_price)} ｜ Sector: {record.sector}")
    st.divider()

    # ── Section 1: Performance ────────────────────────────
    st.markdown("**💰 Performance**")
    p1, p2, p3 = st.columns(3)
    with p1:
        rc = roi_color(record.roi)
        st.markdown(f"**ROI**<br><span style='color:{rc}; font-size:1.4em; font-weight:bold;'>{fmt_pct(record.roi)}</span>", unsafe_allow_html=True)
    with p2:
        rc = roi_color(record.unrealized_pnl)
        st.markdown(f"**Unrealized PnL**<br><span style='color:{rc}; font-size:1.4em;'>{fmt_pnl(record.unrealized_pnl)}</span>", unsafe_allow_html=True)
    with p3:
        st.markdown(f"**Position Value**<br><span style='font-size:1.4em;'>{fmt_price(record.position_value)}</span>", unsafe_allow_html=True)

    st.caption(f"現價 {fmt_price(record.curr_price)} ｜ 止損 {fmt_price(record.stop_price)} ｜ 距止損 {fmt_pct(record.distance_to_stop)}")
    st.divider()

    # ── Section 2: Technical ──────────────────────────────
    st.markdown("**📈 Technical**")
    t1, t2, t3 = st.columns(3)

    with t1:
        ratio = record.adj_close_to_ma50_ratio
        ratio_color = "#2ecc71" if ratio >= 1.0 else "#e74c3c"
        st.markdown(f"**Price / MA50**<br><span style='color:{ratio_color}; font-size:1.3em; font-weight:bold;'>{ratio:.3f}</span>", unsafe_allow_html=True)
        st.caption("≥1.0 = 站上 MA50")

    with t2:
        sc = score_color(record.current_score)
        st.markdown(f"**Score**<br><span style='color:{sc}; font-size:1.3em; font-weight:bold;'>{record.current_score:.0f} / 100</span>", unsafe_allow_html=True)
        _score_bar(record.current_score)

    with t3:
        dc = score_delta_color(record.score_delta)
        st.markdown(f"**Score Delta**<br><span style='color:{dc}; font-size:1.3em; font-weight:bold;'>{fmt_score_delta(record.score_delta)}</span>", unsafe_allow_html=True)
        st.caption(f"Momentum: {record.momentum_raw:+.2f}")

    st.divider()

    # ── Section 3: Decision ───────────────────────────────
    st.markdown("**⚡ Decision**")

    if record.should_exit:
        d1, d2 = st.columns([1, 2])
        with d1:
            st.markdown(
                f"<div style='background-color:{grade_clr}22; border: 2px solid {grade_clr}; "
                f"border-radius:8px; padding:12px; text-align:center;'>"
                f"<span style='color:{grade_clr}; font-size:1.2em; font-weight:bold;'>{grade_label(record.exit_grade)}</span><br>"
                f"<span style='font-size:0.85em;'>{record.exit_reason_code}</span>"
                f"</div>",
                unsafe_allow_html=True
            )
        with d2:
            st.markdown(f"**出場原因：**")
            st.info(record.exit_reason_detail)
            st.caption(f"建議出場價：{fmt_price(record.exit_price)} ｜ 距 MA50：{fmt_pct(record.distance_to_ma50)}")
    else:
        st.success(f"✅ **Hold** — 今日無出場訊號（Entry Score: {record.entry_score}）")


def _score_bar(score: float) -> None:
    """簡單的分數長條（用 HTML progress bar 模擬）"""
    color = score_color(score)
    pct = int(score)
    st.markdown(
        f"<div style='background:#333; border-radius:4px; height:8px; margin-top:4px;'>"
        f"<div style='background:{color}; width:{pct}%; height:100%; border-radius:4px;'></div>"
        f"</div>",
        unsafe_allow_html=True
    )
