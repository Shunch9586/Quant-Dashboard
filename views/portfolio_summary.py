"""
Zone A — PortfolioSummaryView
全寬、永遠可見的頂部 KPI 列。
回答問題：整體組合現在健康嗎？今天有多緊急？
"""

import streamlit as st
import plotly.graph_objects as go
from data.models import PortfolioSummaryData
from utils.formatters import fmt_pct, drawdown_status
import config


def render(summary: PortfolioSummaryData) -> None:
    st.markdown("### 📊 Portfolio Overview")

    # ── KPI Cards ────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        roi_val = summary.portfolio_roi
        st.metric(
            label="Portfolio ROI",
            value=fmt_pct(roi_val),
            delta=None,
            help="全組合加權平均報酬率"
        )
        _color_metric("Portfolio ROI", roi_val >= 0)

    with c2:
        dd = summary.current_drawdown
        status = drawdown_status(dd, config.DRAWDOWN_ALERT_THRESHOLD)
        icon = "🔴" if status == "danger" else ("🟡" if status == "warning" else "🟢")
        st.metric(
            label=f"{icon} Drawdown",
            value=fmt_pct(dd),
            help=f"從高點回落幅度（警戒線：{fmt_pct(config.DRAWDOWN_ALERT_THRESHOLD)}）"
        )

    with c3:
        hard = summary.hard_exits_pending
        adv  = summary.advisory_exits_pending
        icon = "🔴" if hard > 0 else ("🟠" if adv > 0 else "✅")
        st.metric(
            label=f"{icon} Exit Signals",
            value=f"{hard} Hard / {adv} Advisory",
            help="今日系統建議出場的持倉數"
        )

    with c4:
        risk = summary.positions_at_stop_risk
        icon = "⚠️" if risk > 0 else "✅"
        st.metric(
            label=f"{icon} Stop Risk",
            value=f"{risk} 筆",
            help="距離 stop price 5% 以內的持倉數"
        )

    with c5:
        st.metric(
            label="Total Positions",
            value=summary.total_positions,
            help="目前持倉總數"
        )

    # ── Sector Weights Bar ────────────────────────────────
    st.markdown("**Sector Weights**")
    _render_sector_bar(summary.sector_weights)


def _render_sector_bar(sector_weights: dict) -> None:
    """水平堆疊 bar，每個 sector 一個顏色"""
    if not sector_weights:
        st.caption("（無 sector 資料）")
        return

    colors = [
        "#3498db", "#2ecc71", "#e74c3c", "#f39c12",
        "#9b59b6", "#1abc9c", "#e67e22", "#34495e"
    ]

    fig = go.Figure()
    for i, (sector, weight) in enumerate(sorted(sector_weights.items(), key=lambda x: -x[1])):
        fig.add_trace(go.Bar(
            name=sector,
            x=[weight * 100],
            y=["Sector"],
            orientation="h",
            marker_color=colors[i % len(colors)],
            text=f"{sector}<br>{weight*100:.1f}%",
            textposition="inside",
            hovertemplate=f"{sector}: {weight*100:.1f}%<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        height=70,
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,
        xaxis=dict(range=[0, 100], showticklabels=False, showgrid=False),
        yaxis=dict(showticklabels=False),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _color_metric(label: str, is_positive: bool) -> None:
    """用 CSS 讓 metric 數字變色（green / red）"""
    color = "#2ecc71" if is_positive else "#e74c3c"
    st.markdown(
        f"<style>[data-testid='stMetricValue'] {{ color: {color}; }}</style>",
        unsafe_allow_html=True
    )
