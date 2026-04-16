"""
Zone E — HistoryView
選中持倉的 ROI + Score 時間序列圖，標記歷史 exit 觸發點。
回答問題：這個持倉過去的走勢如何？這次的出場訊號可信嗎？
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data.models import HistoryPoint
from data.loader import load_history


def render(symbol: str | None) -> None:
    st.markdown("### 📜 History")

    if symbol is None:
        st.info("← 選擇一個持倉查看歷史走勢")
        return

    # ── 載入歷史資料 ──────────────────────────────────────
    col_title, col_range = st.columns([3, 1])
    with col_title:
        st.markdown(f"**{symbol}** 歷史走勢")
    with col_range:
        days = st.selectbox("區間", [30, 60, 90, 120, 180], index=2, key="history_days")

    history = load_history(symbol, days)

    if not history:
        st.warning(f"找不到 {symbol} 的歷史資料")
        return

    _render_dual_axis_chart(symbol, history)
    _render_score_delta_bar(history)


def _render_dual_axis_chart(symbol: str, history: list[HistoryPoint]) -> None:
    """雙軸折線圖：ROI (左軸) + Score (右軸) + exit event markers"""

    dates      = [h.date for h in history]
    rois       = [h.roi * 100 for h in history]      # 轉成百分比
    scores     = [h.score for h in history]

    # Exit 觸發點
    hard_dates     = [h.date for h in history if h.exit_grade == "hard"]
    hard_rois      = [h.roi * 100 for h in history if h.exit_grade == "hard"]
    advisory_dates = [h.date for h in history if h.exit_grade == "advisory"]
    advisory_rois  = [h.roi * 100 for h in history if h.exit_grade == "advisory"]

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # ROI 折線
    roi_color_line = "#2ecc71" if rois[-1] >= 0 else "#e74c3c"
    fig.add_trace(go.Scatter(
        x=dates, y=rois,
        name="ROI %",
        line=dict(color=roi_color_line, width=2),
        hovertemplate="%{x}<br>ROI: %{y:.2f}%<extra></extra>",
    ), secondary_y=False)

    # Score 折線
    fig.add_trace(go.Scatter(
        x=dates, y=scores,
        name="Score",
        line=dict(color="#3498db", width=1.5, dash="dot"),
        hovertemplate="%{x}<br>Score: %{y:.0f}<extra></extra>",
    ), secondary_y=True)

    # Hard exit markers（紅色三角）
    if hard_dates:
        fig.add_trace(go.Scatter(
            x=hard_dates, y=hard_rois,
            name="Hard Exit",
            mode="markers",
            marker=dict(symbol="triangle-down", size=14, color="#FF4B4B", line=dict(width=1, color="white")),
            hovertemplate="%{x}<br>🔴 Hard Exit @ ROI: %{y:.2f}%<extra></extra>",
        ), secondary_y=False)

    # Advisory exit markers（橘色圓形）
    if advisory_dates:
        fig.add_trace(go.Scatter(
            x=advisory_dates, y=advisory_rois,
            name="Advisory Exit",
            mode="markers",
            marker=dict(symbol="circle", size=10, color="#FFA500", line=dict(width=1, color="white")),
            hovertemplate="%{x}<br>🟠 Advisory @ ROI: %{y:.2f}%<extra></extra>",
        ), secondary_y=False)

    # 零軸參考線
    fig.add_hline(y=0, line_dash="dash", line_color="#555", opacity=0.4, secondary_y=False)

    fig.update_layout(
        height=300,
        margin=dict(l=40, r=40, t=10, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="ROI %",   secondary_y=False, gridcolor="#333")
    fig.update_yaxes(title_text="Score",   secondary_y=True,  range=[0, 100], showgrid=False)
    fig.update_xaxes(gridcolor="#222")

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _render_score_delta_bar(history: list[HistoryPoint]) -> None:
    """Score Delta 長條 sparkline（綠=正，紅=負）"""
    dates  = [h.date for h in history]
    deltas = [h.score_delta for h in history]
    colors = ["#2ecc71" if d >= 0 else "#e74c3c" for d in deltas]

    fig = go.Figure(go.Bar(
        x=dates, y=deltas,
        marker_color=colors,
        hovertemplate="%{x}<br>Δ Score: %{y:+.1f}<extra></extra>",
        name="Score Delta",
    ))

    fig.add_hline(y=0, line_color="#555", line_width=1)

    fig.update_layout(
        height=100,
        margin=dict(l=40, r=40, t=5, b=20),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(gridcolor="#222", title="Δ Score"),
        xaxis=dict(gridcolor="#222"),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
