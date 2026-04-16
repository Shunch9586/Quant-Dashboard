"""
Zone D — TechnicalHealthView
全持倉技術指標 Heatmap + Score 散點圖。
回答問題：哪些持倉在技術面出現問題？有沒有跨組合的惡化模式？
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from data.models import DecisionRecord


def render(records: list[DecisionRecord]) -> str | None:
    """
    顯示 Technical Health。
    回傳被選中的 symbol，沒有選中則回傳 None。
    """
    st.markdown("### 🌡️ Technical Health")

    if not records:
        st.info("無持倉資料")
        return None

    tab_heat, tab_scatter = st.tabs(["📊 Heatmap", "🔵 Scatter"])
    selected_symbol = None

    with tab_heat:
        selected_symbol = _render_heatmap(records) or selected_symbol

    with tab_scatter:
        _render_scatter(records)

    return selected_symbol


def _render_heatmap(records: list[DecisionRecord]) -> str | None:
    """Heatmap Table：每列一個持倉，每行一個指標，顏色代表健康程度"""

    # ── 建立 DataFrame ────────────────────────────────────
    rows = []
    for r in records:
        rows.append({
            "Symbol":    r.symbol,
            "Sector":    r.sector,
            "Score":     r.current_score,
            "Δ Score":   r.score_delta,
            "P/MA50":    r.adj_close_to_ma50_ratio,
            "Momentum":  r.momentum_raw,
            "_symbol":   r.symbol,
        })

    df = pd.DataFrame(rows).sort_values("Score")

    # ── 顏色矩陣 ──────────────────────────────────────────
    # Score:    0=red, 50=yellow, 100=green
    # Δ Score:  負=red, 0=yellow, 正=green
    # P/MA50:   <1=red, 1=yellow, >1.1=green
    # Momentum: 負=red, 0=yellow, 正=green

    def score_z(v):     return v / 100
    def delta_z(v):     return (v + 30) / 60
    def ratio_z(v):     return min(max((v - 0.85) / 0.3, 0), 1)
    def momentum_z(v):  return min(max((v + 1) / 2, 0), 1)

    z_score    = [[score_z(r["Score"])    for r in rows]]
    z_delta    = [[delta_z(r["Δ Score"])  for r in rows]]
    z_ratio    = [[ratio_z(r["P/MA50"])   for r in rows]]
    z_momentum = [[momentum_z(r["Momentum"]) for r in rows]]

    symbols = [r["Symbol"] for r in rows]

    z_all = z_score + z_delta + z_ratio + z_momentum
    y_labels = ["Score", "Δ Score", "P/MA50", "Momentum"]

    # ── Plotly Heatmap ────────────────────────────────────
    text_all = [
        [f"{r['Score']:.0f}"  for r in rows],
        [f"{r['Δ Score']:+.0f}" for r in rows],
        [f"{r['P/MA50']:.3f}" for r in rows],
        [f"{r['Momentum']:+.2f}" for r in rows],
    ]

    fig = go.Figure(data=go.Heatmap(
        z=z_all,
        x=symbols,
        y=y_labels,
        text=text_all,
        texttemplate="%{text}",
        textfont={"size": 11},
        colorscale=[[0, "#e74c3c"], [0.5, "#f39c12"], [1, "#2ecc71"]],
        showscale=False,
        hoverongaps=False,
        hovertemplate="<b>%{x}</b><br>%{y}: %{text}<extra></extra>",
    ))

    fig.update_layout(
        height=220,
        margin=dict(l=60, r=10, t=10, b=40),
        xaxis=dict(side="bottom"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── 點擊選擇 symbol ───────────────────────────────────
    selected = st.selectbox(
        "查看持倉詳情",
        options=["（請選擇）"] + symbols,
        key="health_symbol_select"
    )
    return selected if selected != "（請選擇）" else None


def _render_scatter(records: list[DecisionRecord]) -> None:
    """Scatter：current_score vs score_delta，顏色=sector"""

    df = pd.DataFrame([{
        "Symbol":    r.symbol,
        "Score":     r.current_score,
        "Δ Score":   r.score_delta,
        "Sector":    r.sector,
        "ROI":       f"{r.roi*100:.1f}%",
        "Exit":      "🔴 Hard" if r.exit_grade == "hard" else ("🟠 Advisory" if r.exit_grade == "advisory" else "✅"),
    } for r in records])

    fig = px.scatter(
        df, x="Score", y="Δ Score",
        color="Sector", text="Symbol",
        hover_data=["ROI", "Exit"],
        title="Score vs Δ Score（四象限健康地圖）",
        labels={"Score": "Current Score (0-100)", "Δ Score": "Score Delta"},
    )

    # 四象限參考線
    fig.add_hline(y=0,  line_dash="dash", line_color="#555", opacity=0.5)
    fig.add_vline(x=50, line_dash="dash", line_color="#555", opacity=0.5)

    # 象限標籤
    annotations = [
        dict(x=80, y=15,  text="💪 Strong",    showarrow=False, font=dict(color="#2ecc71", size=11)),
        dict(x=80, y=-15, text="👀 Watch",     showarrow=False, font=dict(color="#f39c12", size=11)),
        dict(x=20, y=15,  text="🔄 Recovering",showarrow=False, font=dict(color="#3498db", size=11)),
        dict(x=20, y=-15, text="⚠️ Critical",  showarrow=False, font=dict(color="#e74c3c", size=11)),
    ]
    fig.update_layout(
        annotations=annotations,
        height=320,
        margin=dict(l=40, r=20, t=40, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_traces(textposition="top center", marker=dict(size=12))

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
