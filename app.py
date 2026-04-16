"""
Quant Dashboard — 主程式入口
5-Zone 佈局：
  Zone A (top)     — PortfolioSummaryView
  Zone B (left)    — AlertQueueView
  Zone C (right)   — PositionDetailView
  Zone D (left)    — TechnicalHealthView
  Zone E (right)   — HistoryView
"""

import streamlit as st
from datetime import date

import config
from data.loader import load_positions, load_portfolio_summary
from views import portfolio_summary, alert_queue, position_detail, technical_health, history_view

# ── 頁面基礎設定 ──────────────────────────────────────────
st.set_page_config(
    page_title="Quant Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── 全域 CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    /* 緊縮上方空白 */
    .block-container { padding-top: 1rem; }
    /* 分隔線顏色 */
    hr { border-color: #333 !important; }
    /* 表格字體 */
    .stDataFrame { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)


# ── Session State：記住目前選中的 symbol ──────────────────
if "selected_symbol" not in st.session_state:
    st.session_state.selected_symbol = None


# ── 載入資料 ──────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="載入資料中...")
def load_data():
    records = load_positions()
    summary = load_portfolio_summary(records)
    return records, summary


records, summary = load_data()

# ── 右上角：資料來源標記 + 最後更新時間 ──────────────────
header_col, info_col = st.columns([4, 1])
with header_col:
    st.title("📈 Quant Dashboard")
with info_col:
    mode_badge = "🧪 Mock Data" if config.USE_MOCK_DATA else "✅ Live S3"
    st.markdown(f"<div style='text-align:right; padding-top:12px;'>"
                f"<span style='background:#333; padding:4px 10px; border-radius:12px; font-size:0.8em;'>{mode_badge}</span><br>"
                f"<span style='color:#888; font-size:0.75em;'>📅 {date.today()}</span>"
                f"</div>", unsafe_allow_html=True)

st.divider()

# ════════════════════════════════════════════════════════
# ZONE A — Portfolio Summary（全寬）
# ════════════════════════════════════════════════════════
portfolio_summary.render(summary)

st.divider()

# ════════════════════════════════════════════════════════
# ZONE B + C — Alert Queue（左）＋ Position Detail（右）
# ════════════════════════════════════════════════════════
zone_b, zone_c = st.columns([4, 6], gap="large")

with zone_b:
    clicked_from_alert = alert_queue.render(records)
    if clicked_from_alert:
        st.session_state.selected_symbol = clicked_from_alert

with zone_c:
    selected = st.session_state.selected_symbol
    record   = next((r for r in records if r.symbol == selected), None)
    position_detail.render(record)

st.divider()

# ════════════════════════════════════════════════════════
# ZONE D + E — Technical Health（左）＋ History（右）
# ════════════════════════════════════════════════════════
zone_d, zone_e = st.columns([4, 6], gap="large")

with zone_d:
    clicked_from_health = technical_health.render(records)
    if clicked_from_health:
        st.session_state.selected_symbol = clicked_from_health

with zone_e:
    history_view.render(st.session_state.selected_symbol)

# ── Footer ────────────────────────────────────────────────
st.divider()
st.caption(
    "⚠️ 本 Dashboard 僅供決策輔助，不構成投資建議。"
    f" | Mode: {'Mock Data' if config.USE_MOCK_DATA else 'Live S3'}"
    f" | Drawdown 警戒線：{config.DRAWDOWN_ALERT_THRESHOLD*100:.0f}%"
)
