"""
市場掃描頁面
全市場股票列表，支援多條件篩選與排序。
資料來自 ETL pipeline 每日產出的 scan.parquet（S3）。
"""

import streamlit as st
import pandas as pd

from data.market_scan_loader import load_market_scan


# ── 篩選預設值 ────────────────────────────────────────────
_TREND_LABELS = {
    "全部": None,
    "🟢 多頭排列": "bull",
    "🔴 空頭排列": "bear",
    "🟡 混合": "mixed",
}

_SIGNAL_OPTIONS = {
    "VCP 型態":   "vcp_flag",
    "突破前高":   "breakout_flag",
    "反轉訊號":   "reversal_flag",
    "爆量":        "high_volume",
    "近 52 週高點": "near_52w_high",
}

_SORT_OPTIONS = {
    "Score（高→低）":       ("score",           False),
    "Score Delta（高→低）": ("score_delta",      False),
    "爆量倍數（高→低）":    ("vol_ratio_20d",    False),
    "距 52 週高（近→遠）":  ("dist_to_52w_high", False),
    "代號（A→Z）":           ("symbol",           True),
}


@st.cache_data(ttl=300, show_spinner="載入市場掃描資料...")
def _cached_scan() -> pd.DataFrame:
    return load_market_scan()


def render() -> None:
    st.markdown("### 🔍 市場掃描")

    df = _cached_scan()

    # ── 尚無資料（ETL 尚未產出）────────────────────────────
    if df.empty:
        st.info(
            "📭 尚無市場掃描資料。\n\n"
            "請先執行 ETL pipeline 的 `full_market_scan.py`，\n"
            "確認 S3 路徑 `data_lake/market_scan/latest/scan.parquet` 存在後重新載入。"
        )
        if st.button("🔄 重新載入", key="scan_reload"):
            _cached_scan.clear()
            st.rerun()
        return

    # ════════════════════════════════════════════════════════
    # 篩選列（三行排列，保持頁面不雜亂）
    # ════════════════════════════════════════════════════════
    with st.expander("🎛️ 篩選條件", expanded=True):
        row1_cols = st.columns([2, 3, 2, 2])
        row2_cols = st.columns([3, 3, 2, 2])
        row3_cols = st.columns(5)

        # Row 1
        with row1_cols[0]:
            market_choice = st.selectbox(
                "市場", ["全部", "TW", "US"], key="scan_market"
            )
        with row1_cols[1]:
            # 過濾空字串，確保選單清晰
            industries = sorted([
                i for i in df["industry"].dropna().unique().tolist()
                if str(i).strip() not in ("", "nan")
            ])
            industry_choice = st.multiselect(
                "產業別", industries, key="scan_industry",
                placeholder="全部產業"
            )
        with row1_cols[2]:
            trend_choice = st.selectbox(
                "技術狀態", list(_TREND_LABELS.keys()), key="scan_trend"
            )
        with row1_cols[3]:
            sort_choice = st.selectbox(
                "排序方式", list(_SORT_OPTIONS.keys()), key="scan_sort"
            )

        # Row 2
        with row2_cols[0]:
            score_min, score_max = st.slider(
                "Score 範圍", 0, 100, (0, 100), step=5, key="scan_score"
            )
        with row2_cols[1]:
            keyword = st.text_input(
                "代號 / 名稱搜尋", placeholder="例如：2330 或 NVDA", key="scan_kw"
            )
        with row2_cols[2]:
            ma50_only = st.checkbox("收盤 > MA50", key="scan_ma50")
        with row2_cols[3]:
            ma200_only = st.checkbox("MA50 > MA200", key="scan_ma200")

        # Row 3：策略訊號 checkboxes
        signal_choices = []
        for i, (label, col_name) in enumerate(_SIGNAL_OPTIONS.items()):
            with row3_cols[i]:
                if st.checkbox(label, key=f"scan_sig_{col_name}"):
                    signal_choices.append(col_name)

    # ════════════════════════════════════════════════════════
    # 套用篩選
    # ════════════════════════════════════════════════════════
    filtered = df.copy()

    if market_choice != "全部":
        filtered = filtered[filtered["market"] == market_choice]

    if industry_choice:
        filtered = filtered[filtered["industry"].isin(industry_choice)]

    trend_val = _TREND_LABELS[trend_choice]
    if trend_val:
        filtered = filtered[filtered["trend_state"] == trend_val]

    filtered = filtered[
        (filtered["score"] >= score_min) & (filtered["score"] <= score_max)
    ]

    if keyword.strip():
        kw = keyword.strip().upper()
        filtered = filtered[
            filtered["symbol"].str.upper().str.contains(kw, na=False) |
            filtered["name"].str.upper().str.contains(kw, na=False)
        ]

    if ma50_only:
        filtered = filtered[filtered["above_ma50"] == True]

    if ma200_only:
        filtered = filtered[filtered["ma50_above_ma200"] == True]

    for sig_col in signal_choices:
        filtered = filtered[filtered[sig_col] == True]

    # ── 排序 ──────────────────────────────────────────────
    sort_col, sort_asc = _SORT_OPTIONS[sort_choice]
    filtered = filtered.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)

    # ════════════════════════════════════════════════════════
    # 結果統計列
    # ════════════════════════════════════════════════════════
    # 資料日期（若有 date 欄位）
    data_date = ""
    if "date" in df.columns and not df["date"].isnull().all():
        try:
            data_date = f"　｜　資料日期：{df['date'].max()}"
        except Exception:
            pass

    st.markdown(
        f"**找到 {len(filtered)} 支**（共 {len(df)} 支）"
        f"　TW: {len(filtered[filtered['market']=='TW'])} 支"
        f"　US: {len(filtered[filtered['market']=='US'])} 支"
        f"{data_date}"
    )

    if filtered.empty:
        st.warning("目前篩選條件沒有符合的股票，請放寬條件。")
        return

    # ════════════════════════════════════════════════════════
    # 顯示表格
    # ════════════════════════════════════════════════════════
    display = _build_display(filtered)
    st.dataframe(
        display,
        use_container_width=True,
        height=520,
        column_config={
            "symbol":      st.column_config.TextColumn("代號", width=80),
            "name":        st.column_config.TextColumn("名稱", width=120),
            "market":      st.column_config.TextColumn("市場", width=60),
            "industry":    st.column_config.TextColumn("產業別", width=120),
            "close":       st.column_config.NumberColumn("收盤", format="%.2f", width=80),
            "score":       st.column_config.ProgressColumn(
                               "Score", min_value=0, max_value=100, format="%.0f", width=100
                           ),
            "Δ Score":     st.column_config.NumberColumn("Δ Score", format="%+.1f", width=80),
            "trend":       st.column_config.TextColumn("狀態", width=90),
            "vol_x":       st.column_config.NumberColumn("量/均量", format="%.1fx", width=80),
            "dist_52w":    st.column_config.NumberColumn("距高點", format="%.1f%%", width=80),
            "signals":     st.column_config.TextColumn("訊號", width=100),
        },
        hide_index=True,
    )

    # ── 底部重新載入按鈕 ──────────────────────────────────
    if st.button("🔄 重新載入掃描資料", key="scan_reload_bottom"):
        _cached_scan.clear()
        st.rerun()


# ════════════════════════════════════════════════════════
# 輔助：建立顯示用 DataFrame
# ════════════════════════════════════════════════════════

def _build_display(df: pd.DataFrame) -> pd.DataFrame:
    """從原始 DataFrame 建立適合顯示的精簡版本"""

    def trend_badge(t):
        return {"bull": "🟢 多頭", "bear": "🔴 空頭", "mixed": "🟡 混合"}.get(t, t)

    def signal_badges(row):
        badges = []
        if row.get("vcp_flag"):       badges.append("VCP")
        if row.get("breakout_flag"):  badges.append("突破")
        if row.get("reversal_flag"):  badges.append("反轉")
        if row.get("high_volume"):    badges.append("爆量")
        if row.get("near_52w_high"):  badges.append("近高點")
        return " ".join(badges) if badges else "—"

    out = pd.DataFrame({
        "symbol":   df["symbol"],
        "name":     df["name"],
        "market":   df["market"],
        "industry": df["industry"],
        "close":    df["close"],
        "score":    df["score"],
        "Δ Score":  df["score_delta"],
        "trend":    df["trend_state"].apply(trend_badge),
        "vol_x":    df["vol_ratio_20d"],
        "dist_52w": df["dist_to_52w_high"] * 100,   # 轉成百分比
        "signals":  df.apply(signal_badges, axis=1),
    })
    return out
