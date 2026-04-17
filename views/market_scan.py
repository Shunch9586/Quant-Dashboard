"""
市場掃描頁面
全市場股票列表，支援多條件篩選與排序。
TW 資料由 tw_market.db（SQLite）計算。
US 資料由 yfinance 全市場掃描 + Tiingo IEX 每日快速更新。
"""

import streamlit as st
import pandas as pd

import config
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
    "Score（高→低）":         ("score",             False),
    "Score Delta（高→低）":   ("score_delta",        False),
    "今日漲跌（高→低）":      ("daily_change_pct",   False),
    "爆量倍數（高→低）":      ("vol_ratio_20d",      False),
    "距 52 週高（近→遠）":    ("dist_to_52w_high",   False),
    "代號（A→Z）":             ("symbol",             True),
}


@st.cache_data(ttl=300, show_spinner="載入市場掃描資料...")
def _cached_scan() -> pd.DataFrame:
    return load_market_scan()


def _get_fs():
    import s3fs
    return s3fs.S3FileSystem(
        key=config.AWS_ACCESS_KEY_ID or None,
        secret=config.AWS_SECRET_ACCESS_KEY or None,
    )


def render() -> None:
    st.markdown("### 🔍 市場掃描")

    # ── 頂部控制列 ────────────────────────────────────────
    _render_data_controls()

    df = _cached_scan()

    if df.empty:
        st.info(
            "📭 尚無市場掃描資料。\n\n"
            "- **台股**：點擊上方「🔄 更新台股」\n"
            "- **美股**：點擊「🔄 全市場掃描」（首次約 2–3 分鐘），之後用「⚡ 快速更新」"
        )
        return

    # ════════════════════════════════════════════════════════
    # 篩選列
    # ════════════════════════════════════════════════════════
    with st.expander("🎛️ 篩選條件", expanded=True):
        row1_cols = st.columns([2, 3, 2, 2])
        row2_cols = st.columns([3, 3, 2, 2])
        row3_cols = st.columns(5)

        with row1_cols[0]:
            market_choice = st.selectbox("市場", ["全部", "TW", "US"], key="scan_market")
        with row1_cols[1]:
            industries = sorted([
                i for i in df["industry"].dropna().unique().tolist()
                if str(i).strip() not in ("", "nan")
            ])
            industry_choice = st.multiselect(
                "產業別", industries, key="scan_industry", placeholder="全部產業"
            )
        with row1_cols[2]:
            trend_choice = st.selectbox(
                "技術狀態", list(_TREND_LABELS.keys()), key="scan_trend"
            )
        with row1_cols[3]:
            sort_choice = st.selectbox(
                "排序方式", list(_SORT_OPTIONS.keys()), key="scan_sort"
            )

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

    # ── 排序（daily_change_pct 可能不存在，容錯） ──────────
    sort_col, sort_asc = _SORT_OPTIONS[sort_choice]
    if sort_col not in filtered.columns:
        sort_col, sort_asc = "score", False
    filtered = filtered.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)

    # ════════════════════════════════════════════════════════
    # 結果統計
    # ════════════════════════════════════════════════════════
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
    has_daily = "daily_change_pct" in filtered.columns
    display   = _build_display(filtered, has_daily=has_daily)

    col_cfg = {
        "symbol":      st.column_config.TextColumn("代號",   width=80),
        "name":        st.column_config.TextColumn("名稱",   width=120),
        "market":      st.column_config.TextColumn("市場",   width=60),
        "industry":    st.column_config.TextColumn("產業別", width=120),
        "close":       st.column_config.NumberColumn("收盤",  format="%.2f", width=80),
        "score":       st.column_config.ProgressColumn(
                           "Score", min_value=0, max_value=100, format="%.0f", width=100
                       ),
        "Δ Score":     st.column_config.NumberColumn("Δ Score",  format="%+.1f", width=80),
        "trend":       st.column_config.TextColumn("狀態",   width=90),
        "vol_x":       st.column_config.NumberColumn("量/均量", format="%.1fx", width=80),
        "dist_52w":    st.column_config.NumberColumn("距高點", format="%.1f%%", width=80),
        "signals":     st.column_config.TextColumn("訊號",   width=100),
    }
    if has_daily:
        col_cfg["今日%"] = st.column_config.NumberColumn(
            "今日%", format="%+.2f%%", width=75
        )

    st.dataframe(
        display,
        use_container_width=True,
        height=520,
        column_config=col_cfg,
        hide_index=True,
    )

    # ── 底部重新載入 ─────────────────────────────────────
    if st.button("🔄 重新載入（清快取）", key="scan_reload_bottom"):
        _cached_scan.clear()
        st.rerun()

    # ════════════════════════════════════════════════════════
    # 📰 個股新聞（Tiingo News API）
    # ════════════════════════════════════════════════════════
    _render_news_panel()


# ════════════════════════════════════════════════════════
# 資料控制列（頂部）
# ════════════════════════════════════════════════════════

def _render_data_controls() -> None:
    if config.USE_MOCK_DATA:
        st.caption("🧪 Mock 模式：顯示假資料")
        return

    from data.market_scan_fetcher import tw_scan_is_fresh, get_tw_scan_date
    from data.us_scan_fetcher    import us_scan_is_fresh, get_us_scan_date

    # 第一列：狀態
    col_tw, col_us = st.columns(2)
    with col_tw:
        scan_date = get_tw_scan_date()
        hint = f"（{scan_date}）" if scan_date else ""
        if tw_scan_is_fresh():
            st.success(f"✅ **台股** 有效{hint}")
        else:
            st.warning("⏳ **台股** 待更新")

    with col_us:
        scan_date = get_us_scan_date()
        hint = f"（{scan_date}）" if scan_date else ""
        if us_scan_is_fresh():
            st.success(f"✅ **美股** 有效{hint}")
        else:
            st.warning("⏳ **美股** 待更新")

    # 第二列：按鈕
    col_tw_btn, col_us_fast, col_us_full = st.columns(3)

    with col_tw_btn:
        if st.button("🔄 更新台股", key="scan_tw_refresh", use_container_width=True):
            _do_tw_refresh()

    with col_us_fast:
        help_txt = "用 Tiingo IEX 更新今日報價（< 30 秒），需先跑過全市場掃描"
        if st.button("⚡ 快速更新今日報價", key="scan_us_fast",
                     use_container_width=True, help=help_txt):
            _do_us_daily_update()

    with col_us_full:
        help_txt = "yfinance 下載全市場 14 個月歷史並重算 Score（約 2–3 分鐘）"
        if st.button("🔄 美股全市場掃描", key="scan_us_refresh",
                     use_container_width=True, help=help_txt):
            _do_us_refresh()


# ════════════════════════════════════════════════════════
# 更新動作
# ════════════════════════════════════════════════════════

def _do_us_daily_update() -> None:
    """IEX 批次快速更新今日報價（< 30 秒）"""
    from data.us_scan_fetcher import run_us_daily_update

    with st.status("⚡ Tiingo IEX 快速更新今日報價...", expanded=True) as status:
        try:
            ok, msg = run_us_daily_update(progress_cb=st.write)
        except Exception as e:
            status.update(label=f"❌ 更新失敗：{e}", state="error")
            return

        if ok:
            status.update(label=f"✅ {msg}", state="complete", expanded=False)
            _cached_scan.clear()
            st.rerun()
        else:
            status.update(label=f"❌ {msg}", state="error")


def _do_us_refresh() -> None:
    """yfinance 全市場掃描（2–3 分鐘）"""
    from data.us_scan_fetcher import run_us_scan

    with st.status("🌐 美股全市場掃描（yfinance + Tiingo）...", expanded=True) as status:
        try:
            ok, msg = run_us_scan(fs=None, progress_cb=st.write)
        except Exception as e:
            status.update(label=f"❌ 更新失敗：{e}", state="error")
            return

        if ok:
            status.update(label=f"✅ {msg}", state="complete", expanded=False)
            _cached_scan.clear()
            st.rerun()
        else:
            status.update(label=f"❌ {msg}", state="error")


def _do_tw_refresh() -> None:
    from data.market_scan_fetcher import run_tw_scan

    with st.status("🗄️ 從 tw_market.db 更新台股...", expanded=True) as status:
        st.write("⬇️ 確認 tw_market.db 快取（首次需下載 ~370MB）...")
        st.write("📊 讀取全市場近 310 天價格...")
        st.write("⚙️ 計算 MA / Score（約 1,800 支）...")
        st.write("💾 儲存至本地快取...")
        try:
            ok, msg = run_tw_scan(_get_fs())
        except Exception as e:
            status.update(label=f"❌ 更新失敗：{e}", state="error")
            return

        if ok:
            status.update(label=f"✅ {msg}", state="complete", expanded=False)
            _cached_scan.clear()
            st.rerun()
        else:
            status.update(label=f"❌ {msg}", state="error")


# ════════════════════════════════════════════════════════
# 📰 個股新聞面板（Tiingo News API）
# ════════════════════════════════════════════════════════

def _render_news_panel() -> None:
    """顯示個股最新新聞，由使用者輸入代號觸發"""
    st.divider()
    with st.expander("📰 個股新聞（Tiingo News API）", expanded=False):
        api_key = config.fresh("TIINGO_API_KEY")
        if not api_key:
            st.info("請設定 TIINGO_API_KEY 以啟用新聞功能")
            return

        news_sym = st.text_input(
            "輸入代號查詢新聞",
            placeholder="例如：NVDA 或 AAPL",
            key="scan_news_sym",
        ).strip().upper()

        if not news_sym:
            st.caption("輸入股票代號後顯示最新 8 則新聞")
            return

        with st.spinner(f"載入 {news_sym} 新聞..."):
            from data.tiingo_utils import fetch_stock_news
            articles = fetch_stock_news([news_sym], api_key, limit=8)

        if not articles:
            st.warning(f"找不到 {news_sym} 的相關新聞，請確認代號是否正確")
            return

        for art in articles:
            pub_date = str(art.get("publishedDate", ""))[:10]
            title    = art.get("title", "（無標題）")
            source   = art.get("source", "")
            url      = art.get("url", "")
            tickers  = ", ".join(art.get("tickers", []))

            col_date, col_body = st.columns([1, 6])
            with col_date:
                st.caption(pub_date)
                if source:
                    st.caption(f"_{source}_")
            with col_body:
                if url:
                    st.markdown(f"**[{title}]({url})**")
                else:
                    st.markdown(f"**{title}**")
                if tickers:
                    st.caption(f"相關：{tickers}")

            st.divider()


# ════════════════════════════════════════════════════════
# 輔助：建立顯示用 DataFrame
# ════════════════════════════════════════════════════════

def _build_display(df: pd.DataFrame, has_daily: bool = False) -> pd.DataFrame:

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
        "dist_52w": df["dist_to_52w_high"] * 100,
        "signals":  df.apply(signal_badges, axis=1),
    })

    if has_daily:
        out["今日%"] = df["daily_change_pct"]

    return out
