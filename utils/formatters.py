"""
格式化輔助函數
- 數字顯示（百分比、金額）
- 顏色判斷（漲跌、等級）
- Streamlit metric delta 格式
"""


def fmt_pct(value: float, digits: int = 2) -> str:
    """0.1523 → '+15.23%'"""
    sign = "+" if value >= 0 else ""
    return f"{sign}{value * 100:.{digits}f}%"


def fmt_price(value: float, currency: str = "") -> str:
    """1234.5 → '$1,234.50'"""
    prefix = "$" if not currency or currency == "USD" else ""
    return f"{prefix}{value:,.2f}"


def fmt_pnl(value: float) -> str:
    """1234.5 → '+$1,234.50' / -500 → '-$500.00'"""
    sign = "+" if value >= 0 else ""
    return f"{sign}${value:,.2f}"


def fmt_score_delta(delta: float) -> str:
    """+8 → '▲ 8'  /  -12 → '▼ 12'"""
    if delta > 0:
        return f"▲ {delta:.0f}"
    elif delta < 0:
        return f"▼ {abs(delta):.0f}"
    return "─ 0"


def grade_color(exit_grade: str) -> str:
    """回傳 exit_grade 對應的 CSS 顏色"""
    return {"hard": "#FF4B4B", "advisory": "#FFA500", "": "#4CAF50"}.get(exit_grade, "#888")


def grade_label(exit_grade: str) -> str:
    return {"hard": "🔴 Hard", "advisory": "🟠 Advisory", "": "✅ Hold"}.get(exit_grade, "")


def roi_color(roi: float) -> str:
    """正報酬綠色，負報酬紅色"""
    return "#2ecc71" if roi >= 0 else "#e74c3c"


def score_color(score: float) -> str:
    """70+ 綠，40-70 黃，<40 紅"""
    if score >= 70:
        return "#2ecc71"
    elif score >= 40:
        return "#f39c12"
    return "#e74c3c"


def score_delta_color(delta: float) -> str:
    return "#2ecc71" if delta >= 0 else "#e74c3c"


def drawdown_status(drawdown: float, threshold: float = -0.15) -> str:
    """回傳 drawdown 的警示等級"""
    if drawdown <= threshold:
        return "danger"
    elif drawdown <= threshold * 0.6:
        return "warning"
    return "normal"
