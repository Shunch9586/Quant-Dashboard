"""
Mock 資料產生器
用途：在真實 S3 資料接通之前，讓 Dashboard 可以正常啟動與展示。
切換方式：config.py 的 USE_MOCK_DATA = True / False
"""

from datetime import date, timedelta
import random
from data.models import DecisionRecord, PortfolioSummaryData, HistoryPoint

random.seed(42)


# ── 持倉清單（模擬真實組合） ──────────────────────────────
_POSITIONS_META = [
    # symbol, market, type,    sector,        entry_price, entry_days_ago, shares, stop_price, entry_score
    ("AAPL",  "US", "quant",  "Technology",   172.0,  80,  15,  155.0, 78),
    ("NVDA",  "US", "quant",  "Technology",   480.0,  45,   5,  430.0, 85),
    ("TSLA",  "US", "quant",  "Automotive",   210.0,  30,  10,  185.0, 62),
    ("MSFT",  "US", "dca",    "Technology",   380.0, 120,   8,  340.0, 80),
    ("XOM",   "US", "quant",  "Energy",       105.0,  55,  20,   92.0, 71),
    ("JPM",   "US", "manual", "Finance",      195.0,  90,  12,  175.0, 66),
    ("2330",  "TW", "quant",  "Technology",   580.0,  60,   5,  530.0, 82),
    ("2317",  "TW", "dca",    "Technology",    98.0, 150,  50,   85.0, 59),
    ("0050",  "TW", "dca",    "ETF",           95.0, 200, 100,   85.0, 74),
    ("2454",  "TW", "quant",  "Technology",   720.0,  25,   3,  650.0, 55),
    ("2882",  "TW", "manual", "Finance",       43.0, 110,  80,   38.0, 63),
]

# ── 模擬當前價格（相對 entry_price 有漲有跌） ─────────────
_PRICE_MULTIPLIERS = {
    "AAPL": 1.12, "NVDA": 0.88, "TSLA": 0.78,
    "MSFT": 1.22, "XOM":  1.05, "JPM":  1.08,
    "2330": 1.15, "2317": 0.93, "0050": 1.09,
    "2454": 0.82, "2882": 1.03,
}

# ── 模擬 Decision Layer 輸出 ──────────────────────────────
_DECISIONS = {
    #  should_exit, exit_grade,  reason_code,         reason_detail
    "AAPL": (False, "",         "",                   ""),
    "NVDA": (True,  "hard",     "STOP_BREACH",        "Price crossed stop_price $430. Distance: -2.3%"),
    "TSLA": (True,  "hard",     "MA50_BREAKDOWN",     "Price 12.5% below MA50. Momentum deteriorating."),
    "MSFT": (False, "",         "",                   ""),
    "XOM":  (True,  "advisory", "SCORE_DECLINE",      "Score dropped from 71 to 48 over 7 days."),
    "JPM":  (False, "",         "",                   ""),
    "2330": (False, "",         "",                   ""),
    "2317": (True,  "advisory", "SCORE_DECLINE",      "Score delta -18 over 5 days. Watch closely."),
    "0050": (False, "",         "",                   ""),
    "2454": (True,  "hard",     "STOP_BREACH",        "Price crossed stop_price $650. Position underwater -13.9%."),
    "2882": (False, "",         "",                   ""),
}

# ── 技術指標模擬 ──────────────────────────────────────────
_TECHNICALS = {
    # symbol: (ma50_ratio, momentum_raw, current_score, score_delta)
    "AAPL": (1.08,  0.42,  76, +4),
    "NVDA": (0.96, -0.38,  41, -18),
    "TSLA": (0.87, -0.61,  29, -22),
    "MSFT": (1.14,  0.55,  83, +6),
    "XOM":  (1.02, -0.12,  48, -14),
    "JPM":  (1.06,  0.28,  69, +2),
    "2330": (1.11,  0.49,  80, +7),
    "2317": (0.91, -0.29,  45, -18),
    "0050": (1.05,  0.31,  72, +3),
    "2454": (0.84, -0.55,  33, -20),
    "2882": (1.03,  0.15,  65, -3),
}


def get_mock_positions() -> list[DecisionRecord]:
    """今日所有持倉的 DecisionRecord 清單"""
    records = []
    today = date.today()

    for (symbol, market, ptype, sector, entry_price,
         entry_days_ago, shares, stop_price, entry_score) in _POSITIONS_META:

        curr_price = round(entry_price * _PRICE_MULTIPLIERS[symbol], 2)
        roi = (_PRICE_MULTIPLIERS[symbol] - 1.0)
        position_value = round(curr_price * shares, 2)
        unrealized_pnl = round((curr_price - entry_price) * shares, 2)

        ma50 = round(curr_price / _TECHNICALS[symbol][0], 2)
        ma200 = round(ma50 * random.uniform(0.90, 1.05), 2)
        ma50_ratio, momentum_raw, score, score_delta = _TECHNICALS[symbol]

        should_exit, exit_grade, reason_code, reason_detail = _DECISIONS[symbol]
        exit_price = curr_price if should_exit else 0.0
        distance_to_stop = round((curr_price - stop_price) / stop_price, 4)
        distance_to_ma50 = round((curr_price - ma50) / ma50, 4)

        records.append(DecisionRecord(
            symbol=symbol,
            market=market,
            position_type=ptype,
            entry_price=entry_price,
            entry_date=today - timedelta(days=entry_days_ago),
            shares=shares,
            stop_price=stop_price,
            entry_score=entry_score,
            curr_price=curr_price,
            roi=roi,
            position_value=position_value,
            unrealized_pnl=unrealized_pnl,
            ma50=ma50,
            ma200=ma200,
            adj_close_to_ma50_ratio=ma50_ratio,
            momentum_raw=momentum_raw,
            current_score=score,
            score_delta=score_delta,
            sector=sector,
            avg_volume_20d=random.uniform(1e6, 5e7),
            should_exit=should_exit,
            exit_grade=exit_grade,
            exit_reason_code=reason_code,
            exit_reason_detail=reason_detail,
            exit_price=exit_price,
            distance_to_stop=distance_to_stop,
            distance_to_ma50=distance_to_ma50,
        ))

    return records


def get_mock_portfolio_summary(records: list[DecisionRecord]) -> PortfolioSummaryData:
    """從 DecisionRecord 列表聚合出 Zone A 所需的 summary"""
    total_value = sum(r.position_value for r in records)
    portfolio_roi = sum(r.roi * r.position_value for r in records) / total_value if total_value else 0

    # 模擬 peak value（比現在高 8%）
    peak_value = total_value * 1.08
    current_drawdown = (total_value - peak_value) / peak_value

    sector_weights = {}
    for r in records:
        sector_weights[r.sector] = sector_weights.get(r.sector, 0) + r.position_value
    sector_weights = {k: v / total_value for k, v in sector_weights.items()}

    return PortfolioSummaryData(
        portfolio_roi=portfolio_roi,
        current_drawdown=current_drawdown,
        total_positions=len(records),
        hard_exits_pending=sum(1 for r in records if r.exit_grade == "hard"),
        advisory_exits_pending=sum(1 for r in records if r.exit_grade == "advisory"),
        positions_at_stop_risk=sum(1 for r in records if r.at_stop_risk),
        sector_weights=sector_weights,
    )


def get_mock_history(symbol: str, days: int = 120) -> list[HistoryPoint]:
    """產生某個 symbol 的模擬歷史資料（用於 Zone E）"""
    random.seed(hash(symbol) % 999)
    today = date.today()
    records = []

    price = dict(zip(
        [m[0] for m in _POSITIONS_META],
        [m[4] for m in _POSITIONS_META]
    )).get(symbol, 100.0)

    score = 70.0
    for i in range(days, 0, -1):
        d = today - timedelta(days=i)
        price *= random.uniform(0.975, 1.025)
        roi = (price / dict(zip(
            [m[0] for m in _POSITIONS_META],
            [m[4] for m in _POSITIONS_META]
        )).get(symbol, price)) - 1.0

        score += random.uniform(-5, 5)
        score = max(10, min(95, score))
        score_delta = random.uniform(-10, 10)

        # 模擬幾個 exit 觸發點
        should_exit = random.random() < 0.08
        exit_grade = ""
        if should_exit:
            exit_grade = "hard" if random.random() < 0.4 else "advisory"

        records.append(HistoryPoint(
            date=d,
            roi=round(roi, 4),
            curr_price=round(price, 2),
            score=round(score, 1),
            score_delta=round(score_delta, 1),
            should_exit=should_exit,
            exit_grade=exit_grade,
        ))

    return records
