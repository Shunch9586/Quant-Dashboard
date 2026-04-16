from dataclasses import dataclass, field
from typing import Optional
from datetime import date


@dataclass
class DecisionRecord:
    """
    核心資料單位：每個持倉 × 每天 的完整快照。
    由 4 個資料來源 JOIN 而成：
      - GSheet Inventory（持倉基本資料）
      - Price Parquet（每日價格）
      - Feature Parquet（每日技術指標）
      - Decision Layer（portfolio_manager 輸出）
    """

    # ── GSheet Inventory ──────────────────────────────────
    symbol:        str
    market:        str            # "US" | "TW" | "CRYPTO"
    position_type: str            # "quant" | "dca" | "manual"
    entry_price:   float
    entry_date:    date
    shares:        float
    stop_price:    float
    entry_score:   float

    # ── Price Parquet ─────────────────────────────────────
    curr_price:      float
    roi:             float        # 小數，例如 0.15 = +15%
    position_value:  float
    unrealized_pnl:  float

    # ── Feature Parquet ───────────────────────────────────
    ma50:                    float
    ma200:                   float
    adj_close_to_ma50_ratio: float
    momentum_raw:            float
    current_score:           float   # 0–100
    score_delta:             float
    sector:                  str
    avg_volume_20d:          float

    # ── Decision Layer ────────────────────────────────────
    should_exit:        bool
    exit_grade:         str             # "hard" | "advisory" | ""
    exit_reason_code:   str
    exit_reason_detail: str
    exit_price:         float
    distance_to_stop:   float           # 正數 = 還有空間，負數 = 已破
    distance_to_ma50:   float

    # ── 衍生欄位 ──────────────────────────────────────────
    @property
    def holding_days(self) -> int:
        return (date.today() - self.entry_date).days

    @property
    def at_stop_risk(self) -> bool:
        """距離 stop price 5% 以內視為高風險（未設止損的持倉不計入）"""
        return self.stop_price > 0 and 0 <= self.distance_to_stop < 0.05


@dataclass
class PortfolioSummaryData:
    """Zone A 所需的聚合資料"""
    portfolio_roi:          float
    current_drawdown:       float
    total_positions:        int
    hard_exits_pending:     int
    advisory_exits_pending: int
    positions_at_stop_risk: int
    sector_weights:         dict   # {sector_name: weight_float}


@dataclass
class HistoryPoint:
    """Zone E 時間序列的單一資料點"""
    date:        date
    roi:         float
    curr_price:  float
    score:       float
    score_delta: float
    should_exit: bool
    exit_grade:  str   # "hard" | "advisory" | ""
