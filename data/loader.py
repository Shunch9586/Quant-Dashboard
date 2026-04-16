"""
資料載入器
- USE_MOCK_DATA=True  → 回傳 mock_data.py 的假資料（開發用）
- USE_MOCK_DATA=False → 從 S3 讀取真實 Parquet（正式環境）

當你的 S3 資料路徑確定後，填寫下方 TODO 區塊即可。
"""

import pandas as pd
from datetime import date
from functools import lru_cache

import config
from data.models import DecisionRecord, PortfolioSummaryData, HistoryPoint
from data import mock_data


# ── 公開介面（views 層只呼叫這三個函數） ──────────────────

def load_positions() -> list[DecisionRecord]:
    """載入今日所有持倉的 DecisionRecord"""
    if config.USE_MOCK_DATA:
        return mock_data.get_mock_positions()
    return _load_positions_from_s3()


def load_portfolio_summary(records: list[DecisionRecord]) -> PortfolioSummaryData:
    """從 DecisionRecord 列表聚合 Zone A summary"""
    if config.USE_MOCK_DATA:
        return mock_data.get_mock_portfolio_summary(records)
    return mock_data.get_mock_portfolio_summary(records)   # 聚合邏輯相同，不需要分開


def load_history(symbol: str, days: int = 120) -> list[HistoryPoint]:
    """載入某個 symbol 的歷史時間序列（Zone E 用）"""
    if config.USE_MOCK_DATA:
        return mock_data.get_mock_history(symbol, days)
    return _load_history_from_s3(symbol, days)


# ── S3 載入（待填寫） ──────────────────────────────────────

def _load_positions_from_s3() -> list[DecisionRecord]:
    """
    TODO: 從 S3 讀取今日資料並 JOIN 成 DecisionRecord 列表。

    需要完成的步驟：
    1. 讀取 GSheet Inventory → df_inventory
    2. 讀取 s3://{BUCKET}/{PRICE_PREFIX}{today}.parquet → df_price
    3. 讀取 s3://{BUCKET}/{FEATURE_PREFIX}{today}.parquet → df_feature
    4. 讀取 s3://{BUCKET}/{DECISION_PREFIX}{today}.parquet → df_decision
    5. JOIN 4 個 DataFrame on symbol
    6. 轉換成 DecisionRecord 列表

    範例程式碼（根據你的實際 S3 路徑調整）：

        import boto3, io
        import gspread

        today_str = date.today().strftime("%Y-%m-%d")
        s3 = boto3.client("s3", region_name=config.AWS_REGION)

        def read_parquet(prefix: str) -> pd.DataFrame:
            key = f"{prefix}{today_str}.parquet"
            obj = s3.get_object(Bucket=config.S3_BUCKET, Key=key)
            return pd.read_parquet(io.BytesIO(obj["Body"].read()))

        df_price    = read_parquet(config.S3_PRICE_PREFIX)
        df_feature  = read_parquet(config.S3_FEATURE_PREFIX)
        df_decision = read_parquet(config.S3_DECISION_PREFIX)

        # GSheet Inventory
        gc = gspread.service_account(filename="credentials.json")
        sh = gc.open("你的GSheet名稱")
        df_inventory = pd.DataFrame(sh.sheet1.get_all_records())

        # JOIN
        df = (df_inventory
              .merge(df_price,    on="symbol", how="left")
              .merge(df_feature,  on="symbol", how="left")
              .merge(df_decision, on="symbol", how="left"))

        return [DecisionRecord(**row) for row in df.to_dict("records")]
    """
    raise NotImplementedError(
        "請填寫 _load_positions_from_s3()，或設定 USE_MOCK_DATA=true 使用假資料。"
    )


def _load_history_from_s3(symbol: str, days: int) -> list[HistoryPoint]:
    """
    TODO: 從 S3 讀取指定 symbol 的歷史多日資料。

    需要完成的步驟：
    1. 列出 s3://{BUCKET}/{PRICE_PREFIX} 下最近 N 天的檔案
    2. 讀取每個 parquet，filter symbol，取出 roi / curr_price
    3. 讀取 feature parquet，取出 score / score_delta
    4. 讀取 decision parquet，取出 should_exit / exit_grade
    5. 合併成 HistoryPoint 列表

    範例程式碼：

        import boto3, io
        from datetime import timedelta

        s3 = boto3.client("s3", region_name=config.AWS_REGION)
        result = []
        for i in range(days, 0, -1):
            d = date.today() - timedelta(days=i)
            date_str = d.strftime("%Y-%m-%d")
            try:
                obj = s3.get_object(
                    Bucket=config.S3_BUCKET,
                    Key=f"{config.S3_PRICE_PREFIX}{date_str}.parquet"
                )
                df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
                row = df[df["symbol"] == symbol].iloc[0]
                result.append(HistoryPoint(date=d, roi=row.roi, ...))
            except Exception:
                continue
        return result
    """
    raise NotImplementedError(
        "請填寫 _load_history_from_s3()，或設定 USE_MOCK_DATA=true 使用假資料。"
    )
