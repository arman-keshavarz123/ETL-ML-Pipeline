"""Baseline ML model — predict tomorrow's price movement from today's features.

Loads the enriched OHLCV feature set from finance.db, trains XGBRegressor and
Ridge models using time-series cross-validation, and saves a feature-importance
bar chart to feature_importance.png.

Usage:
    python predict.py
"""

from __future__ import annotations

import pandas as pd
import sqlalchemy
from sklearn.linear_model import Ridge
from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


FEATURES = [
    "open", "high", "low", "close", "volume",
    "sma_50", "rsi_14", "bb_upper", "bb_lower",
    "macd", "macd_signal", "macd_histogram",
]


def load_data() -> pd.DataFrame:
    """Load the daily_price_features table from finance.db, sorted by date."""
    engine = sqlalchemy.create_engine("sqlite:///finance.db")
    df = pd.read_sql("SELECT * FROM daily_price_features", engine)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def create_target(df: pd.DataFrame) -> pd.DataFrame:
    """Add target_return column: next-day % change in close price."""
    df = df.copy()
    df["target_return"] = (df["close"].shift(-1) - df["close"]) / df["close"] * 100
    df = df.iloc[:-1]  # drop last row (no future target)
    return df


def train_evaluate(df: pd.DataFrame) -> None:
    """Train XGBRegressor and Ridge with TimeSeriesSplit, print RMSE per fold."""
    X = df[FEATURES]
    y = df["target_return"]

    tscv = TimeSeriesSplit(n_splits=5)

    models = {
        "XGBoost": XGBRegressor(n_estimators=100, max_depth=4, random_state=42),
        "Ridge": Ridge(alpha=1.0),
    }

    for name, model in models.items():
        rmses: list[float] = []
        for fold, (train_idx, test_idx) in enumerate(tscv.split(X), start=1):
            X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
            y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

            model.fit(X_train, y_train)
            preds = model.predict(X_test)
            rmse = root_mean_squared_error(y_test, preds)
            rmses.append(rmse)
            print(f"  {name} fold {fold}: RMSE = {rmse:.4f}")

        mean_rmse = sum(rmses) / len(rmses)
        print(f"  {name} mean RMSE: {mean_rmse:.4f}\n")


def plot_importance(df: pd.DataFrame) -> None:
    """Train final XGBoost on full data and save top-5 feature importance plot."""
    X = df[FEATURES]
    y = df["target_return"]

    model = XGBRegressor(n_estimators=100, max_depth=4, random_state=42)
    model.fit(X, y)

    importances = pd.Series(model.feature_importances_, index=FEATURES)
    top5 = importances.nlargest(5)

    fig, ax = plt.subplots(figsize=(8, 4))
    top5.sort_values().plot.barh(ax=ax)
    ax.set_xlabel("Importance")
    ax.set_title("Top 5 Feature Importances (XGBoost)")
    fig.tight_layout()
    fig.savefig("feature_importance.png", dpi=150)
    plt.close(fig)
    print("Saved feature_importance.png")


def main() -> None:
    print("Loading data from finance.db...")
    df = load_data()
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns\n")

    df = create_target(df)
    print(f"After target creation: {len(df)} rows\n")

    print("Training models with TimeSeriesSplit (5 folds):\n")
    train_evaluate(df)

    plot_importance(df)


if __name__ == "__main__":
    main()
