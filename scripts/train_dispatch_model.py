import argparse
import joblib
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBRegressor


FEATURE_COLUMNS = [
    "fragment_type",
    "scan_rows",
    "avg_row_size",
    "dop",
    "join_count",
    "agg_count",
    "sort_count",
    "plan_height",
]

CATEGORICAL_FEATURES = ["fragment_type"]
NUMERIC_FEATURES = ["scan_rows", "avg_row_size", "dop", "join_count",
                    "agg_count", "sort_count", "plan_height"]


def build_preprocessor():
    categorical_transformer = OneHotEncoder(handle_unknown="ignore")
    numeric_transformer = Pipeline(steps=[("scaler", StandardScaler())])

    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", categorical_transformer, CATEGORICAL_FEATURES),
            ("numeric", numeric_transformer, NUMERIC_FEATURES),
        ])
    return preprocessor


def build_model(objective="reg:squarederror"):
    return XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        objective=objective,
        n_jobs=4,
        reg_lambda=1.0,
    )


def train_pipeline(df, target_column, feature_cols):
    features = df[feature_cols].copy()
    target = df[target_column].copy()

    preprocessor = build_preprocessor()
    model = build_model()
    pipeline = Pipeline(steps=[("preprocess", preprocessor),
                               ("model", model)])

    X_train, X_valid, y_train, y_valid = train_test_split(
        features, target, test_size=0.2, random_state=42)
    pipeline.fit(X_train, y_train)

    score = pipeline.score(X_valid, y_valid)
    print(f"{target_column} R^2 on hold-out: {score:.4f}")

    y_pred = pipeline.predict(X_valid)
    mae = np.mean(np.abs(y_valid - y_pred))
    print(f"{target_column} MAE on hold-out: {mae:.4f}")

    return pipeline


def dump_feature_importance(pipeline, label):
    booster = pipeline.named_steps["model"].get_booster()
    feature_names = pipeline.named_steps["preprocess"].get_feature_names_out()
    scores = booster.get_score(importance_type="weight")
    print(f"\nFeature importance for {label}:")
    for fname, imp in sorted(scores.items(), key=lambda kv: kv[1], reverse=True):
        readable = feature_names[int(fname[1:])] if fname.startswith("f") else fname
        print(f"  {readable}: {imp}")


def prepare_data(df):
    required_features = set(FEATURE_COLUMNS)
    required_targets = {"cpu_usage_percent", "avg_mem_bytes", "max_time_ms"}
    required = required_features | required_targets

    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing columns in input CSV: {missing}")

    df = df.copy()

    df = df[df["cpu_usage_percent"] >= 0]
    df = df[df["cpu_usage_percent"] <= 100]
    df = df[df["scan_rows"] >= 0]
    df = df[df["avg_row_size"] >= 0]
    df = df[df["avg_mem_bytes"] >= 0]
    df = df[df["max_time_ms"] >= 0]

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Train dispatch cost models (CPU usage + memory + time) from CSV logs.")
    parser.add_argument("--input", default="dispatch_training.csv",
                        help="Path to training CSV (default: dispatch_training.csv)")
    parser.add_argument("--cpu-model", default="dispatch_cpu_model.pkl",
                        help="Output path for CPU usage model pipeline")
    parser.add_argument("--mem-model", default="dispatch_mem_model.pkl",
                        help="Output path for memory model pipeline")
    parser.add_argument("--time-model", default="dispatch_time_model.pkl",
                        help="Output path for execution time model pipeline")
    args = parser.parse_args()

    print(f"Loading training data from {args.input}...")
    df = pd.read_csv(args.input)
    df = prepare_data(df)

    print("\n=== Training CPU Usage Model ===")
    cpu_pipeline = train_pipeline(df, "cpu_usage_percent", FEATURE_COLUMNS)
    dump_feature_importance(cpu_pipeline, "cpu_usage_percent")
    joblib.dump(cpu_pipeline, args.cpu_model)
    print(f"Saved CPU model to {args.cpu_model}")

    print("\n=== Training Memory Model ===")
    mem_pipeline = train_pipeline(df, "avg_mem_bytes", FEATURE_COLUMNS)
    dump_feature_importance(mem_pipeline, "avg_mem_bytes")
    joblib.dump(mem_pipeline, args.mem_model)
    print(f"Saved memory model to {args.mem_model}")

    print("\n=== Training Execution Time Model ===")
    time_pipeline = train_pipeline(df, "max_time_ms", FEATURE_COLUMNS)
    dump_feature_importance(time_pipeline, "max_time_ms")
    joblib.dump(time_pipeline, args.time_model)
    print(f"Saved time model to {args.time_model}")

    print("\n=== Training Complete ===")
    print(f"Models saved:")
    print(f"  CPU model: {args.cpu_model}")
    print(f"  Memory model: {args.mem_model}")
    print(f"  Time model: {args.time_model}")


if __name__ == "__main__":
    main()
