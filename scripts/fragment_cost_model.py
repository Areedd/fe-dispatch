#!/usr/bin/env python3
import argparse
import json
import sys

import joblib
import pandas as pd


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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Predict fragment cost (CPU% + memory + time) using trained models.")
    parser.add_argument("--cpu-model", required=True,
                        help="Path to joblib CPU usage model pipeline")
    parser.add_argument("--mem-model", required=True,
                        help="Path to joblib memory model pipeline")
    parser.add_argument("--time-model", required=True,
                        help="Path to joblib execution time model pipeline")
    parser.add_argument("--input-json", required=False,
                        help="JSON array of feature dicts. Keys: fragment_type, scan_rows, "
                             "avg_row_size, dop, join_count, agg_count, sort_count, plan_height")
    parser.add_argument("--server", action="store_true",
                        help="Run in stdin/stdout server mode; each input line is a JSON payload.")
    return parser.parse_args()


def load_models(cpu_path, mem_path, time_path):
    cpu_model = joblib.load(cpu_path)
    mem_model = joblib.load(mem_path)
    time_model = joblib.load(time_path)
    return cpu_model, mem_model, time_model


def predict(cpu_model, mem_model, time_model, features_json):
    rows = json.loads(features_json)
    if not isinstance(rows, list):
        rows = [rows]

    df = pd.DataFrame(rows)

    for col in FEATURE_COLUMNS:
        if col not in df.columns:
            if col == "fragment_type":
                df[col] = "UNKNOWN"
            else:
                df[col] = 0

    features = df[FEATURE_COLUMNS]

    cpu_pred = cpu_model.predict(features)
    mem_pred = mem_model.predict(features)
    time_pred = time_model.predict(features)

    cpu_pred = pd.Series(cpu_pred).clip(lower=0, upper=100).tolist()
    mem_pred = pd.Series(mem_pred).clip(lower=0).tolist()
    time_pred = pd.Series(time_pred).clip(lower=0).tolist()

    return [
        {
            "predictedCpuUsagePercent": float(cpu),
            "predictedMemBytes": float(mem),
            "predictedMaxTimeMs": float(time)
        }
        for cpu, mem, time in zip(cpu_pred, mem_pred, time_pred)
    ]


def main():
    args = parse_args()
    cpu_model, mem_model, time_model = load_models(args.cpu_model, args.mem_model, args.time_model)

    if args.server:
        for line in sys.stdin:
            payload = line.strip()
            if not payload:
                continue
            try:
                result = predict(cpu_model, mem_model, time_model, payload)
                sys.stdout.write(json.dumps(result) + "\n")
                sys.stdout.flush()
            except Exception as exc:
                sys.stdout.write(json.dumps({"error": str(exc)}) + "\n")
                sys.stdout.flush()
    else:
        if not args.input_json:
            print("Error: --input-json required when not in server mode", file=sys.stderr)
            sys.exit(1)
        result = predict(cpu_model, mem_model, time_model, args.input_json)
        sys.stdout.write(json.dumps(result))


if __name__ == "__main__":
    main()
