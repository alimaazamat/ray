"""
Embedded benchmark runner for IPPR tests.

This script is embedded in the RayJob via ConfigMap and runs the benchmark
immediately when the cluster starts, triggering autoscaling right away.
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd
import ray
from ray.data.aggregate import Count, Mean, Sum
from ray.data.context import DataContext, ShuffleStrategy


def filter_shipdate(
    batch: pd.DataFrame,
    target_date=datetime.strptime("1998-12-01", "%Y-%m-%d").date()
    - timedelta(days=90),
) -> pd.DataFrame:
    """Filter rows where ship date is before target date."""
    return batch[batch["column10"] <= target_date]


def compute_disc_price(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    """Compute discounted price."""
    batch["disc_price"] = batch["column05"] * (1 - batch["column06"])
    return batch


def compute_charge(batch):
    """Compute charge."""
    batch["charge"] = (
        batch["column05"] * (1 - batch["column06"]) * (1 + batch["column07"])
    )
    return batch


def run_tpch_q1(scale_factor: int = 10):
    """Run TPCH Q1 query and return execution time."""
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{scale_factor}/lineitem"
    DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    start_time = time.time()

    ds = (
        ray.data.read_parquet(path)
        .map_batches(filter_shipdate, batch_format="pandas")
        .map_batches(compute_disc_price)
        .map_batches(compute_charge)
        .groupby(["column08", "column09"])
        .aggregate(
            Sum(on="column04", alias_name="sum_qty"),
            Sum(on="column05", alias_name="sum_base_price"),
            Sum(on="disc_price", alias_name="sum_disc_price"),
            Sum(on="charge", alias_name="sum_charge"),
            Mean(on="column04", alias_name="avg_qty"),
            Mean(on="column05", alias_name="avg_price"),
            Mean(on="column06", alias_name="avg_disc"),
            Count(),
        )
        .sort(["column08", "column09"])
        .materialize()
    )

    execution_time = time.time() - start_time
    print(f"Query completed in {execution_time:.2f}s")
    print(ds.stats())

    return execution_time


def main():
    """Run benchmark and output results as JSON."""
    # Configuration from environment
    mode = os.environ.get("TEST_MODE", "ippr")
    scale_factor = int(os.environ.get("TPCH_SCALE_FACTOR", "10"))
    num_runs = int(os.environ.get("NUM_RUNS", "3"))

    print(f"="*60)
    print(f"IPPR TPCH Benchmark - Mode: {mode}")
    print(f"Scale Factor: {scale_factor}, Runs: {num_runs}")
    print(f"="*60)

    # Connect to Ray
    ray.init(address="auto")
    print(f"Connected to Ray cluster")
    print(f"Cluster resources: {ray.cluster_resources()}")
    print(f"Available resources: {ray.available_resources()}")

    # Run benchmark multiple times
    execution_times = []
    for run in range(1, num_runs + 1):
        print(f"\n--- Run {run}/{num_runs} ---")
        exec_time = run_tpch_q1(scale_factor)
        execution_times.append(exec_time)

        # Wait between runs
        if run < num_runs:
            print("Waiting 30s before next run...")
            time.sleep(30)

    # Calculate statistics
    avg_time = sum(execution_times) / len(execution_times)
    min_time = min(execution_times)
    max_time = max(execution_times)

    results = {
        "avg_execution_time_s": avg_time,
        "min_execution_time_s": min_time,
        "max_execution_time_s": max_time,
        "all_times": execution_times,
    }

    print(f"\n{'='*60}")
    print(f"Benchmark Complete - {mode}")
    print(f"{'='*60}")
    print(f"Average: {avg_time:.2f}s")
    print(f"Min: {min_time:.2f}s")
    print(f"Max: {max_time:.2f}s")
    print(f"{'='*60}")

    # Output JSON for parsing by orchestrator
    print(json.dumps(results))

    ray.shutdown()


if __name__ == "__main__":
    main()
