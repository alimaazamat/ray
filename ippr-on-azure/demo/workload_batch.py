"""
Batch workload for IPPR demo.

Submits 1 Ray task requiring 3 CPUs (pinned to batch-workers).
The batch-workers pod starts at 1 CPU and resizes to 3 CPU via IPPR.

Usage:
    ray job submit --address http://<head-svc>:8265 --working-dir . -- python workload_batch.py
"""

import ray
import time
import os


@ray.remote(num_cpus=3, resources={"batch": 1})
def batch_workload(task_id: int):
    """Simulate a batch processing workload that needs 3 CPUs. Pinned to batch-workers."""
    import numpy as np

    pid = os.getpid()
    node = ray.get_runtime_context().get_node_id()[:8]
    print(f"[batch-{task_id}] starting on node={node} pid={pid}, using 3 CPUs")

    start = time.time()
    data = np.random.randn(5_000_000)
    data = np.sort(data)
    result = {
        "mean": float(np.mean(data)),
        "std": float(np.std(data)),
        "p99": float(np.percentile(data, 99)),
    }
    time.sleep(5)

    elapsed = time.time() - start
    print(f"[batch-{task_id}] done in {elapsed:.1f}s")
    return {"task_id": task_id, "elapsed": elapsed, **result}


def main():
    ray.init()

    print("=" * 60)
    print("Submitting 1 batch task × 3 CPUs.")
    print("batch-workers pod starts at 1 CPU.")
    print("IPPR should resize the pod from 1 → 3 CPU.")
    print("=" * 60)

    future = batch_workload.remote(0)
    print("\n[batch] Task submitted. Waiting for IPPR resize + execution...")

    result = ray.get(future)
    print(f"  task={result['task_id']}  elapsed={result['elapsed']:.1f}s")
    print("\n[batch] Batch task complete!")


if __name__ == "__main__":
    main()
