"""
Training workload for IPPR demo.

Submits 1 Ray task requiring 3 CPUs (pinned to training-workers).
The training-workers pod starts at 1 CPU and resizes to 3 CPU via IPPR.

Usage:
    ray job submit --address http://<head-svc>:8265 --working-dir . -- python workload_train.py
"""

import ray
import time
import os


@ray.remote(num_cpus=3, resources={"training": 1})
def train_workload(task_id: int):
    """Simulate a training workload that needs 3 CPUs. Pinned to training-workers."""
    import numpy as np

    pid = os.getpid()
    node = ray.get_runtime_context().get_node_id()[:8]
    print(f"[train-{task_id}] starting on node={node} pid={pid}, using 3 CPUs")

    start = time.time()
    a = np.random.randn(1000, 1000)
    for _ in range(10):
        a = a @ a.T
        a = a / np.linalg.norm(a)
        time.sleep(1)

    elapsed = time.time() - start
    print(f"[train-{task_id}] done in {elapsed:.1f}s")
    return {"task_id": task_id, "elapsed": elapsed}


def main():
    ray.init()

    print("=" * 60)
    print("Submitting 1 training task × 3 CPUs.")
    print("training-workers pod starts at 1 CPU.")
    print("IPPR should resize the pod from 1 → 3 CPU.")
    print("=" * 60)

    future = train_workload.remote(0)
    print("\n[train] Task submitted. Waiting for IPPR resize + execution...")

    result = ray.get(future)
    print(f"  task={result['task_id']}  elapsed={result['elapsed']:.1f}s")
    print("\n[train] Training task complete!")


if __name__ == "__main__":
    main()
