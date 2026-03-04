"""
Serving workload for IPPR demo.

Submits 1 Ray task requiring 4 CPUs (pinned to serving-workers).
The serving-workers pod starts at 2 CPU and resizes to 4 CPU via IPPR.

Usage:
    ray job submit --address http://<head-svc>:8265 --working-dir . -- python workload_serve.py
"""

import ray
import time
import os


@ray.remote(num_cpus=4, resources={"serving": 1})
def serve_workload(task_id: int):
    """Simulate a serving workload that needs 4 CPUs. Pinned to serving-workers."""
    import numpy as np

    pid = os.getpid()
    node = ray.get_runtime_context().get_node_id()[:8]
    print(f"[serve-{task_id}] starting on node={node} pid={pid}, using 4 CPUs")

    start = time.time()
    for i in range(10):
        a = np.random.randn(1000, 1000)
        _ = a @ a.T
        time.sleep(1)

    elapsed = time.time() - start
    print(f"[serve-{task_id}] done in {elapsed:.1f}s")
    return {"task_id": task_id, "elapsed": elapsed}


def main():
    ray.init()

    print("=" * 60)
    print("Submitting 1 serving task × 4 CPUs.")
    print("serving-workers pod starts at 2 CPU.")
    print("IPPR should resize the pod from 2 → 4 CPU.")
    print("=" * 60)

    future = serve_workload.remote(0)
    print("\n[serve] Task submitted. Waiting for IPPR resize + execution...")

    result = ray.get(future)
    print(f"  task={result['task_id']}  elapsed={result['elapsed']:.1f}s")
    print("\n[serve] Serving task complete!")


if __name__ == "__main__":
    main()
