"""
IPPR benchmark test for Kubernetes - Following standard k8s_tests pattern.

This script follows the same pattern as run_gcs_ft_on_k8s.py:
1. Embeds Python scripts in YAML template using string formatting
2. Deploys RayJob with embedded benchmark code
3. Waits for RayJob completion
4. Extracts results from logs
5. Compares IPPR vs baseline and writes to TEST_OUTPUT_JSON
"""

import json
import os
import pathlib
import subprocess
import time
import uuid
from kubernetes import client, config, watch

# Global variables
CLUSTER_ID = None
IPPR_CLUSTER_NAME = None
BASELINE_CLUSTER_NAME = None

# Ray image - follow same pattern as run_gcs_ft_on_k8s.py
RAY_IMAGE = os.environ.get("RAY_IMAGE", "alimaa3amat/ray-ippr:dev")


def generate_cluster_id():
    """Generate unique cluster ID for this test run."""
    global CLUSTER_ID, IPPR_CLUSTER_NAME, BASELINE_CLUSTER_NAME
    CLUSTER_ID = str(uuid.uuid4()).split("-")[0]
    IPPR_CLUSTER_NAME = f"ray-ippr-test-{CLUSTER_ID}"
    BASELINE_CLUSTER_NAME = f"ray-baseline-test-{CLUSTER_ID}"
    print(f"Generated cluster ID: {CLUSTER_ID}")


def deploy_cluster(cluster_name, mode):
    """
    Deploy RayJob with embedded benchmark script.

    Follows the same pattern as start_rayservice() in run_gcs_ft_on_k8s.py:
    - Reads benchmark script from file
    - Embeds it in YAML template using string formatting
    - Writes to temp file and applies with kubectl
    """
    print(f"=== Deploying {mode} cluster: {cluster_name} ===")

    # Read benchmark script and indent for YAML embedding
    benchmark_script = "\n".join(
        [
            f"    {line}"
            for line in pathlib.Path("./benchmark_runner.py").read_text().splitlines()
        ]
    )

    # Configure worker resources based on mode
    if mode == "ippr":
        # IPPR: Workers start SMALL and can resize UP to max limits (vertical scaling)
        worker_cpu_limit = "8"
        worker_mem_limit = "8Gi"
        resize_policy = """resizePolicy:
            - resourceName: cpu
              restartPolicy: NotRequired
            - resourceName: memory
              restartPolicy: NotRequired"""
    else:  # baseline
        # Baseline: Workers start SMALL but CANNOT resize (forces horizontal scaling)
        # This tests traditional scaling: adding MORE pods vs IPPR resizing existing pods
        worker_cpu_limit = "8"
        worker_mem_limit = "8Gi"
        resize_policy = "# No resizePolicy - must scale horizontally by adding more pods"

    # Format YAML template with variables
    template = (
        pathlib.Path("rayjob_template.yaml")
        .read_text()
        .format(
            cluster_name=cluster_name,
            cluster_id=CLUSTER_ID,
            ray_image=RAY_IMAGE,
            mode=mode,
            worker_cpu_limit=worker_cpu_limit,
            worker_mem_limit=worker_mem_limit,
            resize_policy=resize_policy,
            benchmark_script=benchmark_script,
        )
    )

    # Remove IPPR annotation for baseline mode
    if mode == "baseline":
        lines = []
        in_annotations = False
        for line in template.split("\n"):
            # Skip the entire annotations section
            if line.strip() == "annotations:":
                in_annotations = True
                continue
            if in_annotations:
                # Skip lines that are part of the annotation (indented)
                if line.startswith("  ") and not line.strip().startswith("spec:"):
                    continue
                else:
                    # End of annotations section
                    in_annotations = False
            lines.append(line)
        template = "\n".join(lines)

    print("=== Generated YAML (first 100 lines) ===")
    print("\n".join(template.split("\n")[:100]))

    # Write to temp file
    tmp_yaml = pathlib.Path(f"/tmp/{cluster_name}.yaml")
    tmp_yaml.write_text(template)

    # Deploy with kubectl
    print(f"Creating RayJob {cluster_name}")
    result = subprocess.run(
        ["kubectl", "create", "-f", str(tmp_yaml)],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Failed to create RayJob: {result.stderr}")


def wait_for_rayjob_completion(rayjob_name, timeout=1800):
    """
    Wait for RayJob to complete.

    Similar pattern to waiting in run_gcs_ft_on_k8s.py but for RayJob status.
    """
    print(f"=== Waiting for RayJob {rayjob_name} to complete ===")

    start_time = time.time()

    while time.time() - start_time < timeout:
        # Check RayJob status
        result = subprocess.run(
            ["kubectl", "get", "rayjob", rayjob_name, "-o", "json"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            rayjob = json.loads(result.stdout)
            status = rayjob.get("status", {})
            job_status = status.get("jobStatus")

            print(f"RayJob status: {job_status}")

            if job_status == "SUCCEEDED":
                elapsed = time.time() - start_time
                print(f"RayJob {rayjob_name} completed in {elapsed:.1f}s")
                return "COMPLETE"

            if job_status == "FAILED":
                print(f"RayJob {rayjob_name} failed")
                return "FAILED"

        time.sleep(10)

    print(f"Timeout waiting for RayJob {rayjob_name}")
    return "TIMEOUT"


def get_rayjob_logs(rayjob_name):
    """Get logs from RayJob submitter pod (where job output is written)."""
    print(f"Fetching logs from {rayjob_name}...")

    config.load_kube_config()
    cli = client.CoreV1Api()

    # Find submitter pod (contains job output)
    pods = cli.list_namespaced_pod(
        namespace="default",
        label_selector=f"job-name={rayjob_name}"
    )

    for pod in pods.items:
        pod_name = pod.metadata.name
        print(f"Found submitter pod: {pod_name}")

        result = subprocess.run(
            ["kubectl", "logs", pod_name, "--tail=1000"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            return result.stdout

    print("Warning: Submitter pod not found, trying head pod...")
    # Fallback to head pod
    pods = cli.list_namespaced_pod(
        namespace="default",
        label_selector="ray.io/node-type=head"
    )

    for pod in pods.items:
        cluster_label = pod.metadata.labels.get("ray.io/cluster", "")
        if cluster_label.startswith(rayjob_name):
            pod_name = pod.metadata.name
            print(f"Found head pod: {pod_name}")

            result = subprocess.run(
                ["kubectl", "logs", pod_name, "-c", "ray-head", "--tail=1000"],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                return result.stdout

    return None


def parse_benchmark_results_from_logs(logs):
    """Parse benchmark results from job logs - look for JSON output."""
    if not logs:
        return None

    for line in logs.split("\n"):
        line = line.strip()
        if line.startswith("{") and "avg_execution_time_s" in line:
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue

    print("Warning: Could not parse benchmark results from logs")
    return None


def cleanup_cluster(cluster_name):
    """Delete RayJob and associated resources."""
    print(f"=== Cleaning up {cluster_name} ===")

    subprocess.run(
        ["kubectl", "delete", "rayjob", cluster_name, "--ignore-not-found=true"],
        capture_output=True,
        text=True
    )

    subprocess.run(
        ["kubectl", "delete", "configmap", f"benchmark-{CLUSTER_ID}", "--ignore-not-found=true"],
        capture_output=True,
        text=True
    )

    time.sleep(10)


def compare_results(ippr_results, baseline_results):
    """Compare IPPR vs baseline results."""
    comparison = {
        "ippr": {
            "avg_time_s": ippr_results.get("avg_execution_time_s") if ippr_results else None,
            "min_time_s": ippr_results.get("min_execution_time_s") if ippr_results else None,
            "max_time_s": ippr_results.get("max_execution_time_s") if ippr_results else None,
        },
        "baseline": {
            "avg_time_s": baseline_results.get("avg_execution_time_s") if baseline_results else None,
            "min_time_s": baseline_results.get("min_execution_time_s") if baseline_results else None,
            "max_time_s": baseline_results.get("max_execution_time_s") if baseline_results else None,
        }
    }

    if (ippr_results and baseline_results and
        ippr_results.get("avg_execution_time_s") and
        baseline_results.get("avg_execution_time_s")):
        ippr_avg = ippr_results["avg_execution_time_s"]
        baseline_avg = baseline_results["avg_execution_time_s"]
        improvement_pct = ((baseline_avg - ippr_avg) / baseline_avg) * 100
        comparison["improvement_pct"] = improvement_pct
        comparison["faster"] = "ippr" if improvement_pct > 0 else "baseline"

        print(f"\n=== Results Comparison ===")
        print(f"IPPR avg time: {ippr_avg:.2f}s")
        print(f"Baseline avg time: {baseline_avg:.2f}s")
        print(f"Improvement: {improvement_pct:.2f}%")

    return comparison


def main():
    """Main test execution - follows pattern from run_gcs_ft_on_k8s.py."""
    generate_cluster_id()

    ippr_results = None
    baseline_results = None
    exception = None

    try:
        print("=== Assuming KubeRay operator with IPPR support is already installed ===")
        time.sleep(2)

        # Test 1: IPPR-enabled cluster
        print("\n" + "="*60)
        print("STARTING IPPR TEST")
        print("="*60)
        try:
            deploy_cluster(IPPR_CLUSTER_NAME, "ippr")
            status = wait_for_rayjob_completion(IPPR_CLUSTER_NAME, timeout=1800)
            if status == "COMPLETE":
                logs = get_rayjob_logs(IPPR_CLUSTER_NAME)
                ippr_results = parse_benchmark_results_from_logs(logs)
                if ippr_results:
                    print(f"IPPR results: {ippr_results}")
        finally:
            cleanup_cluster(IPPR_CLUSTER_NAME)

        # Test 2: Baseline cluster
        print("\n" + "="*60)
        print("STARTING BASELINE TEST")
        print("="*60)
        try:
            deploy_cluster(BASELINE_CLUSTER_NAME, "baseline")
            status = wait_for_rayjob_completion(BASELINE_CLUSTER_NAME, timeout=1800)
            if status == "COMPLETE":
                logs = get_rayjob_logs(BASELINE_CLUSTER_NAME)
                baseline_results = parse_benchmark_results_from_logs(logs)
                if baseline_results:
                    print(f"Baseline results: {baseline_results}")
        finally:
            cleanup_cluster(BASELINE_CLUSTER_NAME)

    except Exception as e:
        print(f"Test failed with error: {e}")
        exception = e

    # Compare results and write output
    if ippr_results and baseline_results:
        comparison = compare_results(ippr_results, baseline_results)
    else:
        comparison = {
            "ippr": ippr_results or {},
            "baseline": baseline_results or {},
            "error": "One or both tests failed to complete"
        }

    test_output_json_path = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/release_test_output.json"
    )

    final_results = {
        "success": 1 if (ippr_results and baseline_results and not exception) else 0,
        "results": comparison
    }

    with open(test_output_json_path, "wt") as f:
        json.dump(final_results, f, indent=2)

    print(f"\n=== Results written to {test_output_json_path} ===")
    print(json.dumps(final_results, indent=2))

    if exception:
        raise exception


if __name__ == "__main__":
    main()
