# IPPR TPCH Benchmark Tests

Automated benchmark tests for In-Place Pod Resizing (IPPR) on Azure Kubernetes Service (AKS).

## Overview

Tests Ray's ability to leverage Kubernetes In-Place Pod Resizing (IPPR) for faster autoscaling. Compares IPPR-enabled clusters vs traditional pod addition/removal autoscaling using TPCH Q1 query workload.

**IPPR Benefits:**
- Faster scaling (no pod restart overhead)
- Preserved in-memory state during resize
- Reduced scheduling latency

## Test Components

- **`run_ippr_benchmark.py`**: Main test orchestration (follows `run_gcs_ft_on_k8s.py` pattern)
  - Creates ConfigMaps with embedded benchmark scripts
  - Deploys IPPR and baseline RayJobs
  - Waits for RayJob completion
  - Extracts results from logs and compares
- **`prepare_ippr.sh`**: AKS authentication, KubeRay operator verification, and CRD updates
- **`benchmark_runner.py`**: TPCH Q1 benchmark implementation
- **`rayjob_template.yaml`**: RayJob template with Python script embedding
- **`driver_compute.yaml`**: Driver VM config

## Prerequisites

### Required Infrastructure
- **Pre-existing AKS cluster**: `ray-ippr-cluster` in resource group `ray-ippr-rg`
- **Azure credentials** in AWS Secrets Manager: `oss-nightly-ci-aks-cluster-service-principal`
- **AKS cluster**: Kubernetes 1.33+ (IPPR enabled by default)

### Required KubeRay Operator
**CRITICAL**: You **MUST** use the custom KubeRay operator built from PR #3960 with IPPR support:
- Standard KubeRay releases don't support IPPR fields (`autoscalerOptions.version`, IPPR annotations)
- The operator must be deployed in the **`default` namespace** (not `ray-system`)
- Required operator image: `alimaa3amat/kuberay-operator:ippr-test`

### Required CRDs
**CRITICAL**: The RayCluster and RayJob CRDs must include the `version` field in `autoscalerOptions`:
- These updated CRDs are included when deploying the IPPR-enabled operator via `make deploy`
- The `prepare_ippr.sh` script will verify and update CRDs if needed

The `prepare_ippr.sh` script will:
1. Authenticate with Azure AKS
2. Check if the IPPR-enabled operator is running
3. Update the operator image if needed
4. Update CRDs with IPPR support (including the `version` field)
5. Verify CRDs support autoscaler v2

## Running the Tests

### Via Ray Release Test Framework
```bash
python release/ray_release/scripts/run_release_test.py --test k8s_ippr_benchmark
```

### Via Buildkite
- Tag PR with `release-test` label
- Or trigger manually through Buildkite UI

### Test Flow

1. Anyscale provisions driver VM (`m5.2xlarge`)
2. `prepare_ippr.sh` executes:
   - Authenticates with Azure AKS
   - Verifies/updates KubeRay operator to IPPR-enabled version
   - Updates CRDs with IPPR support (adds `version` field)
   - Validates CRDs support autoscaler v2
3. `run_ippr_benchmark.py` executes:
   - **IPPR Test**: Deploy RayJob with IPPR → Benchmark runs → Extract results → Cleanup
   - **Baseline Test**: Deploy RayJob without IPPR → Benchmark runs → Extract results → Cleanup
   - Compare results → Write to TEST_OUTPUT_JSON

## How It Works

### Script Embedding Pattern
Follows the same pattern as `run_gcs_ft_on_k8s.py`:

```python
# Embed benchmark script in YAML template
benchmark_script = "\n".join([
    f"    {line}"
    for line in pathlib.Path("./benchmark_runner.py").read_text().splitlines()
])

template = pathlib.Path("rayjob_template.yaml").read_text().format(
    benchmark_script=benchmark_script,
    cluster_name=cluster_name,
    mode=mode
)

# Deploy with kubectl
subprocess.run(["kubectl", "create", "-f", tmp_yaml])
```

### Immediate Execution
The benchmark runs **immediately** as the RayJob entrypoint:
```yaml
spec:
  entrypoint: python /home/ray/benchmark/benchmark.py
  shutdownAfterJobFinishes: true
```

This triggers autoscaling right away (unlike sleep-based approaches that caused 30+ minute delays).

### Scaling Strategies Compared
The test compares two autoscaling approaches when workload demands more resources than initially available:

- **IPPR mode (Vertical Scaling)**:
  - Workers start at 8 CPU/8Gi
  - Autoscaler resizes existing pods to 14 CPU/20Gi in-place
  - No pod restart, preserved in-memory state
  - Faster scaling with lower overhead

- **Baseline mode (Horizontal Scaling)**:
  - Workers start at 8 CPU/8Gi (same as IPPR)
  - Workers cannot resize (no resizePolicy)
  - Autoscaler must add more 8/8Gi worker pods to meet resource demands
  - More pod scheduling overhead, more pods to manage

This properly tests IPPR's benefit: **vertical scaling without restart** vs **traditional horizontal scaling**.

## Test Configuration

### Release Test Entry
```yaml
- name: k8s_ippr_benchmark
  group: k8s-test
  working_dir: k8s_tests/ippr
  frequency: manual
  team: core
  run:
    timeout: 7200
    prepare: bash prepare_ippr.sh
    script: python run_ippr_benchmark.py
```

### IPPR RayJob Config (`rayjob_template.yaml` with mode="ippr")
- **Type**: `RayJob` (required for IPPR field support)
- **Entrypoint**: `python /home/ray/benchmark/benchmark.py` (runs immediately!)
- **Workers start**: 8 CPU / 8Gi memory
- **Can resize to**: 14 CPU / 20Gi memory (no restart - vertical scaling in-place)
- **Ray autoscaler**: v2 (required for IPPR)
- **Resize policy**: `restartPolicy: NotRequired` on both head and worker containers
- **IPPR annotation**: Configures max resources per worker group (14 CPU / 20Gi)
- **minReplicas**: 0, **maxReplicas**: 3 (workers scale on demand when workload runs)
- **Scaling strategy**: Vertical (resize existing pods in-place)
- **Benchmark**: TPCH Q1 with SF=10, 1 run

### Baseline RayJob Config (`rayjob_template.yaml` with mode="baseline")
- **Type**: `RayJob` (for consistency)
- **Entrypoint**: `python /home/ray/benchmark/benchmark.py` (same as IPPR)
- **Workers start**: 8 CPU / 8Gi memory (same starting size as IPPR)
- **Workers cannot resize**: Fixed at 8 CPU / 8Gi (no resizePolicy)
- **minReplicas**: 0, **maxReplicas**: 3 (same as IPPR)
- **Scaling strategy**: Horizontal (add more pods to meet resource demands)
- **No IPPR** annotation or resize policy
- **Same Ray autoscaler v2** (for fair comparison)

**Key Difference**: When workload demands more resources:
- **IPPR**: Autoscaler resizes 3 workers from 8/8Gi → 14/20Gi in-place (no pod restart)
- **Baseline**: Autoscaler must add more 8/8Gi pods (more pods, more scheduling overhead)

## Output Format

Results written to `TEST_OUTPUT_JSON`:

```json
{
  "success": 1,
  "results": {
    "ippr": {
      "avg_time_s": 45.23,
      "min_time_s": 44.12,
      "max_time_s": 46.89
    },
    "baseline": {
      "avg_time_s": 48.56,
      "min_time_s": 47.32,
      "max_time_s": 49.87
    },
    "improvement_pct": 6.86,
    "faster": "ippr"
  }
}
```

## Architecture

```
┌───────────────────────────────┐
│ Buildkite / Release Framework │
└───────────┬───────────────────┘
            │
            ▼
┌───────────────────────────────┐
│ Driver VM (m5.2xlarge)        │
│  - prepare_ippr.sh            │
│  - run_ippr_benchmark_v3.py   │
└───────────┬───────────────────┘
            │ kubectl
            ▼
┌───────────────────────────────┐
│ AKS Cluster                   │
│  - KubeRay Operator (IPPR)    │
│  - IPPR RayJob (temp)         │
│  - Baseline RayJob (temp)     │
└───────────────────────────────┘
```

## Troubleshooting

### Authentication failures
- Verify secret exists: `oss-nightly-ci-aks-cluster-service-principal`
- Check secret contains: `client_id`, `client_secret`, `tenant_id`, `subscription_id`

### Cluster connection failures
- Verify AKS cluster exists and is running
- Check Kubernetes version: `kubectl version --short` (needs 1.33+)

### Workers not being created (benchmark stuck at "Parquet dataset sampling")
**This is the most common issue!** Symptoms:
- Benchmark runs but hangs at data loading phase for 30+ minutes
- Submitter pod logs show: `Parquet dataset sampling 0: 0%|...`
- No worker pods are created: `kubectl get pods -l ray.io/node-type=worker` returns empty

**Root Cause**: The RayJob controller is stripping out `autoscalerOptions.version: v2` when creating the RayCluster. This happens when:
- The RayCluster CRD doesn't support the `version` field in `autoscalerOptions`
- Even with the IPPR-enabled operator, if CRDs aren't updated, the field gets stripped

This causes:
- Autoscaler runs in v1 mode instead of v2
- Workers are never created
- IPPR doesn't work
- Benchmark hangs waiting for compute resources

**Solution**:
1. Ensure you're using the IPPR-enabled KubeRay operator:
   ```bash
   kubectl get deployment kuberay-operator -n default -o jsonpath='{.spec.template.spec.containers[0].image}'
   # Should return: alimaa3amat/kuberay-operator:ippr-test
   ```

2. **Update CRDs with IPPR support** (this is the critical step):
   ```bash
   # Apply updated CRDs from the IPPR-enabled KubeRay repo
   kubectl apply -f /path/to/kuberay/ray-operator/config/crd/bases/ray.io_rayclusters.yaml --server-side --force-conflicts
   kubectl apply -f /path/to/kuberay/ray-operator/config/crd/bases/ray.io_rayjobs.yaml --server-side --force-conflicts
   ```

   The `prepare_ippr.sh` script handles this automatically if the kuberay repo is at `/Users/alimaazamat/kuberay`.

3. If manually updating operator:
   ```bash
   kubectl set image deployment/kuberay-operator \
     kuberay-operator=alimaa3amat/kuberay-operator:ippr-test \
     -n default
   kubectl rollout status deployment/kuberay-operator -n default
   ```

4. Verify the created RayCluster has `version: v2`:
   ```bash
   kubectl get raycluster <name> -o yaml | grep -A 5 "autoscalerOptions:"
   # Should include: version: v2
   ```

### IPPR not working
- Check autoscaler logs for resize events:
  ```bash
  kubectl logs <head-pod> -c autoscaler | grep -i resize
  ```
- Verify K8s events: `kubectl get events | grep Resize`
- Ensure Ray autoscaler v2 is being used (see above)
- Verify worker pods have `resizePolicy` set:
  ```bash
  kubectl get pod <worker-pod> -o jsonpath='{.spec.containers[0].resizePolicy}'
  ```

### "unknown field spec.autoscalerOptions.version" error
- This means the CRD doesn't support autoscaler v2
- Verify CRD is from IPPR-enabled operator:
  ```bash
  kubectl get crd rayjobs.ray.io -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.rayClusterSpec.properties.autoscalerOptions.properties}' | jq 'keys'
  # Should include "version" in the list
  ```
- If not, deploy the IPPR-enabled operator

### KubeRay operator issues
- Verify IPPR-enabled operator is installed: `kubectl get deployment -n default kuberay-operator`
- Check operator logs: `kubectl logs -n default deployment/kuberay-operator`
- The operator must be from [KubeRay PR #3960](https://github.com/ray-project/kuberay/pull/3960) or later
- Update with: `bash prepare_ippr.sh` (script handles this automatically)

## Key Differences from Manual Setup

Your manual verification steps used:
```yaml
# ray-sample-job.yaml
entrypoint: python /home/ray/tpch/q1.py --sf 10  # Direct workload execution
```

The automated tests use the same approach:
```yaml
# rayjob_template.yaml
entrypoint: python /home/ray/benchmark/benchmark.py  # Direct workload execution
```

Both trigger autoscaling immediately when the RayJob starts, ensuring workers are created right away.

**Why the old approach failed:**
```yaml
# Old approach (WRONG)
entrypoint: python -c "import time; time.sleep(3600)"  # Dummy sleep
# Then: kubectl exec to inject workload later
# Result: Autoscaler never triggered, workers never created, 30+ min timeout
```

## References

- [Ray IPPR PR #55961](https://github.com/ray-project/ray/pull/55961)
- [KubeRay IPPR PR #3960](https://github.com/ray-project/kuberay/pull/3960)
- [Kubernetes In-Place Pod Resizing (KEP-1287)](https://kubernetes.io/docs/concepts/workloads/pods/pod-resize/)
- [KubeRay Documentation](https://docs.ray.io/en/latest/cluster/kubernetes/index.html)
