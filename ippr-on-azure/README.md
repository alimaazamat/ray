# IPPR for Multi-Tenant KubeRay on AKS

## Scenario

An e-commerce company runs a multi-tenant ML platform on AKS where teams share one cluster:

- **Ray Serve**: CLIP (image search) and ResNet50 (product categorization) APIs running 24/7
- **Ray Train**: Fine-tuning jobs 3x/day on new product data
- **Ray Data**: Overnight batch inference pipelines

**Challenge**: Workloads have variable resource needs but static pod sizing forces choosing between waste or insufficient capacity

## The Problem: Static Pod Sizing

**Without IPPR**, pods are statically sized:

**Size too large** → Waste resources:
- Serving pod: Peak 18 CPU, off-peak 6 CPU → **Waste 12 CPU**
- Training pod: Varies 6-12 CPU by phase → **Waste up to 6 CPU**
- Batch pod: Varies 6-10 CPU by stage → **Waste up to 4 CPU**

**Size too small** → Failed workloads:
- Peak traffic throttles serving pods → High latency, failures
- Training/batch jobs OOM killed → Must retry

**Result**: Need 4-node cluster (32 vCPU) at **$1,110/month** but still can't run all workloads simultaneously
## The Solution: IPPR Enables Dynamic Right-Sizing

**With IPPR** (In-Place Pod Resize), pods can **size up and down** dynamically:

Serving pod (Ray Serve) adapts to traffic:

- Peak hours (8am-8pm):   18 CPU ← IPPR sized up
- Off-peak (8pm-8am):     10 CPU ← IPPR sized down

Training pod (Ray Train) adapts to phase:

- Data loading (10 min):   6 CPU ← IPPR sized for I/O
- Training (30 min):      12 CPU ← IPPR sized for compute
- Validation (5 min):      8 CPU ← IPPR sized down

Batch pod (Ray Data) adapts to pipeline stage:

- Read from storage (15 min):  6 CPU ← IPPR sized for I/O
- Inference (30 min):         10 CPU ← IPPR sized for compute
- Write results (15 min):      6 CPU ← IPPR sized for I/O

## Cluster Configuration

### Without IPPR (Static Sizing)

**AKS Cluster**:

- **SKU**: 4 × Standard_D8s_v3 nodes (8 vCPU, 32 GiB each)
- **Total**: 32 vCPU, 128 GiB
- **Cost**: $0.38/hour/node × 4 = $1,110/month

**Ray Clusters & Pods** (sized for peak):

1. **Multi-Model Serve Cluster** (CLIP + ResNet50)
   - 1 head pod: 2 CPU, 4 GiB
   - 2 worker pods: 8 CPU, 16 GiB each (static) = **16 CPU**
   - Total: **18 CPU, 36 GiB**

2. **Training Job Cluster**
   - 1 head pod: 2 CPU, 4 GiB
   - 1 worker pod: 10 CPU, 24 GiB (static, sized for training phase)
   - Total: **12 CPU, 28 GiB**

3. **Batch Inference Cluster**
   - 1 head pod: 2 CPU, 4 GiB
   - 1 worker pod: 8 CPU, 16 GiB (static, sized for inference phase)
   - Total: **10 CPU, 20 GiB**

**Peak if all run**: 18 + 12 + 10 = **40 CPU needed** (exceeds 32 vCPU capacity!)

**Problem**: Can't run all workloads simultaneously. Serving consumes 18 CPU 24/7, leaving only 14 CPU for training
(needs 12) OR batch (needs 10), but not both. Jobs must queue.

### With IPPR (Dynamic Sizing)

**AKS Cluster**:

- **SKU**: 3 × Standard_D8s_v3 nodes (8 vCPU, 32 GiB each)
- **Total**: 24 vCPU, 96 GiB
- **Cost**: $0.38/hour/node × 3 = $822/month

**Ray Clusters & Pods** (IPPR adjusts):

1. **Multi-Model Serve Cluster** (CLIP + ResNet50)
   - 1 head pod: 2 CPU, 4 GiB
   - 2 worker pods: 4-8 CPU, 8-16 GiB each (IPPR resizes)
   - Peak: **18 CPU, 36 GiB** | Off-peak: **10 CPU, 20 GiB** ← Frees 8 CPU!

2. **Training Job Cluster**
   - 1 head pod: 2 CPU, 4 GiB
   - 1 worker pod: 4-10 CPU, 8-24 GiB (IPPR resizes by phase)
   - Data loading: **6 CPU** | Training: **12 CPU** | Validation: **8 CPU**

3. **Batch Inference Cluster**
   - 1 head pod: 2 CPU, 4 GiB
   - 1 worker pod: 4-8 CPU, 8-16 GiB (IPPR resizes by phase)
   - I/O: **6 CPU** | Inference: **10 CPU** | I/O: **6 CPU**

**Peak hours**: 18 CPU serving + 6 CPU training (data phase) = **24 CPU**

**Off-peak**: 10 CPU serving + 12 CPU training = **22 CPU**

**Benefit**: IPPR enables smaller cluster (24 vCPU vs 32 vCPU) with higher utilization

## Cost Savings on AKS

## Workload Resource Details

**3 nodes × Standard_D8s_v3** (8 vCPU, 32 GiB each) = **24 vCPU total**
**Cost**: $0.38/hour/node × 3 = **$1.14/hour** = **$822/month**

### Workload Comparison

| Metric               | Without IPPR     | With IPPR        | Improvement          |
| -------------------- | ---------------- | ---------------- | -------------------- |
| **Infrastructure**   |                  |                  |                      |
| Nodes needed         | 4 nodes (32 CPU) | 3 nodes (24 CPU) | **-25% nodes**       |
| Monthly cost         | $1,110           | $822             | **-$288/month**      |
| **Utilization**      |                  |                  |                      |
| Avg cluster CPU use  | 55%              | 80%              | **+45% utilization** |
| Wasted CPU-hours/day | 192 CPU-hrs      | 64 CPU-hrs       | **-67% waste**       |
| **Throughput**       |                  |                  |                      |
| Training jobs/day    | 2-3 (queued)     | 5-6 (no queue)   | **+100% throughput** |
| Batch jobs/night     | 1                | 2-3              | **+200% throughput** |
| Queue time (avg)     | 2-4 hours        | 0 minutes        | **Eliminated**       |

