# Ray on Azure Kubernetes Service: Multi-Tenant ML Platform with In-Place Pod Resize

## The Scenario

A company runs a **multi-tenant Kubernetes cluster on AKS** for AI/ML workloads using **Ray with KubeRay**.

**Customer Use Case**:
An e-commerce company runs a shared ML platform where multiple teams share one AKS cluster. The **Search Team**
uses the CLIP model to power product search (customers type "red summer dress" and see matching products). The
**Catalog Team** uses ResNet50 to automatically categorize newly uploaded product images into departments. The
**ML Engineering Team** continuously fine-tunes both models using Ray Train on new product data, while the
**Analytics Team** runs overnight Ray Data pipelines to process thousands of images and generate embeddings for
recommendations. All teams compete for the same cluster resources.

- **Search Team**: Ray Serve running CLIP model for image-text search API
- **Catalog Team**: Ray Serve running ResNet50 for product classification API
- **ML Engineering Team**: Ray Train jobs fine-tuning models 3x/day
- **Analytics Team**: Ray Data batch inference pipelines running overnight

**Challenge**: Multiple teams share a cluster and compete for resources, how to efficiently utilize resources?

## What This Cluster Does

**Purpose**: AI-powered image analysis platform serving production APIs and running ML jobs

**Workloads**:

- **Serving (Ray Serve)**: CLIP and ResNet50 models provide 24/7 image search and classification APIs
- **Training (Ray Train)**: Fine-tune models 3x/day on new customer images
- **Batch Inference (Ray Data)**: Process uploaded images in bulk overnight

**Challenge**: All workloads share one cluster and compete for resources

## The Problem: Static Pod Sizing

**Without IPPR**, pods are statically sized:

### Size pods too big → **Waste resources**

If you size a pod for maximum utilization you end up underutilizing resources at off peak hours.

Serving pod (Ray Serve) sized for peak traffic:

- At peak using 18 CPU -> Off peak using 6 CPU
- Result: Waste 12 CPU

Training pod (Ray Train) sized for training phase:

- Data loading using 6 CPU -> Training is at peak using 12 CPU -> Validation using 8 CPU
- Result: Waste 6 CPU + 4 CPU

Batch pod (Ray Data) sized for inference phase:

- Read from storage using 6 CPU -> Inference is at peak using 10 CPU -> Write results using 6 CPU
- Result: Waste 4 CPU + 4 CPU

### Size pods too small → **Can't handle load**

Serving pod (Ray Serve) under-sized:

- Peak traffic arrives → Pod CPU throttled
- Result: High latency, request failures

Training pod (Ray Train) under-sized:

- Training phase → OOM killed
- Result: Job fails, must retry

Batch pod (Ray Data) under-sized:

- Inference phase → OOM killed
- Result: Job fails, must retry

### Over-provision cluster → **More Expensive**

Using a bigger cluster to fit all pods at peak size

- Serving (Ray Serve): 18 CPU (peak)
- Training (Ray Train): 12 CPU (peak)
- Batch (Ray Data): 10 CPU (peak)
- Total: 40 CPU needed
AKS cluster: 4 nodes × Standard_D8s_v3 = $1,110/month

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

## Directory

```text
ippr-demo/
├── setup/
│   └── install-kuberay.sh     # KubeRay operator setup
├── serving/
│   ├── clip-serve.yaml        # Ray Serve: CLIP model
│   └── resnet-serve.yaml      # Ray Serve: ResNet50
├── training/
│   ├── finetune-clip.yaml     # Ray Train: Fine-tuning
│   └── train_code.py          # Training script
└── batch/
    ├── batch-inference.yaml   # Ray Data: Batch pipeline
    └── inference_code.py      # Inference script
```

---

**Theme**: Running a multi-tenant ML platform on AKS requires flexible resource management. IPPR enables pods to
size up and down dynamically, allowing **better resource utilization** and **better job completion** across training,
serving, and batch workloads. This is the future of cost-efficient ML infrastructure on Kubernetes.
