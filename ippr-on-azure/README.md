# Vertical autoscaling for multi-tenant Ray on AKS with IPPR

Ray is one of the most widely adopted frameworks for distributed AI/ML workloads, and AKS is a natural home for production Ray clusters. As organizations scale their ML platforms, a common pattern emerges: multiple teams sharing AKS infrastructure for serving, training, and batch inference — each with different resource needs that change throughout the day.

With [in-place pod resize](https://kubernetes.io/blog/2025/05/16/kubernetes-v1-33-in-place-pod-resize-beta/) now GA in Kubernetes 1.35, we are integrating vertical scaling capabilities into the Ray Autoscaler v2 for KubeRay on AKS. By scaling pods vertically before scaling horizontally, IPPR enables multi-tenant clusters to run the same workloads on fewer nodes with higher utilization.

## The cost of static sizing on AKS

ML workloads on AKS have a well-known inefficiency: pods must be sized for peak demand at creation and hold those resources for their entire lifetime. On a typical multi-tenant cluster running Ray Serve, Ray Train, and Ray Data, this means a significant portion of reserved compute sits idle — workloads rarely sustain peak demand continuously, but their resource allocations do.

The challenge is that traditional approaches to improving utilization each carry tradeoffs that outweigh the potential savings:

- **Pod restarts** to resize containers drop live connections on Ray Serve endpoints and lose in-progress training state.
- **Horizontal scaling** with new pods takes 30–60 seconds for scheduling, image pulls, and Ray cluster membership — too slow for real-time serving spikes.
- **Manual coordination** between teams to share capacity doesn't scale beyond a handful of workloads.
- **Adding more nodes** increases cost rather than reducing it.

These tradeoffs mean that improvements in utilization go unrealized — not because they are small, but because the path to achieving them has been too disruptive. In-place pod resize removes that barrier entirely.

## How IPPR improves multi-tenant Ray on AKS

In-place pod resize (IPPR) enables the Ray Autoscaler v2 to dynamically adjust a pod's CPU and memory allocation without restarting the container. This unlocks vertical scaling as a first-class capability alongside the existing horizontal scaling in Ray.

On multi-tenant AKS clusters, serving and training workloads have naturally complementary resource patterns. Serving demand peaks during business hours and drops overnight, while training and batch workloads fill the inverse pattern. IPPR lets the cluster exploit this relationship:

```
  CPU Committed Over 24 Hours
  █ IPPR (actual demand)   ░ Saved vs static sizing (34 CPU)
         ┌──────────────────────────────────┐ ← 34 CPU (static)
  12 AM  │██████████████░░░░░░░░░░░░░░░░░░░░│  14 CPU      20 saved
   2 AM  │██████████████░░░░░░░░░░░░░░░░░░░░│  14 CPU      20 saved
   4 AM  │████████████░░░░░░░░░░░░░░░░░░░░░░│  12 CPU      22 saved
   6 AM  │██████████░░░░░░░░░░░░░░░░░░░░░░░░│  10 CPU      24 saved
   8 AM  │██████████████████░░░░░░░░░░░░░░░░│  18 CPU      16 saved
  10 AM  │████████████████████████░░░░░░░░░░│  24 CPU      10 saved
  12 PM  │████████████████████░░░░░░░░░░░░░░│  20 CPU      14 saved
   2 PM  │██████████████████████░░░░░░░░░░░░│  22 CPU      12 saved
   4 PM  │████████████████████░░░░░░░░░░░░░░│  20 CPU      14 saved
   6 PM  │████████████████████░░░░░░░░░░░░░░│  20 CPU      14 saved
   8 PM  │██████████████░░░░░░░░░░░░░░░░░░░░│  14 CPU      20 saved
  10 PM  │██████████████░░░░░░░░░░░░░░░░░░░░│  14 CPU      20 saved
         └──────────────────────────────────┘
                              Avg: 17 CPU saved per hour (50% reduction)
```

Without IPPR, every workload reserves its peak allocation around the clock — 16 + 10 + 8 = 34 CPU committed continuously, requiring 5 nodes. With IPPR, actual allocation averages 17 CPU and peaks at 24, fitting on 3 nodes. The ░ region above represents the CPU that static sizing pays for but never uses — an average of **17 CPU wasted every hour of every day**.

### Faster task scheduling through vertical scale-up

When the Ray Autoscaler detects pending tasks that cannot be placed on existing capacity, it can resize IPPR-enabled pods in seconds — significantly faster than the minutes required to provision a new AKS node. This is particularly impactful for serving workloads where traffic spikes are immediate and latency-sensitive.

### Improved bin-packing and resource utilization

IPPR-enabled pods start with smaller resource requests, allowing the Kubernetes scheduler to bin-pack more pods onto existing nodes. As workload demand grows, pods scale up in-place using available node capacity. This reduces fragmentation and improves overall node utilization compared to static sizing.

### Zero-disruption resizing for stateful workloads

Unlike pod restarts or rolling updates, IPPR resizes containers without interrupting running processes. For Ray workloads, this means:

- **Ray Serve**: No dropped HTTP connections during resize. Inference endpoints remain available throughout scaling events.
- **Ray Train**: No lost training progress. Models mid-epoch retain their checkpoints and continue without interruption.
- **Ray Data**: No broken pipeline stages. Batch jobs reading from Azure Blob Storage maintain their I/O state.

### Reduced node count through smarter capacity planning

When new worker pods are needed for IPPR-enabled groups, the autoscaler considers their maximum capacity during bin-packing decisions. A single new AKS node can host a pod that starts small and grows into its allocation, rather than requiring a node large enough for peak demand from the start.

## How this translates to AKS cost

To illustrate the impact, consider a multi-tenant AKS cluster running concurrent Ray Serve, Ray Train, and Ray Data workloads on Standard_D8s_v3 nodes (8 vCPU, 32 GiB each). With static sizing, each workload must reserve its peak CPU allocation at all times — even when actual demand is well below peak. With IPPR, pods start at their baseline and grow only when the autoscaler detects pending tasks that need more capacity.

In this type of scenario, the difference is straightforward: fewer nodes are needed because the cluster is no longer reserving peak capacity for every workload simultaneously. The Azure compute cost scales directly with the number of nodes, so any reduction in node count translates to proportional savings.

The actual savings depend on workload profiles, traffic patterns, and cluster configuration — but the structural advantage is clear: IPPR lets resource allocation track demand rather than worst-case planning.

## Looking ahead

We are continuing to expand IPPR capabilities for Ray on AKS. Future work includes proactive downsizing — automatically contracting pod resources when demand decreases — and GPU resource support. Together with AKS node autoscaling and the Ray Autoscaler v2, IPPR is a step toward Ray working seamlessly with Kubernetes as the infrastructure layer for distributed AI/ML.

To learn more about Ray on Kubernetes, see the [KubeRay documentation](https://docs.ray.io/en/latest/cluster/kubernetes/index.html). To follow the IPPR implementation, see [PR #55961](https://github.com/ray-project/ray/pull/55961).

