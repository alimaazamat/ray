# Vertical autoscaling for multi-tenant Ray on AKS with IPPR

Ray is one of the most widely adopted frameworks for distributed AI/ML workloads especially on AKS. As organizations scale their ML platforms, a common problem emerges where multiple teams sharing an AKS cluster for multitenant needs (serving, training, and batch inference) each have different resource needs that change throughout the day.

With [in-place pod resize](https://kubernetes.io/blog/2025/05/16/kubernetes-v1-33-in-place-pod-resize-beta/) now GA in Kubernetes 1.35, we are integrating vertical scaling capabilities into the Ray Autoscaler v2 for KubeRay on AKS. By scaling pods vertically before scaling horizontally, IPPR lets multi-tenant clusters establish a smaller starting footprint and lean on better bin-packing during peak contention — yielding marginal but compounding efficiency gains in environments where operational details make "perfect" sizing impossible to predict up front.

## The cost of static sizing on AKS

ML workloads on AKS have a well-known inefficiency: pods must be sized for peak demand at creation and hold those resources for their entire lifetime. On a typical multi-tenant cluster running Ray Serve, Ray Train, and Ray Data, this means a significant portion of reserved compute sits idle — workloads rarely sustain peak demand continuously, but their resource allocations do.

The challenge is that traditional approaches to improving utilization each carry tradeoffs that outweigh the potential savings:

- **Pod restarts** when resizing containers drop live connections on Ray Serve endpoints and lose in-progress training state.
- **Horizontal scaling** with new pods takes 30–60 seconds for scheduling, image pulls, and Ray cluster membership — too slow for real-time serving spikes.
- **Manual coordination** between teams to share capacity doesn't scale beyond a handful of workloads.
- **Adding more nodes** increases cost rather than reducing it.

These tradeoffs mean that improvements in utilization go unrealized — not because they are small, but because the path to achieving them has been too disruptive. In-place pod resize removes that barrier entirely.

## How IPPR improves multi-tenant Ray on AKS

In-place pod resize (IPPR) enables the Ray Autoscaler v2 to dynamically adjust a pod's CPU and memory allocation without restarting the container. This unlocks vertical scaling as a first-class capability alongside the existing horizontal scaling in Ray.

On multi-tenant AKS clusters, serving and training workloads have naturally complementary resource patterns. Serving demand peaks during business hours and drops overnight, while training and batch workloads fill the inverse pattern. IPPR lets the cluster exploit this relationship:

![CPU Allocation by Workload Over 24 Hours](cpu_allocation_chart.png)

Without IPPR, each workload must reserve its peak around the clock (Serving at 4, Training at 3, Batch at 3 giving a total of 10 CPU regardless of actual usage). With IPPR, resource allocation follows actual demand: Serving scales up during business hours while Training and Batch scale down, and overnight the pattern inverts. Because these workloads are naturally complementary, the cluster only needs enough capacity for the highest combined demand at any point in time. The shaded region shows the gap between what static sizing pays for and what the cluster actually needs.

The magnitude of that gap is workload- and operator-dependent — real clusters rarely have neatly complementary curves, and "perfect" sizing is hard to predict in advance. The benefit IPPR delivers is therefore best framed as a marginal aggregate improvement: smaller starting requests give the Kubernetes scheduler more room to bin-pack, peak contention is absorbed by in-place growth rather than provisioning headroom, and any horizontal scale-out that does happen starts from a tighter baseline. Across a fleet of multi-tenant Ray clusters, those small wins compound into meaningful AKS spend reductions without requiring operators to architect for the perfect demand curve up front.

### Faster task scheduling through vertical scale-up

When the Ray Autoscaler detects pending tasks that cannot be placed on existing capacity, it can resize IPPR-enabled pods in seconds — significantly faster than the minutes required to provision a new AKS node. This is particularly impactful for serving workloads where traffic spikes are immediate and latency-sensitive.

### Improved bin-packing and resource utilization

The core mechanic is straightforward: different tenants sharing a node size up and down at different times. Serving's reservation can shrink overnight so Training's reservation can grow into the same node, and the pattern inverts during business hours. Because the Kubernetes scheduler bin-packs against *current* requests rather than a pod's maximum, IPPR makes that time-varying slack visible — slack that static sizing keeps permanently reserved and hidden behind worst-case requests.

In practice, real workload curves aren't perfectly complementary and operators can't predict the "right" peak per tenant up front. So the gain from IPPR is best understood as a marginal aggregate improvement: each node holds a little less unused reservation, each scale-out is delayed a little longer, and each new pod lands on tighter starting requests. Per cluster the win is incremental; across a multi-tenant AKS fleet it compounds into real spend reduction without forcing anyone to architect for a demand pattern they can't actually predict.

### Zero-disruption resizing for stateful workloads

Unlike pod restarts or rolling updates, IPPR resizes containers without interrupting running processes. For Ray workloads, this means:

- **Ray Serve**: No dropped connections during resize. Inference endpoints remain available throughout scaling events.
- **Ray Train**: No lost training progress. Models mid-epoch retain their checkpoints and continue without interruption.
- **Ray Data**: No broken pipeline stages. Batch jobs reading from Azure Blob Storage maintain their I/O state.

### Smarter capacity planning

When new worker pods are needed for IPPR-enabled groups, the autoscaler considers their maximum capacity during bin-packing decisions. A single new AKS node can host a pod that starts small and grows into its allocation rather than reserving peak demand up front. Whether this translates into a measurably lower node count depends on the workload mix — but in the aggregate it consistently shifts the cluster toward fewer, better-utilized nodes.

## Looking Ahead

By enabling the Ray Autoscaler v2 to vertically scale pods without restarting containers, IPPR lets serving, training, and batch workloads share cluster resources dynamically by starting small and growing only when demand requires it, and freeing capacity when ephemeral jobs complete. The result is a smaller starting footprint, better bin-packing during peak contention, and zero disruption to running workloads — marginal per-cluster improvements that compound into meaningful efficiency gains across a multi-tenant AKS fleet.

As IPPR in KubeRay matures with capabilities like proactive downsizing (automatically shrinking pods when demand drops) and gradual resizing (stepping through intermediate allocations rather than jumping to max), new feature work will continue to unlock new ways for Kubernetes to deliver more efficient infrastructure for your AI/ML needs.

To learn more about Ray on Kubernetes, see the [KubeRay documentation](https://docs.ray.io/en/latest/cluster/kubernetes/index.html). To follow the IPPR implementation, see [PR #55961](https://github.com/ray-project/ray/pull/55961).

## DEMO STEPS
```bash
kubectl delete raycluster ippr-multitenant-demo
pkill -f "port-forward.*8265"
kubectl apply -f demo/raycluster-ippr.yaml

# port-forward to the head pod:
HEAD_POD=$(kubectl get pods -l ray.io/cluster=ippr-multitenant-demo,ray.io/node-type=head -o name | sed 's|pod/||')
kubectl port-forward pod/$HEAD_POD 8265:8265 &

# submit all 3 workloads:
ray job submit --address http://localhost:8265 --no-wait --working-dir . -- python workload_serve.py
ray job submit --address http://localhost:8265 --no-wait --working-dir . -- python workload_train.py
ray job submit --address http://localhost:8265 --no-wait --working-dir . -- python workload_batch.py

# watch pods resize in-place:
kubectl get pods -l ray.io/cluster=ippr-multitenant-demo \
  -o custom-columns='NAME:.metadata.name,CPU_REQ:.spec.containers[0].resources.requests.cpu,CPU_LIM:.spec.containers[0].resources.limits.cpu' \
  -w

# results
ippr-multitenant-demo-head-wqlzw                   500m         1
ippr-multitenant-demo-serving-workers-worker-dcjwx    2         2
ippr-multitenant-demo-training-workers-worker-6kr2h   1         1
ippr-multitenant-demo-batch-workers-worker-5wbfg      1         1

ippr-multitenant-demo-serving-workers-worker-dcjwx    4         4
ippr-multitenant-demo-training-workers-worker-6kr2h   3         3
ippr-multitenant-demo-batch-workers-worker-5wbfg      3         3
```

## DEMO RESULTS

| Worker    | IPPR Start  | IPPR Resized To | Static (no IPPR) |
| --------- | ----------- | --------------- | ---------------- |
| serving   | 2 CPU       | **4 CPU**       | 4 CPU (fixed)    |
| training  | 1 CPU       | **3 CPU**       | 3 CPU (fixed)    |
| batch     | 1 CPU       | **3 CPU**       | 3 CPU (fixed)    |
| head      | 0.5 CPU     | —               | 0.5 CPU          |
| **Total** | **4.5 CPU** | **10.5 CPU**    | **10.5 CPU**     |

| Metric               | Without IPPR (static) | With IPPR                    |
| -------------------- | --------------------- | ---------------------------- |
| CPU reserved at idle | 10.5 CPU              | 4.5 CPU                      |
| Number of Nodes      | 2 (24/7)              | 1 (scales to 2 only at peak) |
| **Node saved**       |                       | **1 node**                   |
