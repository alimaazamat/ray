#!/bin/bash
#
# Prepare script for IPPR benchmark tests on AKS.
#
# This script:
# 1. Authenticates with Azure AKS
# 2. Verifies/updates KubeRay operator to use IPPR-enabled image
# 3. Waits for operator to be ready
#
# Prerequisites:
# - AKS cluster exists: ray-ippr-cluster in resource group ray-ippr-rg
# - Azure credentials in AWS Secrets Manager: oss-nightly-ci-aks-cluster-service-principal
# - Custom KubeRay operator with IPPR support (from PR #3960) must be deployed

set -e

echo "=== IPPR Benchmark Test Preparation ==="
echo "======================================="

# Azure credentials from AWS Secrets Manager
echo "Fetching Azure credentials from AWS Secrets Manager..."
SECRET_JSON=$(aws secretsmanager get-secret-value \
    --secret-id oss-nightly-ci-aks-cluster-service-principal \
    --query SecretString \
    --output text)

CLIENT_ID=$(echo "$SECRET_JSON" | jq -r '.client_id')
CLIENT_SECRET=$(echo "$SECRET_JSON" | jq -r '.client_secret')
TENANT_ID=$(echo "$SECRET_JSON" | jq -r '.tenant_id')
SUBSCRIPTION_ID=$(echo "$SECRET_JSON" | jq -r '.subscription_id')

echo "✓ Azure credentials retrieved"

# Authenticate with Azure
echo "Authenticating with Azure..."
az login --service-principal \
    --username "$CLIENT_ID" \
    --password "$CLIENT_SECRET" \
    --tenant "$TENANT_ID" > /dev/null

az account set --subscription "$SUBSCRIPTION_ID"
echo "✓ Authenticated with Azure"

# Get AKS credentials
echo "Getting AKS credentials..."
RESOURCE_GROUP="ray-ippr-rg"
CLUSTER_NAME="ray-ippr-cluster"

az aks get-credentials \
    --resource-group "$RESOURCE_GROUP" \
    --name "$CLUSTER_NAME" \
    --overwrite-existing

echo "✓ AKS credentials configured"

# Verify cluster connection
echo "Verifying cluster connection..."
kubectl cluster-info > /dev/null
echo "✓ Connected to AKS cluster"

# Check Kubernetes version (needs 1.33+ for IPPR)
K8S_VERSION=$(kubectl version --short 2>&1 | grep "Server Version" | awk '{print $3}' | cut -d'v' -f2)
echo "Kubernetes version: v$K8S_VERSION"

# Verify/Update KubeRay operator
echo "Checking KubeRay operator..."

# Check if operator exists
if kubectl get deployment kuberay-operator -n default &> /dev/null; then
    CURRENT_IMAGE=$(kubectl get deployment kuberay-operator -n default -o jsonpath='{.spec.template.spec.containers[0].image}')
    echo "Current KubeRay operator image: $CURRENT_IMAGE"

    # Check if it's the IPPR-enabled image
    if [[ "$CURRENT_IMAGE" == *"alimaa3amat/kuberay-operator:ippr-test"* ]]; then
        echo "✓ IPPR-enabled KubeRay operator is already running"
    else
        echo "⚠ Operator is running but NOT the IPPR-enabled version"
        echo "Updating to IPPR-enabled operator..."

        kubectl set image deployment/kuberay-operator \
            kuberay-operator=alimaa3amat/kuberay-operator:ippr-test \
            -n default

        # Wait for rollout
        kubectl rollout status deployment/kuberay-operator -n default --timeout=300s
        echo "✓ Updated to IPPR-enabled operator"
    fi
else
    echo "✗ KubeRay operator not found in default namespace"
    echo ""
    echo "Please deploy the IPPR-enabled KubeRay operator first:"
    echo "  kubectl set image deployment/kuberay-operator \\"
    echo "    kuberay-operator=alimaa3amat/kuberay-operator:ippr-test \\"
    echo "    -n default"
    exit 1
fi

# Verify operator is running
echo "Verifying operator is ready..."
kubectl wait --for=condition=available --timeout=300s \
    deployment/kuberay-operator -n default

echo "✓ KubeRay operator is ready"

# Update CRDs with IPPR support
echo "Updating KubeRay CRDs with IPPR support..."

# Check if we have the kuberay repo with updated CRDs
if [ -d "/Users/alimaazamat/kuberay/ray-operator/config/crd/bases" ]; then
    echo "Applying updated RayCluster CRD..."
    kubectl apply -f /Users/alimaazamat/kuberay/ray-operator/config/crd/bases/ray.io_rayclusters.yaml --server-side --force-conflicts

    echo "Applying updated RayJob CRD..."
    kubectl apply -f /Users/alimaazamat/kuberay/ray-operator/config/crd/bases/ray.io_rayjobs.yaml --server-side --force-conflicts
else
    echo "⚠ Warning: KubeRay CRD files not found at expected location"
    echo "Assuming CRDs are already updated"
fi

# Verify CRDs support IPPR fields
echo "Verifying RayCluster CRD supports IPPR..."
if kubectl get crd rayclusters.ray.io -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.autoscalerOptions.properties.version}' &> /dev/null; then
    echo "✓ RayCluster CRD supports autoscaler v2"
else
    echo "✗ RayCluster CRD does not support autoscaler v2"
    echo "Please ensure CRDs are from KubeRay PR #3960 or later"
    exit 1
fi

echo "Verifying RayJob CRD supports IPPR..."
if kubectl get crd rayjobs.ray.io -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.rayClusterSpec.properties.autoscalerOptions.properties.version}' &> /dev/null; then
    echo "✓ RayJob CRD supports autoscaler v2"
else
    echo "✗ RayJob CRD does not support autoscaler v2"
    echo "Please ensure CRDs are from KubeRay PR #3960 or later"
    exit 1
fi

echo ""
echo "=== Preparation Complete ==="
echo "✓ Azure authentication configured"
echo "✓ AKS credentials configured"
echo "✓ Kubernetes cluster connected (v$K8S_VERSION)"
echo "✓ IPPR-enabled KubeRay operator running"
echo "✓ CRDs support IPPR fields"
echo ""
echo "Ready to run IPPR benchmarks!"
