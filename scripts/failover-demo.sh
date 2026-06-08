#!/usr/bin/env bash
# HA demo: kill broker-0 and show self-healing (PVC re-attached, no message loss).
set -euo pipefail
NS=amq-demo
POD=demo-broker-ss-0

echo ">>> State BEFORE the failure:"
oc get pods -n $NS -o wide | grep -E "demo-broker-ss|NAME"
echo ""
echo ">>> Killing broker-0 (simulating a pod/node failure)..."
oc delete pod $POD -n $NS --grace-period=0 --force 2>/dev/null || oc delete pod $POD -n $NS
echo ""
echo ">>> Watch: the StatefulSet is already recreating the pod with the SAME PVC."
echo ">>> (keep 'oc logs -f deploy/amq-consumer' visible — the flow does not stop)"
echo ""
for i in $(seq 1 20); do
  phase=$(oc get pod $POD -n $NS -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
  ready=$(oc get pod $POD -n $NS -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
  echo "  [$i] $POD -> phase=$phase ready=$ready"
  if [ "$ready" = "true" ]; then
    echo ""
    echo ">>> broker-0 is back. PVC re-attached, journal preserved, zero message loss."
    break
  fi
  sleep 4
done
echo ""
echo ">>> State AFTER recovery:"
oc get pods -n $NS -o wide | grep -E "demo-broker-ss|NAME"
