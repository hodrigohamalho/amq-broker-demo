#!/usr/bin/env bash
# Remove the demo resources. Pass --all to also remove the operator and namespace.
set -euo pipefail
NS=amq-demo
echo ">>> Removing apps (queue + topic), queue and broker..."
oc delete -f manifests/05-topic-apps.yaml -n $NS --ignore-not-found
oc delete -f manifests/04-apps.yaml -n $NS --ignore-not-found
oc delete -f manifests/03-address-queue.yaml -n $NS --ignore-not-found
oc delete -f manifests/02-broker-ha.yaml -n $NS --ignore-not-found

if [ "${1:-}" = "--all" ]; then
  echo ">>> Removing operator and namespace..."
  oc delete -f manifests/01-operator.yaml -n $NS --ignore-not-found
  oc delete csv -n $NS -l operators.coreos.com/amq-broker-rhel9.amq-demo --ignore-not-found 2>/dev/null || true
  oc delete namespace $NS --ignore-not-found
fi
echo ">>> Done."
