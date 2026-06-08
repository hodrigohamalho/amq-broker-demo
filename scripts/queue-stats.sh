#!/usr/bin/env bash
# Show stats for the demoQueue on both brokers in the cluster.
set -euo pipefail
NS=amq-demo
echo "=== Broker-0 (demo-broker-ss-0) ==="
oc exec -n $NS demo-broker-ss-0 -- /opt/amq/bin/artemis queue stat \
  --url tcp://demo-broker-all-0-svc:61616 --user admin --password admin 2>/dev/null \
  | grep -iE "NAME|demoQueue" || true
echo ""
echo "=== Broker-1 (demo-broker-ss-1) ==="
oc exec -n $NS demo-broker-ss-1 -- /opt/amq/bin/artemis queue stat \
  --url tcp://demo-broker-all-1-svc:61616 --user admin --password admin 2>/dev/null \
  | grep -iE "NAME|demoQueue" || true
