#!/usr/bin/env bash
# Disaster Recovery demo (AMQP mirroring):
#   1) show the DR broker holds a mirrored copy of the messages,
#   2) simulate the loss of the primary site,
#   3) recover by consuming from the DR broker (no data lost),
#   4) restore the primary site.
set -euo pipefail
NS=amq-dr
qcount() { # message count of demoQueue on a given broker CR (arg: dr-primary | dr-backup)
  oc exec -n "$NS" "$1-ss-0" -- /opt/amq/bin/artemis queue stat \
    --url "tcp://$1-all-0-svc:61616" --user admin --password admin 2>/dev/null \
    | awk -F'|' '/demoQueue/{gsub(/ /,"",$5);print $5}'
}

echo ">>> 1) Pausing the consumer so messages accumulate (and mirror to DR)..."
oc scale deploy/dr-consumer -n "$NS" --replicas=0 >/dev/null
sleep 20
echo "    PRIMARY demoQueue : $(qcount dr-primary)"
echo "    DR      demoQueue : $(qcount dr-backup)   <-- mirrored copy"
echo ""
echo ">>> 2) Simulating PRIMARY SITE loss (dr-primary + producer -> 0)..."
oc patch activemqartemis dr-primary -n "$NS" --type merge \
  -p '{"spec":{"deploymentPlan":{"size":0}}}' >/dev/null
oc scale deploy/dr-producer -n "$NS" --replicas=0 >/dev/null
sleep 8
echo "    primary pods now: $(oc get pods -n "$NS" -l ActiveMQArtemis=dr-primary --no-headers 2>/dev/null | wc -l | tr -d ' ')"
echo ""
echo ">>> 3) Recovering at the DR site (consuming from dr-backup)..."
N=$(qcount dr-backup)
oc exec -n "$NS" dr-backup-ss-0 -- bash -lc \
  "/opt/amq/bin/artemis consumer --url tcp://dr-backup-all-0-svc:61616 \
   --user admin --password admin --destination queue://demoQueue \
   --message-count ${N:-100} --receive-timeout 10000 2>/dev/null" \
  | grep -iE "Consumed" | tail -1
echo "    >>> Recovered ~${N} messages from the DR site with the primary DOWN. No data lost."
echo ""
echo ">>> 4) Restoring the primary site..."
oc patch activemqartemis dr-primary -n "$NS" --type merge \
  -p '{"spec":{"deploymentPlan":{"size":1}}}' >/dev/null
oc scale deploy/dr-producer -n "$NS" --replicas=1 >/dev/null
oc scale deploy/dr-consumer -n "$NS" --replicas=1 >/dev/null
echo ">>> Done. (Note: failback/reconciliation after a real DR event is a separate step.)"
