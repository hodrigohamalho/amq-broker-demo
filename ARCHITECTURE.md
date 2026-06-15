# AMQ Broker — HA & DR Architecture Guide

A decision guide for **high availability** and **disaster recovery** with Red Hat
AMQ Broker (ActiveMQ Artemis) on OpenShift. Written to settle the common
"let's do active/active replication" debate with precise terminology and trade-offs.

> Scope note: this guide assumes **own-PV persistence** (each broker writes its own
> journal to a PVC). Shared-store HA is intentionally out of scope here.

---

## 1. The terminology trap: what "replication" actually means

The phrase "active/active replication" conflates mechanisms that Artemis keeps
**separate**. There are **three** of them — and "replication" gets used loosely for
two, which is where the confusion starts:

| Mechanism | What it is | Do brokers serve clients? | Same message copied? | Consistency |
|---|---|---|---|---|
| **HA Replication** | Primary/backup pair | **Only the primary** (backup passive) | Yes, **synchronous** | Strong, exactly-once |
| **Clustering** | N independent brokers linked by cluster connections | **All** (active/active) | **No** — each message lives on **one** broker | Strong (single owner) |
| **Mirroring** (broker connections) | Async copy to another broker/site | **All sites** (active/active possible) | Yes, **asynchronous** | **Eventual, at-least-once** |

**Why you can't have it _synchronously_ for one queue:** an `anycast` queue delivers
each message to **exactly one** consumer. To keep that guarantee while **two active**
brokers hold the same queue, every delivery and ack would need **per-message
distributed consensus** (a quorum round-trip per message) — which destroys throughput.
So no broker offers **synchronous, exactly-once active/active** replication of the
same queue. Kafka sidesteps it with **partitions** (a single leader per partition),
not by sync-replicating a queue to two active nodes.

You **can** do **asynchronous** active/active replication of the same queue — that is
exactly what **Mirroring** does (see §4) — but only by **giving up the strong
guarantee** (eventual consistency, at-least-once, possible duplicates). That async
trade-off is *why* it needs no per-message consensus.

> **One-liner for the architect:** *Artemis **HA replication** is synchronous and
> always active/passive. Active/active comes from **clustering** (each message owned by
> one broker) or from **mirroring** (an async, eventually-consistent copy — §4). What
> does **not** exist is **synchronous, exactly-once** replication of one queue across
> multiple active brokers.*

---

## 2. The real options

| Topology | Active brokers | Message resilience | Failover | Split-brain | Cost | Use when |
|---|---|---|---|---|---|---|
| **Replication HA** | 1 (+1 passive) | Sync replica on its own disk | Backup promoted | **Needs quorum** (ZooKeeper / ≥3 nodes) | High | Sub-second failover without shared storage |
| **Clustered (active/active)** | N | ❌ message stays on its broker | None for the message (broker must return) | N/A | Low | Scale + throughput |
| **Cluster + replicated backup per node** ⭐ | N (+N passive) | Replica per node | Each node has its own backup | Quorum per pair | **High** (2× brokers) | True "active/active + resilient" |
| **Dual mirror (active/active, 2 sites)** | 1+ per site | Async copy on the other site | Manual / client repoint | N/A (async) | Medium | Cross-site active/active & DR |

⭐ (a cluster of live brokers, **each with its own replicated backup**) — i.e. active/active at the
*service* level, active/passive *per node*. That's the recommended on-prem topology.

---

## 3. On OpenShift, the platform already gives you node-failure recovery

This is the strongest counter to "force replication" on OpenShift:

- A broker is a **StatefulSet with a PVC**. If the pod/node dies, OpenShift
  **reschedules the pod with the same name and re-attaches the same PV** — persisted
  messages come back. (No Artemis replication required for node failure.)
- **Clustering** (`size: N`) provides active/active scale and load balancing.
- **`messageMigration`** drains a broker that scales down to the survivors.

So Artemis **replication is often redundant on OpenShift** with what Kubernetes
already does. The Operator doesn't expose replication
as a first-class field; the native path is **PVC + cluster**.

> *Which requirement does replication meet that PVC + reschedule
> does not?* The only legitimate answer is usually **sub-second RTO** (failover without
> waiting ~10–30s for the pod to reschedule). If that SLA isn't real, replication is
> complexity without payoff.

---

## 4. Active/active across two sites — bidirectional (dual) mirror

When the goal is **two sites both active** (the "Kafka MirrorMaker 2 active/active"
ask), AMQ Broker supports it via **dual mirror**: a mirror broker-connection on
**each** broker, replicating sends **and** acknowledgements both ways.

> This **is** the "async active/active replication of the same queue" that §1 said is
> possible. It works precisely because it is **asynchronous and at-least-once** — so it
> needs no per-message consensus. The Artemis features below (loop prevention, dedup)
> make the two copies *converge*; they do **not** turn it into synchronous exactly-once.

```
SITE 1  ──  dual AMQP mirror  ──  SITE 2
 (own PV)   ⇄ messages + acks ⇄    (own PV)
```

- **Loop prevention is automatic** (events that arrived via a mirror are not
  re-mirrored), and the broker runs **duplicate detection** during mirroring, so the
  two queues converge instead of looping forever.
- It uses **only own-PV persistence** — the mirror is async event replication over
  the network, not shared storage.

### The property the architect must accept (same as MM2)

The mirror is **asynchronous → eventually consistent**. With consumers active on
**both** sites on the same queue, there is a **double-delivery window** (a message
produced on Site 1 is mirrored to Site 2 before Site 1's ack propagates). It is
**at-least-once, not global exactly-once**, and there is **no global ordering**. This
is physics, not an AMQ limitation — **MM2 active/active has the exact same property**.

### AMQ dual mirror vs Kafka MM2 (active/active)

| | Kafka MM2 | AMQ dual mirror |
|---|---|---|
| Replicates | topic log + offsets | queue state (**messages + acks**) |
| Loop prevention | topic prefixing / policy | internal annotations (automatic) |
| Dedup | app / consumer-side | **broker-side duplicate detection** + app |
| Resource name on both sites | prefixed (`siteA.topic`) | **same** queue name |
| Consistency | async, eventual | async, eventual |
| Double-processing risk | yes | yes |
| Global exactly-once | ❌ | ❌ |
| Ack propagated (removes on the other side) | no | **yes** |

The last row is an AMQ advantage for queue semantics: a consumed message is removed
on the other site (eventually) because the **ack is mirrored** — MM2 does not do this.

### How to make it safe

- ✅ **Use it for:** availability / mutual DR / locality — each site primarily serves
  its own traffic; if a site dies, the other already holds a converged copy.
- ❌ **Don't use it as:** a single global exactly-once queue with consumers racing on
  both sites.
- **Mitigations when consuming on both sides:** idempotent consumers using a stable
  message ID, the broker's duplicate detection (`_AMQ_DUPL_ID`), and partitioning work
  by site to avoid cross-site races.

---

## 5. Client failover & external load balancing

Two worlds, and the LB choice depends on the client:

### a) Core / JMS clients (Artemis-native) → failover **in the client** (preferred)
Give the client the connector list and topology:
```
(tcp://broker-0:61616,tcp://broker-1:61616)?ha=true&reconnectAttempts=3&retryInterval=1000
```
- `ha=true` → the client learns the topology (incl. the backup) and fails over itself.
- `reconnectAttempts` **finite** (not `-1`) → it doesn't get stuck on the dead node.
- **No smart LB needed** — failover is the client's job. Most robust option.

### b) AMQP / MQTT / STOMP clients (no native failover) → **external LB / Service**
- **k8s Service + readiness gating:** in a primary/backup pair only the **active**
  broker is "ready", so a Service routes only to it; on failover the endpoints update
  automatically. Transparent failover with no client change.
- Or **multi-host in the client library's URI** (e.g. Qpid JMS
  `failover:(amqp://b0,amqp://b1)`).
- L4 (NLB / `Service type LoadBalancer`) beats L7 (Route) for messaging; AMQP over a
  Route needs **passthrough TLS + SNI**.

> ⚠️ **Classic trap:** a dumb round-robin LB in front of a primary/backup pair will
> hit the **passive** backup. Health-checked routing (readiness) is **mandatory** so
> the LB only targets the active broker. For a clustered (all-active) set the LB may
> spread across all — but watch consumer affinity and message locality.

**Rules of thumb**
- Core/JMS client → client-side failover list, no LB.
- AMQP/MQTT client → Service/LB with health-check to the active, or multi-host URI.
- Cross-site DR → the client/LB needs **both sites' endpoints**; the mirror provides
  the data, the client/LB performs the switch.

---

## 6. How this maps to the demo scenarios in this repo

| This guide | Repo scenario |
|---|---|
| Clustered active/active (PV) + client failover | **Scenario A** (`manifests/`) |
| Shared-store active/passive *(out of scope here, but available)* | Scenario B (`manifests/jdbc/`) |
| One-way DR mirror | Scenario C (`manifests/dr/`) |
| **Active/active two sites, dual mirror** | **`manifests/active-active/`** + the visualizer's **DR mode** |
