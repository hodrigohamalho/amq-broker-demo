# AMQ Broker — HA & DR Architecture Guide

A decision guide for **high availability** and **disaster recovery** with Red Hat
AMQ Broker (ActiveMQ Artemis) on OpenShift. A precise look at the terminology and
trade-offs behind "active/active replication", to support an informed design decision.

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

> **In one line:** *Artemis **HA replication** is synchronous and
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

A consideration specific to OpenShift worth weighing:

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

### Key property: asynchronous and eventually consistent (same as MM2)

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

## 6. Exposure & load balancing across two OpenShift clusters

In a real DR/active-active deployment the two sites are **separate OpenShift clusters,
each with its own Ingress and DNS** — there is no shared load balancer by default. So
there are **two layers** to design: how AMQP is exposed *inside* each cluster, and how
clients are steered *between* the two sites.

### 6.1 Exposing AMQP inside each cluster

AMQP is **TCP, not HTTP** — the default OpenShift Router is L7. Two options:

| Option | How | Use when |
|---|---|---|
| **Passthrough Route + SNI** (port 443) | The Router routes TLS by **SNI**. Acceptor must have **`sslEnabled: true`** (amqps). Client: `amqps://broker.apps.<cluster>/:443` | You want to **reuse the existing Ingress/DNS** (`*.apps.<cluster>`). Most common. |
| **Service `type: LoadBalancer`** (L4 / NLB) | A dedicated cloud NLB per broker/acceptor, raw TCP | You need **plain AMQP** (no TLS) or a dedicated port |

> ⚠️ To go through the Router (reusing the cluster DNS), **AMQP must be TLS** — SNI
> lives in the TLS handshake. Plain `61616` can't be host-routed by the Router; for
> that, use a LoadBalancer Service. With a cluster of N brokers per site, the Operator
> creates **one route per broker pod**.

### 6.2 Steering clients between the two sites

Two independent clusters/DNS ⇒ the cross-site layer sits **above** both:

- **Client-side failover (multi-host)** — *preferred for AMQP.* The client library
  (Qpid JMS, Proton) is given **both** sites:
  `failover:(amqps://broker.site1…:443,amqps://broker.site2…:443)`. It connects to one
  and reconnects to the other on failure (which already holds the mirrored data). No
  extra infrastructure; failover in seconds. Needs each app configured with both URLs.
- **GSLB / global DNS** — *single endpoint.* A global DNS balancer (Route 53 w/ health
  checks, F5 GTM, Infoblox, Akamai…) fronts both sites; apps use **one** hostname that
  resolves to the healthy site. Best when apps are off-the-shelf and only accept a
  **single** broker URL. Caveat: DNS **TTL/caching** means failover isn't instant — an
  AMQP client only re-resolves on reconnect, so keep TTL low (30–60s).

### 6.3 Recommended pattern ("mostly AMQP", two active sites)

```
                 ┌─────────────── GSLB (geo / health) ───────────────┐
                 │  broker.example.com → nearest healthy site         │
                 └───────────────┬───────────────────┬───────────────┘
            apps (region A) ─────┘                   └───── apps (region B)
                 │                                             │
        amqps :443 (SNI route)                        amqps :443 (SNI route)
                 ▼                                             ▼
      ┌────────────────────┐   dual AMQP mirror (amqps,  ┌────────────────────┐
      │  OpenShift Site 1  │ ◀═══ cross-cluster, TLS ═══▶ │  OpenShift Site 2  │
      │  broker(s) + PV    │                              │  broker(s) + PV    │
      └────────────────────┘                              └────────────────────┘
```

- **GSLB steers by locality** (each site serves its own region — both active) and
  fails everyone over to the survivor on a site outage.
- **Client failover list** as a backstop for apps that support it (reconnects without
  waiting for DNS TTL).
- The **mirror** keeps the data converged; GSLB/client does the **switch**.

### 6.4 The mirror connection is itself cross-cluster

In the single-cluster demo the mirror uses internal Service DNS. Across **two real
clusters** the mirror (Site 1 → Site 2) is **just another AMQP client** and must reach
the **external** endpoint of the remote site:
```
AMQPConnections.toSite2.uri=tcp://broker.apps.site2…:443   # TLS/SNI + mutual trust
```
So each site's AMQP acceptor must be **routable from the other site** (route or LB),
with TLS and a configured **truststore** between clusters — normally over private
network/peering, not the public internet.

> Reminder: GSLB/LB solve **reach and failover, not exactly-once**. If the same logical
> queue is consumed on both sites, the async mirror's duplicate window still applies —
> use idempotent consumers / dedup (see §4).

## 7. Active/passive across two sites: failover control, mirror direction & fencing

A common DR shape: **Site 1 active, Site 2 passive** standby, two separate OpenShift
clusters, mostly **AMQP** apps reaching the broker through a load balancer. Three
decisions define how robust (and how controllable) it is.

### 7.1 Failover control — GSLB (centralized) vs client failover list

| | Client-side failover list | **GSLB / DNS (centralized)** |
|---|---|---|
| Who decides to fail over | each app, independently | one arbiter: the **GSLB health-check** |
| RTO | seconds | health-check + DNS TTL (≈ 1–3 min, tunable) |
| Behaviour on a **false negative** (transient blip) | some apps jump to Site 2, others stay → **load split unpredictably** across sites → accidental active/active | apps just reconnect to Site 1 (GSLB didn't flip) → **nothing moves** |
| Control of "who is where" | lost (per-app decisions) | full — it's the state of one DNS record |
| Switch is | per-app, gradual, sticky | **all-or-nothing**, deterministic |

For an active/passive design where the goal is *everyone on Site 1, switch together*,
**GSLB-only is the right trade**: you give up RTO for **centralized, deterministic**
failover. The flow: clients hold **one hostname** (no list); a blip drops connections
and they reconnect — re-resolving DNS to **the same** site; only when the GSLB
health-check confirms a sustained failure does the record flip and **all** clients move
on their next reconnect.

> **The one knob:** the **GSLB health-check** (require N consecutive failures; low DNS
> TTL of 30–60 s; ensure clients re-resolve DNS on each reconnect, not pin the IP).
> Because the decision is centralized, a false negative on a single client can't scatter
> the load.

### 7.2 Mirror direction — one-way + invert vs bidirectional

The dual-mirror "downside" (duplicate-delivery) comes from consumers on **both** sites.
In active/passive **Site 2 is passive — no consumers — so that risk does not apply**,
which makes bidirectional the *simpler* choice, not the riskier one.

| | One-way (Site1→Site2) + invert on failover | **Bidirectional (dual)** |
|---|---|---|
| Normal state | 1 mirror connection | 2 (the return one idles, harmless) |
| On failover | **manual step**: reconfigure the mirror on Site 2 → a `brokerProperties` change = **broker reconcile/restart** mid-incident | **nothing to do** — the Site2→Site1 link already exists |
| Failback | Site 1 is stale; must re-sync (invert again) | **automatic**: what Site 2 produced while Site 1 was down was buffered and **flushes to Site 1 on recovery** |
| Operational risk | high (human step + restart at the worst time) | low |

**Recommendation: keep it bidirectional and change nothing on failure.** It's safe here
(only one site consumes), and failover **and** failback need no action on the mirror —
the recovering site re-syncs itself.

### 7.3 Fencing — preventing split-brain

DNS failover is **not atomic**. The dangerous case isn't a clean site loss — it's a
**false positive / partition**: the GSLB can't reach Site 1 and flips, but Site 1 is
**still up** and some clients (stale DNS cache, connections that never dropped) keep
processing on it → **both sites active** → double processing and divergent state.

**Fencing** = guaranteeing Site 1 is truly out when Site 2 takes over.

> ⚠️ AMQP mirror is **async replication — no quorum, no leader election, no built-in
> fencing** (unlike Artemis HA *replication*). So **fencing is an infra/ops
> responsibility**, not something the broker does for you.

How to fence Site 1 in this setup (most → least robust):
1. **Cut Site 1 at the network** (firewall / close the broker route or LB / network
   ACL) when the GSLB declares the failure — no client reaches it, even with a stale
   cache. STONITH-style.
2. **Scale the Site 1 brokers to 0** — only works if Site 1 is still *reachable* (in a
   partition it may not be, which is why the network cut above is more reliable).
3. **Automation tied to the GSLB flip** triggers the fence; or a **runbook** step
   ("confirm/fence Site 1 before declaring Site 2 active").

When it matters: a **true site loss** needs no fencing (it's already dead); fencing is
for the **partial-failure / false-positive** case.

**Three layers, none sufficient alone:** (1) tune the health-check to avoid false flips;
(2) **fence** the suspect site; (3) **idempotent consumers / dedup** in the app as the
net for the short DNS-propagation window. (The broker's duplicate detection avoids
re-adding the same mirrored message, but does **not** coordinate consumption across two
active sites — app idempotency does.)

### Recommended blueprint (active/passive, two clusters, mostly AMQP)

- **GSLB-only failover**, single hostname, **no client failover list** → centralized,
  all-or-nothing switch; tune the health-check + low DNS TTL.
- **Bidirectional (dual) mirror** → automatic failover *and* failback, no mirror change.
- **Fence Site 1** on failover (network cut / scale 0) → no split-brain.
- **Idempotent consumers** → safety net for the propagation window.
- Accept the trade: **RTO of minutes for full control and determinism.**

## 8. How this maps to the demo scenarios in this repo

| This guide | Repo scenario |
|---|---|
| Clustered active/active (PV) + client failover | **Scenario A** (`manifests/`) |
| Shared-store active/passive *(out of scope here, but available)* | Scenario B (`manifests/jdbc/`) |
| One-way DR mirror | Scenario C (`manifests/dr/`) |
| **Active/active two sites, dual mirror** | **`manifests/active-active/`** + the visualizer's **DR mode** |
