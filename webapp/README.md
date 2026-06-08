# AMQ Broker · High Availability Visualizer

A didactic, animated web app that shows **live** two AMQ brokers in an HA cluster
(the **PV**-persistence scenario, namespace `amq-demo`):

- messages being **produced, redistributed between brokers, and consumed**;
- the difference between **Queue (anycast / point-to-point)** and **Topic (multicast / pub-sub)**;
- **failover** and recovery (a broker goes down → the survivor takes over, no message loss).

The data is **real**: the backend reads each broker's metrics via **Jolokia**
(the console's management API), over the HTTPS routes, with `admin/admin`.
👉 This **does not depend on `oc`** — it works even if the cluster token has expired.

## Requirements
- Python 3 (standard library only — **no `pip install`**).
- The `amq-demo` brokers running and their console routes reachable
  (`amq-console-0-...` and `amq-console-1-...`).

## How to run
```bash
cd webapp
python3 server.py
# open http://localhost:8080
```

Stop with `Ctrl+C`.

## Suggested demo script
1. **Queue (anycast)** — open in the default mode. Show the producer publishing to
   **Broker-0**, the cluster **redistributing** to **Broker-1** (where the consumer
   is), and delivery. Point out the live counters going up.
2. **Topic (multicast)** — click *Topic*. Show the **fan-out**: 1 publish becomes
   **a copy for each subscriber** (pub/sub).
3. **Failover** — click *Simulate Broker-0 failure* (or run
   `../scripts/failover-demo.sh`). The card turns **OFFLINE**, a banner appears and
   the narration explains that messages persisted on the **PV** are not lost.
4. **Migration** — click *Scale-down 2→1*. With `messageMigration`, the messages
   from the removed broker **migrate** to the survivor. Then *Restore HA*.

## Reading the broker metrics
- **routed (passed here)** — `RoutedMessageCount` of the address: everything that
  went *through* this broker, **including** what it forwarded to the other broker
  via the cluster store-and-forward bridge. This is why Broker-0 shows activity
  even when it has no local consumer.
- **in queue** — messages currently sitting in this broker's local queue.
- **consumers** — consumers attached to this broker.
- **delivered** — acknowledged (consumed) messages on this broker.

## Configuration (optional environment variables)
| Var | Default |
|---|---|
| `PORT` | `8080` |
| `BROKER0_CONSOLE` / `BROKER1_CONSOLE` | the `amq-demo` routes |
| `BROKER_USER` / `BROKER_PASS` | `admin` / `admin` |
| `QUEUE` | `demoQueue` |
| `TOPIC` | `demoTopic` |
| `NAMESPACE` / `BROKER_CR` | `amq-demo` / `demo-broker` |

Example pointing at another cluster:
```bash
BROKER0_CONSOLE=https://console-0.example BROKER1_CONSOLE=https://console-1.example \
  python3 server.py
```

## Notes
- The demo buttons (*kill / scale*) use the `oc` CLI on your machine. If the login
  expired, the app shows the exact command to run manually — the animation reacts
  either way, because it reads the brokers' real state.
- **Topic** mode is *illustrative* until a real `demoTopic` address exists. Once the
  topic and its subscribers are deployed, it reflects real data automatically
  (`topicLive`).

## Architecture
```
 browser (index.html)  --HTTP-->  server.py (stdlib)  --Jolokia/HTTPS-->  Broker-0 / Broker-1
   animates dots, cards, modes      normalizes JSON state                 real metrics
```
