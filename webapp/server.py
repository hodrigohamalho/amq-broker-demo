#!/usr/bin/env python3
"""
AMQ Broker HA Visualizer - backend (stdlib only, no dependencies).

Reads real metrics from each broker via Jolokia (the AMQ Broker console) and
exposes a clean JSON API for the animated frontend. It also offers demo actions
(failover / scale) that use `oc` when available.

Usage:
    python3 server.py
    # opens http://localhost:8080

Config via environment variables (with defaults for the amq-demo scenario):
    PORT                HTTP port (default 8080)
    BROKER0_CONSOLE     base URL of broker-0's console
    BROKER1_CONSOLE     base URL of broker-1's console
    BROKER_USER         console user (default admin)
    BROKER_PASS         console password (default admin)
    QUEUE               anycast queue name (default demoQueue)
    TOPIC               multicast address/topic name (default demoTopic)
    NAMESPACE           brokers' namespace (default amq-demo)
    BROKER_CR           ActiveMQArtemis CR name (default demo-broker)
"""
import base64
import json
import os
import ssl
import subprocess
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

HERE = os.path.dirname(os.path.abspath(__file__))

CFG = {
    "port": int(os.environ.get("PORT", "8080")),
    "user": os.environ.get("BROKER_USER", "admin"),
    "pass": os.environ.get("BROKER_PASS", "admin"),
    "queue": os.environ.get("QUEUE", "demoQueue"),
    "topic": os.environ.get("TOPIC", "demoTopic"),
    "namespace": os.environ.get("NAMESPACE", "amq-demo"),
    "cr": os.environ.get("BROKER_CR", "demo-broker"),
}

BROKERS = [
    {
        "id": 0,
        "name": "Broker-0",
        "pod": f"{CFG['cr']}-ss-0",
        "console": os.environ.get(
            "BROKER0_CONSOLE",
            "https://amq-console-0-amq-demo.apps.cluster1.sandbox1992.opentlc.com",
        ).rstrip("/"),
    },
    {
        "id": 1,
        "name": "Broker-1",
        "pod": f"{CFG['cr']}-ss-1",
        "console": os.environ.get(
            "BROKER1_CONSOLE",
            "https://amq-console-1-amq-demo.apps.cluster1.sandbox1992.opentlc.com",
        ).rstrip("/"),
    },
]

# --- Active/Active two-site (dual mirror) DR scenario ---
DR_NAMESPACE = os.environ.get("DR_NAMESPACE", "amq-aa")
SITES = [
    {
        "id": 1, "name": "Site 1", "cr": "site1-broker", "mirror": "toSite2",
        "console": os.environ.get(
            "SITE1_CONSOLE",
            "https://site1-console-amq-aa.apps.cluster1.sandbox1992.opentlc.com",
        ).rstrip("/"),
    },
    {
        "id": 2, "name": "Site 2", "cr": "site2-broker", "mirror": "toSite1",
        "console": os.environ.get(
            "SITE2_CONSOLE",
            "https://site2-console-amq-aa.apps.cluster1.sandbox1992.opentlc.com",
        ).rstrip("/"),
    },
]

_SSL = ssl.create_default_context()
_SSL.check_hostname = False
_SSL.verify_mode = ssl.CERT_NONE
_AUTH = "Basic " + base64.b64encode(
    f"{CFG['user']}:{CFG['pass']}".encode()
).decode()
_POOL = ThreadPoolExecutor(max_workers=8)


def _jolokia_post(base, body, timeout=3.0):
    """Batched Jolokia POST. Returns a list of responses, or None."""
    url = f"{base}/console/jolokia/"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", _AUTH)
    req.add_header("Content-Type", "application/json")
    req.add_header("Origin", base)  # some Jolokia setups require Origin
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def _q_mbean(queue, routing="anycast"):
    return (
        f'org.apache.activemq.artemis:address="{queue}",broker="amq-broker",'
        f'component=addresses,queue="{queue}",routing-type="{routing}",'
        f"subcomponent=queues"
    )


def _addr_mbean(addr):
    return (
        f'org.apache.activemq.artemis:address="{addr}",broker="amq-broker",'
        f"component=addresses"
    )


def read_broker(b):
    """Read a broker's real state via Jolokia (batched)."""
    qmb = _q_mbean(CFG["queue"])
    tmb = _addr_mbean(CFG["topic"])
    batch = [
        {"type": "read", "mbean": 'org.apache.activemq.artemis:broker="amq-broker"',
         "attribute": "Active"},
        {"type": "read", "mbean": qmb,
         "attribute": ["MessageCount", "ConsumerCount", "MessagesAdded",
                       "MessagesAcknowledged", "DeliveringCount"]},
        {"type": "read", "mbean": tmb,
         "attribute": ["MessageCount", "RoutedMessageCount", "QueueNames"]},
        {"type": "read", "mbean": _addr_mbean(CFG["queue"]),
         "attribute": ["RoutedMessageCount"]},
    ]
    res = _jolokia_post(b["console"], batch)
    out = {
        "id": b["id"], "name": b["name"], "pod": b["pod"],
        "online": False, "active": False,
        "queue": None, "topic": None,
    }
    if not res or not isinstance(res, list):
        return out
    out["online"] = True
    try:
        if res[0].get("status") == 200:
            out["active"] = bool(res[0].get("value"))
    except Exception:
        pass
    try:
        if res[1].get("status") == 200:
            v = res[1]["value"]
            out["queue"] = {
                "messageCount": v.get("MessageCount", 0),
                "consumerCount": v.get("ConsumerCount", 0),
                "messagesAdded": v.get("MessagesAdded", 0),
                "messagesAcknowledged": v.get("MessagesAcknowledged", 0),
                "deliveringCount": v.get("DeliveringCount", 0),
                "routed": 0,
            }
    except Exception:
        pass
    # RoutedMessageCount on the ADDRESS: counts everything that passed through the
    # broker (including what was forwarded via store-and-forward to the other broker).
    try:
        if len(res) > 3 and res[3].get("status") == 200 and out["queue"] is not None:
            out["queue"]["routed"] = res[3]["value"].get("RoutedMessageCount", 0)
    except Exception:
        pass
    try:
        if res[2].get("status") == 200:
            v = res[2]["value"]
            qn = v.get("QueueNames") or []
            out["topic"] = {
                "messageCount": v.get("MessageCount", 0),
                "routedMessageCount": v.get("RoutedMessageCount", 0),
                "subscriptions": len(qn) if isinstance(qn, list) else 0,
            }
    except Exception:
        pass
    return out


# DEMO_DATA mode: serve synthetic-but-realistic state so the UI is alive without a
# live cluster (great for screenshots and for trying the app before deploying).
DEMO = os.environ.get("DEMO_DATA", "").lower() in ("1", "true", "yes", "on")
_demo = {"r0": 480, "r1": 1190, "ack1": 1188, "troute": 240}


def demo_state():
    # advance counters to simulate a steady ~5 msg/s flow between polls
    _demo["r0"] += 5
    _demo["r1"] += 5
    _demo["ack1"] += 5
    _demo["troute"] += 3
    b0 = {"id": 0, "name": "Broker-0", "pod": "demo-broker-ss-0",
          "online": True, "active": True,
          "queue": {"messageCount": 0, "consumerCount": 0, "messagesAdded": 15,
                    "messagesAcknowledged": 0, "deliveringCount": 0,
                    "routed": _demo["r0"]},
          "topic": {"messageCount": 0, "routedMessageCount": _demo["troute"],
                    "subscriptions": 3}}
    b1 = {"id": 1, "name": "Broker-1", "pod": "demo-broker-ss-1",
          "online": True, "active": True,
          "queue": {"messageCount": 0, "consumerCount": 1,
                    "messagesAdded": _demo["r1"],
                    "messagesAcknowledged": _demo["ack1"], "deliveringCount": 0,
                    "routed": _demo["r1"]},
          "topic": None}
    return {"queue": CFG["queue"], "topic": CFG["topic"],
            "topicLive": True, "brokers": [b0, b1]}


def get_state():
    if DEMO:
        return demo_state()
    brokers = list(_POOL.map(read_broker, BROKERS))
    brokers.sort(key=lambda x: x["id"])
    topic_live = any(b.get("topic") for b in brokers)
    return {
        "queue": CFG["queue"],
        "topic": CFG["topic"],
        "topicLive": topic_live,
        "brokers": brokers,
    }


# ---- Two-site dual-mirror (DR) state ----
def _mirror_mbean(name):
    q = f"$ACTIVEMQ_ARTEMIS_MIRROR_{name}"
    return (f'org.apache.activemq.artemis:address="{q}",broker="amq-broker",'
            f'component=addresses,queue="{q}",routing-type="anycast",'
            f"subcomponent=queues")


def read_site(site):
    """Read a site's state: broker Active, demoQueue depth/routed, and the
    outbound mirror queue MessagesAdded (= events replicated to the other site)."""
    out = {"id": site["id"], "name": site["name"], "cr": site["cr"],
           "mirror": site["mirror"], "online": False, "active": False,
           "messageCount": 0, "routed": 0, "mirrorOut": 0, "consumers": 0}
    batch = [
        {"type": "read", "mbean": 'org.apache.activemq.artemis:broker="amq-broker"',
         "attribute": "Active"},
        {"type": "read", "mbean": _addr_mbean(CFG["queue"]),
         "attribute": ["MessageCount", "RoutedMessageCount"]},
        {"type": "read", "mbean": _mirror_mbean(site["mirror"]),
         "attribute": ["MessagesAdded"]},
        {"type": "read", "mbean": _q_mbean(CFG["queue"]),
         "attribute": ["ConsumerCount"]},
    ]
    res = _jolokia_post(site["console"], batch)
    if not res or not isinstance(res, list):
        return out
    out["online"] = True
    try:
        if res[0].get("status") == 200:
            out["active"] = bool(res[0].get("value"))
    except Exception:
        pass
    try:
        if res[1].get("status") == 200:
            v = res[1]["value"]
            out["messageCount"] = v.get("MessageCount", 0)
            out["routed"] = v.get("RoutedMessageCount", 0)
    except Exception:
        pass
    try:
        if res[2].get("status") == 200:
            out["mirrorOut"] = res[2]["value"].get("MessagesAdded", 0)
    except Exception:
        pass
    try:
        if len(res) > 3 and res[3].get("status") == 200:
            out["consumers"] = res[3]["value"].get("ConsumerCount", 0)
    except Exception:
        pass
    return out


_dr_demo = {"s1q": 18, "s2q": 21, "m12": 140, "m21": 150}


def dr_demo_state():
    _dr_demo["s1q"] += 2
    _dr_demo["s2q"] += 2
    _dr_demo["m12"] += 4
    _dr_demo["m21"] += 3
    return {"sites": [
        {"id": 1, "name": "Site 1", "cr": "site1-broker", "mirror": "toSite2",
         "online": True, "active": True, "messageCount": _dr_demo["s1q"],
         "routed": _dr_demo["s1q"] + _dr_demo["m21"], "mirrorOut": _dr_demo["m12"],
         "consumers": 1},
        {"id": 2, "name": "Site 2", "cr": "site2-broker", "mirror": "toSite1",
         "online": True, "active": True, "messageCount": _dr_demo["s2q"],
         "routed": _dr_demo["s2q"] + _dr_demo["m12"], "mirrorOut": _dr_demo["m21"],
         "consumers": 1},
    ]}


def get_dr_state():
    if DEMO:
        return dr_demo_state()
    sites = list(_POOL.map(read_site, SITES))
    sites.sort(key=lambda x: x["id"])
    return {"sites": sites}


def dr_action(kind, site_id):
    ns = DR_NAMESPACE
    site = next((s for s in SITES if s["id"] == site_id), None)
    cr = site["cr"] if site else f"site{site_id}-broker"
    size = "0" if kind == "kill" else "1"
    if kind not in ("kill", "restore"):
        return {"ok": False, "error": "unknown action"}
    ok, out = run_oc(["patch", "activemqartemis", cr, "-n", ns, "--type", "merge",
                      "-p", '{"spec":{"deploymentPlan":{"size":%s}}}' % size])
    manual = ("oc patch activemqartemis %s -n %s --type merge "
              "-p '{\"spec\":{\"deploymentPlan\":{\"size\":%s}}}'" % (cr, ns, size))
    # On restore, bounce that site's consumer so a failed-over client returns home —
    # but only AFTER the broker is ready again (delayed, detached), to avoid it just
    # failing back over to the survivor.
    if kind == "restore":
        try:
            subprocess.Popen(
                ["bash", "-c",
                 f"sleep 50; oc rollout restart deployment/site{site_id}-consumer -n {ns}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True)
        except Exception:
            pass
    return {"ok": ok, "output": out, "manual": manual,
            "needsLogin": (not ok and ("Unauthorized" in out or "login" in out.lower()))}


def run_oc(args, timeout=20):
    """Run an oc command; returns (ok, text)."""
    try:
        p = subprocess.run(
            ["oc"] + args, capture_output=True, text=True, timeout=timeout
        )
        ok = p.returncode == 0
        return ok, (p.stdout + p.stderr).strip()
    except FileNotFoundError:
        return False, "oc nao encontrado no PATH"
    except subprocess.TimeoutExpired:
        return False, "timeout no comando oc"


def action(kind, broker_id):
    ns = CFG["namespace"]
    cr = CFG["cr"]
    pod = f"{cr}-ss-{broker_id}"
    if kind == "kill":
        ok, out = run_oc(["delete", "pod", pod, "-n", ns,
                          "--grace-period=0", "--force"])
        manual = f"oc delete pod {pod} -n {ns} --grace-period=0 --force"
    elif kind == "scaledown":
        ok, out = run_oc(["patch", "activemqartemis", cr, "-n", ns,
                          "--type", "merge", "-p",
                          '{"spec":{"deploymentPlan":{"size":1}}}'])
        manual = (f"oc patch activemqartemis {cr} -n {ns} --type merge "
                  "-p '{\"spec\":{\"deploymentPlan\":{\"size\":1}}}'")
    elif kind == "scaleup":
        ok, out = run_oc(["patch", "activemqartemis", cr, "-n", ns,
                          "--type", "merge", "-p",
                          '{"spec":{"deploymentPlan":{"size":2}}}'])
        manual = (f"oc patch activemqartemis {cr} -n {ns} --type merge "
                  "-p '{\"spec\":{\"deploymentPlan\":{\"size\":2}}}'")
    else:
        return {"ok": False, "error": "unknown action"}
    return {"ok": ok, "output": out, "manual": manual,
            "needsLogin": (not ok and ("Unauthorized" in out or "login" in out.lower()))}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass  # silent

    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                with open(os.path.join(HERE, "index.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(404, "index.html nao encontrado")
        elif path == "/api/config":
            self._send(200, {
                "queue": CFG["queue"], "topic": CFG["topic"],
                "namespace": CFG["namespace"], "cr": CFG["cr"],
                "brokers": [{"id": b["id"], "name": b["name"],
                             "pod": b["pod"], "console": b["console"]}
                            for b in BROKERS],
            })
        elif path == "/api/state":
            self._send(200, get_state())
        elif path == "/api/dr-state":
            self._send(200, get_dr_state())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        if path.startswith("/api/dr-action/"):
            parts = path.split("/")
            # /api/dr-action/<kind>/<siteId>
            kind = parts[3] if len(parts) > 3 else ""
            try:
                sid = int(parts[4]) if len(parts) > 4 else 1
            except ValueError:
                sid = 1
            self._send(200, dr_action(kind, sid))
        elif path.startswith("/api/action/"):
            parts = path.split("/")
            # /api/action/<kind>/<brokerId>
            kind = parts[3] if len(parts) > 3 else ""
            try:
                bid = int(parts[4]) if len(parts) > 4 else 0
            except ValueError:
                bid = 0
            self._send(200, action(kind, bid))
        else:
            self._send(404, {"error": "not found"})


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", CFG["port"]), Handler)
    print("=" * 60)
    print("  AMQ Broker HA Visualizer")
    print("=" * 60)
    for b in BROKERS:
        print(f"  {b['name']}: {b['console']}")
    print(f"  Queue:  {CFG['queue']}")
    print(f"  Topic:  {CFG['topic']}")
    print(f"\n  >> Open in your browser:  http://localhost:{CFG['port']}\n")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()
