#!/usr/bin/env python3
"""send Stripe events to the intake webhook the way Stripe does: signed, repeated, out of order

    replay.py --synthetic 500 --dup-rate 0.3 --bad-rate 0.05 --concurrency 16
    replay.py --events events.jsonl --shuffle --miss-rate 0.02   (2% never sent, reconcile has to find them)

signing secret comes from STRIPE_WEBHOOK_SECRET (e.g. `set -a; . ~/.config/stripe-xero/env; set +a`)
stdlib only
"""
import argparse
import hashlib
import hmac
import json
import os
import random
import statistics
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor


def sign(body: bytes, secret: str, ts: int) -> str:
    mac = hmac.new(secret.encode(), str(ts).encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def synthetic(n: int):
    # bare minimum invoice.paid, enough for intake. real payloads come from --events
    for i in range(n):
        yield {
            "id": f"evt_synth_{i:06d}",
            "object": "event",
            "type": "invoice.paid",
            "created": int(time.time()),
            "data": {"object": {"id": f"in_synth_{i:06d}", "object": "invoice", "customer_name": "Zoë Ångström"}},
        }


def deliver(url: str, body: bytes, header: str):
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json; charset=utf-8",
        "Stripe-Signature": header,
        "User-Agent": "Stripe/1.0 (+https://stripe.com/docs/webhooks) replay.py",
    })
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        status, payload = e.code, e.read()
    except OSError as e:
        status, payload = 0, str(e).encode()
    return status, time.perf_counter() - started, payload


def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--events", help="JSONL file, one Stripe event per line")
    src.add_argument("--synthetic", type=int, metavar="N")
    ap.add_argument("--url", default="http://127.0.0.1:5678/webhook/stripe")
    ap.add_argument("--dup-rate", type=float, default=0.0, help="share of events delivered twice")
    ap.add_argument("--bad-rate", type=float, default=0.0, help="share of extra deliveries with a forged signature")
    ap.add_argument("--miss-rate", type=float, default=0.0, help="share of events never delivered")
    ap.add_argument("--shuffle", action="store_true")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    secret = os.environ.get("STRIPE_WEBHOOK_SECRET")
    if not secret:
        raise SystemExit("STRIPE_WEBHOOK_SECRET is not set")
    rnd = random.Random(args.seed)

    if args.events:
        with open(args.events, encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]
    else:
        events = list(synthetic(args.synthetic))

    bodies = [json.dumps(e, ensure_ascii=False).encode() for e in events]
    missed = [e["id"] for e in events if rnd.random() < args.miss_rate]
    bodies = [b for e, b in zip(events, bodies) if e["id"] not in missed]
    jobs = [(b, True) for b in bodies]
    jobs += [(b, True) for b in bodies if rnd.random() < args.dup_rate]
    jobs += [(b, False) for b in bodies if rnd.random() < args.bad_rate]
    if args.shuffle or args.dup_rate or args.bad_rate:
        rnd.shuffle(jobs)

    def run(job):
        body, good = job
        header = sign(body, secret if good else "whsec_forged", int(time.time()))
        return good, *deliver(args.url, body, header)

    started = time.perf_counter()
    with ThreadPoolExecutor(args.concurrency) as pool:
        results = list(pool.map(run, jobs))
    wall = time.perf_counter() - started

    by_kind = Counter((("signed" if good else "forged"), status) for good, status, _, _ in results)
    dupes = sum(1 for good, status, _, p in results if good and status == 200 and b'"duplicate":true' in p)
    lat = sorted(l for _, _, l, _ in results)
    print(json.dumps({
        "unique_events": len(bodies),
        "never_delivered": missed,
        "deliveries": len(jobs),
        "responses": {f"{k} {s}": n for (k, s), n in sorted(by_kind.items())},
        "acked_as_duplicate": dupes,
        "latency_ms": {"p50": round(statistics.median(lat) * 1000, 1),
                       "p95": round(lat[int(len(lat) * 0.95) - 1] * 1000, 1),
                       "max": round(lat[-1] * 1000, 1)},
        "wall_s": round(wall, 2),
        "per_s": round(len(jobs) / wall, 1),
    }, indent=2))
    failed = [r for r in results if (r[0] and r[1] != 200) or (not r[0] and r[1] != 400)]
    if failed:
        print(f"{len(failed)} unexpected responses, first: {failed[0][1]} {failed[0][3][:300]!r}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
