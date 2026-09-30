#!/usr/bin/env python3
"""chaos soak: the whole sync on a local n8n with the failures prod brings, then a full check

    soak.py [--invoices 1500] [--token-ttl 90] [--kill-cmd 'systemctl --user kill --signal=SIGKILL n8n.service']

1. fresh fixtures, fresh mock (Xero's limits on, tokens expire every --token-ttl s), empty sync tables
2. events go to the intake webhook shuffled, 30% of them twice, plus 5% more with forged signatures.
   2% are never sent at all
3. while the processor drains the queue: Xero answers 3 calls with 503, Xero saves a contact, an invoice,
   a payment and a credit note but the answers get lost (504), n8n gets SIGKILL in the middle of a batch
4. reconcile is run to find the events that were never sent. once work is going again, another workflow
   on the same Xero connection eats the minute budget until Xero refuses the sync too (429)
5. when there's nothing left to do, Xero is checked against Stripe (fixtures.py verify) and the numbers printed

wipes the sync tables of --db, won't run unless app_setting points the sync at the mock.
STRIPE_WEBHOOK_SECRET and OPS_TOKEN come from the environment or ~/.config/stripe-xero/env. stdlib and psql
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
T0 = time.time()


def log(msg):
    print(f'[{time.time() - T0:4.0f}s] {msg}', flush=True)


def psql(db, sql):
    return subprocess.run(['psql', '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-d', db, '-c', sql],
                          check=True, capture_output=True, text=True).stdout.strip()


def http(method, url, body=None, headers=None):
    data = None if body is None else body.encode() if isinstance(body, str) else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={'Content-Type': 'application/json', **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.headers, json.loads(r.read() or b'null')
    except urllib.error.HTTPError as e:
        return e.code, e.headers, None


def up(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.status == 200
    except OSError:
        return False


def wait_until(check, timeout, what):
    end = time.time() + timeout
    while not check():
        if time.time() > end:
            sys.exit(f'gave up waiting for {what}')
        time.sleep(0.5)


def secret(name):
    if os.environ.get(name):
        return os.environ[name]
    env = Path.home() / '.config/stripe-xero/env'
    for line in env.read_text().splitlines() if env.exists() else []:
        key, _, value = line.partition('=')
        if key.strip() == name and value.strip():
            return value.strip().strip('"\'')
    sys.exit(f'{name} is not set (environment or ~/.config/stripe-xero/env)')


def queue(db):
    """(events claimed by a run, events not finished yet)"""
    row = psql(db, "SELECT count(*) FILTER (WHERE status = 'processing') || ' ' || "
                   "count(*) FILTER (WHERE status IN ('pending', 'processing')) FROM stripe_event")
    return tuple(map(int, row.split()))


def batch_in_flight(db, timeout=300):
    """wait for a run holding claimed events, returns how many it holds, 0 if the queue ran dry first"""
    end = time.time() + timeout
    while time.time() < end:
        claimed, todo = queue(db)
        if claimed or not todo:
            return claimed
        time.sleep(0.1)
    return 0


def spend_minute_budget(mock, timeout=180):
    """another workflow on the same Xero connection eats the minute budget until Xero refuses the sync too"""
    _, _, token = http('POST', mock + '/xero/token', 'grant_type=client_credentials', {
        'Authorization': 'Basic ' + base64.b64encode(b'other:workflow').decode(),
        'Content-Type': 'application/x-www-form-urlencoded'})
    auth = {'Authorization': 'Bearer ' + token['access_token']}
    spent = Counter({'tokens issued': 1})
    sync_refused = lambda: http('GET', mock + '/_mock/state')[2]['calls'].get('429 minute', 0) - spent['429 minute']
    before, end = sync_refused(), time.time() + timeout
    while sync_refused() == before and time.time() < end:
        problem = http('GET', mock + '/xero/api.xro/2.0/Organisation', headers=auth)[1].get('X-Rate-Limit-Problem')
        spent['calls'] += 1
        if problem:
            spent['429 ' + problem] += 1
            if problem == 'day':
                break
            time.sleep(1)
    return spent


def drain(db, started, timeout):
    last = 0
    while (todo := queue(db))[1]:
        if time.time() - started > timeout:
            sys.exit(f'{todo[1]} events still open after {timeout} s')
        if time.time() - last >= 60:
            last = time.time()
            log(f'{todo[1]} events to go, {todo[0]} claimed')
        time.sleep(2)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--invoices', type=int, default=1500)
    ap.add_argument('--token-ttl', type=int, default=90, help='seconds a Xero token lives')
    ap.add_argument('--port', type=int, default=8765)
    ap.add_argument('--db', default='stripe_xero_dev')
    ap.add_argument('--n8n', default='http://127.0.0.1:5678')
    ap.add_argument('--kill-cmd', default='systemctl --user kill --signal=SIGKILL n8n.service',
                    help="kills n8n mid-batch; its supervisor must bring it back. '' skips the kill")
    ap.add_argument('--timeout', type=int, default=1800, help='seconds to wait for the queue to drain')
    args = ap.parse_args()
    mock = f'http://127.0.0.1:{args.port}'

    api_base = psql(args.db, "SELECT value FROM app_setting WHERE name = 'xero_api_base'")
    if not api_base.startswith(mock + '/'):
        sys.exit(f'app_setting xero_api_base is {api_base!r}, not the mock at {mock}: refusing to send test data')
    if up(mock + '/_mock/state'):
        sys.exit(f'a mock already runs on {mock}: stop it, the soak starts its own')
    if not up(args.n8n + '/healthz'):
        sys.exit(f'n8n does not answer on {args.n8n}')
    webhook_secret, ops_token = secret('STRIPE_WEBHOOK_SECRET'), secret('OPS_TOKEN')

    work = Path(tempfile.mkdtemp(prefix='soak-'))
    subprocess.run([sys.executable, TOOLS / 'fixtures.py', 'generate', '--invoices', str(args.invoices),
                    '--out', work], check=True)
    server = subprocess.Popen([sys.executable, TOOLS / 'mock_apis.py', '--port', str(args.port),
                               '--stripe-state', work / 'stripe_state.json', '--token-ttl', str(args.token_ttl)])
    try:
        wait_until(lambda: up(mock + '/_mock/state'), 10, 'the mock to start')
        psql(args.db, "TRUNCATE stripe_event, xero_link; UPDATE sync_state SET lease_holder = NULL, "
                      "lease_until = '-infinity', xero_paused_until = '-infinity', pause_reason = NULL")
        # eaten by the first Xero calls and PUTs the sync makes, whenever they come
        http('POST', mock + '/_mock/chaos',
             {'fail': 3, 'status': 503, 'lose': ['Contacts', 'Invoices', 'Payments', 'CreditNotes']})

        started = time.time()
        replay = subprocess.run([sys.executable, TOOLS / 'replay.py', '--events', work / 'events.jsonl',
                                 '--dup-rate', '0.3', '--bad-rate', '0.05', '--miss-rate', '0.02',
                                 '--url', args.n8n + '/webhook/stripe'],
                                env={**os.environ, 'STRIPE_WEBHOOK_SECRET': webhook_secret},
                                capture_output=True, text=True)
        if replay.returncode:
            sys.exit('delivery failed:\n' + replay.stdout + replay.stderr)
        delivery = json.loads(replay.stdout)
        missed = delivery['never_delivered']
        log(f"{delivery['deliveries']} deliveries of {delivery['unique_events']} events in {delivery['wall_s']} s: "
            f"{delivery['responses']}; {len(missed)} events never sent")

        kill = None
        if args.kill_cmd and (claimed := batch_in_flight(args.db)):
            subprocess.run(args.kill_cmd, shell=True, check=True)
            killed = time.time()
            log(f'n8n killed with {claimed} events claimed by the running batch')
            time.sleep(1)
            wait_until(lambda: up(args.n8n + '/healthz'), 180, 'n8n to come back')
            kill = {'events_claimed': claimed, 'n8n_back_after_s': round(time.time() - killed, 1)}
            log(f"n8n is back after {kill['n8n_back_after_s']} s; the dead run's lease and locks expire on their own")

        # nightly reconcile, run now. has to find every event the webhook never brought
        run_now = lambda: http('POST', args.n8n + '/webhook/stripe-xero/reconcile', {},
                               {'Authorization': 'Bearer ' + ops_token})[0] == 202
        wait_until(run_now, 60, 'the reconcile webhook (is the workflow published?)')  # 404 while n8n starts up
        found = f"SELECT count(*) FROM stripe_event WHERE source = 'reconcile' AND id = ANY ('{{{','.join(missed)}}}')"
        wait_until(lambda: int(psql(args.db, found)) == len(missed), 300, 'reconcile to queue the missed events')
        log(f'reconcile queued the {len(missed)} missed events')

        # once work is going again another workflow on the same Xero connection hogs the minute budget
        done = lambda: int(psql(args.db, "SELECT count(*) FROM stripe_event WHERE status = 'done'"))
        before = done()
        wait_until(lambda: done() > before, 600, 'processing to resume')
        neighbour = spend_minute_budget(mock)
        log(f"another workflow spent the Xero minute budget: {neighbour['calls']} calls, "
            f"{neighbour['429 minute']} refused")

        drain(args.db, started, args.timeout)
        wall = time.time() - started
        log('queue drained')

        verified = subprocess.run([sys.executable, TOOLS / 'fixtures.py', 'verify', '--out', work, '--mock', mock,
                                   '--db', args.db]).returncode == 0
        calls = http('GET', mock + '/_mock/state')[2]['calls']
    finally:
        server.terminate()

    ev = json.loads(psql(args.db, """
        SELECT json_build_object('events', count(*), 'done', count(*) FILTER (WHERE status = 'done'),
                                 'dead', count(*) FILTER (WHERE status = 'dead'), 'redeliveries', sum(redeliveries),
                                 'retried', count(*) FILTER (WHERE attempts > 1), 'most_attempts', max(attempts))
          FROM stripe_event"""))
    sync = {k: calls.get(k, 0) - neighbour.get(k, 0) for k in ('calls', '429 minute', '429 concurrent', 'tokens issued')}
    print(json.dumps({
        'events': ev['events'], 'deliveries': delivery['deliveries'], 'redeliveries_absorbed': ev['redeliveries'],
        'forged_rejected': delivery['responses'].get('forged 400', 0), 'never_sent_found_by_reconcile': len(missed),
        'outcome': {'done': ev['done'], 'dead_as_expected': ev['dead']},
        'wall_s': round(wall), 'events_per_min': round(ev['events'] / wall * 60),
        'retried_events': ev['retried'], 'most_attempts': ev['most_attempts'],
        'xero_calls': sync['calls'], 'xero_calls_per_event': round(sync['calls'] / ev['events'], 3),
        'faults_survived': {
            'xero_503': calls.get('chaos 503', 0),
            'lost_responses': sorted(k.split()[2] for k in calls if k.startswith('chaos lost')),
            'xero_429': sync['429 minute'] + sync['429 concurrent'],
            'expired_tokens_401': calls.get('401', 0), 'tokens_issued': sync['tokens issued'],
            'n8n_sigkill': kill},
        'idempotent_replays': calls.get('idempotent replays', 0),
    }, indent=2))
    if not verified:
        sys.exit('soak: FAILED, see the differences above')
    print('soak: OK')


if __name__ == '__main__':
    main()
