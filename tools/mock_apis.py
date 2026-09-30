#!/usr/bin/env python3
"""local fake of the bits of Xero and Stripe the sync uses, with Xero's limits on

    mock_apis.py [--port 8765] [--stripe-state fixtures/stripe_state.json] [--day-limit 1000]

Xero    POST /xero/token                      client credentials, tokens expire (--token-ttl)
        /xero/api.xro/2.0/Contacts            GET (where ContactNumber/Name == ...), PUT (create only)
        /xero/api.xro/2.0/Invoices            GET (InvoiceNumbers=...), PUT
        /xero/api.xro/2.0/CreditNotes         GET (where Reference == ...), PUT
        /xero/api.xro/2.0/Payments            PUT, for invoices and credit notes
        /xero/api.xro/2.0/Organisation, /Accounts
        60 calls/min, 1000/day and 5 in flight per tenant (all tunable), over that a 429 with Retry-After
        and X-Rate-Limit-Problem. X-*Limit-Remaining on every response. Idempotency-Key replays for
        6 min. summarizeErrors=false gives per-element results. USD and NoTax only
Stripe  GET /stripe/v1/invoice_payments?payment[payment_intent]=pi_...   from --stripe-state
        GET /stripe/v1/events?types[]=...&created[gte]=...   paged like Stripe, newest first
        GET /stripe/v1/invoices/in_..., /stripe/v1/refunds/re_...   latest state from the events
Telegram POST /telegram/bot<token>/sendMessage   alerts are kept and shown in /_mock/state
Control GET /_mock/state, POST /_mock/reset, POST /_mock/chaos with
        {"fail": 3, "status": 503}      next 3 Xero calls fail before Xero does anything
        {"lose": ["Invoices", ...]}     next PUT to each is saved but the answer gets lost (504)

stdlib only. not a full Xero, just what the sync depends on, checked against Xero's docs
"""
import argparse
import hashlib
import json
import math
import re
import threading
import time
import uuid
from collections import Counter
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

ACCOUNTS = [
    {'AccountID': 'a0000000-0000-4000-8000-000000000200', 'Code': '200', 'Name': 'Sales', 'Type': 'REVENUE',
     'Status': 'ACTIVE'},
    {'AccountID': 'a0000000-0000-4000-8000-000000000090', 'Code': '090', 'Name': 'Business Bank Account',
     'Type': 'BANK', 'Status': 'ACTIVE'},
]
BANK_CODES = {'090'}
ORG = {'Name': 'Demo Company (US)', 'BaseCurrency': 'USD', 'CountryCode': 'US', 'Class': 'DEMO'}
IDEMPOTENCY_TTL = 360


def money(v):
    return Decimal(str(v)).quantize(Decimal('0.01'))


def num(d):
    return float(d)


def where_terms(where):
    """Field=="value" terms of a Xero where clause joined by OR, that's all the mock needs"""
    return re.findall(r'([\w.]+)\s*==\s*"((?:[^"\\]|\\.)*)"', where or '')


class Xero:
    def __init__(self, day_limit, token_ttl, latency, minute_limit=60, concurrent_limit=5):
        self.day_limit, self.token_ttl, self.latency = day_limit, token_ttl, latency
        self.minute_limit, self.concurrent_limit = minute_limit, concurrent_limit
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.contacts, self.invoices, self.credit_notes, self.payments = {}, {}, {}, {}
        self.tokens, self.idem, self.alerts = {}, {}, []
        self.minute_start, self.minute_calls, self.day_calls, self.in_flight = 0.0, 0, 0, 0
        self.stats = Counter()
        self.chaos = {'fail': 0, 'status': 503, 'lose': []}

    # --- limits -----------------------------------------------------------------------------------
    def admit(self):
        """count a call against the tenant's limits, returns None or a 429 (headers, problem)"""
        now = time.time()
        if now - self.minute_start >= 60:
            self.minute_start, self.minute_calls = now, 0
        if self.in_flight >= self.concurrent_limit:
            return {}, 'concurrent'
        if self.day_calls >= self.day_limit:
            return {'Retry-After': '3600'}, 'day'
        if self.minute_calls >= self.minute_limit:
            return {'Retry-After': str(math.ceil(60 - (now - self.minute_start)))}, 'minute'
        self.minute_calls += 1
        self.day_calls += 1
        self.in_flight += 1
        return None

    def remaining(self):
        return {'X-DayLimit-Remaining': str(self.day_limit - self.day_calls),
                'X-MinLimit-Remaining': str(self.minute_limit - self.minute_calls),
                'X-AppMinLimit-Remaining': str(10000 - self.minute_calls)}

    # --- contacts ---------------------------------------------------------------------------------
    def get_contacts(self, q):
        terms = where_terms(q.get('where', [''])[0])
        ids = set(q.get('IDs', [''])[0].split(',')) - {''}
        out = []
        for c in self.contacts.values():
            hit = c['ContactID'] in ids or any(
                (f == 'ContactNumber' and c.get('ContactNumber') == v) or (f == 'Name' and c['Name'].lower() == v.lower())
                for f, v in terms)
            if hit:
                out.append(c)
        return {'Contacts': out}

    def put_contact(self, el):
        name, number = (el.get('Name') or '').strip(), el.get('ContactNumber')
        if not name:
            return ['The contact name must be specified.']
        if any(c['Name'].lower() == name.lower() for c in self.contacts.values()):
            return [f'The contact name {name} is already assigned to another contact. '
                    'The contact name must be unique across all active contacts.']
        if number and any(c.get('ContactNumber') == number for c in self.contacts.values()):
            return [f'The contact number {number} is already assigned to another contact.']
        c = {'ContactID': str(uuid.uuid4()), 'ContactNumber': number, 'Name': name,
             'EmailAddress': el.get('EmailAddress', ''), 'ContactStatus': 'ACTIVE'}
        self.contacts[c['ContactID']] = c
        return c

    # --- documents --------------------------------------------------------------------------------
    def lines(self, el, errors):
        out = []
        if el.get('LineAmountTypes', 'Exclusive') != 'NoTax':
            errors.append('This mock only supports LineAmountTypes NoTax.')
        if not el.get('LineItems'):
            errors.append('At least one line item is required.')
        for li in el.get('LineItems') or []:
            if li.get('AccountCode') not in {a['Code'] for a in ACCOUNTS}:
                errors.append(f"Account code '{li.get('AccountCode')}' is not a valid code for this document.")
                continue
            amount = money(Decimal(str(li.get('Quantity', 1))) * money(li.get('UnitAmount', 0)))
            out.append({**li, 'LineItemID': str(uuid.uuid4()), 'LineAmount': num(amount)})
        return out, money(sum((Decimal(str(li['LineAmount'])) for li in out), Decimal(0)))

    def common(self, el, errors):
        contact = self.contacts.get((el.get('Contact') or {}).get('ContactID'))
        if contact is None:
            errors.append('A valid ContactID is required.')
        if set(el.get('Contact') or {}) - {'ContactID'}:
            errors.append('Send only ContactID in Contact: other fields would update the contact.')
        if el.get('CurrencyCode', 'USD') != 'USD':
            errors.append(f"Organisation is not subscribed to currency {el.get('CurrencyCode')}")
        if el.get('Status') not in (None, 'DRAFT', 'AUTHORISED'):
            errors.append('Status must be DRAFT or AUTHORISED.')
        return contact

    def put_invoice(self, el):
        errors = []
        if el.get('Type') != 'ACCREC':
            errors.append('This mock only supports ACCREC invoices.')
        contact = self.common(el, errors)
        lines, total = self.lines(el, errors)
        number = el.get('InvoiceNumber') or f'INV-{len(self.invoices) + 1:04d}'
        if any(i['InvoiceNumber'] == number for i in self.invoices.values()):
            errors.append('Invoice # must be unique.')
        if errors:
            return errors
        inv = {'InvoiceID': str(uuid.uuid4()), 'Type': 'ACCREC', 'InvoiceNumber': number,
               'Reference': el.get('Reference', ''), 'Contact': {'ContactID': contact['ContactID'], 'Name': contact['Name']},
               'DateString': el.get('Date'), 'DueDateString': el.get('DueDate'), 'Status': el.get('Status', 'DRAFT'),
               'LineAmountTypes': 'NoTax', 'LineItems': lines, 'CurrencyCode': 'USD', 'SubTotal': num(total),
               'TotalTax': 0.0, 'Total': num(total), 'AmountDue': num(total), 'AmountPaid': 0.0, 'Payments': []}
        self.invoices[inv['InvoiceID']] = inv
        return inv

    def put_credit_note(self, el):
        errors = []
        if el.get('Type') != 'ACCRECCREDIT':
            errors.append('This mock only supports ACCRECCREDIT credit notes.')
        contact = self.common(el, errors)
        lines, total = self.lines(el, errors)
        number = el.get('CreditNoteNumber') or f'CN-{len(self.credit_notes) + 1:04d}'
        if any(c['CreditNoteNumber'] == number for c in self.credit_notes.values()):
            errors.append('Credit Note # must be unique.')
        if errors:
            return errors
        cn = {'CreditNoteID': str(uuid.uuid4()), 'Type': 'ACCRECCREDIT', 'CreditNoteNumber': number,
              'Reference': el.get('Reference', ''), 'Contact': {'ContactID': contact['ContactID'], 'Name': contact['Name']},
              'DateString': el.get('Date'), 'Status': el.get('Status', 'DRAFT'), 'LineAmountTypes': 'NoTax',
              'LineItems': lines, 'CurrencyCode': 'USD', 'SubTotal': num(total), 'TotalTax': 0.0, 'Total': num(total),
              'RemainingCredit': num(total), 'Payments': []}
        self.credit_notes[cn['CreditNoteID']] = cn
        return cn

    def put_payment(self, el):
        if 'Invoice' in el:
            doc, due_key, kind = self.invoices.get(el['Invoice'].get('InvoiceID')), 'AmountDue', 'ACCRECPAYMENT'
        else:
            doc, due_key, kind = self.credit_notes.get((el.get('CreditNote') or {}).get('CreditNoteID')), 'RemainingCredit', 'ARCREDITPAYMENT'
        errors = []
        amount = money(el.get('Amount', 0))
        if doc is None:
            errors.append('Invoice or CreditNote could not be found.')
        elif doc['Status'] != 'AUTHORISED':
            errors.append(f"Payments can only be applied to AUTHORISED documents, this one is {doc['Status']}.")
        elif amount > money(doc[due_key]):
            errors.append('Payment amount exceeds the amount outstanding on this document.')
        if (el.get('Account') or {}).get('Code') not in BANK_CODES:
            errors.append('Payments can only be made to BANK accounts.')
        if amount <= 0:
            errors.append('Payment amount must be greater than zero.')
        if errors:
            return errors
        p = {'PaymentID': str(uuid.uuid4()), 'PaymentType': kind, 'Status': 'AUTHORISED', 'Amount': num(amount),
             'DateString': el.get('Date'), 'Reference': el.get('Reference', '')}
        if kind == 'ACCRECPAYMENT':
            p['Invoice'] = {'InvoiceID': doc['InvoiceID'], 'InvoiceNumber': doc['InvoiceNumber']}
        else:
            p['CreditNote'] = {'CreditNoteID': doc['CreditNoteID'], 'CreditNoteNumber': doc['CreditNoteNumber']}
        self.payments[p['PaymentID']] = p
        doc[due_key] = num(money(doc[due_key]) - amount)
        if 'AmountPaid' in doc:
            doc['AmountPaid'] = num(money(doc['AmountPaid']) + amount)
        doc['Payments'].append({'PaymentID': p['PaymentID'], 'Amount': p['Amount']})
        if doc[due_key] == 0:
            doc['Status'] = 'PAID'
        return p

    def batch(self, key, elements, create, summarize):
        results, failed = [], False
        for el in elements:
            r = create(el)
            if isinstance(r, list):
                failed = True
                results.append({**el, 'StatusAttributeString': 'ERROR', 'ValidationErrors': [{'Message': m} for m in r]})
            else:
                results.append({**r, 'StatusAttributeString': 'OK'})
        # with summarizeErrors=true Xero still creates the valid elements, which ones exactly isn't
        # modelled here. the sync always sends summarizeErrors=false
        if failed and summarize:
            return 400, {'ErrorNumber': 10, 'Type': 'ValidationException',
                         'Message': 'A validation exception occurred', 'Elements': results}
        return 200, {key: results}

    def route(self, method, path, q, body):
        summarize = q.get('summarizeErrors', ['true'])[0].lower() != 'false'
        res = path.rstrip('/').split('/')[-1]
        if method == 'GET' and res == 'Organisation':
            return 200, {'Organisations': [ORG]}
        if method == 'GET' and res == 'Accounts':
            return 200, {'Accounts': ACCOUNTS}
        if method == 'GET' and res == 'Contacts':
            return 200, self.get_contacts(q)
        if method == 'GET' and res == 'Invoices':
            numbers = set(q.get('InvoiceNumbers', [''])[0].split(',')) - {''}
            refs = {v for f, v in where_terms(q.get('where', [''])[0]) if f == 'Reference'}
            return 200, {'Invoices': [i for i in self.invoices.values()
                                      if i['InvoiceNumber'] in numbers or i['Reference'] in refs]}
        if method == 'GET' and res == 'CreditNotes':
            refs = {v for f, v in where_terms(q.get('where', [''])[0]) if f == 'Reference'}
            return 200, {'CreditNotes': [c for c in self.credit_notes.values() if c['Reference'] in refs]}
        creators = {'Contacts': self.put_contact, 'Invoices': self.put_invoice,
                    'CreditNotes': self.put_credit_note, 'Payments': self.put_payment}
        if method == 'PUT' and res in creators:
            elements = body.get(res) if isinstance(body, dict) and res in body else [body]
            if not isinstance(elements, list) or not all(isinstance(e, dict) for e in elements):
                return 400, {'Type': 'PostDataInvalidException', 'Message': 'Invalid JSON body.'}
            return self.batch(res, elements, creators[res], summarize)
        return 404, {'Title': 'NotFound', 'Detail': f'{method} {path} is not implemented by the mock'}

    def handle(self, method, path, q, raw, headers):
        """one Xero API call: auth, chaos, limits, idempotency, then the route. returns (status, headers, body)"""
        auth = headers.get('Authorization', '')
        with self.lock:
            self.stats['calls'] += 1
            exp = self.tokens.get(auth.removeprefix('Bearer '))
            if exp is None or exp < time.time():
                self.stats['401'] += 1
                return 401, {'WWW-Authenticate': 'Bearer error="invalid_token"'}, {
                    'Title': 'Unauthorized', 'Status': 401, 'Detail': 'TokenExpired' if exp else 'AuthenticationUnsuccessful'}
            limited = self.admit()
            if limited:
                self.stats['429 ' + limited[1]] += 1
                return 429, {**limited[0], 'X-Rate-Limit-Problem': limited[1], **self.remaining()}, {
                    'Title': 'Too Many Requests', 'Detail': f'{limited[1]} limit exceeded'}
        try:
            time.sleep(self.latency)
            with self.lock:
                if self.chaos['fail'] > 0:
                    self.chaos['fail'] -= 1
                    self.stats[f'chaos {self.chaos["status"]}'] += 1
                    return self.chaos['status'], self.remaining(), {'Title': 'Service Unavailable', 'Status': self.chaos['status']}
                key = headers.get('Idempotency-Key')
                fingerprint = hashlib.sha256(f'{method} {path} {sorted(q.items())} '.encode() + raw).hexdigest()
                if key and method in ('POST', 'PUT'):
                    if len(key) > 128:
                        return 400, self.remaining(), {'Message': f'Idempotency Key: {key} with length {len(key)} is too long.'}
                    cached = self.idem.get(key)
                    if cached and cached[0] > time.time():
                        if cached[1] != fingerprint:
                            return 400, self.remaining(), {'Message': f'Idempotency Key: {key} is used with a different request.'}
                        self.stats['idempotent replays'] += 1
                        return cached[2], self.remaining(), cached[3]
                try:
                    body = json.loads(raw) if raw else None
                except ValueError:
                    return 400, self.remaining(), {'Type': 'PostDataInvalidException', 'Message': 'Invalid JSON body.'}
                status, out = self.route(method, path, q, body)
                if key and method in ('POST', 'PUT'):
                    self.idem[key] = (time.time() + IDEMPOTENCY_TTL, fingerprint, status, out)
                res = path.rsplit('/', 1)[-1]
                self.stats[f'{method} {res} {status}'] += 1
                if method == 'PUT' and res in self.chaos['lose']:
                    # Xero saved it but the caller never hears back (gateway timeout, dropped connection)
                    self.chaos['lose'].remove(res)
                    self.stats[f'chaos lost {res} response'] += 1
                    return 504, self.remaining(), {'Title': 'Gateway Timeout', 'Status': 504}
                return status, self.remaining(), out
        finally:
            with self.lock:
                self.in_flight -= 1

    def state(self):
        with self.lock:
            return {'contacts': list(self.contacts.values()), 'invoices': list(self.invoices.values()),
                    'credit_notes': list(self.credit_notes.values()), 'payments': list(self.payments.values()),
                    'alerts': list(self.alerts), 'calls': dict(self.stats), 'day_calls': self.day_calls}


def make_handler(xero, stripe_state):
    events = sorted(stripe_state.get('events', []), key=lambda e: (-e['created'], e['id']))  # newest first
    objects = {e['data']['object']['id']: e['data']['object'] for e in reversed(events)}  # the latest state wins

    def stripe_get(path, q):
        """Stripe reads the sync makes, answered from --stripe-state"""
        one = lambda k: q.get(k, [None])[0]
        if path == '/v1/invoice_payments':
            pi, inv = one('payment[payment_intent]'), one('invoice')
            data = [p for p in stripe_state.get('invoice_payments', [])
                    if (pi and p['payment']['payment_intent'] == pi) or (inv and p['invoice'] == inv)]
            return 200, {'object': 'list', 'data': data, 'has_more': False, 'url': path}
        if path == '/v1/events':
            types = set(q.get('types[]', []) + q.get('type', []))
            found = [e for e in events if (not types or e['type'] in types) and e['created'] >= int(one('created[gte]') or 0)]
            ids = [e['id'] for e in found]
            found = found[ids.index(one('starting_after')) + 1:] if one('starting_after') in ids else found
            limit = min(int(one('limit') or 10), 100)
            return 200, {'object': 'list', 'data': found[:limit], 'has_more': len(found) > limit, 'url': path}
        m = re.fullmatch(r'/v1/(invoice|refund)s/(\w+)', path)
        if m and objects.get(m[2], {}).get('object') == m[1]:
            return 200, objects[m[2]]
        return 404, {'error': {'type': 'invalid_request_error', 'code': 'resource_missing', 'message': f'No such object: {path}'}}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def reply(self, status, body, headers=None):
            data = json.dumps(body).encode()
            self.send_response(status)
            for k, v in {'Content-Type': 'application/json; charset=utf-8', **(headers or {})}.items():
                self.send_header(k, v)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt, *args):
            pass

        def dispatch(self, method):
            url = urlsplit(self.path)
            q = parse_qs(url.query)
            raw = self.rfile.read(int(self.headers.get('Content-Length') or 0))
            if url.path == '/xero/token' and method == 'POST':
                form = parse_qs(raw.decode())
                if form.get('grant_type') != ['client_credentials'] or not self.headers.get('Authorization', '').startswith('Basic '):
                    return self.reply(400, {'error': 'invalid_client'})
                token = uuid.uuid4().hex
                with xero.lock:
                    xero.tokens[token] = time.time() + xero.token_ttl
                    xero.stats['tokens issued'] += 1
                return self.reply(200, {'access_token': token, 'expires_in': xero.token_ttl, 'token_type': 'Bearer',
                                        'scope': form.get('scope', [''])[0]})
            if url.path.startswith('/xero/api.xro/2.0/'):
                status, headers, body = xero.handle(method, url.path, q, raw, self.headers)
                return self.reply(status, body, headers)
            if url.path.startswith('/stripe/v1/') and method == 'GET':
                if not self.headers.get('Authorization', '').startswith('Bearer sk_test_'):
                    return self.reply(401, {'error': {'type': 'invalid_request_error', 'message': 'Invalid API Key'}})
                return self.reply(*stripe_get(url.path.removeprefix('/stripe'), q))
            if url.path.startswith('/telegram/bot') and url.path.endswith('/sendMessage') and method == 'POST':
                msg = json.loads(raw or b'{}')
                if not msg.get('chat_id') or not msg.get('text'):
                    return self.reply(400, {'ok': False, 'error_code': 400, 'description': 'Bad Request: chat_id and text are required'})
                with xero.lock:
                    xero.alerts.append({'chat_id': msg['chat_id'], 'text': msg['text']})
                    n = len(xero.alerts)
                return self.reply(200, {'ok': True, 'result': {'message_id': n, 'date': int(time.time()),
                                                               'chat': {'id': msg['chat_id'], 'type': 'private'},
                                                               'text': msg['text']}})
            if url.path == '/_mock/state':
                return self.reply(200, xero.state())
            if url.path == '/_mock/chaos' and method == 'POST':
                with xero.lock:
                    xero.chaos.update(json.loads(raw or b'{}'))
                return self.reply(200, xero.chaos)
            if url.path == '/_mock/reset' and method == 'POST':
                with xero.lock:
                    xero.reset()
                return self.reply(200, {'reset': True})
            return self.reply(404, {'error': f'no mock route for {method} {url.path}'})

        def do_GET(self):
            self.dispatch('GET')

        def do_POST(self):
            self.dispatch('POST')

        def do_PUT(self):
            self.dispatch('PUT')

    return Handler


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--port', type=int, default=8765)
    ap.add_argument('--day-limit', type=int, default=1000)
    ap.add_argument('--minute-limit', type=int, default=60, help='lower it to watch the sync back off')
    ap.add_argument('--concurrent-limit', type=int, default=5)
    ap.add_argument('--token-ttl', type=int, default=1800)
    ap.add_argument('--latency-ms', type=int, default=80, help='per Xero call, so concurrency limits can bite')
    ap.add_argument('--stripe-state', help='JSON with invoice_payments, written by fixtures.py')
    args = ap.parse_args()
    state = json.load(open(args.stripe_state)) if args.stripe_state else {}
    xero = Xero(args.day_limit, args.token_ttl, args.latency_ms / 1000, args.minute_limit, args.concurrent_limit)
    server = ThreadingHTTPServer(('127.0.0.1', args.port), make_handler(xero, state))
    print(f'mock Xero/Stripe on http://127.0.0.1:{args.port}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
