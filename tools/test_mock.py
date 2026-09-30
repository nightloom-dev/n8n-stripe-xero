#!/usr/bin/env python3
"""self-check for mock_apis.py, the Xero behaviour the sync relies on. run: python3 test_mock.py"""
import base64
import json
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer

from mock_apis import Xero, make_handler

xero = Xero(day_limit=1000, token_ttl=1800, latency=0)
EVENTS = [{'id': f'evt_{i}', 'type': 'refund.created' if i % 3 == 0 else 'invoice.paid', 'created': 1000 + i,
           'data': {'object': {'id': f'in_{i}', 'object': 'invoice', 'status': 'paid'} if i % 3 else
                    {'id': f're_{i}', 'object': 'refund', 'status': 'succeeded'}}} for i in range(1, 8)]
EVENTS.append({'id': 'evt_8', 'type': 'invoice.paid', 'created': 900,  # older news about in_1
               'data': {'object': {'id': 'in_1', 'object': 'invoice', 'status': 'open'}}})
server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(xero, {'invoice_payments': [
    {'id': 'inpay_1', 'invoice': 'in_1', 'payment': {'type': 'payment_intent', 'payment_intent': 'pi_1'}}],
    'events': EVENTS}))
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = f'http://127.0.0.1:{server.server_port}'
X = BASE + '/xero/api.xro/2.0'


def call(method, url, body=None, headers=None, form=None):
    data = form.encode() if form else None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, dict(r.headers), json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), json.loads(e.read())


basic = 'Basic ' + base64.b64encode(b'client:secret').decode()
st, _, tok = call('POST', BASE + '/xero/token', form='grant_type=client_credentials&scope=accounting.contacts',
                  headers={'Authorization': basic, 'Content-Type': 'application/x-www-form-urlencoded'})
assert st == 200 and tok['token_type'] == 'Bearer'
H = {'Authorization': 'Bearer ' + tok['access_token'], 'Content-Type': 'application/json'}

assert call('GET', X + '/Organisation')[0] == 401
st, hdr, _ = call('GET', X + '/Organisation', headers=H)
assert st == 200 and hdr['X-MinLimit-Remaining'] == '59'

# contacts: create only, unique names and numbers, per-element errors with summarizeErrors=false
st, _, r = call('PUT', X + '/Contacts?summarizeErrors=false', {'Contacts': [
    {'Name': 'Zoë Ångström', 'ContactNumber': 'cus_1'},
    {'Name': 'zoë ångström', 'ContactNumber': 'cus_2'}]}, H)
assert st == 200 and [c['StatusAttributeString'] for c in r['Contacts']] == ['OK', 'ERROR'], r
contact = r['Contacts'][0]['ContactID']
st, _, r = call('PUT', X + '/Contacts', {'Contacts': [{'Name': 'Other', 'ContactNumber': 'cus_1'}]}, H)
assert st == 400 and r['Type'] == 'ValidationException'
st, _, r = call('GET', X + '/Contacts?where=' + urllib.request.quote('ContactNumber=="cus_1" OR Name=="nobody"'),
                headers=H)
assert [c['ContactID'] for c in r['Contacts']] == [contact]

# invoice, paying it in full makes it PAID, a second payment is rejected
inv = {'Type': 'ACCREC', 'InvoiceNumber': 'in_1', 'Reference': 'in_1', 'Contact': {'ContactID': contact},
       'Status': 'AUTHORISED', 'LineAmountTypes': 'NoTax', 'CurrencyCode': 'USD',
       'LineItems': [{'Description': 'Plan', 'Quantity': 1, 'UnitAmount': 49.5, 'AccountCode': '200'}]}
st, _, r = call('PUT', X + '/Invoices?summarizeErrors=false', {'Invoices': [
    inv, {**inv, 'InvoiceNumber': 'in_eur', 'CurrencyCode': 'EUR'}, inv]}, H)
assert [i['StatusAttributeString'] for i in r['Invoices']] == ['OK', 'ERROR', 'ERROR'], r
invoice = r['Invoices'][0]['InvoiceID']
pay = {'Invoice': {'InvoiceID': invoice}, 'Account': {'Code': '090'}, 'Amount': 49.5, 'Date': '2026-09-27'}
st, _, r = call('PUT', X + '/Payments?summarizeErrors=false', {'Payments': [pay, pay]}, H)
assert [p['StatusAttributeString'] for p in r['Payments']] == ['OK', 'ERROR'], r
st, _, r = call('GET', X + '/Invoices?InvoiceNumbers=in_1,in_x', headers=H)
assert [(i['Status'], i['AmountDue']) for i in r['Invoices']] == [('PAID', 0.0)]

# refund: credit note plus a cash refund against it
cn = {'Type': 'ACCRECCREDIT', 'Reference': 're_1', 'Contact': {'ContactID': contact}, 'Status': 'AUTHORISED',
      'LineAmountTypes': 'NoTax', 'LineItems': [{'Description': 'Refund', 'Quantity': 1, 'UnitAmount': 20,
                                                'AccountCode': '200'}]}
st, _, r = call('PUT', X + '/CreditNotes?summarizeErrors=false', {'CreditNotes': [cn]}, H)
note = r['CreditNotes'][0]['CreditNoteID']
st, _, r = call('PUT', X + '/Payments?summarizeErrors=false', {'Payments': [
    {'CreditNote': {'CreditNoteID': note}, 'Account': {'Code': '090'}, 'Amount': 20}]}, H)
assert r['Payments'][0]['PaymentType'] == 'ARCREDITPAYMENT' and r['Payments'][0]['CreditNote']['CreditNoteID'] == note, r
st, _, r = call('GET', X + '/CreditNotes?where=' + urllib.request.quote('Reference=="re_1"'), headers=H)
assert r['CreditNotes'][0]['Status'] == 'PAID' and r['CreditNotes'][0]['RemainingCredit'] == 0

# idempotency: same key and request replays, same key with another request is a 400
body = {'Contacts': [{'Name': 'Idem', 'ContactNumber': 'cus_idem'}]}
first = call('PUT', X + '/Contacts?summarizeErrors=false', body, {**H, 'Idempotency-Key': 'k1'})
again = call('PUT', X + '/Contacts?summarizeErrors=false', body, {**H, 'Idempotency-Key': 'k1'})
assert first[2] == again[2] and again[2]['Contacts'][0]['StatusAttributeString'] == 'OK'
assert call('PUT', X + '/Contacts', {'Contacts': [{'Name': 'Else'}]}, {**H, 'Idempotency-Key': 'k1'})[0] == 400

# chaos
call('POST', BASE + '/_mock/chaos', {'fail': 1, 'status': 503})
assert call('GET', X + '/Organisation', headers=H)[0] == 503
assert call('GET', X + '/Organisation', headers=H)[0] == 200
call('POST', BASE + '/_mock/chaos', {'lose': ['Contacts']})
body = {'Contacts': [{'Name': 'Lost', 'ContactNumber': 'cus_lost'}]}
assert call('PUT', X + '/Contacts?summarizeErrors=false', body, {**H, 'Idempotency-Key': 'k2'})[0] == 504
st, _, r = call('PUT', X + '/Contacts?summarizeErrors=false', body, {**H, 'Idempotency-Key': 'k2'})
assert st == 200 and r['Contacts'][0]['StatusAttributeString'] == 'OK', r  # saved the first time, the retry is told so

# minute limit: 429 with Retry-After and the problem named
xero.minute_calls = 59
assert call('GET', X + '/Organisation', headers=H)[0] == 200
st, hdr, _ = call('GET', X + '/Organisation', headers=H)
assert st == 429 and hdr['X-Rate-Limit-Problem'] == 'minute' and 0 < int(hdr['Retry-After']) <= 60, hdr

# concurrency: at most 5 in flight, the rest get 429 without Retry-After
xero.minute_calls, xero.latency = 0, 0.5
with ThreadPoolExecutor(8) as pool:
    got = list(pool.map(lambda _: call('GET', X + '/Organisation', headers=H), range(8)))
xero.latency = 0
limited = [h for s, h, _ in got if s == 429]
assert sum(s == 200 for s, _, _ in got) == 5 and len(limited) == 3, [s for s, _, _ in got]
assert all(h['X-Rate-Limit-Problem'] == 'concurrent' and 'Retry-After' not in h for h in limited)

# stripe invoice_payments lookup by payment intent
st, _, r = call('GET', BASE + '/stripe/v1/invoice_payments?payment%5Bpayment_intent%5D=pi_1&payment%5Btype%5D=payment_intent',
                headers={'Authorization': 'Bearer sk_test_x'})
assert [p['invoice'] for p in r['data']] == ['in_1']

# stripe events: filtered by type and time, newest first, paged. objects come in their latest state
SK = {'Authorization': 'Bearer sk_test_x'}
page = lambda after='': call('GET', BASE + '/stripe/v1/events?types%5B%5D=invoice.paid&created%5Bgte%5D=1002&limit=2'
                                          + (f'&starting_after={after}' if after else ''), headers=SK)[2]
first = page()
assert [e['id'] for e in first['data']] == ['evt_7', 'evt_5'] and first['has_more'], first
last = page('evt_4')
assert [e['id'] for e in last['data']] == ['evt_2'] and not last['has_more'], last
assert call('GET', BASE + '/stripe/v1/invoices/in_1', headers=SK)[2]['status'] == 'paid'
assert call('GET', BASE + '/stripe/v1/refunds/re_3', headers=SK)[2]['object'] == 'refund'
assert call('GET', BASE + '/stripe/v1/refunds/in_1', headers=SK)[0] == 404
assert call('GET', BASE + '/stripe/v1/events')[0] == 401

# payments say what they settled, alerts sent to the fake Telegram are kept
st, _, r = call('POST', BASE + '/telegram/botTOKEN/sendMessage', {'chat_id': '1', 'text': 'hello'})
assert st == 200 and r['ok'] and call('GET', BASE + '/_mock/state')[2]['alerts'] == [{'chat_id': '1', 'text': 'hello'}]

server.shutdown()
print('mock tests: OK')
