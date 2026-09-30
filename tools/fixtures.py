#!/usr/bin/env python3
"""Stripe test data in the shape of API version 2026-08-26.dahlia, plus a check of what got to Xero

    fixtures.py generate [--invoices 300] [--seed 7] [--out fixtures]
        events.jsonl       webhook events for replay.py (invoice.paid, refund.created, invoice.payment_failed)
        stripe_state.json  what mock_apis.py answers for Stripe: invoice_payments, the event log, objects
        expected.json      where the sync must end up: event statuses, contacts, invoices, credit notes
    fixtures.py verify [--out fixtures] [--mock http://127.0.0.1:8765] [--db stripe_xero_dev]
        compares expected.json with the mock's Xero state and the event table, exit 1 on any diff

besides the random bulk there are always the nasty cases: two customers with one name, non-ASCII and
missing names, discounts, many lines, a $0 invoice, tax, a foreign currency, an invoice paid partly
from customer balance, two events for one invoice, partial refunds, a refund that comes before its
invoice, a refund with no invoice and a failed refund. stdlib only
"""
import argparse
import json
import random
import string
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

API_VERSION = '2026-08-26.dahlia'
PRODUCTS = [('Starter plan', 1900), ('Pro plan', 4900), ('Team plan', 14900), ('Onboarding session', 25000),
            ('Extra seat', 1200), ('Priority support', 9900)]
NAMES = ['Ava Thompson', 'Liam Carter', 'Mia Rossi', 'Noah Becker', 'Emma Novak', 'Lucas Martin', 'Chloe Dubois',
         'Ethan Walsh', 'Sofia Lindqvist', 'Oliver Grant', 'Isla Murphy', 'Leo Fischer', 'Harbor & Pine LLC',
         'Northwind Traders', 'Blue Fern Studio', 'Quillon Analytics', 'Maple Street Dental', 'Orbit Labs Inc.']


class Gen:
    def __init__(self, seed):
        self.rnd = random.Random(seed)
        self.now = int(time.time())
        self.events, self.payments, self.customers = [], [], []
        self.expect = {'events': {}, 'contacts': {}, 'invoices': {}, 'credit_notes': {}}

    def id(self, prefix, n=24):
        return prefix + ''.join(self.rnd.choices(string.ascii_letters + string.digits, k=n))

    def customer(self, name, email=None):
        c = {'id': self.id('cus_', 14), 'name': name, 'email': email, 'prefix': ''.join(
            self.rnd.choices('0123456789ABCDEF', k=8)), 'seq': 0}
        self.customers.append(c)
        return c

    def event(self, type_, obj, created, status='done', error=None):
        evt = {'id': self.id('evt_'), 'object': 'event', 'api_version': API_VERSION, 'created': created,
               'data': {'object': obj}, 'livemode': False, 'pending_webhooks': 1,
               'request': {'id': self.id('req_', 14), 'idempotency_key': None}, 'type': type_}
        self.events.append(evt)
        self.expect['events'][evt['id']] = {'status': status, **({'error': error} if error else {})}
        return evt

    def line(self, inv_id, desc, amount, discount=0, created=0):
        return {'id': self.id('il_'), 'object': 'line_item', 'amount': amount, 'currency': 'usd', 'description': desc,
                'discount_amounts': [{'amount': discount, 'discount': self.id('di_')}] if discount else [],
                'discountable': True, 'discounts': [], 'invoice': inv_id, 'livemode': False, 'metadata': {},
                'parent': {'type': 'invoice_item_details', 'invoice_item_details': {
                    'invoice_item': self.id('ii_'), 'proration': False, 'proration_details': {'credited_items': None},
                    'subscription': None}, 'subscription_item_details': None},
                'period': {'start': created, 'end': created}, 'pretax_credit_amounts': [],
                'pricing': {'type': 'price_details', 'price_details': {'price': self.id('price_'),
                                                                       'product': self.id('prod_', 14)},
                            'unit_amount_decimal': str(amount)},
                'quantity': 1, 'taxes': []}

    def invoice(self, cust, items, *, status='paid', currency='usd', tax=0, balance=0, has_more=False, created=None):
        """items: [(description, amount, discount)], returns the invoice object"""
        created = created or self.now - self.rnd.randint(3600, 20 * 86400)
        cust['seq'] += 1
        inv_id = self.id('in_')
        lines = [self.line(inv_id, d, a, disc, created) for d, a, disc in items]
        subtotal = sum(a for _, a, _ in items)
        total = subtotal - sum(disc for _, _, disc in items) + tax
        paid = status == 'paid'
        inv = {
            'id': inv_id, 'object': 'invoice', 'account_country': 'US', 'account_name': 'Nightloom Test',
            'amount_due': total + balance, 'amount_overpaid': 0, 'amount_paid': total + balance if paid else 0,
            'amount_remaining': 0 if paid else total + balance, 'amount_shipping': 0,
            'attempt_count': 1 if paid else self.rnd.randint(1, 3), 'attempted': True, 'auto_advance': not paid,
            'billing_reason': 'manual', 'collection_method': 'charge_automatically', 'created': created,
            'currency': currency, 'customer': cust['id'], 'customer_address': None, 'customer_email': cust['email'],
            'customer_name': cust['name'], 'customer_phone': None, 'customer_shipping': None,
            'customer_tax_exempt': 'none', 'default_payment_method': None, 'description': None, 'discounts': [],
            'due_date': None, 'effective_at': created, 'ending_balance': 0 if balance else None, 'footer': None,
            'hosted_invoice_url': f'https://invoice.stripe.com/i/acct_test/test_{inv_id[3:]}',
            'invoice_pdf': f'https://pay.stripe.com/invoice/acct_test/test_{inv_id[3:]}/pdf',
            'issuer': {'type': 'self'}, 'last_finalization_error': None, 'latest_revision': None,
            'lines': {'object': 'list', 'data': lines[:10] if has_more else lines, 'has_more': has_more,
                      'total_count': len(lines), 'url': f'/v1/invoices/{inv_id}/lines'},
            'livemode': False, 'metadata': {},
            'next_payment_attempt': None if paid else created + 3 * 86400,
            'number': f"{cust['prefix']}-{cust['seq']:04d}", 'on_behalf_of': None, 'parent': None,
            'payment_settings': {'default_mandate': None, 'payment_method_options': None, 'payment_method_types': None},
            'period_end': created, 'period_start': created, 'post_payment_credit_notes_amount': 0,
            'pre_payment_credit_notes_amount': 0, 'receipt_number': None, 'rendering': None, 'shipping_cost': None,
            'shipping_details': None, 'starting_balance': balance, 'statement_descriptor': None, 'status': status,
            'status_transitions': {'finalized_at': created, 'marked_uncollectible_at': None,
                                   'paid_at': created + 5 if paid else None, 'voided_at': None},
            'subtotal': subtotal, 'subtotal_excluding_tax': subtotal, 'test_clock': None, 'total': total,
            'total_discount_amounts': [{'amount': disc, 'discount': self.id('di_')} for _, _, disc in items if disc],
            'total_excluding_tax': total - tax, 'total_pretax_credit_amounts': [],
            'total_taxes': [{'amount': tax, 'tax_behavior': 'exclusive', 'tax_rate_details': {
                'tax_rate': self.id('txr_')}, 'taxability_reason': 'standard_rated', 'taxable_amount': subtotal,
                'type': 'tax_rate_details'}] if tax else [],
            'webhooks_delivered_at': created + 6}
        if paid:
            inv['_pi'] = self.id('pi_')
            self.payments.append({
                'id': self.id('inpay_'), 'object': 'invoice_payment', 'amount_paid': inv['amount_paid'],
                'amount_requested': inv['amount_paid'], 'created': created + 5, 'currency': currency,
                'invoice': inv_id, 'is_default': True, 'livemode': False,
                'payment': {'type': 'payment_intent', 'payment_intent': inv['_pi']}, 'status': 'paid',
                'status_transitions': {'canceled_at': None, 'paid_at': created + 5}})
        return inv

    def paid(self, cust, items, lines_expected=None, **kw):
        """invoice.paid event that must end up as a PAID Xero invoice"""
        inv = self.invoice(cust, items, **kw)
        self.event('invoice.paid', public(inv), inv['status_transitions']['paid_at'])
        self.expect['invoices'][inv['number']] = {
            'Reference': inv['id'], 'Total': inv['total'] / 100, 'Status': 'PAID', 'AmountDue': 0.0,
            'lines': lines_expected or len(items), 'contact': cust['id']}
        self.expect['contacts'][cust['id']] = contact_name(cust)
        return inv

    def refund(self, inv, amount, *, status='succeeded', before_invoice=False):
        created = inv['status_transitions']['paid_at'] - 60 if before_invoice else \
            inv['status_transitions']['paid_at'] + self.rnd.randint(600, 86400)
        re = {'id': self.id('re_'), 'object': 'refund', 'amount': amount, 'balance_transaction': self.id('txn_'),
              'charge': self.id('ch_'), 'created': created, 'currency': inv['currency'], 'customer': inv['customer'],
              'destination_details': {'type': 'card', 'card': {'reference_status': 'pending', 'type': 'refund'}},
              'metadata': {}, 'payment_intent': inv.get('_pi') or self.id('pi_'), 'payment_method': self.id('pm_'),
              'reason': 'requested_by_customer', 'receipt_number': None, 'source_transfer_reversal': None,
              'status': status, 'transfer_reversal': None}
        self.event('refund.created', re, created)
        if status in ('succeeded', 'pending') and inv.get('_pi'):
            self.expect['credit_notes'][re['id']] = {'Total': amount / 100, 'Status': 'PAID',
                                                     'contact': inv['customer']}
        return re

    def build(self, n_invoices):
        rnd = self.rnd
        # the nasty cases
        smith_a, smith_b = self.customer('John Smith', 'john@smith.example'), self.customer('John Smith', 'js@example.org')
        self.paid(smith_a, [('Pro plan', 4900, 0)])
        self.paid(smith_b, [('Starter plan', 1900, 0)])
        for name in ('Zoë Ångström', 'Łukasz Żółć', '山田 太郎', 'José Núñez-Ürquiza', 'Café ☕ “Quotes” Ltd',
                     'Anne-Marie O\'Neil'):
            self.paid(self.customer(name, None), [('Team plan', 14900, 0)])
        self.paid(self.customer(None, 'no-name@example.com'), [('Extra seat', 1200, 0)])
        self.paid(self.customer(None, None), [('Extra seat', 1200, 0)])
        self.paid(self.customer('X' * 300, None), [('Extra seat', 1200, 0)])  # Xero names max 255
        disc = self.customer('Discount Dana', 'dana@example.com')
        self.paid(disc, [('Pro plan', 4900, 980), ('Extra seat', 1200, 0)])  # 20 % off one line
        multi = self.customer('Multi Line Corp', 'ap@multiline.example')
        self.paid(multi, [(d, a, 0) for d, a in PRODUCTS[:4]])
        self.paid(multi, [(f'Usage {i}', 100 + i, 0) for i in range(14)], lines_expected=1, has_more=True)

        odd = self.customer('Edge Case Emma', 'emma@example.com')
        zero = self.invoice(odd, [('Trial month', 0, 0)])
        self.event('invoice.paid', public(zero), zero['created'] + 5)  # $0: nothing to post, done
        tax = self.invoice(odd, [('Pro plan', 4900, 0)], tax=392)
        self.event('invoice.paid', public(tax), tax['created'] + 5, 'dead', 'PERMANENT: invoice has tax')
        eur = self.invoice(odd, [('Pro plan', 4900, 0)], currency='eur')
        self.event('invoice.paid', public(eur), eur['created'] + 5, 'dead', 'PERMANENT: currency EUR')
        bal = self.invoice(odd, [('Team plan', 14900, 0)], balance=-5000)
        self.event('invoice.paid', public(bal), bal['created'] + 5, 'dead', 'PERMANENT: paid partly')

        twice = self.paid(self.customer('Double Event Dan', 'dan@example.com'), [('Pro plan', 4900, 0)])
        self.event('invoice.paid', public(twice), twice['created'] + 9)  # second event for the same invoice

        refunded = self.paid(self.customer('Refund Rita', 'rita@example.com'), [('Team plan', 14900, 0)])
        self.refund(refunded, 5000)
        self.refund(refunded, 2500)
        early = self.paid(self.customer('Early Refund Eli', 'eli@example.com'), [('Pro plan', 4900, 0)])
        self.refund(early, 4900, before_invoice=True)  # comes first, has to wait for the invoice
        self.refund(self.paid(self.customer('Failed Refund Fay', None), [('Pro plan', 4900, 0)]), 4900,
                    status='failed')
        stray = {'currency': 'usd', 'customer': odd['id'], 'status_transitions': {'paid_at': self.now - 7200}}
        self.refund(stray, 3000)  # PaymentIntent with no invoice, nothing to credit, done

        # the bulk
        pool = [self.customer(n, n.lower().replace(' ', '.').replace('&', 'and') + '@example.com') for n in NAMES]
        paid = []
        for _ in range(n_invoices):
            c = rnd.choice(pool)
            items = [(d, a, rnd.choice([0, 0, 0, a // 10])) for d, a in rnd.sample(PRODUCTS, rnd.randint(1, 3))]
            paid.append(self.paid(c, items))
        for inv in rnd.sample(paid, len(paid) // 10):
            self.refund(inv, rnd.choice([inv['total'], inv['total'] // 2]))
        for _ in range(max(1, n_invoices // 20)):
            inv = self.invoice(rnd.choice(pool), [rnd.choice(PRODUCTS) + (0,)], status='open')
            self.event('invoice.payment_failed', public(inv), inv['created'] + 60)


def public(inv):
    return {k: v for k, v in inv.items() if not k.startswith('_')}


def contact_name(c):
    """contact name the sync must give Xero: Stripe name or email, tagged with the customer id"""
    base = c['name'] or c['email']
    name = f"{base} ({c['id']})" if base else c['id']
    return name if len(name) <= 255 else base[:255 - len(c['id']) - 4] + f"… ({c['id']})"


def generate(args):
    g = Gen(args.seed)
    g.build(args.invoices)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    events = sorted(g.events, key=lambda e: e['created'])
    with open(out / 'events.jsonl', 'w', encoding='utf-8') as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + '\n')
    (out / 'stripe_state.json').write_text(json.dumps({'invoice_payments': g.payments, 'events': events}, indent=1))
    (out / 'expected.json').write_text(json.dumps(g.expect, indent=1, ensure_ascii=False))
    types = Counter(e['type'] for e in events)
    print(f'{len(events)} events ({", ".join(f"{n} {t}" for t, n in types.items())}) in {out}/')


def verify(args):
    out = Path(args.out)
    exp = json.loads((out / 'expected.json').read_text())
    with urllib.request.urlopen(args.mock + '/_mock/state') as r:
        xero = json.load(r)
    rows = subprocess.run(['psql', '-X', '-A', '-t', '-d', args.db, '-c',
                           "SELECT coalesce(json_object_agg(id, json_build_object('status', status, 'error', "
                           "last_error, 'attempts', attempts)), '{}') FROM stripe_event"],
                          check=True, capture_output=True, text=True).stdout
    events = json.loads(rows)
    problems = []

    for eid, want in exp['events'].items():
        got = events.get(eid)
        if not got:
            problems.append(f'event {eid}: not in the event table')
        elif got['status'] != want['status'] or (want.get('error') and want['error'] not in (got['error'] or '')):
            problems.append(f"event {eid}: want {want}, got {got['status']} {got['error']!r}")

    by_number = {}
    for c in xero['contacts']:
        by_number.setdefault(c.get('ContactNumber'), []).append(c)
    for cus, name in exp['contacts'].items():
        found = by_number.get(cus, [])
        if len(found) != 1 or found[0]['Name'] != name:
            problems.append(f'contact {cus}: want one named {name!r}, got {[c["Name"] for c in found]}')
    extra = set(by_number) - set(exp['contacts'])
    if extra:
        problems.append(f'contacts nobody asked for: {sorted(extra)[:5]}')

    contact_ids = {c['ContactID']: c.get('ContactNumber') for c in xero['contacts']}
    invoices = Counter(i['InvoiceNumber'] for i in xero['invoices'])
    for number, want in exp['invoices'].items():
        found = [i for i in xero['invoices'] if i['InvoiceNumber'] == number]
        if invoices[number] != 1:
            problems.append(f'invoice {number}: {invoices[number]} copies in Xero')
            continue
        i = found[0]
        got = {'Reference': i['Reference'], 'Total': i['Total'], 'Status': i['Status'], 'AmountDue': i['AmountDue'],
               'lines': len(i['LineItems']), 'contact': contact_ids.get(i['Contact']['ContactID'])}
        if got != want:
            problems.append(f'invoice {number}: want {want}, got {got}')
    extra = set(invoices) - set(exp['invoices'])
    if extra:
        problems.append(f'invoices nobody asked for: {sorted(extra)[:5]}')

    notes = Counter(c['Reference'] for c in xero['credit_notes'])
    for ref, want in exp['credit_notes'].items():
        found = [c for c in xero['credit_notes'] if c['Reference'] == ref]
        if notes[ref] != 1:
            problems.append(f'credit note {ref}: {notes[ref]} copies in Xero')
            continue
        c = found[0]
        got = {'Total': c['Total'], 'Status': c['Status'], 'contact': contact_ids.get(c['Contact']['ContactID'])}
        if got != want:
            problems.append(f'credit note {ref}: want {want}, got {got}')
    extra = set(notes) - set(exp['credit_notes'])
    if extra:
        problems.append(f'credit notes nobody asked for: {sorted(extra)[:5]}')

    statuses = Counter(e['status'] for e in events.values())
    calls = xero['calls']
    print(json.dumps({'events': dict(statuses), 'xero': {k: len(xero[k]) for k in
                                                          ('contacts', 'invoices', 'credit_notes', 'payments')},
                      'xero_calls': calls.get('calls', 0), 'rate_limited': {k: v for k, v in calls.items()
                                                                             if k.startswith('429')},
                      'idempotent_replays': calls.get('idempotent replays', 0)}, indent=2, ensure_ascii=False))
    for p in problems[:40]:
        print('  ' + p)
    if problems:
        raise SystemExit(f'{len(problems)} differences')
    print('Xero matches Stripe: no duplicates, nothing missing')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    g = sub.add_parser('generate')
    g.add_argument('--invoices', type=int, default=300)
    g.add_argument('--seed', type=int, default=7)
    g.add_argument('--out', default='fixtures')
    v = sub.add_parser('verify')
    v.add_argument('--out', default='fixtures')
    v.add_argument('--mock', default='http://127.0.0.1:8765')
    v.add_argument('--db', default='stripe_xero_dev')
    args = ap.parse_args()
    generate(args) if args.cmd == 'generate' else verify(args)


if __name__ == '__main__':
    sys.exit(main())
