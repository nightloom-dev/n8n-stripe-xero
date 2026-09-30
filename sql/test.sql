-- self-check for schema.sql, run it on a throwaway db:
--   createdb sx_test && psql -X -q -v ON_ERROR_STOP=1 -d sx_test -f schema.sql -f test.sql; dropdb sx_test

INSERT INTO app_secret VALUES ('stripe_webhook_secret', 'whsec_test') ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value;

CREATE FUNCTION pg_temp.sig(body text, ts bigint, secret text DEFAULT 'whsec_test') RETURNS text
LANGUAGE sql AS $$
    SELECT 't=' || ts || ',v1=' || encode(hmac(convert_to(ts || '.' || body, 'UTF8'), convert_to(secret, 'UTF8'), 'sha256'), 'hex');
$$;
CREATE FUNCTION pg_temp.b64(body text) RETURNS text LANGUAGE sql AS $$ SELECT encode(convert_to(body, 'UTF8'), 'base64') $$;
CREATE FUNCTION pg_temp.now_s() RETURNS bigint LANGUAGE sql AS $$ SELECT extract(epoch FROM now())::bigint $$;

DO $$
DECLARE
    body1 text := '{"id":"evt_1","type":"invoice.paid","data":{"object":{"id":"in_1","customer_name":"Zoë Ångström"}}}';
    r record;
BEGIN
    -- valid, new (non-ASCII body, the signature must cover the exact UTF-8 bytes)
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(body1), pg_temp.sig(body1, pg_temp.now_s()));
    ASSERT r.valid AND r.is_new AND r.event_id = 'evt_1' AND r.reason = 'stored', format('new event: %s', r);

    -- same event again: accepted, not stored twice, counted
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(body1), pg_temp.sig(body1, pg_temp.now_s()));
    ASSERT r.valid AND NOT r.is_new AND r.reason = 'duplicate', format('duplicate: %s', r);
    ASSERT (SELECT redeliveries FROM stripe_event WHERE id = 'evt_1') = 1;
    ASSERT (SELECT count(*) FROM stripe_event) = 1;

    -- wrong secret, tampered body, stale and future timestamps, garbage header
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(body1), pg_temp.sig(body1, pg_temp.now_s(), 'whsec_other'));
    ASSERT NOT r.valid AND r.reason = 'signature mismatch', format('wrong secret: %s', r);
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(replace(body1, 'in_1', 'in_2')), pg_temp.sig(body1, pg_temp.now_s()));
    ASSERT NOT r.valid AND r.reason = 'signature mismatch', format('tampered: %s', r);
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(body1), pg_temp.sig(body1, pg_temp.now_s() - 301));
    ASSERT NOT r.valid AND r.reason = 'timestamp outside tolerance', format('stale: %s', r);
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(body1), pg_temp.sig(body1, pg_temp.now_s() + 301));
    ASSERT NOT r.valid AND r.reason = 'timestamp outside tolerance', format('future: %s', r);
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(body1), 'garbage');
    ASSERT NOT r.valid AND r.reason = 'malformed Stripe-Signature header', format('garbage: %s', r);
    SELECT * INTO r FROM stripe_intake(pg_temp.b64(body1), NULL);
    ASSERT NOT r.valid, 'missing header';

    -- secret rotation: two v1 values, the second one is the valid one
    SELECT * INTO r FROM stripe_intake(
        pg_temp.b64(replace(body1, 'evt_1', 'evt_2')),
        pg_temp.sig(replace(body1, 'evt_1', 'evt_2'), pg_temp.now_s(), 'whsec_old') || ',' ||
        split_part(pg_temp.sig(replace(body1, 'evt_1', 'evt_2'), pg_temp.now_s()), ',', 2));
    ASSERT r.valid AND r.is_new, format('rotation: %s', r);

    -- event types we don't handle are kept for audit but never processed
    SELECT * INTO r FROM stripe_intake(pg_temp.b64('{"id":"evt_3","type":"customer.created","data":{"object":{"id":"cus_1"}}}'),
                                       pg_temp.sig('{"id":"evt_3","type":"customer.created","data":{"object":{"id":"cus_1"}}}', pg_temp.now_s()));
    ASSERT r.valid AND (SELECT status FROM stripe_event WHERE id = 'evt_3') = 'ignored', 'ignored type';
END $$;

-- no secret: must fail closed, never accept
DO $$
BEGIN
    DELETE FROM app_secret;
    BEGIN
        PERFORM stripe_intake(pg_temp.b64('{}'), pg_temp.sig('{}', pg_temp.now_s()));
        RAISE EXCEPTION 'accepted a request without a configured secret';
    EXCEPTION WHEN raise_exception THEN
        ASSERT SQLERRM LIKE 'app_secret%', SQLERRM;
    END;
    INSERT INTO app_secret VALUES ('stripe_webhook_secret', 'whsec_test');
END $$;

DO $$
DECLARE
    r record;
    n int;
BEGIN
    -- lease: one holder at a time, re-entrant for the holder, free after release
    ASSERT take_lease('run-a'), 'a takes lease';
    ASSERT NOT take_lease('run-b'), 'b must wait';
    ASSERT take_lease('run-a'), 'a renews';
    PERFORM release_lease('run-a');
    ASSERT take_lease('run-b'), 'b after release';
    PERFORM release_lease('run-b');

    -- claim: pending events only, each once
    SELECT count(*) INTO n FROM claim_events(10);
    ASSERT n = 2, format('claimed %s, expected evt_1 and evt_2', n);
    SELECT count(*) INTO n FROM claim_events(10);
    ASSERT n = 0, 'already claimed';

    -- success
    SELECT * INTO r FROM finish_event('evt_1');
    ASSERT r.status = 'done', format('done: %s', r);

    -- failure: back to pending with backoff, dead after max_attempts
    SELECT * INTO r FROM finish_event('evt_2', 'boom', NULL, 2);
    ASSERT r.status = 'pending' AND r.attempts = 1, format('retry: %s', r);
    ASSERT (SELECT next_attempt_at > now() + interval '29 seconds' FROM stripe_event WHERE id = 'evt_2'), 'backoff';
    UPDATE stripe_event SET next_attempt_at = now() WHERE id = 'evt_2';
    PERFORM claim_events(10);
    SELECT * INTO r FROM finish_event('evt_2', 'boom again', NULL, 2);
    ASSERT r.status = 'dead' AND r.attempts = 2, format('dead: %s', r);

    -- replay the dead one
    ASSERT (SELECT array_agg(x) FROM replay_dead() x) = ARRAY['evt_2'], 'replay';

    -- rate limit: attempt not counted, everything paused
    PERFORM claim_events(10);
    SELECT * INTO r FROM finish_event('evt_2', 'Xero 429', 60);
    ASSERT r.status = 'pending' AND r.attempts = 0, format('429: %s', r);
    SELECT count(*) INTO n FROM claim_events(10);
    ASSERT n = 0, 'paused';
    UPDATE sync_state SET xero_paused_until = '-infinity';
    SELECT count(*) INTO n FROM claim_events(10);
    ASSERT n = 1, 'resumed';

    -- a run that died, its lock expires and the event gets claimed again
    UPDATE stripe_event SET locked_until = now() - interval '1 second' WHERE id = 'evt_2';
    SELECT count(*) INTO n FROM claim_events(10);
    ASSERT n = 1, 'reclaimed after lock expiry';
END $$;

DO $$
DECLARE
    n   int;
    r   record;
    ctx jsonb;
BEGIN
    DELETE FROM stripe_event;
    UPDATE sync_state SET lease_until = '-infinity', xero_paused_until = '-infinity';
    INSERT INTO stripe_event (id, type, object_id, payload, received_at) VALUES
        ('e1', 'invoice.paid',   'in_1', '{}', now() - interval '5 minutes'),
        ('e2', 'refund.created', 're_1', '{}', now() - interval '4 minutes'),
        ('e3', 'invoice.paid',   'in_2', '{}', now() - interval '3 minutes'),
        ('e4', 'invoice.paid',   'in_3', '{}', now() - interval '2 minutes');

    -- a batch holds one type, the oldest due event's, and only the lease holder gets one
    SELECT count(*) INTO n FROM claim_batch('run-a', 10) WHERE type = 'invoice.paid';
    ASSERT n = 3, format('one-type batch: %s', n);
    SELECT count(*) INTO n FROM claim_batch('run-b', 10);
    ASSERT n = 0, 'run-b has no lease';
    SELECT count(*) INTO n FROM claim_batch('run-a', 10);
    ASSERT n = 1 AND (SELECT status FROM stripe_event WHERE id = 'e2') = 'processing', 'next batch: the refund';

    -- success, a per-event error, and a result the handler forgot
    SELECT * INTO r FROM finish_batch(ARRAY['e1', 'e3', 'e4'],
        '[{"event_id": "e1", "error": null}, {"event_id": "e3", "error": "total mismatch"}]');
    ASSERT r.done = 1 AND r.retrying = 2 AND r.dead = '{}' AND r.paused_for_s = 0, format('mixed: %s', r);
    ASSERT (SELECT last_error FROM stripe_event WHERE id = 'e4') = 'handler returned no result for this event';

    -- whole batch hit Xero's rate limit: no attempt counted, all Xero work paused
    SELECT * INTO r FROM finish_batch(ARRAY['e2'], NULL, 'XERO_RATE_LIMITED retry_after=42');
    ASSERT r.retrying = 1 AND r.paused_for_s BETWEEN 41 AND 42, format('429: %s', r);
    ASSERT (SELECT attempts FROM stripe_event WHERE id = 'e2') = 0, '429 is not an attempt';
    SELECT count(*) INTO n FROM claim_batch('run-a', 10);
    ASSERT n = 0, 'paused';

    UPDATE sync_state SET xero_paused_until = '-infinity';
    PERFORM claim_batch('run-a', 10);
    SELECT * INTO r FROM finish_batch(ARRAY['e2'], '[{"event_id": "e2"}]');
    ASSERT r.done = 1, format('e2 done: %s', r);

    -- dead after max_attempts, reported back by id
    UPDATE stripe_event SET next_attempt_at = now() WHERE id IN ('e3', 'e4');
    PERFORM claim_batch('run-a', 10);
    SELECT * INTO r FROM finish_batch(ARRAY['e3', 'e4'], '[]', 'Xero 500', 2);
    ASSERT r.dead = ARRAY['e3', 'e4'] AND r.retrying = 0, format('dead: %s', r);

    -- an error a retry can't fix goes dead on the first attempt
    INSERT INTO stripe_event (id, type, object_id, payload) VALUES ('e5', 'invoice.paid', 'in_5', '{}');
    PERFORM claim_batch('run-a', 10);
    SELECT * INTO r FROM finish_batch(ARRAY['e5'], '[{"event_id": "e5", "error": "PERMANENT: tax lines"}]');
    ASSERT r.dead = ARRAY['e5'] AND (SELECT attempts FROM stripe_event WHERE id = 'e5') = 1, format('permanent: %s', r);

    -- waiting on another event isn't a failure: no attempt spent, checked again soon, dead only after 2 days
    INSERT INTO stripe_event (id, type, object_id, payload) VALUES ('e6', 'refund.created', 're_6', '{}');
    PERFORM claim_batch('run-a', 10);
    SELECT * INTO r FROM finish_batch(ARRAY['e6'], '[{"event_id": "e6", "error": "WAITING: invoice in_6 is not in Xero yet"}]');
    ASSERT r.retrying = 1 AND (SELECT attempts FROM stripe_event WHERE id = 'e6') = 0, format('waiting: %s', r);
    ASSERT (SELECT next_attempt_at = now() + interval '30 seconds' FROM stripe_event WHERE id = 'e6'), 'first wait 30 s';
    INSERT INTO stripe_event (id, type, object_id, payload) VALUES ('e7', 'invoice.paid', 'in_6', '{}');
    PERFORM claim_batch('run-a', 10);
    PERFORM finish_batch(ARRAY['e7'], '[{"event_id": "e7", "error": null}]');
    ASSERT (SELECT next_attempt_at = now() FROM stripe_event WHERE id = 'e6'), 'woken when its invoice is done';
    UPDATE stripe_event SET received_at = now() - interval '3 days', next_attempt_at = now() WHERE id = 'e6';
    PERFORM claim_batch('run-a', 10);
    SELECT * INTO r FROM finish_batch(ARRAY['e6'], '[{"event_id": "e6", "error": "WAITING: invoice in_6 is not in Xero yet"}]');
    ASSERT r.dead = ARRAY['e6'], format('waited too long: %s', r);

    -- links: written once, updated on conflict, read back with the settings
    ASSERT save_links('[{"stripe_id": "cus_1", "kind": "contact", "xero_id": "00000000-0000-4000-8000-000000000001"},
                        {"stripe_id": "in_1", "kind": "invoice", "xero_id": "00000000-0000-4000-8000-000000000002",
                         "amount": 49.00}]') = 2;
    ASSERT save_links('[{"stripe_id": "cus_1", "kind": "contact", "xero_id": "00000000-0000-4000-8000-000000000003"}]') = 1;
    ASSERT save_links(NULL) = 0;
    ctx := batch_context(ARRAY['cus_1', 'in_1', 'in_x']);
    ASSERT ctx #>> '{links,cus_1,contact}' = '00000000-0000-4000-8000-000000000003', ctx::text;
    ASSERT ctx #>> '{links,in_1,invoice}' = '00000000-0000-4000-8000-000000000002', ctx::text;
    ASSERT ctx #>> '{settings,xero_bank_account}' = '090' AND NOT ctx->'links' ? 'in_x', ctx::text;

    -- reconcile: only ids the inbox lacks are missing, and queuing one twice stores it once
    ASSERT (SELECT array_agg(x ORDER BY x) FROM missing_events(ARRAY['e1', 'e9', 'e8']) x) = ARRAY['e8', 'e9'], 'missing';
    ASSERT (SELECT array_agg(x) FROM enqueue_events(
        '[{"id": "e9", "type": "invoice.paid", "data": {"object": {"id": "in_9"}}}]', 'reconcile') x) = ARRAY['e9'];
    ASSERT (SELECT count(*) FROM enqueue_events(
        '[{"id": "e9", "type": "invoice.paid", "data": {"object": {"id": "in_9"}}}]', 'reconcile')) = 0, 'queued once';
    ASSERT (SELECT concat_ws(' ', source, status, object_id) FROM stripe_event WHERE id = 'e9') = 'reconcile pending in_9';
END $$;

\echo 'schema tests: OK'
