-- Stripe -> Xero sync. all state lives here, the n8n workflows stay thin and stateless
-- safe to rerun: psql -v ON_ERROR_STOP=1 -d <db> -f schema.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- secrets the workflows need but must never have in their JSON or execution logs
CREATE TABLE IF NOT EXISTS app_secret (
    name  text PRIMARY KEY,
    value text NOT NULL
);

-- inbox: every verified Stripe event, stored once, processed at our own pace
CREATE TABLE IF NOT EXISTS stripe_event (
    id              text PRIMARY KEY,                -- evt_... or a synthetic id from backfill/reconcile
    type            text NOT NULL,
    object_id       text NOT NULL,                   -- data.object.id
    source          text NOT NULL DEFAULT 'webhook' CHECK (source IN ('webhook', 'backfill', 'reconcile')),
    payload         jsonb NOT NULL,
    received_at     timestamptz NOT NULL DEFAULT now(),
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'done', 'dead', 'ignored')),
    attempts        int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_until    timestamptz,
    last_error      text,
    finished_at     timestamptz,
    redeliveries    int NOT NULL DEFAULT 0           -- how many times Stripe sent it again, for the report
);
CREATE INDEX IF NOT EXISTS stripe_event_due ON stripe_event (next_attempt_at)
    WHERE status IN ('pending', 'processing');

-- Stripe object -> Xero object, this is what makes the Xero side idempotent
CREATE TABLE IF NOT EXISTS xero_link (
    stripe_id  text NOT NULL,
    kind       text NOT NULL CHECK (kind IN ('contact', 'invoice', 'payment', 'credit_note', 'refund')),
    xero_id    uuid NOT NULL,
    amount     numeric(14, 2),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stripe_id, kind)
);

-- per-environment settings read at run time, so dev and prod differ and the workflow JSON doesn't
CREATE TABLE IF NOT EXISTS app_setting (
    name  text PRIMARY KEY,
    value text NOT NULL
);
INSERT INTO app_setting VALUES
    ('xero_api_base',      'https://api.xero.com/api.xro/2.0'),
    ('stripe_api_base',    'https://api.stripe.com'),
    ('xero_sales_account', '200'),   -- revenue account for invoice and credit note lines
    ('xero_bank_account',  '090'),   -- where Stripe money lands, payments in and refunds out
    ('xero_currency',      'USD'),   -- the org's base currency, anything else is rejected
    ('alert_chat_id',      '')       -- Telegram chat for alerts
ON CONFLICT (name) DO NOTHING;

-- one row: the processor lease (one run at a time) and the Xero rate-limit pause
CREATE TABLE IF NOT EXISTS sync_state (
    id                int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    lease_holder      text,
    lease_until       timestamptz NOT NULL DEFAULT '-infinity',
    xero_paused_until timestamptz NOT NULL DEFAULT '-infinity',
    pause_reason      text
);
INSERT INTO sync_state DEFAULT VALUES ON CONFLICT DO NOTHING;


-- check the Stripe-Signature header and store the event in one call
-- body_b64 is the raw request body in base64, the signature covers exact bytes, not re-serialized JSON
CREATE OR REPLACE FUNCTION stripe_intake(
    body_b64    text,
    sig_header  text,
    tolerance_s int    DEFAULT 300,
    handled     text[] DEFAULT ARRAY['invoice.paid', 'refund.created', 'invoice.payment_failed']
) RETURNS TABLE (valid boolean, is_new boolean, event_id text, event_type text, reason text)
LANGUAGE plpgsql AS $$
DECLARE
    body     bytea  := decode(body_b64, 'base64');
    ts       text   := substring(sig_header FROM '(?:^|,)\s*t=(\d{1,12})(?:,|$)');
    sigs     text[] := ARRAY(SELECT m[1] FROM regexp_matches(coalesce(sig_header, ''),
                                                             '(?:^|,)\s*v1=([0-9a-f]{64})', 'g') AS m);
    secret   text;
    expected text;
    evt      jsonb;
BEGIN
    IF ts IS NULL OR cardinality(sigs) = 0 THEN
        RETURN QUERY SELECT false, false, NULL::text, NULL::text, 'malformed Stripe-Signature header';
        RETURN;
    END IF;
    IF abs(extract(epoch FROM clock_timestamp()) - ts::bigint) > tolerance_s THEN
        RETURN QUERY SELECT false, false, NULL::text, NULL::text, 'timestamp outside tolerance';
        RETURN;
    END IF;

    SELECT value INTO secret FROM app_secret WHERE name = 'stripe_webhook_secret';
    IF secret IS NULL THEN
        RAISE EXCEPTION 'app_secret "stripe_webhook_secret" is not set';  -- fail closed
    END IF;

    expected := encode(hmac(convert_to(ts || '.', 'UTF8') || body, convert_to(secret, 'UTF8'), 'sha256'), 'hex');
    -- during a secret rotation there are several v1 values, any match will do
    -- comparing hashes, not raw strings, so '=' short-circuiting leaks nothing useful
    IF NOT EXISTS (SELECT 1 FROM unnest(sigs) s WHERE digest(s, 'sha256') = digest(expected, 'sha256')) THEN
        RETURN QUERY SELECT false, false, NULL::text, NULL::text, 'signature mismatch';
        RETURN;
    END IF;

    evt := convert_from(body, 'UTF8')::jsonb;
    INSERT INTO stripe_event (id, type, object_id, payload, status)
    VALUES (evt->>'id', evt->>'type', evt #>> '{data,object,id}', evt,
            CASE WHEN evt->>'type' = ANY (handled) THEN 'pending' ELSE 'ignored' END)
    ON CONFLICT (id) DO NOTHING;
    IF FOUND THEN
        RETURN QUERY SELECT true, true, evt->>'id', evt->>'type', 'stored';
    ELSE
        UPDATE stripe_event SET redeliveries = redeliveries + 1 WHERE id = evt->>'id';
        RETURN QUERY SELECT true, false, evt->>'id', evt->>'type', 'duplicate';
    END IF;
END $$;


-- one processor run at a time. Xero gives 60 calls/min per org, parallel runs would only buy
-- 429s and races between events for the same invoice
CREATE OR REPLACE FUNCTION take_lease(holder text, ttl interval DEFAULT '5 minutes')
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    UPDATE sync_state SET lease_holder = holder, lease_until = now() + ttl
     WHERE lease_until < now() OR lease_holder = holder;
    RETURN FOUND;
END $$;

CREATE OR REPLACE FUNCTION release_lease(holder text)
RETURNS void LANGUAGE sql AS $$
    UPDATE sync_state SET lease_until = '-infinity' WHERE lease_holder = holder;
$$;


-- claim a batch of due events of one type. the oldest due event picks the type, so a handler gets
-- one kind of work and Xero gets one batched request per step, not one per event
-- a 'processing' row with an expired lock is from a run that died, take it back
CREATE OR REPLACE FUNCTION claim_events(max_n int, lock_for interval DEFAULT '5 minutes')
RETURNS SETOF stripe_event LANGUAGE sql AS $$
    WITH due AS (
        SELECT id, type, received_at FROM stripe_event
         WHERE status IN ('pending', 'processing')
           AND coalesce(locked_until, '-infinity') < now()
           AND next_attempt_at <= now()
           AND (SELECT xero_paused_until FROM sync_state) <= now()
    ), batch AS (
        SELECT s.id FROM stripe_event s
         WHERE s.id IN (SELECT id FROM due WHERE type = (SELECT type FROM due ORDER BY received_at LIMIT 1))
           AND s.status IN ('pending', 'processing')             -- checked again on the locked row
           AND coalesce(s.locked_until, '-infinity') < now()
         ORDER BY s.received_at
         LIMIT max_n
         FOR UPDATE SKIP LOCKED
    )
    UPDATE stripe_event e
       SET status = 'processing', attempts = e.attempts + 1, locked_until = now() + lock_for
      FROM batch
     WHERE e.id = batch.id
    RETURNING e.*;
$$;


-- one step of the processor loop: renew the lease, claim the next batch. returns nothing if another
-- run holds the lease, Xero is paused or the queue is empty
CREATE OR REPLACE FUNCTION claim_batch(holder text, max_n int DEFAULT 50, lease_ttl interval DEFAULT '5 minutes')
RETURNS SETOF stripe_event LANGUAGE plpgsql AS $$
BEGIN
    IF take_lease(holder, lease_ttl) THEN
        RETURN QUERY SELECT * FROM claim_events(max_n, lease_ttl) ORDER BY received_at;
    END IF;
END $$;


-- stop all Xero work for a while, after a 429 or before the minute budget runs out
CREATE OR REPLACE FUNCTION pause_xero(seconds int, reason text)
RETURNS timestamptz LANGUAGE sql AS $$
    UPDATE sync_state
       SET xero_paused_until = greatest(xero_paused_until, now() + make_interval(secs => seconds)),
           pause_reason = reason
    RETURNING xero_paused_until;
$$;


-- save how one attempt went
--   error NULL           -> done
--   retry_after_s given  -> rate limited, pause all Xero work, the attempt doesn't count
--   'PERMANENT: ...'     -> dead right away, a retry won't help (tax lines, a currency Xero lacks). replay_dead after a fix
--   'WAITING: ...'       -> needs another event first (a refund needs its invoice). no attempt spent, dead after 2 days
--                           (enough for a nightly reconcile). woken when an event for an object named in the message
--                           is done, else checked again after half the time waited so far (30 s .. 10 min)
--   otherwise            -> exponential backoff (30 s, 1 min, 2 min ... max 1 h), dead after max_attempts
CREATE OR REPLACE FUNCTION finish_event(
    p_event_id    text,
    p_error       text DEFAULT NULL,
    retry_after_s int  DEFAULT NULL,
    max_attempts  int  DEFAULT 8
) RETURNS TABLE (event_id text, status text, attempts int)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
BEGIN
    IF retry_after_s IS NOT NULL THEN
        PERFORM pause_xero(retry_after_s, p_error);
        RETURN QUERY
        UPDATE stripe_event e SET status = 'pending', attempts = e.attempts - 1, locked_until = NULL, last_error = p_error
         WHERE e.id = p_event_id RETURNING e.id, e.status, e.attempts;
    ELSIF p_error IS NULL THEN
        RETURN QUERY
        UPDATE stripe_event e SET status = 'done', locked_until = NULL, finished_at = now(), last_error = NULL
         WHERE e.id = p_event_id RETURNING e.id, e.status, e.attempts;
        UPDATE stripe_event w SET next_attempt_at = now()  -- whoever waits for this object goes next
          FROM stripe_event e
         WHERE e.id = p_event_id AND w.status = 'pending' AND w.last_error LIKE 'WAITING:%'
           AND strpos(w.last_error, e.object_id) > 0;
    ELSIF p_error LIKE 'WAITING:%' THEN
        RETURN QUERY
        UPDATE stripe_event e
           SET status = CASE WHEN e.received_at < now() - interval '2 days' THEN 'dead' ELSE 'pending' END,
               attempts = e.attempts - 1, locked_until = NULL, last_error = p_error,
               next_attempt_at = now() + least(interval '10 minutes', greatest(interval '30 seconds', (now() - e.received_at) / 2))
         WHERE e.id = p_event_id RETURNING e.id, e.status, e.attempts;
    ELSE
        RETURN QUERY
        UPDATE stripe_event e
           SET status = CASE WHEN e.attempts >= max_attempts OR p_error LIKE 'PERMANENT:%' THEN 'dead'
                             ELSE 'pending' END,
               locked_until = NULL,
               last_error = p_error,
               next_attempt_at = now() + least(interval '1 hour', interval '30 seconds' * 2 ^ (e.attempts - 1))
         WHERE e.id = p_event_id RETURNING e.id, e.status, e.attempts;
    END IF;
END $$;


-- save a handler run over a claimed batch and sum it up for the processor
--   results      [{event_id, error}] from the handler, error null on success. a claimed event
--                missing there counts as failed, so a handler bug can't lose events
--   batch_error  the handler failed as a whole (Xero down, 429), every claimed event gets it
-- an error with "XERO_RATE_LIMITED retry_after=N" pauses Xero work and doesn't count as an attempt
CREATE OR REPLACE FUNCTION finish_batch(
    claimed      text[],
    results      jsonb,
    batch_error  text DEFAULT NULL,
    max_attempts int  DEFAULT 8
) RETURNS TABLE (done int, retrying int, dead text[], paused_for_s int)
LANGUAGE plpgsql AS $$
DECLARE
    ev_id text;
    err   text;
    res   jsonb;
    r     record;
BEGIN
    done := 0; retrying := 0; dead := '{}';
    FOREACH ev_id IN ARRAY claimed LOOP
        err := batch_error;
        IF err IS NULL THEN
            res := NULL;
            SELECT e INTO res FROM jsonb_array_elements(coalesce(results, '[]')) e WHERE e->>'event_id' = ev_id LIMIT 1;
            err := CASE WHEN res IS NULL THEN 'handler returned no result for this event' ELSE res->>'error' END;
        END IF;
        SELECT * INTO r FROM finish_event(ev_id, err, substring(err FROM 'XERO_RATE_LIMITED retry_after=(\d+)')::int,
                                          max_attempts);
        IF r.status = 'done' THEN
            done := done + 1;
        ELSIF r.status = 'dead' THEN
            dead := dead || ev_id;
        ELSE
            retrying := retrying + 1;
        END IF;
    END LOOP;
    SELECT CASE WHEN xero_paused_until > now() THEN ceil(extract(epoch FROM xero_paused_until - now()))::int ELSE 0 END
      INTO paused_for_s FROM sync_state;
    RETURN NEXT;
END $$;


-- what a handler needs before talking to Xero: settings and the Xero ids already linked to these Stripe ids
CREATE OR REPLACE FUNCTION batch_context(stripe_ids text[])
RETURNS jsonb LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object(
        'settings', (SELECT coalesce(jsonb_object_agg(name, value), '{}') FROM app_setting),
        'links',    (SELECT coalesce(jsonb_object_agg(stripe_id, kinds), '{}')
                       FROM (SELECT stripe_id, jsonb_object_agg(kind, xero_id) AS kinds
                               FROM xero_link WHERE stripe_id = ANY (stripe_ids) GROUP BY stripe_id) l));
$$;

-- remember what a handler created in Xero: [{stripe_id, kind, xero_id, amount}], returns rows written
CREATE OR REPLACE FUNCTION save_links(links jsonb)
RETURNS int LANGUAGE sql AS $$
    WITH saved AS (
        INSERT INTO xero_link (stripe_id, kind, xero_id, amount)
        SELECT l->>'stripe_id', l->>'kind', (l->>'xero_id')::uuid, (l->>'amount')::numeric
          FROM jsonb_array_elements(coalesce(links, '[]')) l
        ON CONFLICT (stripe_id, kind) DO UPDATE SET xero_id = EXCLUDED.xero_id, amount = EXCLUDED.amount
        RETURNING 1)
    SELECT count(*)::int FROM saved;
$$;


-- reconcile: which of these Stripe event ids never got to the inbox
CREATE OR REPLACE FUNCTION missing_events(ids text[])
RETURNS SETOF text LANGUAGE sql STABLE AS $$
    SELECT u.id FROM unnest(ids) AS u(id) WHERE NOT EXISTS (SELECT 1 FROM stripe_event e WHERE e.id = u.id);
$$;

-- queue events that came some other way than the webhook, ones already in the inbox are left alone
CREATE OR REPLACE FUNCTION enqueue_events(events jsonb, p_source text)
RETURNS SETOF text LANGUAGE sql AS $$
    INSERT INTO stripe_event (id, type, object_id, source, payload)
    SELECT e->>'id', e->>'type', e #>> '{data,object,id}', p_source, e
      FROM jsonb_array_elements(coalesce(events, '[]')) e
    ON CONFLICT (id) DO NOTHING
    RETURNING id;
$$;


-- put dead events back in the queue (all of them or the listed ids)
CREATE OR REPLACE FUNCTION replay_dead(ids text[] DEFAULT NULL)
RETURNS SETOF text LANGUAGE sql AS $$
    UPDATE stripe_event SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL
     WHERE status = 'dead' AND (ids IS NULL OR id = ANY (ids))
    RETURNING id;
$$;
