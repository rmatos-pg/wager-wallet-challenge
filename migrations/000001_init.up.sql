BEGIN;

-- Money = BIGINT cents + ISO 4217 code. See docs/DESIGN.md section 2 for the
-- invariant -> constraint map.

CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_units bigint      NOT NULL CHECK (balance_units >= 0),
    version       bigint      NOT NULL CHECK (version >= 1),
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    UNIQUE (player_id, currency),
    -- Target of the composite FKs below: a movement cannot disagree with its
    -- wallet on player or currency.
    UNIQUE (id, player_id, currency)
);

CREATE TABLE wager_transactions (
    id                       uuid        PRIMARY KEY,
    origin                   text        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                     text        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                   text        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                uuid        NOT NULL,
    player_id                uuid        NOT NULL,
    currency                 char(3)     NOT NULL,
    amount_units             bigint      NOT NULL CHECK (amount_units >= 0),

    provider_id              text,
    external_id              text,
    idempotency_key          text,
    payload_hash             text,
    round_id                 text,
    game_id                  text,
    reference_external_id    text,
    reference_id             uuid REFERENCES wager_transactions (id),

    failure_code             text,
    result_balance_units     bigint CHECK (result_balance_units >= 0),

    attempts                 integer     NOT NULL DEFAULT 0,
    next_attempt_at          timestamptz,
    expires_at               timestamptz,

    created_at               timestamptz NOT NULL,
    updated_at               timestamptz NOT NULL,
    completed_at             timestamptz,

    FOREIGN KEY (wallet_id, player_id, currency) REFERENCES wallets (id, player_id, currency),

    -- OPENING is internal and has no provider metadata; everything else is
    -- external and must carry it.
    CONSTRAINT origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING' AND status = 'PROCESSED'
            AND provider_id IS NULL AND external_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_id IS NULL AND reference_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT zero_only_for_loss CHECK ((kind = 'LOSS') = (amount_units = 0)),
    CONSTRAINT reversal_needs_reference CHECK (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_id IS NOT NULL),
    CONSTRAINT terminal_fields CHECK (
        (status = 'PROCESSED' AND result_balance_units IS NOT NULL AND failure_code IS NULL AND completed_at IS NOT NULL)
        OR (status IN ('REJECTED', 'FAILED') AND failure_code IS NOT NULL AND completed_at IS NOT NULL)
        OR (status IN ('PENDING', 'PENDING_REFERENCE') AND completed_at IS NULL)
    )
);

-- Idempotency: one operation per (provider, external id) and per (provider, key).
CREATE UNIQUE INDEX wager_tx_external_uk ON wager_transactions (provider_id, external_id) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_tx_key_uk ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';

-- At most one initial credit per wallet.
CREATE UNIQUE INDEX wager_tx_one_opening_uk ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- At most one successful reversal (REFUND or ROLLBACK) per referenced
-- transaction: the same debit can never be returned twice.
CREATE UNIQUE INDEX wager_tx_one_reversal_uk ON wager_transactions (reference_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

CREATE INDEX wager_tx_due_idx ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';

-- Terminal rows never change; identity columns never change; nothing is deleted.
CREATE FUNCTION wager_tx_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager_transactions rows cannot be deleted' USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is terminal (%)', OLD.id, OLD.status USING ERRCODE = 'restrict_violation';
    END IF;
    IF (NEW.id, NEW.origin, NEW.kind, NEW.wallet_id, NEW.player_id, NEW.currency, NEW.amount_units, NEW.created_at)
        IS DISTINCT FROM (OLD.id, OLD.origin, OLD.kind, OLD.wallet_id, OLD.player_id, OLD.currency, OLD.amount_units, OLD.created_at)
       OR (NEW.provider_id, NEW.external_id, NEW.idempotency_key, NEW.payload_hash)
        IS DISTINCT FROM (OLD.provider_id, OLD.external_id, OLD.idempotency_key, OLD.payload_hash) THEN
        RAISE EXCEPTION 'immutable columns of transaction % cannot change', OLD.id USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wager_tx_guard_trg BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_tx_guard();

CREATE TABLE wallet_ledger_entries (
    id                   uuid        PRIMARY KEY,
    -- Stable order for the opaque cursor. Writers hold the wallet lock, so
    -- within a wallet seq follows commit order.
    seq                  bigint      GENERATED ALWAYS AS IDENTITY UNIQUE,
    wallet_id            uuid        NOT NULL,
    player_id            uuid        NOT NULL,
    currency             char(3)     NOT NULL,
    transaction_id       uuid        NOT NULL REFERENCES wager_transactions (id),
    direction            text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_units         bigint      NOT NULL CHECK (amount_units > 0),
    balance_before_units bigint      NOT NULL CHECK (balance_before_units >= 0),
    balance_after_units  bigint      NOT NULL CHECK (balance_after_units >= 0),
    wallet_version       bigint      NOT NULL CHECK (wallet_version >= 1),
    created_at           timestamptz NOT NULL,
    FOREIGN KEY (wallet_id, player_id, currency) REFERENCES wallets (id, player_id, currency),
    UNIQUE (wallet_id, transaction_id),
    CONSTRAINT balance_math CHECK (
        (direction = 'CREDIT' AND balance_after_units = balance_before_units + amount_units)
        OR (direction = 'DEBIT' AND balance_after_units = balance_before_units - amount_units)
    )
);

CREATE INDEX ledger_wallet_seq_idx ON wallet_ledger_entries (wallet_id, seq);

CREATE FUNCTION append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (% rejected)', TG_TABLE_NAME, TG_OP USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_append_only_trg BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION append_only();
CREATE TRIGGER ledger_no_truncate_trg BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION append_only();

CREATE TABLE inbox_messages (
    consumer_name text        NOT NULL,
    message_id    text        NOT NULL,
    payload_hash  text        NOT NULL,
    received_at   timestamptz NOT NULL,
    processed_at  timestamptz NOT NULL,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    id              uuid        PRIMARY KEY,
    aggregate_id    uuid        NOT NULL,
    event_type      text        NOT NULL,
    event_version   integer     NOT NULL,
    correlation_id  text        NOT NULL,
    causation_id    text,
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    locked_by       text,
    locked_until    timestamptz,
    published_at    timestamptz,
    last_error      text
);

CREATE INDEX outbox_due_idx ON outbox_events (next_attempt_at) WHERE published_at IS NULL;

-- The event snapshot never changes; only delivery bookkeeping does.
CREATE FUNCTION outbox_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'outbox_events rows cannot be deleted' USING ERRCODE = 'restrict_violation';
    END IF;
    IF (NEW.id, NEW.aggregate_id, NEW.event_type, NEW.event_version, NEW.correlation_id, NEW.causation_id, NEW.payload, NEW.occurred_at)
        IS DISTINCT FROM (OLD.id, OLD.aggregate_id, OLD.event_type, OLD.event_version, OLD.correlation_id, OLD.causation_id, OLD.payload, OLD.occurred_at) THEN
        RAISE EXCEPTION 'outbox event % is immutable', OLD.id USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_guard_trg BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_guard();

COMMIT;
