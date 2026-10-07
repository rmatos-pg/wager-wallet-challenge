BEGIN;

DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
DROP FUNCTION IF EXISTS outbox_guard();
DROP FUNCTION IF EXISTS append_only();
DROP FUNCTION IF EXISTS wager_tx_guard();

COMMIT;
