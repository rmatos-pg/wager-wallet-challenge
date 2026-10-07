#!/usr/bin/env bash
# Provisions the FIFO queues used by the wallet. Idempotent: create-queue
# returns the existing queue when attributes match.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"

awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","MessageRetentionPeriod":"1209600"}'

DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions-dlq.fifo"

# Content-based dedup is off: producers must send MessageDeduplicationId.
# maxReceiveCount=5 moves poison/exhausted messages to the DLQ.
awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"30\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"

# Outbound integration events published by the outbox worker.
awslocal sqs create-queue \
  --queue-name wallet-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'

echo "SQS queues provisioned"
