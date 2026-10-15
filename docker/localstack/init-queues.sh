#!/bin/bash
# Cria as filas na subida do LocalStack. Idempotente: CreateQueue com os mesmos atributos é no-op.
set -euo pipefail

# Rede de segurança: depois de MAX_RECEIVE recebimentos sem ack (crash loop, poison message),
# o próprio SQS move a mensagem para a DLQ. O consumer move antes, explicitamente, nos erros
# classificados como permanentes ou quando esgota os retries transitórios.
MAX_RECEIVE="${SQS_MAX_RECEIVE_COUNT:-5}"

awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false,MessageRetentionPeriod=1209600

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url "$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes "{
  \"FifoQueue\": \"true\",
  \"ContentBasedDeduplication\": \"false\",
  \"VisibilityTimeout\": \"30\",
  \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE}\\\"}\"
}"

# Destino dos eventos de integração publicados pela outbox (decisão F).
awslocal sqs create-queue --queue-name wager-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

touch /tmp/queues-ready
echo "queues ready"
