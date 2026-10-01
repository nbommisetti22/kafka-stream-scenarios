#!/usr/bin/env bash
# End-to-end walkthrough of the banking scenarios.
# Prerequisites: Kafka on localhost:9092 and all five services running (see README).
set -euo pipefail

ACCOUNT_API=${ACCOUNT_API:-http://localhost:8081/api}
LEDGER_API=${LEDGER_API:-http://localhost:8083/api}
NOTIFY_API=${NOTIFY_API:-http://localhost:8085/api}

post() { curl -sS -X POST "$ACCOUNT_API$1" -H 'Content-Type: application/json' -d "$2"; echo; }
step() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }

step "1. Create customer + two accounts (compacted topics bank.customers / bank.accounts)"
post /customers '{"customerId":"CUST-1","fullName":"Ada Lovelace","email":"ada@example.com","homeCountry":"GB","tier":"GOLD"}'
post /accounts  '{"accountId":"ACC-1","customerId":"CUST-1","accountType":"CHECKING","currency":"USD"}'
post /accounts  '{"accountId":"ACC-2","customerId":"CUST-1","accountType":"SAVINGS","currency":"USD"}'
sleep 2

step "2. Deposit + payment (keyed by account => ordered on one partition)"
post /transactions '{"accountId":"ACC-1","type":"DEPOSIT","amount":2500,"currency":"USD","channel":"BRANCH","country":"US"}'
post /transactions '{"accountId":"ACC-1","type":"PAYMENT","amount":45.90,"currency":"USD","channel":"MOBILE","country":"US","merchant":"Coffee"}'

step "3. Idempotency: the same transactionId submitted twice is booked once"
post /transactions '{"transactionId":"dup-001","accountId":"ACC-1","type":"PAYMENT","amount":10,"currency":"USD","country":"US"}'
post /transactions '{"transactionId":"dup-001","accountId":"ACC-1","type":"PAYMENT","amount":10,"currency":"USD","country":"US"}'

step "4. Transfer ACC-1 -> ACC-2 (ledger re-keys the credit leg to ACC-2)"
post /transfers '{"fromAccountId":"ACC-1","toAccountId":"ACC-2","amount":500,"currency":"USD","country":"US"}'

step "5. Business rejections: ATM limit, unsupported currency, insufficient funds"
post /transactions '{"accountId":"ACC-1","type":"WITHDRAWAL","amount":5000,"currency":"USD","channel":"ATM","country":"US"}'
post /transactions '{"accountId":"ACC-1","type":"PAYMENT","amount":10,"currency":"JPY","country":"US"}'
post /transactions '{"accountId":"ACC-2","type":"PAYMENT","amount":9999,"currency":"USD","channel":"BRANCH","country":"US"}'

step "6. Atomic payroll batch (Kafka transaction, all-or-nothing)"
post /transactions/batch '[
  {"accountId":"ACC-1","type":"DEPOSIT","amount":3000,"currency":"USD","channel":"BRANCH","country":"US","merchant":"Payroll"},
  {"accountId":"ACC-2","type":"DEPOSIT","amount":3200,"currency":"USD","channel":"BRANCH","country":"US","merchant":"Payroll"}]'

step "7. Fraud: large amount, velocity burst, impossible travel, money mule"
post /transactions '{"accountId":"ACC-2","type":"DEPOSIT","amount":12000,"currency":"USD","channel":"BRANCH","country":"US"}'
for i in 1 2 3 4 5 6 7; do
  post /transactions '{"accountId":"ACC-1","type":"PAYMENT","amount":1,"currency":"USD","channel":"MOBILE","country":"US"}' > /dev/null
done
post /transactions '{"accountId":"ACC-1","type":"PAYMENT","amount":20,"currency":"USD","channel":"MOBILE","country":"SG"}'
post /transfers '{"fromAccountId":"ACC-2","toAccountId":"ACC-1","amount":9000,"currency":"USD","channel":"BRANCH","country":"US"}'

step "8. Freeze ACC-2, then try to pay from it (rejected: ACCOUNT_FROZEN)"
post /accounts/ACC-2/freeze ''
sleep 1
post /transactions '{"accountId":"ACC-2","type":"PAYMENT","amount":5,"currency":"USD","country":"US"}'

sleep 5
step "9. Interactive query: balances served from the Kafka Streams state store"
curl -sS "$LEDGER_API/balances/ACC-1"; echo
curl -sS "$LEDGER_API/balances/ACC-2"; echo

step "10. Notifications sent (push receipts, rejection e-mails, fraud SMS)"
curl -sS "$NOTIFY_API/notifications" | head -c 3000; echo
