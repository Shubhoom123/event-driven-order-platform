#!/usr/bin/env bash
# Helper for chaos.sh: place order number $1. Needs ORDER, TAG, SKU in the environment. Prints the HTTP status.
i=$1; amt=2000; [ $((i % 5)) -eq 0 ] && amt=500000
curl -s -o /dev/null -w '%{http_code}\n' -X POST "$ORDER/orders" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $TAG-$i" \
  -d "{\"customerId\":\"$TAG\",\"items\":[{\"sku\":\"$SKU\",\"quantity\":1}],\"amountCents\":$amt,\"currency\":\"USD\"}"
