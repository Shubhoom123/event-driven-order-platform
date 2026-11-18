#!/usr/bin/env bash
# End-to-end check against the running stack: happy path and declined-payment path.
# Needs: docker compose up -d, and all four services running (see README).
set -u
ORDER=${ORDER:-http://localhost:8091}; INV=${INV:-http://localhost:8092}; NOTIF=${NOTIF:-http://localhost:8094}
SKU="SMOKE-$RANDOM"
json() { sed -n "s/.*\"$1\":\"\{0,1\}\([^\",}]*\).*/\1/p"; }
fail() { echo "FAIL: $*"; exit 1; }

wait_status() {  # order id, expected status
  for _ in $(seq 1 40); do
    s=$(curl -s "$ORDER/orders/$1" | json status)
    [ "$s" = "$2" ] && return 0
    sleep 1
  done
  fail "order $1 ended as '$s', expected $2"
}
place() {  # amount cents, key
  curl -s -X POST "$ORDER/orders" -H 'Content-Type: application/json' -H "Idempotency-Key: $2" \
    -d "{\"customerId\":\"smoke\",\"items\":[{\"sku\":\"$SKU\",\"quantity\":2}],\"amountCents\":$1,\"currency\":\"USD\"}" | json orderId
}
stock() { curl -s "$INV/stock/$SKU" | json "$1"; }

curl -s -X PUT "$INV/stock/$SKU" -H 'Content-Type: application/json' -d '{"available":10}' >/dev/null || fail "inventory not reachable"

echo "== happy path"
ok=$(place 2000 "smoke-ok-$RANDOM-$SKU"); [ -n "$ok" ] || fail "order not created"
wait_status "$ok" CONFIRMED
sleep 2
[ "$(stock available)" = 8 ] && [ "$(stock sold)" = 2 ] && [ "$(stock reserved)" = 0 ] || fail "stock after confirm: $(curl -s $INV/stock/$SKU)"
curl -s "$NOTIF/notifications/$ok" | grep -q OrderConfirmed || fail "no OrderConfirmed notification"
echo "ok: order $ok CONFIRMED, stock sold, customer notified"

echo "== declined payment (compensation)"
bad=$(place 500000 "smoke-bad-$RANDOM-$SKU"); [ -n "$bad" ] || fail "order not created"
wait_status "$bad" CANCELLED
sleep 2
[ "$(stock available)" = 8 ] && [ "$(stock reserved)" = 0 ] || fail "stock not released: $(curl -s $INV/stock/$SKU)"
curl -s "$NOTIF/notifications/$bad" | grep -q OrderCancelled || fail "no OrderCancelled notification"
echo "ok: order $bad CANCELLED, stock released, customer notified"

echo "== idempotent intake"
k="smoke-dup-$RANDOM-$SKU"; a=$(place 2000 "$k"); b=$(place 2000 "$k")
[ "$a" = "$b" ] || fail "same Idempotency-Key created two orders"
echo "ok: repeated request returned the same order"
echo "ALL PASSED"
