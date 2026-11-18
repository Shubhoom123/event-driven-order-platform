#!/usr/bin/env bash
# Load test: k6 at RATE orders/s for DURATION, then wait for every order to finish and reconcile.
# Usage: load.sh [rate=200] [duration_seconds=60]   (needs k6, the stack and all 4 services running)
set -u
RATE=${1:-200}; SECS=${2:-60}
export ORDER=${ORDER:-http://localhost:8091} INV=${INV:-http://localhost:8092}
export TAG="load-$RANDOM$RANDOM" SKU="LOAD-$RANDOM" RATE DURATION="${SECS}s"
STOCK=$(( RATE * SECS + RATE * 5 ))
PG=$(docker ps -qf name=postgres | head -1)
now() { python3 -c 'import time;print(int(time.time()*1000))'; }
count() { docker exec -i "$PG" psql -U postgres -d orders -At -c "$1"; }

curl -s -X PUT "$INV/stock/$SKU" -H 'Content-Type: application/json' -d "{\"available\":$STOCK}" >/dev/null || { echo "inventory not reachable"; exit 1; }
echo "run $TAG: $RATE orders/s for ${SECS}s (10% declined), stock $STOCK"
t0=$(now)
k6 run --quiet scripts/k6/orders.js
t1=$(now)
placed=$(count "SELECT count(*) FROM orders WHERE customer_id='$TAG'")
echo "k6 finished after $((t1 - t0)) ms, $placed orders created; waiting for the saga to finish them"
for _ in $(seq 1 600); do
  p=$(count "SELECT count(*) FROM orders WHERE customer_id='$TAG' AND status NOT IN ('CONFIRMED','CANCELLED','MANUAL_REVIEW')")
  [ "$p" = 0 ] && break; sleep 1
done
t2=$(now)
echo "all $placed orders finished $((t2 - t0)) ms after the first request (drain after k6 ended: $((t2 - t1)) ms)"
echo "end-to-end rate: $(python3 -c "print(round($placed / (($t2 - $t0) / 1000), 1))") orders/s"
for _ in $(seq 1 60); do scripts/reconcile.sh "$TAG" "$SKU" "$STOCK" >/dev/null 2>&1 && break; sleep 2; done  # notifications lag the saga
scripts/reconcile.sh "$TAG" "$SKU" "$STOCK"
