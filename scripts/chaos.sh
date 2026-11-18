#!/usr/bin/env bash
# Failure injection against the running stack: place mixed orders, kill -9 one service mid-flight,
# restart it, wait for every order to finish, then check cross-service consistency.
# Usage: chaos.sh [orders=300] [service=inventory-service]   (stack and all 4 services must be running)
set -u
N=${1:-300}; SVC=${2:-inventory-service}
export ORDER=${ORDER:-http://localhost:8091} INV=${INV:-http://localhost:8092}
case $SVC in order-service) PORT=8091;; inventory-service) PORT=8092;; payment-service) PORT=8093;; notification-service) PORT=8094;; *) echo "unknown service"; exit 2;; esac
export TAG="chaos-$RANDOM$RANDOM" SKU="CHAOS-$RANDOM"
PG=$(docker ps -qf name=postgres | head -1)
now() { python3 -c 'import time;print(int(time.time()*1000))'; }
pending() { docker exec -i "$PG" psql -U postgres -d orders -At -c "SELECT count(*) FROM orders WHERE customer_id='$TAG' AND status NOT IN ('CONFIRMED','CANCELLED','MANUAL_REVIEW')"; }

curl -s -X PUT "$INV/stock/$SKU" -H 'Content-Type: application/json' -d "{\"available\":$N}" >/dev/null || { echo "inventory not reachable"; exit 1; }
echo "run $TAG: $N orders (20% will be declined), killing $SVC mid-flight"

t0=$(now)
( seq 1 $N | xargs -P 16 -n 1 scripts/place-order.sh | sort | uniq -c | sed 's/^/  http /' ) &
placer=$!
sleep 0.4
pid=$(lsof -ti tcp:$PORT -sTCP:LISTEN | head -1)
[ -n "$pid" ] && kill -9 $pid && echo "killed $SVC (pid $pid)"
wait $placer
t1=$(now)
[ "$(docker exec -i "$PG" psql -U postgres -d orders -At -c "SELECT count(*) FROM orders WHERE customer_id='$TAG'")" -gt 0 ] || { echo "no orders were created, aborting"; exit 1; }
echo "placement finished in $((t1 - t0)) ms"

nohup mvn -q -pl $SVC spring-boot:run > /tmp/$SVC.chaos.log 2>&1 &
echo "restarting $SVC, waiting for orders to finish (max 180 s)"
t2=$(now)
for _ in $(seq 1 180); do [ "$(pending)" = 0 ] && break; sleep 1; done
t3=$(now)
echo "all orders finished $((t3 - t0)) ms after the first request ($((t3 - t2)) ms after the restart began)"
for _ in $(seq 1 60); do scripts/reconcile.sh "$TAG" "$SKU" "$N" >/dev/null 2>&1 && break; sleep 2; done  # notifications lag the saga
scripts/reconcile.sh "$TAG" "$SKU" "$N"
