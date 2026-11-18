#!/usr/bin/env bash
# Cross-service consistency check for one chaos run. Usage: reconcile.sh TAG SKU INITIAL_STOCK
set -u
TAG=$1; SKU=$2; INITIAL=$3
PG=$(docker ps -qf name=postgres | head -1)
q() { docker exec -i "$PG" psql -U postgres -d "$1" -At -c "$2"; }
W=$(mktemp -d)

q orders "SELECT id FROM orders WHERE customer_id='$TAG' AND status='CONFIRMED'" | sort > $W/C
q orders "SELECT id FROM orders WHERE customer_id='$TAG' AND status='CANCELLED'" | sort > $W/X
q orders "SELECT count(*) FROM orders WHERE customer_id='$TAG'" > $W/total
q orders "SELECT status||'='||count(*) FROM orders WHERE customer_id='$TAG' GROUP BY status ORDER BY 1" | tr '\n' ' ' > $W/mix
cat $W/C $W/X > $W/ids
qin() {  # db, query joined against t(id) holding this run's order ids (sent over stdin, no argument limit)
  { echo "CREATE TEMP TABLE t(id uuid); COPY t FROM STDIN;"; cat $W/ids; echo '\.'; echo "$2"; } \
    | docker exec -i "$PG" psql -q -U postgres -d "$1" -At | sort
}
qin payments "SELECT p.order_id FROM payments p JOIN t ON t.id=p.order_id WHERE p.status='CHARGED';" > $W/charged
qin inventory "SELECT r.order_id FROM reservations r JOIN t ON t.id=r.order_id WHERE r.status='COMMITTED';" > $W/committed
qin inventory "SELECT r.order_id FROM reservations r JOIN t ON t.id=r.order_id WHERE r.status IN ('RESERVED','COMMITTED');" > $W/active
qin notifications "SELECT n.order_id FROM notifications n JOIN t ON t.id=n.order_id WHERE n.type='OrderConfirmed';" > $W/nconf
qin notifications "SELECT n.order_id FROM notifications n JOIN t ON t.id=n.order_id WHERE n.type='OrderCancelled';" > $W/ncanc

n() { wc -l < "$1" | tr -d ' '; }
sub() { comm -23 "$1" "$2" | wc -l | tr -d ' '; }
both() { comm -12 "$1" "$2" | wc -l | tr -d ' '; }
total=$(cat $W/total)
[ "$total" -gt 0 ] || { echo "RECONCILE FAIL (no orders for this run)"; exit 1; }
 conf=$(n $W/C); canc=$(n $W/X)
v_unfinished=$(( total - conf - canc ))
v_conf_nocharge=$(sub $W/C $W/charged)
v_conf_nocommit=$(sub $W/C $W/committed)
v_conf_nonotice=$(sub $W/C $W/nconf)
v_canc_charged=$(both $W/X $W/charged)
v_canc_holding=$(both $W/X $W/active)
v_canc_nonotice=$(sub $W/X $W/ncanc)
v_charged_unconf=$(sub $W/charged $W/C)
IFS=, read -r avail reserved sold <<< "$(q inventory "SELECT available||','||reserved||','||sold FROM stock WHERE sku='$SKU'")"
v_stock=0
[ "$reserved" = 0 ] || v_stock=$((v_stock+1))
[ $((avail + sold)) = "$INITIAL" ] || v_stock=$((v_stock+1))
[ "$sold" = "$conf" ] || v_stock=$((v_stock+1))

echo "orders: $total ($(cat $W/mix))"
echo "stock:  available=$avail reserved=$reserved sold=$sold (initial $INITIAL)"
echo "violations:"
printf '  %-34s %s\n' "orders not finished" $v_unfinished "confirmed without a charge" $v_conf_nocharge \
  "confirmed without committed stock" $v_conf_nocommit "confirmed without notification" $v_conf_nonotice \
  "cancelled but charged" $v_canc_charged "cancelled but still holding stock" $v_canc_holding \
  "cancelled without notification" $v_canc_nonotice "charged but not confirmed" $v_charged_unconf \
  "stock invariant breaks" $v_stock
sum=$((v_unfinished+v_conf_nocharge+v_conf_nocommit+v_conf_nonotice+v_canc_charged+v_canc_holding+v_canc_nonotice+v_charged_unconf+v_stock))
rm -rf $W
if [ $sum = 0 ]; then echo "RECONCILE PASS"; else echo "RECONCILE FAIL ($sum violations)"; exit 1; fi
