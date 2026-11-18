import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const RATE = Number(__ENV.RATE || 200);
const DURATION = __ENV.DURATION || '60s';

export const options = {
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 100, maxVUs: 400,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

export default function () {
  const i = exec.scenario.iterationInTest;
  const amount = i % 10 === 0 ? 500000 : 2000;
  const res = http.post(`${__ENV.ORDER}/orders`, JSON.stringify({
    customerId: __ENV.TAG,
    items: [{ sku: __ENV.SKU, quantity: 1 }],
    amountCents: amount, currency: 'USD',
  }), { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `${__ENV.TAG}-${i}` } });
  check(res, { 'created': (r) => r.status === 201 });
}
