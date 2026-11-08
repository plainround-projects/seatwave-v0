// k6 2.3.0 script (checked 8 November 2026). Same mix as LoadGenerator: 70% seat maps, 30% holds.
import http from 'k6/http';
import { check } from 'k6';

const SECTIONS = 'ABCDEFGHIJKLMNOPQRST';
const SEATS_PER_SECTION = 1000;
const HOLD_SHARE = 0.3;
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    onsale_ramp: {
      // Open model: k6 starts iterations at the target rate even when the server slows down.
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { target: 100, duration: '20s' },
        { target: 200, duration: '20s' },
        { target: 400, duration: '20s' },
        { target: 800, duration: '20s' },
        { target: 1600, duration: '20s' },
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<300'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const section = SECTIONS[Math.floor(Math.random() * SECTIONS.length)];
  if (Math.random() < HOLD_SHARE) {
    const seatNumber = 1 + Math.floor(Math.random() * SEATS_PER_SECTION);
    const response = http.post(`${BASE_URL}/holds`,
      { eventId: '1', seat: `${section}-${seatNumber}`, customer: `fan${__VU}_${__ITER}` },
      { timeout: '2s', responseCallback: http.expectedStatuses(201, 409) });
    check(response, { 'hold answered': (reply) => reply.status === 201 || reply.status === 409 });
  } else {
    const response = http.get(`${BASE_URL}/events/1/seats?section=${section}`, { timeout: '2s' });
    check(response, { 'seat map ok': (reply) => reply.status === 200 });
  }
}
