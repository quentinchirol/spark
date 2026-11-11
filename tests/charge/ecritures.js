// Test de charge des écritures : RATE écritures/s pendant DUREE, via la passerelle (POST /ecritures).
// Mélange réaliste : 70 % INSERT (nouvel id), 25 % UPDATE et 5 % DELETE d'un id déjà créé par le même
// utilisateur virtuel (ordre respecté par id). Objectif US-6 : p99 < 500 ms et moins de 0,1 % d'échecs.
//
//   k6 run -e URL=http://passerelle:8000 -e RATE=10000 -e DUREE=5m tests/charge/ecritures.js      (GKE)
//   k6 run -e URL=http://localhost:18000 -e RATE=200 -e DUREE=30s tests/charge/ecritures.js       (kind, CI)
// La table « charge » doit exister : SELECT lake.creer_table('charge', 'client text, montant numeric(12,2)');
import http from 'k6/http';
import { check } from 'k6';

const URL = __ENV.URL || 'http://passerelle:8000';
const RATE = parseInt(__ENV.RATE || '10000', 10);
const DUREE = __ENV.DUREE || '5m';

export const options = {
  scenarios: {
    pic: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DUREE,
      preAllocatedVUs: Math.min(Math.max(RATE / 5, 10), 2000),
      maxVUs: Math.min(Math.max(RATE, 50), 5000),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    http_req_duration: ['p(99)<500'],
  },
};

const mesIds = [];   // ids créés par cet utilisateur virtuel (état propre à chaque VU)

function uuidv4() {
  const h = [];
  for (let i = 0; i < 32; i++) h.push(Math.floor(Math.random() * 16).toString(16));
  h[12] = '4';
  h[16] = ['8', '9', 'a', 'b'][Math.floor(Math.random() * 4)];
  const s = h.join('');
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}

export default function () {
  const r = Math.random();
  let message;
  if (r < 0.70 || mesIds.length === 0) {
    const id = uuidv4();
    mesIds.push(id);
    message = { id, op: 'INSERT', donnees: { id, client: `c${__VU}`, montant: 10 } };
  } else if (r < 0.95) {
    const id = mesIds[Math.floor(Math.random() * mesIds.length)];
    message = { id, op: 'UPDATE', donnees: { id, client: `c${__VU}`, montant: Math.round(Math.random() * 1000) } };
  } else {
    const id = mesIds.splice(Math.floor(Math.random() * mesIds.length), 1)[0];
    message = { id, op: 'DELETE', donnees: {} };
  }
  message.table = 'charge';
  message.date_creation = '2026-11-11';
  const res = http.post(`${URL}/ecritures`, JSON.stringify(message), { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'accepté (202)': (x) => x.status === 202 });
}
