import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate } from "k6/metrics";

// Cenario: 50 usuarios concorrentes por 2 minutos (conforme guia do projeto)
export const options = {
  vus: 50,
  duration: "2m",
};

const BASE_URL = __ENV.API_BASE_URL || "http://localhost:3000";

const latencyW1 = new Trend("latency_workload1_ms", true);
const latencyW2 = new Trend("latency_workload2_ms", true);
const latencyW3 = new Trend("latency_workload3_ms", true);
const errorRate = new Rate("error_rate");

// IDs de usuarios presentes no datasample (1..150)
const USER_IDS = Array.from({ length: 150 }, (_, i) => i + 1);

function randomUserId() {
  return USER_IDS[Math.floor(Math.random() * USER_IDS.length)];
}

export default function () {
  const userId = randomUserId();

  // WORKLOAD-1: recomendacao por genero
  let res1 = http.get(`${BASE_URL}/recommend?userId=${userId}&limit=10`);
  latencyW1.add(res1.timings.duration);
  errorRate.add(res1.status !== 200 && res1.status !== 404);
  check(res1, { "W1 status ok": (r) => r.status === 200 || r.status === 404 });

  sleep(0.1);

  // WORKLOAD-2: filtragem colaborativa
  let res2 = http.get(`${BASE_URL}/recommend/collaborative?userId=${userId}&limit=10`);
  latencyW2.add(res2.timings.duration);
  errorRate.add(res2.status !== 200 && res2.status !== 404);
  check(res2, { "W2 status ok": (r) => r.status === 200 || r.status === 404 });

  sleep(0.1);

  // WORKLOAD-3: tag genome
  let res3 = http.get(`${BASE_URL}/recommend/tag-genome?userId=${userId}&limit=10`);
  latencyW3.add(res3.timings.duration);
  errorRate.add(res3.status !== 200 && res3.status !== 404);
  check(res3, { "W3 status ok": (r) => r.status === 200 || r.status === 404 });

  sleep(0.2);
}
