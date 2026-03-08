import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = "http://localhost:8000";

export const options = {
  stages: [
    { duration: "30s", target: 10 },
    { duration: "1m",  target: 10 },
    { duration: "30s", target: 0 },
  ],
};

const headers = { "Content-Type": "application/json" };

const payloads = [
  { seller_id: 1, is_verified_seller: true,  item_id: 1, name: "Good Ad",      description: "Verified seller, many images, legit product", category: 5,  images_qty: 8 },
  { seller_id: 2, is_verified_seller: false, item_id: 2, name: "Suspicious",   description: "x",                                          category: 99, images_qty: 0 },
  { seller_id: 3, is_verified_seller: true,  item_id: 3, name: "Normal Ad",    description: "A regular advertisement with normal length",  category: 10, images_qty: 4 },
  { seller_id: 4, is_verified_seller: false, item_id: 4, name: "Cheap iPhone", description: "Brand new iPhone for $10 only!!!",            category: 50, images_qty: 1 },
];

export default function () {
  const base = payloads[Math.floor(Math.random() * payloads.length)];
  const uniqueSuffix = `${__VU}-${__ITER}-${Date.now()}`;
  const p = Object.assign({}, base, {
    description: base.description + " " + uniqueSuffix,
  });

  const predictRes = http.post(`${BASE_URL}/predict`, JSON.stringify(p), { headers });
  check(predictRes, { "predict 200": (r) => r.status === 200 });

  const simpleRes = http.post(`${BASE_URL}/simple_predict?item_id=1`);
  check(simpleRes, { "simple_predict ok": (r) => r.status === 200 || r.status === 404 });

  const notFound = http.post(`${BASE_URL}/simple_predict?item_id=999999`);
  check(notFound, { "not found 404": (r) => r.status === 404 });

  sleep(0.5);
}