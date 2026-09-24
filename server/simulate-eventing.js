// Simule la fonction Eventing : envoie quelques événements à POST /hook.
// Usage : HOOK_TOKEN=... node simulate-eventing.js [url]

const url = process.argv[2] ?? "http://localhost:3000/hook";
const token = process.env.HOOK_TOKEN ?? "change-me";

const events = [
  { type: "mutation", id: "order::1001", cas: "1727170000000000000", doc: { type: "order", status: "created", amount: 42.5 } },
  { type: "mutation", id: "order::1002", cas: "1727170000000000001", doc: { type: "order", status: "created", amount: 13 } },
  { type: "mutation", id: "order::1001", cas: "1727170000000000002", doc: { type: "order", status: "shipped", amount: 42.5 } },
  { type: "deletion", id: "order::1002", cas: "1727170000000000003" },
];

for (const evt of events) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: JSON.stringify(evt),
  });
  console.log(evt.type, evt.id, "->", res.status, await res.text());
  await new Promise((r) => setTimeout(r, 1000));
}
