import { SQL, RedisClient } from "bun";

const env = process.env;
const PORT = Number(env.PORT ?? 8080);

const db = new SQL(
  `mysql://${env.DB_USER}:${env.DB_PASS}@${env.DB_HOST}:${env.DB_PORT ?? "3306"}/${env.DB_NAME}`,
);
const redis = new RedisClient(
  `redis://${env.REDIS_HOST}:${env.REDIS_PORT ?? "6379"}/${env.REDIS_DB ?? "0"}`,
);

type User = { id: number; name: string; status: string };

async function createUser(name: string): Promise<User> {
  const res = await db`INSERT INTO users (name) VALUES (${name})`;
  const id = Number(res.lastInsertRowid);
  const user: User = { id, name, status: "active" };
  await db`INSERT INTO audit_log (user_id, action) VALUES (${id}, 'create')`;
  await redis.set(`user:${id}`, JSON.stringify(user));
  return user;
}

type Result =
  | { ok: true; body: unknown }
  | { ok: false; code: number; body: unknown };

async function deactivateUser(id: number): Promise<Result> {
  const rows = await db`SELECT status FROM users WHERE id = ${id}`;
  if (rows.length === 0) return { ok: false, code: 404, body: { error: "user not found" } };
  if (rows[0].status === "inactive") {
    return { ok: false, code: 409, body: { error: "user already inactive" } };
  }
  await db`UPDATE users SET status = 'inactive' WHERE id = ${id}`;
  await db`INSERT INTO audit_log (user_id, action) VALUES (${id}, 'deactivate')`;
  await redis.del(`user:${id}`);
  return { ok: true, body: { id, status: "inactive" } };
}

const json = (data: unknown, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    if (req.method === "GET" && url.pathname === "/healthz") return json({ ok: true });

    if (req.method === "POST" && url.pathname === "/users") {
      const { name } = (await req.json()) as { name: string };
      return json(await createUser(name));
    }

    const deactivate = url.pathname.match(/^\/users\/(\d+)\/deactivate$/);
    if (req.method === "POST" && deactivate) {
      const r = await deactivateUser(Number(deactivate[1]));
      return r.ok ? json(r.body) : json(r.body, r.code);
    }

    return json({ error: "not found" }, 404);
  },
});

console.log(`listening on ${PORT}`);
