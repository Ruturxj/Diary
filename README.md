# Personal Diary — Online Full-Stack Backend + Offline-First Suite (`Ruturxj`)

## 1. Quick Start (Run Locally or on Any VPS)
No `npm install` is required — `server.mjs` uses pure built-in Node.js modules (`node:http`, `node:crypto`, `node:fs`).

```bash
node server.mjs
```
Open **`http://localhost:3000`** in your browser.

## 2. Online Backend API Architecture (`server.mjs`)
- **`GET /api/health`** — Live server status, uptime, revision counter, and record counts.
- **`GET /api/events`** — Real-time Server-Sent Events (SSE) stream that pushes instant updates across your Phone, Laptop, and Tablet.
- **`GET / POST /api/workspace` & `POST /api/workspace/delta`** — Full persistence and server-side relational sync for Contacts, Document Vault, 7/12 Land, EMIs, Renewals, Credit (CIBIL & Experian), Gym & Fitness, Mantralaya, Daily Log, and Code Studio.
- **`GET / POST /api/attachments`** — Stores uploaded Document Vault PDFs, 7/12 Utara scans, and images directly in `./data/attachments/` on disk.
- **`/api/journal/*` & `/api/portfolio/*` & `/api/prices/*` & `/api/market`** — Complete authentication (Scrypt + IST 2FA), Trade Journal calendar, Portfolio books/entries/holdings, and market data.
- **`GET /api/backup/export` & `POST /api/backup/restore`** — Full server database backup plus automatic daily rolling backups in `./data/backups/`.

## 3. Deploy Online in 2 Minutes
- **Option A — Ubuntu/Debian VPS + DuckDNS (`ruturxj.duckdns.org`)**:
  ```bash
  npm install -g pm2
  pm2 start ecosystem.config.cjs
  pm2 save && pm2 startup
  # For automatic HTTPS with Caddy:
  sudo caddy run --config Caddyfile
  ```
- **Option B — Docker / Docker Compose**:
  ```bash
  docker compose up -d --build
  ```
- **Option C — Render.com / Railway.app (1-Click Cloud Hosting)**:
  Push this folder to a private GitHub repo and connect it to **Render** (uses `render.yaml`) or **Railway** (uses `railway.json`).
