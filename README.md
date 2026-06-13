# mimo-free-proxy

OpenAI-compatible proxy for the MiMo free channel.

The host-level Node.js service is the known-good baseline. The Docker build is a low-memory Go implementation that follows the same native free-channel request shape.

Stable Node fallback tag:

```bash
git checkout stable-node-20260613
```

## Host Run

```bash
cd /opt
git clone https://github.com/csy87704403/mimo-free-proxy.git
cd /opt/mimo-free-proxy

cat > /etc/mimo-free-proxy.env <<'EOF'
HOST=0.0.0.0
PORT=39173
PROXY_API_KEY=replace-with-your-private-key
UPSTREAM_BASE=https://api.xiaomimimo.com
MAX_429_WAIT_MS=180000
DEFAULT_MODEL=mimo-auto
EOF

cp /opt/mimo-free-proxy/mimo-free-proxy.service.example /etc/systemd/system/mimo-free-proxy.service

systemctl daemon-reload
systemctl enable mimo-free-proxy
systemctl start mimo-free-proxy
systemctl status mimo-free-proxy --no-pager
```

If `ufw` is enabled:

```bash
ufw allow 39173/tcp
```

## Host Update

```bash
cd /opt/mimo-free-proxy
git pull
systemctl restart mimo-free-proxy
```

## Host Logs

```bash
journalctl -u mimo-free-proxy -f
```

## Docker Run

The Docker image runs the Go proxy and is intended for lower memory usage.

```bash
cd /opt
git clone https://github.com/csy87704403/mimo-free-proxy.git
cd /opt/mimo-free-proxy

cp .env.example .env
nano .env
```

Set your private key in `.env`:

```text
PROXY_API_KEY=replace-with-your-private-key
```

If native `mimo` already works well on the VPS, reuse its free-channel client id:

```bash
grep '^MIMO_CLIENT=' .env >/dev/null || echo "MIMO_CLIENT=$(cat ~/.local/share/mimocode/mimo-free-client)" >> .env
```

The proxy stores a valid upstream JWT in the Docker volume at `/data/jwt`, so rebuilding the container does not force a new bootstrap every time.

Start the service:

```bash
docker compose up -d --build
docker compose ps
```

If `ufw` is enabled:

```bash
ufw allow 39173/tcp
```

## Test

```bash
curl http://127.0.0.1:39173/health

curl http://127.0.0.1:39173/v1/chat/completions \
  -H "Authorization: Bearer replace-with-your-private-key" \
  -H "Content-Type: application/json" \
  -d '{"model":"mimo-auto","messages":[{"role":"user","content":"hello"}],"stream":false}'
```

Client config:

```text
Base URL: http://YOUR_VPS_IP:39173/v1
API Key: replace-with-your-private-key
Model: mimo-auto
```

## Update

```bash
cd /opt/mimo-free-proxy
git pull
docker compose up -d --build
```

## Roll Back

Return to the stable Node baseline:

```bash
cd /opt/mimo-free-proxy
git fetch --tags
git checkout stable-node-20260613
systemctl restart mimo-free-proxy
```

## Memory

The host-level Node.js service typically uses more memory than the Go Docker build.

Actual memory depends on Docker, kernel accounting, request size, and concurrent requests.

## Uninstall

Host-level service:

```bash
systemctl stop mimo-free-proxy
systemctl disable mimo-free-proxy

rm -f /etc/systemd/system/mimo-free-proxy.service
rm -f /etc/mimo-free-proxy.env

systemctl daemon-reload
systemctl reset-failed
```

Docker experiment:

```bash
cd /opt/mimo-free-proxy
docker compose down -v
```

If `ufw` was used to open the port:

```bash
ufw delete allow 39173/tcp
```

Remove project files after the service is stopped:

```bash
rm -rf /opt/mimo-free-proxy
```

Verify removal:

```bash
curl http://127.0.0.1:39173/health
```
