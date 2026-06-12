# mimo-free-proxy

OpenAI-compatible proxy for the MiMo free channel. The default implementation is a small Go binary packaged with Docker.

## Docker Run

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

The proxy defaults to a Bun-like upstream user agent to match native mimocode more closely:

```text
UPSTREAM_USER_AGENT=Bun/1.3.14
```

If native `mimo` already works well on the VPS, reuse its free-channel client id:

```bash
grep '^MIMO_CLIENT=' .env >/dev/null || echo "MIMO_CLIENT=$(cat ~/.local/share/mimocode/mimo-free-client)" >> .env
```

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

## Memory

The Go container is designed to stay small. Typical idle RSS should be much lower than the old Node.js version. The Docker image sets:

```text
GOMEMLIMIT=24MiB
GOGC=50
```

Actual memory depends on Docker, kernel accounting, request size, and concurrent requests.

## Uninstall

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
