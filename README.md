# mimo-free-proxy

OpenAI-compatible proxy for the MiMo free channel.

## Run On VPS

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

## Test

```bash
curl http://127.0.0.1:39173/health

curl http://127.0.0.1:39173/v1/chat/completions \
  -H "Authorization: Bearer replace-with-your-private-key" \
  -H "Content-Type: application/json" \
  -d '{"model":"mimo-auto","messages":[{"role":"user","content":"你好"}],"stream":false}'
```

Client config:

```text
Base URL: http://YOUR_VPS_IP:39173/v1
API Key: replace-with-your-private-key
Model: mimo-auto
```

## Uninstall

```bash
systemctl stop mimo-free-proxy
systemctl disable mimo-free-proxy

rm -f /etc/systemd/system/mimo-free-proxy.service
rm -f /etc/mimo-free-proxy.env
rm -rf /opt/mimo-free-proxy

systemctl daemon-reload
systemctl reset-failed
```

If `ufw` was used to open the port:

```bash
ufw delete allow 39173/tcp
```

Verify removal:

```bash
systemctl status mimo-free-proxy --no-pager
curl http://127.0.0.1:39173/health
```
