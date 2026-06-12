import base64
import hashlib
import http.client
import http.server
import json
import os
import socketserver
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse


MAX_BODY_BYTES = 20 * 1024 * 1024


def env(name, default=""):
    return os.environ.get(name) or default


class Config:
    host = env("HOST", "0.0.0.0")
    port = int(env("PORT", "39173"))
    proxy_api_key = env("PROXY_API_KEY")
    upstream_base = env("UPSTREAM_BASE", "https://api.xiaomimimo.com").rstrip("/")
    upstream_user_agent = env("UPSTREAM_USER_AGENT", "Python-urllib/3")
    default_model = env("DEFAULT_MODEL", "mimo-auto")
    max_429_wait_ms = int(env("MAX_429_WAIT_MS", "180000"))
    allow_custom_model = env("ALLOW_CUSTOM_MODEL") == "1"
    mimo_client = env("MIMO_CLIENT")
    client_file = env("CLIENT_FILE", "/data/client")
    jwt_file = env("JWT_FILE", "/data/jwt")


if not Config.proxy_api_key:
    raise SystemExit("PROXY_API_KEY is required. Refusing to start an open proxy.")


def load_client_id():
    if Config.mimo_client.strip():
        return Config.mimo_client.strip()

    candidates = [
        Path.home() / ".local" / "share" / "mimocode" / "mimo-free-client",
        Path(Config.client_file),
        Path("/data/client"),
    ]
    for path in candidates:
        try:
            value = path.read_text(encoding="utf-8").strip()
            if value:
                return value
        except OSError:
            pass

    seed = "|".join(
        [
            os.uname().nodename if hasattr(os, "uname") else "unknown-host",
            os.name,
            os.environ.get("USER", "unknown-user"),
        ]
    )
    value = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    try:
        path = Path(Config.client_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8")
        path.chmod(0o600)
    except OSError:
        pass
    return value


CLIENT_ID = load_client_id()
CACHED_JWT = ""
CACHED_JWT_EXP = 0.0


def jwt_exp(jwt):
    try:
        payload = jwt.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        exp = data.get("exp")
        if isinstance(exp, (int, float)):
            return float(exp)
    except Exception:
        pass
    return time.time() + 1800


def read_jwt_file():
    try:
        jwt = Path(Config.jwt_file).read_text(encoding="utf-8").strip()
    except OSError:
        return "", 0.0
    return jwt, jwt_exp(jwt)


def write_jwt_file(jwt):
    if not jwt:
        return
    try:
        path = Path(Config.jwt_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(jwt, encoding="utf-8")
        path.chmod(0o600)
    except OSError:
        pass


def backoff_seconds(attempt):
    steps = [20, 45, 90, 180, 300]
    return steps[min(attempt, len(steps) - 1)]


def retry_after_seconds(value, attempt):
    if value:
        try:
            seconds = float(value)
            if seconds > 0:
                return seconds
        except ValueError:
            pass
    return backoff_seconds(attempt)


def upstream_request(path, body, headers, timeout=120):
    url = Config.upstream_base + path
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl.create_default_context()) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def bootstrap_jwt(force=False):
    global CACHED_JWT, CACHED_JWT_EXP

    now = time.time()
    if not force and CACHED_JWT and CACHED_JWT_EXP - now > 300:
        return CACHED_JWT

    if not force:
        jwt, exp = read_jwt_file()
        if jwt and exp - now > 300:
            CACHED_JWT = jwt
            CACHED_JWT_EXP = exp
            return jwt

    started = time.time()
    payload = json.dumps({"client": CLIENT_ID}, separators=(",", ":")).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": Config.upstream_user_agent,
    }

    attempt = 0
    while True:
        status, resp_headers, data = upstream_request("/api/free-ai/bootstrap", payload, headers)
        if status == 429:
            delay = retry_after_seconds(resp_headers.get("Retry-After"), attempt)
            if (time.time() - started + delay) * 1000 > Config.max_429_wait_ms:
                raise RuntimeError(f"bootstrap failed 429: {data.decode('utf-8', 'replace')}")
            print(f"bootstrap got 429, waiting {int(delay)}s before retry", flush=True)
            time.sleep(delay)
            attempt += 1
            continue
        if status < 200 or status >= 300:
            raise RuntimeError(f"bootstrap failed {status}: {data.decode('utf-8', 'replace')}")
        parsed = json.loads(data)
        jwt = parsed.get("jwt")
        if not jwt:
            raise RuntimeError("bootstrap response missing jwt")
        CACHED_JWT = jwt
        CACHED_JWT_EXP = jwt_exp(jwt)
        write_jwt_file(jwt)
        return jwt


def upstream_chat(body):
    jwt = bootstrap_jwt()
    started = time.time()
    attempt = 0

    while True:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {jwt}",
            "X-Mimo-Source": "mimocode-cli-free",
            "User-Agent": Config.upstream_user_agent,
        }
        status, resp_headers, data = upstream_request("/api/free-ai/openai/chat", body, headers, timeout=300)
        if status in (401, 403):
            jwt = bootstrap_jwt(force=True)
            continue
        if status != 429:
            return status, resp_headers, data
        delay = retry_after_seconds(resp_headers.get("Retry-After"), attempt)
        if (time.time() - started + delay) * 1000 > Config.max_429_wait_ms:
            return status, resp_headers, data
        print(f"chat got 429, waiting {int(delay)}s before retry", flush=True)
        time.sleep(delay)
        attempt += 1


def normalize_body(raw):
    raw = raw.lstrip(b"\xef\xbb\xbf")
    body = json.loads(raw.decode("utf-8"))
    if not isinstance(body, dict):
        raise ValueError("request body must be a JSON object")
    if not Config.allow_custom_model or not body.get("model"):
        body["model"] = Config.default_model
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "mimo-free-proxy-python/1.0"

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "authorization, content-type, x-api-key")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            return self.write_json(200, {"ok": True, "upstream": Config.upstream_base})
        if not self.authorized():
            return self.write_json(401, {"error": {"message": "Unauthorized"}})
        if self.path in ("/v1/models", "/models"):
            return self.write_json(
                200,
                {
                    "object": "list",
                    "data": [{"id": Config.default_model, "object": "model", "owned_by": "mimo-free-proxy"}],
                },
            )
        self.write_json(404, {"error": {"message": "Not found"}})

    def do_POST(self):
        if not self.authorized():
            return self.write_json(401, {"error": {"message": "Unauthorized"}})
        if self.path not in ("/v1/chat/completions", "/chat/completions", "/api/free-ai/openai/chat"):
            return self.write_json(404, {"error": {"message": "Not found"}})

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > MAX_BODY_BYTES:
                return self.write_json(413, {"error": {"message": "request body too large"}})
            raw = self.rfile.read(length)
            body = normalize_body(raw)
            status, resp_headers, data = upstream_chat(body)
            self.send_response(status)
            self.send_header("Access-Control-Allow-Origin", "*")
            content_type = resp_headers.get("Content-Type", "application/json")
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:
            self.write_json(502, {"error": {"message": str(exc)}})

    def authorized(self):
        return (
            self.headers.get("Authorization") == f"Bearer {Config.proxy_api_key}"
            or self.headers.get("X-Api-Key") == Config.proxy_api_key
        )

    def write_json(self, status, value):
        data = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}", flush=True)


class ThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def main():
    print(f"mimo-free-proxy listening on http://{Config.host}:{Config.port}", flush=True)
    print(f"client prefix: {CLIENT_ID[:12]}", flush=True)
    ThreadingServer((Config.host, Config.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
