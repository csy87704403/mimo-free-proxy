package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxBodyBytes = 20 * 1024 * 1024

type config struct {
	host           string
	port           string
	proxyAPIKey    string
	upstreamBase   string
	upstreamUA     string
	defaultModel   string
	max429Wait     time.Duration
	allowCustom    bool
	clientOverride string
	clientFile     string
}

type server struct {
	cfg      config
	client   *http.Client
	clientID string
	jwt      string
	jwtExp   time.Time
	jwtMu    sync.Mutex
}

func main() {
	cfg := loadConfig()
	if cfg.proxyAPIKey == "" {
		log.Fatal("PROXY_API_KEY is required. Refusing to start an open proxy.")
	}

	srv := &server{
		cfg: cfg,
		client: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				Proxy:             http.ProxyFromEnvironment,
				ForceAttemptHTTP2: false,
				TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
			},
		},
	}
	clientID, err := srv.stableClient()
	if err != nil {
		log.Fatalf("client id: %v", err)
	}
	srv.clientID = clientID

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handle)

	addr := cfg.host + ":" + cfg.port
	log.Printf("mimo-free-proxy listening on http://%s", addr)
	log.Printf("client prefix: %s", prefix(clientID, 12))
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func loadConfig() config {
	return config{
		host:           env("HOST", "0.0.0.0"),
		port:           env("PORT", "39173"),
		proxyAPIKey:    os.Getenv("PROXY_API_KEY"),
		upstreamBase:   strings.TrimRight(env("UPSTREAM_BASE", "https://api.xiaomimimo.com"), "/"),
		upstreamUA:     env("UPSTREAM_USER_AGENT", "Bun/1.3.14"),
		defaultModel:   env("DEFAULT_MODEL", "mimo-auto"),
		max429Wait:     durationMs(env("MAX_429_WAIT_MS", "180000")),
		allowCustom:    os.Getenv("ALLOW_CUSTOM_MODEL") == "1",
		clientOverride: os.Getenv("MIMO_CLIENT"),
		clientFile:     os.Getenv("CLIENT_FILE"),
	}
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		writeRaw(w, http.StatusNoContent, map[string]string{
			"Access-Control-Allow-Origin":  "*",
			"Access-Control-Allow-Headers": "authorization, content-type, x-api-key",
			"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
		}, nil)
		return
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "upstream": s.cfg.upstreamBase})
	case !s.authorized(r):
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]string{"message": "Unauthorized"}})
	case r.Method == http.MethodGet && (r.URL.Path == "/v1/models" || r.URL.Path == "/models"):
		writeJSON(w, http.StatusOK, map[string]any{
			"object": "list",
			"data": []map[string]any{{
				"id":       s.cfg.defaultModel,
				"object":   "model",
				"owned_by": "mimo-free-proxy",
			}},
		})
	case r.Method == http.MethodPost && isChatPath(r.URL.Path):
		s.handleChat(w, r)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]string{"message": "Not found"}})
	}
}

func (s *server) authorized(r *http.Request) bool {
	return r.Header.Get("Authorization") == "Bearer "+s.cfg.proxyAPIKey || r.Header.Get("X-Api-Key") == s.cfg.proxyAPIKey
}

func isChatPath(path string) bool {
	switch path {
	case "/v1/chat/completions", "/chat/completions", "/api/free-ai/openai/chat":
		return true
	default:
		return false
	}
}

func (s *server) handleChat(w http.ResponseWriter, r *http.Request) {
	body, err := readLimitedBody(r.Body, maxBodyBytes)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"message": err.Error()}})
		return
	}
	body, err = normalizeChatBody(body, s.cfg.defaultModel, s.cfg.allowCustom)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"message": err.Error()}})
		return
	}

	resp, err := s.upstreamChat(r.Context(), body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]string{"message": err.Error()}})
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *server) upstreamChat(ctx context.Context, body []byte) (*http.Response, error) {
	jwt, err := s.bootstrapJWT(ctx, false)
	if err != nil {
		return nil, err
	}

	started := time.Now()
	for attempt := 0; ; attempt++ {
		resp, err := s.postChat(ctx, jwt, body)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			resp.Body.Close()
			jwt, err = s.bootstrapJWT(ctx, true)
			if err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}

		delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
		resp.Body.Close()
		if time.Since(started)+delay > s.cfg.max429Wait {
			return s.postChat(ctx, jwt, body)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (s *server) postChat(ctx context.Context, jwt string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.upstreamBase+"/api/free-ai/openai/chat", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("X-Mimo-Source", "mimocode-cli-free")
	req.Header.Set("User-Agent", s.cfg.upstreamUA)
	return s.client.Do(req)
}

func (s *server) bootstrapJWT(ctx context.Context, force bool) (string, error) {
	s.jwtMu.Lock()
	defer s.jwtMu.Unlock()

	if !force && s.jwt != "" && time.Until(s.jwtExp) > 5*time.Minute {
		return s.jwt, nil
	}

	payload, _ := json.Marshal(map[string]string{"client": s.clientID})

	started := time.Now()
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.upstreamBase+"/api/free-ai/bootstrap", bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", s.cfg.upstreamUA)

		resp, err := s.client.Do(req)
		if err != nil {
			return "", err
		}

		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			if time.Since(started)+delay > s.cfg.max429Wait {
				return "", fmt.Errorf("bootstrap failed %d: %s", resp.StatusCode, string(text))
			}
			log.Printf("bootstrap got 429, waiting %s before retry", delay.Round(time.Second))
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("bootstrap failed %d: %s", resp.StatusCode, string(text))
		}

		var out struct {
			JWT string `json:"jwt"`
		}
		if err := json.Unmarshal(text, &out); err != nil {
			return "", err
		}
		if out.JWT == "" {
			return "", errors.New("bootstrap response missing jwt")
		}

		s.jwt = out.JWT
		s.jwtExp = jwtExp(out.JWT)
		return s.jwt, nil
	}
}

func normalizeChatBody(body []byte, defaultModel string, allowCustom bool) ([]byte, error) {
	body = bytes.TrimPrefix(body, []byte{0xEF, 0xBB, 0xBF})
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, err
	}
	model, _ := value["model"].(string)
	if model == "" || !allowCustom {
		value["model"] = defaultModel
	}
	return json.Marshal(value)
}

func (s *server) stableClient() (string, error) {
	if s.cfg.clientOverride != "" {
		return strings.TrimSpace(s.cfg.clientOverride), nil
	}

	files := []string{}
	if s.cfg.clientFile != "" {
		files = append(files, s.cfg.clientFile)
	}
	if home, err := os.UserHomeDir(); err == nil {
		files = append(files,
			filepath.Join(home, ".local", "share", "mimocode", "mimo-free-client"),
			filepath.Join(home, ".local", "share", "mimo-free-proxy", "client"),
		)
	}
	files = append(files, "/data/client")

	for _, file := range files {
		if file == "" {
			continue
		}
		if value, err := os.ReadFile(file); err == nil {
			if text := strings.TrimSpace(string(value)); text != "" {
				return text, nil
			}
		}
	}

	seed := stableSeed()
	sum := sha256.Sum256([]byte(seed))
	value := hex.EncodeToString(sum[:])

	target := firstWritable(files)
	if target != "" {
		if err := os.MkdirAll(filepath.Dir(target), 0700); err == nil {
			_ = os.WriteFile(target, []byte(value), 0600)
		}
	}
	return value, nil
}

func stableSeed() string {
	hostname, _ := os.Hostname()
	username := "unknown-user"
	if current, err := user.Current(); err == nil && current.Username != "" {
		username = current.Username
	}
	return strings.Join([]string{hostname, runtime.GOOS, runtime.GOARCH, username}, "|")
}

func firstWritable(files []string) string {
	for _, file := range files {
		if file != "" && strings.HasPrefix(file, "/data/") {
			return file
		}
	}
	for _, file := range files {
		if file != "" {
			return file
		}
	}
	return ""
}

func readLimitedBody(r io.Reader, limit int64) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, io.LimitReader(r, limit+1)); err != nil {
		return nil, err
	}
	if int64(buf.Len()) > limit {
		return nil, fmt.Errorf("request body too large")
	}
	return buf.Bytes(), nil
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	if date, err := http.ParseTime(value); err == nil {
		if delay := time.Until(date); delay > 0 {
			return delay
		}
	}
	steps := []time.Duration{20 * time.Second, 45 * time.Second, 90 * time.Second, 180 * time.Second, 300 * time.Second}
	if attempt >= len(steps) {
		return steps[len(steps)-1]
	}
	return steps[attempt]
}

func jwtExp(jwt string) time.Time {
	parts := strings.Split(jwt, ".")
	if len(parts) < 2 {
		return time.Now().Add(30 * time.Minute)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Now().Add(30 * time.Minute)
	}
	var data struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &data) == nil && data.Exp > 0 {
		return time.Unix(data.Exp, 0)
	}
	return time.Now().Add(30 * time.Minute)
}

func copyHeaders(dst, src http.Header) {
	blocked := map[string]bool{
		"Connection":        true,
		"Content-Length":    true,
		"Transfer-Encoding": true,
		"Content-Encoding":  true,
	}
	for key, values := range src {
		if blocked[key] {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	payload, _ := json.Marshal(body)
	writeRaw(w, status, map[string]string{"Content-Type": "application/json; charset=utf-8"}, payload)
}

func writeRaw(w http.ResponseWriter, status int, headers map[string]string, body []byte) {
	for key, value := range headers {
		w.Header().Set(key, value)
	}
	w.WriteHeader(status)
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationMs(value string) time.Duration {
	ms, err := strconv.Atoi(value)
	if err != nil || ms <= 0 {
		return 180 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

func prefix(value string, n int) string {
	if len(value) <= n {
		return value
	}
	return value[:n]
}
