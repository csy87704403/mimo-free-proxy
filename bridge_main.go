package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxBodyBytes = 20 * 1024 * 1024

//go:embed external-tool.ts
var assets embed.FS

type config struct {
	host          string
	port          string
	proxyAPIKey   string
	defaultModel  string
	mimoBin       string
	mimoHost      string
	mimoPort      string
	mimoUsername  string
	mimoPassword  string
	mimoWorkdir   string
	mimoConfigDir string
	mimoProxyURL  string
	idleTimeout   time.Duration
	startTimeout  time.Duration
	internalKey   string
}

type chatMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  []toolCall      `json:"tool_calls,omitempty"`
}

type toolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function functionCall `json:"function"`
}

type functionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type toolDefinition struct {
	Type     string       `json:"type"`
	Function toolFunction `json:"function"`
}

type toolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

type chatRequest struct {
	Model         string           `json:"model"`
	Messages      []chatMessage    `json:"messages"`
	Tools         []toolDefinition `json:"tools,omitempty"`
	ToolChoice    any              `json:"tool_choice,omitempty"`
	Stream        bool             `json:"stream,omitempty"`
	StreamOptions struct {
		IncludeUsage bool `json:"include_usage,omitempty"`
	} `json:"stream_options,omitempty"`
}

type pendingTool struct {
	callID    string
	sessionID string
	name      string
	arguments string
	result    chan string
	created   time.Time
}

type manager struct {
	cfg        config
	httpClient *http.Client
	mu         sync.Mutex
	cmd        *exec.Cmd
	starting   chan struct{}
	startErr   error
	busy       int
	lastUsed   time.Time
	sessions   map[string]string
	pending    map[string]*pendingTool
	pendingSig chan struct{}
}

type server struct {
	cfg     config
	mgr     *manager
	chatMu  sync.Mutex
	started time.Time
}

type bridgeResult struct {
	content   string
	toolCall  *toolCall
	usage     map[string]int
	finish    string
	sessionID string
}

type streamWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
	id      string
	model   string
	created int64
	started bool
}

func main() {
	cfg := loadConfig()
	if cfg.proxyAPIKey == "" {
		log.Fatal("PROXY_API_KEY is required")
	}
	if err := installToolPlugin(cfg.mimoConfigDir, cfg.mimoProxyURL); err != nil {
		log.Fatalf("install external tool: %v", err)
	}

	mgr := &manager{
		cfg:        cfg,
		httpClient: &http.Client{},
		lastUsed:   time.Now(),
		sessions:   map[string]string{},
		pending:    map[string]*pendingTool{},
		pendingSig: make(chan struct{}, 1),
	}
	srv := &server{cfg: cfg, mgr: mgr, started: time.Now()}
	go mgr.idleLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/v1/models", srv.authorized(srv.handleModels))
	mux.HandleFunc("/models", srv.authorized(srv.handleModels))
	mux.HandleFunc("/v1/chat/completions", srv.authorized(srv.handleChat))
	mux.HandleFunc("/chat/completions", srv.authorized(srv.handleChat))
	mux.HandleFunc("/internal/tool/call", srv.handleInternalTool)

	addr := cfg.host + ":" + cfg.port
	log.Printf("mimo native bridge listening on http://%s", addr)
	log.Printf("mimo starts on demand and stops after %s idle", cfg.idleTimeout)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func loadConfig() config {
	password := os.Getenv("MIMO_SERVER_PASSWORD")
	if password == "" {
		password = randomToken(24)
	}
	internalKey := os.Getenv("MIMO_BRIDGE_INTERNAL_KEY")
	if internalKey == "" {
		internalKey = randomToken(32)
	}
	workdir := env("MIMO_WORKDIR", defaultWorkdir())
	configDir := env("MIMO_CONFIG_HOME", filepath.Join(workdir, ".mimo-bridge-config"))
	return config{
		host:          env("HOST", "127.0.0.1"),
		port:          env("PORT", "39173"),
		proxyAPIKey:   os.Getenv("PROXY_API_KEY"),
		defaultModel:  env("DEFAULT_MODEL", "mimo-auto"),
		mimoBin:       env("MIMO_BIN", "mimo"),
		mimoHost:      "127.0.0.1",
		mimoPort:      env("MIMO_PORT", "39450"),
		mimoUsername:  env("MIMO_SERVER_USERNAME", "mimocode"),
		mimoPassword:  password,
		mimoWorkdir:   workdir,
		mimoConfigDir: configDir,
		mimoProxyURL:  strings.TrimSpace(os.Getenv("MIMO_PROXY_URL")),
		idleTimeout:   durationEnv("MIMO_IDLE_TIMEOUT", 15*time.Minute),
		startTimeout:  durationEnv("MIMO_START_TIMEOUT", 25*time.Second),
		internalKey:   internalKey,
	}
}

func defaultWorkdir() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return cwd
}

func installToolPlugin(configHome, proxyURL string) error {
	data, err := assets.ReadFile("external-tool.ts")
	if err != nil {
		return err
	}
	root := filepath.Join(configHome, "mimocode")
	dir := filepath.Join(root, "tools")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	packagePath := filepath.Join(root, "package.json")
	if _, err := os.Stat(packagePath); errors.Is(err, os.ErrNotExist) {
		packageData := []byte("{\n  \"private\": true,\n  \"dependencies\": {\n    \"@mimo-ai/plugin\": \"0.1.2\"\n  }\n}\n")
		if err := os.WriteFile(packagePath, packageData, 0600); err != nil {
			return err
		}
	}
	path := filepath.Join(dir, "external.ts")
	current, _ := os.ReadFile(path)
	if !bytes.Equal(current, data) {
		if err := os.WriteFile(path, data, 0600); err != nil {
			return err
		}
	}
	pluginPackage := filepath.Join(root, "node_modules", "@mimo-ai", "plugin", "package.json")
	if _, err := os.Stat(pluginPackage); err == nil {
		return nil
	}
	npm, err := exec.LookPath("npm")
	if err != nil {
		return errors.New("@mimo-ai/plugin is missing and npm is not installed")
	}
	cmd := exec.Command(npm, "install", "--omit=dev", "--no-audit", "--no-fund")
	cmd.Dir = root
	cmd.Env = childEnvironment(proxyURL)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (s *server) authorized(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !bearerOK(r, s.cfg.proxyAPIKey) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]string{"message": "Unauthorized"}})
			return
		}
		next(w, r)
	}
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	running := s.mgr.healthy(ctx)
	cancel()
	s.mgr.mu.Lock()
	running = running || s.mgr.cmd != nil
	busy := s.mgr.busy
	s.mgr.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "mimo_running": running, "busy": busy,
		"uptime_seconds": int(time.Since(s.started).Seconds()),
	})
}

func (s *server) handleModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   []map[string]string{{"id": s.cfg.defaultModel, "object": "model", "owned_by": "mimo-native-bridge"}},
	})
}

func (s *server) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": map[string]string{"message": "Method not allowed"}})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil || len(body) > maxBodyBytes {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"message": "Invalid request body"}})
		return
	}
	var input chatRequest
	if err := json.Unmarshal(body, &input); err != nil || len(input.Messages) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"message": "Invalid Chat Completions request"}})
		return
	}
	if input.Model == "" {
		input.Model = s.cfg.defaultModel
	}

	s.chatMu.Lock()
	defer s.chatMu.Unlock()
	s.mgr.markBusy(true)
	defer s.mgr.markBusy(false)
	if err := s.mgr.ensureStarted(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]string{"message": err.Error()}})
		return
	}

	result, err := s.runChat(r.Context(), w, input)
	if err != nil {
		if input.Stream {
			writeStreamError(w, err)
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]string{"message": err.Error()}})
		return
	}
	if input.Stream {
		return
	}
	s.writeCompletion(w, input, result)
}

func (s *server) runChat(ctx context.Context, w http.ResponseWriter, input chatRequest) (bridgeResult, error) {
	if callID, output, ok := lastToolResult(input.Messages); ok {
		pending, err := s.mgr.waitPending(ctx, callID, 5*time.Second)
		if err != nil {
			return bridgeResult{}, err
		}
		events, err := s.mgr.openEvents(ctx)
		if err != nil {
			return bridgeResult{}, err
		}
		defer events.Close()
		select {
		case pending.result <- output:
		case <-ctx.Done():
			return bridgeResult{}, ctx.Err()
		}
		return s.consumeEvents(ctx, w, input, events, pending.sessionID)
	}

	prefix := conversationHash(input.Messages[:len(input.Messages)-1])
	sessionID := s.mgr.session(prefix)
	newSession := sessionID == ""
	if newSession {
		var err error
		sessionID, err = s.mgr.createSession(ctx)
		if err != nil {
			return bridgeResult{}, err
		}
	}

	events, err := s.mgr.openEvents(ctx)
	if err != nil {
		return bridgeResult{}, err
	}
	defer events.Close()
	prompt := buildPrompt(input, newSession)
	if err := s.mgr.promptAsync(ctx, sessionID, input.Model, prompt); err != nil {
		if !newSession && strings.Contains(err.Error(), "404") {
			sessionID, err = s.mgr.createSession(ctx)
			if err == nil {
				events.Close()
				events, err = s.mgr.openEvents(ctx)
				if err == nil {
					defer events.Close()
					err = s.mgr.promptAsync(ctx, sessionID, input.Model, buildPrompt(input, true))
				}
			}
		}
		if err != nil {
			return bridgeResult{}, err
		}
	}
	result, err := s.consumeEvents(ctx, w, input, events, sessionID)
	if err != nil {
		return result, err
	}
	assistant := assistantMessage(result)
	updated := append(append([]chatMessage{}, input.Messages...), assistant)
	s.mgr.remember(conversationHash(updated), sessionID)
	return result, nil
}

type eventStream struct {
	resp    *http.Response
	scanner *bufio.Scanner
}

func (e *eventStream) Close() error { return e.resp.Body.Close() }

func (s *server) consumeEvents(ctx context.Context, w http.ResponseWriter, input chatRequest, events *eventStream, sessionID string) (bridgeResult, error) {
	result := bridgeResult{finish: "stop", sessionID: sessionID, usage: map[string]int{}}
	var sw *streamWriter
	if input.Stream {
		var ok bool
		sw, ok = resultWriter(w, input.Model)
		if !ok {
			return result, errors.New("streaming unsupported by response writer")
		}
		sw.role()
	}
	assistantIDs := map[string]bool{}
	seenParts := map[string]string{}
	partTypes := map[string]string{}
	answerStarted := false

	for events.scanner.Scan() {
		line := events.scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(payload), &event) != nil {
			continue
		}
		if wrapped, ok := event["payload"].(map[string]any); ok {
			event = wrapped
		}
		typ, _ := event["type"].(string)
		props, _ := event["properties"].(map[string]any)
		switch typ {
		case "message.updated":
			info, _ := props["info"].(map[string]any)
			if stringValue(info["sessionID"]) != sessionID || stringValue(info["role"]) != "assistant" {
				continue
			}
			if agentID := stringValue(info["agentID"]); agentID != "" && agentID != "main" {
				continue
			}
			id := stringValue(info["id"])
			if id != "" {
				assistantIDs[id] = true
			}
			if tokens, ok := info["tokens"].(map[string]any); ok {
				result.usage["prompt_tokens"] = intValue(tokens["input"])
				result.usage["completion_tokens"] = intValue(tokens["output"])
				result.usage["total_tokens"] = result.usage["prompt_tokens"] + result.usage["completion_tokens"]
			}
			if rawErr, ok := info["error"].(map[string]any); ok && rawErr != nil {
				return result, errors.New(errorMessage(rawErr))
			}
			if stringValue(info["finish"]) != "" {
				answerStarted = true
			}
		case "message.part.updated":
			part, _ := props["part"].(map[string]any)
			if stringValue(part["sessionID"]) != sessionID || !assistantIDs[stringValue(part["messageID"])] {
				continue
			}
			partID := stringValue(part["id"])
			partType := stringValue(part["type"])
			if partID != "" {
				partTypes[partID] = partType
			}
			switch partType {
			case "text":
				delta := stringValue(props["delta"])
				if delta == "" {
					text := stringValue(part["text"])
					delta = strings.TrimPrefix(text, seenParts[partID])
					seenParts[partID] = text
				}
				if delta != "" {
					answerStarted = true
					result.content += delta
					if sw != nil {
						sw.text(delta)
					}
				}
			case "tool":
				if stringValue(part["tool"]) != "external" {
					continue
				}
				state, _ := part["state"].(map[string]any)
				status := stringValue(state["status"])
				if status != "pending" && status != "running" {
					continue
				}
				inputMap, _ := state["input"].(map[string]any)
				name := stringValue(inputMap["name"])
				arguments := stringValue(inputMap["arguments"])
				if name == "" {
					continue
				}
				pending, err := s.mgr.waitPendingTool(ctx, sessionID, name, 2*time.Second)
				if err != nil {
					continue
				}
				callID := pending.callID
				call := &toolCall{ID: callID, Type: "function", Function: functionCall{Name: name, Arguments: normalizeArguments(arguments)}}
				result.toolCall = call
				answerStarted = true
				result.finish = "tool_calls"
				if sw != nil {
					sw.tool(*call)
					sw.finish("tool_calls")
					sw.done()
				}
				return result, nil
			}
		case "message.part.delta":
			if stringValue(props["sessionID"]) != sessionID || !assistantIDs[stringValue(props["messageID"])] {
				continue
			}
			partID := stringValue(props["partID"])
			if partTypes[partID] != "text" || stringValue(props["field"]) != "text" {
				continue
			}
			delta := stringValue(props["delta"])
			if delta == "" {
				continue
			}
			seenParts[partID] += delta
			answerStarted = true
			result.content += delta
			if sw != nil {
				sw.text(delta)
			}
		case "session.status":
			if stringValue(props["sessionID"]) != sessionID {
				continue
			}
			status, _ := props["status"].(map[string]any)
			if stringValue(status["type"]) == "idle" && answerStarted {
				if sw != nil {
					sw.finish("stop")
					if input.StreamOptions.IncludeUsage {
						sw.usage(result.usage)
					}
					sw.done()
				}
				return result, nil
			}
		case "session.idle":
			if stringValue(props["sessionID"]) == sessionID && answerStarted {
				if sw != nil {
					sw.finish("stop")
					sw.done()
				}
				return result, nil
			}
		case "session.error":
			if stringValue(props["sessionID"]) == sessionID {
				return result, fmt.Errorf("mimo session error: %v", props["error"])
			}
		}
		select {
		case <-ctx.Done():
			_ = s.mgr.abortSession(context.Background(), sessionID)
			return result, ctx.Err()
		default:
		}
	}
	if err := events.scanner.Err(); err != nil {
		return result, err
	}
	return result, errors.New("mimo event stream closed before completion")
}

func (s *server) writeCompletion(w http.ResponseWriter, input chatRequest, result bridgeResult) {
	message := map[string]any{"role": "assistant", "content": result.content}
	if result.toolCall != nil {
		message["content"] = nil
		message["tool_calls"] = []toolCall{*result.toolCall}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": randomID("chatcmpl_"), "object": "chat.completion", "created": time.Now().Unix(), "model": input.Model,
		"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": result.finish}},
		"usage":   result.usage,
	})
}

func resultWriter(w http.ResponseWriter, model string) (*streamWriter, bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, false
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	return &streamWriter{w: w, flusher: flusher, id: randomID("chatcmpl_"), model: model, created: time.Now().Unix()}, true
}

func (s *streamWriter) send(choices any, usage any) {
	payload := map[string]any{"id": s.id, "object": "chat.completion.chunk", "created": s.created, "model": s.model, "choices": choices}
	if usage != nil {
		payload["usage"] = usage
	}
	data, _ := json.Marshal(payload)
	_, _ = fmt.Fprintf(s.w, "data: %s\n\n", data)
	s.flusher.Flush()
}

func (s *streamWriter) role() {
	s.send([]any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "content": ""}, "finish_reason": nil}}, nil)
}

func (s *streamWriter) text(text string) {
	s.send([]any{map[string]any{"index": 0, "delta": map[string]any{"content": text}, "finish_reason": nil}}, nil)
}

func (s *streamWriter) tool(call toolCall) {
	s.send([]any{map[string]any{"index": 0, "delta": map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": call.ID, "type": "function", "function": call.Function}}}, "finish_reason": nil}}, nil)
}

func (s *streamWriter) finish(reason string) {
	s.send([]any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": reason}}, nil)
}

func (s *streamWriter) usage(usage map[string]int) { s.send([]any{}, usage) }

func (s *streamWriter) error(err error) {
	data, _ := json.Marshal(map[string]any{"error": map[string]string{"message": err.Error()}})
	_, _ = fmt.Fprintf(s.w, "data: %s\n\n", data)
	s.flusher.Flush()
}

func (s *streamWriter) done() {
	_, _ = io.WriteString(s.w, "data: [DONE]\n\n")
	s.flusher.Flush()
}

func writeStreamError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	data, _ := json.Marshal(map[string]any{"error": map[string]string{"message": err.Error()}})
	_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", data)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (m *manager) ensureStarted(ctx context.Context) error {
	if m.healthy(ctx) {
		return nil
	}
	m.mu.Lock()
	if m.cmd != nil {
		m.mu.Unlock()
		return nil
	}
	if m.starting != nil {
		wait := m.starting
		m.mu.Unlock()
		select {
		case <-wait:
			m.mu.Lock()
			err := m.startErr
			m.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	wait := make(chan struct{})
	m.starting = wait
	m.startErr = nil
	m.mu.Unlock()

	err := m.startProcess(ctx)
	m.mu.Lock()
	m.startErr = err
	m.starting = nil
	close(wait)
	m.mu.Unlock()
	return err
}

func (m *manager) startProcess(ctx context.Context) error {
	if err := os.MkdirAll(m.cfg.mimoWorkdir, 0700); err != nil {
		return err
	}
	cmd := exec.Command(m.cfg.mimoBin, "serve", "--hostname", m.cfg.mimoHost, "--port", m.cfg.mimoPort)
	prepareChild(cmd)
	cmd.Dir = m.cfg.mimoWorkdir
	cmd.Env = append(childEnvironment(m.cfg.mimoProxyURL),
		"BUN_OPTIONS=--smol",
		"MIMOCODE_SERVER_PASSWORD="+m.cfg.mimoPassword,
		"XDG_CONFIG_HOME="+m.cfg.mimoConfigDir,
		"MIMO_BRIDGE_TOOL_URL=http://"+m.cfg.host+":"+m.cfg.port+"/internal/tool/call",
		"MIMO_BRIDGE_INTERNAL_KEY="+m.cfg.internalKey,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start mimo: %w", err)
	}
	m.mu.Lock()
	m.cmd = cmd
	m.lastUsed = time.Now()
	m.mu.Unlock()
	go func() {
		err := cmd.Wait()
		m.mu.Lock()
		if m.cmd == cmd {
			m.cmd = nil
		}
		m.mu.Unlock()
		if err != nil {
			log.Printf("mimo exited: %v", err)
		}
	}()

	deadline := time.Now().Add(m.cfg.startTimeout)
	for time.Now().Before(deadline) {
		if m.healthy(ctx) {
			log.Printf("mimo ready on %s", m.baseURL())
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	_ = cmd.Process.Kill()
	return fmt.Errorf("mimo did not become ready within %s", m.cfg.startTimeout)
}

func childEnvironment(proxyURL string) []string {
	base := os.Environ()
	if proxyURL == "" {
		return base
	}
	proxyKeys := map[string]bool{
		"HTTP_PROXY": true, "HTTPS_PROXY": true, "ALL_PROXY": true, "NO_PROXY": true,
	}
	filtered := make([]string, 0, len(base)+8)
	for _, entry := range base {
		name, _, _ := strings.Cut(entry, "=")
		if !proxyKeys[strings.ToUpper(name)] {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered,
		"HTTP_PROXY="+proxyURL,
		"HTTPS_PROXY="+proxyURL,
		"ALL_PROXY="+proxyURL,
		"http_proxy="+proxyURL,
		"https_proxy="+proxyURL,
		"all_proxy="+proxyURL,
		"NO_PROXY=127.0.0.1,localhost,::1",
		"no_proxy=127.0.0.1,localhost,::1",
	)
}

func (m *manager) healthy(ctx context.Context) bool {
	checkCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	req, _ := m.request(checkCtx, http.MethodGet, "/global/health", nil)
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (m *manager) createSession(ctx context.Context) (string, error) {
	body := map[string]any{"permission": []any{
		map[string]string{"permission": "question", "action": "deny", "pattern": "*"},
		map[string]string{"permission": "plan_enter", "action": "deny", "pattern": "*"},
		map[string]string{"permission": "plan_exit", "action": "deny", "pattern": "*"},
	}}
	var out map[string]any
	if err := m.doJSON(ctx, http.MethodPost, "/session", body, &out); err != nil {
		return "", err
	}
	id := stringValue(out["id"])
	if id == "" {
		return "", errors.New("mimo session response missing id")
	}
	return id, nil
}

func (m *manager) openEvents(ctx context.Context) (*eventStream, error) {
	req, _ := m.request(ctx, http.MethodGet, "/event", nil)
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("mimo event stream failed %d: %s", resp.StatusCode, text)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	return &eventStream{resp: resp, scanner: scanner}, nil
}

func (m *manager) promptAsync(ctx context.Context, sessionID, model string, prompt map[string]any) error {
	prompt["model"] = map[string]string{"providerID": "mimo", "modelID": model}
	path := "/session/" + sessionID + "/prompt_async"
	req, err := m.request(ctx, http.MethodPost, path, prompt)
	if err != nil {
		return err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("mimo prompt failed %d: %s", resp.StatusCode, text)
	}
	return nil
}

func (m *manager) abortSession(ctx context.Context, sessionID string) error {
	return m.doJSON(ctx, http.MethodPost, "/session/"+sessionID+"/abort", map[string]any{}, nil)
}

func (m *manager) request(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, m.baseURL()+path, reader)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(m.cfg.mimoUsername, m.cfg.mimoPassword)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func (m *manager) doJSON(ctx context.Context, method, path string, body, out any) error {
	req, err := m.request(ctx, method, path, body)
	if err != nil {
		return err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("mimo request failed %d: %s", resp.StatusCode, text)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (m *manager) baseURL() string { return "http://" + m.cfg.mimoHost + ":" + m.cfg.mimoPort }

func (m *manager) markBusy(active bool) {
	m.mu.Lock()
	if active {
		m.busy++
	} else if m.busy > 0 {
		m.busy--
	}
	m.lastUsed = time.Now()
	m.mu.Unlock()
}

func (m *manager) idleLoop() {
	interval := 5 * time.Second
	if m.cfg.idleTimeout < interval {
		interval = m.cfg.idleTimeout
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		cmd := m.cmd
		shouldStop := cmd != nil && m.busy == 0 && m.starting == nil && time.Since(m.lastUsed) >= m.cfg.idleTimeout
		m.mu.Unlock()
		if shouldStop {
			log.Printf("stopping mimo after %s idle", m.cfg.idleTimeout)
			_ = signalChild(cmd)
			time.Sleep(2 * time.Second)
			m.mu.Lock()
			stillRunning := m.cmd == cmd
			m.mu.Unlock()
			if stillRunning {
				_ = killChild(cmd)
			}
		}
	}
}

func (m *manager) session(hash string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[hash]
}

func (m *manager) remember(hash, sessionID string) {
	if hash == "" || sessionID == "" {
		return
	}
	m.mu.Lock()
	if len(m.sessions) > 512 {
		m.sessions = map[string]string{}
	}
	m.sessions[hash] = sessionID
	m.mu.Unlock()
}

func (m *manager) waitPending(ctx context.Context, callID string, timeout time.Duration) (*pendingTool, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		m.mu.Lock()
		pending := m.pending[callID]
		m.mu.Unlock()
		if pending != nil {
			return pending, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("external tool call %s is no longer pending", callID)
		case <-m.pendingSig:
		}
	}
}

func (m *manager) waitPendingTool(ctx context.Context, sessionID, name string, timeout time.Duration) (*pendingTool, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		m.mu.Lock()
		var match *pendingTool
		for _, pending := range m.pending {
			if pending.sessionID == sessionID && pending.name == name {
				match = pending
				break
			}
		}
		m.mu.Unlock()
		if match != nil {
			return match, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, errors.New("external tool callback did not arrive")
		case <-m.pendingSig:
		}
	}
}

func (s *server) handleInternalTool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.Header.Get("X-Mimo-Bridge-Key") != s.cfg.internalKey {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	var input struct {
		SessionID string `json:"sessionID"`
		MessageID string `json:"messageID"`
		CallID    string `json:"callID"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 1024*1024)).Decode(&input) != nil || input.SessionID == "" || input.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tool request"})
		return
	}
	callID := input.CallID
	if callID == "" {
		callID = toolCallID(input.SessionID, input.MessageID, input.Name, input.Arguments)
	}
	pending := &pendingTool{callID: callID, sessionID: input.SessionID, name: input.Name, arguments: normalizeArguments(input.Arguments), result: make(chan string, 1), created: time.Now()}
	s.mgr.mu.Lock()
	s.mgr.pending[callID] = pending
	s.mgr.mu.Unlock()
	select {
	case s.mgr.pendingSig <- struct{}{}:
	default:
	}

	select {
	case result := <-pending.result:
		s.mgr.mu.Lock()
		delete(s.mgr.pending, callID)
		s.mgr.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, result)
	case <-r.Context().Done():
		s.mgr.mu.Lock()
		delete(s.mgr.pending, callID)
		s.mgr.mu.Unlock()
	}
}

func buildPrompt(input chatRequest, fullHistory bool) map[string]any {
	system := []string{}
	for _, message := range input.Messages {
		if message.Role == "system" || message.Role == "developer" {
			if text := messageText(message); text != "" {
				system = append(system, text)
			}
		}
	}
	if len(input.Tools) > 0 {
		system = append(system, externalToolsPrompt(input.Tools))
	}
	var text string
	if fullHistory {
		parts := []string{}
		for _, message := range input.Messages {
			if message.Role == "system" || message.Role == "developer" {
				continue
			}
			parts = append(parts, strings.ToUpper(message.Role)+":\n"+messageText(message))
		}
		text = strings.Join(parts, "\n\n")
	} else {
		text = messageText(input.Messages[len(input.Messages)-1])
	}
	toolFlags := map[string]bool{
		"bash": false, "read": false, "glob": false, "grep": false, "edit": false, "write": false,
		"actor": false, "webfetch": false, "skill": false, "change_directory": false, "memory": false,
		"history": false, "task": false, "workflow": false, "external": len(input.Tools) > 0,
	}
	return map[string]any{
		"system": strings.Join(system, "\n\n"),
		"tools":  toolFlags,
		"parts":  []any{map[string]any{"type": "text", "text": text}},
	}
}

func externalToolsPrompt(tools []toolDefinition) string {
	data, _ := json.Marshal(tools)
	return "The caller provides external tools. Do not use local filesystem, shell, memory, web, task, or workflow tools. When a tool is needed, call the external tool with the exact tool name and a JSON-encoded arguments string. Available external tool definitions:\n" + string(data)
}

func assistantMessage(result bridgeResult) chatMessage {
	if result.toolCall != nil {
		content := json.RawMessage("null")
		return chatMessage{Role: "assistant", Content: content, ToolCalls: []toolCall{*result.toolCall}}
	}
	data, _ := json.Marshal(result.content)
	return chatMessage{Role: "assistant", Content: data}
}

func lastToolResult(messages []chatMessage) (string, string, bool) {
	if len(messages) == 0 {
		return "", "", false
	}
	last := messages[len(messages)-1]
	if last.Role != "tool" || last.ToolCallID == "" {
		return "", "", false
	}
	return last.ToolCallID, messageText(last), true
}

func messageText(message chatMessage) string {
	if len(message.Content) == 0 || string(message.Content) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(message.Content, &text) == nil {
		return text
	}
	var parts []map[string]any
	if json.Unmarshal(message.Content, &parts) == nil {
		values := []string{}
		for _, part := range parts {
			if stringValue(part["type"]) == "text" {
				values = append(values, stringValue(part["text"]))
			}
		}
		return strings.Join(values, "\n")
	}
	return string(message.Content)
}

func conversationHash(messages []chatMessage) string {
	data, _ := json.Marshal(messages)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func toolCallID(sessionID, messageID, name, arguments string) string {
	sum := sha256.Sum256([]byte(sessionID + "\x00" + messageID + "\x00" + name + "\x00" + arguments))
	return "call_" + hex.EncodeToString(sum[:12])
}

func normalizeArguments(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "{}"
	}
	var parsed any
	if json.Unmarshal([]byte(value), &parsed) == nil {
		data, _ := json.Marshal(parsed)
		return string(data)
	}
	data, _ := json.Marshal(map[string]string{"value": value})
	return string(data)
}

func errorMessage(value map[string]any) string {
	if data, ok := value["data"].(map[string]any); ok {
		if message := stringValue(data["message"]); message != "" {
			return message
		}
	}
	data, _ := json.Marshal(value)
	return string(data)
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func intValue(value any) int {
	switch number := value.(type) {
	case float64:
		return int(number)
	case json.Number:
		value, _ := number.Int64()
		return int(value)
	case int:
		return number
	default:
		return 0
	}
}

func bearerOK(r *http.Request, expected string) bool {
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	return value == "Bearer "+expected
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
		return parsed
	}
	if ms, err := strconv.Atoi(value); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return fallback
}

func randomToken(size int) string {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func randomID(prefix string) string { return prefix + randomToken(12) }
