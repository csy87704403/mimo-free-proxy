package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeMimo struct {
	t       *testing.T
	server  *httptest.Server
	events  chan map[string]any
	mu      sync.Mutex
	session string
}

func newFakeMimo(t *testing.T) *fakeMimo {
	f := &fakeMimo{t: t, events: make(chan map[string]any, 32), session: "ses_test"}
	mux := http.NewServeMux()
	mux.HandleFunc("/global/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"healthy": true})
	})
	mux.HandleFunc("/session", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"id": f.session})
	})
	mux.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "data: {\"type\":\"server.connected\",\"properties\":{}}\n\n")
		flusher.Flush()
		for {
			select {
			case event := <-f.events:
				data, _ := json.Marshal(event)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	})
	mux.HandleFunc("/session/"+f.session+"/prompt_async", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
		go f.emitText("OK")
	})
	mux.HandleFunc("/session/"+f.session+"/abort", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, true)
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeMimo) emitText(text string) {
	messageID := "msg_test"
	partID := "prt_test"
	f.events <- map[string]any{"type": "message.updated", "properties": map[string]any{"info": map[string]any{
		"id": messageID, "sessionID": f.session, "role": "assistant", "tokens": map[string]any{"input": 10, "output": len(text)},
	}}}
	f.events <- map[string]any{"type": "message.part.updated", "properties": map[string]any{
		"part": map[string]any{"id": partID, "sessionID": f.session, "messageID": messageID, "type": "text", "text": ""},
	}}
	for _, delta := range []string{text[:1], text[1:]} {
		f.events <- map[string]any{"type": "message.part.delta", "properties": map[string]any{
			"sessionID": f.session, "messageID": messageID, "partID": partID, "field": "text", "delta": delta,
		}}
	}
	f.events <- map[string]any{"type": "message.part.updated", "properties": map[string]any{
		"part": map[string]any{"id": partID, "sessionID": f.session, "messageID": messageID, "type": "text", "text": text},
	}}
	f.events <- map[string]any{"type": "session.status", "properties": map[string]any{"sessionID": f.session, "status": map[string]any{"type": "idle"}}}
}

func newBridgeForTest(t *testing.T, fake *fakeMimo) (*server, http.Handler) {
	host, port, err := net.SplitHostPort(strings.TrimPrefix(fake.server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{
		proxyAPIKey:  "test-key",
		defaultModel: "mimo-auto",
		mimoHost:     host,
		mimoPort:     port,
		mimoUsername: "mimocode",
		mimoPassword: "test",
		idleTimeout:  time.Hour,
	}
	mgr := &manager{
		cfg: cfg, httpClient: &http.Client{}, lastUsed: time.Now(), sessions: map[string]string{}, pending: map[string]*pendingTool{}, pendingSig: make(chan struct{}, 1),
	}
	srv := &server{cfg: cfg, mgr: mgr, started: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", srv.authorized(srv.handleChat))
	return srv, mux
}

func chatBody(stream bool) []byte {
	data, _ := json.Marshal(map[string]any{
		"model": "mimo-auto", "stream": stream,
		"messages": []any{map[string]any{"role": "user", "content": "hello"}},
	})
	return data
}

func TestChatCompletion(t *testing.T) {
	fake := newFakeMimo(t)
	_, handler := newBridgeForTest(t, fake)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(chatBody(false)))
	req.Header.Set("Authorization", "Bearer test-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	choices := response["choices"].([]any)
	message := choices[0].(map[string]any)["message"].(map[string]any)
	if message["content"] != "OK" {
		t.Fatalf("content=%v", message["content"])
	}
}

func TestStreamingChatCompletion(t *testing.T) {
	fake := newFakeMimo(t)
	_, handler := newBridgeForTest(t, fake)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(chatBody(true)))
	req.Header.Set("Authorization", "Bearer test-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, `"content":"O"`) || !strings.Contains(body, `"content":"K"`) || !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("unexpected stream: %s", body)
	}
}

func TestToolContinuation(t *testing.T) {
	fake := newFakeMimo(t)
	srv, _ := newBridgeForTest(t, fake)
	pending := &pendingTool{callID: "call_test", sessionID: fake.session, name: "bash", arguments: `{"command":"pwd"}`, result: make(chan string, 1), created: time.Now()}
	srv.mgr.pending[pending.callID] = pending

	go func() {
		result := <-pending.result
		if result != "tool output" {
			t.Errorf("tool result=%q", result)
		}
		fake.emitText("DONE")
	}()

	content, _ := json.Marshal("tool output")
	input := chatRequest{Model: "mimo-auto", Messages: []chatMessage{
		{Role: "assistant", Content: json.RawMessage("null"), ToolCalls: []toolCall{{ID: pending.callID, Type: "function", Function: functionCall{Name: "bash", Arguments: pending.arguments}}}},
		{Role: "tool", Content: content, ToolCallID: pending.callID},
	}}
	result, err := srv.runChat(context.Background(), httptest.NewRecorder(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.content != "DONE" {
		t.Fatalf("content=%q", result.content)
	}
}

func TestBuildPromptExternalTools(t *testing.T) {
	content, _ := json.Marshal("run a command")
	input := chatRequest{
		Messages: []chatMessage{{Role: "user", Content: content}},
		Tools:    []toolDefinition{{Type: "function", Function: toolFunction{Name: "bash", Parameters: map[string]any{"type": "object"}}}},
	}
	prompt := buildPrompt(input, true)
	flags := prompt["tools"].(map[string]bool)
	if !flags["external"] || flags["bash"] {
		t.Fatalf("unexpected tool flags: %#v", flags)
	}
	if !strings.Contains(prompt["system"].(string), `"name":"bash"`) {
		t.Fatal("external tool schema missing from system prompt")
	}
	if !strings.Contains(prompt["system"].(string), "remote bridge host") {
		t.Fatal("external execution environment guidance missing from system prompt")
	}
}

func TestEventScannerHandlesSSE(t *testing.T) {
	scanner := bufio.NewScanner(strings.NewReader("data: {}\n\n"))
	if !scanner.Scan() {
		t.Fatal("missing SSE line")
	}
}

func TestChildEnvironmentUsesDedicatedProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://old-proxy:8080")
	env := childEnvironment("http://127.0.0.1:7890")
	joined := "\n" + strings.Join(env, "\n") + "\n"
	for _, expected := range []string{
		"\nHTTP_PROXY=http://127.0.0.1:7890\n",
		"\nHTTPS_PROXY=http://127.0.0.1:7890\n",
		"\nNO_PROXY=127.0.0.1,localhost,::1\n",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("missing child environment entry %q", expected)
		}
	}
	if strings.Contains(joined, "old-proxy") {
		t.Fatal("inherited proxy was not replaced")
	}
}

func TestEnsureStartedDoesNotDuplicateLiveProcess(t *testing.T) {
	mgr := &manager{
		cfg:        config{mimoBin: "definitely-not-a-real-mimo-binary", mimoHost: "127.0.0.1", mimoPort: "1"},
		httpClient: &http.Client{},
		cmd:        &exec.Cmd{},
	}
	if err := mgr.ensureStarted(context.Background()); err != nil {
		t.Fatalf("existing process should prevent duplicate start: %v", err)
	}
}
