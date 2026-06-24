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

func TestChatRequestAcceptsObjectToolArguments(t *testing.T) {
	body := []byte(`{
		"model":"mimo-auto",
		"messages":[
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_write","type":"function","function":{"name":"Write","arguments":{"file_path":"result.txt","content":"ok"}}}]},
			{"role":"tool","tool_call_id":"call_write","content":"created"}
		]
	}`)
	var input chatRequest
	if err := json.Unmarshal(body, &input); err != nil {
		t.Fatal(err)
	}
	got := input.Messages[0].ToolCalls[0].Function.Arguments
	if got != `{"file_path":"result.txt","content":"ok"}` {
		t.Fatalf("arguments=%q", got)
	}
}

func TestChatRequestAcceptsFlattenedToolCall(t *testing.T) {
	body := []byte(`{
		"model":"mimo-auto",
		"messages":[
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_write","type":"function","name":"Write","arguments":{"file_path":"result.txt"}}]},
			{"role":"tool","tool_call_id":"call_write","content":[{"type":"text","text":"created"}]}
		]
	}`)
	var input chatRequest
	if err := json.Unmarshal(body, &input); err != nil {
		t.Fatal(err)
	}
	call := input.Messages[0].ToolCalls[0]
	if call.Function.Name != "Write" || call.Function.Arguments != `{"file_path":"result.txt"}` {
		t.Fatalf("tool call=%#v", call)
	}
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
	updated := append(append([]chatMessage{}, input.Messages...), assistantMessage(result))
	if got := srv.mgr.session(conversationHash(updated)); got != fake.session {
		t.Fatalf("continued session mapping=%q", got)
	}
}

func TestToolCallbackDoesNotDependOnMimoPartEvent(t *testing.T) {
	fake := newFakeMimo(t)
	srv, _ := newBridgeForTest(t, fake)
	events, err := srv.mgr.openEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()

	go func() {
		time.Sleep(20 * time.Millisecond)
		pending := &pendingTool{callID: "call_direct", sessionID: fake.session, name: "bash", arguments: `{"command":"pwd"}`, result: make(chan string, 1), created: time.Now()}
		srv.mgr.mu.Lock()
		srv.mgr.pending[pending.callID] = pending
		srv.mgr.mu.Unlock()
		select {
		case srv.mgr.pendingSig <- struct{}{}:
		default:
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := srv.consumeEvents(ctx, httptest.NewRecorder(), chatRequest{Model: "mimo-auto"}, events, fake.session)
	if err != nil {
		t.Fatal(err)
	}
	if result.toolCall == nil || result.toolCall.ID != "call_direct" || result.finish != "tool_calls" {
		t.Fatalf("unexpected tool result: %#v", result)
	}
}

func TestParallelToolCallbacksAreTakenInOrder(t *testing.T) {
	now := time.Now()
	mgr := &manager{pending: map[string]*pendingTool{
		"second": {callID: "second", sessionID: "session", created: now.Add(time.Second)},
		"first":  {callID: "first", sessionID: "session", created: now},
	}}
	if got := mgr.takePendingTool("session"); got == nil || got.callID != "first" {
		t.Fatalf("first callback=%#v", got)
	}
	if got := mgr.takePendingTool("session"); got == nil || got.callID != "second" {
		t.Fatalf("second callback=%#v", got)
	}
	if got := mgr.takePendingTool("session"); got != nil {
		t.Fatalf("callback emitted twice: %#v", got)
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
	if !strings.Contains(prompt["system"].(string), "use relative paths only") {
		t.Fatal("relative-path guidance missing from system prompt")
	}
}

func TestBuildPromptConvertsOpenAIImageURL(t *testing.T) {
	content, _ := json.Marshal([]any{
		map[string]any{"type": "text", "text": "read the code"},
		map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64,iVBORw0KGgo="}},
	})
	input := chatRequest{Messages: []chatMessage{{Role: "user", Content: content}}}
	count, err := validateImageParts(input.Messages)
	if err != nil || count != 1 {
		t.Fatalf("image validation count=%d err=%v", count, err)
	}
	prompt := buildPrompt(input, false)
	parts := prompt["parts"].([]any)
	if len(parts) != 2 {
		t.Fatalf("prompt parts=%#v", parts)
	}
	image := parts[1].(map[string]any)
	if image["type"] != "file" || image["mime"] != "image/png" || image["filename"] != "image.png" {
		t.Fatalf("image part=%#v", image)
	}
}

func TestBuildPromptKeepsImagesInFullHistory(t *testing.T) {
	content, _ := json.Marshal([]any{
		map[string]any{"type": "text", "text": "describe"},
		map[string]any{"type": "image_url", "image_url": map[string]any{"url": "https://example.com/sample.webp"}},
	})
	input := chatRequest{Messages: []chatMessage{
		{Role: "user", Content: content},
		{Role: "assistant", Content: json.RawMessage(`"a picture"`)},
	}}
	prompt := buildPrompt(input, true)
	parts := prompt["parts"].([]any)
	if len(parts) != 3 {
		t.Fatalf("history parts=%#v", parts)
	}
	if got := parts[0].(map[string]any)["text"]; got != "USER:\ndescribe" {
		t.Fatalf("history prefix=%q", got)
	}
	if got := parts[1].(map[string]any)["mime"]; got != "image/webp" {
		t.Fatalf("history image MIME=%q", got)
	}
}

func TestRejectsInvalidImageURL(t *testing.T) {
	content, _ := json.Marshal([]any{
		map[string]any{"type": "text", "text": "describe"},
		map[string]any{"type": "image_url", "image_url": map[string]any{"url": "file:///tmp/image.png"}},
	})
	_, err := validateImageParts([]chatMessage{{Role: "user", Content: content}})
	if err == nil || !strings.Contains(err.Error(), "HTTP(S)") {
		t.Fatalf("invalid image error=%v", err)
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

func TestInternalToolURLAlwaysUsesLoopback(t *testing.T) {
	if got := internalToolURL("39173"); got != "http://127.0.0.1:39173/internal/tool/call" {
		t.Fatalf("internal tool URL=%q", got)
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
