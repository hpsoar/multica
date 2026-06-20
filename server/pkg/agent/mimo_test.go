package agent

import (
	"strings"
	"testing"
)

func TestNewReturnsMimoBackend(t *testing.T) {
	t.Parallel()
	b, err := New("mimo", Config{ExecutablePath: "/nonexistent/mimo"})
	if err != nil {
		t.Fatalf("New(mimo) error: %v", err)
	}
	if _, ok := b.(*mimoBackend); !ok {
		t.Fatalf("expected *mimoBackend, got %T", b)
	}
}

func TestMimoProcessEventsOpenCodeCompatible(t *testing.T) {
	t.Parallel()

	stream := strings.Join([]string{
		`{"type":"step_start","sessionID":"ses_1","part":{"type":"step-start"}}`,
		`{"type":"tool_use","sessionID":"ses_1","part":{"type":"tool","tool":"bash","callID":"call_1","state":{"status":"completed","input":{"command":"pwd"},"output":"/tmp\n"}}}`,
		`{"type":"text","sessionID":"ses_1","part":{"type":"text","text":"MIMO_OK"}}`,
		`{"type":"step_finish","sessionID":"ses_1","part":{"type":"step-finish","tokens":{"input":10,"output":2,"cache":{"read":3,"write":4}}}}`,
	}, "\n")

	b := &mimoBackend{}
	ch := make(chan Message, 10)
	result := b.processEvents(strings.NewReader(stream), ch)

	if result.status != "completed" {
		t.Fatalf("status = %q, want completed", result.status)
	}
	if result.sessionID != "ses_1" {
		t.Fatalf("sessionID = %q, want ses_1", result.sessionID)
	}
	if result.output != "MIMO_OK" {
		t.Fatalf("output = %q, want MIMO_OK", result.output)
	}
	if result.usage.InputTokens != 10 || result.usage.OutputTokens != 2 || result.usage.CacheReadTokens != 3 || result.usage.CacheWriteTokens != 4 {
		t.Fatalf("usage = %+v", result.usage)
	}

	if len(ch) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(ch))
	}
	<-ch // status
	toolUse := <-ch
	if toolUse.Type != MessageToolUse || toolUse.Tool != "bash" || toolUse.CallID != "call_1" {
		t.Fatalf("unexpected tool-use message: %+v", toolUse)
	}
	toolResult := <-ch
	if toolResult.Type != MessageToolResult || toolResult.Output != "/tmp\n" {
		t.Fatalf("unexpected tool-result message: %+v", toolResult)
	}
	text := <-ch
	if text.Type != MessageText || text.Content != "MIMO_OK" {
		t.Fatalf("unexpected text message: %+v", text)
	}
}
