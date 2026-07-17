package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBuildAtomcodeArgsKeepsACPSubcommandLast(t *testing.T) {
	t.Parallel()

	args := buildAtomcodeArgs(ExecOptions{
		Cwd:             "/tmp/work",
		Model:           "deepseek-chat",
		CustomArgs:      []string{"--provider", "openai", "acp", "--dir", "/wrong"},
		ResumeSessionID: "ignored",
	}, slog.Default())

	wantPrefix := []string{
		"--dev",
		"--no-telemetry",
		"--dangerously-skip-permissions",
		"-C",
		"/tmp/work",
		"--model",
		"deepseek-chat",
		"--provider",
		"openai",
	}
	if !slices.Equal(args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("args prefix = %#v, want %#v", args[:len(wantPrefix)], wantPrefix)
	}
	if got := args[len(args)-1]; got != "acp" {
		t.Fatalf("last arg = %q, want acp", got)
	}
	if strings.Contains(strings.Join(args[:len(args)-1], "\x00"), "acp") {
		t.Fatalf("custom acp leaked before subcommand: %#v", args)
	}
	if slices.Contains(args, "/wrong") {
		t.Fatalf("blocked custom --dir value leaked: %#v", args)
	}
}

func TestAtomcodeBackendExecutesACPWithoutPersistingSessionID(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	recordPath := filepath.Join(tmp, "argv.json")
	fakePath := filepath.Join(tmp, "atomcode")
	writeTestExecutable(t, fakePath, []byte(fakeAtomcodeACPScript(recordPath)))

	backend, err := New("atomcode", Config{ExecutablePath: fakePath, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("new atomcode backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "hello", ExecOptions{
		Cwd:     tmp,
		Timeout: 5 * time.Second,
		Model:   "deepseek-chat",
		CustomArgs: []string{
			"--provider",
			"openai",
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	var streamed strings.Builder
	for msg := range session.Messages {
		if msg.Type == MessageText {
			streamed.WriteString(msg.Content)
		}
	}

	select {
	case result := <-session.Result:
		if result.Status != "completed" {
			t.Fatalf("expected completed result, got %q: %s", result.Status, result.Error)
		}
		if result.Output != "hi from atomcode" {
			t.Fatalf("output = %q, want streamed text", result.Output)
		}
		if streamed.String() != "hi from atomcode" {
			t.Fatalf("streamed = %q, want text message", streamed.String())
		}
		if result.SessionID != "" {
			t.Fatalf("SessionID = %q, want empty because AtomCode ACP sessions are process-local", result.SessionID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read argv record: %v", err)
	}
	var argv []string
	if err := json.Unmarshal(data, &argv); err != nil {
		t.Fatalf("unmarshal argv: %v", err)
	}
	if got := argv[len(argv)-1]; got != "acp" {
		t.Fatalf("last argv = %q, want acp; argv=%#v", got, argv)
	}
	for _, want := range []string{"--dev", "--no-telemetry", "--dangerously-skip-permissions", "-C", tmp, "--model", "deepseek-chat", "--provider", "openai"} {
		if !slices.Contains(argv, want) {
			t.Fatalf("argv missing %q: %#v", want, argv)
		}
	}
}

func fakeAtomcodeACPScript(recordPath string) string {
	return `#!/bin/sh
RECORD_PATH=` + recordPath + `
printf '[' > "$RECORD_PATH"
first=1
for arg in "$@"; do
  if [ "$first" = "1" ]; then
    first=0
  else
    printf ',' >> "$RECORD_PATH"
  fi
  escaped=$(printf '%s' "$arg" | sed 's/\\/\\\\/g; s/"/\\"/g')
  printf '"%s"' "$escaped" >> "$RECORD_PATH"
done
printf ']\n' >> "$RECORD_PATH"

while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":false}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"acp-1"}}\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hi from atomcode"}}}}\n'
      printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id"
      exit 0
      ;;
  esac
done
`
}
