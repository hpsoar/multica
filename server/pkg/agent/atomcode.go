package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// atomcodeBlockedArgs are flags hardcoded by the daemon that must not be
// overridden by user-configured custom_args.
var atomcodeBlockedArgs = map[string]blockedArgMode{
	"acp":                              blockedStandalone,
	"-C":                               blockedWithValue,
	"--dir":                            blockedWithValue,
	"--model":                          blockedWithValue,
	"--dev":                            blockedStandalone,
	"--dangerously-skip-permissions":   blockedStandalone,
	"--dangerously-allow-all-commands": blockedStandalone,
}

// atomcodeBackend implements Backend by spawning `atomcode ... acp` and
// communicating via the standard ACP JSON-RPC 2.0 transport over stdin/stdout.
//
// AtomCode's ACP server keeps sessions in the child process only and advertises
// load_session=false, so Multica starts a fresh ACP session for each task and
// deliberately does not return the process-local `acp-N` id as a resumable
// daemon session.
type atomcodeBackend struct {
	cfg Config
}

func (b *atomcodeBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "atomcode"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("atomcode executable not found at %q: %w", execPath, err)
	}

	if len(opts.McpConfig) > 0 {
		b.cfg.Logger.Warn("atomcode ACP does not accept daemon-managed MCP config; using AtomCode's own config")
	}
	if opts.ResumeSessionID != "" {
		b.cfg.Logger.Warn("atomcode ACP does not support resumable sessions; starting a fresh session", "resume_session_id", opts.ResumeSessionID)
	}
	if opts.MaxTurns > 0 {
		b.cfg.Logger.Warn("atomcode does not support --max-turns; ignoring", "maxTurns", opts.MaxTurns)
	}

	timeout := opts.Timeout
	runCtx, cancel := runContext(ctx, timeout)

	args := buildAtomcodeArgs(opts, b.cfg.Logger)
	cmd := exec.CommandContext(runCtx, execPath, args...)
	hideAgentWindow(cmd)
	b.cfg.Logger.Info("agent command", "exec", execPath, "args", args)
	agentsMDPresent := false
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
		if _, err := os.Stat(filepath.Join(opts.Cwd, "AGENTS.md")); err == nil {
			agentsMDPresent = true
		}
	}
	b.cfg.Logger.Info("atomcode acp starting", "cwd", opts.Cwd, "agents_md_present", agentsMDPresent, "model", opts.Model)
	if opts.SystemPrompt != "" {
		b.cfg.Logger.Debug("atomcode ignoring ExecOptions.SystemPrompt; using cwd-scoped context files", "cwd", opts.Cwd)
	}

	cmd.Env = buildEnv(b.cfg.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("atomcode stdout pipe: %w", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("atomcode stdin pipe: %w", err)
	}
	providerErr := newACPProviderErrorSniffer("atomcode")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("atomcode stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start atomcode: %w", err)
	}

	stderrSink := io.MultiWriter(newLogWriter(b.cfg.Logger, "[atomcode:stderr] "), providerErr)
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		_, _ = io.Copy(stderrSink, stderr)
	}()

	b.cfg.Logger.Info("atomcode acp started", "pid", cmd.Process.Pid, "cwd", opts.Cwd)

	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)

	var outputMu sync.Mutex
	var output strings.Builder
	var streamingCurrentTurn atomic.Bool

	promptDone := make(chan hermesPromptResult, 1)

	c := &hermesClient{
		cfg:          b.cfg,
		stdin:        stdin,
		pending:      make(map[int]*pendingRPC),
		pendingTools: make(map[string]*pendingToolCall),
		acceptNotification: func(string) bool {
			return streamingCurrentTurn.Load()
		},
		onMessage: func(msg Message) {
			if !streamingCurrentTurn.Load() {
				return
			}
			if msg.Type == MessageText {
				outputMu.Lock()
				output.WriteString(msg.Content)
				outputMu.Unlock()
			}
			trySend(msgCh, msg)
		},
		onPromptDone: func(result hermesPromptResult) {
			if !streamingCurrentTurn.Load() {
				return
			}
			select {
			case promptDone <- result:
			default:
			}
		},
	}

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			c.handleLine(line)
		}
		c.closeAllPending(fmt.Errorf("atomcode process exited"))
	}()

	go func() {
		defer cancel()
		defer close(msgCh)
		defer close(resCh)
		defer func() {
			stdin.Close()
			_ = cmd.Wait()
		}()

		startTime := time.Now()
		finalStatus := "completed"
		var finalError string
		var acpSessionID string

		if _, err := c.request(runCtx, "initialize", map[string]any{
			"protocolVersion": 1,
			"clientInfo": map[string]any{
				"name":    "multica-agent-sdk",
				"version": "0.2.0",
			},
			"clientCapabilities": map[string]any{},
		}); err != nil {
			finalStatus = "failed"
			finalError = fmt.Sprintf("atomcode initialize failed: %v", err)
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}

		cwd := opts.Cwd
		if cwd == "" {
			cwd = "."
		}
		result, err := c.request(runCtx, "session/new", map[string]any{
			"cwd": cwd,
		})
		if err != nil {
			finalStatus = "failed"
			finalError = fmt.Sprintf("atomcode session/new failed: %v", err)
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}
		acpSessionID = extractACPSessionID(result)
		if acpSessionID == "" {
			finalStatus = "failed"
			finalError = "atomcode session/new returned no session ID"
			resCh <- Result{Status: finalStatus, Error: finalError, DurationMs: time.Since(startTime).Milliseconds()}
			return
		}

		c.sessionID = acpSessionID
		b.cfg.Logger.Info("atomcode session created", "session_id", acpSessionID)

		streamingCurrentTurn.Store(true)
		_, err = c.request(runCtx, "session/prompt", map[string]any{
			"sessionId": acpSessionID,
			"prompt": []map[string]any{
				{"type": "text", "text": prompt},
			},
		})
		if err != nil {
			if runCtx.Err() == context.DeadlineExceeded {
				finalStatus = "timeout"
				finalError = fmt.Sprintf("atomcode timed out after %s", timeout)
			} else if runCtx.Err() == context.Canceled {
				finalStatus = "aborted"
				finalError = "execution cancelled"
			} else {
				finalStatus = "failed"
				finalError = fmt.Sprintf("atomcode session/prompt failed: %v", err)
			}
		} else {
			select {
			case pr := <-promptDone:
				if pr.stopReason == "cancelled" {
					finalStatus = "aborted"
					finalError = "atomcode cancelled the prompt"
				}
				c.usageMu.Lock()
				c.usage.InputTokens += pr.usage.InputTokens
				c.usage.OutputTokens += pr.usage.OutputTokens
				c.usage.CacheReadTokens += pr.usage.CacheReadTokens
				c.usageMu.Unlock()
			default:
			}
		}

		duration := time.Since(startTime)
		b.cfg.Logger.Info("atomcode finished", "pid", cmd.Process.Pid, "status", finalStatus, "duration", duration.Round(time.Millisecond).String())

		stdin.Close()
		cancel()

		<-readerDone
		<-stderrDone

		outputMu.Lock()
		finalOutput := output.String()
		outputMu.Unlock()

		finalStatus, finalError = promoteACPResultOnProviderError(finalStatus, finalError, finalOutput, providerErr)

		c.usageMu.Lock()
		u := c.usage
		c.usageMu.Unlock()

		var usageMap map[string]TokenUsage
		if u.InputTokens > 0 || u.OutputTokens > 0 || u.CacheReadTokens > 0 {
			model := strings.TrimSpace(opts.Model)
			if model == "" {
				model = "unknown"
			}
			usageMap = map[string]TokenUsage{model: u}
		}

		resCh <- Result{
			Status:     finalStatus,
			Output:     finalOutput,
			Error:      finalError,
			DurationMs: duration.Milliseconds(),
			SessionID:  "",
			Usage:      usageMap,
		}
	}()

	return &Session{Messages: msgCh, Result: resCh}, nil
}

func buildAtomcodeArgs(opts ExecOptions, logger *slog.Logger) []string {
	args := []string{"--dev", "--no-telemetry", "--dangerously-skip-permissions"}
	if opts.Cwd != "" {
		args = append(args, "-C", opts.Cwd)
	}
	if strings.TrimSpace(opts.Model) != "" {
		args = append(args, "--model", opts.Model)
	}
	args = append(args, filterCustomArgs(opts.CustomArgs, atomcodeBlockedArgs, logger)...)
	args = append(args, "acp")
	return args
}
