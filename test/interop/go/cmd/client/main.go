// A2A Go SDK client over gRPC, driven by the CT suite.
//
// usage: client <host:port> <scenario>
//
// Builds an Agent Card naming a single GRPC interface at the given
// target, creates an SDK client bound to that binding and runs one
// scenario against the suite's agent. Every step prints one JSON object
// per line on stdout; the Erlang side asserts on those. Exit code 0
// means the scenario ran to the end.
//
// This mirrors test/interop/a2a_grpc_client.py step for step and field
// for field: the suite runs the same assertions against every language.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const timeout = 40 * time.Second

func emit(fields map[string]any) {
	line, err := json.Marshal(fields)
	if err != nil {
		fail(err)
	}
	fmt.Println(string(line))
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
}

func partsText(parts a2a.ContentParts) string {
	var b strings.Builder
	for _, p := range parts {
		if p == nil {
			continue
		}
		if t, ok := p.Content.(a2a.Text); ok {
			b.WriteString(string(t))
		}
	}
	return b.String()
}

func artifactText(t *a2a.Task) string {
	if t == nil {
		return ""
	}
	var b strings.Builder
	for _, a := range t.Artifacts {
		b.WriteString(partsText(a.Parts))
	}
	return b.String()
}

func newClient(ctx context.Context, target string) *a2aclient.Client {
	endpoints := []*a2a.AgentInterface{
		a2a.NewAgentInterface(target, a2a.TransportProtocolGRPC),
	}
	c, err := a2aclient.NewFromEndpoints(ctx, endpoints,
		a2agrpc.WithGRPCTransport(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		fail(fmt.Errorf("create client: %w", err))
	}
	return c
}

func message(text string, taskID a2a.TaskID, returnImmediately bool) *a2a.SendMessageRequest {
	m := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text))
	m.TaskID = taskID
	req := &a2a.SendMessageRequest{Message: m}
	if returnImmediately {
		req.Config = &a2a.SendMessageConfig{ReturnImmediately: true}
	}
	return req
}

func kindOf(ev a2a.Event) string {
	switch ev.(type) {
	case *a2a.Task:
		return "task"
	case *a2a.Message:
		return "message"
	case *a2a.TaskStatusUpdateEvent:
		return "status_update"
	case *a2a.TaskArtifactUpdateEvent:
		return "artifact_update"
	default:
		return "empty"
	}
}

// A blocking send answers with one event; the suite reports it as a
// single-entry list so both shapes read the same.
func send(ctx context.Context, c *a2aclient.Client, req *a2a.SendMessageRequest) []a2a.Event {
	res, err := c.SendMessage(ctx, req)
	if err != nil {
		fail(fmt.Errorf("send: %w", err))
	}
	return []a2a.Event{res}
}

func stream(ctx context.Context, c *a2aclient.Client, req *a2a.SendMessageRequest) []a2a.Event {
	var events []a2a.Event
	for ev, err := range c.SendStreamingMessage(ctx, req) {
		if err != nil {
			fail(fmt.Errorf("stream: %w", err))
		}
		events = append(events, ev)
	}
	return events
}

func kinds(events []a2a.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, kindOf(e))
	}
	return out
}

func lastTask(events []a2a.Event) *a2a.Task {
	var task *a2a.Task
	for _, e := range events {
		switch v := e.(type) {
		case *a2a.Task:
			task = v
		case *a2a.TaskStatusUpdateEvent:
			if task != nil {
				task.Status = v.Status
			}
		case *a2a.TaskArtifactUpdateEvent:
			if task != nil {
				task.Artifacts = append(task.Artifacts, v.Artifact)
			}
		}
	}
	return task
}

// The artifact reassembled from every chunk the stream carried.
func streamedText(events []a2a.Event) string {
	var b strings.Builder
	for _, e := range events {
		if u, ok := e.(*a2a.TaskArtifactUpdateEvent); ok {
			b.WriteString(partsText(u.Artifact.Parts))
		}
	}
	return b.String()
}

func scenarioSend(ctx context.Context, c *a2aclient.Client) {
	events := send(ctx, c, message("echo: interop", "", false))
	task := lastTask(events)
	emit(map[string]any{
		"step": "send", "kinds": kinds(events),
		"state": task.Status.State.String(), "text": artifactText(task),
	})
}

func scenarioStream(ctx context.Context, c *a2aclient.Client) {
	events := stream(ctx, c, message("stream", "", false))
	states := []string{}
	for _, e := range events {
		if u, ok := e.(*a2a.TaskStatusUpdateEvent); ok {
			states = append(states, u.Status.State.String())
		}
	}
	emit(map[string]any{
		"step": "stream", "kinds": kinds(events),
		"states": states, "text": streamedText(events),
	})
}

func scenarioMultiturn(ctx context.Context, c *a2aclient.Client) {
	events := send(ctx, c, message("ask", "", false))
	task := lastTask(events)
	emit(map[string]any{
		"step": "ask", "state": task.Status.State.String(), "task_id": string(task.ID),
	})
	done := lastTask(send(ctx, c, message("second", task.ID, false)))
	emit(map[string]any{
		"step": "follow_up", "state": done.Status.State.String(),
		"task_id": string(done.ID), "text": artifactText(done),
	})
}

func scenarioCancel(ctx context.Context, c *a2aclient.Client) {
	task := lastTask(send(ctx, c, message("cancel-me", "", true)))
	emit(map[string]any{"step": "started", "task_id": string(task.ID)})
	cancelled, err := c.CancelTask(ctx, &a2a.CancelTaskRequest{ID: task.ID})
	if err != nil {
		fail(fmt.Errorf("cancel: %w", err))
	}
	emit(map[string]any{"step": "cancel", "state": cancelled.Status.State.String()})
	read, err := c.GetTask(ctx, &a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		fail(fmt.Errorf("get after cancel: %w", err))
	}
	emit(map[string]any{"step": "get", "state": read.Status.State.String()})
}

func scenarioGet(ctx context.Context, c *a2aclient.Client) {
	task := lastTask(send(ctx, c, message("echo: fetch me", "", false)))
	read, err := c.GetTask(ctx, &a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		fail(fmt.Errorf("get: %w", err))
	}
	emit(map[string]any{
		"step": "get", "state": read.Status.State.String(),
		"text": artifactText(read), "same_id": read.ID == task.ID,
	})
}

func scenarioDirect(ctx context.Context, c *a2aclient.Client) {
	events := stream(ctx, c, message("direct", "", false))
	var b strings.Builder
	for _, e := range events {
		if m, ok := e.(*a2a.Message); ok {
			b.WriteString(partsText(m.Parts))
		}
	}
	emit(map[string]any{"step": "direct", "kinds": kinds(events), "text": b.String()})
}

// A missing task must arrive as the A2A error type, not a bare status.
func scenarioError(ctx context.Context, c *a2aclient.Client) {
	if _, err := c.GetTask(ctx, &a2a.GetTaskRequest{ID: "no-such-task"}); err != nil {
		emit(map[string]any{
			"step": "error", "error": a2a.ErrorReason(err), "text": err.Error(),
		})
		return
	}
	emit(map[string]any{"step": "error", "error": "none", "text": ""})
}

func main() {
	args := os.Args[1:]
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: client <host:port> <scenario>")
		os.Exit(2)
	}
	target, name := args[0], args[1]

	scenarios := map[string]func(context.Context, *a2aclient.Client){
		"send":      scenarioSend,
		"stream":    scenarioStream,
		"multiturn": scenarioMultiturn,
		"cancel":    scenarioCancel,
		"get":       scenarioGet,
		"direct":    scenarioDirect,
		"error":     scenarioError,
	}
	run, ok := scenarios[name]
	if !ok {
		fmt.Fprintln(os.Stderr, "usage: client <host:port> <scenario>")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	run(ctx, newClient(ctx, target))
}
