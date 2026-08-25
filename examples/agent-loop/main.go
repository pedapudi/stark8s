// Command agent-loop runs a small agent graph as a cyclic workload.
//
//	agent    holds one conversation per record key (the thread). It plans
//	         the next step from the remaining question, calls a tool by
//	         emitting the thread state to that tool's channel, and finalises
//	         by emitting to "answers".
//	search   a stub search tool: returns a canned snippet for a term.
//	calc     a stub calculator: evaluates "<int> <op> <int>".
//
// The planner is rule based so that the example is deterministic; the point
// of the example is the graph. A question is a list of segments separated by
// ";". A segment "search: <term>" or "calc: <expr>" is a tool call; a segment
// containing "confirm" parks the thread on "ask-human" until a record with
// the same key arrives on "human"; any other segment ends the plan and the
// steps so far are the answer. The segment "search: again" is never
// consumed, so a thread with that question loops until the feedback channel
// bound diverts it to the overflow channel.
//
// All state travels in the record value. Nothing is kept in the agent pod
// between hops, so any replica that owns the thread's partition can take the
// next hop.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/pedapudi/stark8s/pkg/sdk"
)

// State is the whole conversation of one thread, carried on every record.
type State struct {
	Thread string `json:"thread"`
	// Question is the part of the question that has not been planned yet.
	Question string `json:"question"`
	// Steps is the trace of tool calls, results, and human replies so far.
	Steps []string `json:"steps"`
	// Scratch carries the argument of the pending tool call on the way to
	// the tool and the tool's result on the way back. On "human" it carries
	// the human's reply text.
	Scratch string `json:"scratch"`
}

// loopSegment is the one question segment the planner never consumes.
const loopSegment = "search: again"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: agent-loop agent|search|calc")
	}
	w, err := sdk.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	var h sdk.Handlers
	switch os.Args[1] {
	case "agent":
		h.OnRecord = agentRecord
	case "search":
		h.OnRecord = toolRecord("search-results", runSearch)
	case "calc":
		h.OnRecord = toolRecord("calc-results", runCalc)
	default:
		log.Fatalf("unknown operation %q", os.Args[1])
	}
	if err := w.Run(context.Background(), h); err != nil {
		log.Fatal(err)
	}
}

// hop logs one edge traversal so the flow of a thread is visible in pod logs.
func hop(w *sdk.Worker, thread string, epoch int32, channel, what string) {
	log.Printf("op=%s thread=%s epoch=%d channel=%s %s", w.Operation, thread, epoch, channel, what)
}

// agentRecord handles every inbound channel of the agent operation.
func agentRecord(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
	var s State
	switch r.Channel {
	case "prompts":
		// A prompt's value is either a bare string or a State object.
		var q string
		if err := json.Unmarshal(r.Value, &q); err != nil {
			if err := json.Unmarshal(r.Value, &s); err != nil {
				return fmt.Errorf("thread %s: prompt is neither a string nor a state: %w", r.Key, err)
			}
			q = s.Question
		}
		s = State{Thread: r.Key, Question: q}
		hop(w, r.Key, r.Epoch, r.Channel, "received prompt "+strconv.Quote(q))
	case "search-results", "calc-results":
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return err
		}
		s.Steps = append(s.Steps, s.Scratch)
		s.Scratch = ""
		hop(w, r.Key, r.Epoch, r.Channel, "received tool result "+strconv.Quote(s.Steps[len(s.Steps)-1]))
	case "human":
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return err
		}
		s.Thread = r.Key
		reply := s.Scratch
		if reply == "" {
			reply = "approved"
		}
		s.Steps = append(s.Steps, "human: "+reply)
		s.Scratch = ""
		hop(w, r.Key, r.Epoch, r.Channel, "received human reply "+strconv.Quote(reply))
		return finalize(w, s)
	default:
		return fmt.Errorf("unexpected channel %s", r.Channel)
	}
	return plan(w, s)
}

// plan takes the next segment of the remaining question and emits the thread
// to the channel that handles it.
func plan(w *sdk.Worker, s State) error {
	segment, rest := nextSegment(s.Question)
	switch {
	case strings.HasPrefix(segment, "search:"):
		if segment != loopSegment {
			s.Question = rest
		}
		s.Scratch = strings.TrimSpace(strings.TrimPrefix(segment, "search:"))
		s.Steps = append(s.Steps, "search("+s.Scratch+")")
		hop(w, s.Thread, w.Epoch(), "to-search", "emit "+strconv.Quote(s.Scratch))
		return w.Emit("to-search", s.Thread, s)
	case strings.HasPrefix(segment, "calc:"):
		s.Question = rest
		s.Scratch = strings.TrimSpace(strings.TrimPrefix(segment, "calc:"))
		s.Steps = append(s.Steps, "calc("+s.Scratch+")")
		hop(w, s.Thread, w.Epoch(), "to-calc", "emit "+strconv.Quote(s.Scratch))
		return w.Emit("to-calc", s.Thread, s)
	case strings.Contains(segment, "confirm"):
		s.Question = rest
		s.Steps = append(s.Steps, "ask-human("+segment+")")
		s.Scratch = segment
		hop(w, s.Thread, w.Epoch(), "ask-human", "emit; thread parked until a record with this key arrives on human")
		return w.Emit("ask-human", s.Thread, s)
	default:
		return finalize(w, s)
	}
}

// finalize ends the thread: the joined steps become the answer.
func finalize(w *sdk.Worker, s State) error {
	s.Question = ""
	s.Scratch = strings.Join(s.Steps, " -> ")
	hop(w, s.Thread, w.Epoch(), "answers", "emit "+strconv.Quote(s.Scratch))
	return w.Emit("answers", s.Thread, s)
}

// nextSegment splits "a; b; c" into "a" and "b; c".
func nextSegment(q string) (segment, rest string) {
	segment, rest, _ = strings.Cut(q, ";")
	return strings.TrimSpace(segment), strings.TrimSpace(rest)
}

// toolRecord builds the handler of a tool operation: run the tool on the
// state's scratch argument and send the state back on the result channel
// under the same thread key.
func toolRecord(result string, run func(arg string) string) func(context.Context, *sdk.Worker, sdk.Record) error {
	return func(ctx context.Context, w *sdk.Worker, r sdk.Record) error {
		var s State
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return err
		}
		hop(w, r.Key, r.Epoch, r.Channel, "received "+strconv.Quote(s.Scratch))
		s.Scratch = run(s.Scratch)
		hop(w, r.Key, w.Epoch(), result, "emit "+strconv.Quote(s.Scratch))
		return w.Emit(result, r.Key, s)
	}
}

// snippets is the whole corpus of the stub search tool.
var snippets = map[string]string{
	"stark8s":   "stark8s runs graphs of operations connected by channels on Kubernetes.",
	"langgraph": "LangGraph builds stateful agent applications as graphs of nodes and edges.",
	"pagerank":  "PageRank ranks vertices by the stationary distribution of a random walk.",
}

func runSearch(term string) string {
	key := strings.ToLower(strings.TrimSpace(term))
	if s, ok := snippets[key]; ok {
		return "search " + strconv.Quote(term) + ": " + s
	}
	return "search " + strconv.Quote(term) + ": no snippet for this term"
}

// runCalc evaluates "<int> <op> <int>" with op one of + - * /. The operator
// may be surrounded by spaces or not.
func runCalc(expr string) string {
	fields := strings.Fields(expr)
	if len(fields) != 3 {
		for _, op := range []string{"+", "-", "*", "/"} {
			if a, b, ok := strings.Cut(expr, op); ok && a != "" && b != "" {
				fields = []string{a, op, b}
				break
			}
		}
	}
	if len(fields) != 3 {
		return "calc error: expected <int> <op> <int>, got " + strconv.Quote(expr)
	}
	a, errA := strconv.Atoi(strings.TrimSpace(fields[0]))
	b, errB := strconv.Atoi(strings.TrimSpace(fields[2]))
	if errA != nil || errB != nil {
		return "calc error: operands must be integers in " + strconv.Quote(expr)
	}
	var v int
	switch fields[1] {
	case "+":
		v = a + b
	case "-":
		v = a - b
	case "*":
		v = a * b
	case "/":
		if b == 0 {
			return "calc error: division by zero in " + strconv.Quote(expr)
		}
		v = a / b
	default:
		return "calc error: unknown operator " + strconv.Quote(fields[1])
	}
	return expr + " = " + strconv.Itoa(v)
}
