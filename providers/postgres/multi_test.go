package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/YakirOren/turbine"

	"github.com/turbine-adapter/turbinedb/providers/postgres"
)

// twoInstances opens two DBs (as two processes would) on the same schema. The
// poll fallback is set so high that only LISTEN/NOTIFY can wake the waiters in
// the time the tests allow.
func twoInstances(t *testing.T) (a, b *postgres.DB) {
	t.Helper()
	schema := randomSchema()
	t.Cleanup(func() { dropSchema(t, schema) })
	slow := func(c *postgres.Config) { c.PollInterval = time.Minute }
	a = openSchema(t, schema, slow)
	b = openSchema(t, schema, slow)
	t.Cleanup(func() {
		a.Shutdown(context.Background(), 5*time.Second)
		b.Shutdown(context.Background(), 5*time.Second)
	})
	return a, b
}

func status(id, queue string, st turbine.StatusType) turbine.Status {
	now := time.Now()
	return turbine.Status{ID: id, Status: st, Name: "wf", ExecutorID: "x", QueueName: queue, CreatedAt: now, UpdatedAt: now}
}

func within(t *testing.T, d time.Duration, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("%s: no wake-up within %v", what, d)
	}
}

func TestMultiInstanceNotifications(t *testing.T) {
	ctx := context.Background()
	a, b := twoInstances(t)

	t.Run("enqueue on A wakes WaitForEnqueue on B", func(t *testing.T) {
		ch := b.WaitForEnqueue(ctx, "q")
		if _, err := a.InsertStatus(ctx, turbine.InsertStatusInput{Status: status("wf-q", "q", turbine.StatusEnqueued)}); err != nil {
			t.Fatal(err)
		}
		within(t, time.Second, ch, "WaitForEnqueue on B")
	})

	t.Run("Send on A wakes Recv on B", func(t *testing.T) {
		type result struct {
			msg *string
			err error
		}
		res := make(chan result, 1)
		go func() {
			msg, err := b.Recv(ctx, turbine.RecvInput{WorkflowUUID: "dest", Topic: "t", Timeout: 10 * time.Second})
			res <- result{msg, err}
		}()
		time.Sleep(200 * time.Millisecond) // let Recv register and find nothing
		start := time.Now()
		msg := `"hello"`
		if err := a.Send(ctx, turbine.SendInput{DestinationUUID: "dest", Topic: "t", Message: &msg}); err != nil {
			t.Fatal(err)
		}
		select {
		case r := <-res:
			if r.err != nil || r.msg == nil || *r.msg != msg {
				t.Fatalf("Recv = %v, %v", r.msg, r.err)
			}
			if d := time.Since(start); d > time.Second {
				t.Fatalf("Recv woke after %v, want < 1s", d)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Recv on B was not woken by Send on A")
		}
	})

	t.Run("SetEvent on A wakes GetEvent on B", func(t *testing.T) {
		res := make(chan *string, 1)
		go func() {
			v, _ := b.GetEvent(ctx, turbine.GetEventInput{TargetWorkflowUUID: "wf-e", Key: "k", Timeout: 10 * time.Second})
			res <- v
		}()
		time.Sleep(200 * time.Millisecond)
		v := `"v"`
		if err := a.SetEvent(ctx, turbine.SetEventInput{WorkflowUUID: "wf-e", Key: "k", Value: &v}); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-res:
			if got == nil || *got != v {
				t.Fatalf("GetEvent = %v", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("GetEvent on B was not woken by SetEvent on A")
		}
	})

	t.Run("outcome on A wakes AwaitWorkflowResult on B", func(t *testing.T) {
		if _, err := a.InsertStatus(ctx, turbine.InsertStatusInput{Status: status("wf-r", "", turbine.StatusPending)}); err != nil {
			t.Fatal(err)
		}
		res := make(chan string, 1)
		go func() {
			out, err := b.AwaitWorkflowResult(ctx, "wf-r", time.Minute)
			if err != nil || out == nil {
				res <- fmt.Sprintf("err=%v out=%v", err, out)
				return
			}
			res <- *out
		}()
		time.Sleep(200 * time.Millisecond)
		out := `"done"`
		if err := a.UpdateWorkflowOutcome(ctx, turbine.UpdateWorkflowOutcomeInput{WorkflowID: "wf-r", Status: turbine.StatusSuccess, Output: &out}); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-res:
			if got != out {
				t.Fatalf("Await = %s", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Await on B was not woken by the outcome on A")
		}
	})
}

// dequeueAll drains the queue from n goroutines until nothing more is claimed.
func dequeueAll(t *testing.T, db *postgres.DB, in turbine.DequeueWorkflowsInput, workers int) [][]string {
	t.Helper()
	var (
		mu  sync.Mutex
		out [][]string
		wg  sync.WaitGroup
	)
	for range workers {
		wg.Go(func() {
			for {
				got, err := db.DequeueWorkflows(context.Background(), in)
				if err != nil {
					t.Errorf("DequeueWorkflows: %v", err)
					return
				}
				if len(got) == 0 {
					return
				}
				ids := make([]string, len(got))
				for i, w := range got {
					ids[i] = w.WorkflowID
				}
				mu.Lock()
				out = append(out, ids)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return out
}

func TestMultiInstanceDequeueNeverDoubleClaims(t *testing.T) {
	ctx := context.Background()
	a, b := twoInstances(t)
	const total = 200
	for i := range total {
		if _, err := a.InsertStatus(ctx, turbine.InsertStatusInput{Status: status(fmt.Sprintf("wf-%03d", i), "q", turbine.StatusEnqueued)}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := map[string]int{}
	for i, db := range []*postgres.DB{a, b} {
		wg.Go(func() {
			in := turbine.DequeueWorkflowsInput{QueueName: "q", ExecutorID: fmt.Sprintf("exec-%d", i), Limit: 7}
			for _, ids := range dequeueAll(t, db, in, 4) {
				mu.Lock()
				for _, id := range ids {
					claimed[id]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(claimed) != total {
		t.Fatalf("claimed %d distinct workflows, want %d", len(claimed), total)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("workflow %s claimed %d times", id, n)
		}
	}
}

func TestMultiInstanceDequeueRespectsGlobalConcurrency(t *testing.T) {
	ctx := context.Background()
	a, b := twoInstances(t)
	for i := range 60 {
		if _, err := a.InsertStatus(ctx, turbine.InsertStatusInput{Status: status(fmt.Sprintf("wf-%03d", i), "q", turbine.StatusEnqueued)}); err != nil {
			t.Fatal(err)
		}
	}

	global := 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := map[string]int{}
	for i, db := range []*postgres.DB{a, b} {
		wg.Go(func() {
			in := turbine.DequeueWorkflowsInput{QueueName: "q", ExecutorID: fmt.Sprintf("exec-%d", i), Limit: 6, GlobalConcurrency: &global}
			for _, ids := range dequeueAll(t, db, in, 4) {
				mu.Lock()
				for _, id := range ids {
					claimed[id]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(claimed) != global {
		t.Fatalf("claimed %d workflows with GlobalConcurrency %d", len(claimed), global)
	}
	page, err := a.QueryWorkflows(ctx, turbine.WorkflowQuery{Statuses: []turbine.StatusType{turbine.StatusPending}})
	if err != nil || page.Total != global {
		t.Fatalf("pending = %d (%v), want %d", page.Total, err, global)
	}

	// Once a slot is freed (on either instance), exactly one more can be claimed.
	if err := b.UpdateWorkflowOutcome(ctx, turbine.UpdateWorkflowOutcomeInput{WorkflowID: page.Items[0].ID, Status: turbine.StatusSuccess}); err != nil {
		t.Fatal(err)
	}
	got, err := a.DequeueWorkflows(ctx, turbine.DequeueWorkflowsInput{QueueName: "q", ExecutorID: "exec-0", GlobalConcurrency: &global})
	if err != nil || len(got) != 1 {
		t.Fatalf("dequeue after a slot was freed = %d (%v), want 1", len(got), err)
	}
}

func TestMultiInstanceRecvExactlyOnce(t *testing.T) {
	ctx := context.Background()
	a, b := twoInstances(t)
	const msgs = 40
	for i := range msgs {
		m := strconv.Itoa(i)
		if err := a.Send(ctx, turbine.SendInput{DestinationUUID: "d", Topic: "t", Message: &m}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for _, db := range []*postgres.DB{a, b} {
		for range 4 {
			wg.Go(func() {
				for {
					m, err := db.Recv(ctx, turbine.RecvInput{WorkflowUUID: "d", Topic: "t"})
					if err != nil || m == nil {
						return
					}
					mu.Lock()
					seen[*m]++
					mu.Unlock()
				}
			})
		}
	}
	wg.Wait()
	if len(seen) != msgs {
		t.Fatalf("received %d distinct messages, want %d", len(seen), msgs)
	}
	for m, n := range seen {
		if n != 1 {
			t.Fatalf("message %s received %d times", m, n)
		}
	}
}

func TestWaitForEnqueuePollFallback(t *testing.T) {
	schema := randomSchema()
	t.Cleanup(func() { dropSchema(t, schema) })
	db := openSchema(t, schema, func(c *postgres.Config) { c.PollInterval = 100 * time.Millisecond })
	t.Cleanup(func() { db.Shutdown(context.Background(), 5*time.Second) })

	// No write at all: the ticker alone must wake the waiter.
	within(t, 2*time.Second, db.WaitForEnqueue(context.Background(), "idle"), "poll fallback")
}

func TestShutdownStopsListenerAndHonorsContexts(t *testing.T) {
	schema := randomSchema()
	t.Cleanup(func() { dropSchema(t, schema) })
	db := openSchema(t, schema, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := db.Recv(ctx, turbine.RecvInput{WorkflowUUID: "x", Topic: "t", Timeout: time.Minute})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Recv err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv did not return after the context was cancelled")
	}

	start := time.Now()
	db.Shutdown(context.Background(), 5*time.Second)
	db.Shutdown(context.Background(), 5*time.Second) // idempotent
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Shutdown took %v", d)
	}
}
