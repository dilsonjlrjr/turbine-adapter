package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// maxNotifyPayload is the PostgreSQL limit for a NOTIFY payload (8000 bytes).
// Longer keys are not sent; waiters then rely on the poll fallback.
const maxNotifyPayload = 7900

// bus is the in-process waiter registry. Keys follow the SystemDatabase
// contract: "queue::<queue>", "workflow::<id>" and "<workflow>::<topic|key>".
type bus struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
	timers  map[chan struct{}]*time.Timer
}

func newBus() *bus {
	return &bus{
		waiters: make(map[string][]chan struct{}),
		timers:  make(map[chan struct{}]*time.Timer),
	}
}

// wait registers a single-shot waiter for key.
func (b *bus) wait(key string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan struct{}, 1)
	b.waiters[key] = append(b.waiters[key], ch)
	return ch
}

// waitTimed registers a single-shot waiter that also fires after d.
func (b *bus) waitTimed(key string, d time.Duration) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan struct{}, 1)
	b.waiters[key] = append(b.waiters[key], ch)
	b.timers[ch] = time.AfterFunc(d, func() {
		b.mu.Lock()
		delete(b.timers, ch)
		b.removeLocked(key, ch)
		b.mu.Unlock()
		select {
		case ch <- struct{}{}:
		default:
		}
	})
	return ch
}

// notify wakes and unregisters every waiter of key.
func (b *bus) notify(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fireLocked(key)
}

// notifyAll wakes every waiter, used after a listener (re)connect because
// notifications may have been missed meanwhile.
func (b *bus) notifyAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key := range b.waiters {
		b.fireLocked(key)
	}
}

func (b *bus) fireLocked(key string) {
	for _, ch := range b.waiters[key] {
		if t, ok := b.timers[ch]; ok {
			t.Stop()
			delete(b.timers, ch)
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	delete(b.waiters, key)
}

// remove unregisters a waiter; unknown channels are ignored.
func (b *bus) remove(key string, ch chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeLocked(key, ch)
	if t, ok := b.timers[ch]; ok {
		t.Stop()
		delete(b.timers, ch)
	}
}

// swap replaces an already fired waiter with a fresh one without a gap.
func (b *bus) swap(key string, old chan struct{}) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeLocked(key, old)
	ch := make(chan struct{}, 1)
	b.waiters[key] = append(b.waiters[key], ch)
	return ch
}

func (b *bus) removeLocked(key string, ch chan struct{}) {
	ws := b.waiters[key]
	for i, w := range ws {
		if w == ch {
			b.waiters[key] = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(b.waiters[key]) == 0 {
		delete(b.waiters, key)
	}
}

// close stops every pending timer.
func (b *bus) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch, t := range b.timers {
		t.Stop()
		delete(b.timers, ch)
	}
}

// start launches the LISTEN loop once.
func (d *DB) start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started || d.stopped {
		return
	}
	d.started = true
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	ready := make(chan struct{})
	d.listenWG.Go(func() { d.listenLoop(ctx, ready) })
	// Return only once LISTEN is active, so a notification issued right after
	// Open/Launch can never be missed.
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		d.log.Warn("notification listener not ready yet, continuing with the poll fallback")
	}
}

// listenLoop holds one pooled connection in LISTEN mode and fans the received
// notifications out to the local waiters. It reconnects with exponential
// backoff and wakes every waiter after each (re)connect.
func (d *DB) listenLoop(ctx context.Context, ready chan struct{}) {
	const (
		minBackoff = 100 * time.Millisecond
		maxBackoff = 5 * time.Second
	)
	backoff := minBackoff
	first := true
	for ctx.Err() == nil {
		connected, err := d.listenOnce(ctx, ready, first)
		first = false
		if ctx.Err() != nil {
			return
		}
		if connected {
			backoff = minBackoff
		}
		d.log.Warn("notification listener disconnected, retrying", "error", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (d *DB) listenOnce(ctx context.Context, ready chan struct{}, first bool) (connected bool, err error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()

	if _, err = conn.Exec(ctx, "LISTEN "+quoteIdent(d.channel)); err != nil {
		return false, err
	}
	if first {
		close(ready)
	} else {
		// Notifications may have been missed while we were not listening.
		d.bus.notifyAll()
	}
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			// Never hand a connection in an unknown state back to the pool.
			_ = conn.Conn().Close(context.WithoutCancel(ctx))
			return true, err
		}
		d.bus.notify(n.Payload)
	}
}

// txn is a transaction that collects the notification keys it emitted.
type txn struct {
	pgx.Tx
	d    *DB
	keys []string
}

// notify issues pg_notify inside the transaction (delivered on commit).
func (t *txn) notify(ctx context.Context, key string) error {
	t.keys = append(t.keys, key)
	if len(key) > maxNotifyPayload {
		return nil
	}
	_, err := t.Exec(ctx, "SELECT pg_notify($1, $2)", t.d.channel, key)
	return err
}

// inTx runs fn in a transaction. After a successful commit the notification
// keys emitted through the txn are delivered to the local waiters too (the
// listener delivers them to the other processes, and to this one a second time,
// which is harmless).
func (d *DB) inTx(ctx context.Context, fn func(t *txn) error) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	t := &txn{Tx: tx, d: d}
	if err := fn(t); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, k := range t.keys {
		d.bus.notify(k)
	}
	return nil
}
