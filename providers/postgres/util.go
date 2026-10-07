package postgres

import (
	"crypto/rand"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	idAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	idLength   = 15
)

// randomID returns a 15-character [a-z0-9] identifier (the PocketBase format).
func randomID() string {
	b := make([]byte, idLength)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = idAlphabet[int(b[i])%len(idAlphabet)]
	}
	return string(b)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// msToTime converts stored epoch milliseconds; 0 means unset (zero time).
func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.Unix(0, ms*int64(time.Millisecond))
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// where accumulates ANDed conditions with positional arguments. A condition
// contains exactly one "?" which is replaced by the argument's $n.
type where struct {
	conds []string
	args  []any
}

func (w *where) add(cond string, arg any) {
	w.args = append(w.args, arg)
	w.conds = append(w.conds, strings.Replace(cond, "?", "$"+strconv.Itoa(len(w.args)), 1))
}

func (w *where) sql() string {
	if len(w.conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.conds, " AND ")
}

func strs[T ~string](in []T) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func clampPage(offset, limit, def, maxLimit int) (int, int) {
	offset = max(offset, 0)
	if limit <= 0 {
		limit = def
	}
	return offset, min(limit, maxLimit)
}
