package internal

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

type dbDialect string

const (
	dialectSQLite   dbDialect = "sqlite"
	dialectPostgres dbDialect = "postgres"
)

func rebindSQL(d dbDialect, query string) string {
	if d != dialectPostgres {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	arg := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(arg))
			arg++
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

var errDBNotInitialized = errors.New("db not initialized")

func (m *Module) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	m.mu.RLock()
	db := m.db
	d := m.dbDialect
	m.mu.RUnlock()
	if db == nil {
		return nil, errDBNotInitialized
	}
	return db.ExecContext(ctx, rebindSQL(d, query), args...)
}

func (m *Module) queryRows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	m.mu.RLock()
	db := m.db
	d := m.dbDialect
	m.mu.RUnlock()
	if db == nil {
		return nil, errDBNotInitialized
	}
	return db.QueryContext(ctx, rebindSQL(d, query), args...)
}

func (m *Module) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	m.mu.RLock()
	db := m.db
	d := m.dbDialect
	m.mu.RUnlock()
	return db.QueryRowContext(ctx, rebindSQL(d, query), args...)
}

func (m *Module) txExec(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	m.mu.RLock()
	d := m.dbDialect
	m.mu.RUnlock()
	return tx.ExecContext(ctx, rebindSQL(d, query), args...)
}

func (m *Module) txQueryRows(ctx context.Context, tx *sql.Tx, query string, args ...any) (*sql.Rows, error) {
	m.mu.RLock()
	d := m.dbDialect
	m.mu.RUnlock()
	return tx.QueryContext(ctx, rebindSQL(d, query), args...)
}
