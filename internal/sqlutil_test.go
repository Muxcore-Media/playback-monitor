package internal

import "testing"

func TestRebindSQLPostgres(t *testing.T) {
	got := rebindSQL(dialectPostgres, `SELECT * FROM sessions WHERE id = ? AND server_id = ?`)
	want := `SELECT * FROM sessions WHERE id = $1 AND server_id = $2`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if rebindSQL(dialectSQLite, want) != want {
		t.Fatal("sqlite should pass through")
	}
}
