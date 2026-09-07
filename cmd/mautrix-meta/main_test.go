package main

import "testing"

func TestDatabaseOwner(t *testing.T) {
	t.Setenv("MAUTRIX_META_PREPROVISIONED_DATABASE", "")
	if got := databaseOwner(); got != defaultDatabaseOwner {
		t.Fatalf("databaseOwner() = %q, want %q", got, defaultDatabaseOwner)
	}

	t.Setenv("MAUTRIX_META_PREPROVISIONED_DATABASE", "1")
	if got := databaseOwner(); got != "" {
		t.Fatalf("databaseOwner() = %q, want empty owner for pre-provisioned database", got)
	}
}
