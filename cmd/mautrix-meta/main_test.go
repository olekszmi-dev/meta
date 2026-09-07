package main

import "testing"

func TestDatabaseOwnerRemainsStable(t *testing.T) {
	if m.DBOwner != defaultDatabaseOwner {
		t.Fatalf("database owner = %q, want %q", m.DBOwner, defaultDatabaseOwner)
	}
}
