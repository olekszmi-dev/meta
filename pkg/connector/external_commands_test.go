package connector

import (
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func TestExternalSendResult(t *testing.T) {
	const otid int64 = 1234
	tests := []struct {
		name     string
		response *table.LSTable
		ok       bool
		error    string
	}{
		{name: "nil response", error: "provider_receipt_missing_nil_response"},
		{name: "provider rejection", response: &table.LSTable{LSIssueNewError: []*table.LSIssueNewError{{}}}, error: "provider_rejected_send"},
		{name: "optimistic rejection", response: &table.LSTable{LSMarkOptimisticMessageFailed: []*table.LSMarkOptimisticMessageFailed{{}}}, error: "provider_rejected_optimistic_send"},
		{name: "task failure", response: &table.LSTable{LSHandleFailedTask: []*table.LSHandleFailedTask{{}}}, error: "provider_failed_send_task"},
		{name: "missing replacement", response: &table.LSTable{}, error: "provider_receipt_missing_replacement"},
		{name: "confirmed", response: &table.LSTable{LSReplaceOptimsiticMessage: []*table.LSReplaceOptimsiticMessage{{OfflineThreadingId: "1234", MessageId: "mid.confirmed"}}}, ok: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := externalSendResult(test.response, otid)
			if got, _ := result["ok"].(bool); got != test.ok {
				t.Fatalf("ok = %v, want %v", got, test.ok)
			}
			if got, _ := result["error"].(string); got != test.error {
				t.Fatalf("error = %q, want %q", got, test.error)
			}
			if test.ok && result["externalRef"] != "mid.confirmed" {
				t.Fatalf("externalRef = %q", result["externalRef"])
			}
		})
	}
}
