package connector

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func TestNormalizeExternalContacts(t *testing.T) {
	response := &table.LSTable{
		LSDeleteThenInsertContact: []*table.LSDeleteThenInsertContact{
			{
				Id:                     101,
				Name:                   " Alex Example ",
				Username:               "alex.example",
				SecondaryName:          "alex-fallback",
				ProfilePictureUrl:      "https://example.test/small.jpg",
				ProfilePictureLargeUrl: "https://example.test/large.jpg",
				IsMessengerUser:        true,
				CanViewerMessage:       true,
			},
			{Id: 101, IsMessengerUser: true, CanViewerMessage: true},
			{Id: 202, IsMessengerUser: true, CanViewerMessage: false},
			{Id: 303, IsMessengerUser: false, CanViewerMessage: true},
			{Id: 0, IsMessengerUser: true, CanViewerMessage: true},
		},
	}

	result := normalizeExternalContacts(response)
	if len(result.Contacts) != 1 {
		t.Fatalf("contacts = %d, want 1", len(result.Contacts))
	}
	contact := result.Contacts[0]
	if contact.ProviderID != "101" || contact.Name != "Alex Example" || contact.Username != "alex.example" {
		t.Fatalf("normalized contact = %#v", contact)
	}
	if contact.AvatarURL != "https://example.test/large.jpg" {
		t.Fatalf("avatar URL = %q, want large avatar", contact.AvatarURL)
	}
	if result.Evidence.ProtocolRows != 5 || result.Evidence.MessageablePersonRows != 2 ||
		result.Evidence.UniqueMessageableCount != 1 || result.Evidence.DuplicateRows != 1 || result.Evidence.SkippedRows != 3 {
		t.Fatalf("evidence = %#v", result.Evidence)
	}
}

func TestNormalizeExternalContactsUsesSecondaryNameFallback(t *testing.T) {
	result := normalizeExternalContacts(&table.LSTable{
		LSDeleteThenInsertContact: []*table.LSDeleteThenInsertContact{{
			Id:               101,
			SecondaryName:    "alex-fallback",
			IsMessengerUser:  true,
			CanViewerMessage: true,
		}},
	})
	if got := result.Contacts[0].Username; got != "alex-fallback" {
		t.Fatalf("username = %q, want secondary-name fallback", got)
	}
}

func TestExternalContactsSyncResponseContainsNoRawProtocolPayload(t *testing.T) {
	response := externalContactsSyncResponse(&table.LSTable{
		LSDeleteThenInsertContact: []*table.LSDeleteThenInsertContact{{
			Id:               101,
			Name:             "Alex",
			IsMessengerUser:  true,
			CanViewerMessage: true,
			Unrecognized:     map[int]any{99: "protocol-secret"},
		}},
	})

	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || string(encoded) == "null" {
		t.Fatalf("response did not serialize: %s", encoded)
	}
	if !json.Valid(encoded) {
		t.Fatalf("invalid response payload: %s", encoded)
	}
	if bytes.Contains(encoded, []byte("protocol-secret")) {
		t.Fatalf("raw protocol field leaked: %s", encoded)
	}
}

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
