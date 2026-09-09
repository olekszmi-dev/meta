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
	if result.Evidence.ContactSetDigest != "16dc368a89b428b2485484313ba67a3912ca03f2b2b42429174a4f8b3dc84e44" {
		t.Fatalf("contact set digest = %q", result.Evidence.ContactSetDigest)
	}
}

func TestExternalContactSetDigestSortsAndDeduplicatesIDs(t *testing.T) {
	contacts := []externalContactSyncContact{
		{ProviderID: "303"},
		{ProviderID: "101"},
		{ProviderID: "303"},
		{ProviderID: "202"},
		{ProviderID: ""},
	}
	want := "83214d83da0ad2db63f84295d515370edef277ad9513508504f8cc2e5d77ac2f"
	if got := externalContactSetDigest(contacts); got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}
	if got := externalContactSetDigest([]externalContactSyncContact{{ProviderID: "202"}, {ProviderID: "101"}, {ProviderID: "303"}}); got != want {
		t.Fatalf("digest changes with order: %q, want %q", got, want)
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
	if _, ok := response["contacts"]; ok {
		t.Fatalf("plaintext contacts were included in the control response: %s", encoded)
	}
	for _, raw := range []string{"protocol-secret", "Alex", "https://example.test", "providerId"} {
		if bytes.Contains(encoded, []byte(raw)) {
			t.Fatalf("plaintext contact data leaked (%q): %s", raw, encoded)
		}
	}
	if bytes.Contains(encoded, []byte("101")) {
		t.Fatalf("raw protocol field leaked: %s", encoded)
	}
}

func TestExternalContactsSyncLimit(t *testing.T) {
	tests := []struct {
		name    string
		payload map[string]any
		want    int64
		valid   bool
	}{
		{name: "default", want: externalContactsDefaultLimit, valid: true},
		{name: "minimum", payload: map[string]any{"limit": "1"}, want: 1, valid: true},
		{name: "maximum", payload: map[string]any{"limit": "500"}, want: 500, valid: true},
		{name: "zero rejected", payload: map[string]any{"limit": "0"}},
		{name: "over maximum rejected", payload: map[string]any{"limit": "501"}},
		{name: "negative rejected", payload: map[string]any{"limit": "-1"}},
		{name: "non numeric rejected", payload: map[string]any{"limit": "many"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := externalContactsSyncLimit(test.payload)
			if test.valid {
				if err != nil || got != test.want {
					t.Fatalf("limit = %d, err = %v, want %d", got, err, test.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected invalid limit, got %d", got)
			}
		})
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
