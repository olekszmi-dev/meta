package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func TestExternalCommandTimeoutOnlyBoundsReadOnlyProjections(t *testing.T) {
	if got := externalCommandTimeout("contacts_sync"); got != externalProjectionTimeout {
		t.Fatalf("contacts timeout = %v, want %v", got, externalProjectionTimeout)
	}
	if got := externalCommandTimeout("threads_sync"); got != externalProjectionTimeout {
		t.Fatalf("threads timeout = %v, want %v", got, externalProjectionTimeout)
	}
	for _, commandType := range []string{"send", "reconnect", "history", "pin_restore"} {
		if got := externalCommandTimeout(commandType); got != 0 {
			t.Fatalf("%s timeout = %v, want synchronous execution", commandType, got)
		}
	}
}

func TestExecuteBoundedExternalCommandReturnsCompletedResult(t *testing.T) {
	want := map[string]any{"ok": true, "status": "completed"}
	got := executeBoundedExternalCommand(context.Background(), time.Second, func(context.Context) map[string]any {
		return want
	})
	if got["ok"] != true || got["status"] != "completed" {
		t.Fatalf("result = %#v, want %#v", got, want)
	}
}

func TestExecuteBoundedExternalCommandReleasesLoopWhenProviderIgnoresCancellation(t *testing.T) {
	release := make(chan struct{})
	finished := make(chan struct{})
	got := executeBoundedExternalCommand(context.Background(), 20*time.Millisecond, func(context.Context) map[string]any {
		defer close(finished)
		<-release
		return map[string]any{"ok": true}
	})
	if got["ok"] != false || got["error"] != "provider_command_timeout" {
		t.Fatalf("result = %#v, want provider timeout", got)
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("test executor did not exit after release")
	}
}

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
	if len(result.Contacts) != 2 {
		t.Fatalf("contacts = %d, want 2", len(result.Contacts))
	}
	contact := result.Contacts[0]
	if contact.ProviderID != "101" || contact.Name != "Alex Example" || contact.Username != "alex.example" {
		t.Fatalf("normalized contact = %#v", contact)
	}
	if contact.AvatarURL != "https://example.test/large.jpg" {
		t.Fatalf("avatar URL = %q, want large avatar", contact.AvatarURL)
	}
	if !contact.Messageable || result.Contacts[1].ProviderID != "202" || result.Contacts[1].Messageable {
		t.Fatalf("messageable evidence was not preserved separately: %#v", result.Contacts)
	}
	if result.Evidence.ProtocolRows != 5 || result.Evidence.MessageablePersonRows != 2 ||
		result.Evidence.UniquePersonCount != 2 || result.Evidence.UniqueMessageableCount != 1 ||
		result.Evidence.DuplicateRows != 1 || result.Evidence.SkippedRows != 2 {
		t.Fatalf("evidence = %#v", result.Evidence)
	}
	if result.Evidence.ContactSetDigest != "a3a09bf82df4739560d41ba6c360f9ca9a0c7dc586d5eb92b551838a237a0c18" {
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

func TestNormalizeExternalContactsIncludesVerifiedRows(t *testing.T) {
	result := normalizeExternalContacts(&table.LSTable{
		LSVerifyContactRowExists: []*table.LSVerifyContactRowExists{
			{ContactId: 404, Name: "Verified Contact", SecondaryName: "verified", CanViewerMessage: true},
			{ContactId: 404, CanViewerMessage: true},
			{ContactId: 505, CanViewerMessage: false},
			{ContactId: 0, CanViewerMessage: true},
			{ContactId: 606, CanViewerMessage: true, IsSelf: true},
		},
	})
	if len(result.Contacts) != 2 || result.Contacts[0].ProviderID != "404" || result.Contacts[1].ProviderID != "505" {
		t.Fatalf("contacts = %#v, want both verified contacts", result.Contacts)
	}
	if result.Evidence.ProtocolRows != 5 || result.Evidence.MessageablePersonRows != 2 ||
		result.Evidence.UniquePersonCount != 2 || result.Evidence.UniqueMessageableCount != 1 ||
		result.Evidence.DuplicateRows != 1 || result.Evidence.SkippedRows != 2 {
		t.Fatalf("evidence = %#v", result.Evidence)
	}
}

func TestExternalPersonUpsertEventKeepsProfilePrivateAndMessageabilitySeparate(t *testing.T) {
	contact := externalContactSyncContact{
		ProviderID:  "202",
		Name:        "Verified Contact",
		AvatarURL:   "https://example.test/private-avatar.jpg",
		Messageable: false,
		SourceRows:  []string{"verified"},
	}
	event, ok := externalPersonUpsertEvent(contact, time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("valid provider person was rejected")
	}
	if event["eventType"] != "person.upsert" || event["source"] != "mautrix_task_452" || event["visibility"] != "private" ||
		event["connectorLane"] != externalLaneMessengerGroup || event["conversationKind"] != externalConversationGroup {
		t.Fatalf("unexpected private person envelope: %#v", event)
	}
	payload := event["payload"].(map[string]any)
	if payload["personId"] != int64(202) || payload["displayName"] != "Verified Contact" {
		t.Fatalf("unexpected person identity: %#v", payload)
	}
	avatarRef, _ := payload["avatarRef"].(string)
	if !strings.HasPrefix(avatarRef, "mautrix_contact_avatar:") || strings.Contains(avatarRef, "http") {
		t.Fatalf("avatar reference is not opaque: %q", avatarRef)
	}
	aliases := payload["aliases"].([]map[string]string)
	if len(aliases) != 1 || aliases[0]["namespace"] != "meta_user_id" || aliases[0]["id"] != "202" {
		t.Fatalf("unexpected aliases: %#v", aliases)
	}
	messageable := payload["messageableEvidence"].(map[string]any)
	if messageable["canViewerMessage"] != false {
		t.Fatalf("non-messageable provider evidence was lost: %#v", messageable)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(contact.AvatarURL)) {
		t.Fatalf("plaintext avatar URL leaked into private event: %s", encoded)
	}
}

func TestExternalPersonUpsertRejectsInvalidProviderID(t *testing.T) {
	for _, providerID := range []string{"", "username", "0", "-1"} {
		if _, ok := externalPersonUpsertEvent(externalContactSyncContact{ProviderID: providerID}, time.Now()); ok {
			t.Fatalf("invalid provider ID %q was accepted", providerID)
		}
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
		LSVerifyContactRowExists: []*table.LSVerifyContactRowExists{{
			ContactId:        202,
			Name:             "Verified Secret",
			SecondaryName:    "verified-secret",
			CanViewerMessage: true,
			Unrecognized:     map[int]any{98: "verified-protocol-secret"},
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
	for _, raw := range []string{"protocol-secret", "verified-protocol-secret", "Verified Secret", "verified-secret", "Alex", "https://example.test", "providerId"} {
		if bytes.Contains(encoded, []byte(raw)) {
			t.Fatalf("plaintext contact data leaked (%q): %s", raw, encoded)
		}
	}
	if bytes.Contains(encoded, []byte("101")) {
		t.Fatalf("raw protocol field leaked: %s", encoded)
	}
	if bytes.Contains(encoded, []byte("202")) {
		t.Fatalf("verified contact ID leaked: %s", encoded)
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
