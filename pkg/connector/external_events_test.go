package connector

import (
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestExternalE2EEMessageDataText(t *testing.T) {
	timestamp := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	evt := &WAMessageEvent{FBMessage: &events.FBMessage{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     types.NewJID("123", types.MessengerServer),
				Sender:   types.NewJID("456", types.MessengerServer),
				IsFromMe: false,
			},
			ID:        "mid.1",
			PushName:  "Sender",
			Timestamp: timestamp,
		},
		Message: &waConsumerApplication.ConsumerApplication{
			Payload: &waConsumerApplication.ConsumerApplication_Payload{
				Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
					Content: &waConsumerApplication.ConsumerApplication_Content{
						Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{
							MessageText: &waCommon.MessageText{Text: proto.String("hello")},
						},
					},
				},
			},
		},
	}}

	eventType, message := externalE2EEMessageData(evt)
	want := map[string]any{
		"eventType":         "message.upsert",
		"kind":              "text",
		"text":              "hello",
		"providerMessageId": "mid.1",
		"direction":         "inbound",
		"timestamp":         timestamp.Format(time.RFC3339Nano),
	}
	got := map[string]any{
		"eventType":         eventType,
		"kind":              message["kind"],
		"text":              message["text"],
		"providerMessageId": message["providerMessageId"],
		"direction":         message["direction"],
		"timestamp":         message["timestamp"],
	}
	for key, expected := range want {
		if got[key] != expected {
			t.Fatalf("%s: got %v, want %v", key, got[key], expected)
		}
	}
}

func TestExternalE2EEThreadDataPreservesGroupMarker(t *testing.T) {
	if !externalE2EEIsGroup(true, "login", table.UNKNOWN_THREAD_TYPE) {
		t.Fatal("explicit group marker must be preserved")
	}
}

func TestExternalE2EEThreadDataClassifiesVestaGroupKey(t *testing.T) {
	if !externalE2EEIsGroup(false, "", table.UNKNOWN_THREAD_TYPE) {
		t.Fatal("shared Vesta portal must be classified as a group")
	}
	if externalE2EEIsGroup(false, "login", table.UNKNOWN_THREAD_TYPE) {
		t.Fatal("per-user portal must remain a direct conversation")
	}
}

func TestExternalE2EEThreadDataUsesPersistedGroupType(t *testing.T) {
	if !externalE2EEIsGroup(false, "login", table.GROUP_THREAD) {
		t.Fatal("persisted Messenger group type must override the per-user portal receiver")
	}
	if externalE2EEIsGroup(false, "login", table.ONE_TO_ONE) {
		t.Fatal("persisted Messenger one-to-one type must remain a direct conversation")
	}
}

func TestExternalE2EEPortalLookupKeysFallsBackToSharedPortal(t *testing.T) {
	portalKey := networkid.PortalKey{ID: "group", Receiver: "login"}
	keys := externalE2EEPortalLookupKeys(portalKey)
	if len(keys) != 2 || keys[0] != portalKey {
		t.Fatalf("expected scoped portal first, got %#v", keys)
	}
	if keys[1].ID != portalKey.ID || keys[1].Receiver != "" {
		t.Fatalf("expected shared portal fallback, got %#v", keys[1])
	}

	sharedKey := networkid.PortalKey{ID: "group"}
	keys = externalE2EEPortalLookupKeys(sharedKey)
	if len(keys) != 1 || keys[0] != sharedKey {
		t.Fatalf("shared portal must not be duplicated, got %#v", keys)
	}
}

func TestExternalThreadDataMarksLegacyGroupPortal(t *testing.T) {
	thread := externalThreadData("group", false, networkid.PortalKey{ID: "group"}, table.UNKNOWN_THREAD_TYPE)
	if thread["isGroup"] != true {
		t.Fatalf("shared legacy Messenger portal must be a group, got %#v", thread)
	}

	thread = externalThreadData("direct", false, networkid.PortalKey{ID: "direct", Receiver: "login"}, table.ONE_TO_ONE)
	if thread["isGroup"] != false {
		t.Fatalf("account-scoped legacy Messenger portal must remain direct, got %#v", thread)
	}
}
