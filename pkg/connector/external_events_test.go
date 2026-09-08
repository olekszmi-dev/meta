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

func TestExternalE2EEClassificationIsAlwaysVestaDirect(t *testing.T) {
	thread := externalThreadDataWithClassification("group-looking-id", externalConversationClassification{
		connectorLane:    externalLaneMessenger1To1Vesta,
		conversationKind: externalConversationDirect,
	})
	if thread["connectorLane"] != externalLaneMessenger1To1Vesta || thread["conversationKind"] != externalConversationDirect {
		t.Fatalf("unexpected E2EE classification: %#v", thread)
	}
	if thread["isGroup"] != false {
		t.Fatalf("E2EE Vesta events must remain direct: %#v", thread)
	}
}

func TestExternalE2EEThreadDataDoesNotInferFromChatID(t *testing.T) {
	evt := &WAMessageEvent{FBMessage: &events.FBMessage{Info: types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:    types.NewJID("group-looking-id", types.MessengerServer),
			IsGroup: true,
		},
		ID: "mautrix_task_228:group-looking-id",
	}}}
	thread := (&MetaClient{}).externalE2EEThreadData(nil, evt)
	if thread["connectorLane"] != externalLaneMessenger1To1Vesta || thread["conversationKind"] != externalConversationDirect || thread["isGroup"] != false {
		t.Fatalf("E2EE producer must use explicit Vesta/direct classification: %#v", thread)
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
	thread := externalThreadData("group", table.GROUP_THREAD)
	if thread["isGroup"] != true {
		t.Fatalf("shared legacy Messenger portal must be a group, got %#v", thread)
	}
	if thread["connectorLane"] != externalLaneMessengerGroup || thread["conversationKind"] != externalConversationGroup {
		t.Fatalf("unexpected legacy group classification: %#v", thread)
	}

	thread = externalThreadData("direct", table.ONE_TO_ONE)
	if thread["isGroup"] != false {
		t.Fatalf("account-scoped legacy Messenger portal must remain direct, got %#v", thread)
	}
	if thread["connectorLane"] != externalLaneMessengerUnknown || thread["conversationKind"] != externalConversationDirect {
		t.Fatalf("legacy direct must be quarantined without Vesta classification: %#v", thread)
	}
}

func TestExternalLegacyUnknownIsQuarantined(t *testing.T) {
	classification := classifyLegacyExternalConversation(table.UNKNOWN_THREAD_TYPE)
	if classification.connectorLane != externalLaneMessengerUnknown || classification.conversationKind != externalConversationUnknown {
		t.Fatalf("unknown legacy thread must be quarantined: %#v", classification)
	}
}

func TestExternalTaskClassificationDoesNotInferFromEventID(t *testing.T) {
	group := classifyExternalTask("228")
	if group.connectorLane != externalLaneMessengerGroup || group.conversationKind != externalConversationGroup {
		t.Fatalf("task 228 must be classified as group/table: %#v", group)
	}
	if classifyExternalTask("209").connectorLane != externalLaneMessengerGroup {
		t.Fatal("task 209 must be classified as group/table")
	}
	unknown := classifyLegacyExternalConversation(table.UNKNOWN_THREAD_TYPE)
	if unknown.connectorLane == group.connectorLane || unknown.conversationKind == group.conversationKind {
		t.Fatal("legacy classification must not inherit task classification")
	}
	if externalThreadData("same-event-id", table.UNKNOWN_THREAD_TYPE)["connectorLane"] != externalLaneMessengerUnknown {
		t.Fatal("event ID must not upgrade an unknown legacy event to the group lane")
	}
}

func TestTask209DiscoveryClassificationIsExplicitGroup(t *testing.T) {
	classification := classifyExternalTask("209")
	if classification.connectorLane != externalLaneMessengerGroup || classification.conversationKind != externalConversationGroup {
		t.Fatalf("task 209 discovery must be group/table: %#v", classification)
	}
}

func TestExternalHealthPreservesProtocolLaneAndScope(t *testing.T) {
	group := externalHealthData(externalLaneMessengerGroup, "discovery_209", "degraded", "task failed")
	if group["connectorLane"] != externalLaneMessengerGroup || group["scope"] != "discovery_209" {
		t.Fatalf("group discovery health lost protocol provenance: %#v", group)
	}
	vesta := externalHealthData(externalLaneMessenger1To1Vesta, "live", "healthy", "")
	if vesta["connectorLane"] != externalLaneMessenger1To1Vesta || vesta["runtimeLive"] != true {
		t.Fatalf("Vesta live health lost protocol provenance: %#v", vesta)
	}
}
