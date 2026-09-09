package connector

import (
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
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
		"personId":          uint64(456),
		"senderId":          "456",
		"senderName":        "Sender",
		"direction":         "inbound",
		"timestamp":         timestamp.Format(time.RFC3339Nano),
	}
	got := map[string]any{
		"eventType":         eventType,
		"kind":              message["kind"],
		"text":              message["text"],
		"providerMessageId": message["providerMessageId"],
		"personId":          message["personId"],
		"senderId":          message["senderId"],
		"senderName":        message["senderName"],
		"direction":         message["direction"],
		"timestamp":         message["timestamp"],
	}
	for key, expected := range want {
		if got[key] != expected {
			t.Fatalf("%s: got %v, want %v", key, got[key], expected)
		}
	}
	aliases := message["senderAliases"].([]map[string]string)
	if len(aliases) != 2 || aliases[0]["namespace"] != "meta_user_id" || aliases[0]["id"] != "456" ||
		aliases[1]["namespace"] != "vesta_jid" || aliases[1]["id"] != "456@msgr" {
		t.Fatalf("sender aliases = %#v", aliases)
	}
}

func TestExternalE2EEMessageDataRejectsPlaceholderPushNames(t *testing.T) {
	for _, pushName := range []string{"", "  ", "username", " USERNAME "} {
		evt := &WAMessageEvent{FBMessage: &events.FBMessage{
			Info: types.MessageInfo{
				MessageSource: types.MessageSource{Sender: types.NewJID("456", types.MessengerServer)},
				PushName:      pushName,
			},
		}}
		_, message := externalE2EEMessageData(evt)
		if _, exists := message["senderName"]; exists {
			t.Fatalf("placeholder push name %q was retained: %#v", pushName, message)
		}
	}
}

func TestExternalE2EEReactionUsesNumericIdentityAndJIDAlias(t *testing.T) {
	evt := &WAMessageEvent{FBMessage: &events.FBMessage{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Sender: types.NewJID("789", types.MessengerServer)},
			ID:            "reaction.1",
		},
		Message: wrapReaction(&waConsumerApplication.ConsumerApplication_ReactionMessage{
			Text: proto.String("+1"),
			Key:  &waCommon.MessageKey{ID: proto.String("mid.target")},
		}),
	}}
	eventType, message := externalE2EEMessageData(evt)
	if eventType != "message.reaction" || message["senderId"] != "789" {
		t.Fatalf("unexpected reaction identity: %s %#v", eventType, message)
	}
	reactions := message["reactions"].([]map[string]any)
	if len(reactions) != 1 || reactions[0]["personId"] != uint64(789) || reactions[0]["senderId"] != "789" {
		t.Fatalf("unexpected reaction payload: %#v", reactions)
	}
	aliases := reactions[0]["senderAliases"].([]map[string]string)
	if len(aliases) != 2 || aliases[0]["id"] != "789" || aliases[1]["id"] != "789@msgr" {
		t.Fatalf("unexpected reaction aliases: %#v", aliases)
	}
}

func TestExternalThreadProjectionFromLiveTableIncludesTitleAndParticipants(t *testing.T) {
	projection, ok := externalThreadProjectionFromResync(&FBChatResync{
		PortalKey: networkid.PortalKey{ID: "2546149165905190"},
		Raw: &table.LSDeleteThenInsertThread{
			ThreadKey:   2546149165905190,
			ThreadName:  "Moneyhus, Alex",
			ThreadType:  table.GROUP_THREAD,
			MemberCount: 2,
		},
		Members: map[int64]bridgev2.ChatMember{202: {}, 101: {}},
	})
	if !ok {
		t.Fatal("valid live thread resync was rejected")
	}
	if projection.Title != "Moneyhus, Alex" || !projection.ParticipantsComplete ||
		len(projection.Participants) != 2 || projection.Participants[0] != 101 || projection.Participants[1] != 202 {
		t.Fatalf("unexpected live thread projection: %#v", projection)
	}
	event := externalThreadUpsertEvent("mautrix_live_table", projection, time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC))
	if event["eventType"] != "thread.upsert" || event["source"] != "mautrix_live_table" || event["visibility"] != "private" || event["connectorLane"] != externalLaneMessengerGroup {
		t.Fatalf("unexpected thread envelope: %#v", event)
	}
	payload := event["payload"].(map[string]any)
	if payload["title"] != "Moneyhus, Alex" {
		t.Fatalf("thread title missing: %#v", payload)
	}
	participantIDs := payload["participantIds"].([]int64)
	if len(participantIDs) != 2 || participantIDs[0] != 101 || participantIDs[1] != 202 {
		t.Fatalf("thread participant IDs missing: %#v", payload)
	}
}

func TestExternalThreadProjectionFromStoredPortalSupportsReadOnlySync(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "2546149165905190"},
		Name:      "Existing Group 4b2",
		Metadata:  &metaid.PortalMetadata{ThreadType: table.GROUP_THREAD},
	}}
	projection, ok := externalThreadProjectionFromPortal(portal)
	if !ok || projection.ThreadID != "2546149165905190" || projection.Title != "Existing Group 4b2" {
		t.Fatalf("unexpected stored portal projection: %#v, ok=%v", projection, ok)
	}
	if projection.Classification.connectorLane != externalLaneMessengerGroup || projection.ParticipantsComplete {
		t.Fatalf("stored group metadata overstated participant evidence: %#v", projection)
	}
	nativeEvent := externalThreadUpsertEvent("mautrix_native_portal", projection, time.Now())
	if nativeEvent["source"] != "mautrix_native_portal" {
		t.Fatalf("stored portal source changed: %#v", nativeEvent)
	}

	portal.NameIsCustom = true
	projection, _ = externalThreadProjectionFromPortal(portal)
	if projection.Title != "Existing Group 4b2" {
		t.Fatalf("stored group title was suppressed: %#v", projection)
	}

	portal.Metadata = &metaid.PortalMetadata{ThreadType: table.ONE_TO_ONE}
	projection, _ = externalThreadProjectionFromPortal(portal)
	if projection.Title != "" {
		t.Fatalf("locally customized direct-chat name was treated as provider evidence: %#v", projection)
	}
}

func TestExternalThreadSetDigestIsStableAndDoesNotExposeProjectionData(t *testing.T) {
	first := externalThreadProjection{
		ThreadID: "4b2",
		Title:    "Moneyhus, Alex",
		Classification: externalConversationClassification{
			connectorLane:    externalLaneMessengerGroup,
			conversationKind: externalConversationGroup,
		},
		Participants:         []int64{101, 202},
		ParticipantsComplete: true,
	}
	second := externalThreadProjection{
		ThreadID: "direct-7",
		Title:    "Private Contact",
		Classification: externalConversationClassification{
			connectorLane:    externalLaneMessengerUnknown,
			conversationKind: externalConversationDirect,
		},
	}

	got := externalThreadSetDigest([]externalThreadProjection{second, first, first})
	if got != externalThreadSetDigest([]externalThreadProjection{first, second}) {
		t.Fatalf("thread set digest changed with order or duplicate projections: %q", got)
	}
	if len(got) != 64 || strings.Contains(got, "4b2") || strings.Contains(got, "Moneyhus") {
		t.Fatalf("thread set digest is not an opaque SHA-256 aggregate: %q", got)
	}
	if got == externalThreadSetDigest([]externalThreadProjection{first}) {
		t.Fatalf("thread set digest ignored a distinct projection: %q", got)
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
