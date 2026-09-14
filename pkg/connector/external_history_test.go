package connector

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func TestMessengerHistoryProviderMessageIDsCoverDirectAndGroupLanes(t *testing.T) {
	groupID, groupOK := messengerProviderMessageID(metaid.MakeFBMessageID("group-message"))
	directID, directOK := messengerProviderMessageID(metaid.MakeWAMessageID(
		types.NewJID("100", types.MessengerServer),
		types.NewJID("200", types.MessengerServer),
		"direct-message",
	))
	if !groupOK || groupID != "group-message" || !directOK || directID != "direct-message" {
		t.Fatalf("provider IDs = group(%t,%q), direct(%t,%q)", groupOK, groupID, directOK, directID)
	}
}

func TestMessengerHistoryDecoratorUsesStoredProtocolLane(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "thread-1"},
		Metadata:  &metaid.PortalMetadata{ThreadType: table.GROUP_THREAD},
	}}
	event := map[string]any{"payload": map[string]any{}}
	decorateMessengerHistoryEvent(portal, event)
	if event["connectorLane"] != externalLaneMessengerGroup || event["conversationKind"] != externalConversationGroup {
		t.Fatalf("history event lost group lane: %#v", event)
	}
	payload := event["payload"].(map[string]any)
	if payload["connectorLane"] != externalLaneMessengerGroup || payload["conversationKind"] != externalConversationGroup {
		t.Fatalf("history payload lost group lane: %#v", payload)
	}
}
