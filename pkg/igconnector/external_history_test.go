package igconnector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func TestInstagramHistoryDecoratorPreservesRequestAndConversationKind(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey:      networkid.PortalKey{ID: "thread-1"},
		RoomType:       database.RoomTypeDM,
		MessageRequest: true,
		Metadata:       &metaid.PortalMetadata{ThreadType: table.ONE_TO_ONE},
	}}
	event := map[string]any{"payload": map[string]any{}}
	decorateInstagramHistoryEvent(portal, event)
	if event["conversationKind"] != externalInstagramConversationDirect {
		t.Fatalf("history event kind = %#v", event["conversationKind"])
	}
	payload := event["payload"].(map[string]any)
	if payload["requestStatus"] != externalInstagramRequestPending || payload["conversationKind"] != externalInstagramConversationDirect {
		t.Fatalf("history payload classification = %#v", payload)
	}
}
