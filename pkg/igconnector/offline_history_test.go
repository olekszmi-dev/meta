package igconnector

import (
	"testing"

	"go.mau.fi/mautrix-meta/pkg/externalhistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestOfflineHistoryUsesInstagramProductionPortalClassifier(t *testing.T) {
	direct, err := ClassifyOfflineHistoryPortal(externalhistory.OfflinePortal{
		ID: "direct", RoomType: string(database.RoomTypeDM), ThreadType: int64(table.ONE_TO_ONE), MessageRequest: true,
	})
	if err != nil || direct["conversationKind"] != externalInstagramConversationDirect || direct["requestStatus"] != externalInstagramRequestPending {
		t.Fatalf("pending direct classification = %#v, %v", direct, err)
	}
	group, err := ClassifyOfflineHistoryPortal(externalhistory.OfflinePortal{
		ID: "group", RoomType: string(database.RoomTypeGroupDM), ThreadType: int64(table.GROUP_THREAD),
	})
	if err != nil || group["conversationKind"] != externalInstagramConversationGroup || group["providerIsGroup"] != true {
		t.Fatalf("group classification = %#v, %v", group, err)
	}
}
