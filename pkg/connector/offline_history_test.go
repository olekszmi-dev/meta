package connector

import (
	"errors"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/externalhistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func TestOfflineHistoryUsesMessengerProductionLaneClassifier(t *testing.T) {
	group, err := ClassifyOfflineHistoryPortal(externalhistory.OfflinePortal{ThreadType: int64(table.GROUP_THREAD)})
	if err != nil || group["connectorLane"] != externalLaneMessengerGroup || group["conversationKind"] != externalConversationGroup {
		t.Fatalf("group classification = %#v, %v", group, err)
	}
	direct, err := ClassifyOfflineHistoryPortal(externalhistory.OfflinePortal{ThreadType: int64(table.ENCRYPTED_OVER_WA_ONE_TO_ONE)})
	if err != nil || direct["connectorLane"] != externalLaneMessenger1To1Vesta || direct["conversationKind"] != externalConversationDirect {
		t.Fatalf("direct classification = %#v, %v", direct, err)
	}
	if _, err = ClassifyOfflineHistoryPortal(externalhistory.OfflinePortal{ThreadType: int64(table.ONE_TO_ONE)}); !errors.Is(err, externalhistory.ErrProtocolUnclassified) {
		t.Fatalf("legacy direct error = %v", err)
	}
}
