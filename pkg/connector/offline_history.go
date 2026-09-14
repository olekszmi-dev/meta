package connector

import (
	"go.mau.fi/mautrix-meta/pkg/externalhistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// ClassifyOfflineHistoryPortal reuses the production Messenger lane split
// without initializing a provider client. Unsupported legacy direct portals
// remain unclassified and cannot be sealed as Vesta history.
func ClassifyOfflineHistoryPortal(portal externalhistory.OfflinePortal) (map[string]any, error) {
	classification := classifyStoredExternalConversation(table.ThreadType(portal.ThreadType))
	if classification.connectorLane == externalLaneMessengerUnknown {
		return nil, externalhistory.ErrProtocolUnclassified
	}
	return map[string]any{
		"connectorLane":    classification.connectorLane,
		"conversationKind": classification.conversationKind,
	}, nil
}
