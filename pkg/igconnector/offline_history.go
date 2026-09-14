package igconnector

import (
	"go.mau.fi/mautrix-meta/pkg/externalhistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// ClassifyOfflineHistoryPortal uses the same stored-portal classifier as live
// discovery and history, while avoiding all Instagram client initialization.
func ClassifyOfflineHistoryPortal(portal externalhistory.OfflinePortal) (map[string]any, error) {
	stored := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{
			ID:       networkid.PortalID(portal.ID),
			Receiver: networkid.UserLoginID(portal.Receiver),
		},
		RoomType:       database.RoomType(portal.RoomType),
		MessageRequest: portal.MessageRequest,
		Metadata:       &metaid.PortalMetadata{ThreadType: table.ThreadType(portal.ThreadType)},
	}}
	classification := classifyExternalInstagramPortal(stored)
	return map[string]any{
		"conversationKind":       classification.ConversationKind,
		"requestStatus":          classification.RequestStatus,
		"providerIsGroup":        classification.ProviderIsGroup,
		"providerSubtype":        classification.ProviderSubtypeClass,
		"roomType":               portal.RoomType,
		"classificationEvidence": classification,
	}, nil
}
