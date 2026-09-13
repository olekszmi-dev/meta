package igconnector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func TestExternalInstagramMessagePayloadPreservesAttachmentAndReplyReferences(t *testing.T) {
	ic := &IGClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID("100")}}}
	message := &slidetypes.Message{
		ID: "message-1", SenderFBID: 200, RepliedToMessageID: "message-0",
		Content: slidetypes.MessageContentWrapper{Content: &slidetypes.MessageContentImage{
			Attachments: []*slidetypes.Attachment{{AttachmentFBID: "attachment-1"}},
		}},
	}
	payload := ic.externalMessagePayload("thread-1", message)
	if payload["replyToProviderMessageId"] != "message-0" {
		t.Fatalf("reply reference = %#v", payload["replyToProviderMessageId"])
	}
	attachments := payload["attachments"].([]map[string]any)
	if len(attachments) != 1 || attachments[0]["providerAttachmentId"] != "attachment-1" || attachments[0]["kind"] != "image" {
		t.Fatalf("attachments = %#v", attachments)
	}
}

func TestExternalInstagramPortalClassificationUsesNativeThreadType(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		RoomType:       database.RoomTypeDM,
		MessageRequest: true,
		Metadata:       &metaid.PortalMetadata{ThreadType: table.ONE_TO_ONE},
	}}
	direct := classifyExternalInstagramPortal(portal)
	if direct.ConversationKind != externalInstagramConversationDirect || direct.RequestStatus != externalInstagramRequestPending {
		t.Fatalf("direct request classification = %#v", direct)
	}
	portal.Metadata = &metaid.PortalMetadata{ThreadType: table.GROUP_THREAD}
	portal.MessageRequest = false
	group := classifyExternalInstagramPortal(portal)
	if group.ConversationKind != externalInstagramConversationGroup || group.RequestStatus != externalInstagramRequestAccepted {
		t.Fatalf("group classification = %#v", group)
	}
}
