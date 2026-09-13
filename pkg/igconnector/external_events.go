package igconnector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func cookieDigest(input *cookies.Cookies) string {
	data, _ := json.Marshal(cookieMap(input))
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func externalEventDigest(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

func (ic *IGClient) persistExternalCredentials(ctx context.Context) error {
	if ic.Main.ExternalControl == nil || !ic.Main.ExternalControl.OwnsLogin(string(ic.UserLogin.ID)) || ic.LoginMeta.Cookies == nil {
		return nil
	}
	digest := cookieDigest(ic.LoginMeta.Cookies)
	ic.externalCredentialLock.Lock()
	defer ic.externalCredentialLock.Unlock()
	if digest == ic.externalCredentialDigest {
		return nil
	}
	generation, err := ic.Main.ExternalControl.StoreCredentials(
		ctx, string(ic.UserLogin.ID), ic.LoginMeta.Cookies, ic.LoginMeta.LoginUA,
	)
	if err != nil {
		return err
	}
	ic.externalCredentialDigest = digest
	ic.LoginMeta.CredentialRef = ic.Main.ExternalControl.AccountID
	ic.LoginMeta.CredentialGeneration = generation
	return nil
}

func externalThreadID(thread *slidetypes.ThreadInfo) string {
	if thread == nil {
		return ""
	}
	if thread.ID != "" {
		return thread.ID
	}
	return thread.ThreadFBID
}

func externalPersonID(user *slidetypes.User) string {
	if user == nil {
		return ""
	}
	if user.ID != "" {
		return user.ID
	}
	if user.PK != "" {
		return user.PK
	}
	if user.InteropMessagingUserFBID != 0 {
		return strconv.FormatInt(user.InteropMessagingUserFBID, 10)
	}
	return ""
}

func (ic *IGClient) emitExternalThread(ctx context.Context, thread *slidetypes.ThreadInfo, source string) error {
	if ic.Main.ExternalControl == nil || !ic.Main.ExternalControl.OwnsLogin(string(ic.UserLogin.ID)) || thread == nil {
		return nil
	}
	threadID := externalThreadID(thread)
	if threadID == "" {
		return fmt.Errorf("Instagram thread has no durable ID")
	}
	participants, _ := externalInstagramDistinctNonSelfUsers(thread, ic.UserLogin.ID)
	participantIDs := make([]string, 0, len(participants))
	for _, user := range participants {
		personID := externalPersonID(user)
		if personID == "" {
			continue
		}
		participantIDs = append(participantIDs, personID)
		personPayload := map[string]any{
			"personId": personID, "username": user.Username, "displayName": user.FullName,
			"avatarRef": user.ProfilePicURL, "messageable": true,
		}
		if err := ic.Main.ExternalControl.EmitEvent(ctx, map[string]any{
			"eventId":          "instagram_person:" + personID + ":" + externalEventDigest(personPayload),
			"eventType":        "person.upsert",
			"source":           source,
			"occurredAt":       time.Now().UTC().Format(time.RFC3339Nano),
			"conversationKind": "direct",
			"payload":          map[string]any{"person": personPayload},
		}); err != nil {
			return err
		}
	}
	classification := classifyExternalInstagramThread(thread, ic.UserLogin.ID)
	kind := classification.ConversationKind
	threadPayload := map[string]any{
		"threadId": threadID, "conversationKind": kind, "title": thread.ThreadTitle,
		"avatarRef": thread.ThreadImageURL, "participantIds": participantIDs,
		"lastMessageAt":          thread.LastActivityTimestampMS.Time.UTC().Format(time.RFC3339Nano),
		"requestStatus":          classification.RequestStatus,
		"classificationEvidence": classification,
	}
	return ic.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":          "instagram_thread:" + threadID + ":" + externalEventDigest(threadPayload),
		"eventType":        "thread.upsert",
		"source":           source,
		"occurredAt":       time.Now().UTC().Format(time.RFC3339Nano),
		"conversationKind": kind,
		"payload":          map[string]any{"thread": threadPayload},
	})
}

func externalAttachments(msg *slidetypes.Message) []map[string]any {
	result := make([]map[string]any, 0)
	add := func(id, kind string) {
		if id != "" {
			result = append(result, map[string]any{"providerAttachmentId": id, "kind": kind})
		}
	}
	if msg == nil {
		return result
	}
	switch content := msg.Content.Content.(type) {
	case *slidetypes.MessageContentImage:
		for _, attachment := range content.Attachments {
			if attachment != nil {
				add(attachment.AttachmentFBID, "image")
			}
		}
	case *slidetypes.MessageContentVideo:
		for _, attachment := range content.Videos {
			if attachment != nil {
				add(attachment.AttachmentFBID, "video")
			}
		}
	case *slidetypes.MessageContentMultiMedia:
		for _, attachment := range content.Attachments {
			if attachment != nil {
				add(attachment.AttachmentFBID, "media")
			}
		}
	case *slidetypes.MessageContentAudio:
		for _, attachment := range content.AudioAttachments {
			if attachment != nil {
				add(attachment.AttachmentFBID, "audio")
			}
		}
	case *slidetypes.MessageContentRavenImage:
		if content.Attachment != nil {
			add(content.Attachment.AttachmentFBID, "view_once_image")
		}
	case *slidetypes.MessageContentRavenVideo:
		if content.Attachment != nil {
			add(content.Attachment.AttachmentFBID, "view_once_video")
		}
	}
	return result
}

func (ic *IGClient) externalMessagePayload(threadID string, msg *slidetypes.Message) map[string]any {
	direction := "inbound"
	if msg.SenderFBID == metaid.ParseUserLoginID(ic.UserLogin.ID) {
		direction = "outbound"
	}
	senderID := strconv.FormatInt(msg.SenderFBID, 10)
	senderName := ""
	if msg.Sender != nil {
		if id := externalPersonID(&msg.Sender.UserDict); id != "" {
			senderID = id
		}
		senderName = msg.Sender.Name
	}
	messageID := msg.ID
	if messageID == "" {
		messageID = msg.MessageID
	}
	return map[string]any{
		"providerMessageId":        messageID,
		"threadId":                 threadID,
		"senderId":                 senderID,
		"senderName":               senderName,
		"direction":                direction,
		"timestamp":                msg.TimestampMS.Time.UTC().Format(time.RFC3339Nano),
		"text":                     msg.TextBody,
		"replyToProviderMessageId": msg.RepliedToMessageID,
		"attachments":              externalAttachments(msg),
	}
}

func (ic *IGClient) externalPortalDescriptor(
	ctx context.Context,
	portalKey networkid.PortalKey,
) (threadID string, classification externalInstagramClassification, err error) {
	portal, err := ic.Main.Bridge.GetPortalByKey(ctx, portalKey)
	if err != nil {
		return "", classification, err
	}
	threadID = string(portalKey.ID)
	if metadata, ok := portal.Metadata.(*metaid.PortalMetadata); ok {
		if metadata.IGID != "" {
			threadID = metadata.IGID
		} else if metadata.IGThreadID != "" {
			threadID = metadata.IGThreadID
		}
	}
	return threadID, classifyExternalInstagramPortal(portal), nil
}

func (ic *IGClient) emitExternalMessage(ctx context.Context, portalKey networkid.PortalKey, msg *slidetypes.Message) error {
	if ic.Main.ExternalControl == nil || !ic.Main.ExternalControl.OwnsLogin(string(ic.UserLogin.ID)) || msg == nil {
		return nil
	}
	threadID := msg.ThreadFBID
	portalThreadID, classification, err := ic.externalPortalDescriptor(ctx, portalKey)
	if err != nil {
		return err
	}
	if threadID == "" {
		threadID = portalThreadID
	}
	kind := classification.ConversationKind
	messageID := msg.ID
	if messageID == "" {
		messageID = msg.MessageID
	}
	if messageID == "" {
		return fmt.Errorf("Instagram message has no durable ID")
	}
	return ic.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":          "instagram_live:" + messageID,
		"eventType":        "message.upsert",
		"source":           "instagram_live",
		"occurredAt":       time.Now().UTC().Format(time.RFC3339Nano),
		"conversationKind": kind,
		"payload": map[string]any{
			"threadId":      threadID,
			"requestStatus": classification.RequestStatus,
			"message":       ic.externalMessagePayload(threadID, msg),
		},
	})
}

func (ic *IGClient) emitExternalMessageEdit(
	ctx context.Context,
	portalKey networkid.PortalKey,
	evt *slidetypes.EditMessageEvent,
) error {
	if ic.Main.ExternalControl == nil || !ic.Main.ExternalControl.OwnsLogin(string(ic.UserLogin.ID)) || evt == nil {
		return nil
	}
	threadID, classification, err := ic.externalPortalDescriptor(ctx, portalKey)
	if err != nil {
		return err
	}
	editedAt := evt.SlideEditHistoryEntry.TimestampMS.Time.UTC()
	payload := map[string]any{
		"threadId":      threadID,
		"requestStatus": classification.RequestStatus,
		"message": map[string]any{
			"providerMessageId": evt.MessageID,
			"threadId":          threadID,
			"timestamp":         editedAt.Format(time.RFC3339Nano),
			"text":              evt.TextBody,
			"editedAt":          editedAt.Format(time.RFC3339Nano),
		},
	}
	return ic.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":   "instagram_edit:" + evt.MessageID + ":" + strconv.FormatInt(editedAt.UnixMilli(), 10),
		"eventType": "message.edit", "source": "instagram_live",
		"occurredAt":       editedAt.Format(time.RFC3339Nano),
		"conversationKind": classification.ConversationKind,
		"payload":          payload,
	})
}

func (ic *IGClient) emitExternalMessageRemove(
	ctx context.Context,
	portalKey networkid.PortalKey,
	messageID string,
) error {
	if ic.Main.ExternalControl == nil || !ic.Main.ExternalControl.OwnsLogin(string(ic.UserLogin.ID)) {
		return nil
	}
	threadID, classification, err := ic.externalPortalDescriptor(ctx, portalKey)
	if err != nil {
		return err
	}
	removedAt := time.Now().UTC()
	return ic.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":   "instagram_remove:" + messageID,
		"eventType": "message.remove", "source": "instagram_live",
		"occurredAt":       removedAt.Format(time.RFC3339Nano),
		"conversationKind": classification.ConversationKind,
		"payload": map[string]any{
			"threadId": threadID, "requestStatus": classification.RequestStatus,
			"message": map[string]any{
				"providerMessageId": messageID, "threadId": threadID,
				"timestamp": removedAt.Format(time.RFC3339Nano), "removed": true,
			},
		},
	})
}

func (ic *IGClient) emitExternalReaction(
	ctx context.Context,
	portalKey networkid.PortalKey,
	eventType, messageID string,
	reaction slidetypes.Reaction,
) error {
	if ic.Main.ExternalControl == nil || !ic.Main.ExternalControl.OwnsLogin(string(ic.UserLogin.ID)) {
		return nil
	}
	threadID, classification, err := ic.externalPortalDescriptor(ctx, portalKey)
	if err != nil {
		return err
	}
	occurredAt := reaction.ReactionTimestampMS.Time.UTC()
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	reactionPayload := map[string]any{
		"providerMessageId": messageID,
		"senderId":          strconv.FormatInt(reaction.SenderFBID, 10),
		"reaction":          reaction.Reaction,
		"occurredAt":        occurredAt.Format(time.RFC3339Nano),
	}
	payload := map[string]any{
		"threadId": threadID, "requestStatus": classification.RequestStatus,
		"reaction": reactionPayload,
	}
	eventIdentity := map[string]any{
		"eventType": eventType, "providerMessageId": messageID,
		"senderId":             strconv.FormatInt(reaction.SenderFBID, 10),
		"reactionLogMessageId": reaction.LogMessageID, "reaction": reaction.Reaction,
	}
	return ic.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":   "instagram_" + eventType + ":" + externalEventDigest(eventIdentity),
		"eventType": eventType, "source": "instagram_live",
		"occurredAt":       occurredAt.Format(time.RFC3339Nano),
		"conversationKind": classification.ConversationKind,
		"payload":          payload,
	})
}
