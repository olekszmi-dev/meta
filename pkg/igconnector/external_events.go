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
	participantIDs := make([]string, 0, len(thread.Users))
	for _, user := range thread.Users {
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
	kind := "direct"
	if thread.IsGroup || len(thread.Users) > 1 {
		kind = "group"
	}
	threadPayload := map[string]any{
		"threadId": threadID, "conversationKind": kind, "title": thread.ThreadTitle,
		"avatarRef": thread.ThreadImageURL, "participantIds": participantIDs,
		"lastMessageAt": thread.LastActivityTimestampMS.Time.UTC().Format(time.RFC3339Nano),
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

func (ic *IGClient) emitExternalMessage(ctx context.Context, portalKey networkid.PortalKey, msg *slidetypes.Message) error {
	if ic.Main.ExternalControl == nil || !ic.Main.ExternalControl.OwnsLogin(string(ic.UserLogin.ID)) || msg == nil {
		return nil
	}
	threadID := msg.ThreadFBID
	if threadID == "" {
		threadID = string(portalKey.ID)
	}
	portal, err := ic.Main.Bridge.GetPortalByKey(ctx, portalKey)
	if err != nil {
		return err
	}
	kind := "direct"
	if portal.RoomType != "dm" {
		kind = "group"
	}
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
			"threadId": threadID,
			"message":  ic.externalMessagePayload(threadID, msg),
		},
	})
}
