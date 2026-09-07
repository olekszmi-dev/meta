package connector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/whatsmeow/proto/waArmadilloApplication"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"maunium.net/go/mautrix/bridgev2"
)

func externalMedia(msg *table.WrappedMessage) []map[string]any {
	media := make([]map[string]any, 0, len(msg.Attachments)+len(msg.BlobAttachments)+len(msg.XMAAttachments)+len(msg.Stickers))
	for _, attachment := range msg.Attachments {
		if attachment.AttachmentFbid == "" {
			continue
		}
		media = append(media, map[string]any{
			"durableRef": "mautrix_attachment:" + attachment.AttachmentFbid,
			"mimeType":   attachment.AttachmentMimeType,
			"fileName":   attachment.Filename,
			"byteLength": attachment.Filesize,
		})
	}
	for _, attachment := range msg.BlobAttachments {
		if attachment.AttachmentFbid == "" {
			continue
		}
		media = append(media, map[string]any{"durableRef": "mautrix_blob:" + attachment.AttachmentFbid})
	}
	for _, attachment := range msg.XMAAttachments {
		if attachment.AttachmentFbid == "" {
			continue
		}
		media = append(media, map[string]any{"durableRef": "mautrix_xma:" + attachment.AttachmentFbid})
	}
	for _, attachment := range msg.Stickers {
		if attachment.AttachmentFbid == "" {
			continue
		}
		media = append(media, map[string]any{"durableRef": "mautrix_sticker:" + attachment.AttachmentFbid})
	}
	return media
}

func (m *MetaClient) externalMessage(msg *table.WrappedMessage) map[string]any {
	kind := "text"
	media := externalMedia(msg)
	if len(media) > 0 {
		kind = "media"
	} else if msg.IsAdminMessage {
		kind = "system"
	}
	reactions := make([]map[string]any, 0, len(msg.Reactions))
	for _, reaction := range msg.Reactions {
		reactions = append(reactions, map[string]any{
			"senderId": strconv.FormatInt(reaction.ActorId, 10),
			"reaction": reaction.Reaction,
		})
	}
	replyAliases := make([]map[string]string, 0, 1)
	if msg.ReplySourceId != "" {
		replyAliases = append(replyAliases, map[string]string{"namespace": "meta_message_id", "id": msg.ReplySourceId})
	}
	return map[string]any{
		"kind":              kind,
		"providerMessageId": msg.MessageId,
		"senderId":          strconv.FormatInt(msg.SenderId, 10),
		"timestamp":         time.UnixMilli(msg.TimestampMs).UTC().Format(time.RFC3339Nano),
		"direction":         map[bool]string{true: "outbound", false: "inbound"}[msg.SenderId == metaid.ParseUserLoginID(m.UserLogin.ID)],
		"text":              msg.Text,
		"media":             media,
		"reactions":         reactions,
		"replyToAliases":    replyAliases,
	}
}

func (m *MetaClient) emitExternalMessage(ctx context.Context, source string, msg *table.WrappedMessage) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return nil
	}
	threadID := strconv.FormatInt(msg.ThreadKey, 10)
	return m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":    fmt.Sprintf("%s:%s", source, msg.MessageId),
		"eventType":  "message.upsert",
		"source":     source,
		"occurredAt": time.UnixMilli(msg.TimestampMs).UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"threadId": threadID,
			"message":  m.externalMessage(msg),
		},
	})
}

func externalE2EEText(evt *WAMessageEvent) string {
	switch msg := evt.Message.(type) {
	case *waConsumerApplication.ConsumerApplication:
		content := msg.GetPayload().GetContent()
		if text := content.GetMessageText(); text != nil {
			return text.GetText()
		}
		if text := content.GetExtendedTextMessage(); text != nil {
			return text.GetText().GetText()
		}
		if edit := content.GetEditMessage(); edit != nil {
			return edit.GetMessage().GetText()
		}
	case *waArmadilloApplication.Armadillo:
		if content := msg.GetPayload().GetContent().GetExtendedContentMessage(); content != nil {
			return content.GetMessageText()
		}
	}
	return ""
}

func externalE2EEHasMedia(evt *WAMessageEvent) bool {
	switch msg := evt.Message.(type) {
	case *waConsumerApplication.ConsumerApplication:
		content := msg.GetPayload().GetContent()
		return content.GetImageMessage() != nil ||
			content.GetStickerMessage() != nil ||
			content.GetViewOnceMessage() != nil ||
			content.GetDocumentMessage() != nil ||
			content.GetAudioMessage() != nil ||
			content.GetVideoMessage() != nil
	case *waArmadilloApplication.Armadillo:
		content := msg.GetPayload().GetContent()
		return content.GetCommonSticker() != nil ||
			content.GetRavenMessage() != nil ||
			content.GetRavenMessageMsgr() != nil ||
			content.GetImageGalleryMessage() != nil
	default:
		return false
	}
}

func externalE2EETargetID(evt *WAMessageEvent) string {
	msg, ok := evt.Message.(*waConsumerApplication.ConsumerApplication)
	if !ok {
		return ""
	}
	payload := msg.GetPayload()
	if content := payload.GetContent(); content != nil {
		if edit := content.GetEditMessage(); edit != nil {
			return edit.GetKey().GetID()
		}
		if reaction := content.GetReactionMessage(); reaction != nil {
			return reaction.GetKey().GetID()
		}
	}
	if application := payload.GetApplicationData(); application != nil {
		return application.GetRevoke().GetKey().GetID()
	}
	return ""
}

func externalE2EEMessageData(evt *WAMessageEvent) (eventType string, message map[string]any) {
	eventType = "message.upsert"
	kind := "text"
	message = map[string]any{
		"kind":              kind,
		"providerMessageId": evt.Info.ID,
		"senderId":          evt.Info.Sender.String(),
		"senderName":        evt.Info.PushName,
		"timestamp":         evt.GetTimestamp().UTC().Format(time.RFC3339Nano),
		"direction":         map[bool]string{true: "outbound", false: "inbound"}[evt.Info.IsFromMe],
		"text":              externalE2EEText(evt),
	}
	if externalE2EEHasMedia(evt) {
		kind = "media"
		message["media"] = []map[string]any{{
			"durableRef": fmt.Sprintf("mautrix_e2ee:%s:%s", evt.Info.Chat.String(), evt.Info.ID),
		}}
	}
	if quoted := evt.FBApplication.GetMetadata().GetQuotedMessage(); quoted != nil && quoted.GetStanzaID() != "" {
		message["replyToAliases"] = []map[string]string{{"namespace": "meta_message_id", "id": quoted.GetStanzaID()}}
	}
	switch evt.GetType() {
	case bridgev2.RemoteEventEdit:
		eventType = "message.edit"
		kind = "edit"
		if target := externalE2EETargetID(evt); target != "" {
			message["editOfAliases"] = []map[string]string{{"namespace": "meta_message_id", "id": target}}
		}
	case bridgev2.RemoteEventReaction, bridgev2.RemoteEventReactionRemove:
		eventType = "message.reaction"
		kind = "reaction"
		reaction, _ := evt.GetReactionEmoji()
		message["text"] = ""
		if reaction != "" {
			message["reactions"] = []map[string]string{{"senderId": evt.Info.Sender.String(), "reaction": reaction}}
		} else {
			kind = "redaction"
			eventType = "message.redaction"
		}
		if target := externalE2EETargetID(evt); target != "" {
			message["replyToAliases"] = []map[string]string{{"namespace": "meta_message_id", "id": target}}
		}
	case bridgev2.RemoteEventMessageRemove:
		eventType = "message.redaction"
		kind = "redaction"
		message["text"] = ""
		if target := externalE2EETargetID(evt); target != "" {
			message["replyToAliases"] = []map[string]string{{"namespace": "meta_message_id", "id": target}}
		}
	}
	message["kind"] = kind
	if kind == "text" && message["text"] == "" {
		message["kind"] = "system"
	}
	return eventType, message
}

func externalE2EEThreadData(evt *WAMessageEvent) map[string]any {
	return map[string]any{
		"threadId": evt.Info.Chat.String(),
		"isGroup":  evt.Info.IsGroup,
	}
}

func (m *MetaClient) emitExternalE2EEMessage(ctx context.Context, evt *WAMessageEvent) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return nil
	}
	eventType, message := externalE2EEMessageData(evt)
	return m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":    fmt.Sprintf("mautrix_live_e2ee:%s:%s", evt.Info.Chat.String(), evt.Info.ID),
		"eventType":  eventType,
		"source":     "mautrix_live",
		"occurredAt": evt.GetTimestamp().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"thread":  externalE2EEThreadData(evt),
			"message": message,
		},
	})
}

func (m *MetaClient) emitExternalHistoryPage(ctx context.Context, portal *bridgev2.Portal, upsert *table.UpsertMessages) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) || upsert == nil || upsert.Range == nil {
		return nil
	}
	messages := make([]map[string]any, 0, len(upsert.Messages))
	for _, msg := range upsert.Messages {
		messages = append(messages, m.externalMessage(msg))
	}
	threadID := string(portal.ID)
	return m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":    fmt.Sprintf("mautrix_task_228:%s:%d:%s", threadID, upsert.Range.MinTimestampMs, upsert.Range.MinMessageId),
		"eventType":  "history.page",
		"source":     "mautrix_task_228",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"threadId":      threadID,
			"hasMoreBefore": upsert.Range.HasMoreBefore,
			"messages":      messages,
		},
	})
}

func (m *MetaClient) emitExternalHealth(ctx context.Context, scope, state, failureReason string) {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return
	}
	health := map[string]any{"scope": scope, "state": state, "runtimeLive": scope == "live" && state == "healthy"}
	if failureReason != "" {
		health["failureReason"] = failureReason
	}
	if err := m.Main.ExternalControl.EmitEvent(ctx, map[string]any{"type": "health", "health": health}); err != nil {
		m.UserLogin.Log.Err(err).Str("scope", scope).Msg("Failed to emit external health")
	}
}
