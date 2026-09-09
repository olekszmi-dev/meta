package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/whatsmeow/proto/waArmadilloApplication"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	externalLaneMessenger1To1Vesta = "messenger_1to1_vesta"
	externalLaneMessengerGroup     = "messenger_group_table"
	externalLaneMessengerUnknown   = "messenger_unclassified"

	externalConversationDirect  = "direct"
	externalConversationGroup   = "group"
	externalConversationUnknown = "unknown"
)

type externalConversationClassification struct {
	connectorLane    string
	conversationKind string
}

type externalThreadProjection struct {
	ThreadID             string
	Title                string
	Participants         []int64
	ParticipantsComplete bool
	Classification       externalConversationClassification
}

type externalThreadSyncEvidence struct {
	ScannedPortalCount   int    `json:"scannedPortalCount"`
	ProjectedThreadCount int    `json:"projectedThreadCount"`
	SkippedPortalCount   int    `json:"skippedPortalCount"`
	ThreadSetDigest      string `json:"threadSetDigest"`
}

func classifyLegacyExternalConversation(threadType table.ThreadType) externalConversationClassification {
	if externalThreadTypeIsGroup(threadType) {
		return externalConversationClassification{
			connectorLane:    externalLaneMessengerGroup,
			conversationKind: externalConversationGroup,
		}
	}
	if threadType.IsOneToOne() {
		return externalConversationClassification{
			connectorLane:    externalLaneMessengerUnknown,
			conversationKind: externalConversationDirect,
		}
	}
	return externalConversationClassification{
		connectorLane:    externalLaneMessengerUnknown,
		conversationKind: externalConversationUnknown,
	}
}

func classifyExternalTask(taskLabel string) externalConversationClassification {
	switch taskLabel {
	case "209", "228":
		return externalConversationClassification{
			connectorLane:    externalLaneMessengerGroup,
			conversationKind: externalConversationGroup,
		}
	default:
		return externalConversationClassification{
			connectorLane:    externalLaneMessengerUnknown,
			conversationKind: externalConversationUnknown,
		}
	}
}

func externalThreadDataWithClassification(threadID string, classification externalConversationClassification) map[string]any {
	return map[string]any{
		"threadId":         threadID,
		"isGroup":          classification.conversationKind == externalConversationGroup,
		"connectorLane":    classification.connectorLane,
		"conversationKind": classification.conversationKind,
	}
}

func externalIdentityAliases(numericID, namespace, rawID string) []map[string]string {
	aliases := []map[string]string{{"namespace": "meta_user_id", "id": numericID}}
	if rawID != "" {
		aliases = append(aliases, map[string]string{"namespace": namespace, "id": rawID})
	}
	return aliases
}

func externalE2EEIdentity(sender types.JID) (string, []map[string]string) {
	numericID := strconv.FormatUint(sender.UserInt(), 10)
	return numericID, externalIdentityAliases(numericID, "vesta_jid", sender.String())
}

func externalProviderPushName(pushName string) string {
	pushName = strings.TrimSpace(pushName)
	if strings.EqualFold(pushName, "username") {
		return ""
	}
	return pushName
}

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

func (m *MetaClient) emitExternalMessage(ctx context.Context, source string, evt *FBMessageEvent) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return nil
	}
	msg := evt.WrappedMessage
	threadID := strconv.FormatInt(msg.ThreadKey, 10)
	portalKey := evt.GetPortalKey()
	classification := classifyLegacyExternalConversation(m.externalPortalThreadType(ctx, portalKey))
	thread := externalThreadDataWithClassification(threadID, classification)
	if projection, ok := m.externalStoredThreadProjection(ctx, portalKey); ok {
		if projection.Title != "" {
			thread["title"] = projection.Title
		}
		if len(projection.Participants) > 0 {
			thread["participantIds"] = projection.Participants
		}
	}
	return m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":          fmt.Sprintf("%s:%s", source, msg.MessageId),
		"eventType":        "message.upsert",
		"source":           source,
		"connectorLane":    classification.connectorLane,
		"conversationKind": classification.conversationKind,
		"occurredAt":       time.UnixMilli(msg.TimestampMs).UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"thread":  thread,
			"message": m.externalMessage(msg),
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
	senderID, senderAliases := externalE2EEIdentity(evt.Info.Sender)
	personID := evt.Info.Sender.UserInt()
	message = map[string]any{
		"kind":              kind,
		"providerMessageId": evt.Info.ID,
		"personId":          personID,
		"senderId":          senderID,
		"senderAliases":     senderAliases,
		"timestamp":         evt.GetTimestamp().UTC().Format(time.RFC3339Nano),
		"direction":         map[bool]string{true: "outbound", false: "inbound"}[evt.Info.IsFromMe],
		"text":              externalE2EEText(evt),
	}
	if senderName := externalProviderPushName(evt.Info.PushName); senderName != "" {
		message["senderName"] = senderName
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
			message["reactions"] = []map[string]any{{
				"personId":      personID,
				"senderId":      senderID,
				"senderAliases": senderAliases,
				"reaction":      reaction,
			}}
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

func classifyStoredExternalConversation(threadType table.ThreadType) externalConversationClassification {
	if threadType == table.ENCRYPTED_OVER_WA_ONE_TO_ONE {
		return externalConversationClassification{
			connectorLane:    externalLaneMessenger1To1Vesta,
			conversationKind: externalConversationDirect,
		}
	}
	return classifyLegacyExternalConversation(threadType)
}

func externalSortedParticipantIDs(members map[int64]bridgev2.ChatMember) []int64 {
	participants := make([]int64, 0, len(members))
	for id := range members {
		if id > 0 {
			participants = append(participants, id)
		}
	}
	sort.Slice(participants, func(i, j int) bool { return participants[i] < participants[j] })
	return participants
}

func externalThreadProjectionFromResync(resync *FBChatResync) (externalThreadProjection, bool) {
	if resync == nil || (resync.Raw == nil && resync.Update == nil) {
		return externalThreadProjection{}, false
	}
	projection := externalThreadProjection{
		ThreadID:     strings.TrimSpace(string(resync.PortalKey.ID)),
		Participants: externalSortedParticipantIDs(resync.Members),
	}
	var threadType table.ThreadType
	if resync.Raw != nil {
		projection.Title = strings.TrimSpace(resync.Raw.ThreadName)
		projection.ParticipantsComplete = resync.Raw.MemberCount > 0 && int64(len(projection.Participants)) == resync.Raw.MemberCount
		threadType = resync.Raw.ThreadType
	} else {
		projection.Title = strings.TrimSpace(resync.Update.ThreadName)
		threadType = resync.Update.ThreadType
	}
	projection.Classification = classifyLegacyExternalConversation(threadType)
	return projection, projection.ThreadID != ""
}

func externalThreadProjectionFromPortal(portal *bridgev2.Portal) (externalThreadProjection, bool) {
	if portal == nil || portal.Portal == nil {
		return externalThreadProjection{}, false
	}
	threadID := strings.TrimSpace(string(portal.ID))
	if threadID == "" {
		return externalThreadProjection{}, false
	}
	threadType := table.UNKNOWN_THREAD_TYPE
	if metadata, ok := getPortalMetadata(portal); ok {
		threadType = metadata.ThreadType
	}
	projection := externalThreadProjection{
		ThreadID:       threadID,
		Classification: classifyStoredExternalConversation(threadType),
	}
	if !portal.NameIsCustom {
		projection.Title = strings.TrimSpace(portal.Name)
	}
	if otherUserID, err := strconv.ParseInt(strings.TrimSpace(string(portal.OtherUserID)), 10, 64); err == nil && otherUserID > 0 {
		projection.Participants = []int64{otherUserID}
		projection.ParticipantsComplete = projection.Classification.conversationKind == externalConversationDirect
	}
	return projection, true
}

func externalThreadProjectionPayload(projection externalThreadProjection) map[string]any {
	payload := externalThreadDataWithClassification(projection.ThreadID, projection.Classification)
	if projection.Title != "" {
		payload["title"] = projection.Title
	}
	if len(projection.Participants) > 0 {
		payload["participantIds"] = projection.Participants
	}
	payload["participantEvidence"] = map[string]any{
		"complete": projection.ParticipantsComplete,
	}
	return payload
}

func externalThreadProjectionDigest(projection externalThreadProjection) string {
	parts := []string{
		projection.ThreadID,
		projection.Title,
		projection.Classification.connectorLane,
		projection.Classification.conversationKind,
		strconv.FormatBool(projection.ParticipantsComplete),
	}
	for _, participant := range projection.Participants {
		parts = append(parts, strconv.FormatInt(participant, 10))
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(digest[:])
}

func externalThreadSetDigest(projections []externalThreadProjection) string {
	uniqueDigests := make(map[string]struct{}, len(projections))
	for _, projection := range projections {
		uniqueDigests[externalThreadProjectionDigest(projection)] = struct{}{}
	}
	digests := make([]string, 0, len(uniqueDigests))
	for digest := range uniqueDigests {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	digest := sha256.Sum256([]byte(strings.Join(digests, "\n")))
	return hex.EncodeToString(digest[:])
}

func externalThreadUpsertEvent(source string, projection externalThreadProjection, occurredAt time.Time) map[string]any {
	return map[string]any{
		"eventId":          fmt.Sprintf("%s:thread:%s", source, externalThreadProjectionDigest(projection)),
		"eventType":        "thread.upsert",
		"source":           source,
		"visibility":       "private",
		"connectorLane":    projection.Classification.connectorLane,
		"conversationKind": projection.Classification.conversationKind,
		"occurredAt":       occurredAt.UTC().Format(time.RFC3339Nano),
		"payload":          externalThreadProjectionPayload(projection),
	}
}

func (m *MetaClient) emitExternalThreadProjection(ctx context.Context, source string, projection externalThreadProjection) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return nil
	}
	return m.Main.ExternalControl.EmitEvent(ctx, externalThreadUpsertEvent(source, projection, time.Now()))
}

func (m *MetaClient) emitExternalThreadResync(ctx context.Context, resync *FBChatResync) error {
	projection, ok := externalThreadProjectionFromResync(resync)
	if !ok {
		return nil
	}
	return m.emitExternalThreadProjection(ctx, "mautrix_live_table", projection)
}

func (m *MetaClient) externalStoredThreadProjection(ctx context.Context, portalKey networkid.PortalKey) (externalThreadProjection, bool) {
	portal, err := m.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil || portal == nil {
		return externalThreadProjection{}, false
	}
	return externalThreadProjectionFromPortal(portal)
}

func (m *MetaClient) emitExternalStoredThreads(ctx context.Context) (externalThreadSyncEvidence, error) {
	evidence := externalThreadSyncEvidence{}
	emitted := make([]externalThreadProjection, 0)
	links, err := m.Main.Bridge.DB.UserPortal.GetAllForLogin(ctx, m.UserLogin.UserLogin)
	if err != nil {
		return evidence, err
	}
	sort.SliceStable(links, func(i, j int) bool {
		if links[i] == nil {
			return false
		}
		if links[j] == nil {
			return true
		}
		if links[i].Portal.ID == links[j].Portal.ID {
			return links[i].Portal.Receiver < links[j].Portal.Receiver
		}
		return links[i].Portal.ID < links[j].Portal.ID
	})
	seen := make(map[networkid.PortalKey]struct{}, len(links))
	for _, link := range links {
		if link == nil {
			evidence.SkippedPortalCount++
			continue
		}
		if _, ok := seen[link.Portal]; ok {
			continue
		}
		seen[link.Portal] = struct{}{}
		evidence.ScannedPortalCount++
		portal, loadErr := m.Main.Bridge.GetExistingPortalByKey(ctx, link.Portal)
		if loadErr != nil {
			return evidence, loadErr
		}
		projection, ok := externalThreadProjectionFromPortal(portal)
		if !ok {
			evidence.SkippedPortalCount++
			continue
		}
		if err = m.emitExternalThreadProjection(ctx, "mautrix_native_portal", projection); err != nil {
			return evidence, err
		}
		emitted = append(emitted, projection)
		evidence.ProjectedThreadCount++
	}
	evidence.ThreadSetDigest = externalThreadSetDigest(emitted)
	return evidence, nil
}

func externalAvatarRef(contact externalContactSyncContact) string {
	if strings.TrimSpace(contact.AvatarURL) == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(contact.ProviderID + "\n" + strings.TrimSpace(contact.AvatarURL)))
	return "mautrix_contact_avatar:" + hex.EncodeToString(digest[:])
}

func externalPersonUpsertEvent(contact externalContactSyncContact, occurredAt time.Time) (map[string]any, bool) {
	personID, err := strconv.ParseInt(strings.TrimSpace(contact.ProviderID), 10, 64)
	if err != nil || personID <= 0 {
		return nil, false
	}
	sources := append([]string(nil), contact.SourceRows...)
	sort.Strings(sources)
	payload := map[string]any{
		"personId": personID,
		"aliases":  externalIdentityAliases(contact.ProviderID, "", ""),
		"messageableEvidence": map[string]any{
			"canViewerMessage": contact.Messageable,
			"sourceRows":       sources,
		},
	}
	if displayName := strings.TrimSpace(contact.Name); displayName != "" {
		payload["displayName"] = displayName
	}
	if avatarRef := externalAvatarRef(contact); avatarRef != "" {
		payload["avatarRef"] = avatarRef
	}
	digestInput := []string{
		contact.ProviderID,
		strings.TrimSpace(contact.Name),
		externalAvatarRef(contact),
		strconv.FormatBool(contact.Messageable),
		strings.Join(sources, ","),
	}
	digest := sha256.Sum256([]byte(strings.Join(digestInput, "\n")))
	return map[string]any{
		"eventId":          "mautrix_task_452:person:" + hex.EncodeToString(digest[:]),
		"eventType":        "person.upsert",
		"source":           "mautrix_task_452",
		"visibility":       "private",
		"connectorLane":    externalLaneMessengerGroup,
		"conversationKind": externalConversationGroup,
		"occurredAt":       occurredAt.UTC().Format(time.RFC3339Nano),
		"payload":          payload,
	}, true
}

func (m *MetaClient) emitExternalPeople(ctx context.Context, contacts []externalContactSyncContact) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return nil
	}
	for _, contact := range contacts {
		event, ok := externalPersonUpsertEvent(contact, time.Now())
		if !ok {
			continue
		}
		if err := m.Main.ExternalControl.EmitEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func externalThreadTypeIsGroup(threadType table.ThreadType) bool {
	switch threadType {
	case table.GROUP_THREAD,
		table.TINCAN_GROUP_DISAPPEARING,
		table.CARRIER_MESSAGING_GROUP,
		table.ENCRYPTED_OVER_WA_GROUP,
		table.COMMUNITY_GROUP,
		table.COMMUNITY_GROUP_UNJOINED,
		table.COMMUNITY_PRIVATE_HIDDEN_JOINED_THREAD,
		table.COMMUNITY_PRIVATE_HIDDEN_UNJOINED_THREAD,
		table.COMMUNITY_GROUP_INVITED_UNJOINED,
		table.COMMUNITY_SUB_THREAD,
		table.XAC_GROUP:
		return true
	default:
		return false
	}
}

func externalE2EEPortalLookupKeys(portalKey networkid.PortalKey) []networkid.PortalKey {
	keys := []networkid.PortalKey{portalKey}
	if portalKey.Receiver != "" {
		sharedKey := portalKey
		sharedKey.Receiver = ""
		keys = append(keys, sharedKey)
	}
	return keys
}

func (m *MetaClient) externalPortalThreadType(ctx context.Context, portalKey networkid.PortalKey) table.ThreadType {
	for _, lookupKey := range externalE2EEPortalLookupKeys(portalKey) {
		portal, err := m.Main.Bridge.GetExistingPortalByKey(ctx, lookupKey)
		if err == nil && portal != nil {
			if metadata, ok := portal.Metadata.(*metaid.PortalMetadata); ok && metadata.ThreadType != table.UNKNOWN_THREAD_TYPE {
				return metadata.ThreadType
			}
		}
	}
	return table.UNKNOWN_THREAD_TYPE
}

func externalThreadData(threadID string, threadType table.ThreadType) map[string]any {
	return externalThreadDataWithClassification(threadID, classifyLegacyExternalConversation(threadType))
}

func (m *MetaClient) externalE2EEThreadData(ctx context.Context, evt *WAMessageEvent) map[string]any {
	return externalThreadDataWithClassification(evt.Info.Chat.String(), externalConversationClassification{
		connectorLane:    externalLaneMessenger1To1Vesta,
		conversationKind: externalConversationDirect,
	})
}

func (m *MetaClient) emitExternalE2EEMessage(ctx context.Context, evt *WAMessageEvent) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return nil
	}
	eventType, message := externalE2EEMessageData(evt)
	classification := externalConversationClassification{
		connectorLane:    externalLaneMessenger1To1Vesta,
		conversationKind: externalConversationDirect,
	}
	return m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":          fmt.Sprintf("mautrix_live_e2ee:%s:%s", evt.Info.Chat.String(), evt.Info.ID),
		"eventType":        eventType,
		"source":           "mautrix_live",
		"connectorLane":    classification.connectorLane,
		"conversationKind": classification.conversationKind,
		"occurredAt":       evt.GetTimestamp().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"thread":  m.externalE2EEThreadData(ctx, evt),
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
	classification := externalConversationClassification{
		connectorLane:    externalLaneMessengerUnknown,
		conversationKind: externalConversationUnknown,
	}
	if metadata, ok := getPortalMetadata(portal); ok {
		classification = classifyLegacyExternalConversation(metadata.ThreadType)
	}
	return m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":          fmt.Sprintf("mautrix_task_228:%s:%d:%s", threadID, upsert.Range.MinTimestampMs, upsert.Range.MinMessageId),
		"eventType":        "history.page",
		"source":           "mautrix_task_228",
		"connectorLane":    classification.connectorLane,
		"conversationKind": classification.conversationKind,
		"occurredAt":       time.Now().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"threadId":         threadID,
			"isGroup":          classification.conversationKind == externalConversationGroup,
			"connectorLane":    classification.connectorLane,
			"conversationKind": classification.conversationKind,
			"hasMoreBefore":    upsert.Range.HasMoreBefore,
			"messages":         messages,
		},
	})
}

func (m *MetaClient) emitExternalDiscoveryPage(ctx context.Context, batch int, minThreadKey int64, hasMoreBefore bool) error {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return nil
	}
	classification := classifyExternalTask("209")
	cursor := fmt.Sprintf("task209:%d:%d", batch, minThreadKey)
	return m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"eventId":          fmt.Sprintf("mautrix_task_209:%d:%d", batch, minThreadKey),
		"eventType":        "history.page",
		"source":           "mautrix_task_209",
		"connectorLane":    classification.connectorLane,
		"conversationKind": classification.conversationKind,
		"occurredAt":       time.Now().UTC().Format(time.RFC3339Nano),
		"payload": map[string]any{
			"connectorLane":    classification.connectorLane,
			"conversationKind": classification.conversationKind,
			"hasMoreBefore":    hasMoreBefore,
			"cursorHash":       cursor,
			"messages":         []any{},
		},
	})
}

func externalHealthData(connectorLane, scope, state, failureReason string) map[string]any {
	health := map[string]any{
		"scope":         scope,
		"state":         state,
		"runtimeLive":   scope == "live" && state == "healthy",
		"connectorLane": connectorLane,
	}
	if failureReason != "" {
		health["failureReason"] = failureReason
	}
	return health
}

func (m *MetaClient) emitExternalHealth(ctx context.Context, connectorLane, scope, state, failureReason string) {
	if m.Main.ExternalControl == nil || !m.Main.ExternalControl.OwnsLogin(string(m.UserLogin.ID)) {
		return
	}
	health := externalHealthData(connectorLane, scope, state, failureReason)
	if err := m.Main.ExternalControl.EmitEvent(ctx, map[string]any{
		"type":          "health",
		"connectorLane": connectorLane,
		"health":        health,
	}); err != nil {
		m.UserLogin.Log.Err(err).Str("scope", scope).Msg("Failed to emit external health")
	}
}
