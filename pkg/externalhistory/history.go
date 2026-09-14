package externalhistory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const (
	DefaultPageSize = 50
	MaxPageSize     = 100

	EvidenceVersion = "zero.messaging.transferred-snapshot.v1"
	EvidenceKind    = "transferred_snapshot"
)

var (
	ErrCommandPayloadInvalid = errors.New("history_snapshot_payload_invalid")
	ErrLoginNotOwned         = errors.New("history_snapshot_login_not_owned")
	ErrPortalAmbiguous       = errors.New("history_snapshot_portal_ambiguous")
	ErrPortalNotMaterialized = errors.New("history_snapshot_portal_not_materialized")
	ErrNoMatrixRoom          = errors.New("history_snapshot_matrix_room_missing")
	ErrMessageMapping        = errors.New("history_snapshot_message_mapping_invalid")
	ErrMatrixEventMissing    = errors.New("history_snapshot_matrix_event_missing")
)

type EmitFunc func(context.Context, any) error

type ProviderMessageIDFunc func(networkid.MessageID) (string, bool)

type PortalConversationIDsFunc func(*bridgev2.Portal) []string

type EventDecorator func(*bridgev2.Portal, map[string]any)

type Result struct {
	PageCount      int    `json:"pageCount"`
	MessageCount   int    `json:"messageCount"`
	EventCount     int    `json:"eventCount"`
	SnapshotRef    string `json:"snapshotRef"`
	CorpusDigest   string `json:"corpusDigest"`
	SnapshotDigest string `json:"snapshotDigest"`
	CursorHash     string `json:"cursorHash"`
	EvidenceRef    string `json:"evidenceRef"`
}

type SnapshotMessage struct {
	ProviderMessageID        string
	SenderID                 string
	Direction                string
	Timestamp                time.Time
	ReplyToProviderMessageID string
	Text                     string
	Media                    []map[string]any
	Edits                    []map[string]any
	Reactions                []map[string]any
}

type matrixPart struct {
	Text  string
	Media []map[string]any
	Edit  map[string]any
}

type groupedMessage struct {
	ProviderMessageID string
	Rows              []*database.Message
}

func ParseRequest(payload map[string]any) (string, int, error) {
	if payload == nil {
		return "", 0, ErrCommandPayloadInvalid
	}
	conversationID := strings.TrimSpace(fmt.Sprint(payload["conversationId"]))
	if conversationID == "" || conversationID == "<nil>" {
		return "", 0, ErrCommandPayloadInvalid
	}
	pageSize := DefaultPageSize
	if raw, ok := payload["pageSize"]; ok && strings.TrimSpace(fmt.Sprint(raw)) != "" && fmt.Sprint(raw) != "<nil>" {
		parsed, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(raw)))
		if err != nil || parsed < 1 || parsed > MaxPageSize {
			return "", 0, ErrCommandPayloadInvalid
		}
		pageSize = parsed
	}
	return conversationID, pageSize, nil
}

func Export(
	ctx context.Context,
	bridge *bridgev2.Bridge,
	login *bridgev2.UserLogin,
	conversationID string,
	pageSize int,
	source string,
	conversationIDs PortalConversationIDsFunc,
	providerMessageID ProviderMessageIDFunc,
	decorate EventDecorator,
	emit EmitFunc,
) (Result, error) {
	if bridge == nil || bridge.DB == nil || login == nil || login.UserLogin == nil || conversationIDs == nil || providerMessageID == nil || emit == nil {
		return Result{}, ErrLoginNotOwned
	}
	if conversationID == "" || pageSize < 1 || pageSize > MaxPageSize {
		return Result{}, ErrCommandPayloadInvalid
	}

	portal, err := resolveOwnedPortal(ctx, bridge, login, conversationID, conversationIDs)
	if err != nil {
		return Result{}, err
	}
	if err = validatePortalRoom(portal); err != nil {
		return Result{}, err
	}

	rows, err := bridge.DB.Message.GetMessagesBetweenTimeQuery(ctx, portal.PortalKey, time.Unix(0, -1), time.Unix(0, math.MaxInt64))
	if err != nil {
		return Result{}, ErrMessageMapping
	}
	messages, err := buildSnapshotMessages(ctx, portal, login, rows, providerMessageID, matrixEventFromBot(portal), reactionsFor(bridge))
	if err != nil {
		return Result{}, err
	}

	corpusDigest := digestSnapshot(messages)
	snapshotRef := digestStrings(source, conversationID, string(portal.ID), string(portal.Receiver), string(login.ID), corpusDigest)
	pageCount := (len(messages) + pageSize - 1) / pageSize
	pageDigests := make([]string, pageCount)
	pageMessageCounts := make([]int, pageCount)
	for pageIndex := range pageDigests {
		start := pageIndex * pageSize
		end := min(start+pageSize, len(messages))
		pageDigests[pageIndex] = digestSnapshot(messages[start:end])
		pageMessageCounts[pageIndex] = end - start
	}
	snapshotDigest := digestPageDescriptors(pageDigests, pageMessageCounts)
	evidenceRef := digestStrings(EvidenceVersion, EvidenceKind, snapshotRef, corpusDigest, snapshotDigest, strconv.Itoa(pageCount), strconv.Itoa(len(messages)))

	for pageIndex := 0; pageIndex < pageCount; pageIndex++ {
		start := pageIndex * pageSize
		end := min(start+pageSize, len(messages))
		pageMessages := messages[start:end]
		cursorHash := digestStrings(snapshotRef, strconv.Itoa(pageIndex), strconv.Itoa(pageCount), pageCursor(pageMessages))
		payload := historyPayload(conversationID, pageMessages, historyEvidence{
			Phase:         "page",
			SnapshotRef:   snapshotRef,
			CorpusDigest:  corpusDigest,
			CursorHash:    cursorHash,
			PageDigest:    pageDigests[pageIndex],
			PageIndex:     pageIndex,
			PageCount:     pageCount,
			MessageCount:  len(pageMessages),
			Complete:      false,
			HasMoreBefore: nil,
			ObservedAt:    observedAt(pageMessages),
		})
		historyEvent := historyEvent(source, snapshotRef, pageIndex, pageMessages, payload)
		if decorate != nil {
			decorate(portal, historyEvent)
		}
		if err = emit(ctx, historyEvent); err != nil {
			return Result{}, err
		}
	}

	terminalCursorHash := digestStrings(snapshotRef, "terminal", strconv.Itoa(pageCount))
	terminalPayload := historyPayload(conversationID, nil, historyEvidence{
		Phase:          "terminal",
		SnapshotRef:    snapshotRef,
		CorpusDigest:   corpusDigest,
		SnapshotDigest: snapshotDigest,
		CursorHash:     terminalCursorHash,
		PageIndex:      pageCount,
		PageCount:      pageCount,
		MessageCount:   len(messages),
		Complete:       true,
		HasMoreBefore:  false,
		EvidenceRef:    evidenceRef,
		ObservedAt:     observedAt(messages),
	})
	terminalEvent := historyEvent(source, snapshotRef, pageCount, nil, terminalPayload)
	if decorate != nil {
		decorate(portal, terminalEvent)
	}
	if err = emit(ctx, terminalEvent); err != nil {
		return Result{}, err
	}
	return Result{
		PageCount:      pageCount,
		MessageCount:   len(messages),
		EventCount:     pageCount + 1,
		SnapshotRef:    snapshotRef,
		CorpusDigest:   corpusDigest,
		SnapshotDigest: snapshotDigest,
		CursorHash:     terminalCursorHash,
		EvidenceRef:    evidenceRef,
	}, nil
}

func validatePortalRoom(portal *bridgev2.Portal) error {
	if portal == nil {
		return ErrPortalNotMaterialized
	}
	if portal.MXID == "" || portal.Bridge == nil || portal.Bridge.Bot == nil {
		return ErrNoMatrixRoom
	}
	return nil
}

func resolveOwnedPortal(ctx context.Context, bridge *bridgev2.Bridge, login *bridgev2.UserLogin, conversationID string, conversationIDs PortalConversationIDsFunc) (*bridgev2.Portal, error) {
	if bridge == nil || bridge.DB == nil || login == nil || login.UserLogin == nil {
		return nil, ErrLoginNotOwned
	}
	links, err := bridge.DB.UserPortal.GetAllForLogin(ctx, login.UserLogin)
	if err != nil {
		return nil, ErrPortalNotMaterialized
	}
	var matches []*bridgev2.Portal
	seen := make(map[networkid.PortalKey]struct{}, len(links))
	for _, link := range links {
		if link == nil {
			continue
		}
		if _, ok := seen[link.Portal]; ok {
			continue
		}
		seen[link.Portal] = struct{}{}
		portal, loadErr := bridge.GetExistingPortalByKey(ctx, link.Portal)
		if loadErr != nil {
			return nil, ErrPortalNotMaterialized
		}
		if portal == nil {
			continue
		}
		if portal.Receiver != "" && portal.Receiver != login.ID {
			continue
		}
		for _, candidate := range conversationIDs(portal) {
			if candidate == conversationID {
				matches = append(matches, portal)
				break
			}
		}
	}
	return selectPortalMatch(matches)
}

func selectPortalMatch(matches []*bridgev2.Portal) (*bridgev2.Portal, error) {
	if len(matches) > 1 {
		return nil, ErrPortalAmbiguous
	}
	if len(matches) == 0 || matches[0] == nil {
		return nil, ErrPortalNotMaterialized
	}
	return matches[0], nil
}

func aggregateMultipartRows(rows []*database.Message, providerMessageID ProviderMessageIDFunc) ([]groupedMessage, error) {
	groups := make(map[string]*groupedMessage, len(rows))
	for _, row := range rows {
		if row == nil || row.ID == "" || row.MXID == "" {
			return nil, ErrMessageMapping
		}
		providerID, ok := providerMessageID(row.ID)
		if !ok || providerID == "" {
			return nil, ErrMessageMapping
		}
		group := groups[providerID]
		if group == nil {
			group = &groupedMessage{ProviderMessageID: providerID}
			groups[providerID] = group
		}
		group.Rows = append(group.Rows, row)
	}
	result := make([]groupedMessage, 0, len(groups))
	for _, group := range groups {
		sort.SliceStable(group.Rows, func(i, j int) bool {
			left, right := group.Rows[i], group.Rows[j]
			if !left.Timestamp.Equal(right.Timestamp) {
				return left.Timestamp.Before(right.Timestamp)
			}
			if left.PartID != right.PartID {
				return left.PartID < right.PartID
			}
			return left.RowID < right.RowID
		})
		result = append(result, *group)
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i].Rows[0], result[j].Rows[0]
		if !left.Timestamp.Equal(right.Timestamp) {
			return left.Timestamp.Before(right.Timestamp)
		}
		return result[i].ProviderMessageID < result[j].ProviderMessageID
	})
	return result, nil
}

type MatrixEventFetcher func(context.Context, id.RoomID, id.EventID) (*event.Event, error)

type ReactionFetcher func(context.Context, networkid.UserLoginID, networkid.MessageID) ([]*database.Reaction, error)

func buildSnapshotMessages(ctx context.Context, portal *bridgev2.Portal, login *bridgev2.UserLogin, rows []*database.Message, providerMessageID ProviderMessageIDFunc, fetch MatrixEventFetcher, fetchReactions ReactionFetcher) ([]SnapshotMessage, error) {
	groups, err := aggregateMultipartRows(rows, providerMessageID)
	if err != nil {
		return nil, err
	}
	result := make([]SnapshotMessage, 0, len(groups))
	for _, group := range groups {
		first := group.Rows[0]
		if first.SenderID == "" {
			return nil, ErrMessageMapping
		}
		message := SnapshotMessage{
			ProviderMessageID: group.ProviderMessageID,
			SenderID:          string(first.SenderID),
			Direction:         directionFor(first.SenderID, login.ID),
			Timestamp:         first.Timestamp,
			Media:             make([]map[string]any, 0),
			Edits:             make([]map[string]any, 0),
			Reactions:         make([]map[string]any, 0),
		}
		var replyTarget networkid.MessageID
		for _, row := range group.Rows {
			if row.ReplyTo.MessageID != "" {
				replyTarget = row.ReplyTo.MessageID
				break
			}
		}
		if replyTarget != "" {
			replyID, ok := providerMessageID(replyTarget)
			if !ok || replyID == "" {
				return nil, ErrMessageMapping
			}
			message.ReplyToProviderMessageID = replyID
		}
		seenText := make(map[string]struct{})
		for _, row := range group.Rows {
			evt, fetchErr := fetch(ctx, portal.MXID, row.MXID)
			if fetchErr != nil || evt == nil {
				return nil, ErrMatrixEventMissing
			}
			part, projectErr := projectMatrixEvent(evt)
			if projectErr != nil {
				return nil, ErrMatrixEventMissing
			}
			if part.Text != "" {
				if _, seen := seenText[part.Text]; !seen {
					if message.Text != "" {
						message.Text += "\n"
					}
					message.Text += part.Text
					seenText[part.Text] = struct{}{}
				}
			}
			message.Media = append(message.Media, part.Media...)
			if part.Edit != nil {
				message.Edits = append(message.Edits, part.Edit)
			}
			if row.EditCount > 0 && len(message.Edits) == 0 {
				message.Edits = append(message.Edits, editMetadata(row, message.Text))
			}
		}
		if fetchReactions != nil {
			reactions, reactionErr := fetchReactions(ctx, portal.Receiver, first.ID)
			if reactionErr != nil {
				return nil, ErrMessageMapping
			}
			sort.SliceStable(reactions, func(i, j int) bool {
				if !reactions[i].Timestamp.Equal(reactions[j].Timestamp) {
					return reactions[i].Timestamp.Before(reactions[j].Timestamp)
				}
				if reactions[i].SenderID != reactions[j].SenderID {
					return reactions[i].SenderID < reactions[j].SenderID
				}
				return reactions[i].Emoji < reactions[j].Emoji
			})
			for _, reaction := range reactions {
				if reaction == nil || reaction.SenderID == "" || reaction.Emoji == "" {
					continue
				}
				message.Reactions = append(message.Reactions, map[string]any{
					"senderId":  string(reaction.SenderID),
					"reaction":  reaction.Emoji,
					"timestamp": reaction.Timestamp.UTC().Format(time.RFC3339Nano),
				})
			}
		}
		result = append(result, message)
	}
	return result, nil
}

func directionFor(senderID networkid.UserID, loginID networkid.UserLoginID) string {
	if string(senderID) == string(loginID) {
		return "outbound"
	}
	return "inbound"
}

func matrixEventFromBot(portal *bridgev2.Portal) MatrixEventFetcher {
	return func(ctx context.Context, roomID id.RoomID, eventID id.EventID) (*event.Event, error) {
		return portal.Bridge.Bot.GetEvent(ctx, roomID, eventID)
	}
}

func reactionsFor(bridge *bridgev2.Bridge) ReactionFetcher {
	return func(ctx context.Context, receiver networkid.UserLoginID, messageID networkid.MessageID) ([]*database.Reaction, error) {
		return bridge.DB.Reaction.GetAllToMessage(ctx, receiver, messageID)
	}
}

func projectMatrixEvent(evt *event.Event) (*matrixPart, error) {
	if evt == nil {
		return nil, ErrMatrixEventMissing
	}
	if evt.Content.Parsed == nil && len(evt.Content.VeryRaw) > 0 {
		_ = evt.Content.ParseRaw(evt.Type)
	}
	content, ok := evt.Content.Parsed.(*event.MessageEventContent)
	if !ok {
		return nil, ErrMatrixEventMissing
	}
	part := &matrixPart{Media: make([]map[string]any, 0)}
	originalContent := content
	if content.NewContent != nil {
		part.Edit = map[string]any{
			"editedAt": evtTime(evt).Format(time.RFC3339Nano),
			"text":     content.NewContent.Body,
		}
		content = content.NewContent
	}
	part.Text = content.Body
	part.Media = matrixMedia(content)
	if len(part.Media) == 0 && originalContent != content {
		part.Media = matrixMedia(originalContent)
	}
	mediaInfo := content.Info
	if mediaInfo == nil && originalContent != content {
		mediaInfo = originalContent.Info
	}
	if mediaInfo != nil && len(part.Media) > 0 && mediaInfo.MimeType != "" {
		for _, media := range part.Media {
			media["mimeType"] = mediaInfo.MimeType
		}
	}
	return part, nil
}

func matrixMedia(content *event.MessageEventContent) []map[string]any {
	media := make([]map[string]any, 0, 2)
	if content == nil {
		return media
	}
	if content.URL != "" {
		matrixRef := string(content.URL)
		media = append(media, map[string]any{
			"providerAttachmentId": digestStrings("matrix_media", matrixRef),
			"durableRef":           matrixRef,
			"matrixRef":            matrixRef,
		})
	}
	if content.File != nil && content.File.URL != "" {
		matrixRef := string(content.File.URL)
		media = append(media, map[string]any{
			"providerAttachmentId": digestStrings("matrix_media", matrixRef),
			"durableRef":           matrixRef,
			"matrixRef":            matrixRef,
			"encrypted":            true,
		})
	}
	return media
}

func evtTime(evt *event.Event) time.Time {
	if evt.Timestamp == 0 {
		return time.Unix(0, 0).UTC()
	}
	return time.UnixMilli(evt.Timestamp).UTC()
}

func editMetadata(row *database.Message, text string) map[string]any {
	edit := map[string]any{"count": row.EditCount, "text": text}
	data, err := json.Marshal(row.Metadata)
	if err == nil {
		var metadata map[string]any
		if json.Unmarshal(data, &metadata) == nil {
			if timestamp, ok := metadata["edit_timestamp"].(float64); ok && timestamp > 0 {
				edit["editedAt"] = time.UnixMilli(int64(timestamp)).UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return edit
}

type historyEvidence struct {
	Version        string    `json:"version"`
	Kind           string    `json:"kind"`
	Phase          string    `json:"phase"`
	SnapshotRef    string    `json:"snapshotRef"`
	CorpusDigest   string    `json:"corpusDigest"`
	SnapshotDigest string    `json:"snapshotDigest,omitempty"`
	CursorHash     string    `json:"cursorHash"`
	PageDigest     string    `json:"pageDigest,omitempty"`
	PageIndex      int       `json:"pageIndex"`
	PageCount      int       `json:"pageCount"`
	MessageCount   int       `json:"messageCount"`
	Complete       bool      `json:"complete"`
	HasMoreBefore  any       `json:"hasMoreBefore"`
	EvidenceRef    string    `json:"evidenceRef,omitempty"`
	ObservedAt     time.Time `json:"observedAt"`
}

func historyPayload(conversationID string, messages []SnapshotMessage, evidence historyEvidence) map[string]any {
	evidence.Version = EvidenceVersion
	evidence.Kind = EvidenceKind
	serializedMessages := serializeSnapshotMessages(messages)
	return map[string]any{
		"conversationId": conversationID,
		"threadId":       conversationID,
		"messages":       serializedMessages,
		"evidence":       evidence,
	}
}

func serializeSnapshotMessages(messages []SnapshotMessage) []map[string]any {
	serializedMessages := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		kind := "text"
		if len(message.Media) > 0 {
			kind = "media"
		} else if message.Text == "" && len(message.Reactions) == 0 {
			kind = "system"
		}
		payload := map[string]any{
			"providerMessageId": message.ProviderMessageID,
			"senderId":          message.SenderID,
			"direction":         message.Direction,
			"timestamp":         message.Timestamp.UTC().Format(time.RFC3339Nano),
			"kind":              kind,
			"text":              message.Text,
			"media":             message.Media,
			"edits":             message.Edits,
			"reactions":         message.Reactions,
		}
		if message.ReplyToProviderMessageID != "" {
			payload["replyToProviderMessageId"] = message.ReplyToProviderMessageID
			payload["replyTo"] = []map[string]string{{"namespace": "meta_message_id", "id": message.ReplyToProviderMessageID}}
		}
		serializedMessages = append(serializedMessages, payload)
	}
	return serializedMessages
}

func historyEvent(source, snapshotRef string, pageIndex int, messages []SnapshotMessage, payload map[string]any) map[string]any {
	return map[string]any{
		"eventId":    fmt.Sprintf("%s:history:%s:%d", source, snapshotRef, pageIndex),
		"eventType":  "history.page",
		"source":     source,
		"occurredAt": observedAt(messages).Format(time.RFC3339Nano),
		"payload":    payload,
	}
}

func pageCursor(messages []SnapshotMessage) string {
	if len(messages) == 0 {
		return "empty"
	}
	return messages[len(messages)-1].ProviderMessageID
}

func digestStrings(values ...string) string {
	hash := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return hex.EncodeToString(hash[:])
}

func digestSnapshot(messages []SnapshotMessage) string {
	data, _ := json.Marshal(serializeSnapshotMessages(messages))
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func ErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrCommandPayloadInvalid):
		return ErrCommandPayloadInvalid.Error()
	case errors.Is(err, ErrLoginNotOwned):
		return ErrLoginNotOwned.Error()
	case errors.Is(err, ErrPortalAmbiguous):
		return ErrPortalAmbiguous.Error()
	case errors.Is(err, ErrPortalNotMaterialized):
		return ErrPortalNotMaterialized.Error()
	case errors.Is(err, ErrNoMatrixRoom):
		return ErrNoMatrixRoom.Error()
	case errors.Is(err, ErrMessageMapping):
		return ErrMessageMapping.Error()
	case errors.Is(err, ErrMatrixEventMissing):
		return ErrMatrixEventMissing.Error()
	default:
		return "history_snapshot_failed"
	}
}

func digestPageDescriptors(pageDigests []string, messageCounts []int) string {
	type pageDescriptor struct {
		PageIndex    int    `json:"pageIndex"`
		PageDigest   string `json:"pageDigest"`
		MessageCount int    `json:"messageCount"`
	}
	descriptors := make([]pageDescriptor, len(pageDigests))
	for index, pageDigest := range pageDigests {
		descriptors[index] = pageDescriptor{PageIndex: index, PageDigest: pageDigest, MessageCount: messageCounts[index]}
	}
	data, _ := json.Marshal(descriptors)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func observedAt(messages []SnapshotMessage) time.Time {
	if len(messages) == 0 {
		return time.Unix(0, 0).UTC()
	}
	return messages[len(messages)-1].Timestamp.UTC()
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
