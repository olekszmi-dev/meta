package externalhistory

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"maunium.net/go/mautrix/bridgev2"
	bridgedb "maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const OfflineBundleVersion = "zero.messaging.offline-history-bundle.v1"

var (
	ErrOfflineOptions       = errors.New("offline_history_options_invalid")
	ErrOfflineProvider      = errors.New("offline_history_provider_invalid")
	ErrOfflineLoginScope    = errors.New("offline_history_login_scope_invalid")
	ErrOfflineNativeRead    = errors.New("offline_history_native_read_failed")
	ErrOfflineSynapseRead   = errors.New("offline_history_synapse_read_failed")
	ErrOfflineBundleWrite   = errors.New("offline_history_bundle_write_failed")
	ErrOfflineTerminalProof = errors.New("offline_history_terminal_proof_invalid")
)

func OfflineErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrOfflineOptions):
		return ErrOfflineOptions.Error()
	case errors.Is(err, ErrOfflineProvider):
		return ErrOfflineProvider.Error()
	case errors.Is(err, ErrOfflineLoginScope):
		return ErrOfflineLoginScope.Error()
	case errors.Is(err, ErrOfflineNativeRead):
		return ErrOfflineNativeRead.Error()
	case errors.Is(err, ErrOfflineSynapseRead):
		return ErrOfflineSynapseRead.Error()
	case errors.Is(err, ErrOfflineBundleWrite):
		return ErrOfflineBundleWrite.Error()
	case errors.Is(err, ErrOfflineTerminalProof):
		return ErrOfflineTerminalProof.Error()
	default:
		return ErrorCode(err)
	}
}

type OfflineBundle struct {
	Version            string          `json:"version"`
	Provider           string          `json:"provider"`
	Source             string          `json:"source"`
	NativeLoginRefHash string          `json:"nativeLoginRefHash"`
	CreatedAt          string          `json:"createdAt"`
	PageSize           int             `json:"pageSize"`
	CompleteThreads    []BundleThread  `json:"completeThreads"`
	FailedThreads      []BundleFailure `json:"failedThreads"`
	Manifest           BundleManifest  `json:"manifest"`
}

type BundleThread struct {
	ConversationID string           `json:"conversationId"`
	Events         []map[string]any `json:"events"`
}

type BundleFailure struct {
	ConversationRefHash string `json:"conversationRefHash"`
	ErrorCode           string `json:"errorCode"`
}

type BundleManifest struct {
	CompleteThreadCount int    `json:"completeThreadCount"`
	FailedThreadCount   int    `json:"failedThreadCount"`
	EventCount          int    `json:"eventCount"`
	MessageCount        int    `json:"messageCount"`
	DatasetDigest       string `json:"datasetDigest"`
}

type OfflineOptions struct {
	Provider   string
	NativeURL  string
	SynapseURL string
	LoginID    string
	OutputPath string
	PageSize   int
	Now        func() time.Time
	Classify   OfflinePortalClassifier
}

type OfflinePortalClassifier func(OfflinePortal) (map[string]any, error)

type OfflineReport struct {
	Provider            string         `json:"provider"`
	NativeLoginRefHash  string         `json:"nativeLoginRefHash"`
	CompleteThreadCount int            `json:"completeThreadCount"`
	FailedThreadCount   int            `json:"failedThreadCount"`
	EventCount          int            `json:"eventCount"`
	MessageCount        int            `json:"messageCount"`
	DatasetDigest       string         `json:"datasetDigest"`
	FailureCodes        map[string]int `json:"failureCodes,omitempty"`
}

type OfflinePortal struct {
	OwnerLoginID   string
	BridgeID       string
	ID             string
	Receiver       string
	MXID           string
	RoomType       string
	MessageRequest bool
	ThreadType     int64
	Messages       []*bridgedb.Message
	Reactions      []*bridgedb.Reaction
}

type OfflineNativeSnapshot struct {
	BridgeID string
	UserMXID string
	LoginID  string
	Portals  []OfflinePortal
}

type OfflineSnapshotReader interface {
	ReadNative(context.Context, string, string) (OfflineNativeSnapshot, error)
	ReadSynapseEvents(context.Context, string, []string) (map[string]json.RawMessage, error)
}

type PostgresSnapshotReader struct {
	native  *sql.DB
	synapse *sql.DB
}

func NewPostgresSnapshotReader(native, synapse *sql.DB) *PostgresSnapshotReader {
	return &PostgresSnapshotReader{native: native, synapse: synapse}
}

func (r *PostgresSnapshotReader) ReadNative(ctx context.Context, provider, loginID string) (OfflineNativeSnapshot, error) {
	if r == nil || r.native == nil {
		return OfflineNativeSnapshot{}, ErrOfflineNativeRead
	}
	tx, err := readOnlyTx(ctx, r.native)
	if err != nil {
		return OfflineNativeSnapshot{}, ErrOfflineNativeRead
	}
	defer tx.Rollback()

	loginRows, err := tx.QueryContext(ctx, `
		SELECT bridge_id, user_mxid, id
		FROM user_login
		WHERE id = $1
		ORDER BY bridge_id
	`, loginID)
	if err != nil {
		return OfflineNativeSnapshot{}, ErrOfflineNativeRead
	}
	var snapshot OfflineNativeSnapshot
	loginCount := 0
	for loginRows.Next() {
		loginCount++
		if err = loginRows.Scan(&snapshot.BridgeID, &snapshot.UserMXID, &snapshot.LoginID); err != nil {
			loginRows.Close()
			return OfflineNativeSnapshot{}, ErrOfflineNativeRead
		}
	}
	if err = loginRows.Err(); err != nil {
		loginRows.Close()
		return OfflineNativeSnapshot{}, ErrOfflineNativeRead
	}
	loginRows.Close()
	if loginCount != 1 || snapshot.LoginID != loginID {
		return OfflineNativeSnapshot{}, ErrOfflineLoginScope
	}

	portalRows, err := tx.QueryContext(ctx, `
		SELECT p.id, p.receiver, p.mxid, p.room_type, p.message_request, p.metadata
		FROM user_portal up
		JOIN portal p ON p.bridge_id = up.bridge_id
			AND p.id = up.portal_id
			AND p.receiver = up.portal_receiver
		WHERE up.bridge_id = $1 AND up.user_mxid = $2 AND up.login_id = $3
		ORDER BY p.id, p.receiver
	`, snapshot.BridgeID, snapshot.UserMXID, snapshot.LoginID)
	if err != nil {
		return OfflineNativeSnapshot{}, ErrOfflineNativeRead
	}
	for portalRows.Next() {
		var portal OfflinePortal
		var mxid sql.NullString
		var metadata []byte
		if err = portalRows.Scan(&portal.ID, &portal.Receiver, &mxid, &portal.RoomType, &portal.MessageRequest, &metadata); err != nil {
			return OfflineNativeSnapshot{}, ErrOfflineNativeRead
		}
		portal.OwnerLoginID = snapshot.LoginID
		portal.BridgeID = snapshot.BridgeID
		portal.MXID = mxid.String
		portal.ThreadType = metadataThreadType(metadata)
		snapshot.Portals = append(snapshot.Portals, portal)
	}
	if err = portalRows.Err(); err != nil {
		portalRows.Close()
		return OfflineNativeSnapshot{}, ErrOfflineNativeRead
	}
	portalRows.Close()
	for index := range snapshot.Portals {
		if err = readPortalRows(ctx, tx, &snapshot.Portals[index]); err != nil {
			return OfflineNativeSnapshot{}, ErrOfflineNativeRead
		}
	}
	return snapshot, nil
}

func readPortalRows(ctx context.Context, tx *sql.Tx, portal *OfflinePortal) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT rowid, id, part_id, mxid, sender_id, timestamp, edit_count,
		       thread_root_id, reply_to_id, reply_to_part_id, metadata
		FROM message
		WHERE bridge_id = $1 AND room_id = $2 AND room_receiver = $3
		ORDER BY timestamp, id, part_id, rowid
	`, portal.BridgeID, portal.ID, portal.Receiver)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var row bridgedb.Message
		var partID, replyID, replyPartID, threadRoot sql.NullString
		var timestamp int64
		var metadata []byte
		if err = rows.Scan(&row.RowID, &row.ID, &partID, &row.MXID, &row.SenderID, &timestamp, &row.EditCount, &threadRoot, &replyID, &replyPartID, &metadata); err != nil {
			return err
		}
		row.BridgeID = networkid.BridgeID(portal.BridgeID)
		row.PartID = networkid.PartID(partID.String)
		row.Room = networkid.PortalKey{ID: networkid.PortalID(portal.ID), Receiver: networkid.UserLoginID(portal.Receiver)}
		row.Timestamp = time.Unix(0, timestamp).UTC()
		row.ThreadRoot = networkid.MessageID(threadRoot.String)
		row.ReplyTo = networkid.MessageOptionalPartID{MessageID: networkid.MessageID(replyID.String)}
		if replyPartID.Valid {
			part := networkid.PartID(replyPartID.String)
			row.ReplyTo.PartID = &part
		}
		if len(metadata) > 0 {
			row.Metadata = json.RawMessage(metadata)
		}
		portal.Messages = append(portal.Messages, &row)
	}
	if err = rows.Err(); err != nil {
		return err
	}

	reactionRows, err := tx.QueryContext(ctx, `
		SELECT message_id, message_part_id, sender_id, emoji_id, emoji, mxid, timestamp, metadata
		FROM reaction
		WHERE bridge_id = $1 AND room_id = $2 AND room_receiver = $3
		ORDER BY message_id, message_part_id, sender_id, emoji_id
	`, portal.BridgeID, portal.ID, portal.Receiver)
	if err != nil {
		return err
	}
	defer reactionRows.Close()
	for reactionRows.Next() {
		var reaction bridgedb.Reaction
		var partID, metadata []byte
		var timestamp int64
		if err = reactionRows.Scan(&reaction.MessageID, &partID, &reaction.SenderID, &reaction.EmojiID, &reaction.Emoji, &reaction.MXID, &timestamp, &metadata); err != nil {
			return err
		}
		reaction.BridgeID = networkid.BridgeID(portal.BridgeID)
		reaction.Room = networkid.PortalKey{ID: networkid.PortalID(portal.ID), Receiver: networkid.UserLoginID(portal.Receiver)}
		reaction.MessagePartID = networkid.PartID(string(partID))
		reaction.Timestamp = time.Unix(0, timestamp).UTC()
		if len(metadata) > 0 {
			reaction.Metadata = json.RawMessage(metadata)
		}
		portal.Reactions = append(portal.Reactions, &reaction)
	}
	return reactionRows.Err()
}

func (r *PostgresSnapshotReader) ReadSynapseEvents(ctx context.Context, roomID string, eventIDs []string) (map[string]json.RawMessage, error) {
	if r == nil || r.synapse == nil {
		return nil, ErrOfflineSynapseRead
	}
	result := make(map[string]json.RawMessage, len(eventIDs))
	if len(eventIDs) == 0 {
		return result, nil
	}
	tx, err := readOnlyTx(ctx, r.synapse)
	if err != nil {
		return nil, ErrOfflineSynapseRead
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		SELECT event_id, json
		FROM event_json
		WHERE room_id = $1 AND event_id = ANY($2)
	`, roomID, pqArray(eventIDs))
	if err != nil {
		return nil, ErrOfflineSynapseRead
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		var raw []byte
		if err = rows.Scan(&eventID, &raw); err != nil {
			return nil, ErrOfflineSynapseRead
		}
		result[eventID] = append(json.RawMessage(nil), raw...)
	}
	if err = rows.Err(); err != nil {
		return nil, ErrOfflineSynapseRead
	}
	return result, nil
}

// pqArray is kept behind this small helper so the reader's public contract
// does not expose a driver-specific argument type.
func pqArray(values []string) any {
	return pq.Array(values)
}

func readOnlyTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func BuildOfflineBundle(ctx context.Context, reader OfflineSnapshotReader, options OfflineOptions) (OfflineBundle, OfflineReport, error) {
	if err := validateOfflineOptions(options); err != nil {
		return OfflineBundle{}, OfflineReport{}, err
	}
	native, err := reader.ReadNative(ctx, options.Provider, options.LoginID)
	if err != nil {
		return OfflineBundle{}, OfflineReport{}, err
	}
	if native.LoginID != options.LoginID || native.LoginID == "" {
		return OfflineBundle{}, OfflineReport{}, ErrOfflineLoginScope
	}
	loginHash := sha256Hex(native.LoginID)
	complete := make([]BundleThread, 0, len(native.Portals))
	failed := make([]BundleFailure, 0)
	source := sourceForProvider(options.Provider)
	sort.SliceStable(native.Portals, func(i, j int) bool {
		if native.Portals[i].ID != native.Portals[j].ID {
			return native.Portals[i].ID < native.Portals[j].ID
		}
		return native.Portals[i].Receiver < native.Portals[j].Receiver
	})

	for _, portal := range native.Portals {
		if portal.OwnerLoginID != native.LoginID || portal.BridgeID != native.BridgeID || portal.ID == "" {
			continue
		}
		conversationRefHash := sha256Hex(portal.ID)
		roomID := portal.MXID
		if roomID == "" {
			failed = append(failed, BundleFailure{ConversationRefHash: conversationRefHash, ErrorCode: ErrorCode(ErrNoMatrixRoom)})
			continue
		}
		eventIDs := make([]string, 0, len(portal.Messages))
		for _, row := range portal.Messages {
			if row == nil || row.MXID == "" {
				failed = append(failed, BundleFailure{ConversationRefHash: conversationRefHash, ErrorCode: ErrorCode(ErrMessageMapping)})
				eventIDs = nil
				break
			}
			eventIDs = append(eventIDs, string(row.MXID))
		}
		eventJSON, readErr := reader.ReadSynapseEvents(ctx, roomID, eventIDs)
		if readErr != nil {
			failed = append(failed, BundleFailure{ConversationRefHash: conversationRefHash, ErrorCode: ErrOfflineSynapseRead.Error()})
			continue
		}
		bridgePortal := &bridgev2.Portal{Portal: &bridgedb.Portal{
			BridgeID:  networkid.BridgeID(portal.BridgeID),
			PortalKey: networkid.PortalKey{ID: networkid.PortalID(portal.ID), Receiver: networkid.UserLoginID(portal.Receiver)},
			MXID:      id.RoomID(roomID),
		}}
		login := &bridgev2.UserLogin{UserLogin: &bridgedb.UserLogin{ID: networkid.UserLoginID(native.LoginID)}}
		fetch := func(_ context.Context, _ id.RoomID, eventID id.EventID) (*event.Event, error) {
			raw, ok := eventJSON[string(eventID)]
			if !ok {
				return nil, ErrMatrixEventMissing
			}
			var evt event.Event
			if err := json.Unmarshal(raw, &evt); err != nil {
				return nil, ErrMatrixEventMissing
			}
			return &evt, nil
		}
		reactions := func(_ context.Context, _ networkid.UserLoginID, messageID networkid.MessageID) ([]*bridgedb.Reaction, error) {
			result := make([]*bridgedb.Reaction, 0)
			for _, reaction := range portal.Reactions {
				if reaction != nil && reaction.MessageID == messageID {
					result = append(result, reaction)
				}
			}
			return result, nil
		}
		messages, buildErr := BuildSnapshotMessages(ctx, bridgePortal, login, portal.Messages, func(messageID networkid.MessageID) (string, bool) {
			return DecodeProviderMessageID(options.Provider, messageID)
		}, fetch, reactions)
		if buildErr != nil {
			failed = append(failed, BundleFailure{ConversationRefHash: conversationRefHash, ErrorCode: ErrorCode(buildErr)})
			continue
		}
		classification, classifyErr := options.Classify(portal)
		if classifyErr != nil {
			failed = append(failed, BundleFailure{ConversationRefHash: conversationRefHash, ErrorCode: ErrorCode(classifyErr)})
			continue
		}
		events, _, buildErr := BuildHistoryEvents(source, portal.ID, messages, options.PageSize, func(history map[string]any) {
			DecorateHistoryEvent(history, classification)
		})
		if buildErr != nil {
			failed = append(failed, BundleFailure{ConversationRefHash: conversationRefHash, ErrorCode: ErrorCode(buildErr)})
			continue
		}
		if !hasValidTerminalEvent(events) {
			failed = append(failed, BundleFailure{ConversationRefHash: conversationRefHash, ErrorCode: ErrOfflineTerminalProof.Error()})
			continue
		}
		complete = append(complete, BundleThread{ConversationID: portal.ID, Events: events})
	}

	sort.SliceStable(complete, func(i, j int) bool { return complete[i].ConversationID < complete[j].ConversationID })
	sort.SliceStable(failed, func(i, j int) bool {
		if failed[i].ConversationRefHash != failed[j].ConversationRefHash {
			return failed[i].ConversationRefHash < failed[j].ConversationRefHash
		}
		return failed[i].ErrorCode < failed[j].ErrorCode
	})
	eventCount := 0
	messageCount := 0
	for _, thread := range complete {
		eventCount += len(thread.Events)
		messageCount += threadMessageCount(thread.Events)
	}
	datasetDigest := digestDataset(options.Provider, source, loginHash, options.PageSize, complete, failed)
	failureCodes := make(map[string]int)
	for _, failure := range failed {
		failureCodes[failure.ErrorCode]++
	}
	now := time.Now
	if options.Now != nil {
		now = options.Now
	}
	bundle := OfflineBundle{
		Version: OfflineBundleVersion, Provider: options.Provider, Source: source,
		NativeLoginRefHash: loginHash, CreatedAt: now().UTC().Format(time.RFC3339Nano), PageSize: options.PageSize,
		CompleteThreads: complete, FailedThreads: failed,
		Manifest: BundleManifest{CompleteThreadCount: len(complete), FailedThreadCount: len(failed), EventCount: eventCount, MessageCount: messageCount, DatasetDigest: datasetDigest},
	}
	report := OfflineReport{Provider: options.Provider, NativeLoginRefHash: loginHash, CompleteThreadCount: len(complete), FailedThreadCount: len(failed), EventCount: eventCount, MessageCount: messageCount, DatasetDigest: datasetDigest, FailureCodes: failureCodes}
	return bundle, report, nil
}

func ValidateProvider(provider string) bool {
	return provider == "messenger" || provider == "instagram"
}

func sourceForProvider(provider string) string {
	if provider == "instagram" {
		return "instagram_native_portal"
	}
	return "mautrix_native_portal"
}

func validateOfflineOptions(options OfflineOptions) error {
	if !ValidateProvider(options.Provider) || strings.TrimSpace(options.NativeURL) == "" || strings.TrimSpace(options.SynapseURL) == "" || strings.TrimSpace(options.LoginID) == "" || strings.TrimSpace(options.OutputPath) == "" || options.Classify == nil {
		if !ValidateProvider(options.Provider) {
			return ErrOfflineProvider
		}
		return ErrOfflineOptions
	}
	if options.PageSize < 1 || options.PageSize > MaxPageSize {
		return ErrOfflineOptions
	}
	return nil
}

func DecorateHistoryEvent(history map[string]any, classification map[string]any) {
	for key, value := range classification {
		history[key] = value
	}
	if payload, ok := history["payload"].(map[string]any); ok {
		for key, value := range classification {
			payload[key] = value
		}
	}
}

func hasValidTerminalEvent(events []map[string]any) bool {
	if len(events) == 0 {
		return false
	}
	terminal := events[len(events)-1]
	if terminal["eventType"] != "history.page" {
		return false
	}
	payload, ok := terminal["payload"].(map[string]any)
	if !ok {
		return false
	}
	evidence, ok := payload["evidence"].(historyEvidence)
	return ok && evidence.Phase == "terminal" && evidence.Complete && evidence.HasMoreBefore == false
}

func threadMessageCount(events []map[string]any) int {
	count := 0
	for _, history := range events {
		payload, ok := history["payload"].(map[string]any)
		if !ok {
			continue
		}
		messages, ok := payload["messages"].([]map[string]any)
		if ok {
			count += len(messages)
		}
	}
	return count
}

type datasetDigestInput struct {
	Version            string          `json:"version"`
	Provider           string          `json:"provider"`
	Source             string          `json:"source"`
	NativeLoginRefHash string          `json:"nativeLoginRefHash"`
	PageSize           int             `json:"pageSize"`
	CompleteThreads    []BundleThread  `json:"completeThreads"`
	FailedThreads      []BundleFailure `json:"failedThreads"`
}

func digestDataset(provider, source, loginHash string, pageSize int, complete []BundleThread, failed []BundleFailure) string {
	data := canonicalJSON(datasetDigestInput{Version: OfflineBundleVersion, Provider: provider, Source: source, NativeLoginRefHash: loginHash, PageSize: pageSize, CompleteThreads: complete, FailedThreads: failed})
	return sha256Hex(string(data))
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func metadataThreadType(metadata []byte) int64 {
	if len(metadata) == 0 {
		return 0
	}
	var decoded map[string]any
	if json.Unmarshal(metadata, &decoded) != nil {
		return 0
	}
	value, ok := decoded["thread_type"]
	if !ok {
		return 0
	}
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case json.Number:
		result, _ := typed.Int64()
		return result
	case string:
		result, _ := strconv.ParseInt(typed, 10, 64)
		return result
	default:
		return 0
	}
}

func WriteBundleAtomic(path string, bundle OfflineBundle) error {
	if strings.TrimSpace(path) == "" {
		return ErrOfflineBundleWrite
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".zero-offline-history-*")
	if err != nil {
		return ErrOfflineBundleWrite
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err = temporary.Chmod(0600); err != nil {
		temporary.Close()
		return ErrOfflineBundleWrite
	}
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err == nil {
		_, err = temporary.Write(append(data, '\n'))
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return ErrOfflineBundleWrite
	}
	if err = os.Rename(temporaryName, path); err != nil {
		return ErrOfflineBundleWrite
	}
	if err = os.Chmod(path, 0600); err != nil {
		return ErrOfflineBundleWrite
	}
	return nil
}
