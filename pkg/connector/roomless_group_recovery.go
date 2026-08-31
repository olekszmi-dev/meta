package connector

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"

	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var providerTraceSalt = func() [32]byte {
	var salt [32]byte
	if _, err := rand.Read(salt[:]); err != nil {
		panic("failed to initialize provider trace salt")
	}
	return salt
}()

func providerTraceHash(kind, value string) string {
	hasher := sha256.New()
	_, _ = hasher.Write(providerTraceSalt[:])
	_, _ = hasher.Write([]byte(kind))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(value))
	return hex.EncodeToString(hasher.Sum(nil)[:12])
}

func roomlessGroupRecoveryTableCounts(response *table.LSTable) map[string]int {
	counts := make(map[string]int)
	if response == nil {
		return counts
	}
	reflected := reflect.ValueOf(response).Elem()
	for _, fieldName := range response.NonNilFields() {
		field := reflected.FieldByName(fieldName)
		if field.IsValid() && field.Kind() == reflect.Slice && field.Len() > 0 {
			counts[fieldName] = field.Len()
		}
	}
	return counts
}

func roomlessGroupRecoveryErrorCodes(response *table.LSTable) []int64 {
	if response == nil || len(response.LSIssueNewError) == 0 {
		return nil
	}
	codes := make([]int64, 0, len(response.LSIssueNewError))
	for _, issue := range response.LSIssueNewError {
		codes = append(codes, issue.ErrorCode)
	}
	return codes
}

func roomlessGroupRecoveryReadyEvent(evt any) bool {
	switch evt.(type) {
	case *messagix.ConnectedEvent, *messagix.ReconnectedEvent:
		return true
	default:
		return false
	}
}

func (m *MetaClient) recoverRoomlessGroups(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	if m.Main == nil || m.Main.Bridge == nil || m.Main.Bridge.DB == nil || m.UserLogin == nil {
		log.Debug().Msg("Skipping roomless group recovery before bridge initialization")
		return
	}
	userPortals, err := m.Main.Bridge.DB.UserPortal.GetAllForLogin(ctx, m.UserLogin.UserLogin)
	if err != nil {
		log.Err(err).Msg("Failed to load login portal mappings for roomless group recovery")
		return
	}

	recoveryRequests := 0
	for _, userPortal := range userPortals {
		portal, portalErr := m.Main.Bridge.DB.Portal.GetByKey(ctx, userPortal.Portal)
		if portalErr != nil {
			log.Warn().Err(portalErr).Msg("Failed to load mapped portal for roomless group recovery")
			continue
		}
		metadata, threadID, ok := roomlessGroupRecoveryTarget(portal)
		if !ok || metadata.FetchAttempted.Swap(true) {
			continue
		}
		if err = m.recoverRoomlessGroup(ctx, threadID); err != nil {
			metadata.FetchAttempted.Store(false)
			log.Warn().Err(err).
				Str("target_hash", providerTraceHash("thread", strconv.FormatInt(threadID, 10))).
				Msg("Roomless group metadata recovery failed")
			continue
		}
		metadata.FetchAttempted.Store(false)
		recoveryRequests++
	}
	log.Info().Int("recovery_requests", recoveryRequests).Msg("Roomless Messenger group recovery sweep completed")
}

func roomlessGroupRecoveryTarget(portal *database.Portal) (*metaid.PortalMetadata, int64, bool) {
	if portal == nil || portal.MXID != "" || portal.Receiver != "" || portal.MessageRequest {
		return nil, 0, false
	}
	metadata, ok := portal.Metadata.(*metaid.PortalMetadata)
	if !ok || (metadata.ThreadType != 0 && metadata.ThreadType != table.GROUP_THREAD) {
		return nil, 0, false
	}
	threadID := metaid.ParseFBPortalID(portal.ID)
	return metadata, threadID, threadID != 0
}

func (m *MetaClient) recoverRoomlessGroup(ctx context.Context, threadID int64) error {
	targetHash := providerTraceHash("thread", strconv.FormatInt(threadID, 10))
	// Protocol receipts must not inherit login or Matrix user identifiers from ctx.
	log := zerologlog.Logger
	transport := m.getTaskTransport()
	if transport == nil {
		return errors.New("task transport unavailable")
	}
	log.Info().Str("trace_event", "task_209_request").Str("target_hash", targetHash).Msg("Requesting roomless group metadata")
	response, err := transport.ExecuteTasks(ctx, newRoomlessGroupRecoveryTask(threadID))
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("roomless group recovery returned no table")
	}
	parsedEvents := m.parseAndQueueTable(ctx, response, false)
	log.Info().
		Str("trace_event", "task_209_response").
		Str("target_hash", targetHash).
		Interface("field_counts", roomlessGroupRecoveryTableCounts(response)).
		Interface("error_codes", roomlessGroupRecoveryErrorCodes(response)).
		Int("parsed_events", parsedEvents).
		Msg("Processed roomless group metadata response")
	return nil
}

func newRoomlessGroupRecoveryTask(threadID int64) *socket.CreateThreadTask {
	return &socket.CreateThreadTask{
		ThreadFBID:                threadID,
		ForceUpsert:               0,
		UseOpenMessengerTransport: 0,
		SyncGroup:                 1,
		MetadataOnly:              0,
		PreviewOnly:               0,
	}
}
