package connector

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

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
			log.Warn().Err(err).Int64("thread_id", threadID).Msg("Roomless group metadata recovery failed")
			continue
		}
		metadata.FetchAttempted.Store(false)
		recoveryRequests++
	}
	log.Info().Int("recovery_requests", recoveryRequests).Msg("Roomless Messenger group recovery sweep completed")
}

func roomlessGroupRecoveryTarget(portal *database.Portal) (*metaid.PortalMetadata, int64, bool) {
	if portal == nil || portal.MXID != "" || portal.MessageRequest {
		return nil, 0, false
	}
	metadata, ok := portal.Metadata.(*metaid.PortalMetadata)
	if !ok || metadata.ThreadType != table.GROUP_THREAD {
		return nil, 0, false
	}
	threadID := metaid.ParseFBPortalID(portal.ID)
	return metadata, threadID, threadID != 0
}

func (m *MetaClient) recoverRoomlessGroup(ctx context.Context, threadID int64) error {
	transport := m.getTaskTransport()
	if transport == nil {
		return errors.New("task transport unavailable")
	}
	response, err := transport.ExecuteTasks(ctx, newRoomlessGroupRecoveryTask(threadID))
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("roomless group recovery returned no table")
	}
	m.parseAndQueueTable(ctx, response, false)
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
