package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

const (
	roomlessGroupRecoveryLimit       = 10
	roomlessGroupRecoveryAttempts    = 3
	roomlessGroupRecoveryBackoff     = 500 * time.Millisecond
	roomlessGroupMaterializeTimeout  = 15 * time.Second
	roomlessGroupMaterializePollRate = 100 * time.Millisecond
)

var errRoomlessGroupNotMaterialized = errors.New("roomless group portal was not materialized")

func makeFullThreadMetadataTask(threadID int64) *socket.CreateThreadTask {
	return &socket.CreateThreadTask{
		ThreadFBID:                threadID,
		ForceUpsert:               0,
		UseOpenMessengerTransport: 0,
		SyncGroup:                 1,
		MetadataOnly:              0,
		PreviewOnly:               0,
	}
}

func isRoomlessMessengerGroup(portal *bridgev2.Portal) bool {
	if portal == nil || portal.MXID != "" {
		return false
	}
	metadata, ok := getPortalMetadata(portal)
	return ok && metadata.ThreadType == table.GROUP_THREAD
}

func getPortalMetadata(portal *bridgev2.Portal) (*metaid.PortalMetadata, bool) {
	if portal == nil || portal.Portal == nil {
		return nil, false
	}
	metadata, ok := portal.Metadata.(*metaid.PortalMetadata)
	return metadata, ok && metadata != nil
}

func (m *MetaClient) recoverRoomlessGroupPortals(ctx context.Context) {
	if !m.recoveringBareGroups.CompareAndSwap(false, true) {
		return
	}
	defer m.recoveringBareGroups.Store(false)

	if !m.waitForInitialTable(ctx) {
		return
	}
	links, err := m.Main.Bridge.DB.UserPortal.GetAllForLogin(ctx, m.UserLogin.UserLogin)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to list portals for roomless group recovery")
		return
	}
	zerolog.Ctx(ctx).Debug().Int("portal_links", len(links)).Msg("Scanning for roomless Messenger groups")

	recovered := 0
	for _, link := range links {
		if recovered >= roomlessGroupRecoveryLimit || ctx.Err() != nil {
			return
		}
		portal, err := m.Main.Bridge.GetExistingPortalByKey(ctx, link.Portal)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Failed to load portal for roomless group recovery")
			continue
		} else if !isRoomlessMessengerGroup(portal) {
			continue
		}
		if !m.markRoomlessGroupRecoveryStarted(portal) {
			continue
		}

		recovered++
		err = m.recoverRoomlessGroupPortal(ctx, portal.PortalKey)
		if err != nil {
			resetRoomlessGroupRecovery(portal)
			zerolog.Ctx(ctx).Err(err).
				Msg("Roomless group recovery failed; it will be retried on a later connection")
		}
	}
	zerolog.Ctx(ctx).Info().Int("recovery_requests", recovered).Msg("Roomless Messenger group recovery sweep completed")
}

func (m *MetaClient) scheduleRoomlessPortalRecovery(ctx context.Context, portal *bridgev2.Portal) {
	if portal == nil || portal.MXID != "" || !m.markRoomlessGroupRecoveryStarted(portal) {
		return
	}
	go func() {
		err := m.recoverRoomlessGroupPortal(ctx, portal.PortalKey)
		if err != nil {
			resetRoomlessGroupRecovery(portal)
			zerolog.Ctx(ctx).Err(err).
				Msg("Roomless group recovery failed; it will be retried on a later connection")
		}
	}()
}

func (m *MetaClient) markRoomlessGroupRecoveryStarted(portal *bridgev2.Portal) bool {
	metadata, ok := getPortalMetadata(portal)
	if !ok {
		return false
	}
	return !metadata.FetchAttempted.Swap(true)
}

func resetRoomlessGroupRecovery(portal *bridgev2.Portal) {
	if metadata, ok := getPortalMetadata(portal); ok {
		metadata.FetchAttempted.Store(false)
	}
}

func (m *MetaClient) waitForInitialTable(ctx context.Context) bool {
	if m.initialTableHandled.Load() {
		return true
	}
	ticker := time.NewTicker(roomlessGroupMaterializePollRate)
	defer ticker.Stop()
	timer := time.NewTimer(roomlessGroupMaterializeTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			zerolog.Ctx(ctx).Warn().Msg("Timed out waiting for initial table before roomless group recovery")
			return false
		case <-ticker.C:
			if m.initialTableHandled.Load() {
				return true
			}
		}
	}
}

func (m *MetaClient) recoverRoomlessGroupPortal(ctx context.Context, portalKey networkid.PortalKey) error {
	return retryRoomlessGroupRecovery(ctx, roomlessGroupRecoveryAttempts, roomlessGroupRecoveryBackoff, func(ctx context.Context) error {
		portal, err := m.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
		if err != nil {
			return fmt.Errorf("failed to reload roomless group portal: %w", err)
		} else if portal == nil {
			return errors.New("roomless group portal disappeared")
		}

		tbl, err := m.Client.ExecuteTasks(ctx, makeFullThreadMetadataTask(metaid.ParseFBPortalID(portalKey.ID)))
		if err == nil && tbl == nil {
			err = errors.New("full thread metadata request returned no table")
		}
		if err == nil {
			m.parseAndQueueTable(ctx, tbl, false)
			err = m.waitForPortalMaterialization(ctx, portalKey)
		}
		return err
	})
}

func retryRoomlessGroupRecovery(
	ctx context.Context,
	attempts int,
	backoff time.Duration,
	recover func(context.Context) error,
) error {
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		lastErr = recover(ctx)
		if lastErr == nil {
			return nil
		}
		if attempt+1 < attempts && !waitForRoomlessGroupRetry(ctx, backoff*time.Duration(1<<attempt)) {
			return ctx.Err()
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", attempts, lastErr)
}

func (m *MetaClient) waitForPortalMaterialization(ctx context.Context, portalKey networkid.PortalKey) error {
	ticker := time.NewTicker(roomlessGroupMaterializePollRate)
	defer ticker.Stop()
	timer := time.NewTimer(roomlessGroupMaterializeTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return errRoomlessGroupNotMaterialized
		case <-ticker.C:
			portal, err := m.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
			if err != nil {
				return err
			} else if portal != nil && portal.MXID != "" {
				return nil
			}
		}
	}
}

func waitForRoomlessGroupRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
