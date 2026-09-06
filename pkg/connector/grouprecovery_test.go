package connector

import (
	"context"
	"errors"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func makeTestPortal(mxid id.RoomID, threadType table.ThreadType) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		MXID:     mxid,
		Metadata: &metaid.PortalMetadata{ThreadType: threadType},
	}}
}

func TestMakeFullThreadMetadataTask(t *testing.T) {
	task := makeFullThreadMetadataTask(12345)
	if task.GetLabel() != "209" {
		t.Fatalf("expected task 209, got %q", task.GetLabel())
	}
	if task.ThreadFBID != 12345 || task.SyncGroup != 1 || task.ForceUpsert != 0 ||
		task.UseOpenMessengerTransport != 0 || task.MetadataOnly != 0 || task.PreviewOnly != 0 {
		t.Fatalf("unexpected full thread metadata task: %+v", task)
	}
}

func TestIsRoomlessMessengerGroup(t *testing.T) {
	if !isRoomlessMessengerGroup(makeTestPortal("", table.GROUP_THREAD)) {
		t.Fatal("expected a roomless Messenger group to be recoverable")
	}
	if isRoomlessMessengerGroup(makeTestPortal("!room:example.com", table.GROUP_THREAD)) {
		t.Fatal("existing Matrix rooms must not be recovered again")
	}
	if isRoomlessMessengerGroup(makeTestPortal("", table.ONE_TO_ONE)) {
		t.Fatal("DM portals must not be included in the connection sweep")
	}
	if isRoomlessMessengerGroup(&bridgev2.Portal{Portal: &database.Portal{}}) {
		t.Fatal("portals without connector metadata must be ignored")
	}
}

func TestRoomlessGroupRecoveryGuardIsRetryable(t *testing.T) {
	portal := makeTestPortal("", table.GROUP_THREAD)
	client := &MetaClient{}
	if !client.markRoomlessGroupRecoveryStarted(portal) {
		t.Fatal("first recovery should acquire the portal guard")
	}
	if client.markRoomlessGroupRecoveryStarted(portal) {
		t.Fatal("a concurrent recovery must not acquire the portal guard")
	}
	resetRoomlessGroupRecovery(portal)
	if !client.markRoomlessGroupRecoveryStarted(portal) {
		t.Fatal("failed recovery must remain retryable")
	}
}

func TestRetryRoomlessGroupRecoveryEventuallySucceeds(t *testing.T) {
	attempts := 0
	err := retryRoomlessGroupRecovery(context.Background(), 3, time.Millisecond, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary metadata failure")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected bounded recovery to succeed: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryRoomlessGroupRecoveryIsBounded(t *testing.T) {
	attempts := 0
	err := retryRoomlessGroupRecovery(context.Background(), 3, time.Millisecond, func(context.Context) error {
		attempts++
		return errors.New("persistent history failure")
	})
	if err == nil {
		t.Fatal("expected persistent recovery failure")
	}
	if attempts != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", attempts)
	}
}

func TestRetryRoomlessGroupRecoveryStopsOnDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := retryRoomlessGroupRecovery(ctx, 3, time.Hour, func(context.Context) error {
		attempts++
		cancel()
		return errors.New("socket disconnected")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("disconnect must stop retries, got %d attempts", attempts)
	}
}
