package externalhistory

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/metaid"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

type fakeOfflineReader struct {
	native       OfflineNativeSnapshot
	events       map[string]json.RawMessage
	nativeReads  int
	synapseReads int
}

func (r *fakeOfflineReader) ReadNative(_ context.Context, _ string, _ string) (OfflineNativeSnapshot, error) {
	r.nativeReads++
	return r.native, nil
}

func (r *fakeOfflineReader) ReadSynapseEvents(_ context.Context, _ string, _ []string) (map[string]json.RawMessage, error) {
	r.synapseReads++
	return r.events, nil
}

func offlineEventJSON(eventID, roomID, body string) json.RawMessage {
	value := map[string]any{
		"type": "m.room.message", "event_id": eventID, "room_id": roomID,
		"origin_server_ts": int64(1_789_376_400_000), "sender": "@sender:test",
		"content": map[string]any{"msgtype": "m.text", "body": body},
	}
	raw, _ := json.Marshal(value)
	return raw
}

func offlineFixture(provider string, withEvent bool) (*fakeOfflineReader, OfflineOptions) {
	loginID := "native-login"
	roomID := "!room:test"
	eventID := "$event:test"
	messageID := metaid.MakeFBMessageID("provider-message")
	portal := OfflinePortal{
		OwnerLoginID: loginID,
		BridgeID:     "meta",
		ID:           "conversation",
		MXID:         roomID,
		RoomType:     string(database.RoomTypeGroupDM),
		ThreadType:   2,
		Messages: []*database.Message{{
			RowID: 1, BridgeID: "meta", ID: messageID, MXID: id.EventID(eventID),
			Room: networkid.PortalKey{ID: "conversation"}, SenderID: "sender",
			Timestamp: time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC),
		}},
	}
	events := map[string]json.RawMessage{}
	if withEvent {
		events[eventID] = offlineEventJSON(eventID, roomID, "private fixture text")
	}
	reader := &fakeOfflineReader{
		native: OfflineNativeSnapshot{BridgeID: "meta", UserMXID: "@owner:test", LoginID: loginID, Portals: []OfflinePortal{portal}},
		events: events,
	}
	options := OfflineOptions{
		Provider: provider, NativeURL: "postgres://native", SynapseURL: "postgres://synapse",
		LoginID: loginID, OutputPath: filepath.Join(os.TempDir(), "offline-history-test.json"), PageSize: 50,
		Now: func() time.Time { return time.Date(2026, 9, 14, 11, 0, 0, 0, time.UTC) },
		Classify: func(OfflinePortal) (map[string]any, error) {
			if provider == "messenger" {
				return map[string]any{"connectorLane": "messenger_group_table", "conversationKind": "group"}, nil
			}
			return map[string]any{"conversationKind": "group", "requestStatus": "accepted", "providerIsGroup": true}, nil
		},
	}
	return reader, options
}

func TestOfflineBundleInspectIsDeterministicAndNonMutating(t *testing.T) {
	for _, provider := range []string{"messenger", "instagram"} {
		reader, options := offlineFixture(provider, true)
		os.Remove(options.OutputPath)
		first, report, err := BuildOfflineBundle(context.Background(), reader, options)
		if err != nil {
			t.Fatalf("%s inspect failed: %v", provider, err)
		}
		second, secondReport, err := BuildOfflineBundle(context.Background(), reader, options)
		if err != nil {
			t.Fatalf("%s replay failed: %v", provider, err)
		}
		if report.DatasetDigest != secondReport.DatasetDigest || first.Manifest.DatasetDigest != second.Manifest.DatasetDigest {
			t.Fatalf("%s dataset digest changed on replay", provider)
		}
		if report.CompleteThreadCount != 1 || report.FailedThreadCount != 0 || report.MessageCount != 1 || report.EventCount != 2 {
			t.Fatalf("%s aggregate report = %#v", provider, report)
		}
		if _, err = os.Stat(options.OutputPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s inspect wrote output", provider)
		}
		serialized, _ := json.Marshal(report)
		if strings.Contains(string(serialized), "private fixture text") || strings.Contains(string(serialized), "provider-message") {
			t.Fatalf("%s report leaked message evidence", provider)
		}
		if reader.nativeReads != 2 || reader.synapseReads != 2 {
			t.Fatalf("%s unexpected reader calls: %#v", provider, reader)
		}
	}
}

func TestOfflineBundleMissingMatrixEventFailsThreadClosed(t *testing.T) {
	reader, options := offlineFixture("messenger", false)
	bundle, report, err := BuildOfflineBundle(context.Background(), reader, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.CompleteThreads) != 0 || report.CompleteThreadCount != 0 || report.FailedThreadCount != 1 {
		t.Fatalf("missing event was sealed: %#v", report)
	}
	if bundle.FailedThreads[0].ErrorCode != ErrMatrixEventMissing.Error() {
		t.Fatalf("missing event code = %q", bundle.FailedThreads[0].ErrorCode)
	}
}

func TestOfflineBundleOwnerAndClassifierFailClosed(t *testing.T) {
	reader, options := offlineFixture("instagram", true)
	reader.native.LoginID = "other-login"
	if _, _, err := BuildOfflineBundle(context.Background(), reader, options); !errors.Is(err, ErrOfflineLoginScope) {
		t.Fatalf("owner mismatch error = %v", err)
	}
	reader, options = offlineFixture("messenger", true)
	options.Classify = func(OfflinePortal) (map[string]any, error) { return nil, ErrProtocolUnclassified }
	bundle, _, err := BuildOfflineBundle(context.Background(), reader, options)
	if err != nil || len(bundle.CompleteThreads) != 0 || bundle.FailedThreads[0].ErrorCode != ErrProtocolUnclassified.Error() {
		t.Fatalf("unclassified portal did not fail closed: %#v, %v", bundle, err)
	}
}

func TestOfflineBundleWriteIsAtomicAndProtected(t *testing.T) {
	reader, options := offlineFixture("instagram", true)
	options.OutputPath = filepath.Join(t.TempDir(), "bundle.json")
	bundle, _, err := BuildOfflineBundle(context.Background(), reader, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = WriteBundleAtomic(options.OutputPath, bundle); err != nil {
		t.Fatal(err)
	}
	metadata, err := os.Stat(options.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && metadata.Mode().Perm()&0077 != 0 {
		t.Fatalf("bundle permissions = %o", metadata.Mode().Perm())
	}
	var decoded OfflineBundle
	raw, _ := os.ReadFile(options.OutputPath)
	if json.Unmarshal(raw, &decoded) != nil || decoded.Manifest.DatasetDigest != bundle.Manifest.DatasetDigest {
		t.Fatal("written bundle did not round trip")
	}
}

func TestDecodeProviderMessageIDForMessengerAndInstagram(t *testing.T) {
	for _, provider := range []string{"messenger", "instagram"} {
		decoded, ok := DecodeProviderMessageID(provider, metaid.MakeFBMessageID("message-id"))
		if !ok || decoded != "message-id" {
			t.Fatalf("%s provider ID = %q, %t", provider, decoded, ok)
		}
	}
	if _, ok := DecodeProviderMessageID("unsupported", "message-id"); ok {
		t.Fatal("unsupported provider ID was accepted")
	}
}
