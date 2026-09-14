package externalhistory

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func testProviderMessageID(messageID networkid.MessageID) (string, bool) {
	return strings.TrimPrefix(string(messageID), "fb:"), strings.HasPrefix(string(messageID), "fb:")
}

func testMessage(messageID, part string, rowID int64, sender string, timestamp time.Time) *database.Message {
	return &database.Message{
		ID:        networkid.MessageID("fb:" + messageID),
		PartID:    networkid.PartID(part),
		MXID:      id.EventID("$" + messageID + ":" + part),
		RowID:     rowID,
		SenderID:  networkid.UserID(sender),
		Timestamp: timestamp,
	}
}

func testLogin(id string) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID(id)}}
}

func testPortal() *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{MXID: "!room:test", PortalKey: networkid.PortalKey{ID: "conversation"}}}
}

func testTextEvent(text string) *event.Event {
	return &event.Event{Type: event.EventMessage, Content: event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: text}}}
}

func TestParseRequestBoundsAndDefault(t *testing.T) {
	conversationID, pageSize, err := ParseRequest(map[string]any{"conversationId": "conversation"})
	if err != nil || conversationID != "conversation" || pageSize != DefaultPageSize {
		t.Fatalf("default request = %q, %d, %v", conversationID, pageSize, err)
	}
	for _, invalid := range []any{0, 101, "many", 1.5} {
		if _, _, err = ParseRequest(map[string]any{"conversationId": "conversation", "pageSize": invalid}); !errors.Is(err, ErrCommandPayloadInvalid) {
			t.Fatalf("page size %v was accepted: %v", invalid, err)
		}
	}
}

func TestMultipartDedupDirectionAndReactions(t *testing.T) {
	base := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)
	rows := []*database.Message{
		testMessage("message-2", "", 3, "other", base.Add(time.Minute)),
		testMessage("message-1", "1", 2, "owner", base),
		testMessage("message-1", "", 1, "owner", base),
	}
	portal := testPortal()
	login := testLogin("owner")
	messages, err := buildSnapshotMessages(
		context.Background(), portal, login, rows, testProviderMessageID,
		func(_ context.Context, _ id.RoomID, eventID id.EventID) (*event.Event, error) {
			if eventID == "$message-1:" {
				return testTextEvent("first"), nil
			}
			return testTextEvent("second"), nil
		},
		func(_ context.Context, _ networkid.UserLoginID, messageID networkid.MessageID) ([]*database.Reaction, error) {
			if messageID == "fb:message-1" {
				return []*database.Reaction{{SenderID: "other", Emoji: "👍", Timestamp: base.Add(2 * time.Minute)}}, nil
			}
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("snapshot messages failed: %v", err)
	}
	if len(messages) != 2 || messages[0].ProviderMessageID != "message-1" || messages[1].ProviderMessageID != "message-2" {
		t.Fatalf("multipart rows were not chronologically deduplicated: %#v", messages)
	}
	if messages[0].Direction != "outbound" || messages[1].Direction != "inbound" {
		t.Fatalf("directions = %q, %q", messages[0].Direction, messages[1].Direction)
	}
	if len(messages[0].Reactions) != 1 || messages[0].Reactions[0]["reaction"] != "👍" {
		t.Fatalf("reactions = %#v", messages[0].Reactions)
	}
}

func TestMissingMatrixEventFailsClosed(t *testing.T) {
	row := testMessage("message-1", "", 1, "owner", time.Now())
	_, err := buildSnapshotMessages(
		context.Background(), testPortal(), testLogin("owner"), []*database.Message{row}, testProviderMessageID,
		func(context.Context, id.RoomID, id.EventID) (*event.Event, error) { return nil, nil }, nil,
	)
	if !errors.Is(err, ErrMatrixEventMissing) {
		t.Fatalf("missing Matrix event error = %v", err)
	}
}

func TestMatrixMediaAndResolvableEditRemainAvailable(t *testing.T) {
	part, err := projectMatrixEvent(&event.Event{
		Type:      event.EventMessage,
		Timestamp: 1234,
		Content: event.Content{Parsed: &event.MessageEventContent{
			MsgType:    event.MsgImage,
			Body:       "caption",
			URL:        "mxc://example/media",
			Info:       &event.FileInfo{MimeType: "image/jpeg"},
			NewContent: &event.MessageEventContent{Body: "edited caption", MsgType: event.MsgImage},
		}},
	})
	if err != nil || part.Edit == nil || len(part.Media) != 1 || part.Media[0]["matrixRef"] != "mxc://example/media" {
		t.Fatalf("matrix projection lost media or edit: %#v, %v", part, err)
	}
	if part.Edit["text"] != "edited caption" {
		t.Fatalf("edit text = %#v", part.Edit["text"])
	}
}

func TestDeterministicPagingDigestAndTerminalProof(t *testing.T) {
	messages := []SnapshotMessage{{ProviderMessageID: "message-1", Timestamp: time.Unix(1, 0).UTC(), Text: "secret"}}
	snapshotRef := digestStrings("messenger", "conversation")
	corpusDigest := digestSnapshot(messages)
	page := historyPayload("conversation", messages, historyEvidence{
		Version: EvidenceVersion, Kind: EvidenceKind, SnapshotRef: snapshotRef, CorpusDigest: corpusDigest,
		CursorHash: digestStrings(snapshotRef, "0", "1", pageCursor(messages)), PageIndex: 0, PageCount: 1,
		MessageCount: 1, Complete: false, HasMoreBefore: nil,
	})
	repeated := historyPayload("conversation", messages, page["evidence"].(historyEvidence))
	firstID := historyEvent("mautrix_native_portal", snapshotRef, 0, messages, page)["eventId"]
	secondID := historyEvent("mautrix_native_portal", snapshotRef, 0, messages, repeated)["eventId"]
	if firstID != secondID || len(snapshotRef) != 64 || len(corpusDigest) != 64 {
		t.Fatalf("paging identity or digest is not deterministic: %v %v %s %s", firstID, secondID, snapshotRef, corpusDigest)
	}
	pageJSON, err := json.Marshal(page)
	if err != nil || !strings.Contains(string(pageJSON), `"hasMoreBefore":null`) {
		t.Fatalf("data page did not serialize explicit null proof: %s, %v", pageJSON, err)
	}
	terminal := historyPayload("conversation", nil, historyEvidence{
		Version: EvidenceVersion, Kind: EvidenceKind, SnapshotRef: snapshotRef, CorpusDigest: corpusDigest,
		CursorHash: digestStrings(snapshotRef, "terminal", "1"), PageIndex: 1, PageCount: 1,
		MessageCount: 1, Complete: true, HasMoreBefore: false, EvidenceRef: digestStrings(snapshotRef, corpusDigest),
	})
	evidence := terminal["evidence"].(historyEvidence)
	if evidence.Version != EvidenceVersion || evidence.Kind != EvidenceKind || evidence.MessageCount != 1 || !evidence.Complete || evidence.HasMoreBefore != false || evidence.EvidenceRef == "" {
		t.Fatalf("terminal proof is incomplete: %#v", evidence)
	}
	terminalJSON, err := json.Marshal(terminal)
	if err != nil || !strings.Contains(string(terminalJSON), `"hasMoreBefore":false`) {
		t.Fatalf("terminal proof did not serialize explicitly: %s, %v", terminalJSON, err)
	}
}

func TestOwnerAndPortalFailClosed(t *testing.T) {
	if _, err := Export(context.Background(), nil, nil, "conversation", 50, "source", func(*bridgev2.Portal) []string { return nil }, testProviderMessageID, nil, func(context.Context, any) error { return nil }); !errors.Is(err, ErrLoginNotOwned) {
		t.Fatalf("nil owner error = %v", err)
	}
	if _, err := selectPortalMatch([]*bridgev2.Portal{testPortal(), testPortal()}); !errors.Is(err, ErrPortalAmbiguous) {
		t.Fatalf("ambiguous portal error = %v", err)
	}
	if _, err := selectPortalMatch(nil); !errors.Is(err, ErrPortalNotMaterialized) {
		t.Fatalf("missing portal error = %v", err)
	}
	roomless := testPortal()
	roomless.MXID = ""
	if err := validatePortalRoom(roomless); !errors.Is(err, ErrNoMatrixRoom) {
		t.Fatalf("roomless portal error = %v", err)
	}
}

func TestStableErrorCodes(t *testing.T) {
	if ErrorCode(ErrNoMatrixRoom) != "history_snapshot_matrix_room_missing" {
		t.Fatalf("matrix room error code = %q", ErrorCode(ErrNoMatrixRoom))
	}
	if ErrorCode(errors.New("private provider detail")) != "history_snapshot_failed" {
		t.Fatalf("unknown errors must be sanitized")
	}
}

func TestCommandResultContainsOnlyAggregatesAndHashes(t *testing.T) {
	result := Result{PageCount: 1, MessageCount: 2, EventCount: 2, SnapshotRef: strings.Repeat("a", 64), CorpusDigest: strings.Repeat("b", 64), CursorHash: strings.Repeat("c", 64), EvidenceRef: strings.Repeat("d", 64)}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(data)
	for _, forbidden := range []string{"message-1", "plaintext", "secret", "providerMessageId", "eventId"} {
		if strings.Contains(serialized, forbidden) {
			t.Fatalf("command result contains %q: %s", forbidden, serialized)
		}
	}
}
