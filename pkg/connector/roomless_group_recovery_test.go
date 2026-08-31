package connector

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

const fixtureThreadID int64 = 1003957859287190

type scriptedTaskTransport struct {
	mu      sync.Mutex
	cursor  string
	tasks   []socket.Task
	execute func(context.Context, socket.Task) (*table.LSTable, error)
}

func (s *scriptedTaskTransport) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	if len(tasks) != 1 {
		return nil, errors.New("fixture expects one task at a time")
	}
	s.mu.Lock()
	s.tasks = append(s.tasks, tasks[0])
	s.mu.Unlock()
	return s.execute(ctx, tasks[0])
}

func (s *scriptedTaskTransport) GetCursor(int64) string {
	return s.cursor
}

func (s *scriptedTaskTransport) snapshotTasks() []socket.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]socket.Task(nil), s.tasks...)
}

func TestRoomlessGroupRecoveryTarget(t *testing.T) {
	portal := &database.Portal{
		PortalKey: networkid.PortalKey{ID: metaid.MakeFBPortalID(fixtureThreadID)},
		Metadata:  &metaid.PortalMetadata{ThreadType: table.GROUP_THREAD},
	}
	_, threadID, ok := roomlessGroupRecoveryTarget(portal)
	if !ok || threadID != fixtureThreadID {
		t.Fatalf("expected roomless group target, got ok=%v thread=%d", ok, threadID)
	}
	portal.MXID = id.RoomID("!room:example.test")
	if _, _, ok = roomlessGroupRecoveryTarget(portal); ok {
		t.Fatal("portal with a Matrix room must not be recovered again")
	}
	portal.MXID = ""
	portal.MessageRequest = true
	if _, _, ok = roomlessGroupRecoveryTarget(portal); ok {
		t.Fatal("pending group must remain filtered")
	}
	portal.MessageRequest = false
	portal.Metadata.(*metaid.PortalMetadata).ThreadType = table.ONE_TO_ONE
	if _, _, ok = roomlessGroupRecoveryTarget(portal); ok {
		t.Fatal("DM must not enter roomless group recovery")
	}
	portal.Metadata.(*metaid.PortalMetadata).ThreadType = 0
	if _, _, ok = roomlessGroupRecoveryTarget(portal); !ok {
		t.Fatal("unknown shared portal must enter metadata recovery")
	}
	portal.Receiver = networkid.UserLoginID("login")
	if _, _, ok = roomlessGroupRecoveryTarget(portal); ok {
		t.Fatal("unknown login-scoped portal must not be treated as a group")
	}
}

func TestRoomlessGroupRecoveryReadyEvents(t *testing.T) {
	if !roomlessGroupRecoveryReadyEvent(&messagix.ConnectedEvent{}) {
		t.Fatal("initial connection must trigger roomless group recovery")
	}
	if !roomlessGroupRecoveryReadyEvent(&messagix.ReconnectedEvent{}) {
		t.Fatal("persisted-session reconnect must trigger roomless group recovery")
	}
	if roomlessGroupRecoveryReadyEvent(&messagix.TransientDisconnectEvent{}) {
		t.Fatal("disconnect must not trigger roomless group recovery")
	}
}

func TestRecoverRoomlessGroupQueuesTask209Response(t *testing.T) {
	response := &table.LSTable{}
	transport := &scriptedTaskTransport{execute: func(_ context.Context, task socket.Task) (*table.LSTable, error) {
		if _, ok := task.(*socket.CreateThreadTask); !ok {
			t.Fatalf("expected task 209, got %T", task)
		}
		return response, nil
	}}
	client := &MetaClient{
		taskTransport: transport,
		parsedTables:  make(chan *parsedTable, 1),
	}
	if err := client.recoverRoomlessGroup(context.Background(), fixtureThreadID); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	tasks := transport.snapshotTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	create := tasks[0].(*socket.CreateThreadTask)
	if create.GetLabel() != "209" || create.ThreadFBID != fixtureThreadID || create.SyncGroup != 1 || create.MetadataOnly != 0 {
		t.Fatalf("unexpected task 209 payload: %#v", create)
	}
	select {
	case parsed := <-client.parsedTables:
		if parsed.Table != response || parsed.IsInitial {
			t.Fatal("task 209 response did not enter the normal parser queue")
		}
	default:
		t.Fatal("task 209 response was not queued")
	}
	for _, task := range tasks {
		if _, ok := task.(*socket.FetchMessagesTask); ok {
			t.Fatal("task 228 must not run before room creation")
		}
	}
}

func TestRoomlessGroupTask228FixtureOneToSeven(t *testing.T) {
	ctx := zerolog.Nop().WithContext(context.Background())
	client := &MetaClient{
		UserLogin: &bridgev2.UserLogin{
			UserLogin: &database.UserLogin{ID: networkid.UserLoginID("login")},
			Log:       zerolog.Nop(),
		},
		backfillCollectors: make(map[int64]*BackfillCollector),
	}
	pageOne := fixtureHistoryPage(fixtureThreadID, 6, 4, true)
	pageTwo := fixtureHistoryPage(fixtureThreadID, 3, 1, false)
	pages := []*table.LSTable{pageOne, pageTwo}
	var pageIndex atomic.Int32
	var roomCreated atomic.Bool
	transport := &scriptedTaskTransport{cursor: "fixture-cursor"}
	transport.execute = func(taskCtx context.Context, task socket.Task) (*table.LSTable, error) {
		fetch, ok := task.(*socket.FetchMessagesTask)
		if !ok {
			return nil, errors.New("unexpected non-history task")
		}
		if !roomCreated.Load() {
			return nil, errors.New("task 228 ran before room creation")
		}
		index := int(pageIndex.Add(1)) - 1
		if index >= len(pages) {
			return nil, errors.New("unexpected extra history page")
		}
		page := pages[index]
		upsert, _ := page.WrapMessages()
		client.handleUpsertMessages(handlerParams{
			ctx:    taskCtx,
			ID:     fixtureThreadID,
			Type:   table.GROUP_THREAD,
			Portal: networkid.PortalKey{ID: metaid.MakeFBPortalID(fixtureThreadID), Receiver: client.UserLogin.ID},
		}, upsert[fixtureThreadID])
		if fetch.Cursor != "fixture-cursor" || fetch.SyncGroup != 1 || fetch.Direction != 0 {
			t.Errorf("unexpected task 228 payload: %#v", fetch)
		}
		return page, nil
	}
	client.taskTransport = transport

	done := make(chan struct{})
	collector := &BackfillCollector{
		UpsertMessages: &table.UpsertMessages{Range: &table.LSInsertNewMessageRange{ThreadKey: fixtureThreadID}},
		MaxMessages:    6,
		Done:           func() { close(done) },
	}
	if !client.addBackfillCollector(fixtureThreadID, collector) {
		t.Fatal("failed to add fixture collector")
	}

	if len(transport.snapshotTasks()) != 0 {
		t.Fatal("history task ran before the room gate opened")
	}
	roomCreated.Store(true)
	if !client.requestMoreHistory(ctx, fixtureThreadID, 7000, "m7") {
		t.Fatal("initial task 228 request failed")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for paginated history")
	}

	tasks := transport.snapshotTasks()
	if len(tasks) != 2 {
		t.Fatalf("expected two task 228 pages, got %d", len(tasks))
	}
	first := tasks[0].(*socket.FetchMessagesTask)
	second := tasks[1].(*socket.FetchMessagesTask)
	if first.ReferenceMessageId != "m7" || first.ReferenceTimestampMs != 7000 {
		t.Fatalf("unexpected first anchor: %#v", first)
	}
	if second.ReferenceMessageId != "m4" || second.ReferenceTimestampMs != 4000 {
		t.Fatalf("pagination did not advance from the previous oldest message: %#v", second)
	}

	logical := map[string]int64{"m7": 7000}
	for _, message := range collector.Messages {
		if _, exists := logical[message.MessageId]; exists {
			t.Fatalf("duplicate provider message ID %s", message.MessageId)
		}
		logical[message.MessageId] = message.TimestampMs
	}
	if len(logical) != 7 {
		t.Fatalf("expected 1 -> 7 logical messages, got %d", len(logical))
	}
	ordered := make([]string, 0, len(logical))
	for messageID := range logical {
		ordered = append(ordered, messageID)
	}
	sort.Slice(ordered, func(i, j int) bool { return logical[ordered[i]] < logical[ordered[j]] })
	for index, messageID := range ordered {
		expected := "m" + string(rune('1'+index))
		if messageID != expected {
			t.Fatalf("unexpected chronological order: %v", ordered)
		}
	}

	firstPage, _ := pageOne.WrapMessages()
	messages := firstPage[fixtureThreadID].Messages
	if messages[0].ReplySourceId == "" || len(messages[1].BlobAttachments) != 1 || len(messages[2].Reactions) != 1 {
		t.Fatal("reply, media, or reaction metadata was lost by table normalization")
	}
	if len(pageOne.LSEditMessage) != 1 || pageOne.LSEditMessage[0].MessageID != "m5" {
		t.Fatal("edit metadata was lost from the task 228 fixture")
	}

	insertMissing := func(target map[string]int64) int {
		inserted := 0
		for messageID, timestamp := range logical {
			if _, exists := target[messageID]; exists {
				continue
			}
			target[messageID] = timestamp
			inserted++
		}
		return inserted
	}
	durable := make(map[string]int64)
	if inserted := insertMissing(durable); inserted != 7 {
		t.Fatalf("first import inserted %d messages", inserted)
	}
	if inserted := insertMissing(durable); inserted != 0 {
		t.Fatalf("second import inserted %d duplicate messages", inserted)
	}
	restarted := make(map[string]int64, len(durable))
	for messageID, timestamp := range durable {
		restarted[messageID] = timestamp
	}
	if inserted := insertMissing(restarted); inserted != 0 {
		t.Fatalf("post-restart import inserted %d duplicate messages", inserted)
	}
}

func TestRoomlessRecoveryFailureIsRetryable(t *testing.T) {
	attempts := 0
	transport := &scriptedTaskTransport{execute: func(_ context.Context, _ socket.Task) (*table.LSTable, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("fixture failure")
		}
		return &table.LSTable{}, nil
	}}
	client := &MetaClient{taskTransport: transport, parsedTables: make(chan *parsedTable, 1)}
	if err := client.recoverRoomlessGroup(context.Background(), fixtureThreadID); err == nil {
		t.Fatal("expected first recovery to fail")
	}
	if client.permanentErrored.Load() {
		t.Fatal("metadata failure must not disconnect the live connector")
	}
	if err := client.recoverRoomlessGroup(context.Background(), fixtureThreadID); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected two attempts, got %d", attempts)
	}
}

func fixtureHistoryPage(threadID int64, newest, oldest int, hasMore bool) *table.LSTable {
	messages := make([]*table.LSUpsertMessage, 0, newest-oldest+1)
	for index := newest; index >= oldest; index-- {
		message := &table.LSUpsertMessage{
			Text:             "fixture",
			ThreadKey:        threadID,
			TimestampMs:      int64(index * 1000),
			PrimarySortKey:   int64(index * 1000),
			SecondarySortKey: int64(index),
			MessageId:        "m" + string(rune('0'+index)),
			SenderId:         int64(100 + index%2),
		}
		if index == 6 {
			message.ReplySourceId = "m5"
			message.ReplySourceTimestampMs = 5000
		}
		messages = append(messages, message)
	}
	page := &table.LSTable{
		LSInsertNewMessageRange: []*table.LSInsertNewMessageRange{{
			ThreadKey:      threadID,
			MinMessageId:   "m" + string(rune('0'+oldest)),
			MaxMessageId:   "m" + string(rune('0'+newest)),
			MinTimestampMs: int64(oldest * 1000),
			MaxTimestampMs: int64(newest * 1000),
			HasMoreBefore:  hasMore,
		}},
		LSUpsertMessage: messages,
	}
	if newest == 6 {
		page.LSInsertBlobAttachment = []*table.LSInsertBlobAttachment{{
			ThreadKey:      threadID,
			MessageId:      "m5",
			AttachmentFbid: "fixture-media",
			Filename:       "fixture.jpg",
			HasMedia:       true,
		}}
		page.LSUpsertReaction = []*table.LSUpsertReaction{{
			ThreadKey:   threadID,
			TimestampMs: 6100,
			MessageId:   "m4",
			ActorId:     102,
			Reaction:    "ok",
		}}
		page.LSEditMessage = []*table.LSEditMessage{{MessageID: "m5", Text: "edited", EditCount: 1}}
	}
	return page
}
