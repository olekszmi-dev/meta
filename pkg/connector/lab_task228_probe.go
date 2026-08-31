//go:build messengerlab

package connector

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	zerologlog "github.com/rs/zerolog/log"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

const labTask228ProbeSelector = "largest-pending-group"

var labTask228ProbeStarted atomic.Bool

func (m *MetaClient) maybeRunLabTask228Probe(ctx context.Context) {
	enabled := os.Getenv("MAUTRIX_META_LAB_TASK228_PROBE") == labTask228ProbeSelector
	zerologlog.Logger.Info().
		Str("trace_event", "task_228_probe_armed").
		Bool("enabled", enabled).
		Msg("History probe dispatch checked")
	if !enabled {
		return
	}
	if !labTask228ProbeStarted.CompareAndSwap(false, true) {
		return
	}
	go m.runLabTask228Probe(ctx)
}

func (m *MetaClient) runLabTask228Probe(ctx context.Context) {
	log := zerologlog.Logger
	portals, err := m.Main.Bridge.DB.Portal.GetAllWithMXID(ctx)
	if err != nil {
		log.Error().Str("trace_event", "task_228_probe_error").Str("stage", "load_portals").Msg("History probe failed")
		return
	}

	var target *database.Portal
	var targetExistingMessages []*database.Message
	targetMessageCount := -1
	targetCountAtMaximum := 0
	for _, portal := range portals {
		metadata, _, ok := roomlessGroupRecoveryTarget(&database.Portal{
			PortalKey:      portal.PortalKey,
			MessageRequest: portal.MessageRequest,
			Metadata:       portal.Metadata,
		})
		if !ok || metadata == nil {
			continue
		}
		messages, loadErr := m.Main.Bridge.DB.Message.GetLastNInPortal(ctx, portal.PortalKey, 1000)
		if loadErr != nil {
			continue
		}
		task, taskErr := m.Main.Bridge.DB.BackfillTask.GetNextForPortal(ctx, portal.PortalKey, true)
		if taskErr != nil || task == nil || task.QueueDone {
			continue
		}
		if len(messages) > targetMessageCount {
			target = portal
			targetExistingMessages = messages
			targetMessageCount = len(messages)
			targetCountAtMaximum = 1
		} else if len(messages) == targetMessageCount {
			targetCountAtMaximum++
		}
	}
	if target == nil || targetCountAtMaximum != 1 {
		log.Error().Str("trace_event", "task_228_probe_error").Str("stage", "select_target").Int("maximum_match_count", targetCountAtMaximum).Msg("History probe target was not unique")
		return
	}

	threadID := metaid.ParseFBPortalID(target.ID)
	anchor, err := m.Main.Bridge.DB.Message.GetFirstPortalMessage(ctx, target.PortalKey)
	if err != nil || anchor == nil {
		log.Error().Str("trace_event", "task_228_probe_error").Str("stage", "load_anchor").Msg("History probe failed")
		return
	}
	parsedAnchor, ok := metaid.ParseMessageID(anchor.ID).(metaid.ParsedFBMessageID)
	if !ok {
		log.Error().Str("trace_event", "task_228_probe_error").Str("stage", "parse_anchor").Msg("History probe failed")
		return
	}

	done := make(chan struct{})
	collector := &BackfillCollector{
		UpsertMessages: &table.UpsertMessages{Range: &table.LSInsertNewMessageRange{
			ThreadKey:              threadID,
			MinTimestampMsTemplate: anchor.Timestamp.UnixMilli(),
			MaxTimestampMsTemplate: anchor.Timestamp.UnixMilli(),
			MinMessageId:           parsedAnchor.ID,
			MaxMessageId:           parsedAnchor.ID,
			MinTimestampMs:         anchor.Timestamp.UnixMilli(),
			MaxTimestampMs:         anchor.Timestamp.UnixMilli(),
			HasMoreBefore:          true,
			HasMoreAfter:           true,
		}},
		MaxMessages: -1,
		Anchor:      anchor,
		Done:        sync.OnceFunc(func() { close(done) }),
	}
	if !m.addBackfillCollector(threadID, collector) {
		log.Error().Str("trace_event", "task_228_probe_error").Str("stage", "register_collector").Msg("History probe failed")
		return
	}
	defer m.removeBackfillCollector(threadID, collector)

	targetHash := providerTraceHash("thread", strconv.FormatInt(threadID, 10))
	log.Info().Str("trace_event", "task_228_probe_start").Str("thread_hash", targetHash).Msg("History probe started")
	if !m.requestMoreHistory(ctx, threadID, anchor.Timestamp.UnixMilli(), parsedAnchor.ID) {
		log.Error().Str("trace_event", "task_228_probe_error").Str("stage", "request_history").Str("thread_hash", targetHash).Msg("History probe failed")
		return
	}

	select {
	case <-done:
	case <-time.After(3 * time.Minute):
		log.Error().Str("trace_event", "task_228_probe_error").Str("stage", "timeout").Str("thread_hash", targetHash).Msg("History probe failed")
		return
	case <-ctx.Done():
		return
	}

	m.backfillLock.Lock()
	messages := slices.Clone(collector.Messages)
	pageCount := collector.PageCount
	hasMoreBefore := collector.Range.HasMoreBefore
	m.backfillLock.Unlock()

	sourceOrderChronological := true
	for index, message := range messages {
		if index > 0 && messages[index-1].TimestampMs > message.TimestampMs {
			sourceOrderChronological = false
		}
	}
	slices.SortFunc(messages, func(a, b *table.WrappedMessage) int {
		if timestampOrder := cmp.Compare(a.TimestampMs, b.TimestampMs); timestampOrder != 0 {
			return timestampOrder
		}
		return cmp.Compare(a.MessageId, b.MessageId)
	})
	ids := make([]string, 0, len(messages))
	unique := make(map[string]struct{}, len(messages))
	existingIDs := make(map[string]struct{}, len(targetExistingMessages))
	for _, message := range targetExistingMessages {
		existingIDs[string(message.ID)] = struct{}{}
	}
	overlapCount := 0
	for _, message := range messages {
		ids = append(ids, message.MessageId)
		unique[message.MessageId] = struct{}{}
		if _, exists := existingIDs[string(metaid.MakeFBMessageID(message.MessageId))]; exists {
			overlapCount++
		}
	}
	slices.Sort(ids)
	datasetHasher := sha256.New()
	for _, id := range ids {
		_, _ = datasetHasher.Write([]byte(id))
		_, _ = datasetHasher.Write([]byte{0})
	}
	datasetHash := hex.EncodeToString(datasetHasher.Sum(nil)[:12])

	log.Info().
		Str("trace_event", "task_228_probe_complete").
		Str("thread_hash", targetHash).
		Int("page_count", pageCount).
		Int("message_count", len(messages)).
		Int("unique_message_count", len(unique)).
		Int("existing_message_count", len(targetExistingMessages)).
		Int("overlap_count", overlapCount).
		Int("logical_message_count", len(targetExistingMessages)+len(unique)-overlapCount).
		Bool("source_order_chronological", sourceOrderChronological).
		Bool("normalized_order_chronological", true).
		Bool("has_more_before", hasMoreBefore).
		Str("dataset_hash", datasetHash).
		Msg("History probe completed")
}
