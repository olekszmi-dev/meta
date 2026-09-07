package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func (m *MetaConnector) selectedExternalClient() *MetaClient {
	if m.ExternalControl == nil {
		return nil
	}
	for _, login := range m.Bridge.GetAllCachedUserLogins() {
		if m.ExternalControl.OwnsLogin(string(login.ID)) {
			client, _ := login.Client.(*MetaClient)
			return client
		}
	}
	return nil
}

func stringPayload(payload map[string]any, key string) string {
	return strings.TrimSpace(fmt.Sprint(payload[key]))
}

func int64Payload(payload map[string]any, key string) int64 {
	value, _ := strconv.ParseInt(stringPayload(payload, key), 10, 64)
	return value
}

func externalSendResult(response *table.LSTable, otid int64) map[string]any {
	if response == nil {
		return map[string]any{"ok": false, "error": "provider_receipt_missing_nil_response"}
	}
	if len(response.LSIssueNewError) > 0 {
		return map[string]any{"ok": false, "error": "provider_rejected_send"}
	}
	if len(response.LSMarkOptimisticMessageFailed) > 0 {
		return map[string]any{"ok": false, "error": "provider_rejected_optimistic_send"}
	}
	if len(response.LSHandleFailedTask) > 0 {
		return map[string]any{"ok": false, "error": "provider_failed_send_task"}
	}
	otidString := strconv.FormatInt(otid, 10)
	for _, replacement := range response.LSReplaceOptimsiticMessage {
		if replacement.OfflineThreadingId == otidString && replacement.MessageId != "" {
			return map[string]any{"ok": true, "providerConfirmed": true, "externalRef": replacement.MessageId}
		}
	}
	return map[string]any{"ok": false, "error": "provider_receipt_missing_replacement"}
}

func (m *MetaConnector) executeExternalCommand(ctx context.Context, command *externalCommand) map[string]any {
	client := m.selectedExternalClient()
	if client == nil {
		return map[string]any{"ok": false, "error": "selected_login_not_loaded"}
	}
	switch command.CommandType {
	case "reconnect":
		client.FullReconnect()
		return map[string]any{"ok": true, "status": "reconnecting"}
	case "history", "history_backfill":
		threadID := int64Payload(command.Payload, "threadId")
		referenceTimestamp := int64Payload(command.Payload, "referenceTimestampMs")
		referenceMessageID := stringPayload(command.Payload, "referenceMessageId")
		if threadID == 0 || referenceTimestamp == 0 || referenceMessageID == "" {
			return map[string]any{"ok": false, "error": "history_anchor_required"}
		}
		if !client.requestMoreHistory(ctx, threadID, referenceTimestamp, referenceMessageID) {
			return map[string]any{"ok": false, "error": "history_request_failed"}
		}
		return map[string]any{"ok": true, "status": "queued", "task": 228}
	case "send":
		threadID := int64Payload(command.Payload, "threadId")
		body := stringPayload(command.Payload, "body")
		if threadID == 0 || body == "" {
			return map[string]any{"ok": false, "error": "send_target_and_body_required"}
		}
		if err := client.Client.WaitUntilCanSendMessages(ctx, 15*time.Second); err != nil {
			return map[string]any{"ok": false, "error": "provider_not_ready"}
		}
		otid := methods.GenerateEpochID()
		task := &socket.SendMessageTask{
			ThreadId:         threadID,
			Otid:             otid,
			Source:           table.MESSENGER_INBOX_IN_THREAD,
			InitiatingSource: table.FACEBOOK_INBOX,
			SendType:         table.TEXT,
			SyncGroup:        1,
			Text:             body,
		}
		if replyID := stringPayload(command.Payload, "replyToProviderMessageId"); replyID != "" {
			task.ReplyMetaData = &socket.ReplyMetaData{ReplyMessageId: replyID, ReplySourceType: 1}
		}
		response, err := client.Client.ExecuteTasks(ctx, task)
		if err != nil {
			return map[string]any{"ok": false, "error": "provider_send_failed"}
		}
		return externalSendResult(response, otid)
	case "pin_restore":
		return map[string]any{"ok": false, "error": "pin_restore_not_available_in_mautrix_worker"}
	default:
		return map[string]any{"ok": false, "error": "unsupported_command"}
	}
}

func (m *MetaConnector) runExternalCommandLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			command, err := m.ExternalControl.PollCommand(ctx)
			if err != nil {
				m.Bridge.Log.Warn().Err(err).Msg("Failed to poll external command")
				continue
			}
			if command == nil {
				continue
			}
			result := m.executeExternalCommand(ctx, command)
			if err = m.ExternalControl.CompleteCommand(ctx, command.CommandID, result); err != nil {
				m.Bridge.Log.Error().Err(err).Msg("Failed to persist external command result")
			}
		}
	}
}
