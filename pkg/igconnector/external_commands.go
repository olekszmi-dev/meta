package igconnector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
)

func stringPayload(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func (ic *IGClient) executeExternalCommand(ctx context.Context, command *externalCommand) map[string]any {
	if command == nil {
		return map[string]any{"ok": false, "error": "command_missing"}
	}
	switch command.CommandType {
	case "send":
		if command.ApprovalRef == "" || command.ApprovedAt == "" {
			return map[string]any{"ok": false, "error": "approved_send_required"}
		}
		threadID := stringPayload(command.Payload, "conversationId")
		text := stringPayload(command.Payload, "text")
		if threadID == "" || text == "" {
			return map[string]any{"ok": false, "error": "send_payload_invalid"}
		}
		request := &slidetypes.SendTextRequest{
			IGThreadIGID:       &threadID,
			OfflineThreadingID: strconv.FormatInt(methods.GenerateEpochID(), 10),
			Text:               slidetypes.SensitiveString{Value: text},
		}
		if replyID := stringPayload(command.Payload, "replyToProviderMessageId"); replyID != "" {
			request.ReplyToMessageID = &replyID
		}
		response, err := ic.Client.SendMessage(ctx, request)
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		message := response.GetMessage()
		if message.ID == "" {
			return map[string]any{"ok": false, "error": "provider_confirmation_missing"}
		}
		return map[string]any{"ok": true, "providerConfirmed": true, "externalRef": message.ID}
	case "threads_sync":
		response, err := ic.Client.GetMailbox(ctx)
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		count := 0
		if response != nil && response.Mailbox != nil {
			for _, edge := range response.Mailbox.ThreadsByFolder.Edges {
				if thread := edge.Node.AsIGDirectThread; thread != nil {
					if err = ic.emitExternalThread(ctx, thread, "instagram_inbox_sync"); err != nil {
						return map[string]any{"ok": false, "error": err.Error()}
					}
					count++
				}
			}
		}
		return map[string]any{"ok": true, "status": "completed", "evidence": map[string]any{"emittedThreads": count}}
	case "history_backfill":
		threadID := stringPayload(command.Payload, "conversationId")
		if threadID == "" {
			return map[string]any{"ok": false, "error": "history_conversation_required"}
		}
		response, err := ic.Client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(threadID))
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		thread := response.ThreadInfo.AsIGDirectThread
		if thread == nil {
			return map[string]any{"ok": false, "error": "history_thread_missing"}
		}
		kind := "direct"
		if thread.IsGroup || len(thread.Users) > 1 {
			kind = "group"
		}
		messages := make([]map[string]any, 0)
		if thread.SlideMessages != nil {
			for _, edge := range thread.SlideMessages.Edges {
				if edge.Node != nil {
					messages = append(messages, ic.externalMessagePayload(threadID, edge.Node))
				}
			}
		}
		err = ic.Main.ExternalControl.EmitEvent(ctx, map[string]any{
			"eventId":   "instagram_history:" + threadID + ":" + strconv.FormatInt(time.Now().UnixMilli(), 10),
			"eventType": "history.page", "source": "instagram_history",
			"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "conversationKind": kind,
			"payload": map[string]any{"threadId": threadID, "hasMoreBefore": false, "messages": messages},
		})
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true, "status": "completed", "evidence": map[string]any{"emittedMessages": len(messages)}}
	case "reconnect":
		go ic.FullReconnect(false)
		return map[string]any{"ok": true, "status": "requested"}
	default:
		return map[string]any{"ok": false, "error": "command_unsupported"}
	}
}

func (ic *IGClient) pollExternalCommands(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			command, err := ic.Main.ExternalControl.PollCommand(ctx)
			if err != nil {
				ic.UserLogin.Log.Warn().Err(err).Msg("Failed to poll external Instagram command")
				continue
			}
			if command == nil {
				continue
			}
			result := ic.executeExternalCommand(ctx, command)
			if err = ic.Main.ExternalControl.CompleteCommand(ctx, command.CommandID, result); err != nil {
				ic.UserLogin.Log.Warn().Err(err).Msg("Failed to complete external Instagram command")
			}
		}
	}
}
