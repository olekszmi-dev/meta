package igconnector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/externalhistory"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func stringPayload(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func externalReactionRequest(command *externalCommand) (*slidetypes.CreateReactionRequest, error) {
	if command == nil || command.ApprovalRef == "" || command.ApprovedAt == "" {
		return nil, fmt.Errorf("approved_reaction_required")
	}
	threadID := stringPayload(command.Payload, "conversationId")
	messageID := stringPayload(command.Payload, "providerMessageId")
	reaction := stringPayload(command.Payload, "reaction")
	if threadID == "" || messageID == "" || reaction == "" {
		return nil, fmt.Errorf("reaction_payload_invalid")
	}
	return &slidetypes.CreateReactionRequest{Input: slidetypes.ReactionInput{
		Emoji:          reaction,
		ItemID:         "",
		MessageID:      messageID,
		ReactionStatus: slidetypes.ReactionStatusCreated,
		ThreadID:       threadID,
	}}, nil
}

func externalReactionConfirmed(response *slidetypes.SendReactionResponse, messageID string) bool {
	return response != nil && messageID != "" && response.Message.ID == messageID
}

func (ic *IGClient) selectedExternalLogin() (*bridgev2.UserLogin, *IGClient, bool) {
	if ic == nil || ic.Main == nil || ic.Main.Bridge == nil || ic.Main.ExternalControl == nil {
		return nil, nil, false
	}
	var selected *bridgev2.UserLogin
	for _, login := range ic.Main.Bridge.GetAllCachedUserLogins() {
		if !ic.Main.ExternalControl.OwnsLogin(string(login.ID)) {
			continue
		}
		if selected != nil {
			return nil, nil, false
		}
		selected = login
	}
	if selected == nil {
		return nil, nil, false
	}
	client, ok := selected.Client.(*IGClient)
	if !ok || client == nil {
		return nil, nil, false
	}
	return selected, client, true
}

func instagramProviderMessageID(messageID networkid.MessageID) (string, bool) {
	parsed, ok := metaid.ParseMessageID(messageID).(metaid.ParsedFBMessageID)
	return parsed.ID, ok && parsed.ID != ""
}

func instagramPortalConversationIDs(portal *bridgev2.Portal) []string {
	if portal == nil {
		return nil
	}
	ids := []string{string(portal.ID)}
	if metadata, ok := portal.Metadata.(*metaid.PortalMetadata); ok {
		if metadata.IGID != "" && metadata.IGID != ids[0] {
			ids = append(ids, metadata.IGID)
		}
		if metadata.IGThreadID != "" && metadata.IGThreadID != ids[0] && metadata.IGThreadID != metadata.IGID {
			ids = append(ids, metadata.IGThreadID)
		}
	}
	return ids
}

func decorateInstagramHistoryEvent(portal *bridgev2.Portal, event map[string]any) {
	classification := classifyExternalInstagramPortal(portal)
	event["conversationKind"] = classification.ConversationKind
	if payload, ok := event["payload"].(map[string]any); ok {
		payload["conversationKind"] = classification.ConversationKind
		payload["requestStatus"] = classification.RequestStatus
		payload["classificationEvidence"] = classification
	}
}

func (ic *IGClient) executeHistorySnapshot(ctx context.Context, command *externalCommand) map[string]any {
	conversationID, pageSize, err := externalhistory.ParseRequest(command.Payload)
	if err != nil {
		return map[string]any{"ok": false, "error": "history_snapshot_payload_invalid"}
	}
	login, _, ok := ic.selectedExternalLogin()
	if !ok {
		return map[string]any{"ok": false, "error": "history_snapshot_login_ambiguous_or_unavailable"}
	}
	result, err := externalhistory.Export(
		ctx, ic.Main.Bridge, login, conversationID, pageSize, "instagram_native_portal",
		instagramPortalConversationIDs, instagramProviderMessageID, decorateInstagramHistoryEvent, ic.Main.ExternalControl.EmitEvent,
	)
	if err != nil {
		return map[string]any{"ok": false, "error": externalhistory.ErrorCode(err)}
	}
	return map[string]any{"ok": true, "status": "completed", "evidence": result}
}

func (ic *IGClient) executeExternalCommand(ctx context.Context, command *externalCommand) map[string]any {
	if command == nil {
		return map[string]any{"ok": false, "error": "command_missing"}
	}
	switch command.CommandType {
	case "history_snapshot":
		return ic.executeHistorySnapshot(ctx, command)
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
	case "send_reaction":
		request, err := externalReactionRequest(command)
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		response, err := ic.Client.SendReaction(ctx, request)
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		if !externalReactionConfirmed(response, request.Input.MessageID) {
			return map[string]any{"ok": false, "error": "provider_confirmation_missing"}
		}
		return map[string]any{"ok": true, "providerConfirmed": true, "externalRef": response.Message.ID}
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
		classification := classifyExternalInstagramThread(thread, ic.UserLogin.ID)
		kind := classification.ConversationKind
		if err = ic.emitExternalThread(ctx, thread, "instagram_history"); err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
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
			"payload": map[string]any{
				"threadId": threadID, "hasMoreBefore": false, "messages": messages,
				"requestStatus": classification.RequestStatus,
			},
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
