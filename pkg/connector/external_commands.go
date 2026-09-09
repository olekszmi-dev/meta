package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const (
	externalContactsDefaultLimit int64 = 100
	externalContactsMaxLimit     int64 = 500
	externalProjectionTimeout          = 90 * time.Second
)

func externalCommandTimeout(commandType string) time.Duration {
	switch commandType {
	case "contacts_sync", "threads_sync":
		return externalProjectionTimeout
	default:
		return 0
	}
}

func executeBoundedExternalCommand(
	ctx context.Context,
	timeout time.Duration,
	execute func(context.Context) map[string]any,
) map[string]any {
	if timeout <= 0 {
		return execute(ctx)
	}
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resultCh := make(chan map[string]any, 1)
	go func() {
		resultCh <- execute(commandCtx)
	}()
	select {
	case result := <-resultCh:
		return result
	case <-commandCtx.Done():
		return map[string]any{"ok": false, "error": "provider_command_timeout"}
	}
}

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

type externalContactSyncContact struct {
	ProviderID  string   `json:"providerId"`
	Name        string   `json:"name,omitempty"`
	Username    string   `json:"username,omitempty"`
	AvatarURL   string   `json:"avatarUrl,omitempty"`
	Messageable bool     `json:"messageable"`
	SourceRows  []string `json:"sourceRows"`
}

type externalContactSyncEvidence struct {
	TaskLabel              string `json:"taskLabel"`
	ProtocolRows           int    `json:"protocolRows"`
	MessageablePersonRows  int    `json:"messageablePersonRows"`
	UniquePersonCount      int    `json:"uniquePersonCount"`
	UniqueMessageableCount int    `json:"uniqueMessageableCount"`
	DuplicateRows          int    `json:"duplicateRows"`
	SkippedRows            int    `json:"skippedRows"`
	ContactSetDigest       string `json:"contactSetDigest"`
}

type externalContactSyncResult struct {
	Contacts []externalContactSyncContact `json:"contacts"`
	Evidence externalContactSyncEvidence  `json:"evidence"`
}

func externalContactSetDigest(contacts []externalContactSyncContact) string {
	ids := make([]string, 0, len(contacts))
	seen := make(map[string]struct{}, len(contacts))
	for _, contact := range contacts {
		id := strings.TrimSpace(contact.ProviderID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	// Newline-delimited sorted IDs provide one stable representation without exposing them.
	sort.Strings(ids)
	digest := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	return hex.EncodeToString(digest[:])
}

func normalizeExternalContacts(response *table.LSTable) externalContactSyncResult {
	result := externalContactSyncResult{
		Contacts: make([]externalContactSyncContact, 0),
		Evidence: externalContactSyncEvidence{TaskLabel: socket.TaskLabels["GetContactsTask"]},
	}
	if response == nil {
		return result
	}

	seen := make(map[int64]int, len(response.LSDeleteThenInsertContact)+len(response.LSVerifyContactRowExists))
	result.Evidence.ProtocolRows = len(response.LSDeleteThenInsertContact) + len(response.LSVerifyContactRowExists)
	appendContact := func(id int64, name, username, avatarURL, sourceRow string, messageable bool) {
		if messageable {
			result.Evidence.MessageablePersonRows++
		}
		if index, ok := seen[id]; ok {
			result.Evidence.DuplicateRows++
			existing := &result.Contacts[index]
			existing.Messageable = existing.Messageable || messageable
			if existing.Name == "" {
				existing.Name = strings.TrimSpace(name)
			}
			if existing.Username == "" {
				existing.Username = strings.TrimSpace(username)
			}
			if existing.AvatarURL == "" {
				existing.AvatarURL = strings.TrimSpace(avatarURL)
			}
			if !externalContainsString(existing.SourceRows, sourceRow) {
				existing.SourceRows = append(existing.SourceRows, sourceRow)
			}
			return
		}
		seen[id] = len(result.Contacts)
		result.Contacts = append(result.Contacts, externalContactSyncContact{
			ProviderID:  strconv.FormatInt(id, 10),
			Name:        strings.TrimSpace(name),
			Username:    strings.TrimSpace(username),
			AvatarURL:   strings.TrimSpace(avatarURL),
			Messageable: messageable,
			SourceRows:  []string{sourceRow},
		})
	}
	for _, contact := range response.LSDeleteThenInsertContact {
		if contact == nil || contact.Id <= 0 || !contact.IsMessengerUser {
			result.Evidence.SkippedRows++
			continue
		}

		username := strings.TrimSpace(contact.Username)
		if username == "" {
			username = strings.TrimSpace(contact.SecondaryName)
		}
		appendContact(contact.Id, contact.Name, username, contact.GetAvatarURL(), "inserted", contact.CanViewerMessage)
	}
	for _, contact := range response.LSVerifyContactRowExists {
		if contact == nil || contact.ContactId <= 0 || contact.IsSelf {
			result.Evidence.SkippedRows++
			continue
		}
		appendContact(contact.ContactId, contact.Name, contact.SecondaryName, contact.GetAvatarURL(), "verified", contact.CanViewerMessage)
	}
	sort.Slice(result.Contacts, func(i, j int) bool {
		left, _ := strconv.ParseInt(result.Contacts[i].ProviderID, 10, 64)
		right, _ := strconv.ParseInt(result.Contacts[j].ProviderID, 10, 64)
		return left < right
	})
	result.Evidence.UniquePersonCount = len(result.Contacts)
	for _, contact := range result.Contacts {
		if contact.Messageable {
			result.Evidence.UniqueMessageableCount++
		}
	}
	result.Evidence.ContactSetDigest = externalContactSetDigest(result.Contacts)
	return result
}

func externalContainsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func externalContactsSyncResponse(response *table.LSTable) map[string]any {
	if response == nil {
		return map[string]any{"ok": false, "error": "provider_contacts_sync_empty_response"}
	}
	return externalContactsSyncResultResponse(normalizeExternalContacts(response))
}

func externalContactsSyncResultResponse(result externalContactSyncResult) map[string]any {
	return map[string]any{
		"ok":       true,
		"status":   "completed",
		"task":     452,
		"evidence": result.Evidence,
	}
}

func externalContactsSyncLimit(payload map[string]any) (int64, error) {
	if payload == nil {
		return externalContactsDefaultLimit, nil
	}
	raw, ok := payload["limit"]
	if !ok || strings.TrimSpace(fmt.Sprint(raw)) == "" {
		return externalContactsDefaultLimit, nil
	}
	limit, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(raw)), 10, 64)
	if err != nil || limit < 1 || limit > externalContactsMaxLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", externalContactsMaxLimit)
	}
	return limit, nil
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
	case "contacts_sync":
		limit, err := externalContactsSyncLimit(command.Payload)
		if err != nil {
			return map[string]any{"ok": false, "error": "contacts_sync_limit_invalid"}
		}
		response, err := client.Client.ExecuteTasks(ctx, &socket.GetContactsTask{Limit: limit})
		if err != nil {
			return map[string]any{"ok": false, "error": "provider_contacts_sync_failed"}
		}
		if response == nil {
			return map[string]any{"ok": false, "error": "provider_contacts_sync_empty_response"}
		}
		result := normalizeExternalContacts(response)
		if err = client.emitExternalPeople(ctx, result.Contacts); err != nil {
			return map[string]any{"ok": false, "error": "person_projection_failed"}
		}
		return externalContactsSyncResultResponse(result)
	case "threads_sync":
		evidence, err := client.emitExternalStoredThreads(ctx)
		if err != nil {
			return map[string]any{"ok": false, "error": "thread_projection_failed"}
		}
		return map[string]any{
			"ok":       true,
			"status":   "completed",
			"evidence": evidence,
		}
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

func (m *MetaConnector) executeExternalCommandBounded(ctx context.Context, command *externalCommand) map[string]any {
	// Only read-only projections receive a timeout; side-effecting commands must remain synchronous.
	return executeBoundedExternalCommand(ctx, externalCommandTimeout(command.CommandType), func(commandCtx context.Context) map[string]any {
		return m.executeExternalCommand(commandCtx, command)
	})
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
			result := m.executeExternalCommandBounded(ctx, command)
			if err = m.ExternalControl.CompleteCommand(ctx, command.CommandID, result); err != nil {
				m.Bridge.Log.Error().Err(err).Msg("Failed to persist external command result")
			}
		}
	}
}
