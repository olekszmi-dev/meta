package igconnector

import (
	"testing"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func TestExternalReactionRequestRequiresApprovalAndCompleteTarget(t *testing.T) {
	valid := &externalCommand{
		CommandType: "send_reaction",
		ApprovalRef: "approval-1",
		ApprovedAt:  "2026-09-13T20:00:00Z",
		Payload: map[string]any{
			"conversationId":    "thread-1",
			"providerMessageId": "message-1",
			"reaction":          "thumbs-up",
		},
	}
	request, err := externalReactionRequest(valid)
	if err != nil {
		t.Fatalf("valid reaction rejected: %v", err)
	}
	if request.Input.ThreadID != "thread-1" || request.Input.MessageID != "message-1" || request.Input.Emoji != "thumbs-up" {
		t.Fatalf("unexpected request: %+v", request.Input)
	}
	if request.Input.ReactionStatus != slidetypes.ReactionStatusCreated {
		t.Fatalf("unexpected reaction status: %s", request.Input.ReactionStatus)
	}

	withoutApproval := *valid
	withoutApproval.ApprovalRef = ""
	if _, err = externalReactionRequest(&withoutApproval); err == nil || err.Error() != "approved_reaction_required" {
		t.Fatalf("missing approval was not rejected: %v", err)
	}
	withoutTarget := *valid
	withoutTarget.Payload = map[string]any{"conversationId": "thread-1", "reaction": "thumbs-up"}
	if _, err = externalReactionRequest(&withoutTarget); err == nil || err.Error() != "reaction_payload_invalid" {
		t.Fatalf("missing target was not rejected: %v", err)
	}
}

func TestExternalReactionConfirmationRequiresExactProviderTarget(t *testing.T) {
	response := &slidetypes.SendReactionResponse{Message: slidetypes.ReactionUpdateMessage{ID: "message-1"}}
	if !externalReactionConfirmed(response, "message-1") {
		t.Fatal("exact provider target was not confirmed")
	}
	if externalReactionConfirmed(response, "message-2") {
		t.Fatal("a different provider target was accepted as confirmation")
	}
	if externalReactionConfirmed(&slidetypes.SendReactionResponse{}, "message-1") {
		t.Fatal("a response without a provider message reference was accepted")
	}
	if externalReactionConfirmed(nil, "message-1") {
		t.Fatal("a nil provider response was accepted")
	}
}
