package connector

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestExternalE2EEMessageDataText(t *testing.T) {
	timestamp := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	evt := &WAMessageEvent{FBMessage: &events.FBMessage{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     types.NewJID("123", types.MessengerServer),
				Sender:   types.NewJID("456", types.MessengerServer),
				IsFromMe: false,
			},
			ID:        "mid.1",
			PushName:  "Sender",
			Timestamp: timestamp,
		},
		Message: &waConsumerApplication.ConsumerApplication{
			Payload: &waConsumerApplication.ConsumerApplication_Payload{
				Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
					Content: &waConsumerApplication.ConsumerApplication_Content{
						Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{
							MessageText: &waCommon.MessageText{Text: proto.String("hello")},
						},
					},
				},
			},
		},
	}}

	eventType, message := externalE2EEMessageData(evt)
	want := map[string]any{
		"eventType":         "message.upsert",
		"kind":              "text",
		"text":              "hello",
		"providerMessageId": "mid.1",
		"direction":         "inbound",
		"timestamp":         timestamp.Format(time.RFC3339Nano),
	}
	got := map[string]any{
		"eventType":         eventType,
		"kind":              message["kind"],
		"text":              message["text"],
		"providerMessageId": message["providerMessageId"],
		"direction":         message["direction"],
		"timestamp":         message["timestamp"],
	}
	for key, expected := range want {
		if got[key] != expected {
			t.Fatalf("%s: got %v, want %v", key, got[key], expected)
		}
	}
}

func TestExternalE2EEThreadDataPreservesGroupMarker(t *testing.T) {
	evt := &WAMessageEvent{FBMessage: &events.FBMessage{
		Info: types.MessageInfo{MessageSource: types.MessageSource{
			Chat:    types.NewJID("123", types.GroupServer),
			IsGroup: true,
		}},
	}}

	thread := externalE2EEThreadData(evt)
	if thread["threadId"] != evt.Info.Chat.String() {
		t.Fatalf("threadId: got %v, want %v", thread["threadId"], evt.Info.Chat.String())
	}
	if thread["isGroup"] != true {
		t.Fatalf("isGroup: got %v, want true", thread["isGroup"])
	}
}
