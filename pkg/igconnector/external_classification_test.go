package igconnector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func externalTestInstagramUser(fbid int64, igid string) *slidetypes.User {
	return &slidetypes.User{InteropMessagingUserFBID: fbid, ID: igid, PK: igid}
}

func TestExternalInstagramConversationClassification(t *testing.T) {
	const loginID networkid.UserLoginID = "100"
	viewer := externalTestInstagramUser(100, "viewer")
	directPeer := externalTestInstagramUser(200, "peer")
	secondPeer := externalTestInstagramUser(300, "second")

	tests := []struct {
		name              string
		thread            *slidetypes.ThreadInfo
		wantKind          string
		wantRequestStatus string
		wantFolderClass   string
		wantNonSelf       int
		wantSelf          bool
		wantContradiction bool
	}{
		{
			name: "pending direct includes viewer in raw users",
			thread: &slidetypes.ThreadInfo{
				ThreadSubtype: slidetypes.ThreadSubtypeDirectBusiness,
				SystemFolder:  "PENDING",
				Viewer:        viewer,
				Users:         []*slidetypes.User{viewer, directPeer},
			},
			wantKind: externalInstagramConversationDirect, wantRequestStatus: externalInstagramRequestPending, wantFolderClass: "pending",
			wantNonSelf: 1, wantSelf: true,
		},
		{
			name: "accepted direct",
			thread: &slidetypes.ThreadInfo{
				ThreadSubtype: slidetypes.ThreadSubtypeDirect,
				SystemFolder:  "PRIMARY",
				Viewer:        viewer,
				Users:         []*slidetypes.User{directPeer},
			},
			wantKind: externalInstagramConversationDirect, wantRequestStatus: externalInstagramRequestAccepted, wantFolderClass: "primary",
			wantNonSelf: 1,
		},
		{
			name: "genuine group from provider identity",
			thread: &slidetypes.ThreadInfo{
				IsGroup:       true,
				ThreadSubtype: slidetypes.ThreadSubtypeGroup,
				SystemFolder:  "PRIMARY",
				Viewer:        viewer,
				Users:         []*slidetypes.User{directPeer, secondPeer},
			},
			wantKind: externalInstagramConversationGroup, wantRequestStatus: externalInstagramRequestAccepted, wantFolderClass: "primary",
			wantNonSelf: 2,
		},
		{
			name: "contradictory direct subtype with multiple peers",
			thread: &slidetypes.ThreadInfo{
				ThreadSubtype: slidetypes.ThreadSubtypeDirect,
				SystemFolder:  "PENDING",
				Viewer:        viewer,
				Users:         []*slidetypes.User{viewer, directPeer, secondPeer},
			},
			wantKind: externalInstagramConversationGroup, wantRequestStatus: externalInstagramRequestPending, wantFolderClass: "pending",
			wantNonSelf: 2, wantSelf: true, wantContradiction: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifyExternalInstagramThread(test.thread, loginID)
			if got.ConversationKind != test.wantKind || got.RequestStatus != test.wantRequestStatus || got.ProviderFolderClass != test.wantFolderClass ||
				got.DistinctNonSelfParticipantCount != test.wantNonSelf || got.SelfPresent != test.wantSelf ||
				got.Contradictory != test.wantContradiction {
				t.Fatalf("classification = %#v", got)
			}
		})
	}
}

func TestExternalInstagramParticipantAliasesAreDeduplicated(t *testing.T) {
	thread := &slidetypes.ThreadInfo{
		Viewer: externalTestInstagramUser(100, "viewer"),
		Users: []*slidetypes.User{
			{InteropMessagingUserFBID: 200, ID: "peer"},
			{FBIDV2: "200", PK: "peer"},
		},
	}
	participants, selfPresent := externalInstagramDistinctNonSelfUsers(thread, "100")
	if selfPresent || len(participants) != 1 {
		t.Fatalf("participants = %d, selfPresent = %v", len(participants), selfPresent)
	}
}

func TestExternalInstagramProviderGroupWinsContradictoryDirectSubtype(t *testing.T) {
	got := classifyExternalInstagramThread(&slidetypes.ThreadInfo{
		IsGroup:       true,
		ThreadSubtype: slidetypes.ThreadSubtypeDirect,
		SystemFolder:  "PRIMARY",
		Users:         []*slidetypes.User{externalTestInstagramUser(200, "peer")},
	}, "100")
	if got.ConversationKind != externalInstagramConversationGroup || !got.Contradictory {
		t.Fatalf("classification = %#v", got)
	}
}

func TestExternalInstagramFolderFallbackKeepsRequestStateSeparate(t *testing.T) {
	got := classifyExternalInstagramThread(&slidetypes.ThreadInfo{
		ThreadSubtype: slidetypes.ThreadSubtypeDirect,
		Folder:        "PENDING",
		Users:         []*slidetypes.User{externalTestInstagramUser(200, "peer")},
	}, "100")
	if got.ConversationKind != externalInstagramConversationDirect || got.RequestStatus != externalInstagramRequestPending || got.ProviderFolderClass != "pending" {
		t.Fatalf("classification = %#v", got)
	}
}
