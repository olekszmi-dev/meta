package igconnector

import (
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

const (
	externalInstagramConversationDirect = "direct"
	externalInstagramConversationGroup  = "group"

	externalInstagramRequestPending  = "pending"
	externalInstagramRequestAccepted = "accepted"
	externalInstagramRequestSpam     = "spam"
	externalInstagramRequestUnknown  = "unknown"
)

type externalInstagramClassification struct {
	ConversationKind                string `json:"conversationKind"`
	RequestStatus                   string `json:"requestStatus"`
	ProviderIsGroup                 bool   `json:"providerIsGroup"`
	ProviderSubtypeClass            string `json:"providerSubtypeClass"`
	ProviderFolderClass             string `json:"providerFolderClass"`
	PortalThreadTypeClass           string `json:"portalThreadTypeClass,omitempty"`
	RoomTypeClass                   string `json:"roomTypeClass,omitempty"`
	RawParticipantCount             int    `json:"rawParticipantCount"`
	DistinctNonSelfParticipantCount int    `json:"distinctNonSelfParticipantCount"`
	SelfPresent                     bool   `json:"selfPresent"`
	Contradictory                   bool   `json:"contradictory"`
}

func externalInstagramRequestStatus(folder string) string {
	switch strings.ToUpper(strings.TrimSpace(folder)) {
	case "PENDING":
		return externalInstagramRequestPending
	case "SPAM":
		return externalInstagramRequestSpam
	case "PRIMARY", "GENERAL", "INBOX", "ARCHIVED":
		return externalInstagramRequestAccepted
	case "":
		return externalInstagramRequestUnknown
	default:
		return externalInstagramRequestAccepted
	}
}

func externalInstagramFolderClass(folder string) string {
	switch strings.ToUpper(strings.TrimSpace(folder)) {
	case "PENDING":
		return "pending"
	case "SPAM":
		return "spam"
	case "PRIMARY":
		return "primary"
	case "GENERAL":
		return "general"
	case "INBOX":
		return "inbox"
	case "ARCHIVED":
		return "archived"
	case "":
		return externalInstagramRequestUnknown
	default:
		return "other"
	}
}

func externalInstagramClassificationForFolder(
	classification externalInstagramClassification,
	folder string,
) externalInstagramClassification {
	classification.ProviderFolderClass = externalInstagramFolderClass(folder)
	classification.RequestStatus = externalInstagramRequestStatus(folder)
	return classification
}

func externalInstagramSubtypeClass(subtype slidetypes.ThreadSubtype) string {
	switch subtype {
	case slidetypes.ThreadSubtypeGroup:
		return externalInstagramConversationGroup
	case slidetypes.ThreadSubtypeDirect, slidetypes.ThreadSubtypeDirectBusiness, slidetypes.ThreadSubtypeAIBot:
		return externalInstagramConversationDirect
	default:
		return externalInstagramRequestUnknown
	}
}

func externalInstagramUserAliases(user *slidetypes.User) map[string]struct{} {
	aliases := make(map[string]struct{}, 4)
	if user == nil {
		return aliases
	}
	if user.InteropMessagingUserFBID != 0 {
		aliases["fb:"+strconv.FormatInt(user.InteropMessagingUserFBID, 10)] = struct{}{}
	}
	if value := strings.TrimSpace(user.FBIDV2); value != "" {
		aliases["fb:"+value] = struct{}{}
	}
	if value := strings.TrimSpace(user.ID); value != "" {
		aliases["ig:"+value] = struct{}{}
	}
	if value := strings.TrimSpace(user.PK); value != "" {
		aliases["ig:"+value] = struct{}{}
	}
	return aliases
}

func externalInstagramAliasesOverlap(left, right map[string]struct{}) bool {
	for alias := range left {
		if _, ok := right[alias]; ok {
			return true
		}
	}
	return false
}

func externalInstagramMergeAliases(target, source map[string]struct{}) {
	for alias := range source {
		target[alias] = struct{}{}
	}
}

func externalInstagramDistinctNonSelfUsers(
	thread *slidetypes.ThreadInfo,
	loginID networkid.UserLoginID,
) (users []*slidetypes.User, selfPresent bool) {
	if thread == nil {
		return nil, false
	}
	selfAliases := externalInstagramUserAliases(thread.Viewer)
	if loginID != "" {
		selfAliases["fb:"+string(loginID)] = struct{}{}
	}
	participantAliases := make([]map[string]struct{}, 0, len(thread.Users))
	users = make([]*slidetypes.User, 0, len(thread.Users))
	for _, user := range thread.Users {
		aliases := externalInstagramUserAliases(user)
		if len(aliases) == 0 {
			continue
		}
		if externalInstagramAliasesOverlap(aliases, selfAliases) {
			selfPresent = true
			externalInstagramMergeAliases(selfAliases, aliases)
			continue
		}
		merged := false
		for _, existing := range participantAliases {
			if externalInstagramAliasesOverlap(existing, aliases) {
				externalInstagramMergeAliases(existing, aliases)
				merged = true
				break
			}
		}
		if !merged {
			participantAliases = append(participantAliases, aliases)
			users = append(users, user)
		}
	}
	return users, selfPresent
}

func classifyExternalInstagramThread(thread *slidetypes.ThreadInfo, loginID networkid.UserLoginID) externalInstagramClassification {
	classification := externalInstagramClassification{
		ConversationKind:     externalInstagramConversationDirect,
		RequestStatus:        externalInstagramRequestUnknown,
		ProviderSubtypeClass: externalInstagramRequestUnknown,
	}
	if thread == nil {
		return classification
	}
	classification.ProviderIsGroup = thread.IsGroup
	classification.ProviderSubtypeClass = externalInstagramSubtypeClass(thread.ThreadSubtype)
	classification.RawParticipantCount = len(thread.Users)
	folder := thread.SystemFolder
	if strings.TrimSpace(folder) == "" {
		folder = thread.Folder
	}
	classification.ProviderFolderClass = externalInstagramFolderClass(folder)
	classification.RequestStatus = externalInstagramRequestStatus(folder)

	participants, selfPresent := externalInstagramDistinctNonSelfUsers(thread, loginID)
	classification.SelfPresent = selfPresent
	classification.DistinctNonSelfParticipantCount = len(participants)
	providerGroupIdentity := thread.IsGroup || classification.ProviderSubtypeClass == externalInstagramConversationGroup
	if providerGroupIdentity || classification.DistinctNonSelfParticipantCount > 1 {
		classification.ConversationKind = externalInstagramConversationGroup
	}
	classification.Contradictory =
		(thread.IsGroup && classification.ProviderSubtypeClass == externalInstagramConversationDirect) ||
			(!thread.IsGroup && classification.ProviderSubtypeClass == externalInstagramConversationGroup) ||
			(classification.ProviderSubtypeClass == externalInstagramConversationDirect &&
				classification.DistinctNonSelfParticipantCount > 1)
	return classification
}

func classifyExternalInstagramPortal(portal *bridgev2.Portal) externalInstagramClassification {
	classification := externalInstagramClassification{
		ConversationKind:     externalInstagramConversationDirect,
		RequestStatus:        externalInstagramRequestUnknown,
		ProviderSubtypeClass: externalInstagramRequestUnknown,
	}
	if portal == nil {
		return classification
	}
	if portal.MessageRequest {
		classification.RequestStatus = externalInstagramRequestPending
	} else {
		classification.RequestStatus = externalInstagramRequestAccepted
	}
	classification.RoomTypeClass = "group"
	if portal.RoomType == database.RoomTypeDM {
		classification.RoomTypeClass = "direct"
	}
	if metadata, ok := portal.Metadata.(*metaid.PortalMetadata); ok {
		switch metadata.ThreadType {
		case table.GROUP_THREAD:
			classification.ProviderIsGroup = true
			classification.ProviderSubtypeClass = externalInstagramConversationGroup
			classification.PortalThreadTypeClass = externalInstagramConversationGroup
			classification.ConversationKind = externalInstagramConversationGroup
			classification.Contradictory = classification.RoomTypeClass == externalInstagramConversationDirect
			return classification
		case table.ONE_TO_ONE:
			classification.ProviderSubtypeClass = externalInstagramConversationDirect
			classification.PortalThreadTypeClass = externalInstagramConversationDirect
			classification.Contradictory = classification.RoomTypeClass == externalInstagramConversationGroup
			return classification
		}
	}
	if portal.RoomType != database.RoomTypeDM {
		classification.ConversationKind = externalInstagramConversationGroup
		classification.Contradictory = true
	}
	return classification
}
