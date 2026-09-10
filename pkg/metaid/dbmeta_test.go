package metaid

import (
	"encoding/json"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

func TestExternallyManagedLoginMetadataOmitsCredentials(t *testing.T) {
	jar := &cookies.Cookies{Platform: types.Instagram}
	jar.UpdateValues(map[cookies.MetaCookieName]string{cookies.MetaCookieName("sessionid"): "secret-session"})
	encoded, err := json.Marshal(UserLoginMetadata{
		Platform: types.Instagram, Cookies: jar, CredentialRef: "instagram_opaque", CredentialGeneration: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret-session") || strings.Contains(string(encoded), `"cookies"`) {
		t.Fatalf("externally managed metadata leaked credentials: %s", encoded)
	}
}
