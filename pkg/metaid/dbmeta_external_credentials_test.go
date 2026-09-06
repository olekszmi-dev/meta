package metaid

import (
	"encoding/json"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

func TestUserLoginMetadataExternalReferenceOmitsSecrets(t *testing.T) {
	jar := &cookies.Cookies{Platform: types.Messenger}
	jar.UpdateValues(map[cookies.MetaCookieName]string{
		cookies.FBCookieXS:     "secret-xs",
		cookies.FBCookieCUser:  "123",
		cookies.MetaCookieDatr: "secret-datr",
	})
	metadata := UserLoginMetadata{
		Platform:             types.Messenger,
		Cookies:              jar,
		CredentialRef:        "messenger_opaque",
		CredentialGeneration: 4,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-xs") || strings.Contains(string(data), "cookies") {
		t.Fatalf("externally referenced metadata serialized credentials: %s", data)
	}
	if !strings.Contains(string(data), "credential_ref") {
		t.Fatalf("external credential reference missing: %s", data)
	}
}

func TestUserLoginMetadataReadsLegacyCookies(t *testing.T) {
	var metadata UserLoginMetadata
	if err := json.Unmarshal([]byte(`{"platform":"messenger","cookies":{"xs":"legacy-xs","c_user":"123","datr":"legacy-datr"}}`), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.Cookies == nil || metadata.Cookies.Get(cookies.FBCookieXS) != "legacy-xs" {
		t.Fatal("legacy cookies were not loaded for migration")
	}
}
