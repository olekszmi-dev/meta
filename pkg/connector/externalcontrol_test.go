package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

func TestExternalControlLoadsAndStoresOwnedCredentials(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		requests++
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(externalCredentialResponse{
				OK: true, AccountID: "opaque-account", NativeLoginID: "facebook_1", CredentialGeneration: 3,
				Credentials: externalCredentialData{Platform: types.Messenger, Cookies: map[string]string{"xs": "secret", "c_user": "1", "datr": "datr"}},
			})
			return
		}
		var request externalCredentialWrite
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Credentials.Cookies["xs"] != "rotated" {
			t.Error("rotated cookie missing")
		}
		_ = json.NewEncoder(w).Encode(externalCredentialResponse{OK: true, CredentialGeneration: 4})
	}))
	defer server.Close()

	client := &ExternalControlClient{
		BaseURL: server.URL, Token: "test-token", AccountID: "opaque-account", NativeLoginID: "facebook_1", HTTP: server.Client(),
	}
	loaded, err := client.LoadCredentials(context.Background(), "facebook_1")
	if err != nil || loaded.Credentials.Cookies["xs"] != "secret" {
		t.Fatalf("load failed: %v", err)
	}
	jar := &cookies.Cookies{Platform: types.Messenger}
	jar.UpdateValues(map[cookies.MetaCookieName]string{cookies.FBCookieXS: "rotated"})
	generation, err := client.StoreCredentials(context.Background(), "facebook_1", types.Messenger, jar, "", nil)
	if err != nil || generation != 4 {
		t.Fatalf("store failed: generation=%d err=%v", generation, err)
	}
	if requests != 2 {
		t.Fatalf("unexpected request count: %d", requests)
	}
	if _, err = client.LoadCredentials(context.Background(), "facebook_2"); err == nil {
		t.Fatal("non-owned login was accepted")
	}
}
