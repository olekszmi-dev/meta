package igconnector

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
				OK: true, AccountID: "opaque-account", NativeLoginID: "instagram_1", CredentialGeneration: 3,
				Credentials: externalCredentialData{Platform: types.Instagram, Cookies: map[string]string{"sessionid": "secret"}},
			})
			return
		}
		var request externalCredentialWrite
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Credentials.Cookies["sessionid"] != "rotated" {
			t.Error("rotated cookie missing")
		}
		_ = json.NewEncoder(w).Encode(externalCredentialResponse{OK: true, CredentialGeneration: 4})
	}))
	defer server.Close()

	client := &ExternalControlClient{
		BaseURL: server.URL, Token: "test-token", AccountID: "opaque-account", NativeLoginID: "instagram_1", HTTP: server.Client(),
	}
	loaded, err := client.LoadCredentials(context.Background(), "instagram_1")
	if err != nil || loaded.Credentials.Cookies["sessionid"] != "secret" {
		t.Fatalf("load failed: %v", err)
	}
	jar := &cookies.Cookies{Platform: types.Instagram}
	jar.UpdateValues(map[cookies.MetaCookieName]string{cookies.MetaCookieName("sessionid"): "rotated"})
	generation, err := client.StoreCredentials(context.Background(), "instagram_1", jar, "test-agent")
	if err != nil || generation != 4 {
		t.Fatalf("store failed: generation=%d err=%v", generation, err)
	}
	if requests != 2 {
		t.Fatalf("unexpected request count: %d", requests)
	}
	if _, err = client.LoadCredentials(context.Background(), "instagram_2"); err == nil {
		t.Fatal("non-owned login was accepted")
	}
}
