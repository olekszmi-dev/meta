package igconnector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

type externalCredentialData struct {
	Platform types.Platform    `json:"platform"`
	Cookies  map[string]string `json:"cookies"`
	LoginUA  string            `json:"loginUA,omitempty"`
}

type externalCredentialResponse struct {
	OK                   bool                   `json:"ok"`
	AccountID            string                 `json:"accountId"`
	NativeLoginID        string                 `json:"nativeLoginId"`
	CredentialGeneration uint64                 `json:"credentialGeneration"`
	Credentials          externalCredentialData `json:"credentials"`
	Error                string                 `json:"error,omitempty"`
}

type externalCredentialWrite struct {
	AccountID     string                 `json:"accountId"`
	NativeLoginID string                 `json:"nativeLoginId"`
	Credentials   externalCredentialData `json:"credentials"`
}

type externalCommand struct {
	CommandID   string         `json:"commandId"`
	CommandType string         `json:"commandType"`
	ApprovalRef string         `json:"approvalRef,omitempty"`
	ApprovedAt  string         `json:"approvedAt,omitempty"`
	Payload     map[string]any `json:"payload"`
}

type externalCommandResponse struct {
	OK      bool             `json:"ok"`
	Command *externalCommand `json:"command"`
}

type ExternalControlClient struct {
	BaseURL       string
	Token         string
	AccountID     string
	NativeLoginID string
	HTTP          *http.Client
}

func NewExternalControlClientFromEnv() *ExternalControlClient {
	client := &ExternalControlClient{
		BaseURL:       strings.TrimRight(strings.TrimSpace(os.Getenv("ZERO_INSTAGRAM_CONTROL_URL")), "/"),
		Token:         strings.TrimSpace(os.Getenv("ZERO_INSTAGRAM_CONTROL_TOKEN")),
		AccountID:     strings.TrimSpace(os.Getenv("ZERO_INSTAGRAM_ACCOUNT_ID")),
		NativeLoginID: strings.TrimSpace(os.Getenv("ZERO_INSTAGRAM_NATIVE_LOGIN_ID")),
		HTTP:          &http.Client{Timeout: 10 * time.Second},
	}
	if !client.Enabled() {
		return nil
	}
	return client
}

func (c *ExternalControlClient) Enabled() bool {
	return c != nil && c.BaseURL != "" && c.Token != "" && c.AccountID != "" && c.NativeLoginID != ""
}

func (c *ExternalControlClient) OwnsLogin(loginID string) bool {
	return c.Enabled() && c.NativeLoginID == loginID
}

func (c *ExternalControlClient) doJSON(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("external control returned status %d", resp.StatusCode)
	}
	if output != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(output)
	}
	return nil
}

func cookieMap(input *cookies.Cookies) map[string]string {
	result := make(map[string]string)
	if input == nil {
		return result
	}
	for key, value := range input.GetAll() {
		result[string(key)] = value
	}
	return result
}

func hydrateCookies(input map[string]string) *cookies.Cookies {
	values := make(map[cookies.MetaCookieName]string, len(input))
	for key, value := range input {
		values[cookies.MetaCookieName(key)] = value
	}
	result := &cookies.Cookies{Platform: types.Instagram}
	result.UpdateValues(values)
	return result
}

func (c *ExternalControlClient) LoadCredentials(ctx context.Context, loginID string) (*externalCredentialResponse, error) {
	if !c.OwnsLogin(loginID) {
		return nil, fmt.Errorf("external credential login is not owned by this worker")
	}
	var response externalCredentialResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/credentials", nil, &response); err != nil {
		return nil, err
	}
	if !response.OK || response.AccountID != c.AccountID || response.NativeLoginID != loginID {
		return nil, fmt.Errorf("external credential owner mismatch")
	}
	return &response, nil
}

func (c *ExternalControlClient) StoreCredentials(ctx context.Context, loginID string, input *cookies.Cookies, loginUA string) (uint64, error) {
	if !c.OwnsLogin(loginID) {
		return 0, fmt.Errorf("external credential login is not owned by this worker")
	}
	request := externalCredentialWrite{
		AccountID:     c.AccountID,
		NativeLoginID: loginID,
		Credentials: externalCredentialData{
			Platform: types.Instagram, Cookies: cookieMap(input), LoginUA: loginUA,
		},
	}
	var response externalCredentialResponse
	if err := c.doJSON(ctx, http.MethodPut, "/v1/credentials", &request, &response); err != nil {
		return 0, err
	}
	if !response.OK || response.CredentialGeneration == 0 {
		return 0, fmt.Errorf("external credential update was not acknowledged")
	}
	return response.CredentialGeneration, nil
}

func (c *ExternalControlClient) EmitEvent(ctx context.Context, event any) error {
	if !c.Enabled() {
		return nil
	}
	return c.doJSON(ctx, http.MethodPost, "/v1/events", event, nil)
}

func (c *ExternalControlClient) PollCommand(ctx context.Context) (*externalCommand, error) {
	var response externalCommandResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/commands/next", nil, &response); err != nil {
		return nil, err
	}
	if !response.OK {
		return nil, fmt.Errorf("external command poll was not acknowledged")
	}
	return response.Command, nil
}

func (c *ExternalControlClient) CompleteCommand(ctx context.Context, commandID string, result any) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/commands/"+url.PathEscape(commandID)+"/result", result, nil)
}
