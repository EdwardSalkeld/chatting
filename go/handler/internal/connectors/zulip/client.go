// Package zulip implements the authenticated Zulip message API.
package zulip

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Site, Email, APIKey            string
	AllowedChannelIDs, ContextRefs []string
	AllowPrivateMessages           bool
}

type Client struct {
	config Config
	http   *http.Client
}

func NewClient(config Config) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(config.Site, "/"))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("zulip_site must be an http(s) origin")
	}
	if strings.TrimSpace(config.Email) == "" || strings.TrimSpace(config.APIKey) == "" {
		return nil, errors.New("Zulip bot email and API key are required")
	}
	config.Site = strings.TrimRight(config.Site, "/")
	config.Email = strings.TrimSpace(config.Email)
	for _, id := range config.AllowedChannelIDs {
		if n, err := strconv.ParseInt(id, 10, 64); err != nil || n <= 0 {
			return nil, errors.New("zulip_allowed_channel_ids must contain positive numeric IDs")
		}
	}
	return &Client{config: config, http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Target is a channel/topic pair or the other participants in a private chat.
// JSON preserves topic names exactly, including punctuation and Unicode.
type Target struct {
	Site      string  `json:"site"`
	ChannelID int64   `json:"channel_id,omitempty"`
	Topic     string  `json:"topic,omitempty"`
	UserIDs   []int64 `json:"user_ids,omitempty"`
}

func (target Target) String() string { raw, _ := json.Marshal(target); return string(raw) }

func ParseTarget(raw string) (Target, error) {
	var target Target
	if err := json.Unmarshal([]byte(raw), &target); err != nil {
		return target, errors.New("invalid Zulip target JSON")
	}
	if target.Site == "" {
		return target, errors.New("Zulip target needs a site")
	}
	if target.ChannelID > 0 && len(target.UserIDs) == 0 {
		return target, nil
	}
	if target.ChannelID != 0 || target.Topic != "" || len(target.UserIDs) == 0 {
		return target, errors.New("Zulip target needs channel_id/topic or user_ids")
	}
	for _, id := range target.UserIDs {
		if id <= 0 {
			return target, errors.New("invalid Zulip user ID")
		}
	}
	sort.Slice(target.UserIDs, func(i, j int) bool { return target.UserIDs[i] < target.UserIDs[j] })
	return target, nil
}

func (client *Client) request(ctx context.Context, method, path string, values url.Values, result any) error {
	endpoint := client.config.Site + "/api/v1/" + path
	var body io.Reader
	if method == http.MethodGet {
		endpoint += "?" + values.Encode()
	} else {
		body = strings.NewReader(values.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return errors.New("zulip_request_invalid")
	}
	req.SetBasicAuth(client.config.Email, client.config.APIKey)
	req.Header.Set("User-Agent", "Chatting-Zulip/1.0")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.http.Do(req)
	if err != nil {
		return errors.New("zulip_request_failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("zulip_http_status_" + strconv.Itoa(response.StatusCode))
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return errors.New("zulip_response_failed")
	}
	var status struct {
		Result string `json:"result"`
	}
	if json.Unmarshal(raw, &status) != nil || status.Result != "success" {
		return errors.New("zulip_api_failed")
	}
	if json.Unmarshal(raw, result) != nil {
		return errors.New("zulip_response_invalid")
	}
	return nil
}

func (client *Client) Send(ctx context.Context, rawTarget, body string) error {
	target, err := ParseTarget(rawTarget)
	if err != nil {
		return err
	}
	if target.Site != client.config.Site {
		return errors.New("zulip_target_site_mismatch")
	}
	values := url.Values{"content": {body}}
	if target.ChannelID > 0 {
		values.Set("type", "stream")
		values.Set("to", strconv.FormatInt(target.ChannelID, 10))
		values.Set("topic", target.Topic)
	} else {
		values.Set("type", "direct")
		ids, _ := json.Marshal(target.UserIDs)
		values.Set("to", string(ids))
	}
	var response struct {
		ID int64 `json:"id"`
	}
	return client.request(ctx, http.MethodPost, "messages", values, &response)
}
