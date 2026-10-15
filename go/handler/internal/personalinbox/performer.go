package personalinbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/imap"
)

type HandlerPerformer struct {
	Config        Config
	Store         *Store
	Dial          Dial
	HTTP          *http.Client
	TelegramToken string
	TelegramBase  string
	LinearKey     string
	LinearURL     string

	session imap.UIDClient
}

// One login per perform pass rather than one per effect: a backfill can plan
// dozens of mailbox effects at a time, and personal mail providers throttle or
// lock accounts that reconnect that often. Any failure drops the session so the
// next effect reconnects rather than reusing a half-broken one.
func (performer *HandlerPerformer) mailbox(ctx context.Context) (imap.UIDClient, error) {
	if performer.session != nil {
		return performer.session, nil
	}
	client, err := performer.Dial(ctx)
	if err != nil {
		return nil, err
	}
	performer.session = client
	return client, nil
}

func (performer *HandlerPerformer) Release() {
	if performer.session != nil {
		_ = performer.session.Logout()
		performer.session = nil
	}
}

func (performer *HandlerPerformer) post(ctx context.Context, endpoint string, value any, authorization string, result any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := performer.HTTP.Do(request)
	if err != nil {
		return errors.New("external request outcome unknown")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("external request rejected; reconcile before retry")
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1024*1024)).Decode(result); err != nil {
		return errors.New("external receipt invalid")
	}
	return nil
}

func (performer *HandlerPerformer) Perform(ctx context.Context, effect Effect) (string, error) {
	switch effect.Kind {
	case "telegram":
		if effect.Target != performer.Config.OrdersTarget && effect.Target != performer.Config.ActionTarget && effect.Target != performer.Config.DigestTarget {
			return "", errors.New("destination no longer approved")
		}
		var result struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageID int64 `json:"message_id"`
			} `json:"result"`
		}
		err := performer.post(ctx, strings.TrimRight(performer.TelegramBase, "/")+"/bot"+performer.TelegramToken+"/sendMessage",
			map[string]any{"chat_id": effect.Target, "text": effect.Body, "link_preview_options": map[string]bool{"is_disabled": true}}, "", &result)
		if err != nil {
			return "", err
		}
		if !result.OK || result.Result.MessageID <= 0 {
			return "", errors.New("Telegram receipt missing")
		}
		return fmt.Sprint(result.Result.MessageID), nil
	case "linear":
		return performer.linear(ctx, effect)
	case "read", "move":
		if (effect.Kind == "move" && (!performer.Config.MoveNews || effect.Target != performer.Config.NewsFolder)) ||
			(effect.Kind == "read" && (!performer.Config.MarkNewsRead || effect.Target != performer.Config.Mailbox)) {
			return "", errors.New("mailbox operation no longer approved")
		}
		client, err := performer.mailbox(ctx)
		if err != nil {
			return "", errors.New("IMAP unavailable")
		}
		validity, _, err := client.SelectUID(performer.Config.Mailbox)
		if err != nil {
			performer.Release()
			return "", errors.New("IMAP selection failed")
		}
		identityBytes, _ := json.Marshal([]any{performer.Config.Account, performer.Config.Host, performer.Config.Port, performer.Config.Username, performer.Config.Mailbox, validity})
		identity, err := performer.Store.getMeta(ctx, "identity")
		if err != nil {
			return "", err
		}
		if identity != string(identityBytes) {
			return "", errors.New("mailbox identity changed; operation blocked")
		}
		exists, err := client.ExistsUID(effect.UID)
		if err != nil {
			performer.Release()
			return "", errors.New("UID existence check failed")
		}
		if !exists {
			return "", errors.New("source message vanished; reconcile move/read outcome")
		}
		if effect.Kind == "read" {
			err = client.MarkReadUID(effect.UID)
		} else {
			err = client.MoveUID(effect.UID, effect.Target)
		}
		if err != nil {
			performer.Release()
			return "", errors.New("mailbox operation outcome unknown")
		}
		return "IMAP OK", nil
	}
	return "", errors.New("operation not permitted")
}

// A stable client-supplied UUID prevents a retry from creating a second Linear
// issue. It is a UUIDv4-shaped identifier derived from the durable effect key.
func issueID(effectID string) string {
	hash := sha256.Sum256([]byte("chatting-personal-inbox:" + effectID))
	hash[6] = (hash[6] & 0x0f) | 0x40
	hash[8] = (hash[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", hash[:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16])
}

func (performer *HandlerPerformer) linear(ctx context.Context, effect Effect) (string, error) {
	if effect.Target != performer.Config.LinearTeam {
		return "", errors.New("Linear team no longer approved")
	}
	id := issueID(effect.ID)
	if url := performer.linearLookup(ctx, id); url != "" {
		return url, nil
	}
	// Linear may report an absent UUID as a GraphQL error or an HTTP status, so
	// a failed lookup says nothing about whether the issue exists. A mutation
	// with the same UUID is safe either way; it cannot create two IDs.
	input := map[string]any{"id": id, "teamId": effect.Target, "title": string([]rune(strings.SplitN(effect.Body, "\n", 2)[0])[:min(200, len([]rune(strings.SplitN(effect.Body, "\n", 2)[0])))]), "description": effect.Body}
	if performer.Config.LinearProject != "" {
		input["projectId"] = performer.Config.LinearProject
	}
	var created struct {
		Errors []json.RawMessage `json:"errors"`
		Data   struct {
			Create struct {
				Success bool `json:"success"`
				Issue   *struct {
					ID  string `json:"id"`
					URL string `json:"url"`
				} `json:"issue"`
			} `json:"issueCreate"`
		} `json:"data"`
	}
	err := performer.post(ctx, performer.LinearURL, map[string]any{"query": "mutation($input:IssueCreateInput!){issueCreate(input:$input){success issue{id url}}}", "variables": map[string]any{"input": input}}, performer.LinearKey, &created)
	if err != nil {
		return "", err
	}
	if len(created.Errors) > 0 || !created.Data.Create.Success || created.Data.Create.Issue == nil || created.Data.Create.Issue.ID != id {
		// The issue may exist already, from this UUID's earlier attempt or from
		// a creation whose response was lost. Prefer that over a false failure.
		if url := performer.linearLookup(ctx, id); url != "" {
			return url, nil
		}
		return "", errors.New("Linear creation receipt invalid")
	}
	return created.Data.Create.Issue.URL, nil
}

// An empty result means "no issue we can confirm", whether that is a genuine
// absence or an unreachable API. Callers treat both the same way.
func (performer *HandlerPerformer) linearLookup(ctx context.Context, id string) string {
	var existing struct {
		Errors []json.RawMessage `json:"errors"`
		Data   struct {
			Issue *struct {
				ID  string `json:"id"`
				URL string `json:"url"`
			} `json:"issue"`
		} `json:"data"`
	}
	if err := performer.post(ctx, performer.LinearURL, map[string]any{"query": "query($id:String!){issue(id:$id){id url}}", "variables": map[string]string{"id": id}}, performer.LinearKey, &existing); err != nil {
		return ""
	}
	if existing.Data.Issue == nil || existing.Data.Issue.ID != id {
		return ""
	}
	return existing.Data.Issue.URL
}
