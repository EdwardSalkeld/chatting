// Package secrets accepts one-use secret submissions into work-item workspaces.
package secrets

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const prefix = "/secrets/"

var safeID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var tokenPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

//go:embed secret_page.html
var secretPageHTML string
var page = template.Must(template.New("secret").Parse(secretPageHTML))

type pageData struct{ State, Label, ExpiresAt, ExpiresDisplay, Title, Message string }

func renderPage(w http.ResponseWriter, status int, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = page.Execute(w, data)
}

type Service struct {
	Store                    *sqlite.Store
	Root, BaseURL, WorkerURL string
	client                   *http.Client
}

func New(store *sqlite.Store, root, baseURL, workerURL string) (*Service, error) {
	u, err := url.Parse(baseURL)
	if store == nil || !filepath.IsAbs(root) || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("secrets require an absolute workspace root and public base URL")
	}
	worker, err := url.Parse(workerURL)
	if err != nil || worker.Scheme != "http" || worker.Hostname() != "127.0.0.1" || worker.Port() == "" || worker.User != nil || worker.Path != "" || worker.RawQuery != "" || worker.Fragment != "" {
		return nil, errors.New("secret worker URL must be an HTTP loopback origin")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &Service{Store: store, Root: filepath.Clean(root), BaseURL: strings.TrimRight(baseURL, "/"), WorkerURL: workerURL, client: &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func hash(token string) string { h := sha256.Sum256([]byte(token)); return hex.EncodeToString(h[:]) }
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func (s *Service) RegisterWorker(mux *http.ServeMux) {
	mux.HandleFunc("/work-items/secret-request", s.create)
}
func (s *Service) RegisterUI(mux *http.ServeMux) { mux.HandleFunc(prefix, s.form) }
func (s *Service) create(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var in struct {
		TaskID string `json:"task_id"`
		Label  string `json:"label"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in) != nil || strings.TrimSpace(in.TaskID) == "" || strings.TrimSpace(in.Label) == "" || len(in.Label) > 200 {
		http.Error(w, "task_id and label required", 400)
		return
	}
	token, err := randomHex(32)
	if err != nil {
		http.Error(w, "unable to create request", 500)
		return
	}
	id, err := randomHex(16)
	if err != nil {
		http.Error(w, "unable to create request", 500)
		return
	}
	request := sqlite.SecretRequest{ID: id, TokenHash: hash(token), Label: strings.TrimSpace(in.Label), ExpiresAt: time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)}
	if err = s.Store.CreateSecretRequest(r.Context(), in.TaskID, request); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "unknown task", 404)
		} else {
			http.Error(w, "unable to create request", 500)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"url": s.BaseURL + prefix + token, "expires_at": request.ExpiresAt})
}
func (s *Service) form(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "same-origin")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	token := strings.TrimPrefix(r.URL.Path, prefix)
	if !tokenPattern.MatchString(token) {
		http.NotFound(w, r)
		return
	}
	h := hash(token)
	if r.Method == http.MethodGet {
		request, err := s.Store.SecretByHash(r.Context(), h)
		if err != nil || request.Status != "pending" || request.Expired(time.Now()) {
			renderPage(w, http.StatusGone, pageData{State: "unavailable", Title: "This link is no longer available", Message: "It may have expired or already been used. Ask your worker for a new link."})
			return
		}
		expires, _ := time.Parse(time.RFC3339, request.ExpiresAt)
		renderPage(w, http.StatusOK, pageData{State: "form", Label: request.Label, ExpiresAt: request.ExpiresAt, ExpiresDisplay: expires.UTC().Format("2 Jan 2006 at 15:04")})
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	if err := r.ParseForm(); err != nil || len(r.PostForm["secret"]) != 1 || r.PostForm.Get("secret") == "" {
		renderPage(w, http.StatusBadRequest, pageData{State: "error", Title: "Check the value and try again", Message: "Enter a value of at most 64 KiB. Go back to the form to try again."})
		return
	}
	secret := []byte(r.PostForm.Get("secret"))
	alreadySaved := false
	_, err := s.Store.CompleteSecret(r.Context(), h, func(req sqlite.SecretRequest) error {
		var saveErr error
		alreadySaved, saveErr = s.save(req, secret)
		return saveErr
	})
	if err != nil {
		if errors.Is(err, errUnavailable) || errors.Is(err, sql.ErrNoRows) || errors.Is(err, sqlite.ErrSecretUsed) {
			renderPage(w, http.StatusGone, pageData{State: "unavailable", Title: "This link is no longer available", Message: "It may have expired or already been used. Ask your worker for a new link."})
		} else {
			renderPage(w, http.StatusInternalServerError, pageData{State: "error", Title: "Please try again", Message: "The secret could not be saved. Go back to the form and try again."})
		}
		return
	}
	if alreadySaved {
		renderPage(w, http.StatusGone, pageData{State: "unavailable", Title: "This link has already been used", Message: "A previous submission was saved; this value was not used."})
		return
	}
	renderPage(w, http.StatusOK, pageData{State: "success", Title: "Your secret is saved", Message: "Your worker will be notified. You can close this page."})
}

var errUnavailable = errors.New("unavailable")

// The worker owns workspaces and writes the file under its own identity.
func (s *Service) save(req sqlite.SecretRequest, value []byte) (bool, error) {
	if !safeID.MatchString(req.WorkItemID) || !safeID.MatchString(req.ID) {
		return false, errUnavailable
	}
	var secret *string
	if value != nil {
		text := string(value)
		secret = &text
	}
	data, err := json.Marshal(map[string]any{"work_item_id": req.WorkItemID, "request_id": req.ID, "expires_at": req.ExpiresAt, "secret": secret, "workspace_root": s.Root})
	if err != nil {
		return false, err
	}
	response, err := s.client.Post(s.WorkerURL+"/internal/secret-file", "application/json", bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusGone {
		return false, errUnavailable
	}
	if response.StatusCode != http.StatusOK {
		return false, errors.New("worker secret file service failed")
	}
	var result struct {
		Path     string `json:"path"`
		Existing bool   `json:"existing"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result); err != nil {
		return false, err
	}
	if result.Path != s.path(req) {
		return false, errors.New("worker workspace root does not match handler configuration")
	}
	return result.Existing, nil
}
func (s *Service) path(req sqlite.SecretRequest) string {
	return filepath.Join(s.Root, req.WorkItemID, ".chatting-secrets", req.ID)
}

func (s *Service) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	records, err := s.Store.PendingSecrets(ctx)
	if err != nil {
		return nil, err
	}
	var out []contracts.TaskEnvelope
	for _, r := range records {
		if r.Status == "pending" {
			_, err = s.Store.CompleteSecret(ctx, r.TokenHash, func(req sqlite.SecretRequest) error { _, saveErr := s.save(req, nil); return saveErr })
			if err != nil {
				if errors.Is(err, errUnavailable) && r.Expired(time.Now()) {
					if expireErr := s.Store.ExpireSecret(ctx, r.ID); expireErr != nil {
						return nil, expireErr
					}
				}
				if errors.Is(err, errUnavailable) || errors.Is(err, sqlite.ErrSecretUsed) {
					continue
				}
				// One unavailable workspace must not stall every other ingress source.
				log.Printf("secret_recovery_failed request_id=%s", r.ID)
				continue
			}
		}
		id := "secret:" + r.ID
		out = append(out, contracts.TaskEnvelope{SchemaVersion: contracts.SchemaVersion, ID: id, Source: "internal", ReceivedAt: contracts.NewTimestamp(time.Now()), Content: fmt.Sprintf("Secret has been saved to %s. Use it for the requested purpose and delete the file when finished. Never include its contents in a reply.", s.path(r)), ReplyChannel: r.Reply, DedupeKey: id})
	}
	return out, nil
}
func (s *Service) AckEnvelope(ctx context.Context, id string) error {
	return s.Store.AckSecret(ctx, strings.TrimPrefix(id, "secret:"))
}
