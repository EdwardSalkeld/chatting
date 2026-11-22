// Package secrets accepts one-use secret submissions into work-item workspaces.
package secrets

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
	"golang.org/x/sys/unix"
)

const prefix = "/secrets/"

var safeID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var tokenPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var page = template.Must(template.New("secret").Parse(`<!doctype html><html><head><meta name="referrer" content="no-referrer"><title>Send a secret</title></head><body><h1>Send a secret</h1><p>{{.Label}}</p><p>Expires {{.ExpiresAt}}.</p><form method="post"><label>Secret<br><textarea name="secret" rows="8" cols="60" required></textarea></label><br><button type="submit">Save secret</button></form></body></html>`))

type Service struct {
	Store         *sqlite.Store
	Root, BaseURL string
}

func New(store *sqlite.Store, root, baseURL string) (*Service, error) {
	u, err := url.Parse(baseURL)
	if store == nil || !filepath.IsAbs(root) || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("secrets require an absolute workspace root and public base URL")
	}
	return &Service{store, filepath.Clean(root), strings.TrimRight(baseURL, "/")}, nil
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
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	token := strings.TrimPrefix(r.URL.Path, prefix)
	if !tokenPattern.MatchString(token) {
		http.NotFound(w, r)
		return
	}
	h := hash(token)
	if r.Method == http.MethodGet {
		request, err := s.Store.SecretByHash(r.Context(), h)
		if err != nil || request.Status != "pending" || request.Expired(time.Now()) {
			http.Error(w, "This request is unavailable or expired.", http.StatusGone)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = page.Execute(w, request)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	if err := r.ParseForm(); err != nil || len(r.PostForm["secret"]) != 1 || r.PostForm.Get("secret") == "" {
		http.Error(w, "Enter a secret of at most 64 KiB.", 400)
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
			http.Error(w, "This request is unavailable or expired.", http.StatusGone)
		} else {
			http.Error(w, "Unable to save secret. Please retry.", 500)
		}
		return
	}
	if alreadySaved {
		http.Error(w, "A previous submission was already saved; this value was not used.", http.StatusGone)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "Secret saved. You can close this page.")
}

var errUnavailable = errors.New("unavailable")

// Open each directory without following symlinks; all file names are generated.
func (s *Service) directory(req sqlite.SecretRequest) (int, error) {
	if !safeID.MatchString(req.WorkItemID) || !safeID.MatchString(req.ID) {
		return -1, errUnavailable
	}
	root, err := unix.Open(s.Root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	defer unix.Close(root)
	work, err := unix.Openat(root, req.WorkItemID, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	defer unix.Close(work)
	if err = unix.Mkdirat(work, ".chatting-secrets", 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return -1, err
	}
	if err = unix.Fsync(work); err != nil {
		return -1, err
	}
	dir, err := unix.Openat(work, ".chatting-secrets", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	var st unix.Stat_t
	if err = unix.Fstat(dir, &st); err != nil || st.Mode&0077 != 0 || st.Uid != uint32(os.Geteuid()) {
		unix.Close(dir)
		return -1, errUnavailable
	}
	return dir, nil
}
func exists(dir int, name string) (bool, error) {
	var st unix.Stat_t
	err := unix.Fstatat(dir, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return false, errUnavailable
	}
	return true, nil
}
func (s *Service) save(req sqlite.SecretRequest, value []byte) (bool, error) {
	dir, err := s.directory(req)
	if err != nil {
		return false, err
	}
	defer unix.Close(dir)
	present, err := exists(dir, req.ID)
	if err != nil {
		return false, err
	}
	if present {
		return true, unix.Fsync(dir)
	}
	if req.Expired(time.Now()) || value == nil {
		return false, errUnavailable
	}
	// An unnamed inode cannot leave partial secret bytes behind after a crash.
	fd, err := unix.Openat(dir, ".", unix.O_WRONLY|unix.O_TMPFILE|unix.O_CLOEXEC, 0600)
	if err != nil {
		return false, err
	}
	file := os.NewFile(uintptr(fd), "secret")
	defer file.Close()
	_, err = file.Write(value)
	if err == nil {
		err = file.Sync()
	}
	if err != nil {
		return false, err
	}
	if err = unix.Linkat(fd, "", dir, req.ID, unix.AT_EMPTY_PATH); err != nil {
		return false, err
	}
	return false, unix.Fsync(dir)
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
				if errors.Is(err, errUnavailable) || errors.Is(err, unix.ENOENT) || errors.Is(err, sqlite.ErrSecretUsed) {
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
