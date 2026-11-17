// Package platform serves the private single-operator interface and its bounded
// same-origin API gateway. State and validation remain with handler and worker.
package platform

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
)

//go:embed web
var embedded embed.FS

// Assets returns the production frontend embedded at compile time.
func Assets() (fs.FS, error) {
	assets, err := fs.Sub(embedded, "web/dist")
	if err == nil {
		_, err = fs.Stat(assets, "index.html")
	}
	if err != nil {
		return nil, fmt.Errorf("platform assets missing: run npm ci && npm run build in platform/ before building chatting-platform: %w", err)
	}
	return assets, nil
}

type Config struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	HandlerURL string `json:"handler_url"`
	WorkerURL  string `json:"worker_url"`
}

func DefaultConfig() Config {
	return Config{Host: "127.0.0.1", Port: 9466, HandlerURL: "http://127.0.0.1:9464", WorkerURL: "http://127.0.0.1:9465"}
}

func DecodeConfig(reader io.Reader) (Config, error) {
	config := DefaultConfig()
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	input := &config
	if err := decoder.Decode(&input); err != nil {
		return config, err
	}
	if input == nil {
		return config, fmt.Errorf("config must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return config, fmt.Errorf("config must contain one JSON object")
	}
	return config, config.Validate()
}

func (config Config) Validate() error {
	if strings.TrimSpace(config.Host) == "" || config.Port < 1 || config.Port > 65535 {
		return fmt.Errorf("host and a port between 1 and 65535 are required")
	}
	for name, raw := range map[string]string{"handler_url": config.HandlerURL, "worker_url": config.WorkerURL} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("%s must be an HTTP(S) origin without credentials, a path or query", name)
		}
	}
	return nil
}

var collection = regexp.MustCompile(`^/api/handler/(schedules|reminders)$`)
var member = regexp.MustCompile(`^/api/handler/(schedules|reminders)/[^/]+$`)
var worker = regexp.MustCompile(`^/api/worker/(items|items/[^/]+/runs|runs|runs/[^/]+|dead-letters|tasks/[^/]+/events|activity|metrics)$`)
var page = regexp.MustCompile(`^/(|overview|items|items/[^/]+|runs|runs/[^/]+|tasks/[^/]+|schedules|reminders|inbox|system)$`)

func writeError(writer http.ResponseWriter, status int, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": message})
}

func proxy(raw string) *httputil.ReverseProxy {
	target, _ := url.Parse(raw) // Validated by NewHandler.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Local services must not go through environment proxies.
	transport.ResponseHeaderTimeout = 5 * time.Second
	return &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.Host = target.Host
			// Only forward the documented interface, without browser cookies or auth.
			request.Out.Header = http.Header{"Accept": {"application/json"}, "Content-Type": {"application/json"}}
		},
		Transport: transport,
		ErrorHandler: func(writer http.ResponseWriter, _ *http.Request, _ error) {
			writeError(writer, http.StatusBadGateway, "Service unavailable. Check the handler or worker connection.")
		},
	}
}

func NewHandler(config Config, assets fs.FS) (http.Handler, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	handlerProxy, workerProxy := proxy(config.HandlerURL), proxy(config.WorkerURL)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		routePath := request.URL.EscapedPath()
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
		if strings.HasPrefix(request.URL.Path, "/api/") {
			var upstream *httputil.ReverseProxy
			var allowed string
			switch {
			case collection.MatchString(routePath):
				upstream, allowed = handlerProxy, "GET, POST"
			case member.MatchString(routePath):
				upstream, allowed = handlerProxy, "GET, PUT, DELETE"
			case request.URL.Path == "/api/handler/inbox", request.URL.Path == "/api/handler/metrics":
				upstream, allowed = handlerProxy, "GET"
			case worker.MatchString(routePath):
				upstream, allowed = workerProxy, "GET"
			default:
				writeError(writer, http.StatusNotFound, "API route not found")
				return
			}
			if !strings.Contains(", "+allowed+",", ", "+request.Method+",") {
				writer.Header().Set("Allow", allowed)
				writeError(writer, http.StatusMethodNotAllowed, "Method not allowed")
				return
			}
			if request.Method != http.MethodGet {
				// Network isolation is the access boundary. This guards cross-site
				// browser writes without introducing application authentication.
				if origin := request.Header.Get("Origin"); origin != "" {
					parsed, err := url.Parse(origin)
					if err != nil || parsed.Host != request.Host {
						writeError(writer, http.StatusForbidden, "Cross-origin writes are not allowed")
						return
					}
				}
				body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 1<<20))
				if err != nil {
					writeError(writer, http.StatusRequestEntityTooLarge, "Request body exceeds 1 MiB")
					return
				}
				request.Body = io.NopCloser(strings.NewReader(string(body)))
				request.ContentLength = int64(len(body))
			}
			request = request.Clone(request.Context())
			ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
			defer cancel()
			request = request.WithContext(ctx)
			if upstream == workerProxy {
				request.URL.Path = "/api" + strings.TrimPrefix(request.URL.Path, "/api/worker")
				request.URL.RawPath = "/api" + strings.TrimPrefix(routePath, "/api/worker")
				switch request.URL.Path {
				case "/api/activity":
					request.URL.Path, request.URL.RawPath = "/activity.json", ""
				case "/api/metrics":
					request.URL.Path, request.URL.RawPath = "/metrics", ""
				}
			} else if request.URL.Path == "/api/handler/inbox" {
				request.URL.Path = "/api/personal-inbox/decisions"
				request.URL.RawPath = ""
			} else if request.URL.Path == "/api/handler/metrics" {
				request.URL.Path, request.URL.RawPath = "/metrics", ""
			} else {
				request.URL.Path = "/api" + strings.TrimPrefix(request.URL.Path, "/api/handler")
				request.URL.RawPath = "/api" + strings.TrimPrefix(routePath, "/api/handler")
			}
			upstream.ServeHTTP(writer, request)
			return
		}
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writeError(writer, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		filename := strings.TrimPrefix(request.URL.Path, "/")
		if page.MatchString(routePath) {
			filename = "index.html"
		}
		if filename == "" || !fs.ValidPath(filename) || path.Clean(filename) != filename {
			http.NotFound(writer, request)
			return
		}
		if _, err := fs.Stat(assets, filename); err != nil {
			// Unknown app routes get the app shell so the client renders its own not-found page
			// with navigation; missing assets and files stay a plain 404.
			if strings.HasPrefix(filename, "assets/") || strings.Contains(path.Base(filename), ".") {
				http.NotFound(writer, request)
				return
			}
			index, err := fs.ReadFile(assets, "index.html")
			if err != nil {
				http.NotFound(writer, request)
				return
			}
			writer.Header().Set("Content-Type", "text/html; charset=utf-8")
			writer.WriteHeader(http.StatusNotFound)
			if request.Method != http.MethodHead {
				_, _ = writer.Write(index)
			}
			return
		}
		http.ServeFileFS(writer, request, assets, filename)
	}), nil
}
