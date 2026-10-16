package personalinbox

import (
	"fmt"
	"html/template"
	"net/http"
	"time"
)

const uiRoutePrefix = "/personal-inbox"

// RegisterUIRoutes serves the decision list on the handler's own UI, alongside
// schedules and reminders. It shows mail subjects, so it belongs there rather
// than anywhere reachable from outside.
func RegisterUIRoutes(mux *http.ServeMux, service *Service) {
	mux.HandleFunc(uiRoutePrefix, service.handleUIDecisions)
}

type decisionRow struct {
	Received     string
	Sender       string
	Subject      string
	Decision     string
	Score        string
	Why          string
	Link         string
	Unclassified bool
}

type decisionsView struct {
	DryRun     bool
	Threshold  string
	Counts     map[string]int
	Awaiting   int
	Stuck      int
	OldestWait string
	DeadLetter string
	LastError  string
	Rows       []decisionRow
}

func (service *Service) handleUIDecisions(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := request.Context()
	messages, err := service.Store.recent(ctx, 200)
	if err != nil {
		http.Error(writer, "storage unavailable", http.StatusServiceUnavailable)
		return
	}
	now := service.Now().Unix()
	waiting, oldest, stuck, err := service.Store.awaiting(ctx, now)
	if err != nil {
		http.Error(writer, "storage unavailable", http.StatusServiceUnavailable)
		return
	}
	deadLetters, _ := service.Store.getMeta(ctx, "dead_letters")
	lastError, _ := service.Store.getMeta(ctx, "last_error")

	view := decisionsView{
		DryRun:     service.Config.DryRun,
		Threshold:  fmt.Sprintf("%.2f", service.Config.Threshold()),
		Counts:     map[string]int{},
		Awaiting:   waiting,
		Stuck:      stuck,
		OldestWait: humaniseAge(oldest),
		DeadLetter: deadLetters,
		LastError:  lastError,
	}
	if view.DeadLetter == "" {
		view.DeadLetter = "0"
	}
	for _, message := range messages {
		row := decisionRow{
			Received: time.Unix(message.Created, 0).UTC().Format("02 Jan 15:04"),
			Sender:   message.Mail.From,
			Subject:  message.Mail.Subject,
		}
		classification := message.Classification
		if classification == nil {
			row.Decision, row.Unclassified = "waiting", true
			if message.Attempts > 0 {
				row.Why = fmt.Sprintf("asked %d time(s)", message.Attempts)
			}
		} else {
			row.Decision = classification.Category
			row.Score = fmt.Sprintf("%.2f", classification.Confidence)
			if trace := classification.Trace; trace != nil {
				row.Why = trace.Reason
				// A category the model picked and the policy then overrode is
				// the interesting case, so say what it originally read as.
				if trace.JevCategory != "" && trace.JevCategory != classification.Category {
					row.Why += fmt.Sprintf(" (read as %s)", trace.JevCategory)
				}
			}
			row.Link = service.link(message, service.Config.Folder(classification.Category))
			view.Counts[classification.Category]++
		}
		view.Rows = append(view.Rows, row)
	}
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := decisionsTemplate.Execute(writer, view); err != nil {
		http.Error(writer, "render failed", http.StatusInternalServerError)
	}
}

func humaniseAge(seconds int64) string {
	switch {
	case seconds <= 0:
		return "-"
	case seconds < 120:
		return fmt.Sprintf("%ds", seconds)
	case seconds < 7200:
		return fmt.Sprintf("%dm", seconds/60)
	default:
		return fmt.Sprintf("%dh", seconds/3600)
	}
}

const decisionsPageStyle = `
	:root { color-scheme: light dark; } body { font-family: system-ui, sans-serif; max-width: 72rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }
	h1 { font-size: 1.4rem; } a { color: #2563eb; } table { border-collapse: collapse; width: 100%; }
	th, td { text-align: left; padding: .35rem .6rem; border-bottom: 1px solid #8883; vertical-align: top; }
	td.subject { max-width: 26rem; overflow-wrap: anywhere; } td.why, td.received { color: #6b7280; font-size: .85rem; white-space: nowrap; }
	.summary { display: flex; flex-wrap: wrap; gap: .5rem 1.2rem; margin: 0 0 1.2rem; padding: .6rem .9rem; border: 1px solid #8883; border-radius: .4rem; }
	.summary span { white-space: nowrap; } .warn { color: #b91c1c; font-weight: 600; }
	.tag { font-weight: 600; } .tag.review { color: #b45309; } .tag.action { color: #b91c1c; }
	.tag.order { color: #047857; } .tag.news { color: #6b7280; } .tag.waiting { color: #2563eb; }
`

var decisionsTemplate = template.Must(template.New("personal-inbox").Parse(
	`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Personal inbox</title><style>` +
		decisionsPageStyle + `</style></head><body>
<h1>Personal inbox decisions</h1><p><a href="/reminders">Reminders</a> &middot; <a href="/schedules">Schedules</a></p>
<div class="summary">
<span>{{if .DryRun}}<strong>preview only</strong>{{else}}acting{{end}}</span>
<span>threshold {{.Threshold}}</span>
{{range $category, $count := .Counts}}<span>{{$category}} {{$count}}</span>{{end}}
<span{{if .Awaiting}} class="warn"{{end}}>waiting {{.Awaiting}}{{if .Awaiting}} (oldest {{.OldestWait}}){{end}}</span>
{{if .Stuck}}<span class="warn">stuck {{.Stuck}}</span>{{end}}
<span{{if ne .DeadLetter "0"}} class="warn"{{end}}>rejected {{.DeadLetter}}</span>
{{if .LastError}}<span class="warn">{{.LastError}}</span>{{end}}
</div>
<table><thead><tr><th>Received</th><th>From</th><th>Subject</th><th>Decision</th><th>Score</th><th>Why</th></tr></thead><tbody>
{{range .Rows}}<tr>
<td class="received">{{.Received}}</td>
<td>{{.Sender}}</td>
<td class="subject">{{if .Link}}<a href="{{.Link}}">{{.Subject}}</a>{{else}}{{.Subject}}{{end}}</td>
<td><span class="tag {{.Decision}}">{{.Decision}}</span></td>
<td>{{.Score}}</td>
<td class="why">{{.Why}}</td>
</tr>{{else}}<tr><td colspan="6">No mail ingested yet.</td></tr>{{end}}
</tbody></table>
</body></html>`))
