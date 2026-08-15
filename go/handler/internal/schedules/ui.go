package schedules

import (
	"html/template"
	"net/http"
	"strings"

	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const uiRoutePrefix = "/schedules"

// RegisterUIRoutes wires the server-rendered CRUD UI onto the shared mux. Reads
// are rendered from the store; all mutations go through the JSON API via fetch
// so the UI and agents share a single write path.
func RegisterUIRoutes(mux *http.ServeMux, service *Service) {
	mux.HandleFunc(uiRoutePrefix, service.handleUIList)
	mux.HandleFunc(uiRoutePrefix+"/", service.handleUIDetail)
}

type scheduleFormView struct {
	IsNew              bool
	ScheduleID         string
	JobName            string
	Content            string
	Cron               string
	Timezone           string
	ContextRefs        string
	PromptContext      string
	ReplyChannelType   string
	ReplyChannelTarget string
}

func (service *Service) handleUIList(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	records, err := service.store.ListActiveSchedules(request.Context())
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	renderTemplate(writer, listTemplate, schedulesToJSON(records))
}

func (service *Service) handleUIDetail(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	scheduleID := strings.TrimPrefix(request.URL.Path, uiRoutePrefix+"/")
	if scheduleID == "" || strings.Contains(scheduleID, "/") {
		http.NotFound(writer, request)
		return
	}
	if scheduleID == "new" {
		renderTemplate(writer, formTemplate, scheduleFormView{IsNew: true, Timezone: "UTC"})
		return
	}
	record, err := service.store.GetActiveSchedule(request.Context(), scheduleID)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	if record == nil {
		http.NotFound(writer, request)
		return
	}
	renderTemplate(writer, formTemplate, formViewFromRecord(*record))
}

func formViewFromRecord(record sqlitestate.ScheduleRecord) scheduleFormView {
	return scheduleFormView{
		IsNew:              false,
		ScheduleID:         record.ScheduleID,
		JobName:            record.JobName,
		Content:            record.Content,
		Cron:               record.Cron,
		Timezone:           record.Timezone,
		ContextRefs:        strings.Join(record.ContextRefs, "\n"),
		PromptContext:      strings.Join(record.PromptContext, "\n"),
		ReplyChannelType:   record.ReplyChannelType,
		ReplyChannelTarget: record.ReplyChannelTarget,
	}
}

func renderTemplate(writer http.ResponseWriter, tmpl *template.Template, data any) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(writer, data); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

const pageStyle = `
	:root { color-scheme: light dark; }
	body { font-family: system-ui, sans-serif; max-width: 52rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }
	h1 { font-size: 1.4rem; }
	a { color: #2563eb; }
	table { border-collapse: collapse; width: 100%; }
	th, td { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #8883; vertical-align: top; }
	form { display: grid; gap: 0.9rem; margin-top: 1rem; }
	label { display: grid; gap: 0.25rem; font-weight: 600; }
	input, textarea { font: inherit; padding: 0.4rem; width: 100%; box-sizing: border-box; }
	textarea { min-height: 4rem; }
	.hint { font-weight: 400; color: #6b7280; font-size: 0.85rem; }
	.actions { display: flex; gap: 0.6rem; align-items: center; }
	button { font: inherit; padding: 0.45rem 0.9rem; cursor: pointer; }
	button.danger { color: #b91c1c; }
	.new { display: inline-block; margin-bottom: 1rem; }
	#error { color: #b91c1c; white-space: pre-wrap; }
`

var listTemplate = template.Must(template.New("list").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Schedules</title>
<style>` + pageStyle + `</style>
</head>
<body>
<h1>Schedules</h1>
<p><a href="/reminders">One-off reminders</a></p>
<a class="new" href="/schedules/new">+ New schedule</a>
<table>
<thead><tr><th>Job name</th><th>Cron</th><th>Timezone</th><th>Reply target</th></tr></thead>
<tbody>
{{range .}}
<tr>
<td><a href="/schedules/{{.ScheduleID}}">{{.JobName}}</a></td>
<td><code>{{.Cron}}</code></td>
<td>{{.Timezone}}</td>
<td>{{if .ReplyChannelType}}{{.ReplyChannelType}}:{{.ReplyChannelTarget}}{{else}}&mdash;{{end}}</td>
</tr>
{{else}}
<tr><td colspan="4">No active schedules.</td></tr>
{{end}}
</tbody>
</table>
</body>
</html>`))

var formTemplate = template.Must(template.New("form").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{if .IsNew}}New schedule{{else}}Edit {{.JobName}}{{end}}</title>
<style>` + pageStyle + `</style>
</head>
<body>
<a href="/schedules">&larr; All schedules</a>
<h1>{{if .IsNew}}New schedule{{else}}Edit schedule{{end}}</h1>
<form id="schedule-form" data-schedule-id="{{.ScheduleID}}" data-is-new="{{if .IsNew}}1{{else}}0{{end}}">
<label>Job name
<input name="job_name" value="{{.JobName}}" required>
</label>
<label>Content
<textarea name="content" required>{{.Content}}</textarea>
</label>
<label>Cron <span class="hint">5 fields, e.g. <code>5 7 * * *</code></span>
<input name="cron" value="{{.Cron}}" required>
</label>
<label>Timezone
<input name="timezone" value="{{.Timezone}}" placeholder="UTC">
</label>
<label>Context refs <span class="hint">one per line</span>
<textarea name="context_refs">{{.ContextRefs}}</textarea>
</label>
<label>Prompt context <span class="hint">one per line</span>
<textarea name="prompt_context">{{.PromptContext}}</textarea>
</label>
<label>Reply channel type
<input name="reply_channel_type" value="{{.ReplyChannelType}}" placeholder="telegram">
</label>
<label>Reply channel target
<input name="reply_channel_target" value="{{.ReplyChannelTarget}}">
</label>
<div class="actions">
<button type="submit">Save</button>
{{if not .IsNew}}<button type="button" class="danger" id="delete">Delete</button>{{end}}
</div>
<div id="error"></div>
</form>
<script>
const form = document.getElementById("schedule-form");
const errorBox = document.getElementById("error");
const apiBase = "/api/schedules";

function lines(value) {
	return value.split("\n").map(line => line.trim()).filter(line => line.length > 0);
}

function payload() {
	const data = new FormData(form);
	return {
		job_name: (data.get("job_name") || "").trim(),
		content: (data.get("content") || "").trim(),
		cron: (data.get("cron") || "").trim(),
		timezone: (data.get("timezone") || "").trim(),
		context_refs: lines(data.get("context_refs") || ""),
		prompt_context: lines(data.get("prompt_context") || ""),
		reply_channel_type: (data.get("reply_channel_type") || "").trim(),
		reply_channel_target: (data.get("reply_channel_target") || "").trim(),
		created_by: "ui",
	};
}

async function readError(response) {
	try {
		const body = await response.json();
		return body.error || ("request failed with status " + response.status);
	} catch (e) {
		return "request failed with status " + response.status;
	}
}

form.addEventListener("submit", async (event) => {
	event.preventDefault();
	errorBox.textContent = "";
	const isNew = form.dataset.isNew === "1";
	const scheduleId = form.dataset.scheduleId;
	const response = await fetch(isNew ? apiBase : apiBase + "/" + encodeURIComponent(scheduleId), {
		method: isNew ? "POST" : "PUT",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(payload()),
	});
	if (response.ok) {
		window.location.href = "/schedules";
		return;
	}
	errorBox.textContent = await readError(response);
});

const deleteButton = document.getElementById("delete");
if (deleteButton) {
	deleteButton.addEventListener("click", async () => {
		if (!window.confirm("Delete this schedule?")) {
			return;
		}
		errorBox.textContent = "";
		const response = await fetch(apiBase + "/" + encodeURIComponent(form.dataset.scheduleId), { method: "DELETE" });
		if (response.ok) {
			window.location.href = "/schedules";
			return;
		}
		errorBox.textContent = await readError(response);
	});
}
</script>
</body>
</html>`))
