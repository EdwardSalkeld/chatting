package reminders

import (
	"encoding/json"
	"html/template"
	"net/http"
	"strings"
	"time"

	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const uiRoutePrefix = "/reminders"

func RegisterUIRoutes(mux *http.ServeMux, service *Service) {
	mux.HandleFunc(uiRoutePrefix, service.handleUIList)
	mux.HandleFunc(uiRoutePrefix+"/", service.handleUIDetail)
}

type reminderFormView struct {
	IsNew              bool
	ReminderID         string
	RunAt              string
	Prompt             string
	ContextRefs        string
	PromptContext      string
	ReplyChannelType   string
	ReplyChannelTarget string
	ReplyMetadata      string
}

type reminderListView struct {
	Scheduled []reminderJSON
	History   []reminderJSON
}

func (service *Service) handleUIList(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	all, err := service.store.ListReminders(request.Context(), "all")
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	view := reminderListView{Scheduled: []reminderJSON{}, History: []reminderJSON{}}
	for _, record := range all {
		if record.Status == sqlitestate.ReminderStatusScheduled {
			view.Scheduled = append(view.Scheduled, reminderToJSON(record))
		} else {
			view.History = append(view.History, reminderToJSON(record))
		}
	}
	renderReminderTemplate(writer, reminderListTemplate, view)
}

func (service *Service) handleUIDetail(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reminderID := strings.TrimPrefix(request.URL.Path, uiRoutePrefix+"/")
	if reminderID == "" || strings.Contains(reminderID, "/") {
		http.NotFound(writer, request)
		return
	}
	if reminderID == "new" {
		renderReminderTemplate(writer, reminderFormTemplate, reminderFormView{IsNew: true, ReplyMetadata: "{}"})
		return
	}
	record, err := service.store.GetLatestReminder(request.Context(), reminderID)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	if record == nil || record.Status != sqlitestate.ReminderStatusScheduled {
		http.NotFound(writer, request)
		return
	}
	renderReminderTemplate(writer, reminderFormTemplate, reminderFormView{
		ReminderID: record.ReminderID, RunAt: record.RunAt.UTC().Format(time.RFC3339), Prompt: record.Prompt,
		ContextRefs: strings.Join(record.ContextRefs, "\n"), PromptContext: strings.Join(record.PromptContext, "\n"),
		ReplyChannelType: record.ReplyChannelType, ReplyChannelTarget: record.ReplyChannelTarget,
		ReplyMetadata: metadataJSON(record.ReplyChannelMetadata),
	})
}

func metadataJSON(metadata map[string]any) string {
	if len(metadata) == 0 {
		return "{}"
	}
	raw, err := jsonMarshalIndent(metadata)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

var jsonMarshalIndent = func(value any) ([]byte, error) {
	return json.MarshalIndent(value, "", "  ")
}

func renderReminderTemplate(writer http.ResponseWriter, tmpl *template.Template, data any) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(writer, data); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

const reminderPageStyle = `
	:root { color-scheme: light dark; } body { font-family: system-ui, sans-serif; max-width: 60rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }
	h1, h2 { font-size: 1.4rem; } h2 { margin-top: 2rem; } a { color: #2563eb; } table { border-collapse: collapse; width: 100%; }
	th, td { text-align: left; padding: .4rem .6rem; border-bottom: 1px solid #8883; vertical-align: top; } form { display: grid; gap: .9rem; margin-top: 1rem; }
	label { display: grid; gap: .25rem; font-weight: 600; } input, textarea { font: inherit; padding: .4rem; width: 100%; box-sizing: border-box; }
	textarea { min-height: 4rem; } .hint { font-weight: 400; color: #6b7280; font-size: .85rem; } .actions { display: flex; gap: .6rem; }
	button { font: inherit; padding: .45rem .9rem; cursor: pointer; } button.danger, #error { color: #b91c1c; } .new { display: inline-block; margin-bottom: 1rem; }
`

var reminderListTemplate = template.Must(template.New("reminder-list").Parse(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Reminders</title><style>` + reminderPageStyle + `</style></head><body>
<h1>Reminders</h1><p><a href="/schedules">Recurring schedules</a></p><a class="new" href="/reminders/new">+ New reminder</a>
<h2>Upcoming</h2><table><thead><tr><th>When (UTC)</th><th>Prompt</th><th>Reply target</th></tr></thead><tbody>{{range .Scheduled}}<tr><td><a href="/reminders/{{.ReminderID}}">{{.RunAt}}</a></td><td>{{.Prompt}}</td><td>{{.ReplyChannel.Type}}:{{.ReplyChannel.Target}}</td></tr>{{else}}<tr><td colspan="3">No scheduled reminders.</td></tr>{{end}}</tbody></table>
<h2>History</h2><table><thead><tr><th>When (UTC)</th><th>Status</th><th>Prompt</th></tr></thead><tbody>{{range .History}}<tr><td>{{.RunAt}}</td><td>{{.Status}}</td><td>{{.Prompt}}</td></tr>{{else}}<tr><td colspan="3">No reminder history.</td></tr>{{end}}</tbody></table>
</body></html>`))

var reminderFormTemplate = template.Must(template.New("reminder-form").Parse(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{{if .IsNew}}New reminder{{else}}Edit reminder{{end}}</title><style>` + reminderPageStyle + `</style></head><body>
<a href="/reminders">&larr; All reminders</a><h1>{{if .IsNew}}New reminder{{else}}Edit reminder{{end}}</h1>
<form id="reminder-form" data-reminder-id="{{.ReminderID}}" data-is-new="{{if .IsNew}}1{{else}}0{{end}}">
<label>Run at <span class="hint">shown in your browser timezone</span><input type="datetime-local" name="run_at" data-run-at="{{.RunAt}}" required></label>
<label>Prompt<textarea name="prompt" required>{{.Prompt}}</textarea></label>
<label>Reply channel type<input name="reply_channel_type" value="{{.ReplyChannelType}}" placeholder="telegram" required></label>
<label>Reply channel target<input name="reply_channel_target" value="{{.ReplyChannelTarget}}" required></label>
<label>Reply metadata <span class="hint">JSON object</span><textarea name="reply_metadata">{{.ReplyMetadata}}</textarea></label>
<label>Context refs <span class="hint">one per line</span><textarea name="context_refs">{{.ContextRefs}}</textarea></label>
<label>Prompt context <span class="hint">one per line</span><textarea name="prompt_context">{{.PromptContext}}</textarea></label>
<div class="actions"><button type="submit">Save</button>{{if not .IsNew}}<button type="button" class="danger" id="cancel">Cancel reminder</button>{{end}}</div><div id="error"></div></form>
<script>
const form=document.getElementById("reminder-form"), errorBox=document.getElementById("error"), apiBase="/api/reminders";
const runAtInput=form.elements.run_at;if(runAtInput.dataset.runAt){const date=new Date(runAtInput.dataset.runAt),local=new Date(date.getTime()-date.getTimezoneOffset()*60000);runAtInput.value=local.toISOString().slice(0,16);}
const lines=v=>v.split("\n").map(x=>x.trim()).filter(Boolean);
function payload(){const data=new FormData(form);let metadata={};try{metadata=JSON.parse(data.get("reply_metadata")||"{}");}catch(e){throw new Error("Reply metadata must be valid JSON.");}return {run_at:new Date(data.get("run_at")).toISOString(),prompt:(data.get("prompt")||"").trim(),reply_channel:{type:(data.get("reply_channel_type")||"").trim(),target:(data.get("reply_channel_target")||"").trim(),metadata},context_refs:lines(data.get("context_refs")||""),prompt_context:lines(data.get("prompt_context")||""),created_from_task_id:"ui",created_by:"ui",idempotency_key:"ui:"+crypto.randomUUID()};}
async function readError(response){try{const body=await response.json();return body.error||("request failed with status "+response.status);}catch(e){return "request failed with status "+response.status;}}
form.addEventListener("submit",async event=>{event.preventDefault();errorBox.textContent="";let body;try{body=payload();}catch(e){errorBox.textContent=e.message;return;}const isNew=form.dataset.isNew==="1", url=isNew?apiBase:apiBase+"/"+encodeURIComponent(form.dataset.reminderId);const response=await fetch(url,{method:isNew?"POST":"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});if(response.ok){location.href="/reminders";return;}errorBox.textContent=await readError(response);});
const cancelButton=document.getElementById("cancel");if(cancelButton)cancelButton.addEventListener("click",async()=>{if(!confirm("Cancel this reminder?"))return;const response=await fetch(apiBase+"/"+encodeURIComponent(form.dataset.reminderId),{method:"DELETE"});if(response.ok){location.href="/reminders";return;}errorBox.textContent=await readError(response);});
</script></body></html>`))
