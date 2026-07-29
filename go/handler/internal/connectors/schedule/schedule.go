package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/robfig/cron/v3"
)

const Source = "cron"

var (
	allowedJobKeys = map[string]bool{
		"job_name":             true,
		"content":              true,
		"context_refs":         true,
		"cron":                 true,
		"prompt_context":       true,
		"timezone":             true,
		"reply_channel_type":   true,
		"reply_channel_target": true,
	}
	requiredJobKeys = map[string]bool{
		"content":  true,
		"job_name": true,
	}
	cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
)

type NowFunc func() time.Time

type Job struct {
	JobName            string
	Content            string
	ContextRefs        []string
	Cron               string
	PromptContext      []string
	TimezoneName       string
	ReplyChannelType   string
	ReplyChannelTarget string
	schedule           cron.Schedule
	location           *time.Location
}

// Scheduled pairs a stable ScheduleID with its Job. The ScheduleID (not the
// mutable job_name) keys the connector's in-memory scheduling state so that
// edits which rename a job keep their next-run timing.
type Scheduled struct {
	ScheduleID string
	Job        Job
}

// ScheduleSource yields the active schedule set on each Poll, allowing the
// connector to live-reload from an authoritative store rather than a static
// snapshot loaded once at startup.
type ScheduleSource interface {
	ActiveSchedules(ctx context.Context) ([]Scheduled, error)
}

type scheduleState struct {
	job       Job
	cron      string
	nextRunAt time.Time
}

type Connector struct {
	source              ScheduleSource
	globalPromptContext []string
	sourcePromptContext []string
	now                 NowFunc
	states              map[string]*scheduleState
}

// staticSource serves a fixed schedule set, preserving the pre-DB behaviour of
// New where jobs are validated once and never change.
type staticSource struct {
	scheduled []Scheduled
}

func (source staticSource) ActiveSchedules(context.Context) ([]Scheduled, error) {
	return source.scheduled, nil
}

// New builds a connector over a fixed set of jobs. Jobs are validated up front;
// each is keyed by its job_name for scheduling-state continuity.
func New(jobs []Job, globalPromptContext []string, sourcePromptContext []string, now NowFunc) (*Connector, error) {
	scheduled := make([]Scheduled, 0, len(jobs))
	for _, job := range jobs {
		prepared, err := prepareJob(job)
		if err != nil {
			return nil, err
		}
		scheduled = append(scheduled, Scheduled{ScheduleID: prepared.JobName, Job: prepared})
	}
	return NewFromSource(staticSource{scheduled: scheduled}, globalPromptContext, sourcePromptContext, now)
}

// NewFromSource builds a connector that reloads its active schedule set from
// source on every Poll.
func NewFromSource(source ScheduleSource, globalPromptContext []string, sourcePromptContext []string, now NowFunc) (*Connector, error) {
	if source == nil {
		return nil, errors.New("schedule source is required")
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Connector{
		source:              source,
		globalPromptContext: append([]string{}, globalPromptContext...),
		sourcePromptContext: append([]string{}, sourcePromptContext...),
		now:                 now,
		states:              map[string]*scheduleState{},
	}, nil
}

func LoadJobs(path string) ([]Job, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("schedule file path must not be empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return LoadJobsJSON(raw)
}

func LoadJobsJSON(raw []byte) ([]Job, error) {
	var payload []map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, errors.New("schedule file must contain a JSON array")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, errors.New("schedule file must contain a single JSON array")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}

	jobs := make([]Job, 0, len(payload))
	for index, rawJob := range payload {
		unknownKeys := make([]string, 0)
		for key := range rawJob {
			if !allowedJobKeys[key] {
				unknownKeys = append(unknownKeys, key)
			}
		}
		if len(unknownKeys) > 0 {
			sort.Strings(unknownKeys)
			return nil, fmt.Errorf("schedule job at index %d contains unknown keys: %s", index, strings.Join(unknownKeys, ", "))
		}
		missingKeys := make([]string, 0)
		for key := range requiredJobKeys {
			if _, ok := rawJob[key]; !ok {
				missingKeys = append(missingKeys, key)
			}
		}
		if len(missingKeys) > 0 {
			sort.Strings(missingKeys)
			return nil, fmt.Errorf("schedule job at index %d is missing required keys: %s", index, strings.Join(missingKeys, ", "))
		}

		jobName, err := decodeNonEmptyString(rawJob["job_name"], fmt.Sprintf("schedule job at index %d job_name", index))
		if err != nil {
			return nil, err
		}
		content, err := decodeNonEmptyString(rawJob["content"], fmt.Sprintf("schedule job at index %d content", index))
		if err != nil {
			return nil, err
		}
		rawCron, ok := rawJob["cron"]
		if !ok {
			return nil, fmt.Errorf("schedule job at index %d cron must be a non-empty string", index)
		}
		cronExpression, err := decodeNonEmptyString(rawCron, fmt.Sprintf("schedule job at index %d cron", index))
		if err != nil {
			return nil, err
		}
		timezoneName := "UTC"
		if rawTimezone, ok := rawJob["timezone"]; ok && !isNull(rawTimezone) {
			timezoneName, err = decodeNonEmptyString(rawTimezone, fmt.Sprintf("schedule job at index %d timezone", index))
			if err != nil {
				return nil, err
			}
		}
		contextRefs, err := decodeStringListOptional(rawJob, "context_refs", fmt.Sprintf("schedule job at index %d context_refs", index))
		if err != nil {
			return nil, err
		}
		promptContext, err := decodeStringListOptional(rawJob, "prompt_context", fmt.Sprintf("schedule job at index %d prompt_context", index))
		if err != nil {
			return nil, err
		}
		replyChannelType, err := decodeOptionalNonEmptyString(rawJob, "reply_channel_type", fmt.Sprintf("schedule job at index %d reply_channel_type", index))
		if err != nil {
			return nil, err
		}
		replyChannelTarget, err := decodeOptionalNonEmptyString(rawJob, "reply_channel_target", fmt.Sprintf("schedule job at index %d reply_channel_target", index))
		if err != nil {
			return nil, err
		}
		if (replyChannelType == "") != (replyChannelTarget == "") {
			return nil, fmt.Errorf("schedule job at index %d reply_channel_type and reply_channel_target must be provided together", index)
		}

		job, err := prepareJob(Job{
			JobName:            strings.TrimSpace(jobName),
			Content:            strings.TrimSpace(content),
			ContextRefs:        contextRefs,
			Cron:               strings.TrimSpace(cronExpression),
			PromptContext:      promptContext,
			TimezoneName:       strings.TrimSpace(timezoneName),
			ReplyChannelType:   strings.TrimSpace(replyChannelType),
			ReplyChannelTarget: strings.TrimSpace(replyChannelTarget),
		})
		if err != nil {
			return nil, fmt.Errorf("schedule job at index %d %w", index, err)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scheduled, err := connector.source.ActiveSchedules(ctx)
	if err != nil {
		return nil, err
	}
	now := connector.now().UTC()
	envelopes := []contracts.TaskEnvelope{}
	seen := make(map[string]bool, len(scheduled))
	for _, entry := range scheduled {
		job, err := prepareJob(entry.Job)
		if err != nil {
			log.Printf("schedule_skip_invalid schedule_id=%q job_name=%q err=%v", entry.ScheduleID, entry.Job.JobName, err)
			continue
		}
		seen[entry.ScheduleID] = true
		state, ok := connector.states[entry.ScheduleID]
		switch {
		case !ok:
			// A newly active schedule initialises its next run from now.
			state = &scheduleState{cron: job.Cron, nextRunAt: initialNextRunAt(job, now)}
			connector.states[entry.ScheduleID] = state
		case state.cron != job.Cron:
			// A changed cron expression recomputes the next run from now.
			state.cron = job.Cron
			state.nextRunAt = initialNextRunAt(job, now)
		}
		// Refresh the job so non-cron edits (content, reply channel, refs) take
		// effect on the next fire without disturbing the schedule timing.
		state.job = job
		if now.Before(state.nextRunAt) {
			continue
		}
		nextRunAt := state.nextRunAt
		eventID := "cron:" + job.JobName + ":" + pythonUTCISO(nextRunAt)
		replyChannel := contracts.ReplyChannel{Type: "log", Target: job.JobName}
		if job.ReplyChannelType != "" {
			replyChannel = contracts.ReplyChannel{
				Type:   job.ReplyChannelType,
				Target: job.ReplyChannelTarget,
			}
		}
		prompt := &contracts.PromptContext{
			GlobalInstructions: append([]string{}, connector.globalPromptContext...),
			SourceInstructions: append([]string{}, connector.sourcePromptContext...),
			TaskInstructions:   append([]string{}, job.PromptContext...),
		}
		envelopes = append(envelopes, contracts.TaskEnvelope{
			SchemaVersion: contracts.SchemaVersion,
			ID:            eventID,
			Source:        Source,
			ReceivedAt:    contracts.NewTimestamp(now),
			Actor:         nil,
			Content:       job.Content,
			Attachments:   []contracts.AttachmentRef{},
			ContextRefs:   append([]string{}, job.ContextRefs...),
			PromptContext: prompt,
			ReplyChannel:  replyChannel,
			DedupeKey:     eventID,
		})
		state.nextRunAt = nextDueTime(job, now)
	}
	// Drop scheduling state for schedules that are no longer active.
	for scheduleID := range connector.states {
		if !seen[scheduleID] {
			delete(connector.states, scheduleID)
		}
	}
	return envelopes, nil
}

// Validate reports whether the job satisfies the schedule connector's requirements
// without exposing the internal prepared job representation.
func Validate(job Job) error {
	_, err := prepareJob(job)
	return err
}

func prepareJob(job Job) (Job, error) {
	if strings.TrimSpace(job.JobName) == "" {
		return Job{}, errors.New("job_name is required")
	}
	if strings.TrimSpace(job.Content) == "" {
		return Job{}, errors.New("content is required")
	}
	if strings.TrimSpace(job.Cron) == "" {
		return Job{}, errors.New("cron must be non-empty when provided")
	}
	locationName := strings.TrimSpace(job.TimezoneName)
	if locationName == "" {
		locationName = "UTC"
	}
	location, err := time.LoadLocation(locationName)
	if err != nil {
		return Job{}, fmt.Errorf("invalid timezone: %s", locationName)
	}
	parsedSchedule, err := parseCron(job.Cron)
	if err != nil {
		return Job{}, err
	}
	job.JobName = strings.TrimSpace(job.JobName)
	job.Content = strings.TrimSpace(job.Content)
	job.Cron = strings.TrimSpace(job.Cron)
	job.TimezoneName = locationName
	job.ContextRefs = append([]string{}, job.ContextRefs...)
	job.PromptContext = append([]string{}, job.PromptContext...)
	job.schedule = parsedSchedule
	job.location = location
	return job, nil
}

func parseCron(expression string) (cron.Schedule, error) {
	if len(strings.Fields(expression)) != 5 {
		return nil, errors.New("cron must contain exactly 5 fields")
	}
	parsed, err := cronParser.Parse(expression)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}
	return parsed, nil
}

func initialNextRunAt(job Job, now time.Time) time.Time {
	localRef := now.UTC().In(job.location)
	truncated := localRef.Truncate(time.Minute)
	if matches(job, truncated) {
		return truncated.UTC()
	}
	return job.schedule.Next(localRef).UTC()
}

func nextDueTime(job Job, now time.Time) time.Time {
	return job.schedule.Next(now.UTC().In(job.location)).UTC()
}

func matches(job Job, candidate time.Time) bool {
	before := candidate.Add(-time.Minute)
	return job.schedule.Next(before).Equal(candidate)
}

func pythonUTCISO(value time.Time) string {
	return strings.TrimSuffix(value.UTC().Format(time.RFC3339Nano), "Z") + "+00:00"
}

func decodeNonEmptyString(raw json.RawMessage, name string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a non-empty string", name)
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s must be a non-empty string", name)
	}
	return value, nil
}

func decodeOptionalNonEmptyString(rawJob map[string]json.RawMessage, key string, name string) (string, error) {
	raw, ok := rawJob[key]
	if !ok || isNull(raw) {
		return "", nil
	}
	return decodeNonEmptyString(raw, name)
}

func decodeStringListOptional(rawJob map[string]json.RawMessage, key string, name string) ([]string, error) {
	raw, ok := rawJob[key]
	if !ok || isNull(raw) {
		return []string{}, nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("%s must be a list of non-empty strings", name)
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("%s must be a list of non-empty strings", name)
		}
	}
	if values == nil {
		return []string{}, nil
	}
	return values, nil
}

func isNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}
