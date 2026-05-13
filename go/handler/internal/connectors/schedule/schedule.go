package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

type Connector struct {
	jobs                []Job
	globalPromptContext []string
	sourcePromptContext []string
	now                 NowFunc
	nextRunAtByJob      map[string]time.Time
}

func New(jobs []Job, globalPromptContext []string, sourcePromptContext []string, now NowFunc) (*Connector, error) {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	normalized := make([]Job, 0, len(jobs))
	for _, job := range jobs {
		prepared, err := prepareJob(job)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, prepared)
	}
	return &Connector{
		jobs:                normalized,
		globalPromptContext: append([]string{}, globalPromptContext...),
		sourcePromptContext: append([]string{}, sourcePromptContext...),
		now:                 now,
		nextRunAtByJob:      map[string]time.Time{},
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
	now := connector.now().UTC()
	envelopes := []contracts.TaskEnvelope{}
	for _, job := range connector.jobs {
		nextRunAt, ok := connector.nextRunAtByJob[job.JobName]
		if !ok {
			nextRunAt = initialNextRunAt(job, now)
		}
		if now.Before(nextRunAt) {
			connector.nextRunAtByJob[job.JobName] = nextRunAt
			continue
		}
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
		connector.nextRunAtByJob[job.JobName] = nextDueTime(job, now)
	}
	return envelopes, nil
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
