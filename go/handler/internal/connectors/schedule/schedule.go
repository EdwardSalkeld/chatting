package schedule

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/robfig/cron/v3"
)

const Source = "cron"

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

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
	source ScheduleSource
	now    NowFunc
	states map[string]*scheduleState
}

// NewFromSource builds a connector that reloads its active schedule set from
// source on every Poll.
func NewFromSource(source ScheduleSource, now NowFunc) (*Connector, error) {
	if source == nil {
		return nil, errors.New("schedule source is required")
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Connector{
		source: source,
		now:    now,
		states: map[string]*scheduleState{},
	}, nil
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
		eventID := "cron:" + job.JobName + ":" + contracts.PythonUTCISO(nextRunAt)
		replyChannel := contracts.ReplyChannel{Type: "log", Target: job.JobName}
		if job.ReplyChannelType != "" {
			replyChannel = contracts.ReplyChannel{
				Type:   job.ReplyChannelType,
				Target: job.ReplyChannelTarget,
			}
		}
		prompt := &contracts.PromptContext{
			TaskInstructions: append([]string{}, job.PromptContext...),
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
