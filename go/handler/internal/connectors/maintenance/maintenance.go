// Package maintenance schedules built-in database work using the cron connector.
package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/schedule"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const Target = "database-maintenance"

type Source struct{ Job schedule.Job }

func (source Source) ActiveSchedules(context.Context) ([]schedule.Scheduled, error) {
	return []schedule.Scheduled{{ScheduleID: Target, Job: source.Job}}, nil
}

type Connector struct {
	cron    *schedule.Connector
	cleanup func(context.Context) error
	pending []contracts.TaskEnvelope
}

func New(expression string, days int, cleanup func(context.Context) error, now schedule.NowFunc) (*Connector, error) {
	if days <= 0 || cleanup == nil {
		return nil, errors.New("positive retention days and cleanup callback are required")
	}
	content, _ := json.Marshal(map[string]int{"retention_days": days})
	job := schedule.Job{
		// Local is Go's process/server timezone, derived from TZ or /etc/localtime.
		JobName: Target, Content: string(content), Cron: expression, TimezoneName: "Local",
		ReplyChannelType: "internal", ReplyChannelTarget: Target,
	}
	if err := schedule.Validate(job); err != nil {
		return nil, err
	}
	connector, err := schedule.NewFromSource(Source{Job: job}, now)
	if err != nil {
		return nil, err
	}
	return &Connector{cron: connector, cleanup: cleanup}, nil
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(connector.pending) == 0 {
		due, err := connector.cron.Poll(ctx)
		if err != nil {
			return nil, err
		}
		connector.pending = due
	}
	if len(connector.pending) == 0 {
		return nil, nil
	}
	// Retry failed local cleanup on the next poll without stopping ingress.
	// Never pass cleanup work to a model or open the worker's database here.
	started := time.Now()
	if err := connector.cleanup(ctx); err != nil {
		log.Printf("handler_database_maintenance_failed err=%v", err)
		return nil, nil
	}
	log.Printf("handler_database_maintenance_completed duration=%s", time.Since(started))
	result := connector.pending
	connector.pending = nil
	actor := "message-handler"
	for i := range result {
		result[i].Source = "internal"
		result[i].Actor = &actor
	}
	return result, nil
}
