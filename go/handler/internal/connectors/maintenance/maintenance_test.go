package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCronRunsLocalCleanupAndEmitsBuiltinWorkerTask(t *testing.T) {
	now := time.Date(2026, 11, 5, 3, 59, 0, 0, time.UTC)
	calls := 0
	fail := true
	connector, err := New("0 4 * * *", "UTC", 21, 30, func(context.Context) error {
		calls++
		if fail {
			return errors.New("temporarily busy")
		}
		return nil
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if tasks, err := connector.Poll(ctx); err != nil || len(tasks) != 0 || calls != 0 {
		t.Fatalf("early: %v %v", tasks, err)
	}
	now = now.Add(time.Minute)
	if tasks, err := connector.Poll(ctx); err != nil || len(tasks) != 0 || calls != 1 {
		t.Fatalf("failed cleanup: %v %v", tasks, err)
	}
	fail = false
	now = now.Add(time.Minute)
	tasks, err := connector.Poll(ctx)
	if err != nil || len(tasks) != 1 || calls != 2 {
		t.Fatalf("retry: %v %v", tasks, err)
	}
	task := tasks[0]
	if task.Source != "internal" || task.ReplyChannel.Type != "internal" || task.ReplyChannel.Target != Target {
		t.Fatalf("not builtin: %+v", task)
	}
	var policy map[string]int
	if err := json.Unmarshal([]byte(task.Content), &policy); err != nil {
		t.Fatal(err)
	}
	if policy["retention_days"] != 21 || policy["delivery_retention_days"] != 30 {
		t.Fatal(policy)
	}
	if tasks, err := connector.Poll(ctx); err != nil || len(tasks) != 0 || calls != 2 {
		t.Fatalf("duplicate: %v %v", tasks, err)
	}
	now = now.Add(24 * time.Hour)
	if tasks, err := connector.Poll(ctx); err != nil || len(tasks) != 1 || calls != 3 {
		t.Fatalf("next day: %v %v", tasks, err)
	}
}

func TestInvalidMaintenanceScheduleIsRejected(t *testing.T) {
	cleanup := func(context.Context) error { return nil }
	for _, value := range []struct {
		cron, timezone string
		days           int
	}{
		{"broken", "UTC", 14}, {"0 4 * * *", "No/SuchZone", 14}, {"0 4 * * *", "UTC", 0},
	} {
		if _, err := New(value.cron, value.timezone, value.days, 30, cleanup, nil); err == nil {
			t.Fatalf("accepted %+v", value)
		}
	}
}
