package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCronRunsLocalCleanupAndEmitsBuiltinWorkerTask(t *testing.T) {
	previous := time.Local
	time.Local = time.UTC
	t.Cleanup(func() { time.Local = previous })
	now := time.Date(2026, 11, 5, 3, 59, 0, 0, time.UTC)
	calls := 0
	fail := true
	connector, err := New("0 4 * * *", 21, func(context.Context) error {
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
	if policy["retention_days"] != 21 || len(policy) != 1 {
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
		cron string
		days int
	}{
		{"broken", 14}, {"0 4 * * *", 0},
	} {
		if _, err := New(value.cron, value.days, cleanup, nil); err == nil {
			t.Fatalf("accepted %+v", value)
		}
	}
}

func TestMaintenanceFollowsGlobalServerTimezoneInWinterAndSummer(t *testing.T) {
	previous := time.Local
	location, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Fatal(err)
	}
	time.Local = location
	t.Cleanup(func() { time.Local = previous })
	for _, date := range []struct {
		month   time.Month
		utcHour int
	}{{time.January, 4}, {time.July, 3}} {
		now := time.Date(2026, date.month, 5, date.utcHour-1, 59, 0, 0, time.UTC)
		calls := 0
		connector, err := New("0 4 * * *", 14, func(context.Context) error { calls++; return nil }, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if tasks, err := connector.Poll(context.Background()); err != nil || len(tasks) != 0 {
			t.Fatalf("before local 04:00: %v %v", tasks, err)
		}
		now = now.Add(time.Minute)
		if tasks, err := connector.Poll(context.Background()); err != nil || len(tasks) != 1 || calls != 1 {
			t.Fatalf("local 04:00 in %s: %v %v", date.month, tasks, err)
		}
	}
}
