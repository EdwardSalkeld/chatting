package personalinbox

import (
	"context"
	"testing"
)

func TestPerformHealthReflectsFailedDelivery(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	broker.result(message.ID, Classification{Category: "order", Summary: "Order", Confidence: .99})
	performer.failKind = "telegram"
	if err := service.Cycle(ctx); err == nil {
		t.Fatal("expected uncertain delivery error")
	}
	health, err := service.Store.stepHealth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := service.Store.effects(ctx, "1=1")
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 1 || effects[0].Status != "uncertain" {
		t.Fatalf("unexpected effects: %+v", effects)
	}
	if health["perform"]["error"] == "" {
		t.Fatalf("failed delivery reported as healthy: %+v", health["perform"])
	}
}
