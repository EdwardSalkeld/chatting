package personalinbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUncertainEffectsListOmitsBodiesAndResolveClearsThem(t *testing.T) {
	service, _, _, _ := fixture(t)
	ctx := context.Background()
	for _, effect := range []Effect{
		{ID: "m1:telegram", MessageID: "m1", Kind: "telegram", Target: "actions", Body: "private summary", Status: "uncertain", Note: "interrupted before durable receipt"},
		{ID: "m2:file", MessageID: "m2", Kind: "file", Target: "Triage/News", Body: "private summary", Status: "done"},
	} {
		raw, _ := json.Marshal(effect)
		if _, err := service.Store.db.ExecContext(ctx, "INSERT INTO inbox_effects(id,data) VALUES (?,?)", effect.ID, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	list := func() []Effect {
		t.Helper()
		response, err := server.Client().Get(server.URL + "/api/personal-inbox/effects/uncertain")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var view struct {
			Effects []Effect `json:"effects"`
		}
		if err := json.NewDecoder(response.Body).Decode(&view); err != nil || response.StatusCode != 200 {
			t.Fatalf("list: %d %v", response.StatusCode, err)
		}
		return view.Effects
	}
	effects := list()
	if len(effects) != 1 || effects[0].ID != "m1:telegram" || effects[0].Body != "" || effects[0].Note == "" {
		t.Fatalf("unexpected uncertain effects: %+v", effects)
	}
	response, err := server.Client().Post(server.URL+"/api/personal-inbox/effects/uncertain", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 405 {
		t.Fatalf("write accepted on list: %d", response.StatusCode)
	}
	response, err = server.Client().Post(server.URL+"/api/personal-inbox/resolve", "application/json", strings.NewReader(`{"id":"m1:telegram","outcome":"done","note":"Alert seen in Telegram"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 || len(list()) != 0 {
		t.Fatalf("resolve did not clear the effect: %d", response.StatusCode)
	}
}
