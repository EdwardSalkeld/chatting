package personalinbox

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPlatformDecisionsShareExistingPrivateUIView(t *testing.T) {
	service, _, _, _ := fixture(t)
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterUIRoutes(mux, service)
	server := httptest.NewServer(mux)
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/api/personal-inbox/decisions")
	if err != nil {
		t.Fatal(err)
	}
	var view decisionsView
	if err := json.NewDecoder(response.Body).Decode(&view); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 || len(view.Rows) != 1 || view.Rows[0].Subject != "Your order" || !view.Rows[0].Unclassified || view.Awaiting != 1 || view.Rows[0].ReceivedAt == "" {
		t.Fatalf("unexpected decisions: %+v", view)
	}
	response, err = server.Client().Get(server.URL + "/personal-inbox")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 200 || !strings.Contains(string(body), "Your order") || !strings.Contains(string(body), "Personal inbox decisions") {
		t.Fatal("existing UI changed")
	}
	request, _ := http.NewRequest("POST", server.URL+"/api/personal-inbox/decisions", nil)
	response, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 405 {
		t.Fatalf("write accepted: %d", response.StatusCode)
	}
}
func TestPlatformDecisionsReturnEmptyArray(t *testing.T) {
	service, _, _, _ := fixture(t)
	request := httptest.NewRequest("GET", "/api/personal-inbox/decisions", nil)
	response := httptest.NewRecorder()
	service.handleUIDecisions(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"rows":[]`) {
		t.Fatalf("unexpected empty view %s", response.Body.String())
	}
}

func TestPlatformDecisionsIncludeClassificationReasonAndMailLink(t *testing.T) {
	service, _, _, _ := fixture(t)
	message := Message{ID: "fixture-mail", ThreadID: "fixture-thread", Created: service.Now().Unix(), Mail: Mail{From: "sender@example.com", Subject: "A decision"}, Classification: &Classification{Category: "review", Confidence: .8, Trace: &Trace{Reason: "Policy override", JevCategory: "action"}}}
	if err := service.Store.insertMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/api/personal-inbox/decisions", nil)
	response := httptest.NewRecorder()
	service.handleUIDecisions(response, request)
	var view decisionsView
	if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Rows) != 1 || view.Rows[0].Decision != "review" || view.Rows[0].Score != "0.80" || !strings.Contains(view.Rows[0].Why, "read as action") || !strings.HasPrefix(view.Rows[0].Link, "https://app.fastmail.com/") {
		t.Fatalf("classification detail lost: %+v", view)
	}
}
