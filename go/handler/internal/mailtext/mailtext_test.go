package mailtext

import "strings"

import "testing"

func TestStyleAndScriptBodiesAreDropped(t *testing.T) {
	// A real marketing email is mostly CSS; tag stripping alone leaves all of
	// it behind as text, which is what the model then reads instead of content.
	raw := `<!DOCTYPE html><html><head><title>Ignore me</title>
		<style type="text/css">.btn{color:#fff;background:#000} @media only screen {.x{display:none}}</style>
		</head><body><!-- tracking --><div><p>50% off print orders</p><p>Until 31 October</p></div>
		<script>var t=1;</script></body></html>`
	text := HTMLToText(raw)
	for _, unwanted := range []string{"color:#fff", "@media", "var t=1", "Ignore me", "tracking"} {
		if strings.Contains(text, unwanted) {
			t.Fatalf("%q survived:\n%s", unwanted, text)
		}
	}
	if !strings.Contains(text, "50% off print orders") || !strings.Contains(text, "Until 31 October") {
		t.Fatalf("content lost:\n%s", text)
	}
}

func TestEntitiesAndBlocksBecomeReadableText(t *testing.T) {
	text := HTMLToText(`<p>Order&nbsp;#10195 &amp; shipped</p><br><div>Thanks</div>`)
	if text != "Order #10195 & shipped\nThanks" {
		t.Fatalf("%q", text)
	}
	if HTMLToText("   ") != "" {
		t.Fatal("blank input should give blank output")
	}
}

func TestLooksLikeHTMLSeparatesMarkupFromProse(t *testing.T) {
	if !LooksLikeHTML(`<!DOCTYPE html><html lang="en">`) || !LooksLikeHTML(`<div class="x">hi</div>`) {
		t.Fatal("markup not recognised")
	}
	if LooksLikeHTML("Your order is confirmed. 2 < 3 and 5 > 4.") {
		t.Fatal("prose with comparison operators treated as markup")
	}
}
