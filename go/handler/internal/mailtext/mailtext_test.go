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

func TestImageOffersSurviveWithoutLoadingResources(t *testing.T) {
	text := HTMLToText(`<div><a href="https://tracking.invalid/click"><img
		src="https://tracking.invalid/banner.jpg" alt="Free shipping over £25 &amp; 15% off > £50"></a>
		<p>22–25 October</p><img src="https://tracking.invalid/pixel.gif" alt=""></div>`)
	if text != "Free shipping over £25 & 15% off > £50\n22–25 October" {
		t.Fatalf("offer lost or tracking attributes leaked: %q", text)
	}
}

func TestHiddenPreheadersAndPaddingDoNotConsumeTheExcerpt(t *testing.T) {
	text := HTMLToText(`<div style="display: none !important">Preview only</div>
		<div hidden><p>Hidden offer</p></div><span aria-hidden="true">Not for reading</span>
		<p>News` + strings.Repeat("\u034f\u00ad\u200b", 1000) + `</p>
		<table><tr><td>£25 off</td><td>Until Sunday</td></tr></table>`)
	if text != "News\n£25 off\nUntil Sunday" {
		t.Fatalf("hidden material or layout noise survived: %q", text)
	}
}

func TestMalformedNewsletterHTMLStillRenders(t *testing.T) {
	text := HTMLToText(`<p>First offer<p><b>Second &amp; final offer<img alt='£10 off' src=x>`)
	for _, want := range []string{"First offer", "Second & final offer", "£10 off"} {
		if !strings.Contains(text, want) {
			t.Fatalf("%q missing from %q", want, text)
		}
	}
}
