package dispatch

import "testing"

func TestRenderTelegramMarkdown(t *testing.T) {
	tests := []struct{ name, input, want string }{
		{"screening reply", "**Number 24!** Your schedule has **Sheep in the Box**, tonight at **20:15**, Vue Screen 7. That counts both animation programmes as one screening each—we dropped their review drafts, but they still count as screenings.", "<b>Number 24!</b> Your schedule has <b>Sheep in the Box</b>, tonight at <b>20:15</b>, Vue Screen 7. That counts both animation programmes as one screening each—we dropped their review drafts, but they still count as screenings."},
		{"link", "[details](https://example.com/?a=1&b=2)", `<a href="https://example.com/?a=1&amp;b=2">details</a>`},
		{"nested emphasis", "**bold and _italic_**", "<b>bold and <i>italic</i></b>"},
		{"punctuation", "Price £5.50 (today)! A_B_C & 2 < 3.", "Price £5.50 (today)! A_B_C &amp; 2 &lt; 3."},
		{"paragraphs", "one\nline\n\nnext", "one\nline\n\nnext"},
		{"code", "`C:\\new\\test & <tag>`", `<code>C:\new\test &amp; &lt;tag&gt;</code>`},
		{"fenced code", "```text\nx := \"\\n\"\n<tag>\n```", "<pre>x := &#34;\\n&#34;\n&lt;tag&gt;\n</pre>"},
		{"code within emphasis", "**`name` matters**", "<b>name matters</b>"},
		{"multiline code span", "`one\ntwo`", "<code>one two</code>"},
		{"raw html", "Hello <b>literal</b>", "Hello &lt;b&gt;literal&lt;/b&gt;"},
		{"html block", "<script>alert(1)</script>", "&lt;script&gt;alert(1)&lt;/script&gt;"},
		{"unsafe link", "[click](javascript:alert%281%29)", "click (javascript:alert%281%29)"},
		{"heading and list", "# Title\n\n- One\n- Two", "<b>Title</b>\n\n• One\n• Two"},
		{"ordered list", "3. Three\n4. Four", "3. Three\n4. Four"},
		{"escaped markup", `\*literal\*`, "*literal*"},
		{"entities", "&copy; &#169; &amp;", "© © &amp;"},
		{"malformed markup", "**unfinished [link", "**unfinished [link"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := renderTelegramMarkdown(tt.input); got != tt.want {
				t.Fatalf("got %q; want %q", got, tt.want)
			}
		})
	}
}
