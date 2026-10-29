// Package mailtext renders email HTML as the plain text a model should read.
package mailtext

import (
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

var (
	// Non-breaking and zero-width characters are spacing hacks in marketing
	// HTML. They survive unescaping and reach the model as noise.
	zeroWidthPattern = regexp.MustCompile(`[\x{00ad}\x{034f}\x{200b}\x{200c}\x{200d}\x{2060}\x{feff}]`)
	spacePattern     = regexp.MustCompile(`[ \t\x{00a0}\x{2007}\x{202f}]+`)
	htmlLikePattern  = regexp.MustCompile(`(?i)<\s*(!doctype|html|head|body|div|table|td|tr|span|p|br|a|img|style|meta)\b`)
	urlPattern       = regexp.MustCompile(`(?i)https?://[^\s<>()]+`)
)

// Compact removes tracking-link bulk before excerpting. Keep short useful URLs
// and the hostname of long links, but never fetch anything. Idempotent so older
// stored bodies can be compacted again when building a digest.
func Compact(value string) string {
	value = urlPattern.ReplaceAllStringFunc(value, func(raw string) string {
		if len([]rune(raw)) <= 120 {
			return raw
		}
		host := "invalid"
		if parsed, err := url.Parse(raw); err == nil && parsed.Hostname() != "" {
			host = parsed.Hostname()
		}
		return "[link: " + host + "]"
	})
	lines := []string{}
	for _, line := range strings.Split(value, "\n") {
		line = strings.Join(strings.Fields(line), " ")
		if line != "" && line != "(" && line != ")" {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

// LooksLikeHTML reports whether a body is markup rather than prose. Email parts
// are not always labelled accurately, so the content is what decides.
func LooksLikeHTML(value string) bool {
	return htmlLikePattern.MatchString(value)
}

// HTMLToText renders the supplied markup without executing it or fetching any
// resources. Image alt text is email content too: offers often exist only in
// a banner. Hidden preheaders, scripts, CSS and tracking URLs are not content.
func HTMLToText(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	doc, err := html.Parse(strings.NewReader(value))
	if err != nil {
		return ""
	}
	var rendered strings.Builder
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			rendered.WriteString(node.Data)
			return
		}
		block := false
		if node.Type == html.ElementNode {
			switch node.Data {
			case "head", "script", "style", "title", "template":
				return
			}
			for _, attr := range node.Attr {
				style := strings.Join(strings.Fields(strings.ToLower(attr.Val)), "")
				if attr.Key == "hidden" || (attr.Key == "aria-hidden" && strings.EqualFold(attr.Val, "true")) ||
					(attr.Key == "style" && (strings.Contains(style, "display:none") || strings.Contains(style, "visibility:hidden"))) {
					return
				}
			}
			switch node.Data {
			case "img":
				for _, attr := range node.Attr {
					if attr.Key == "alt" && strings.TrimSpace(attr.Val) != "" {
						rendered.WriteString(" " + attr.Val + " ")
					}
				}
			case "br", "p", "div", "li", "tr", "td", "th", "table", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article", "blockquote":
				block = true
				rendered.WriteByte('\n')
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
		if block {
			rendered.WriteByte('\n')
		}
	}
	walk(doc)
	text := rendered.String()
	text = strings.ReplaceAll(text, "\r", "\n")
	text = zeroWidthPattern.ReplaceAllString(text, "")
	text = spacePattern.ReplaceAllString(text, " ")
	lines := strings.Split(text, "\n")
	trimmed := make([]string, 0, len(lines))
	for _, line := range lines {
		if line = strings.TrimSpace(line); line != "" {
			trimmed = append(trimmed, line)
		}
	}
	return strings.TrimSpace(strings.Join(trimmed, "\n"))
}
