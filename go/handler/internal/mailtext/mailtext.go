// Package mailtext renders email HTML as the plain text a model should read.
package mailtext

import (
	"html"
	"regexp"
	"strings"
)

var (
	// Style and script bodies survive tag stripping and dominate marketing
	// email, so they go first, along with comments. One pattern each: RE2 has
	// no backreference to match the closing tag against the opening one.
	dropContentPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?is)<style\b[^>]*>.*?</\s*style\s*>`),
		regexp.MustCompile(`(?is)<script\b[^>]*>.*?</\s*script\s*>`),
		regexp.MustCompile(`(?is)<title\b[^>]*>.*?</\s*title\s*>`),
	}
	commentPattern    = regexp.MustCompile(`(?s)<!--.*?-->`)
	blockBreakPattern = regexp.MustCompile(`(?i)<\s*(br|/p|/div|/li|/tr|/table|/h[1-6])\b[^>]*>`)
	openBreakPattern  = regexp.MustCompile(`(?i)<\s*(p|div|li|tr|h[1-6])\b[^>]*>`)
	tagPattern        = regexp.MustCompile(`(?s)<[^>]+>`)
	multiBreakPattern = regexp.MustCompile(`\n\s*\n\s*\n+`)
	// Non-breaking and zero-width characters are spacing hacks in marketing
	// HTML. They survive unescaping and reach the model as noise.
	zeroWidthPattern = regexp.MustCompile(`[\x{200b}\x{200c}\x{200d}\x{2060}\x{feff}]`)
	spacePattern     = regexp.MustCompile(`[ \t\x{00a0}\x{2007}\x{202f}]+`)
	htmlLikePattern  = regexp.MustCompile(`(?i)<\s*(!doctype|html|head|body|div|table|td|tr|span|p|br|a|img|style|meta)\b`)
)

// LooksLikeHTML reports whether a body is markup rather than prose. Email parts
// are not always labelled accurately, so the content is what decides.
func LooksLikeHTML(value string) bool {
	return htmlLikePattern.MatchString(value)
}

// HTMLToText strips markup, leaving the words in reading order.
func HTMLToText(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	text := commentPattern.ReplaceAllString(value, " ")
	for _, pattern := range dropContentPatterns {
		text = pattern.ReplaceAllString(text, " ")
	}
	text = blockBreakPattern.ReplaceAllString(text, "\n")
	text = openBreakPattern.ReplaceAllString(text, "\n")
	text = tagPattern.ReplaceAllString(text, "")
	text = html.UnescapeString(text)
	text = strings.ReplaceAll(text, "\r", "\n")
	text = zeroWidthPattern.ReplaceAllString(text, "")
	text = spacePattern.ReplaceAllString(text, " ")
	text = multiBreakPattern.ReplaceAllString(text, "\n\n")
	lines := strings.Split(text, "\n")
	trimmed := make([]string, 0, len(lines))
	for _, line := range lines {
		if line = strings.TrimSpace(line); line != "" {
			trimmed = append(trimmed, line)
		}
	}
	return strings.TrimSpace(strings.Join(trimmed, "\n"))
}
