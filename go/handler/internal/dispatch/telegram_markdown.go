package dispatch

import (
	"fmt"
	"html"
	"net/url"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// renderTelegramMarkdown translates CommonMark into Telegram's HTML subset.
// Raw HTML is displayed literally, never passed through as executable markup.
// Unsupported block structures become readable text rather than unsupported tags.
func renderTelegramMarkdown(markdown string) string {
	source := []byte(strings.ReplaceAll(markdown, "\r\n", "\n"))
	document := goldmark.DefaultParser().Parse(text.NewReader(source))
	var out strings.Builder
	var render func(ast.Node)
	children := func(n ast.Node) {
		for child := n.FirstChild(); child != nil; child = child.NextSibling() {
			render(child)
		}
	}
	escape := func(value []byte) { out.WriteString(html.EscapeString(string(value))) }
	decode := func(value []byte) []byte {
		return util.ResolveEntityNames(util.ResolveNumericReferences(util.UnescapePunctuations(value)))
	}
	render = func(n ast.Node) {
		switch node := n.(type) {
		case *ast.Text:
			if node.IsRaw() {
				escape(node.Segment.Value(source))
			} else {
				escape(decode(node.Segment.Value(source)))
			}
			if node.SoftLineBreak() || node.HardLineBreak() {
				out.WriteByte('\n')
			}
		case *ast.String:
			escape(node.Value)
		case *ast.Emphasis:
			tag := "i"
			if node.Level == 2 {
				tag = "b"
			}
			out.WriteString("<" + tag + ">")
			children(n)
			out.WriteString("</" + tag + ">")
		case *ast.CodeSpan:
			// Telegram forbids code entities nested in other entities. Retain
			// the enclosing emphasis/link and display its code as literal text.
			wrap := true
			for parent := n.Parent(); parent != nil; parent = parent.Parent() {
				switch parent.(type) {
				case *ast.Emphasis, *ast.Link, *ast.Heading:
					wrap = false
				}
			}
			if wrap {
				out.WriteString("<code>")
			}
			// CommonMark code spans replace line endings with spaces.
			for child := n.FirstChild(); child != nil; child = child.NextSibling() {
				value := child.(*ast.Text)
				escape([]byte(strings.ReplaceAll(string(value.Segment.Value(source)), "\n", " ")))
			}
			if wrap {
				out.WriteString("</code>")
			}
		case *ast.FencedCodeBlock, *ast.CodeBlock:
			out.WriteString("<pre>")
			for i := 0; i < n.Lines().Len(); i++ {
				line := n.Lines().At(i)
				escape(line.Value(source))
			}
			out.WriteString("</pre>\n\n")
		case *ast.Link:
			destination := string(decode(node.Destination))
			parsed, err := url.Parse(destination)
			if err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http" || parsed.Scheme == "mailto" || parsed.Scheme == "tg") {
				out.WriteString(`<a href="` + html.EscapeString(destination) + `">`)
				children(n)
				out.WriteString("</a>")
			} else {
				children(n)
				out.WriteString(" (")
				escape([]byte(destination))
				out.WriteByte(')')
			}
		case *ast.AutoLink:
			escape(node.Label(source))
		case *ast.Image:
			children(n)
			out.WriteString(" (")
			escape(node.Destination)
			out.WriteByte(')')
		case *ast.RawHTML:
			for i := 0; i < node.Segments.Len(); i++ {
				segment := node.Segments.At(i)
				escape(segment.Value(source))
			}
		case *ast.HTMLBlock:
			for i := 0; i < n.Lines().Len(); i++ {
				line := n.Lines().At(i)
				escape(line.Value(source))
			}
			if node.HasClosure() {
				escape(node.ClosureLine.Value(source))
			}
			out.WriteByte('\n')
		case *ast.Heading:
			out.WriteString("<b>")
			children(n)
			out.WriteString("</b>\n\n")
		case *ast.ListItem:
			list := n.Parent().(*ast.List)
			if list.IsOrdered() {
				index := list.Start
				for prev := n.PreviousSibling(); prev != nil; prev = prev.PreviousSibling() {
					index++
				}
				fmt.Fprintf(&out, "%d. ", index)
			} else {
				out.WriteString("• ")
			}
			children(n)
			out.WriteByte('\n')
		case *ast.Paragraph:
			children(n)
			out.WriteString("\n\n")
		case *ast.List:
			children(n)
			out.WriteByte('\n')
		case *ast.ThematicBreak:
			out.WriteString("———\n\n")
		default:
			children(n)
		}
	}
	render(document)
	return strings.TrimRight(out.String(), "\n")
}
