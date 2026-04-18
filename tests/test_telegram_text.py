import unittest

from app.telegram_text import normalize_telegram_outbound_text


class NormalizeTelegramOutboundTextTests(unittest.TestCase):
    def test_normalizes_literal_escape_sequences(self) -> None:
        self.assertEqual(
            normalize_telegram_outbound_text(r"Line 1.\nLine 2.\n\nTabs:\tokay"),
            "Line 1.\nLine 2.\n\nTabs:\tokay",
        )

    def test_normalizes_double_escaped_sequences(self) -> None:
        self.assertEqual(
            normalize_telegram_outbound_text(
                r"Line 1.\\nLine 2.\\n\\nEscaped \\(paren\\)"
            ),
            "Line 1.\nLine 2.\n\nEscaped (paren)",
        )


if __name__ == "__main__":
    unittest.main()
