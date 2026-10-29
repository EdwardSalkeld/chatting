import unittest

from app.mail_text import compact


class MailTextTests(unittest.TestCase):
    def test_short_links_and_human_instructions_survive(self):
        text = "Pay by Friday at https://example.com/pay\n\nReference: ABC123"
        self.assertEqual(
            compact(text), "Pay by Friday at https://example.com/pay\nReference: ABC123"
        )

    def test_long_urls_are_compacted_without_fetching_or_leaking_tokens(self):
        value = (
            "Read terms (https://clicks.example/"
            + "token" * 100
            + ")\n\n £20 credit\n  £10  off "
        )
        cleaned = compact(value)
        self.assertEqual(
            cleaned, "Read terms ([link: clicks.example])\n£20 credit\n£10 off"
        )
        self.assertEqual(compact(cleaned), cleaned)

    def test_malformed_long_url_is_bounded_without_crashing(self):
        value = "https://[broken/" + "x" * 200
        self.assertEqual(compact(value), "[link: invalid]")
