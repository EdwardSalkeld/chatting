import unittest

from app.model_command import models_from_environment


class ModelConfigurationTests(unittest.TestCase):
    def test_defaults_and_independent_overrides(self):
        defaults = {
            "high": "gpt-6.1-sol",
            "low": "gpt-6-luna",
            "speculative": "gpt-6.2-sol",
            "speculative_deep": "gpt-6.2-astra",
        }
        self.assertEqual(models_from_environment({}), defaults)
        self.assertEqual(
            models_from_environment({"CHATTING_MODEL_HIGH": "gpt-6.1-terra"}),
            {**defaults, "high": "gpt-6.1-terra"},
        )
        self.assertEqual(
            models_from_environment({"CHATTING_MODEL_SPECULATIVE_DEEP": "custom-deep"})[
                "speculative_deep"
            ],
            "custom-deep",
        )
        self.assertEqual(
            models_from_environment({"CHATTING_MODEL_LOW": "custom-low"})["low"],
            "custom-low",
        )

    def test_blank_or_multiple_model_ids_fail(self):
        for name in (
            "CHATTING_MODEL_HIGH",
            "CHATTING_MODEL_LOW",
            "CHATTING_MODEL_SPECULATIVE",
            "CHATTING_MODEL_SPECULATIVE_DEEP",
        ):
            for value in ("", "  ", "model one", "model\none"):
                with (
                    self.subTest(name=name, value=value),
                    self.assertRaisesRegex(ValueError, name),
                ):
                    models_from_environment({name: value})
