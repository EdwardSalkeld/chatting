import json
import os
import subprocess
import sys
import unittest

from app.model_command import models_from_environment


class ModelConfigurationTests(unittest.TestCase):
    def test_defaults_and_independent_overrides(self):
        self.assertEqual(
            models_from_environment({}), {"high": "gpt-6.1-sol", "low": "gpt-6-luna"}
        )
        self.assertEqual(
            models_from_environment({"CHATTING_MODEL_HIGH": "gpt-6.1-terra"}),
            {"high": "gpt-6.1-terra", "low": "gpt-6-luna"},
        )
        self.assertEqual(
            models_from_environment({"CHATTING_MODEL_LOW": "custom-low"})["low"],
            "custom-low",
        )

    def test_blank_or_multiple_model_ids_fail(self):
        for name in ("CHATTING_MODEL_HIGH", "CHATTING_MODEL_LOW"):
            for value in ("", "  ", "model one", "model\none"):
                with (
                    self.subTest(name=name, value=value),
                    self.assertRaisesRegex(ValueError, name),
                ):
                    models_from_environment({name: value})

    def test_fresh_process_uses_both_settings(self):
        environment = {
            **os.environ,
            "CHATTING_MODEL_HIGH": "gpt-6.1-terra",
            "CHATTING_MODEL_LOW": "custom-low",
        }
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import json; from app.model_command import MODELS; print(json.dumps(MODELS))",
            ],
            env=environment,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(
            json.loads(result.stdout), {"high": "gpt-6.1-terra", "low": "custom-low"}
        )
