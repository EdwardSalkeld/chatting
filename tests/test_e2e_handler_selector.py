import unittest
from pathlib import Path

from tests.e2e.handler_selector import (
    HANDLER_IMPLEMENTATION_ENV,
    message_handler_command,
    selected_handler_implementation,
)


class E2EHandlerSelectorTests(unittest.TestCase):
    def test_default_handler_implementation_is_python(self) -> None:
        self.assertEqual(selected_handler_implementation({}), "python")

    def test_python_handler_command_uses_current_python_entrypoint(self) -> None:
        command = message_handler_command(
            Path("/tmp/handler.json"),
            env={HANDLER_IMPLEMENTATION_ENV: "python"},
            python_executable="/bin/python-test",
        )

        self.assertEqual(
            command,
            [
                "/bin/python-test",
                "-m",
                "app.main_message_handler",
                "--config",
                "/tmp/handler.json",
            ],
        )

    def test_go_selection_fails_clearly_until_e2e_path_exists(self) -> None:
        with self.assertRaisesRegex(
            NotImplementedError,
            "Go handler drop-in E2E path is not implemented yet",
        ):
            message_handler_command(
                Path("/tmp/handler.json"),
                env={HANDLER_IMPLEMENTATION_ENV: "go"},
            )

    def test_unknown_handler_implementation_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, HANDLER_IMPLEMENTATION_ENV):
            selected_handler_implementation({HANDLER_IMPLEMENTATION_ENV: "ruby"})


if __name__ == "__main__":
    unittest.main()
