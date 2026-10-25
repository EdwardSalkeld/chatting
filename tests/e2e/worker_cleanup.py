"""Stop detached supervisor process groups owned by a private test fixture."""

import os
from pathlib import Path
import signal


def stop_test_supervisors(db_path: Path) -> None:
    # Call only after stopping the fixture's manager, so it cannot create slots.
    for marker in Path(f"{db_path}.supervisors").glob("supervisor-*.lock"):
        try:
            os.killpg(int(marker.read_text()), signal.SIGKILL)
        except FileNotFoundError, ProcessLookupError, ValueError:
            pass
