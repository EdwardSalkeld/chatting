import random

import pytest

from app.worker.speculative_quota import Job, choose_pair, eligible


NOW = 1_000_000


def snapshot(used=89, reset=NOW + 100):
    return {
        "ordinary_usage_allowed": True,
        "windows": [
            {"duration_minutes": 10080, "used_percent": used, "resets_at": reset}
        ],
    }


def test_only_fresh_weekly_reading_in_window_is_eligible():
    assert eligible(snapshot(), NOW, NOW)
    assert not eligible(snapshot(used=90), NOW, NOW)
    assert not eligible(snapshot(reset=NOW), NOW, NOW)
    assert not eligible(snapshot(reset=NOW + 6 * 3600), NOW, NOW)
    assert not eligible(snapshot(), NOW - 181, NOW)
    assert not eligible(snapshot(), NOW + 1, NOW)
    assert not eligible(
        {"ordinary_usage_allowed": False, "windows": snapshot()["windows"]}, NOW, NOW
    )
    assert not eligible(
        {"ordinary_usage_allowed": None, "windows": snapshot()["windows"]}, NOW, NOW
    )
    assert not eligible({"ordinary_usage_allowed": True, "windows": []}, NOW, NOW)
    assert not eligible(snapshot(used=float("nan")), NOW, NOW)


def test_prefers_untried_pairs_then_oldest():
    jobs = [Job("security", "audit", "high"), Job("cleanup", "clean", "high")]
    assert (
        choose_pair(jobs, ["a"], [("security", "a")], rng=random.Random(1))[0].name
        == "cleanup"
    )
    assert (
        choose_pair(
            jobs, ["a"], [("security", "a"), ("cleanup", "a")], rng=random.Random(1)
        )[0].name
        == "security"
    )
    with pytest.raises(ValueError):
        choose_pair([], ["a"], [], rng=random.Random(1))
