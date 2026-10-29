"""Compact already-rendered mail before applying model excerpt budgets."""

import re
from urllib.parse import urlsplit

VERSION = "mail-text-v1"
URL = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)


def compact(value: str) -> str:
    def shorten(match: re.Match[str]) -> str:
        url = match.group()
        if len(url) <= 120:
            return url
        try:
            host = urlsplit(url).hostname
        except ValueError:
            host = None
        return f"[link: {host or 'invalid'}]"

    value = URL.sub(shorten, value)
    lines = [" ".join(line.split()) for line in value.splitlines()]
    return "\n".join(line for line in lines if line and line not in ("(", ")"))
