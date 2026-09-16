#!/usr/bin/env python3
"""Fail if skills/BUILD_KIT.md Targets LawnDart X.Y != MinVer major.minor."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KIT = ROOT / "skills" / "BUILD_KIT.md"
CSPROJ = ROOT / "src" / "LawnDart" / "LawnDart.csproj"
TARGETS = re.compile(r"^Targets LawnDart (\d+\.\d+)\s*$", re.M)


def main() -> int:
    text = KIT.read_text(encoding="utf-8")
    match = TARGETS.search(text)
    if match is None:
        print("skills/BUILD_KIT.md must contain a line: Targets LawnDart X.Y", file=sys.stderr)
        return 1

    declared = match.group(1)
    major, minor = declared.split(".")

    result = subprocess.run(
        [
            "dotnet",
            "msbuild",
            str(CSPROJ),
            "-t:MinVer",
            "-getProperty:MinVerVersion",
            "-p:MinVerVerbosity=quiet",
            "-nologo",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        return result.returncode

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        print("MinVer produced no version.", file=sys.stderr)
        return 1

    version = lines[-1]
    parsed = re.match(r"^(\d+)\.(\d+)", version)
    if parsed is None:
        print(f"MinVer produced an unparseable version: {version!r}", file=sys.stderr)
        return 1

    if (parsed.group(1), parsed.group(2)) != (major, minor):
        print(
            f"Build kit targets LawnDart {declared} but MinVer is {version}. "
            "Update the Targets line in skills/BUILD_KIT.md.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: Targets LawnDart {declared} matches MinVer {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
