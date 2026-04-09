#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath


VERSIONED_DMG_PATTERN = re.compile(r"^Collector-v(?P<version>\d+\.\d+\.\d+)\.dmg$")


@dataclass(frozen=True)
class ReleaseEvent:
    commit: str
    version: str
    asset_path: str
    asset_blob: str


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def extract_version(path: str) -> str | None:
    match = VERSIONED_DMG_PATTERN.match(PurePosixPath(path).name)
    return match.group("version") if match else None


def list_commits(rev_spec: str) -> list[str]:
    stdout = run_git("rev-list", "--reverse", rev_spec)
    return [line for line in stdout.splitlines() if line]


def release_event_for_commit(commit: str) -> ReleaseEvent | None:
    stdout = run_git("diff-tree", "--root", "--name-status", "-M", "--no-commit-id", "-r", commit)
    matches: list[tuple[str, str]] = []

    for line in stdout.splitlines():
        parts = line.split("\t")
        status = parts[0]

        if status.startswith("R") and len(parts) == 3:
            candidate_path = parts[2]
        elif status == "A" and len(parts) == 2:
            candidate_path = parts[1]
        else:
            continue

        version = extract_version(candidate_path)
        if version:
            matches.append((version, candidate_path))

    if not matches:
        return None

    if len(matches) > 1:
        joined = ", ".join(path for _, path in matches)
        raise RuntimeError(f"commit {commit} introduces multiple release DMGs: {joined}")

    version, asset_path = matches[0]
    asset_blob = run_git("rev-parse", f"{commit}:{asset_path}").strip()
    return ReleaseEvent(commit=commit, version=version, asset_path=asset_path, asset_blob=asset_blob)


def normalize_skipped_versions(raw_values: list[str]) -> set[str]:
    normalized: set[str] = set()

    for raw_value in raw_values:
        for piece in raw_value.split(","):
            version = piece.strip()
            if version:
                normalized.add(version)

    return normalized


def list_release_events(rev_spec: str, skipped_versions: set[str] | None = None) -> list[ReleaseEvent]:
    unique_events: list[ReleaseEvent] = []
    seen_versions: dict[str, ReleaseEvent] = {}
    skipped_versions = skipped_versions or set()

    for commit in list_commits(rev_spec):
        event = release_event_for_commit(commit)
        if event is None:
            continue
        if event.version in skipped_versions:
            continue

        previous = seen_versions.get(event.version)
        if previous is None:
            seen_versions[event.version] = event
            unique_events.append(event)
            continue

        if previous.asset_blob != event.asset_blob:
            raise RuntimeError(
                "version "
                f"{event.version} appears multiple times with different binaries: "
                f"{previous.commit} and {event.commit}"
            )

    return unique_events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect Collector release events from git history.")
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--range", dest="rev_range", help="Git revision range to inspect, for example A..B.")
    selection.add_argument("--all", action="store_true", help="Inspect the full repository history reachable from HEAD.")
    parser.add_argument(
        "--skip-version",
        action="append",
        default=[],
        help="Version to skip. Can be repeated or passed as a comma-separated list.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rev_spec = args.rev_range if args.rev_range else "HEAD"
    skipped_versions = normalize_skipped_versions(args.skip_version)
    events = list_release_events(rev_spec, skipped_versions=skipped_versions)
    print(json.dumps([asdict(event) for event in events], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
