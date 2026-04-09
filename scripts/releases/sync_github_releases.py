#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from release_history import ReleaseEvent, list_release_events, normalize_skipped_versions


def run_git(*args: str, capture_output: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=capture_output,
        text=capture_output,
    )
    return result.stdout if capture_output else ""


def git_tag_target(tag_name: str) -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag_name}^{{commit}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def create_and_push_tag(tag_name: str, commit: str, version: str) -> None:
    run_git("tag", "-a", tag_name, commit, "-m", f"Release {tag_name} for Collector {version}", capture_output=False)
    run_git("push", "origin", f"refs/tags/{tag_name}", capture_output=False)


def materialize_asset(event: ReleaseEvent, target_dir: Path) -> Path:
    asset_name = Path(event.asset_path).name
    asset_path = target_dir / asset_name

    with asset_path.open("wb") as file_handle:
        subprocess.run(["git", "show", f"{event.commit}:{event.asset_path}"], check=True, stdout=file_handle)

    return asset_path


class GitHubReleasesClient:
    def __init__(self, repository: str, token: str) -> None:
        self.repository = repository
        self.token = token
        self.api_base = f"https://api.github.com/repos/{repository}"

    def _request(self, url: str, method: str = "GET", payload: dict | None = None, data: bytes | None = None) -> dict:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        body: bytes | None = data
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(url, headers=headers, data=body, method=method)

        try:
            with urllib.request.urlopen(request) as response:
                response_text = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"GitHub API {method} {url} failed: {error.code} {detail}") from error

        if not response_text:
            return {}

        return json.loads(response_text)

    def get_release_by_tag(self, tag_name: str) -> dict | None:
        url = f"{self.api_base}/releases/tags/{urllib.parse.quote(tag_name)}"
        try:
            return self._request(url)
        except RuntimeError as error:
            if " 404 " in str(error):
                return None
            raise

    def create_release(self, tag_name: str, commit: str, version: str) -> dict:
        url = f"{self.api_base}/releases"
        payload = {
            "tag_name": tag_name,
            "target_commitish": commit,
            "name": f"Collector {version}",
            "body": f"Automated release for Collector {version}.",
            "draft": False,
            "prerelease": False,
        }
        return self._request(url, method="POST", payload=payload)

    def upload_asset_if_missing(self, release: dict, asset_path: Path) -> None:
        asset_name = asset_path.name
        existing_assets = {asset["name"]: asset for asset in release.get("assets", [])}

        if asset_name in existing_assets:
            existing_size = existing_assets[asset_name].get("size")
            local_size = asset_path.stat().st_size
            if existing_size != local_size:
                raise RuntimeError(
                    f"release asset {asset_name} already exists with size {existing_size}, expected {local_size}"
                )
            print(f"Asset {asset_name} already exists on release {release['tag_name']}, skipping upload.")
            return

        upload_url_template = release["upload_url"]
        upload_url = upload_url_template.split("{", 1)[0]
        content_type = mimetypes.guess_type(asset_name)[0] or "application/octet-stream"
        query = urllib.parse.urlencode({"name": asset_name})
        target_url = f"{upload_url}?{query}"

        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": content_type,
            "X-GitHub-Api-Version": "2022-11-28",
        }

        with asset_path.open("rb") as file_handle:
            data = file_handle.read()

        request = urllib.request.Request(target_url, headers=headers, data=data, method="POST")
        try:
            with urllib.request.urlopen(request):
                pass
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"asset upload failed for {asset_name}: {error.code} {detail}") from error


def sync_release(event: ReleaseEvent, client: GitHubReleasesClient | None, dry_run: bool) -> None:
    tag_name = f"v{event.version}"
    existing_tag_target = git_tag_target(tag_name)

    if existing_tag_target is None:
        if dry_run:
            print(f"[dry-run] Would create tag {tag_name} at {event.commit}.")
        else:
            print(f"Creating tag {tag_name} at {event.commit}.")
            create_and_push_tag(tag_name, event.commit, event.version)
    elif existing_tag_target != event.commit:
        raise RuntimeError(
            f"tag {tag_name} already points to {existing_tag_target}, expected {event.commit} for version {event.version}"
        )
    else:
        print(f"Tag {tag_name} already points to {event.commit}, reusing it.")

    if dry_run:
        print(f"[dry-run] Would ensure release {tag_name} exists and upload {Path(event.asset_path).name}.")
        return

    assert client is not None
    release = client.get_release_by_tag(tag_name)
    if release is None:
        print(f"Creating GitHub release {tag_name}.")
        release = client.create_release(tag_name, event.commit, event.version)
    else:
        print(f"GitHub release {tag_name} already exists, reusing it.")

    with tempfile.TemporaryDirectory(prefix="collector-release-") as temp_dir:
        asset_path = materialize_asset(event, Path(temp_dir))
        client.upload_asset_if_missing(release, asset_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Collector git tags and GitHub releases from git history.")
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--range", dest="rev_range", help="Git revision range to inspect, for example A..B.")
    selection.add_argument("--all", action="store_true", help="Process the full repository history reachable from HEAD.")
    parser.add_argument("--dry-run", action="store_true", help="Print the intended work without changing git or GitHub.")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"), help="GitHub repository in owner/name form.")
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

    if not events:
        print(f"No Collector release events found in {rev_spec}.")
        return 0

    client: GitHubReleasesClient | None = None
    if not args.dry_run:
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if not args.repo:
            raise RuntimeError("--repo is required outside dry-run mode.")
        if not token:
            raise RuntimeError("GITHUB_TOKEN or GH_TOKEN must be set outside dry-run mode.")
        client = GitHubReleasesClient(args.repo, token)

    for event in events:
        sync_release(event, client, args.dry_run)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
