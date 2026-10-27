"""Resume one bounded byte range of a Hugging Face dataset file to D: staging."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import requests
from huggingface_hub import get_token, hf_hub_url


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--approved", action="store_true", required=True)
    parser.add_argument("--repo-id", required=True)
    parser.add_argument("--filename", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--length", type=int, required=True)
    parser.add_argument("--total-size", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    stage = Path("/bda/staging")
    temp = stage / "tmp"
    temp.mkdir(parents=True, exist_ok=True)
    os.environ["HF_HOME"] = str(stage / ".hf")
    os.environ["HF_HUB_CACHE"] = str(stage / ".hf" / "hub")
    os.environ["TMP"] = os.environ["TEMP"] = str(temp)

    if args.offset < 0 or args.length <= 0 or args.offset + args.length > args.total_size:
        raise SystemExit("Invalid byte range")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    part = args.output.with_suffix(args.output.suffix + ".part")
    have = part.stat().st_size if part.exists() else 0
    if have > args.length:
        raise SystemExit("Existing partial range exceeds its approved length")
    if have == args.length:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        part.replace(args.output)
        print(f"range_verified_bytes={have} offset={args.offset}")
        return

    start = args.offset + have
    end = args.offset + args.length - 1
    headers = {"Range": f"bytes={start}-{end}", "Accept-Encoding": "identity"}
    token = get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = hf_hub_url(args.repo_id, args.filename, repo_type="dataset", revision=args.revision)
    with requests.get(url, headers=headers, stream=True, timeout=(30, 300)) as response:
        if response.status_code != 206:
            raise SystemExit(f"Hub/CDN did not honor byte range (HTTP {response.status_code}); no full-file fallback is allowed")
        content_range = response.headers.get("Content-Range", "")
        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
        if not match or tuple(map(int, match.groups())) != (start, end, args.total_size):
            raise SystemExit(f"Unexpected Content-Range: {content_range!r}")
        with part.open("ab") as output:
            for block in response.iter_content(chunk_size=8 * 1024 * 1024):
                if block:
                    output.write(block)
    if part.stat().st_size != args.length:
        raise SystemExit(f"Incomplete range retained for resume: {part.stat().st_size}/{args.length}")
    part.replace(args.output)
    print(f"range_verified_bytes={args.length} offset={args.offset} output={args.output}")


if __name__ == "__main__":
    main()
