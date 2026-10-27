"""Download and verify exactly one approved Hub file inside the app container."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

from huggingface_hub import hf_hub_download


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--approved", action="store_true", required=True)
    parser.add_argument("--repo-id", required=True)
    parser.add_argument("--filename", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--size-bytes", type=int, required=True)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()

    stage = Path("/bda/staging")
    cache = stage / ".hf"
    temp = stage / "tmp"
    cache.mkdir(parents=True, exist_ok=True)
    temp.mkdir(parents=True, exist_ok=True)
    os.environ["HF_HOME"] = str(cache)
    os.environ["HF_HUB_CACHE"] = str(cache / "hub")
    os.environ["TMP"] = os.environ["TEMP"] = str(temp)

    downloaded = Path(hf_hub_download(
        repo_id=args.repo_id,
        filename=args.filename,
        repo_type="dataset",
        revision=args.revision,
        local_dir=str(args.output.parent),
        local_dir_use_symlinks=False,
    ))
    if downloaded.stat().st_size != args.size_bytes or sha256(downloaded) != args.sha256:
        raise SystemExit("Hub file size or SHA-256 did not match the approved manifest")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if downloaded.resolve() != args.output.resolve():
        if args.output.exists():
            raise SystemExit(f"Refusing to overwrite existing staged path: {args.output}")
        # hf_hub_download retains repository subdirectories under local_dir.
        # Move the already verified file within the D: staging bind mount to
        # the flat path expected by the resumable PowerShell uploader.
        downloaded.replace(args.output)
    print(f"verified_bytes={args.size_bytes} sha256={args.sha256} output={args.output}")


if __name__ == "__main__":
    main()
