"""Usage: fetch.py PATH SHA512 DEST. Download PATH from the first Apache mirror that serves it with
that SHA-512. The slim base image has no curl.

The CDN keeps only the current releases (it has Hadoop 3.3.6, not Spark 3.5.3); the archive keeps
every release and can be slow (27 min for Spark 3.5.3 on 2026-11-27)."""

import hashlib
import sys
import time
import urllib.request

MIRRORS = (
    "https://dlcdn.apache.org",
    "https://downloads.apache.org",
    "https://archive.apache.org/dist",
)
ROUNDS = 3


def download(url: str, dest: str) -> str:
    digest = hashlib.sha512()
    with urllib.request.urlopen(url, timeout=60) as resp, open(dest, "wb") as out:
        while chunk := resp.read(1 << 20):
            digest.update(chunk)
            out.write(chunk)
    return digest.hexdigest()


def fetch(path: str, expected: str, dest: str, mirrors=MIRRORS, rounds=ROUNDS, pause=5.0) -> None:
    for round_ in range(1, rounds + 1):
        for mirror in mirrors:
            url = f"{mirror}/{path}"
            try:
                digest = download(url, dest)
            except OSError as err:
                print(f"round {round_}/{rounds}: {url}: {err}", file=sys.stderr)
                continue
            if digest == expected.lower():
                print(f"verified {url}")
                return
            print(f"round {round_}/{rounds}: {url}: SHA-512 {digest}", file=sys.stderr)
        time.sleep(pause * round_)
    sys.exit(f"no mirror served {path} with SHA-512 {expected}")


if __name__ == "__main__":
    fetch(*sys.argv[1:4])
