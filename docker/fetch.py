"""Download an archive and verify its SHA-512 before writing it. Usage: fetch.py URL SHA512 DEST.

The slim base image has no curl; Python's standard library is enough. archive.apache.org drops
connections under load, so each attempt restarts the download.
"""

import hashlib
import sys
import time
import urllib.request

ATTEMPTS = 5


def fetch(url: str, expected: str, dest: str) -> None:
    for attempt in range(1, ATTEMPTS + 1):
        digest = hashlib.sha512()
        try:
            with urllib.request.urlopen(url, timeout=60) as resp, open(dest, "wb") as out:
                while chunk := resp.read(1 << 20):
                    digest.update(chunk)
                    out.write(chunk)
        except OSError as err:
            print(f"attempt {attempt}/{ATTEMPTS} failed: {err}", file=sys.stderr)
            time.sleep(5 * attempt)
            continue
        if digest.hexdigest() != expected.lower():
            sys.exit(f"SHA-512 mismatch for {url}: got {digest.hexdigest()}")
        print(f"verified {url}")
        return
    sys.exit(f"could not download {url}")


if __name__ == "__main__":
    fetch(*sys.argv[1:4])
