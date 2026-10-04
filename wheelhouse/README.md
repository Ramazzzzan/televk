# Offline wheelhouse

`EXPECTED_SHA256SUMS` is the release manifest for the exact wheel files pinned by TeleVK 0.1.1.
It is intentionally committed before the binary files are downloaded.

On a PC with PyPI access run `tools/prepare-wheelhouse.ps1` (Windows) or
`tools/prepare-wheelhouse.sh`. The script downloads only wheels, verifies every downloaded file
against `EXPECTED_SHA256SUMS`, and then writes `SHA256SUMS`. Commit the eight `.whl` files and
`SHA256SUMS` to GitHub.

The HTPC installer uses only this local directory unless `--online` is explicitly requested.
