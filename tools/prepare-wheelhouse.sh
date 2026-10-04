#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
WHEELHOUSE="$ROOT/wheelhouse"
PYTHON_BIN="${PYTHON_BIN:-python3}"
mkdir -p "$WHEELHOUSE"
rm -f -- "$WHEELHOUSE"/*.whl "$WHEELHOUSE/SHA256SUMS"

"$PYTHON_BIN" -m pip download \
  --disable-pip-version-check \
  --only-binary=:all: \
  --dest "$WHEELHOUSE" \
  -r "$ROOT/requirements.txt" \
  -c "$ROOT/constraints.txt"

mapfile -t wheels < <(find "$WHEELHOUSE" -maxdepth 1 -type f -name '*.whl' -printf '%f\n' | sort)
((${#wheels[@]} > 0)) || { echo 'No wheel files downloaded' >&2; exit 1; }
for wheel in "${wheels[@]}"; do
  [[ "$wheel" == *-py3-none-any.whl ]] || { echo "Non-portable wheel: $wheel" >&2; exit 1; }
done
expected=$(grep -Ec '^[A-Za-z0-9_.-]+==' "$ROOT/constraints.txt")
[[ ${#wheels[@]} -eq $expected ]] || { echo "Expected $expected wheels, got ${#wheels[@]}" >&2; exit 1; }
[[ -s "$WHEELHOUSE/EXPECTED_SHA256SUMS" ]] || { echo 'EXPECTED_SHA256SUMS missing from release' >&2; exit 1; }
(
  cd "$WHEELHOUSE"
  sha256sum -c EXPECTED_SHA256SUMS
  cp -f EXPECTED_SHA256SUMS SHA256SUMS
)
printf '\nAll wheels match the release SHA-256 manifest.\nWheelhouse ready. Commit wheelhouse/*.whl and wheelhouse/SHA256SUMS to GitHub.\n'
