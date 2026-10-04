$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Wheelhouse = Join-Path $Root "wheelhouse"

# Prefer an explicit override, otherwise discover a usable Python command.
if ($env:PYTHON) {
    $Python = $env:PYTHON
} elseif (Get-Command py -ErrorAction SilentlyContinue) {
    $Python = "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $Python = "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $Python = "python3"
} else {
    throw "Python was not found. Install Python 3 or set `$env:PYTHON to the Python executable."
}

New-Item -ItemType Directory -Force -Path $Wheelhouse | Out-Null
Get-ChildItem -Path $Wheelhouse -Filter "*.whl" -ErrorAction SilentlyContinue | Remove-Item -Force
Remove-Item -Force -ErrorAction SilentlyContinue (Join-Path $Wheelhouse "SHA256SUMS")

# Download every release pin explicitly. This is deliberate: dependency markers can
# differ between the Windows build machine (for example Python 3.13) and the Linux
# target (Python 3.11/3.12). The offline wheelhouse must contain the complete pinned
# set for all supported target interpreters, not only the dependencies selected for
# the Python version running this script.
$Pinned = @(
    Get-Content (Join-Path $Root "constraints.txt") |
    ForEach-Object { ($_ -split '#', 2)[0].Trim() } |
    Where-Object { $_ -match '^[A-Za-z0-9_.-]+==[^\s]+$' }
)
if ($Pinned.Count -eq 0) { throw "No pinned packages found in constraints.txt" }

Write-Host "Downloading the release's complete pinned, binary-only wheel set..."
& $Python -m pip download `
    --disable-pip-version-check `
    --only-binary=:all: `
    --no-deps `
    --dest $Wheelhouse `
    @Pinned
if ($LASTEXITCODE -ne 0) { throw "pip download failed" }

$Wheels = @(Get-ChildItem -Path $Wheelhouse -Filter "*.whl" | Sort-Object Name)
if ($Wheels.Count -eq 0) { throw "No wheel files were downloaded" }

# These dependencies are intentionally platform-independent. Refuse a platform-specific
# wheel so the GitHub repository remains installable on Linux/Python 3.11+.
$Bad = @($Wheels | Where-Object { $_.Name -notmatch '-py3-none-any\.whl$' })
if ($Bad.Count -gt 0) {
    $Names = ($Bad | ForEach-Object Name) -join ", "
    throw "Non-portable wheel(s) downloaded: $Names"
}

if ($Wheels.Count -ne $Pinned.Count) {
    throw "Expected $($Pinned.Count) wheels from constraints.txt, got $($Wheels.Count)."
}

$ExpectedFile = Join-Path $Wheelhouse "EXPECTED_SHA256SUMS"
if (-not (Test-Path $ExpectedFile)) { throw "wheelhouse/EXPECTED_SHA256SUMS is missing from the release" }
$Expected = @{}
foreach ($Line in Get-Content $ExpectedFile) {
    if ($Line -match '^([0-9a-fA-F]{64})\s+(.+\.whl)$') {
        $Expected[$Matches[2]] = $Matches[1].ToLowerInvariant()
    }
}
if ($Expected.Count -ne $Wheels.Count) {
    throw "Release hash manifest contains $($Expected.Count) wheels, but $($Wheels.Count) were downloaded"
}
foreach ($Wheel in $Wheels) {
    if (-not $Expected.ContainsKey($Wheel.Name)) { throw "Unexpected wheel: $($Wheel.Name)" }
    $Hash = (Get-FileHash -Algorithm SHA256 -Path $Wheel.FullName).Hash.ToLowerInvariant()
    if ($Hash -ne $Expected[$Wheel.Name]) { throw "SHA-256 mismatch for $($Wheel.Name)" }
}
Copy-Item -Force $ExpectedFile (Join-Path $Wheelhouse "SHA256SUMS")

Write-Host "All wheels match the release SHA-256 manifest."
Write-Host ""
Write-Host "Wheelhouse ready:"
$Wheels | ForEach-Object { Write-Host "  $($_.Name)" }
Write-Host "  SHA256SUMS"
Write-Host ""
Write-Host "Next: git add .; git commit; git push"
