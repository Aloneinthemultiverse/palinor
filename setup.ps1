# One-command setup for Windows: checks prerequisites, installs Python packages into .venv, then starts everything.
#   powershell -ExecutionPolicy Bypass -File setup.ps1
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Need($cmd, $hint) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) { Write-Output "Missing: $cmd. $hint"; exit 1 }
}
Need python "Install Python 3.11+ from https://www.python.org/downloads/ (tick 'Add to PATH')."
Need docker "Install Docker Desktop from https://www.docker.com/products/docker-desktop/."
$ver = python -c "import sys; print('%d.%d' % sys.version_info[:2])"
if ([version]$ver -lt [version]"3.11") { Write-Output "Python 3.11+ is required (found $ver)."; exit 1 }

if (-not (Test-Path .venv)) {
    Write-Output "Creating Python environment (.venv)..."
    python -m venv .venv
}
Write-Output "Installing Python packages..."
& .venv\Scripts\python.exe -m pip install --quiet --upgrade pip
& .venv\Scripts\python.exe -m pip install --quiet -r requirements.txt

& "$PSScriptRoot\scripts\start_palinor.ps1"
