param(
    [string]$ConfigPath = ""
)

$RepoRoot = Split-Path -Parent $PSScriptRoot
$CliPath = Join-Path $RepoRoot "src\fleetgraph\runtime\cli_entrypoint.py"
$PythonCommand = "python"

if ($ConfigPath -ne "") {
    & $PythonCommand $CliPath --config $ConfigPath
}
else {
    & $PythonCommand $CliPath
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "FleetGraph signal pipeline failed."
    exit $LASTEXITCODE
}

Write-Host "FleetGraph signal pipeline completed successfully."
exit 0
