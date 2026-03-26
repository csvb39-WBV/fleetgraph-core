param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$InputPath
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = Join-Path $repoRoot "src"

Push-Location $repoRoot
try {
    $cliOutputPath = "output.json"

    python -m fleetgraph_core.cli.run $InputPath --output $cliOutputPath

    python -m fleetgraph_core.export.csv_exporter $cliOutputPath .
}
finally {
    Pop-Location
}
