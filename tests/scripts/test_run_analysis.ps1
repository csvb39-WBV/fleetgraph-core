$ErrorActionPreference = "Stop"

$global:Calls = @()

function global:python {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    $global:Calls += ,@($Arguments)
}

$scriptPath = "$PSScriptRoot\..\..\scripts\run_analysis.ps1"
$scriptContent = Get-Content $scriptPath -Raw

& $scriptPath "input.json"

if ($global:Calls.Count -ne 2) {
    throw "Expected 2 python calls, got $($global:Calls.Count)."
}

if (($global:Calls[0] -join " ") -ne "-m fleetgraph_core.cli.run input.json --output output.json") {
    throw "CLI call did not match expected invocation."
}

if (($global:Calls[1] -join " ") -ne "-m fleetgraph_core.export.csv_exporter output.json .") {
    throw "CSV exporter call was not made."
}

if (($global:Calls[1] -join " ") -match "fleetgraph_core\.export\.json_exporter") {
    throw "JSON exporter call was made."
}

if ($scriptContent -match "json\.loads") {
    throw "json.loads pattern was found in the script."
}

if ($scriptContent -match "json\.load") {
    throw "json.load pattern was found in the script."
}

if ($scriptContent -match "read_text") {
    throw "read_text pattern was found in the script."
}

if ($scriptContent -match "open\(") {
    throw "open( pattern was found in the script."
}
