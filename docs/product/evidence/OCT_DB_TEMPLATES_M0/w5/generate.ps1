param(
    [Parameter(Mandatory = $true)][string]$OctRepo,
    [string]$Output = "generated\generated.go"
)

$ErrorActionPreference = "Stop"
$evidenceRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$tempBase = [IO.Path]::GetTempPath()
$assemblyRoot = Join-Path $tempBase ("oct-db-templates-m0-" + [Guid]::NewGuid().ToString("N"))
$resolvedOutput = Join-Path $evidenceRoot $Output

try {
    $mainDir = New-Item -ItemType Directory -Path (Join-Path $assemblyRoot "Main") -Force
    $catalogDir = New-Item -ItemType Directory -Path (Join-Path $assemblyRoot "DatabaseTemplates") -Force
    $contractsDir = New-Item -ItemType Directory -Path (Join-Path $assemblyRoot "DatabaseTemplateContracts") -Force
    Copy-Item -LiteralPath (Join-Path $evidenceRoot "query.template.oct") -Destination (Join-Path $mainDir.FullName "query.template.oct")
    Copy-Item -Path (Join-Path $OctRepo "Libraries\DatabaseTemplates\*.template.oct") -Destination $catalogDir.FullName
    Copy-Item -LiteralPath (Join-Path $OctRepo "Libraries\DatabaseTemplateContracts\DatabaseTemplateContracts.oct") -Destination $contractsDir.FullName
    New-Item -ItemType Directory -Path (Split-Path -Parent $resolvedOutput) -Force | Out-Null
    Push-Location $OctRepo
    try {
        go run ./Experiments/PerfM4 (Join-Path $mainDir.FullName "query.template.oct") $resolvedOutput
        if ($LASTEXITCODE -ne 0) {
            throw "Oct generator failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    $resolvedAssembly = [IO.Path]::GetFullPath($assemblyRoot)
    $resolvedTemp = [IO.Path]::GetFullPath($tempBase)
    if ($resolvedAssembly.StartsWith($resolvedTemp, [StringComparison]::OrdinalIgnoreCase) -and (Test-Path -LiteralPath $resolvedAssembly)) {
        Remove-Item -LiteralPath $resolvedAssembly -Recurse -Force
    }
}
