param(
    [Parameter(Mandatory = $true)][string]$OctRepo,
    [Parameter(Mandatory = $true)][string]$Source,
    [ValidateSet("interpreted", "compiled")][string]$Execution = "interpreted"
)

$ErrorActionPreference = "Stop"
$tempBase = [IO.Path]::GetTempPath()
$assemblyRoot = Join-Path $tempBase ("oct-db-template-app-" + [Guid]::NewGuid().ToString("N"))

try {
    $mainDir = New-Item -ItemType Directory -Path (Join-Path $assemblyRoot "Main") -Force
    $catalogDir = New-Item -ItemType Directory -Path (Join-Path $assemblyRoot "DatabaseTemplates") -Force
    $contractsDir = New-Item -ItemType Directory -Path (Join-Path $assemblyRoot "DatabaseTemplateContracts") -Force
    Copy-Item -LiteralPath ([IO.Path]::GetFullPath($Source)) -Destination (Join-Path $mainDir.FullName "application.octest")
    Copy-Item -Path (Join-Path $OctRepo "Libraries\DatabaseTemplates\*.template.oct") -Destination $catalogDir.FullName
    Copy-Item -LiteralPath (Join-Path $OctRepo "Libraries\DatabaseTemplateContracts\DatabaseTemplateContracts.oct") -Destination $contractsDir.FullName
    Push-Location $OctRepo
    try {
        go run ./cmd/oct test $assemblyRoot --execution $Execution --json
        if ($LASTEXITCODE -ne 0) {
            throw "Oct test failed with exit code $LASTEXITCODE"
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
