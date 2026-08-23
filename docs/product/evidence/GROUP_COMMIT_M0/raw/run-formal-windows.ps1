param([switch]$Released)
$ErrorActionPreference = 'Stop'

$sourceRepo = 'C:\Users\yuech\source\repos\yuechen-li-dev\OctetDB'
$harness = Join-Path $sourceRepo 'docs\product\evidence\PERF_M4\harness'
$mode = if ($Released) { 'v0.2.0' } else { 'current' }
$outputDir = Join-Path $sourceRepo "docs\product\evidence\GROUP_COMMIT_M0\raw\windows-$mode"
$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$work = Join-Path $tempRoot ('octetdb-group-formal-' + [guid]::NewGuid().ToString('N'))
$resolvedWork = [IO.Path]::GetFullPath($work)
if (-not $resolvedWork.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "temporary work directory escaped the system temp root"
}

New-Item -ItemType Directory -Path $work, (Join-Path $work 'data'), $outputDir -Force | Out-Null
try {
    Copy-Item -LiteralPath (Join-Path $harness 'go.mod') -Destination (Join-Path $harness 'current-local.mod')
    Copy-Item -LiteralPath (Join-Path $harness 'go.sum') -Destination (Join-Path $harness 'current-local.sum')
    if (-not $Released) {
        & go mod edit -modfile (Join-Path $harness 'current-local.mod') -replace "github.com/yuechen-li-dev/octetdb=$sourceRepo"
        if ($LASTEXITCODE -ne 0) { throw 'go mod edit failed' }
    }
    Push-Location $harness
    try {
        & go build -modfile (Join-Path $harness 'current-local.mod') -o (Join-Path $work 'perf-m4.exe') .
        if ($LASTEXITCODE -ne 0) { throw 'harness build failed' }
    }
    finally { Pop-Location }
    foreach ($workload in 'w1', 'w2', 'w3', 'w4') {
        foreach ($concurrency in 1, 8, 32) {
            & (Join-Path $work 'perf-m4.exe') -lane default -workload $workload -population 1000 -operations 384 -concurrency $concurrency -warmup 32 -data (Join-Path $work "data\$workload-c$concurrency") -output (Join-Path $outputDir "$workload-c$concurrency.json")
            if ($LASTEXITCODE -ne 0) { throw "$workload c$concurrency failed" }
        }
    }
}
finally {
    Remove-Item -LiteralPath (Join-Path $harness 'current-local.mod'), (Join-Path $harness 'current-local.sum') -Force -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $resolvedWork) {
        Remove-Item -LiteralPath $resolvedWork -Recurse -Force
    }
}
