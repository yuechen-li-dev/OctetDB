param(
    [ValidateSet("primary", "query", "dimensions", "all")]
    [string]$Suite = "all",
    [int]$Repetitions = 3
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$harness = Join-Path $root "harness"
$raw = Join-Path $root "raw/windows"
$profiles = Join-Path $root "profiles/windows"
$binary = Join-Path $env:TEMP "octetdb-perf-m4.exe"
$postgresURL = if ($env:PERF_M4_POSTGRES_URL) { $env:PERF_M4_POSTGRES_URL } else { "postgres://dbsched:dbsched@localhost:54329/dbsched?sslmode=disable" }

New-Item -ItemType Directory -Force -Path $raw, $profiles | Out-Null
Push-Location $harness
try {
    go build -trimpath -o $binary .
    if ($LASTEXITCODE -ne 0) { throw "harness build failed" }
} finally {
    Pop-Location
}

function Invoke-Run {
    param(
        [string]$Name,
        [string]$Lane,
        [string]$Workload,
        [int]$Population,
        [int]$Operations,
        [int]$Concurrency,
        [string[]]$Extra = @()
    )
    for ($rep = 1; $rep -le $Repetitions; $rep++) {
        $output = Join-Path $raw "$Name-$Lane-r$rep.json"
        if (Test-Path -LiteralPath $output) { throw "refusing to overwrite $output" }
        $arguments = @(
            "-lane", $Lane, "-workload", $Workload, "-population", $Population,
            "-operations", $Operations, "-concurrency", $Concurrency,
            "-warmup", [Math]::Min(100, [Math]::Max(10, [int]($Operations / 10))),
            "-postgres", $postgresURL, "-output", $output
        ) + $Extra
        & $binary @arguments
        if ($LASTEXITCODE -ne 0) { throw "run failed: $Name $Lane repetition $rep" }
    }
}

$lanes = @("postgres", "default", "specialized")

if ($Suite -in @("primary", "all")) {
    foreach ($lane in $lanes) {
        foreach ($workload in @("w1", "w2", "w3", "w4")) {
            Invoke-Run "primary-$workload-c8" $lane $workload 1000 1000 8
        }
        Invoke-Run "primary-w5-c8" $lane "w5" 10000 1000 8
        Invoke-Run "primary-w6-c8-70r20w10q" $lane "w6" 1000 5000 8
    }
}

if ($Suite -in @("query", "all")) {
    foreach ($lane in $lanes) {
        foreach ($queryOp in @("point", "filter", "take", "map", "count")) {
            Invoke-Run "w5-$queryOp-s25-p10000-c1" $lane "w5" 10000 500 1 @("-query-op", $queryOp, "-selectivity", "25")
        }
        foreach ($selectivity in @("early", "1", "10", "50", "100", "none")) {
            Invoke-Run "w5-take-s$selectivity-p10000-c1" $lane "w5" 10000 500 1 @("-query-op", "take", "-selectivity", $selectivity)
        }
    }
}

if ($Suite -in @("dimensions", "all")) {
    foreach ($lane in $lanes) {
        Invoke-Run "w1-uniform-c1" $lane "w1" 1000 1000 1 @("-contention", "uniform")
        Invoke-Run "w1-hotset-c8" $lane "w1" 1000 1000 8 @("-contention", "hotset")
        Invoke-Run "w1-hotkey-c32" $lane "w1" 1000 1000 32 @("-contention", "hotkey")
        Invoke-Run "w5-filter-s25-p1000-c1" $lane "w5" 1000 500 1 @("-query-op", "filter", "-selectivity", "25")
        Invoke-Run "w5-filter-s25-p100000-c1" $lane "w5" 100000 100 1 @("-query-op", "filter", "-selectivity", "25")
        Invoke-Run "w6-c8-50r40w10q" $lane "w6" 1000 5000 8 @("-mix", "50r40w10q")
    }
}

Write-Output "PERF-M4 suite complete: $raw"

