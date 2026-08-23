$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$harness = Join-Path $root "harness"
$raw = Join-Path $root "raw/windows"
$binary = Join-Path $env:TEMP "octetdb-perf-m4.exe"
$postgresURL = if ($env:PERF_M4_POSTGRES_URL) { $env:PERF_M4_POSTGRES_URL } else { "postgres://dbsched:dbsched@localhost:54329/dbsched?sslmode=disable" }
$container = "database-scheduler-postgres-1"

Push-Location $harness
try { go build -trimpath -o $binary . } finally { Pop-Location }
if ($LASTEXITCODE -ne 0) { throw "harness build failed" }

function Get-PostgresUsage {
    $value = docker exec $container sh -c 'ticks=0; rss=0; for p in /proc/[0-9]*; do if [ -r "$p/comm" ] && [ "$(cat "$p/comm")" = postgres ]; then set -- $(cat "$p/stat"); ticks=$((ticks+${14}+${15})); value=$(awk "/VmRSS/ {print \$2}" "$p/status"); rss=$((rss+${value:-0})); fi; done; echo "$ticks $rss"'
    if ($LASTEXITCODE -ne 0) { throw "failed to read PostgreSQL process usage" }
    $parts = $value.Trim().Split(' ')
    return [PSCustomObject]@{ Ticks = [int64]$parts[0]; RSSKB = [int64]$parts[1] }
}

$cases = @(
    @{ Workload="w1"; Population=1000; Operations=10000 },
    @{ Workload="w2"; Population=1000; Operations=10000 },
    @{ Workload="w3"; Population=1000; Operations=10000 },
    @{ Workload="w4"; Population=1000; Operations=10000 },
    @{ Workload="w5"; Population=10000; Operations=10000 },
    @{ Workload="w6"; Population=1000; Operations=50000 }
)

foreach ($case in $cases) {
    $resultPath = Join-Path $raw "resource-postgres-$($case.Workload).json"
    $logPath = Join-Path $env:TEMP "perfm4-resource-$($case.Workload).log"
    $arguments = @("-lane","postgres","-workload",$case.Workload,"-population",$case.Population,"-operations",$case.Operations,"-concurrency",8,"-warmup",100,"-postgres",$postgresURL,"-output",$resultPath)
    $before = Get-PostgresUsage
    $process = Start-Process -FilePath $binary -ArgumentList $arguments -PassThru -WindowStyle Hidden -RedirectStandardError $logPath
    $peakRSSKB = $before.RSSKB
	$peakTicks = $before.Ticks
    while (-not $process.HasExited) {
        Start-Sleep -Milliseconds 100
        $sample = Get-PostgresUsage
        if ($sample.RSSKB -gt $peakRSSKB) { $peakRSSKB = $sample.RSSKB }
		if ($sample.Ticks -gt $peakTicks) { $peakTicks = $sample.Ticks }
        $process.Refresh()
    }
    if ($process.ExitCode -ne 0) { throw "resource run failed: $($case.Workload): $(Get-Content -Raw $logPath)" }
    $after = Get-PostgresUsage
	if ($after.Ticks -gt $peakTicks) { $peakTicks = $after.Ticks }
    $sidecar = [PSCustomObject]@{
        workload = $case.Workload
        topology = "Docker Linux PostgreSQL service; sum of postgres process RSS"
        clock_ticks_per_second = 100
		server_cpu_seconds = ($peakTicks - $before.Ticks) / 100.0
        server_peak_rss_bytes = $peakRSSKB * 1024
    } | ConvertTo-Json
    [IO.File]::WriteAllText((Join-Path $raw "resource-postgres-$($case.Workload)-server.json"), $sidecar + [Environment]::NewLine)
}
