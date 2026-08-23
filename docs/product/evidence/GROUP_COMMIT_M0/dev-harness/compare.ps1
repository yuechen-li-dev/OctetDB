param(
    [Parameter(Mandatory=$true)][string]$Current,
    [Parameter(Mandatory=$true)][string]$Baseline,
    [string]$Output = ''
)
$ErrorActionPreference = 'Stop'

$currentReport = Get-Content -Raw -LiteralPath $Current | ConvertFrom-Json
$baselineReport = Get-Content -Raw -LiteralPath $Baseline | ConvertFrom-Json
$baselineByKey = @{}
foreach ($metric in $baselineReport.metrics) {
    $baselineByKey["$($metric.workload)|$($metric.mode)|$($metric.concurrency)"] = $metric
}
$deltas = foreach ($metric in $currentReport.metrics) {
    $key = "$($metric.workload)|$($metric.mode)|$($metric.concurrency)"
    $prior = $baselineByKey[$key]
    if ($null -eq $prior) { continue }
    $throughput = 100 * ($metric.ops_per_second / $prior.ops_per_second - 1)
    $p99 = if ($prior.p99_ns -eq 0) { 0 } else { 100 * ($metric.p99_ns / $prior.p99_ns - 1) }
    $alloc = if ($prior.allocs_per_op -eq 0) { 0 } else { 100 * ($metric.allocs_per_op / $prior.allocs_per_op - 1) }
    [pscustomobject]@{
        workload = $metric.workload
        mode = $metric.mode
        concurrency = $metric.concurrency
        throughput_delta_percent = [math]::Round($throughput, 2)
        p99_delta_percent = [math]::Round($p99, 2)
        allocs_delta_percent = [math]::Round($alloc, 2)
        sync_calls_delta = $metric.sync_calls - $prior.sync_calls
        commands_per_sync_delta = [math]::Round($metric.commands_per_sync - $prior.commands_per_sync, 3)
        warnings = @(
            if ($throughput -lt -10) { 'throughput_below_-10_percent' }
            if ($p99 -gt 20) { 'p99_above_+20_percent' }
            if ($alloc -gt 20) { 'allocs_above_+20_percent' }
        )
    }
}
$result = [pscustomobject]@{ current = $Current; baseline = $Baseline; deltas = @($deltas) }
$json = $result | ConvertTo-Json -Depth 6
if ($Output) { Set-Content -LiteralPath $Output -Value $json -Encoding utf8NoBOM } else { $json }
