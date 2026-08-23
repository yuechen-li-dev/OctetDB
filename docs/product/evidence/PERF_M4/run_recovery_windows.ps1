$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$harness = Join-Path $root "harness"
$raw = Join-Path $root "raw/windows"
$binary = Join-Path $env:TEMP "octetdb-perf-m4-recovery.exe"
$dataRoot = [IO.Path]::GetFullPath((Join-Path $env:TEMP "octetdb-perf-m4-recovery"))
$expectedRoot = [IO.Path]::GetFullPath($env:TEMP)
if (-not $dataRoot.StartsWith($expectedRoot, [StringComparison]::OrdinalIgnoreCase)) { throw "recovery data escaped TEMP" }
New-Item -ItemType Directory -Force -Path $dataRoot, $raw | Out-Null

Push-Location $harness
try { go build -trimpath -o $binary ./recoveryprobe } finally { Pop-Location }
if ($LASTEXITCODE -ne 0) { throw "recovery probe build failed" }

for ($rep = 1; $rep -le 3; $rep++) {
    $cold = Join-Path $dataRoot "cold-r$rep"
    & $binary -mode cold -data $cold -output (Join-Path $raw "recovery-cold-default-r$rep.json")
    foreach ($workload in @("w1", "w3")) {
        foreach ($prepare in @("snapshot", "wal")) {
            $data = Join-Path $dataRoot "$workload-$prepare-r$rep"
            & $binary -mode "prepare-$prepare" -workload $workload -population 10000 -data $data
            & $binary -mode measure -workload $workload -population 10000 -data $data -output (Join-Path $raw "recovery-$workload-$prepare-default-r$rep.json")
        }
    }
}

$resolved = [IO.Path]::GetFullPath($dataRoot)
if (-not $resolved.StartsWith($expectedRoot, [StringComparison]::OrdinalIgnoreCase)) { throw "refusing cleanup outside TEMP" }
Remove-Item -LiteralPath $resolved -Recurse -Force
