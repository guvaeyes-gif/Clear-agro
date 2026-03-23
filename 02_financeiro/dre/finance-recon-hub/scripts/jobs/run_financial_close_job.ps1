param(
    [switch]$AllowStaleData
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $RepoRoot

Write-Output "[START] Financial close job"
$pipelineOutput = & python scripts\run_daily_pipeline.py
$pipelineOutput | ForEach-Object { Write-Output $_ }

$runIdLine = $pipelineOutput | Where-Object { $_ -like "run_id=*" } | Select-Object -First 1
if (-not $runIdLine) {
    throw "run_id nao encontrado no output."
}
$runId = $runIdLine.Split("=", 2)[1]

$preSyncLine = $pipelineOutput | Where-Object { $_ -like "pre_sync_status=*" } | Select-Object -First 1
$preSync = if ($preSyncLine) { $preSyncLine.Split("=", 2)[1] } else { "unknown" }
if ($preSync -ne "ok" -and -not $AllowStaleData) {
    throw "Pre-sync falhou e AllowStaleData nao habilitado."
}

$required = @(
    "out\conciliacao_$runId.xlsx",
    "out\conciliacao_matches_$runId.csv",
    "out\conciliacao_nao_conciliado_$runId.csv",
    "out\conciliacao_pendente_bling_$runId.csv"
)

foreach ($relPath in $required) {
    $full = Join-Path $RepoRoot $relPath
    if (-not (Test-Path $full)) {
        throw "Artefato ausente: $relPath"
    }
}

Write-Output "[OK] Financial close validado. run_id=$runId"
