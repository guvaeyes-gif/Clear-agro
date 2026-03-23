param(
    [string]$TaskName = "",
    [string]$StartTime = "06:10",
    [ValidateSet("CZ","CR","cz","cr")]
    [string]$Company = "CZ",
    [int]$Year = (Get-Date).Year,
    [string]$FromDate = "2025-01-01"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ClearRoot = (Resolve-Path (Join-Path $ScriptDir "..\..")).Path
$CanonicalRegister = Join-Path $ClearRoot "integrations\bling\runners\register_bling_supabase_daily_task.ps1"

if (-not (Test-Path $CanonicalRegister)) {
    throw "Registrador canonico nao encontrado: $CanonicalRegister"
}

Write-Warning "automation/scheduler/register_bling_supabase_daily_task.ps1 e apenas compatibilidade. Use integrations/bling/runners/register_bling_supabase_daily_task.ps1."

& $CanonicalRegister `
    -TaskName $TaskName `
    -StartTime $StartTime `
    -Company $Company `
    -Year $Year `
    -FromDate $FromDate

exit $LASTEXITCODE
