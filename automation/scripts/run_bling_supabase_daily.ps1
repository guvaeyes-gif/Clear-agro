param(
    [ValidateSet("CZ","CR","cz","cr")]
    [string]$Company = "CZ",
    [int]$Year = (Get-Date).Year,
    [string]$FromDate = "2025-01-01",
    [string]$RunId = (Get-Date -Format "yyyyMMdd_HHmmss"),
    [string]$SupabaseTokenPath = "$HOME\Documents\token supabase.txt",
    [switch]$SkipSync,
    [switch]$SkipPush,
    [switch]$SkipRecon
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ClearRoot = (Resolve-Path (Join-Path $ScriptDir "..\..")).Path
$CanonicalRunner = Join-Path $ClearRoot "integrations\bling\runners\run_bling_supabase_daily.ps1"

if (-not (Test-Path $CanonicalRunner)) {
    throw "Runner canonico nao encontrado: $CanonicalRunner"
}

Write-Warning "automation/scripts/run_bling_supabase_daily.ps1 e apenas compatibilidade. Use integrations/bling/runners/run_bling_supabase_daily.ps1."

$invokeParams = @{
    Company = $Company
    Year = $Year
    FromDate = $FromDate
    RunId = $RunId
    SupabaseTokenPath = $SupabaseTokenPath
}

if ($SkipSync) {
    $invokeParams.SkipSync = $true
}
if ($SkipPush) {
    $invokeParams.SkipPush = $true
}
if ($SkipRecon) {
    $invokeParams.SkipRecon = $true
}

& $CanonicalRunner @invokeParams
exit $LASTEXITCODE
