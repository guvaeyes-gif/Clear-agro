param(
    [string]$TaskName = "",
    [string]$StartTime = "06:00",
    [ValidateSet("CZ","CR","cz","cr")]
    [string]$Company = "CZ"
)

$ErrorActionPreference = "Stop"

$companyTag = $Company.ToUpper()
if ([string]::IsNullOrWhiteSpace($TaskName)) {
    $TaskName = "Clara-Bling-Sync-Daily-$companyTag"
}

$RepoRoot = "C:\Users\cesar.zarovski\CRM_Clear_Agro"
$BlingDir = Join-Path $RepoRoot "bling_api"
$LogFile = Join-Path $RepoRoot ("out\bling_sync_task_{0}.log" -f $companyTag.ToLower())

if (!(Test-Path $BlingDir)) {
    throw "Diretorio nao encontrado: $BlingDir"
}

$cmd = "cd /d `"$BlingDir`" && python sync_erp.py --company $companyTag >> `"$LogFile`" 2>&1"

schtasks /Create /TN $TaskName /SC DAILY /ST $StartTime /TR "cmd /c $cmd" /F | Out-Null

Write-Output "Task registrada: $TaskName"
Write-Output "Horario: $StartTime"
Write-Output "Comando: $cmd"
