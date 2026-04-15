param(
    [string]$TaskName = "",
    [string]$StartTime = "06:10",
    [ValidateSet("CZ","CR","cz","cr")]
    [string]$Company = "CZ",
    [int]$Year = (Get-Date).Year
)

$ErrorActionPreference = "Stop"

$CompanyTag = $Company.ToUpper()
if ([string]::IsNullOrWhiteSpace($TaskName)) {
    $TaskName = "ClearOS-Bling-Sales-Daily-$CompanyTag"
}

$RunnerDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ClearRoot = (Resolve-Path (Join-Path $RunnerDir "..\..\..")).Path
$RunScript = Join-Path $RunnerDir "run_bling_sales_daily.ps1"
$LogDir = Join-Path $ClearRoot "logs\integration\scheduler"
$TaskLog = Join-Path $LogDir ("task_runner_sales_{0}.log" -f $CompanyTag.ToLower())
$WrapperDir = Join-Path $ClearRoot "automation\jobs"
$WrapperCmd = Join-Path $WrapperDir ("run_bling_sales_daily_{0}.cmd" -f $CompanyTag.ToLower())

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
New-Item -ItemType Directory -Path $WrapperDir -Force | Out-Null

$wrapperBody = @(
    "@echo off",
    "powershell -NoProfile -ExecutionPolicy Bypass -File `"$RunScript`" -Company $CompanyTag -Year $Year >> `"$TaskLog`" 2>&1"
)
Set-Content -Path $WrapperCmd -Value $wrapperBody -Encoding ASCII

$cmd = "`"$WrapperCmd`""
schtasks /Create /TN $TaskName /SC DAILY /ST $StartTime /TR $cmd /F | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Falha ao registrar task no schtasks (exit code $LASTEXITCODE)."
}

Write-Output "Task registrada: $TaskName"
Write-Output "Horario: $StartTime"
Write-Output "Wrapper: $WrapperCmd"
