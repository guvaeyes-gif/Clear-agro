param(
    [string]$TaskName = "Clara-AR-Weekly",
    [string]$StartTime = "08:00",
    [string]$Day = "MON"
)

$ErrorActionPreference = "Stop"

$RepoRoot = "C:\Users\cesar.zarovski\CRM_Clear_Agro"
$LogFile = "C:\Users\cesar.zarovski\CRM_Clear_Agro\out\ar_weekly_task.log"

if (!(Test-Path $RepoRoot)) {
    throw "Diretorio nao encontrado: $RepoRoot"
}

$cmd = "cd /d `"$RepoRoot`" && python scripts\send_ar_weekly.py >> `"$LogFile`" 2>&1"
schtasks /Create /TN $TaskName /SC WEEKLY /D $Day /ST $StartTime /TR "cmd /c $cmd" /F | Out-Null

Write-Output "Task registrada: $TaskName"
Write-Output "Dia/Horario: $Day $StartTime"
Write-Output "Comando: $cmd"
