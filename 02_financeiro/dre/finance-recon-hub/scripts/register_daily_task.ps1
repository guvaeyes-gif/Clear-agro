param(
    [string]$TaskName = "Finance-Control-Tower-Daily",
    [string]$StartTime = "06:20"
)

$ErrorActionPreference = "Stop"

$RepoRoot = "C:\Users\cesar.zarovski\projects\finance-recon-hub"
$LogFile = Join-Path $RepoRoot "out\recon_task.log"
$Cmd = "cd /d `"$RepoRoot`" && python scripts\run_daily_pipeline.py >> `"$LogFile`" 2>&1"

schtasks /Create /TN $TaskName /SC DAILY /ST $StartTime /TR "cmd /c $Cmd" /F | Out-Null

Write-Output "Task registrada: $TaskName"
Write-Output "Horario: $StartTime"
Write-Output "Comando: $Cmd"
