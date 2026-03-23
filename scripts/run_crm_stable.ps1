param(
  [int]$Port = 8510,
  [string]$HostAddress = "127.0.0.1",
  [switch]$OpenBrowser
)

$ErrorActionPreference = "Stop"

function Get-RepoRoot {
  $scriptDir = $PSScriptRoot
  if (-not $scriptDir) {
    $scriptDir = Split-Path -Parent $PSCommandPath
  }
  return (Resolve-Path (Join-Path $scriptDir "..")).Path
}

function Stop-ProcessOnPort {
  param([int]$TargetPort)
  $lines = netstat -ano -p tcp | Select-String ":$TargetPort\s+.*LISTENING"
  foreach ($ln in $lines) {
    $parts = ($ln.ToString() -split "\s+") | Where-Object { $_ -ne "" }
    if ($parts.Count -ge 5) {
      $procId = [int]$parts[-1]
      if ($procId -gt 0) {
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
      }
    }
  }
}

function Test-HttpUp {
  param(
    [string]$Url,
    [int]$TimeoutSec = 2
  )
  try {
    $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSec
    return ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500)
  } catch {
    return $false
  }
}

$root = Get-RepoRoot
$outDir = Join-Path $root "out"
New-Item -Path $outDir -ItemType Directory -Force | Out-Null

$url = "http://$HostAddress`:$Port"
$stdoutLog = Join-Path $outDir "streamlit_$Port.out.log"
$stderrLog = Join-Path $outDir "streamlit_$Port.err.log"

Write-Host "[crm] root: $root"
Write-Host "[crm] target: $url"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Write-Host "[crm] ERRO: python nao encontrado no PATH."
  exit 1
}

Stop-ProcessOnPort -TargetPort $Port
Start-Sleep -Milliseconds 700

if (Test-Path $stdoutLog) { Remove-Item $stdoutLog -Force }
if (Test-Path $stderrLog) { Remove-Item $stderrLog -Force }

$args = @(
  "-m", "streamlit", "run", "app\main.py",
  "--server.address", $HostAddress,
  "--server.port", "$Port"
)

$proc = Start-Process -FilePath python -ArgumentList $args -WorkingDirectory $root -PassThru -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog

$ok = $false
for ($i = 0; $i -lt 30; $i++) {
  Start-Sleep -Seconds 1
  if (Test-HttpUp -Url $url) {
    $ok = $true
    break
  }
  if ($proc.HasExited) {
    break
  }
}

if (-not $ok) {
  Write-Host "[crm] ERRO: app nao respondeu em $url"
  if (Test-Path $stderrLog) {
    Write-Host "[crm] stderr (ultimas linhas):"
    Get-Content $stderrLog -Tail 40
  }
  if (Test-Path $stdoutLog) {
    Write-Host "[crm] stdout (ultimas linhas):"
    Get-Content $stdoutLog -Tail 40
  }
  exit 1
}

Write-Host "[crm] OK: $url"
Write-Host "[crm] PID: $($proc.Id)"
Write-Host "[crm] logs: $stdoutLog"

if ($OpenBrowser) {
  Start-Process $url | Out-Null
}

