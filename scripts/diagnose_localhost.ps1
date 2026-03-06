param(
  [int]$Port = 9000
)

$ErrorActionPreference = "Stop"

function Test-HttpUp {
  param(
    [string]$Url,
    [int]$TimeoutSec = 2
  )
  try {
    $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSec
    return $resp.StatusCode
  } catch {
    return $null
  }
}

Write-Host "[diag] Python:"
python --version

Write-Host "[diag] Iniciando servidor simples em 127.0.0.1:$Port ..."
$p = Start-Process -FilePath python -ArgumentList "-m","http.server","$Port","--bind","127.0.0.1" -PassThru

Start-Sleep -Seconds 2

$status = Test-HttpUp -Url "http://127.0.0.1:$Port"
$listen = netstat -ano -p tcp | Select-String ":$Port\s+.*LISTENING"

if ($status -and $listen) {
  Write-Host "[diag] OK: localhost funcional (status $status)."
} else {
  Write-Host "[diag] FALHA: localhost nao respondeu."
  Write-Host "[diag] netstat:"
  netstat -ano -p tcp | Select-String ":$Port"
  Write-Host "[diag] Acoes sugeridas:"
  Write-Host "  1) Desativar VPN/Proxy e testar novamente."
  Write-Host "  2) Liberar python.exe no Firewall."
  Write-Host "  3) Testar em PowerShell como Administrador."
}

Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
Write-Host "[diag] Finalizado."

