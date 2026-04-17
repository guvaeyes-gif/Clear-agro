param(
    [ValidateSet("CZ","CR","cz","cr","ALL","all")]
    [string]$Company = "ALL",
    [string]$RunId = (Get-Date -Format "yyyyMMdd_HHmmss"),
    [string]$BlingSecretsPath = "$HOME\Documents\bling id.txt",
    [string]$FromDate,
    [switch]$ForceFull,
    [switch]$DryRun,
    [switch]$SkipSync,
    [switch]$SkipSnapshot
)

$ErrorActionPreference = "Stop"

$PipelineDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ClearRoot = (Resolve-Path $PipelineDir).Path
$BlingDir = Join-Path $ClearRoot "bling_api"
$StatusDir = Join-Path $ClearRoot "logs\integration\status"
$LogDir = Join-Path $ClearRoot "logs\integration\runs"
$SnapshotScript = Join-Path $ClearRoot "02_financeiro\dashboard_online\build_snapshot.py"

New-Item -ItemType Directory -Path $StatusDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$logFile = Join-Path $LogDir ("bling_contas_incremental_{0}.log" -f $RunId)
$statusFile = Join-Path $StatusDir ("sync_contas_incremental_{0}_status.json" -f $RunId)

function Write-Log([string]$msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Write-Output $line
    Add-Content -Path $logFile -Value $line
}

function Get-LastSyncDate([string]$CompanyTag) {
    $pattern = "*sync_bling_cache_roots*{0}*status.json" -f $CompanyTag.ToLower()
    if ($CompanyTag -eq "CZ") {
        $pattern = "*sync_bling_cache_roots*status.json"
    }
    
    $latest = Get-ChildItem -Path $StatusDir -Filter $pattern | 
        Sort-Object LastWriteTime -Descending | 
        Select-Object -First 1
    
    if ($latest) {
        $content = Get-Content $latest.FullName -Raw | ConvertFrom-Json
        $generatedAt = $content.generated_at
        if ($generatedAt) {
            $dateObj = [DateTime]::Parse($generatedAt)
            $dateObj = $dateObj.AddHours(-3)  # Ajuste UTC -> BRT
            return $dateObj.ToString("yyyy-MM-dd")
        }
    }
    
    # Fallback: 7 dias atrás se não encontrar status
    return (Get-Date).AddDays(-7).ToString("yyyy-MM-dd")
}

Write-Log "=== SYNC INCREMENTAL DE CONTAS (AP/AR) ==="
Write-Log "RunId=$RunId Company=$Company"
Write-Log "ClearRoot=$ClearRoot"
Write-Log "BlingDir=$BlingDir"

if (-not (Test-Path $BlingSecretsPath)) {
    throw "Arquivo de credenciais Bling nao encontrado: $BlingSecretsPath"
}

$env:BLING_SECRETS_FILE = $BlingSecretsPath

$companies = if ($Company -eq "ALL") { @("CZ", "CR") } else { @($Company.ToUpper()) }

$results = @{}

foreach ($CompanyTag in $companies) {
    Write-Log ""
    Write-Log "=== EMPRESA: $CompanyTag ==="
    
    $lastSyncDate = Get-LastSyncDate -CompanyTag $CompanyTag
    $syncFrom = if ($ForceFull) { "2024-01-01" } elseif ($FromDate) { $FromDate } else { $lastSyncDate }
    $syncTo = if ($ForceFull) { "2035-12-31" } else { $null }
    
    Write-Log "Data da ultima sync: $lastSyncDate"
    Write-Log "Sincronizando a partir de: $syncFrom"
    if ($ForceFull) {
        Write-Log "Forcando carga completa ate 2035-12-31"
    }
    
    if ($DryRun) {
        Write-Log "[DRY RUN] Simulando sync a partir de $syncFrom"
        continue
    }
    
    if (-not $SkipSync) {
        Set-Location $BlingDir
        
        Write-Log "Iniciando sync_contas_incremental.py para $CompanyTag..."
        $pythonArgs = @(
            "sync_contas_incremental.py",
            "--company", $CompanyTag,
            "--module", "ambos",
            "--date-from", $syncFrom,
            "--secrets-file", $BlingSecretsPath
        )
        if ($syncTo) {
            $pythonArgs += @("--date-to", $syncTo, "--force-full")
        }
        python @pythonArgs
        
        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERRO: sync_contas_incremental.py falhou para $CompanyTag"
            continue
        }
        
        Write-Log "Sync concluido para $CompanyTag"
    } else {
        Write-Log "SYNC pulado por parametro -SkipSync."
    }
}

# Sincronizar arquivos entre diretórios
if (-not $DryRun -and -not $SkipSync) {
    Write-Log ""
    Write-Log "=== Sincronizando arquivos de cache ==="
    Set-Location $ClearRoot
    
    python scripts\sync_bling_cache_roots.py --run-id "bling_cache_incremental_$RunId"
    
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERRO: sync_bling_cache_roots.py falhou"
    } else {
        Write-Log "Cache sincronizado com sucesso"
    }
}

# Atualizar snapshot do dashboard
if (-not $DryRun -and -not $SkipSnapshot -and (Test-Path $SnapshotScript)) {
    Write-Log ""
    Write-Log "=== Atualizando snapshot do dashboard financeiro ==="
    Set-Location $ClearRoot
    
    python $SnapshotScript
    
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERRO: build_snapshot.py falhou"
    } else {
        Write-Log "Snapshot atualizado com sucesso"
    }
}

Write-Log ""
Write-Log "=== SYNC INCREMENTAL FINALIZADO ==="
Write-Output "LOG_FILE=$logFile"
Write-Output "STATUS_FILE=$statusFile"
