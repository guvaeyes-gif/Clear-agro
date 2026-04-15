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

$RunnerDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ClearRoot = (Resolve-Path (Join-Path $RunnerDir "..\..\..")).Path
$BlingPathsConfigPath = Join-Path $ClearRoot "config\paths\bling_cache_roots.json"
$IngestScript = Join-Path $HOME ".codex\skills\finance-ingest-hub\scripts\finance_ingest_hub.py"
$IngestConfigCZ = Join-Path $ClearRoot "integrations\bling\config\bling_ingest_hub_v1_cz.json"
$IngestConfigCR = Join-Path $ClearRoot "integrations\bling\config\bling_ingest_hub_v1_cr.json"
$GeneratorScript = Join-Path $ClearRoot "integrations\bling\load\generate_bling_supabase_import.py"
$NfeGeneratorScript = Join-Path $ClearRoot "integrations\bling\load\generate_bling_nfe_supabase_import.py"
$ReconcileScript = Join-Path $ClearRoot "integrations\bling\reconciliation\reconcile_bling_supabase.py"
$SyncCacheRootsScript = Join-Path $ClearRoot "scripts\sync_bling_cache_roots.py"
$BlingSecretsPath = Join-Path $ClearRoot "00_governanca\politicas_de_acesso\legacy_credenciais\bling id.txt"
$StatusDir = Join-Path $ClearRoot "logs\integration\status"
$LogDir = Join-Path $ClearRoot "logs\integration\runs"
$AuditDir = Join-Path $ClearRoot "logs\audit"
$LockEventsDir = Join-Path $AuditDir "lock_events"
$LocksDir = Join-Path $AuditDir "locks"
$ScriptStartedAt = Get-Date

New-Item -ItemType Directory -Path $StatusDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
New-Item -ItemType Directory -Path $AuditDir -Force | Out-Null
New-Item -ItemType Directory -Path $LockEventsDir -Force | Out-Null
New-Item -ItemType Directory -Path $LocksDir -Force | Out-Null

$logFile = Join-Path $LogDir ("bling_supabase_daily_{0}.log" -f $RunId)
$CompanyTag = $Company.ToUpper()
$IngestConfig = if ($CompanyTag -eq "CR") { $IngestConfigCR } else { $IngestConfigCZ }

function Get-IsoTimestamp([datetime]$Value) {
    return $Value.ToString("yyyy-MM-ddTHH:mm:ssK")
}

function Get-SafeToken([string]$Value) {
    $safe = ($Value -replace "[^a-zA-Z0-9_-]", "_").Trim("_").ToLower()
    if ([string]::IsNullOrWhiteSpace($safe)) {
        return "lock"
    }
    return $safe
}

function Resolve-ConfiguredPath(
    [string]$Value,
    [string]$Fallback
) {
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $Fallback
    }
    if ([System.IO.Path]::IsPathRooted($Value)) {
        return $Value
    }
    return Join-Path $ClearRoot $Value
}

function Get-BlingRoots() {
    $defaultCanonical = Join-Path $ClearRoot "bling_api"
    $defaultCompatibility = Join-Path $ClearRoot "11_agentes_automacoes\11_dev_codex_agent\repos\CRM_Clear_Agro\bling_api"
    $config = $null

    if (Test-Path $BlingPathsConfigPath) {
        try {
            $config = Get-Content -Raw $BlingPathsConfigPath | ConvertFrom-Json
        } catch {
            $config = $null
        }
    }

    $canonicalRoot = Resolve-ConfiguredPath -Value $config.canonical_root -Fallback $defaultCanonical
    $compatibilityRoot = Resolve-ConfiguredPath -Value $config.compatibility_root -Fallback $defaultCompatibility
    return @{
        canonical_root = $canonicalRoot
        compatibility_root = $compatibilityRoot
    }
}

function Write-Log([string]$msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Write-Output $line
    Add-Content -Path $logFile -Value $line
}

function Write-AuditEvent(
    [string]$EventType,
    [string]$ResourceName,
    [hashtable]$Metadata = @{}
) {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss_fff"
    $safeResource = Get-SafeToken $ResourceName
    $safeEvent = Get-SafeToken $EventType
    $eventPath = Join-Path $LockEventsDir ("{0}_{1}_{2}.json" -f $stamp, $safeResource, $safeEvent)

    $payload = [ordered]@{
        event_type = $EventType
        resource_name = $ResourceName
        execution_id = $RunId
        company_code = $CompanyTag
        recorded_at = Get-IsoTimestamp (Get-Date)
        pid = $PID
        hostname = $env:COMPUTERNAME
    }

    foreach ($entry in $Metadata.GetEnumerator()) {
        $payload[$entry.Key] = $entry.Value
    }

    $payload | ConvertTo-Json -Depth 8 | Set-Content -Path $eventPath -Encoding UTF8
}

function Write-BlockedStatus(
    [string]$ResourceName,
    [string]$Message
) {
    $finishedAt = Get-Date
    $statusPath = Join-Path $StatusDir ("bling_supabase_daily_{0}_status.json" -f $RunId)
    $payload = [ordered]@{
        status = "blocked"
        execution_id = $RunId
        run_id = $RunId
        job_name = "run_bling_supabase_daily"
        module_name = "integrations/bling"
        source_system = "bling"
        target_system = "supabase"
        company_code = $CompanyTag
        started_at = Get-IsoTimestamp $ScriptStartedAt
        finished_at = Get-IsoTimestamp $finishedAt
        duration_ms = [int][Math]::Round(($finishedAt - $ScriptStartedAt).TotalMilliseconds)
        error_code = "lock_not_acquired"
        error_message = $Message
        payload_ref = $ResourceName
        triggered_by = "manual_or_scheduler"
        environment = "local"
    }
    $payload | ConvertTo-Json -Depth 8 | Set-Content -Path $statusPath -Encoding UTF8
    Write-Log "STATUS blocked gravado em $statusPath"
}

function Acquire-Lock(
    [string]$ResourceName,
    [hashtable]$Metadata = @{}
) {
    $safeResource = Get-SafeToken $ResourceName
    $lockPath = Join-Path $LocksDir $safeResource
    try {
        New-Item -ItemType Directory -Path $lockPath -ErrorAction Stop | Out-Null
        $lockMetadata = [ordered]@{
            resource_name = $ResourceName
            execution_id = $RunId
            company_code = $CompanyTag
            pid = $PID
            hostname = $env:COMPUTERNAME
            acquired_at = Get-IsoTimestamp (Get-Date)
        }
        foreach ($entry in $Metadata.GetEnumerator()) {
            $lockMetadata[$entry.Key] = $entry.Value
        }
        $metadataPath = Join-Path $lockPath "metadata.json"
        $lockMetadata | ConvertTo-Json -Depth 8 | Set-Content -Path $metadataPath -Encoding UTF8
        Write-AuditEvent -EventType "lock_acquired" -ResourceName $ResourceName -Metadata $lockMetadata
        return @{
            resource_name = $ResourceName
            lock_path = $lockPath
            metadata = $lockMetadata
        }
    } catch {
        $currentHolder = @{}
        $metadataPath = Join-Path $lockPath "metadata.json"
        if (Test-Path $metadataPath) {
            try {
                $holderObject = Get-Content -Raw $metadataPath | ConvertFrom-Json
                if ($holderObject.execution_id) {
                    $currentHolder["execution_id"] = $holderObject.execution_id
                }
                if ($holderObject.company_code) {
                    $currentHolder["company_code"] = $holderObject.company_code
                }
                if ($holderObject.hostname) {
                    $currentHolder["hostname"] = $holderObject.hostname
                }
            } catch {
                $currentHolder = @{ raw_metadata_path = $metadataPath }
            }
        }
        Write-AuditEvent -EventType "lock_blocked" -ResourceName $ResourceName -Metadata @{
            current_holder = $currentHolder
        }
        $holderExecution = if ($currentHolder["execution_id"]) { $currentHolder["execution_id"] } else { "desconhecido" }
        throw "Lock ocupado para $ResourceName. Execucao atual: $holderExecution"
    }
}

function Release-Lock($LockHandle) {
    if (-not $LockHandle) {
        return
    }
    Write-AuditEvent -EventType "lock_released" -ResourceName $LockHandle.resource_name -Metadata @{
        metadata = $LockHandle.metadata
    }
    if (Test-Path $LockHandle.lock_path) {
        Remove-Item -Path $LockHandle.lock_path -Recurse -Force
    }
}

function Invoke-Step([string]$name, [scriptblock]$action) {
    Write-Log "START: $name"
    & $action
    if ($LASTEXITCODE -ne 0) {
        throw "Falha no passo '$name' (exit code $LASTEXITCODE)."
    }
    Write-Log "DONE: $name"
}

function Invoke-StepWithRetry(
    [string]$name,
    [int]$MaxAttempts,
    [int]$DelaySeconds,
    [scriptblock]$action
) {
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            Invoke-Step $name $action
            return
        } catch {
            if ($attempt -ge $MaxAttempts) {
                throw
            }
            Write-Log ("WARN: {0} falhou na tentativa {1}/{2}. Nova tentativa em {3}s. Erro: {4}" -f $name, $attempt, $MaxAttempts, $DelaySeconds, $_.Exception.Message)
            Start-Sleep -Seconds $DelaySeconds
        }
    }
}

function Invoke-OptionalStep([string]$name, [scriptblock]$action) {
    Write-Log "START OPTIONAL: $name"
    try {
        & $action
        if ($LASTEXITCODE -ne 0) {
            Write-Log "WARN: passo opcional '$name' falhou (exit code $LASTEXITCODE)."
            return
        }
        Write-Log "DONE OPTIONAL: $name"
    } catch {
        Write-Log "WARN: passo opcional '$name' falhou. Erro: $($_.Exception.Message)"
    }
}

$PipelineLock = $null
$BlingRoots = Get-BlingRoots
$BlingDir = $BlingRoots.canonical_root
$LegacyBlingDir = $BlingRoots.compatibility_root

try {
    try {
        $PipelineLock = Acquire-Lock -ResourceName ("bling_pipeline_{0}" -f $CompanyTag.ToLower()) -Metadata @{
            module_name = "integrations/bling"
            lock_scope = "company"
            lock_target = "pipeline"
        }
    } catch {
        Write-BlockedStatus -ResourceName ("bling_pipeline_{0}" -f $CompanyTag.ToLower()) -Message $_.Exception.Message
        throw
    }

    Write-Log "RunId=$RunId Company=$CompanyTag Year=$Year FromDate=$FromDate"
    Write-Log "ClearRoot=$ClearRoot"
    Write-Log "BlingCanonicalRoot=$BlingDir"
    Write-Log "BlingCompatibilityRoot=$LegacyBlingDir"

    if (-not $SkipSync) {
        if (-not (Test-Path $BlingDir)) {
            throw "Diretorio bling_api nao encontrado: $BlingDir"
        }
        if (-not (Test-Path (Join-Path $BlingDir "sync_erp.py"))) {
            throw "sync_erp.py nao encontrado na raiz canonica: $BlingDir"
        }
        if (-not (Test-Path $BlingSecretsPath)) {
            throw "Arquivo de credenciais Bling nao encontrado: $BlingSecretsPath"
        }
        $env:BLING_SECRETS_FILE = $BlingSecretsPath
        Write-Log "BLING_SECRETS_FILE=$BlingSecretsPath"
        Invoke-StepWithRetry "Bling sync_erp (contatos, contas_receber, contas_pagar)" 3 20 {
            Set-Location $BlingDir
            python sync_erp.py --company $CompanyTag --year $Year --modules contatos,contas_receber,contas_pagar
        }

        if ((Test-Path $SyncCacheRootsScript) -and ($LegacyBlingDir -ne $BlingDir)) {
            Invoke-OptionalStep "Mirror canonical Bling cache to compatibility root" {
                Set-Location $ClearRoot
                python $SyncCacheRootsScript --source-root $BlingDir --target-root $LegacyBlingDir --run-id ("bling_cache_mirror_{0}" -f $RunId) --overwrite
            }
        }
    } else {
        Write-Log "SYNC pulado por parametro -SkipSync."
    }

    Invoke-Step "Finance ingest hub (bling)" {
        if (-not (Test-Path $IngestConfig)) {
            throw "Config de ingestao nao encontrada: $IngestConfig"
        }
        Set-Location $ClearRoot
        python $IngestScript --config $IngestConfig --run-id ("bling_v1_{0}" -f $RunId)
    }

    Invoke-Step "Generate Bling -> Supabase migration" {
        if (-not (Test-Path $GeneratorScript)) {
            throw "Script gerador nao encontrado: $GeneratorScript"
        }
        Set-Location $ClearRoot
        python $GeneratorScript --bling-dir $BlingDir --status-dir $StatusDir --from-date $FromDate --run-id ("bling_import_v1_{0}" -f $RunId) --batch-size 400 --company $CompanyTag
    }

    Invoke-Step "Generate Bling NF-e -> Supabase migration" {
        if (-not (Test-Path $NfeGeneratorScript)) {
            throw "Script gerador NF-e nao encontrado: $NfeGeneratorScript"
        }
        Set-Location $ClearRoot
        python $NfeGeneratorScript --bling-dir $BlingDir --status-dir $StatusDir --run-id ("bling_nfe_import_v1_{0}" -f $RunId) --batch-size 300 --company $CompanyTag
    }

    if (-not $SkipPush) {
        if (-not (Test-Path $SupabaseTokenPath)) {
            throw "Token Supabase nao encontrado: $SupabaseTokenPath"
        }
        $env:SUPABASE_ACCESS_TOKEN = (Get-Content -Raw $SupabaseTokenPath).Trim()
        if ([string]::IsNullOrWhiteSpace($env:SUPABASE_ACCESS_TOKEN)) {
            throw "SUPABASE_ACCESS_TOKEN vazio em $SupabaseTokenPath"
        }

        $DbPushLock = $null
        try {
            try {
                $DbPushLock = Acquire-Lock -ResourceName "supabase_db_push" -Metadata @{
                    module_name = "integrations/bling"
                    lock_scope = "global"
                    lock_target = "db_push"
                }
            } catch {
                Write-BlockedStatus -ResourceName "supabase_db_push" -Message $_.Exception.Message
                throw
            }

            Invoke-Step "Supabase db push (linked)" {
                Set-Location $ClearRoot
                npx.cmd supabase db push --linked --include-all --yes
            }
        } finally {
            Release-Lock $DbPushLock
        }
    } else {
        Write-Log "PUSH pulado por parametro -SkipPush."
    }

    if (-not $SkipRecon) {
        if (-not (Test-Path $SupabaseTokenPath)) {
            throw "Token Supabase nao encontrado: $SupabaseTokenPath"
        }
        $env:SUPABASE_ACCESS_TOKEN = (Get-Content -Raw $SupabaseTokenPath).Trim()
        if ([string]::IsNullOrWhiteSpace($env:SUPABASE_ACCESS_TOKEN)) {
            throw "SUPABASE_ACCESS_TOKEN vazio em $SupabaseTokenPath"
        }
        if (-not (Test-Path $ReconcileScript)) {
            throw "Script de reconciliacao nao encontrado: $ReconcileScript"
        }
        Invoke-Step "Reconcile Bling x Supabase (AP/AR)" {
            Set-Location $ClearRoot
            python $ReconcileScript --bling-dir $BlingDir --status-dir $StatusDir --from-date $FromDate --run-id ("bling_recon_{0}" -f $RunId) --company $CompanyTag --supabase-token-path $SupabaseTokenPath
        }
    } else {
        Write-Log "RECON pulado por parametro -SkipRecon."
    }

    Write-Log "Pipeline finalizado com sucesso."
    Write-Output "LOG_FILE=$logFile"
} finally {
    Release-Lock $PipelineLock
}
