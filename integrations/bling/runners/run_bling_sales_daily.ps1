param(
    [ValidateSet("CZ","CR","cz","cr")]
    [string]$Company = "CZ",
    [int]$Year = (Get-Date).Year,
    [string]$RunId = (Get-Date -Format "yyyyMMdd_HHmmss"),
    [string]$SupabaseDbUrl = "",
    [string]$SupabaseTokenPath = "$HOME\Documents\token supabase.txt",
    [switch]$SkipSync,
    [switch]$SkipPush
)

$ErrorActionPreference = "Stop"

$RunnerDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ClearRoot = (Resolve-Path (Join-Path $RunnerDir "..\..\..")).Path
$BlingDir = Join-Path $ClearRoot "bling_api"
$NfeFetchScript = Join-Path $BlingDir "fetch_nfe_incremental.py"
$NfeGeneratorScript = Join-Path $ClearRoot "integrations\bling\load\generate_bling_nfe_supabase_import.py"
$BlingSecretsPath = Join-Path $ClearRoot "00_governanca\politicas_de_acesso\legacy_credenciais\bling id.txt"
$StatusDir = Join-Path $ClearRoot "logs\integration\status"
$LogDir = Join-Path $ClearRoot "logs\integration\runs"
$AuditDir = Join-Path $ClearRoot "logs\audit"
$LocksDir = Join-Path $AuditDir "locks"
$ScriptStartedAt = Get-Date
$CompanyTag = $Company.ToUpper()
$logFile = Join-Path $LogDir ("bling_sales_daily_{0}.log" -f $RunId)

New-Item -ItemType Directory -Path $StatusDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
New-Item -ItemType Directory -Path $AuditDir -Force | Out-Null
New-Item -ItemType Directory -Path $LocksDir -Force | Out-Null

function Get-IsoTimestamp([datetime]$Value) {
    return $Value.ToString("yyyy-MM-ddTHH:mm:ssK")
}

function Write-Log([string]$msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Write-Output $line
    Add-Content -Path $logFile -Value $line
}

function Get-SafeToken([string]$Value) {
    $safe = ($Value -replace "[^a-zA-Z0-9_-]", "_").Trim("_").ToLower()
    if ([string]::IsNullOrWhiteSpace($safe)) {
        return "lock"
    }
    return $safe
}

function Acquire-Lock([string]$ResourceName) {
    $safeResource = Get-SafeToken $ResourceName
    $lockPath = Join-Path $LocksDir $safeResource
    try {
        New-Item -ItemType Directory -Path $lockPath -ErrorAction Stop | Out-Null
        $metadata = [ordered]@{
            resource_name = $ResourceName
            execution_id = $RunId
            company_code = $CompanyTag
            pid = $PID
            hostname = $env:COMPUTERNAME
            acquired_at = Get-IsoTimestamp (Get-Date)
        }
        $metadata | ConvertTo-Json -Depth 6 | Set-Content -Path (Join-Path $lockPath "metadata.json") -Encoding UTF8
        return @{ resource_name = $ResourceName; lock_path = $lockPath; metadata = $metadata }
    } catch {
        $metadataPath = Join-Path $lockPath "metadata.json"
        $holderExecution = "desconhecido"
        if (Test-Path $metadataPath) {
            try {
                $holder = Get-Content -Raw $metadataPath | ConvertFrom-Json
                if ($holder.execution_id) {
                    $holderExecution = $holder.execution_id
                }
            } catch {}
        }
        throw "Lock ocupado para $ResourceName. Execucao atual: $holderExecution"
    }
}

function Release-Lock($LockHandle) {
    if (-not $LockHandle) {
        return
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

function Resolve-RemoteDbUrl() {
    $candidates = @(
        $SupabaseDbUrl,
        $env:SUPABASE_DB_URL,
        $env:CRM_DATABASE_URL,
        $env:DATABASE_URL
    )
    foreach ($candidate in $candidates) {
        $value = [string]$candidate
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $value.Trim()
        }
    }
    return ""
}

$SalesLock = $null
$DbPushLock = $null

try {
    $SalesLock = Acquire-Lock -ResourceName ("bling_sales_pipeline_{0}" -f $CompanyTag.ToLower())
    Write-Log "RunId=$RunId Company=$CompanyTag Year=$Year"
    Write-Log "ClearRoot=$ClearRoot"
    Write-Log "BlingDir=$BlingDir"

    if (-not $SkipSync) {
        if (-not (Test-Path $BlingDir)) {
            throw "Diretorio bling_api nao encontrado: $BlingDir"
        }
        if (-not (Test-Path $NfeFetchScript)) {
            throw "Script fetch_nfe_incremental.py nao encontrado em: $BlingDir"
        }
        if (-not (Test-Path $BlingSecretsPath)) {
            throw "Arquivo de credenciais Bling nao encontrado: $BlingSecretsPath"
        }
        $env:BLING_SECRETS_FILE = $BlingSecretsPath
        Write-Log "BLING_SECRETS_FILE=$BlingSecretsPath"
        Invoke-Step "Bling fetch_nfe_incremental" {
            Set-Location $BlingDir
            python $NfeFetchScript --company $CompanyTag --year $Year
        }
    } else {
        Write-Log "SYNC pulado por parametro -SkipSync."
    }

        Invoke-Step "Generate Bling NF-e -> Supabase migration" {
            if (-not (Test-Path $NfeGeneratorScript)) {
                throw "Script gerador NF-e nao encontrado: $NfeGeneratorScript"
            }
            Set-Location $ClearRoot
            python $NfeGeneratorScript --bling-dir $BlingDir --status-dir $StatusDir --run-id $RunId --batch-size 300 --company $CompanyTag
        }

    if (-not $SkipPush) {
        $DbPushLock = Acquire-Lock -ResourceName "supabase_db_push"
        $RemoteDbUrl = Resolve-RemoteDbUrl
        if (-not [string]::IsNullOrWhiteSpace($RemoteDbUrl)) {
            Write-Log "SUPABASE_DB_URL resolved from environment/parameter."
            Invoke-Step "Supabase db push (db-url)" {
                Set-Location $ClearRoot
                npx.cmd supabase db push --db-url $RemoteDbUrl --include-all --yes
            }
        }
        else {
            if (-not (Test-Path $SupabaseTokenPath)) {
                throw "Token Supabase nao encontrado: $SupabaseTokenPath"
            }
            $env:SUPABASE_ACCESS_TOKEN = (Get-Content -Raw $SupabaseTokenPath).Trim()
            if ([string]::IsNullOrWhiteSpace($env:SUPABASE_ACCESS_TOKEN)) {
                throw "SUPABASE_ACCESS_TOKEN vazio em $SupabaseTokenPath"
            }

            Invoke-Step "Supabase db push (linked)" {
                Set-Location $ClearRoot
                npx.cmd supabase db push --linked --include-all --yes
            }
        }
    } else {
        Write-Log "PUSH pulado por parametro -SkipPush."
    }

    $statusPath = Join-Path $StatusDir ("bling_sales_daily_{0}_status.json" -f $RunId)
    [ordered]@{
        status = "success"
        execution_id = $RunId
        run_id = $RunId
        job_name = "run_bling_sales_daily"
        module_name = "integrations/bling"
        source_system = "bling"
        target_system = "supabase"
        company_code = $CompanyTag
        started_at = Get-IsoTimestamp $ScriptStartedAt
        finished_at = Get-IsoTimestamp (Get-Date)
        log_file = $logFile
    } | ConvertTo-Json -Depth 6 | Set-Content -Path $statusPath -Encoding UTF8

    Write-Log "Pipeline comercial finalizado com sucesso."
    Write-Output "LOG_FILE=$logFile"
}
finally {
    Release-Lock $DbPushLock
    Release-Lock $SalesLock
}
