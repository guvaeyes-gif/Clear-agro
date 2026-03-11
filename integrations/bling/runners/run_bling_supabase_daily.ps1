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
$BlingDir = Join-Path $ClearRoot "11_agentes_automacoes\11_dev_codex_agent\repos\CRM_Clear_Agro\bling_api"
$IngestScript = Join-Path $HOME ".codex\skills\finance-ingest-hub\scripts\finance_ingest_hub.py"
$IngestConfigCZ = Join-Path $ClearRoot "integrations\bling\config\bling_ingest_hub_v1_cz.json"
$IngestConfigCR = Join-Path $ClearRoot "integrations\bling\config\bling_ingest_hub_v1_cr.json"
$GeneratorScript = Join-Path $ClearRoot "integrations\bling\load\generate_bling_supabase_import.py"
$ReconcileScript = Join-Path $ClearRoot "integrations\bling\reconciliation\reconcile_bling_supabase.py"
$BlingSecretsPath = Join-Path $ClearRoot "00_governanca\politicas_de_acesso\legacy_credenciais\bling id.txt"
$StatusDir = Join-Path $ClearRoot "logs\integration\status"
$LogDir = Join-Path $ClearRoot "logs\integration\runs"

New-Item -ItemType Directory -Path $StatusDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$logFile = Join-Path $LogDir ("bling_supabase_daily_{0}.log" -f $RunId)
$CompanyTag = $Company.ToUpper()
$IngestConfig = if ($CompanyTag -eq "CR") { $IngestConfigCR } else { $IngestConfigCZ }

function Write-Log([string]$msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Write-Output $line
    Add-Content -Path $logFile -Value $line
}

function Invoke-Step([string]$name, [scriptblock]$action) {
    Write-Log "START: $name"
    & $action
    if ($LASTEXITCODE -ne 0) {
        throw "Falha no passo '$name' (exit code $LASTEXITCODE)."
    }
    Write-Log "DONE: $name"
}

Write-Log "RunId=$RunId Company=$CompanyTag Year=$Year FromDate=$FromDate"
Write-Log "ClearRoot=$ClearRoot"

if (-not $SkipSync) {
    if (-not (Test-Path $BlingDir)) {
        throw "Diretorio bling_api nao encontrado: $BlingDir"
    }
    if (-not (Test-Path $BlingSecretsPath)) {
        throw "Arquivo de credenciais Bling nao encontrado: $BlingSecretsPath"
    }
    $env:BLING_SECRETS_FILE = $BlingSecretsPath
    Write-Log "BLING_SECRETS_FILE=$BlingSecretsPath"
    Invoke-Step "Bling sync_erp (contatos, contas_receber, contas_pagar)" {
        Set-Location $BlingDir
        python sync_erp.py --company $CompanyTag --year $Year --modules contatos,contas_receber,contas_pagar
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

if (-not $SkipPush) {
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
