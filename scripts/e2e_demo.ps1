param(
    [string]$ExpectedDate = "2026-07-11"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) {
        throw $Message
    }
}

function Assert-Equal {
    param($Actual, $Expected, [string]$Message)
    if ($Actual -ne $Expected) {
        throw "$Message (expected=$Expected, actual=$Actual)"
    }
}

function Invoke-Docker {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    $output = & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
    return $output
}

function Wait-HttpJson {
    param([string]$Uri, [int]$TimeoutSeconds = 60)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            return Invoke-RestMethod -Uri $Uri
        }
        catch {
            if ((Get-Date) -ge $deadline) {
                throw "Timed out waiting for $Uri after $TimeoutSeconds seconds: $($_.Exception.Message)"
            }
            Start-Sleep -Seconds 2
        }
    } while ($true)
}

Write-Host "Checking Docker Compose services..."
$runningServices = @(Invoke-Docker compose ps --status running --services)
foreach ($service in @("postgres-oltp", "clickhouse", "redis", "mlflow-server", "fraud-detection-api")) {
    Assert-True ($runningServices -contains $service) "Required service is not running: $service"
}

Write-Host "Checking OLTP boundary..."
$oltpSummary = Invoke-Docker exec fraud-oltp-postgres psql -U postgres -d fraud_bank -At -F "|" -c `
    "SELECT COUNT(*), MIN(event_timestamp)::date, MAX(event_timestamp)::date FROM banking.transactions"
$oltpParts = $oltpSummary.Trim().Split("|")
Assert-Equal $oltpParts[2] $ExpectedDate "banking.transactions is not current"

Write-Host "Checking ClickHouse Gold boundary..."
$goldSummary = Invoke-Docker exec clickhouse clickhouse-client --user abcbank --password abcbank --format TSVRaw --query `
    "SELECT count(), toDate(min(event_timestamp)), toDate(max(event_timestamp)) FROM gold.mart_fraud_ml_features"
$goldParts = $goldSummary.Trim().Split("`t")
Assert-Equal $goldParts[2] $ExpectedDate "gold.mart_fraud_ml_features is not current"

Write-Host "Checking Redis online store..."
$redisKeys = [int](Invoke-Docker exec redis redis-cli DBSIZE)
Assert-True ($redisKeys -gt 0) "Redis online store is empty"

Write-Host "Checking MLflow and API health..."
$mlflowHealth = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:5000/health"
Assert-Equal $mlflowHealth.StatusCode 200 "MLflow health check failed"
$apiHealth = Wait-HttpJson -Uri "http://localhost:8000/health"
Assert-Equal $apiHealth.status "healthy" "FastAPI health check failed"
Assert-True ([bool]$apiHealth.model_loaded) "FastAPI has not loaded the registry model"

Write-Host "Selecting a current entity and checking online inference..."
$entityRow = Invoke-Docker exec clickhouse clickhouse-client --user abcbank --password abcbank --format TSVRaw --query `
    "SELECT customer_id, terminal_id, tx_amount, event_timestamp FROM gold.mart_fraud_ml_features WHERE toDate(event_timestamp) = '$ExpectedDate' AND customer_id IS NOT NULL AND terminal_id IS NOT NULL ORDER BY event_timestamp DESC LIMIT 1"
$entityParts = $entityRow.Trim().Split("`t")
Assert-Equal $entityParts.Count 4 "Could not select a current Gold entity"
$payload = @{
    customer_id = [int64]$entityParts[0]
    terminal_id = [int64]$entityParts[1]
    TX_AMOUNT = [double]$entityParts[2]
    TX_DATETIME = ([datetime]$entityParts[3]).ToString("o")
} | ConvertTo-Json
$prediction = Invoke-RestMethod -Method Post -Uri "http://localhost:8000/predict-online" -ContentType "application/json" -Body $payload
Assert-True ($prediction.fraud_probability -ge 0 -and $prediction.fraud_probability -le 1) "Invalid fraud probability"

[pscustomobject]@{
    expected_date = $ExpectedDate
    oltp_rows = [int64]$oltpParts[0]
    gold_rows = [int64]$goldParts[0]
    redis_keys = $redisKeys
    model_version = $apiHealth.model_version
    fraud_probability = $prediction.fraud_probability
    result = "PASS"
} | Format-List
