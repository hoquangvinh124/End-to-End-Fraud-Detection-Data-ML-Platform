param(
    [int]$TimeoutSeconds = 600,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.Net.Http

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Assert-LastExitCode {
    param([string]$Operation)
    if ($LASTEXITCODE -ne 0) {
        throw "$Operation failed with exit code $LASTEXITCODE"
    }
}

function Wait-HttpEndpoint {
    param([string]$Uri, [datetime]$Deadline)
    do {
        $client = [System.Net.Http.HttpClient]::new()
        $client.Timeout = [TimeSpan]::FromSeconds(5)
        $response = $null
        try {
            $response = $client.GetAsync(
                $Uri,
                [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
            ).GetAwaiter().GetResult()
            if ([int]$response.StatusCode -ge 200 -and [int]$response.StatusCode -lt 300) {
                $response.Dispose()
                $client.Dispose()
                return
            }
        }
        catch {
            if ((Get-Date) -ge $Deadline) {
                throw "Endpoint did not become ready: $Uri ($($_.Exception.Message))"
            }
        }
        finally {
            if ($null -ne $response) {
                $response.Dispose()
            }
            $client.Dispose()
        }
        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $Deadline)

    throw "Endpoint did not become ready: $Uri"
}

Write-Host "[1/5] Checking Docker engine..."
docker info *> $null
Assert-LastExitCode "docker info"

Write-Host "[2/5] Validating Compose configuration..."
docker compose config --quiet
Assert-LastExitCode "docker compose config --quiet"

if (-not $SkipBuild) {
    Write-Host "[3/5] Building application and Airflow images..."
    docker compose --profile batch build silver-transactions
    Assert-LastExitCode "docker compose --profile batch build silver-transactions"
    docker compose up -d --build --remove-orphans
    Assert-LastExitCode "docker compose up -d --build --remove-orphans"
}
else {
    Write-Host "[3/5] Starting cached images..."
    docker compose up -d --remove-orphans
    Assert-LastExitCode "docker compose up -d --remove-orphans"
}

$requiredServices = @(
    "postgres-oltp", "kafka", "schema-registry", "kafka-connect",
    "cdc-transactions", "cdc-fraud-cases", "minio", "metastore-db",
    "hive-metastore", "trino", "clickhouse", "redis", "mlflow-postgres",
    "mlflow-server", "otel-collector", "pipeline-observer", "prometheus",
    "grafana", "airflow-postgres", "airflow-api-server",
    "airflow-dag-processor", "airflow-scheduler", "airflow-triggerer",
    "fraud-detection-api"
)

Write-Host "[4/5] Waiting for required services..."
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$notReady = $requiredServices
do {
    $containers = @{}
    docker compose ps -a --format json | ForEach-Object {
        $item = $_ | ConvertFrom-Json
        $containers[$item.Service] = $item
    }
    Assert-LastExitCode "docker compose ps"

    $notReady = @()
    foreach ($service in $requiredServices) {
        if (-not $containers.ContainsKey($service)) {
            $notReady += $service
            continue
        }

        $container = $containers[$service]
        if ($container.State -eq "exited" -and $container.ExitCode -ne 0) {
            docker compose logs --tail 80 $service
            throw "$service exited with code $($container.ExitCode)"
        }
        if ($container.Health -eq "unhealthy") {
            docker compose logs --tail 80 $service
            throw "$service became unhealthy"
        }
        if ($container.State -ne "running" -or $container.Health -eq "starting") {
            $notReady += $service
        }
    }

    if ($notReady.Count -eq 0) {
        break
    }
    Start-Sleep -Seconds 5
} while ((Get-Date) -lt $deadline)

if ($notReady.Count -gt 0) {
    Write-Host "Services not ready: $($notReady -join ', ')"
    docker compose logs --tail 80 $notReady
    throw "Stack did not become ready within $TimeoutSeconds seconds"
}

Write-Host "[5/5] Running HTTP smoke checks..."
foreach ($uri in @(
    "http://localhost:8000/health",
    "http://localhost:5000/health",
    "http://localhost:8092/api/v2/monitor/health",
    "http://localhost:8090/v1/info",
    "http://localhost:9090/-/ready",
    "http://localhost:3000/api/health"
)) {
    Write-Host "  checking $uri"
    Wait-HttpEndpoint -Uri $uri -Deadline $deadline
}

Write-Host "Stack is ready. Run .\scripts\e2e_demo.ps1 for data and inference verification."
