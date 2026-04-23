# Validation script for monitoring stack
Write-Host "`n==================================================" -ForegroundColor Cyan
Write-Host "   VALIDATING MONITORING STACK" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# 1. Check metrics endpoint
Write-Host "`n[1] Checking metrics endpoint..." -ForegroundColor Yellow
try {
    $metrics = Invoke-RestMethod -Uri "http://localhost:8000/metrics" -TimeoutSec 5
    if ($metrics -match "crawler_urls_total") {
        Write-Host "    ? Metrics endpoint OK" -ForegroundColor Green
    } else {
        Write-Host "    ?? Metrics endpoint responding but missing custom metrics" -ForegroundColor Yellow
    }
} catch {
    Write-Host "    ? Metrics endpoint failed: $_" -ForegroundColor Red
}

# 2. Check Prometheus
Write-Host "`n[2] Checking Prometheus..." -ForegroundColor Yellow
try {
    $targets = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/targets" -TimeoutSec 5
    Write-Host "    ? Prometheus is running" -ForegroundColor Green
} catch {
    Write-Host "    ? Prometheus not responding" -ForegroundColor Red
}

# 3. Check Grafana
Write-Host "`n[3] Checking Grafana..." -ForegroundColor Yellow
try {
    Invoke-RestMethod -Uri "http://localhost:3000/api/health" -TimeoutSec 5 | Out-Null
    Write-Host "    ? Grafana is running" -ForegroundColor Green
} catch {
    Write-Host "    ? Grafana not responding" -ForegroundColor Red
}

# 4. Check queue metrics via Python
Write-Host "`n[4] Checking queue metrics..." -ForegroundColor Yellow
python -c @"
import sys
sys.path.insert(0, '.')
from src.monitoring.metrics import MetricsCollector
MetricsCollector.update_queue_metrics()
from src.monitoring.metrics import crawler_queue_pending
print(f'    Queue pending: {crawler_queue_pending._value.get()}')
"@ 2>$null

Write-Host "`n==================================================" -ForegroundColor Cyan
Write-Host "   ? VALIDATION COMPLETE" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "?? Grafana: http://localhost:3000 (admin/admin)" -ForegroundColor Cyan
Write-Host "?? Prometheus: http://localhost:9090" -ForegroundColor Cyan
Write-Host "?? Metrics: http://localhost:8000/metrics" -ForegroundColor Cyan
