# Script de Deploy para API Sugestão de Compra
# Execute este script após verificar as configurações no docker-compose.yml

Write-Host "=== Deploy API Sugestão de Compra (FreeTDS) ===" -ForegroundColor Green

# Verificar se docker-compose existe
if (!(Test-Path "docker-compose.yml")) {
    Write-Host "ERRO: arquivo docker-compose.yml não encontrado!" -ForegroundColor Red
    exit 1
}

Write-Host "Usando FreeTDS como driver SQL Server (mais compatível)..." -ForegroundColor Yellow

Write-Host "1. Parando containers existentes..." -ForegroundColor Yellow
docker-compose down

Write-Host "2. Construindo nova imagem (sem cache)..." -ForegroundColor Yellow
docker-compose build --no-cache

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Falha na construção da imagem!" -ForegroundColor Red
    exit 1
}

Write-Host "3. Iniciando containers..." -ForegroundColor Yellow
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Falha ao iniciar containers!" -ForegroundColor Red
    exit 1
}

Write-Host "4. Aguardando inicialização..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "5. Verificando status dos containers..." -ForegroundColor Yellow
docker-compose ps

Write-Host "6. Testando endpoint de diagnóstico..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "http://localhost:5000/diagnostico" -Method GET -TimeoutSec 30
    Write-Host "Diagnóstico executado com sucesso!" -ForegroundColor Green
    
    # Mostrar status das conexões
    Write-Host "`nStatus das Conexões:" -ForegroundColor Cyan
    Write-Host "  SQL Server: $($response.sql_server.status) - $($response.sql_server.message)" -ForegroundColor $(if($response.sql_server.status -eq "ok") {"Green"} else {"Red"})
    Write-Host "  PostgreSQL: $($response.postgresql.status) - $($response.postgresql.message)" -ForegroundColor $(if($response.postgresql.status -eq "ok") {"Green"} else {"Red"})
    
    if ($response.sql_server.drivers.Count -gt 0) {
        Write-Host "  Drivers FreeTDS encontrados: $($response.sql_server.drivers -join ', ')" -ForegroundColor Green
    } else {
        Write-Host "  ATENÇÃO: Nenhum driver FreeTDS encontrado!" -ForegroundColor Red
    }
    
} catch {
    Write-Host "ERRO ao testar diagnóstico: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Verificando logs do container..." -ForegroundColor Yellow
    docker-compose logs --tail=20 sugestao-compra-api
}

Write-Host "`n7. Para ver logs em tempo real, execute:" -ForegroundColor Cyan
Write-Host "   docker-compose logs -f sugestao-compra-api" -ForegroundColor White

Write-Host "`n8. Para testar a API, acesse:" -ForegroundColor Cyan
Write-Host "   Health Check: http://localhost:5000/health" -ForegroundColor White
Write-Host "   Diagnóstico:  http://localhost:5000/diagnostico" -ForegroundColor White
Write-Host "   API Principal: http://localhost:5000/sugestao-compra" -ForegroundColor White

Write-Host "`n=== Deploy Concluído ===" -ForegroundColor Green