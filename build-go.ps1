# Script para build e deploy da API Go (Windows)

Write-Host "=== Build e Deploy da API de Sugestão de Compra (Go) ===" -ForegroundColor Green

# Verificar se Docker está rodando
try {
    docker info > $null 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Docker não está rodando"
    }
} catch {
    Write-Host "Erro: Docker não está rodando" -ForegroundColor Red
    exit 1
}

# Build da imagem
Write-Host "Fazendo build da imagem Docker..." -ForegroundColor Yellow
docker build -f Dockerfile.go -t sugestao-compra-go:latest .

if ($LASTEXITCODE -eq 0) {
    Write-Host "Build concluído com sucesso!" -ForegroundColor Green
} else {
    Write-Host "Erro no build da imagem" -ForegroundColor Red
    exit 1
}

# Verificar se a imagem foi criada
$imageExists = docker images | Select-String "sugestao-compra-go"
if ($imageExists) {
    Write-Host "Imagem 'sugestao-compra-go:latest' criada com sucesso" -ForegroundColor Green
} else {
    Write-Host "Erro: Imagem não foi criada" -ForegroundColor Red
    exit 1
}

# Instruções para uso
Write-Host ""
Write-Host "Para testar a aplicação localmente, execute:" -ForegroundColor Cyan
Write-Host "docker run -p 8080:8080 sugestao-compra-go:latest" -ForegroundColor White
Write-Host ""
Write-Host "Ou use docker-compose:" -ForegroundColor Cyan
Write-Host "docker-compose -f docker-compose.go.yml up" -ForegroundColor White
Write-Host ""
Write-Host "API estará disponível em: http://localhost:8080" -ForegroundColor Green
Write-Host "Health check: http://localhost:8080/health" -ForegroundColor Green
Write-Host "Endpoint principal: POST http://localhost:8080/sugestao-compra" -ForegroundColor Green