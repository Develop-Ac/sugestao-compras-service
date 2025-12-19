# Script de build para EasyPanel (PowerShell)
# Execute este script para preparar o deploy

Write-Host "=== Preparando deploy para EasyPanel ===" -ForegroundColor Green

# Verificar se os arquivos necessários existem
$requiredFiles = @(
    "sugestao_compra_api.py",
    "Dockerfile", 
    "requirements.txt",
    "docker-compose.yml"
)

foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        Write-Host "❌ Arquivo $file não encontrado!" -ForegroundColor Red
        exit 1
    }
}

Write-Host "✅ Todos os arquivos necessários encontrados" -ForegroundColor Green

# Testar conexão PostgreSQL (opcional)
Write-Host "🧪 Testando conexão PostgreSQL..." -ForegroundColor Yellow

try {
    & python -c @"
import sys
sys.path.append('.')
from sugestao_compra_api import get_postgres_engine
try:
    engine = get_postgres_engine()
    with engine.connect() as conn:
        print('✅ Conexão PostgreSQL OK')
except Exception as e:
    print(f'⚠️ Aviso: Erro na conexão PostgreSQL: {e}')
    print('   (Isso é esperado se não estiver na rede local)')
"@
} catch {
    Write-Host "⚠️ Não foi possível testar a conexão PostgreSQL" -ForegroundColor Yellow
}

# Criar arquivo zip para upload no EasyPanel
Write-Host "📦 Criando arquivo para deploy..." -ForegroundColor Blue

$filesToZip = @(
    "sugestao_compra_api.py",
    "Dockerfile",
    "Dockerfile.production", 
    "requirements.txt",
    "docker-compose.yml",
    ".dockerignore",
    "README-easypanel.md"
)

# Remover arquivo zip existente se houver
if (Test-Path "sugestao-compra-api.zip") {
    Remove-Item "sugestao-compra-api.zip"
}

# Criar o zip
Compress-Archive -Path $filesToZip -DestinationPath "sugestao-compra-api.zip"

Write-Host "✅ Arquivo sugestao-compra-api.zip criado!" -ForegroundColor Green

Write-Host ""
Write-Host "=== Próximos passos ===" -ForegroundColor Cyan
Write-Host "1. Faça upload do arquivo sugestao-compra-api.zip no EasyPanel"
Write-Host "2. Configure as variáveis de ambiente:"
Write-Host "   POSTGRES_URL=postgresql://intranet:Ac%402025acesso@panel-teste.acacessorios.local:5555/intranet"
Write-Host "3. Configure a porta: 5000" 
Write-Host "4. Faça o deploy!"
Write-Host ""
Write-Host "🔗 Endpoints disponíveis após deploy:" -ForegroundColor Yellow
Write-Host "   GET  /health - Health check"
Write-Host "   GET  /dados-disponiveis - Verificar dados no PostgreSQL"
Write-Host "   POST /sugestao-compra - Gerar sugestão de compra"
Write-Host "   GET  /sugestao-compra - Sugestão via query parameters"

Write-Host ""
Write-Host "💡 Para testar localmente primeiro:" -ForegroundColor Magenta
Write-Host "   docker-compose up --build"