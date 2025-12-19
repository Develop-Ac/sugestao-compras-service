#!/bin/bash

# Script de build para EasyPanel
# Execute este script para preparar o deploy

echo "=== Preparando deploy para EasyPanel ==="

# Verificar se os arquivos necessários existem
required_files=("sugestao_compra_api.py" "Dockerfile" "requirements.txt" "docker-compose.yml")

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Arquivo $file não encontrado!"
        exit 1
    fi
done

echo "✅ Todos os arquivos necessários encontrados"

# Testar se a API funciona localmente (opcional)
echo "🧪 Testando conexão PostgreSQL..."
python3 -c "
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
"

# Criar arquivo zip para upload no EasyPanel
echo "📦 Criando arquivo para deploy..."
zip -r sugestao-compra-api.zip \
    sugestao_compra_api.py \
    Dockerfile \
    Dockerfile.production \
    requirements.txt \
    docker-compose.yml \
    .dockerignore \
    README-easypanel.md

echo "✅ Arquivo sugestao-compra-api.zip criado!"

echo ""
echo "=== Próximos passos ==="
echo "1. Faça upload do arquivo sugestao-compra-api.zip no EasyPanel"
echo "2. Configure as variáveis de ambiente:"
echo "   POSTGRES_URL=postgresql://intranet:Ac%402025acesso@panel-teste.acacessorios.local:5555/intranet"
echo "3. Configure a porta: 5000"
echo "4. Faça o deploy!"
echo ""
echo "🔗 Endpoints disponíveis após deploy:"
echo "   GET  /health - Health check"
echo "   GET  /dados-disponiveis - Verificar dados no PostgreSQL"  
echo "   POST /sugestao-compra - Gerar sugestão de compra"
echo "   GET  /sugestao-compra - Sugestão via query parameters"