#!/bin/bash

# Script para build e deploy da API Go

set -e

echo "=== Build e Deploy da API de Sugestão de Compra (Go) ==="

# Verificar se Docker está rodando
if ! docker info > /dev/null 2>&1; then
    echo "Erro: Docker não está rodando"
    exit 1
fi

# Build da imagem
echo "Fazendo build da imagem Docker..."
docker build -f Dockerfile.go -t sugestao-compra-go:latest .

echo "Build concluído com sucesso!"

# Verificar se a imagem foi criada
if docker images | grep -q "sugestao-compra-go"; then
    echo "Imagem 'sugestao-compra-go:latest' criada com sucesso"
else
    echo "Erro: Imagem não foi criada"
    exit 1
fi

# Opcional: rodar container para teste
echo ""
echo "Para testar a aplicação localmente, execute:"
echo "docker run -p 8080:8080 sugestao-compra-go:latest"
echo ""
echo "Ou use docker-compose:"
echo "docker-compose -f docker-compose.go.yml up"
echo ""
echo "API estará disponível em: http://localhost:8080"
echo "Health check: http://localhost:8080/health"
echo "Endpoint principal: POST http://localhost:8080/sugestao-compra"