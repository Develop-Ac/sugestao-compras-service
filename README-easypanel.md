# Configuração para EasyPanel

## Como usar no EasyPanel:

### 1. Criar o projeto no EasyPanel
1. Acesse seu painel EasyPanel
2. Clique em "Create Project"
3. Escolha "Docker Compose" ou "Dockerfile"

### 2. Upload dos arquivos
Faça upload dos seguintes arquivos para o EasyPanel:
- `Dockerfile`
- `docker-compose.yml`
- `requirements.txt`
- `sugestao_compra_api.py`
- `.dockerignore`

### 3. Configurar variáveis de ambiente
No painel do EasyPanel, configure as seguintes variáveis de ambiente:

```
POSTGRES_URL=postgresql://intranet:Ac%402025acesso@panel-teste.acacessorios.local:5555/intranet
FLASK_ENV=production
```

### 4. Configurar porta
- Porta interna: 5000
- Porta externa: conforme preferência (ex: 80 ou 443 para HTTPS)

### 5. Deploy
1. Clique em "Deploy"
2. Aguarde o build e deploy da aplicação
3. Acesse a URL fornecida pelo EasyPanel

### 6. Testar a API
Após o deploy, teste os endpoints:

- Health check: `GET /health`
- Verificar dados: `GET /dados-disponiveis`
- Sugestão de compra: `POST /sugestao-compra`

### Exemplo de requisição:
```bash
curl -X POST https://seu-dominio.com/sugestao-compra \
  -H "Content-Type: application/json" \
  -d '{
    "dias_compra": 30,
    "marca_descricao": "BOSCH"
  }'
```

### Configurações adicionais para EasyPanel:

#### Custom Domain (opcional)
Se quiser usar um domínio personalizado:
1. Vá em "Domains" no EasyPanel
2. Adicione seu domínio
3. Configure os registros DNS conforme instruído

#### SSL/HTTPS
O EasyPanel geralmente configura SSL automaticamente.

#### Logs
Os logs da aplicação estarão disponíveis na seção "Logs" do EasyPanel.

#### Monitoramento
Use o health check endpoint (`/health`) para monitoramento:
- Interval: 30s
- Timeout: 10s
- Retries: 3

### Troubleshooting:

1. **Erro de conexão com PostgreSQL:**
   - Verifique se a URL do PostgreSQL está correta
   - Confirme se o servidor PostgreSQL está acessível externamente

2. **Erro de conexão ODBC:**
   - O Dockerfile inclui configuração básica ODBC
   - Pode ser necessário ajustar a configuração do DSN no Dockerfile

3. **Erro de dependências:**
   - Verifique se todas as dependências estão no requirements.txt
   - Confirme se as versões são compatíveis

### Estrutura final no EasyPanel:
```
projeto/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── sugestao_compra_api.py
├── .dockerignore
└── README-easypanel.md (este arquivo)
```