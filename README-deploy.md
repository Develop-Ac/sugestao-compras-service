# Configuração para Deploy da API Sugestão de Compra

## Alterações Realizadas

A aplicação foi convertida de **ODBC Firebird** para **SQL Server com OPENQUERY**:

- **Servidor:** 192.168.1.146:1433  
- **Banco:** master
- **Usuário:** BI_AC
- **Senha:** Ac@2025acesso

## Problema Resolvido

O erro anterior `'Adaptive Server is unavailable or does not exist'` foi resolvido com:

1. Conversão de Firebird para SQL Server
2. Implementação de OPENQUERY para consultas
3. Configuração adequada do ODBC Driver 17 for SQL Server no Docker
4. Remoção de dependências do Firebird

## Como Funciona o OPENQUERY

As consultas agora usam OPENQUERY da seguinte forma:
```sql
SELECT * FROM OPENQUERY (
    CONSULTA,
    'SELECT ... FROM tabela WHERE ...'
)
```

Isso permite executar consultas no servidor remoto através do linked server configurado.

## Configurações no Docker

O `docker-compose.yml` já está configurado com:
```yaml
environment:
  - SQL_SERVER_HOST=192.168.1.146
  - SQL_SERVER_PORT=1433  
  - SQL_SERVER_DATABASE=master
  - SQL_SERVER_USER=BI_AC
  - SQL_SERVER_PASSWORD=Ac@2025acesso
```

## Como Fazer o Deploy

1. **Execute o script de deploy**:
   ```powershell
   .\deploy.ps1
   ```

2. **Verifique os logs**:
   ```bash
   docker-compose logs -f sugestao-compra-api
   ```

## Testes de Conectividade

O script de inicialização irá:
- Instalar automaticamente o ODBC Driver 17 for SQL Server
- Configurar os DSNs necessários
- Testar a conectividade com o SQL Server

## Endpoints Disponíveis

- **Health Check:** http://localhost:5000/health
- **Diagnóstico:** http://localhost:5000/diagnostico  
- **API Principal:** http://localhost:5000/sugestao-compra

## Troubleshooting

### Se der erro de conexão SQL Server:
1. Verifique se o SQL Server está acessível: `telnet 192.168.1.146 1433`
2. Confirme se as credenciais estão corretas
3. Verifique se o linked server "CONSULTA" está configurado no SQL Server
4. Teste manualmente:
   ```bash
   docker exec -it <container> bash
   sqlcmd -S 192.168.1.146,1433 -U BI_AC -P 'Ac@2025acesso' -d master
   ```

### Logs úteis:
```bash
# Ver configuração ODBC criada
docker-compose logs sugestao-compra-api | grep "Configuração SQL Server"

# Ver erros de OPENQUERY
docker-compose logs sugestao-compra-api | grep -i "openquery\|error"

# Ver status completo
docker-compose logs --tail=50 sugestao-compra-api
```

### Verificar linked server no SQL Server:
```sql
-- No SQL Server, verifique se o linked server existe
SELECT * FROM sys.servers WHERE name = 'CONSULTA'

-- Teste o linked server
SELECT * FROM OPENQUERY(CONSULTA, 'SELECT 1 as teste')
```