#!/bin/bash
# Script para configurar ODBC do SQL Server e OPENQUERY no container

# Verificar se as variáveis de ambiente estão definidas
if [ -z "$SQL_SERVER_HOST" ]; then
    echo "ATENÇÃO: SQL_SERVER_HOST não definido, usando valor padrão"
    SQL_SERVER_HOST="192.168.1.146"
fi

if [ -z "$SQL_SERVER_PORT" ]; then
    echo "ATENÇÃO: SQL_SERVER_PORT não definido, usando 1433"
    SQL_SERVER_PORT="1433"
fi

if [ -z "$SQL_SERVER_DATABASE" ]; then
    echo "ATENÇÃO: SQL_SERVER_DATABASE não definido, usando master"
    SQL_SERVER_DATABASE="master"
fi

if [ -z "$SQL_SERVER_USER" ]; then
    echo "ATENÇÃO: SQL_SERVER_USER não definido, usando BI_AC"
    SQL_SERVER_USER="BI_AC"
fi

if [ -z "$SQL_SERVER_PASSWORD" ]; then
    echo "ATENÇÃO: SQL_SERVER_PASSWORD não definido, usando valor padrão"
    SQL_SERVER_PASSWORD="Ac@2025acesso"
fi

echo "Configurando SQL Server ODBC..."
echo "Host: ${SQL_SERVER_HOST}:${SQL_SERVER_PORT}"
echo "Database: ${SQL_SERVER_DATABASE}"
echo "User: ${SQL_SERVER_USER}"

# Verificar se algum driver SQL Server está disponível
DRIVER_FOUND=""
if odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
    DRIVER_FOUND="ODBC Driver 18 for SQL Server"
    echo "Driver ODBC 18 encontrado!"
elif odbcinst -q -d | grep -q "ODBC Driver 17 for SQL Server"; then
    DRIVER_FOUND="ODBC Driver 17 for SQL Server"
    echo "Driver ODBC 17 encontrado!"
else
    echo "ERRO: Nenhum driver SQL Server encontrado!"
    echo "Drivers disponíveis:"
    odbcinst -q -d
    echo "Tentando continuar mesmo assim..."
    DRIVER_FOUND="ODBC Driver 18 for SQL Server"
fi

echo "Usando driver: $DRIVER_FOUND"

# Criar configuração ODBC para o SQL Server principal
cat > /etc/odbc.ini << EOF
[Default]
Driver = $DRIVER_FOUND
Server = ${SQL_SERVER_HOST},${SQL_SERVER_PORT}
Database = ${SQL_SERVER_DATABASE}
UID = ${SQL_SERVER_USER}
PWD = ${SQL_SERVER_PASSWORD}
TrustServerCertificate = yes
Encrypt = no

[CONSULTA]
Driver = $DRIVER_FOUND
Server = ${SQL_SERVER_HOST},${SQL_SERVER_PORT}
Database = ${SQL_SERVER_DATABASE}
UID = ${SQL_SERVER_USER}
PWD = ${SQL_SERVER_PASSWORD}
TrustServerCertificate = yes
Encrypt = no
EOF

echo "Configuração ODBC criada:"
cat /etc/odbc.ini

# Verificar conectividade
echo "Testando conectividade SQL Server..."
if command -v sqlcmd &> /dev/null; then
    sqlcmd -S ${SQL_SERVER_HOST},${SQL_SERVER_PORT} -U ${SQL_SERVER_USER} -P ${SQL_SERVER_PASSWORD} -d ${SQL_SERVER_DATABASE} -Q "SELECT 1 as test" -W
else
    echo "sqlcmd não disponível para teste"
fi

echo "Configuração SQL Server concluída!"