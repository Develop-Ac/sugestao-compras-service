#!/bin/bash
# Script para configurar FreeTDS (alternativa mais simples)

# Verificar se as variáveis de ambiente estão definidas
if [ -z "$SQL_SERVER_HOST" ]; then
    SQL_SERVER_HOST="192.168.1.146"
fi

if [ -z "$SQL_SERVER_PORT" ]; then
    SQL_SERVER_PORT="1433"
fi

if [ -z "$SQL_SERVER_DATABASE" ]; then
    SQL_SERVER_DATABASE="master"
fi

if [ -z "$SQL_SERVER_USER" ]; then
    SQL_SERVER_USER="BI_AC"
fi

if [ -z "$SQL_SERVER_PASSWORD" ]; then
    SQL_SERVER_PASSWORD="Ac@2025acesso"
fi

echo "Configurando FreeTDS para SQL Server..."
echo "Host: ${SQL_SERVER_HOST}:${SQL_SERVER_PORT}"
echo "Database: ${SQL_SERVER_DATABASE}"
echo "User: ${SQL_SERVER_USER}"

# Verificar se o driver FreeTDS está disponível
if odbcinst -q -d | grep -q "FreeTDS"; then
    echo "Driver FreeTDS encontrado!"
    DRIVER_FOUND="FreeTDS"
else
    echo "ATENÇÃO: Driver FreeTDS não encontrado!"
    echo "Drivers disponíveis:"
    odbcinst -q -d
    DRIVER_FOUND="FreeTDS"
fi

# Configurar FreeTDS
cat > /etc/freetds/freetds.conf << EOF
[global]
    tds version = 8.0
    dump file = /tmp/freetds.log
    debug flags = 0xffff

[CONSULTA_SERVER]
    host = ${SQL_SERVER_HOST}
    port = ${SQL_SERVER_PORT}
    tds version = 8.0
EOF

# Criar configuração ODBC
cat > /etc/odbc.ini << EOF
[Default]
Driver = FreeTDS
Server = ${SQL_SERVER_HOST}
Port = ${SQL_SERVER_PORT}
Database = ${SQL_SERVER_DATABASE}
UID = ${SQL_SERVER_USER}
PWD = ${SQL_SERVER_PASSWORD}
TDS_Version = 8.0

[CONSULTA]
Driver = FreeTDS
Server = ${SQL_SERVER_HOST}
Port = ${SQL_SERVER_PORT}
Database = ${SQL_SERVER_DATABASE}
UID = ${SQL_SERVER_USER}
PWD = ${SQL_SERVER_PASSWORD}
TDS_Version = 8.0
EOF

echo "Configuração FreeTDS criada:"
echo "--- freetds.conf ---"
cat /etc/freetds/freetds.conf
echo ""
echo "--- odbc.ini ---"
cat /etc/odbc.ini

echo "Configuração FreeTDS concluída!"