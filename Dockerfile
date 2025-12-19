FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema necessárias para PostgreSQL e ODBC
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Configurar DSN ODBC para Firebird (se necessário)
# Criar arquivo odbc.ini
RUN echo "[CONSULTA]" > /etc/odbc.ini && \
    echo "Driver = FreeTDS" >> /etc/odbc.ini && \
    echo "Description = Firebird Database" >> /etc/odbc.ini && \
    echo "Server = your-firebird-server" >> /etc/odbc.ini && \
    echo "Port = 3050" >> /etc/odbc.ini && \
    echo "Database = /path/to/your/database.fdb" >> /etc/odbc.ini

# Copiar requirements.txt primeiro para cache de dependências
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY sugestao_compra_api.py .

# Expor a porta da aplicação
EXPOSE 5000

# Variáveis de ambiente
ENV FLASK_APP=sugestao_compra_api.py
ENV FLASK_ENV=production
ENV PYTHONPATH=/app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Comando para executar a aplicação
CMD ["python", "sugestao_compra_api.py"]