FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema necessárias para PostgreSQL e SQL Server ODBC
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    curl \
    gnupg2 \
    && rm -rf /var/lib/apt/lists/*

# Instalar Microsoft ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements.txt e script de configuração primeiro para cache de dependências
COPY requirements.txt .
COPY configure_odbc.sh .

# Dar permissão de execução ao script
RUN chmod +x configure_odbc.sh

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
CMD ["sh", "-c", "./configure_odbc.sh && python sugestao_compra_api.py"]