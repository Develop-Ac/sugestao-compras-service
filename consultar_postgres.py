"""
Script para consultar os dados salvos no PostgreSQL
"""
import pandas as pd
from sqlalchemy import create_engine, text
import datetime

# Configurações PostgreSQL (mesmas do script principal)
POSTGRES_URL = "postgresql://intranet:Ac%402025acesso@panel-teste.acacessorios.local:5555/intranet"
TABELA_FIFO = "com_fifo_completo"

def get_postgres_engine():
    """Cria engine do SQLAlchemy para PostgreSQL"""
    return create_engine(POSTGRES_URL)

def consultar_dados(tipo_dados=None, data_inicio=None, data_fim=None, pro_codigo=None):
    """
    Consulta dados da tabela com_fifo_completo
    
    Args:
        tipo_dados: Filtro por tipo ('ANALISE_ATUAL', 'RELATORIO_MUDANCAS', 'FIFO_SALDO')
        data_inicio: Data início para filtro (formato YYYY-MM-DD)
        data_fim: Data fim para filtro (formato YYYY-MM-DD)
        pro_codigo: Código do produto para filtro
    """
    
    engine = get_postgres_engine()
    
    # Monta a consulta SQL usando f-strings (mais seguro aqui pois controlamos os inputs)
    query = f"SELECT * FROM {TABELA_FIFO} WHERE 1=1"
    
    if tipo_dados:
        query += f" AND tipo_dados = '{tipo_dados}'"
    
    if data_inicio:
        query += f" AND data_processamento >= '{data_inicio}'"
        
    if data_fim:
        query += f" AND data_processamento <= '{data_fim}'"
        
    if pro_codigo:
        query += f" AND pro_codigo = '{pro_codigo}'"
    
    query += " ORDER BY data_processamento DESC, tipo_dados, pro_codigo"
    
    try:
        df = pd.read_sql(query, engine)
        print(f"Encontrados {len(df)} registros")
        return df
    except Exception as e:
        print(f"Erro na consulta: {e}")
        return pd.DataFrame()

def listar_processamentos():
    """Lista todas as datas de processamento disponíveis"""
    engine = get_postgres_engine()
    
    query = """
    SELECT 
        DATE(data_processamento) as data,
        tipo_dados,
        COUNT(*) as quantidade_registros
    FROM com_fifo_completo 
    GROUP BY DATE(data_processamento), tipo_dados
    ORDER BY data DESC, tipo_dados
    """
    
    try:
        df = pd.read_sql(query, engine)
        print("Processamentos disponíveis:")
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"Erro ao listar processamentos: {e}")
        return pd.DataFrame()

def estatisticas_tabela():
    """Mostra estatísticas da tabela"""
    engine = get_postgres_engine()
    
    query = """
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT DATE(data_processamento)) as dias_processamento,
        MIN(data_processamento) as primeiro_processamento,
        MAX(data_processamento) as ultimo_processamento,
        COUNT(DISTINCT pro_codigo) as produtos_distintos
    FROM com_fifo_completo
    """
    
    try:
        df = pd.read_sql(query, engine)
        print("Estatísticas da tabela:")
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"Erro ao obter estatísticas: {e}")
        return pd.DataFrame()

def exportar_para_excel(df, nome_arquivo=None):
    """Exporta DataFrame para Excel"""
    if df.empty:
        print("Nenhum dado para exportar")
        return
    
    if nome_arquivo is None:
        nome_arquivo = f"consulta_fifo_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    try:
        df.to_excel(nome_arquivo, index=False)
        print(f"Dados exportados para: {nome_arquivo}")
    except Exception as e:
        print(f"Erro ao exportar: {e}")

if __name__ == "__main__":
    print("=== CONSULTA DADOS FIFO NO POSTGRESQL ===\n")
    
    # Mostra estatísticas gerais
    print("1. ESTATÍSTICAS GERAIS:")
    estatisticas_tabela()
    print("\n" + "="*50 + "\n")
    
    # Lista processamentos
    print("2. PROCESSAMENTOS DISPONÍVEIS:")
    listar_processamentos()
    print("\n" + "="*50 + "\n")
    
    # Exemplos de consultas
    print("3. EXEMPLOS DE CONSULTAS:")
    
    # Última análise atual
    print("\n3.1. Última análise atual (primeiros 5 registros):")
    df_atual = consultar_dados(tipo_dados='ANALISE_ATUAL')
    if not df_atual.empty:
        print(df_atual.head()[['pro_codigo', 'pro_descricao', 'estoque_disponivel', 'data_processamento']].to_string(index=False))
    
    # Último FIFO
    print("\n3.2. Último FIFO (primeiros 5 registros):")
    df_fifo = consultar_dados(tipo_dados='FIFO_SALDO')
    if not df_fifo.empty:
        print(df_fifo.head()[['pro_codigo', 'pro_descricao', 'saldo_fifo', 'data_processamento']].to_string(index=False))
    
    print("\n" + "="*50)
    print("Para consultas personalizadas, use as funções:")
    print("- consultar_dados(tipo_dados='ANALISE_ATUAL')")
    print("- consultar_dados(data_inicio='2024-12-01')")
    print("- consultar_dados(pro_codigo='CODIGO_PRODUTO')")