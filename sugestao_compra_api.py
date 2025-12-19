from flask import Flask, request, jsonify
import pyodbc
import pandas as pd
import numpy as np
import os
# Imports para PostgreSQL
from sqlalchemy import create_engine, text
import warnings

# ============================
# CONFIGURAÇÕES
# ============================

ARQ_ENTRADA = "resultado_fifo_completo.xlsx"
ARQ_SAIDA   = "resultado_fifo_sugestao.xlsx"

# Configurações SQL Server
SQL_SERVER_HOST = "192.168.1.146"
SQL_SERVER_PORT = "1433"
SQL_SERVER_DATABASE = "master"
SQL_SERVER_USER = "BI_AC"
SQL_SERVER_PASSWORD = "Ac@2025acesso"

# Configurações PostgreSQL
POSTGRES_URL = "postgresql://intranet:Ac%402025acesso@panel-teste.acacessorios.local:5555/intranet"
TABELA_FIFO = "com_fifo_completo"

# Quantos dias de estoque você quer manter ALÉM do lead time
DIAS_ESTOQUE_DESEJADO = 90

# Prazo logístico (dias entre pedido e chegada da mercadoria)
LEAD_TIME_DIAS = 17

# Empresa fixa (travada em 3)
EMPRESA_PEDIDO  = 3

app = Flask(__name__)


# ============================
# CONEXÃO SQL SERVER
# ============================

def get_connection():
    """Conecta ao SQL Server com tratamento de erro melhorado"""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER_HOST},{SQL_SERVER_PORT};"
        f"DATABASE={SQL_SERVER_DATABASE};"
        f"UID={SQL_SERVER_USER};"
        f"PWD={SQL_SERVER_PASSWORD};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=no;"
    )
    
    try:
        print(f"Tentando conectar com SQL Server: {SQL_SERVER_HOST}:{SQL_SERVER_PORT}")
        
        # Verificar se drivers estão disponíveis
        drivers = [driver for driver in pyodbc.drivers() if 'sql server' in driver.lower()]
        print(f"Drivers SQL Server disponíveis: {drivers}")
        
        if not drivers:
            print("ATENÇÃO: Nenhum driver SQL Server encontrado!")
            print(f"Todos os drivers: {pyodbc.drivers()}")
        
        conn = pyodbc.connect(conn_str)
        print("Conexão SQL Server estabelecida com sucesso!")
        return conn
        
    except pyodbc.Error as e:
        print(f"Erro de conexão SQL Server: {e}")
        print(f"Host: {SQL_SERVER_HOST}:{SQL_SERVER_PORT}")
        print(f"Database: {SQL_SERVER_DATABASE}")
        print(f"User: {SQL_SERVER_USER}")
        
        raise e
    except Exception as e:
        print(f"Erro geral de conexão: {type(e).__name__}: {e}")
        raise e


def get_postgres_engine():
    """Cria engine do SQLAlchemy para PostgreSQL"""
    return create_engine(POSTGRES_URL)


def executar_openquery(sql_interno):
    """Executa uma consulta via OPENQUERY no SQL Server"""
    conn = get_connection()
    
    # Escapa aspas simples na consulta interna
    sql_interno_escaped = sql_interno.replace("'", "''")
    
    # Monta a consulta OPENQUERY
    sql_openquery = f"""
    SELECT * FROM OPENQUERY (
        CONSULTA,
        '{sql_interno_escaped}'
    )
    """
    
    try:
        print(f"Executando OPENQUERY...")
        print(f"SQL interno: {sql_interno[:200]}...")
        df = pd.read_sql(sql_openquery, conn)
        print(f"OPENQUERY executado com sucesso. {len(df)} registros retornados.")
        return df
    except Exception as e:
        print(f"Erro no OPENQUERY: {e}")
        print(f"SQL completo: {sql_openquery}")
        raise e
    finally:
        conn.close()


def carregar_analise_atual_postgres():
    """
    Carrega os dados mais recentes do tipo 'ANALISE_ATUAL' do PostgreSQL
    """
    try:
        engine = get_postgres_engine()
        
        # Query para pegar a análise mais recente
        query = """
        SELECT * FROM com_fifo_completo 
        WHERE tipo_dados = 'ANALISE_ATUAL'
        AND data_processamento = (
            SELECT MAX(data_processamento) 
            FROM com_fifo_completo 
            WHERE tipo_dados = 'ANALISE_ATUAL'
        )
        ORDER BY pro_codigo
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("Nenhum dado encontrado na tabela com_fifo_completo do tipo ANALISE_ATUAL")
            return None
        
        print(f"Carregados {len(df)} registros da análise FIFO mais recente do PostgreSQL")
        print(f"Data do processamento: {df['data_processamento'].iloc[0]}")
        
        # Mapeia as colunas do banco para o formato esperado pela API
        column_mapping = {
            'pro_codigo': 'PRO_CODIGO',
            'pro_descricao': 'PRO_DESCRICAO',
            'subgrp_codigo': 'SUBGRP_CODIGO',
            'mar_descricao': 'MAR_DESCRICAO',
            'fornecedor1': 'FORNECEDOR1',
            'fornecedor2': 'FORNECEDOR2',
            'fornecedor3': 'FORNECEDOR3',
            'estoque_disponivel': 'ESTOQUE_DISPONIVEL',
            'valor_custo_unitario': 'VALOR_CUSTO_UNITARIO',
            'valor_total_custo': 'VALOR_TOTAL_CUSTO',
            'qtd_vendida_periodo': 'QTD_VENDIDA_PERIODO',
            'valor_vendido_periodo': 'VALOR_VENDIDO_PERIODO',
            'data_ultima_venda': 'DATA_ULTIMA_VENDA',
            'margem_lucro': 'MARGEM_LUCRO',
            'giro_estoque': 'GIRO_ESTOQUE',
            'abc_vendas': 'ABC_VENDAS',
            'abc_estoque': 'ABC_ESTOQUE',
            'abc_margem': 'ABC_MARGEM',
            'classificacao_geral': 'CLASSIFICACAO_GERAL',
            'recomendacao': 'RECOMENDACAO'
        }
        
        # Renomeia as colunas
        df = df.rename(columns=column_mapping)
        
        # Remove colunas de controle que não são necessárias para a análise
        columns_to_remove = ['id', 'data_processamento', 'tipo_dados', 'created_at']
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
        
        # Converte PRO_CODIGO para string para evitar problemas de merge
        if 'PRO_CODIGO' in df.columns:
            df['PRO_CODIGO'] = df['PRO_CODIGO'].astype(str)
        
        # Adiciona colunas que podem estar faltando com valores padrão
        required_columns = {
            'ESTOQUE_MIN_SUGERIDO': 0,
            'ESTOQUE_MAX_SUGERIDO': 0,
            'TIPO_PLANEJAMENTO': 'Normal',
            'ALERTA_TENDENCIA_ALTA': 'Não',
            'CURVA_ABC': 'C',
            'DEMANDA_MEDIA_DIA': 0,
            'DEMANDA_MEDIA_DIA_AJUSTADA': 0,
            'NUM_VENDAS': 0
        }
        
        for col, default_value in required_columns.items():
            if col not in df.columns:
                df[col] = default_value
                print(f"Adicionada coluna {col} com valor padrão: {default_value}")
        
        return df
        
    except Exception as e:
        print(f"Erro ao carregar dados do PostgreSQL: {e}")
        return None


def carregar_itens_pedido(pedido_cotacao, empresa, marca_descricao=None):
    """
    Carrega os itens usando OPENQUERY aplicando filtros dinâmicos:
      - pedido_cotacao: opcional (None = sem filtro)
      - empresa: obrigatório
      - marca_descricao: opcional, filtra mar.mar_descricao com LIKE case-insensitive
    """
    
    # Monta a consulta SQL interna para OPENQUERY
    sql_interno = f"""
        SELECT
            pedi.pedido_cotacao,
            pedi.pro_codigo,
            pedi.quantidade,
            mar.mar_descricao
        FROM pedidos_cotacoes_itens pedi
        LEFT JOIN produtos pro
            ON pro.empresa   = pedi.empresa
            AND pro.pro_codigo = pedi.pro_codigo
        LEFT JOIN marcas mar
            ON mar.empresa    = pro.empresa
            AND mar.mar_codigo = pro.mar_codigo
        WHERE pedi.empresa = {empresa}
    """

    if pedido_cotacao is not None:
        sql_interno += f"\n          AND pedi.pedido_cotacao = {pedido_cotacao}"

    if marca_descricao:
        # Escapa aspas simples para OPENQUERY
        marca_escaped = marca_descricao.replace("'", "''")
        sql_interno += f"\n          AND UPPER(mar.mar_descricao) LIKE '%{marca_escaped.upper()}%'"

    # Executa via OPENQUERY
    df_ped = executar_openquery(sql_interno)

    # Converte PRO_CODIGO para string ANTES de qualquer operação
    if 'pro_codigo' in df_ped.columns:
        df_ped['pro_codigo'] = df_ped['pro_codigo'].astype(str)
    if 'PRO_CODIGO' in df_ped.columns:
        df_ped['PRO_CODIGO'] = df_ped['PRO_CODIGO'].astype(str)

    # Normalizar nomes de colunas
    df_ped = df_ped.rename(columns={
        "pro_codigo": "PRO_CODIGO",
        "PRO_CODIGO": "PRO_CODIGO",
        "quantidade": "QTD_PEDIDO",
        "QUANTIDADE": "QTD_PEDIDO",
        "mar_descricao": "MAR_DESCRICAO",
        "MAR_DESCRICAO": "MAR_DESCRICAO",
    })

    # Garante que PRO_CODIGO seja string após rename também
    if 'PRO_CODIGO' in df_ped.columns:
        df_ped['PRO_CODIGO'] = df_ped['PRO_CODIGO'].astype(str)

    # Evitar duplicar MAR_DESCRICAO no merge com métricas (que já tem marca)
    if "MAR_DESCRICAO" in df_ped.columns:
        df_ped = df_ped.drop(columns=["MAR_DESCRICAO"])

    return df_ped

# ============================
# LÓGICA DE SUGESTÃO
# ============================

@app.route('/sugestao-compra', methods=['POST'])
def api_sugestao_compra():
    """
    API endpoint que recebe parâmetros no body e retorna sugestão de compra
    
    Body JSON:
    {
        "pedido_cotacao": int (opcional),
        "marca_descricao": str (opcional),
        "dias_compra": int (opcional, padrão 30)
    }
    
    Retorno:
    {
        "success": bool,
        "data": [
            {
                "PRO_CODIGO": str,
                "QTD_SUGERIDA": int
            }
        ],
        "message": str
    }
    """
    try:
        # Obter dados do body de forma mais flexível
        data = {}
        
        # Verificar se há conteúdo no body
        if request.content_length and request.content_length > 0:
            try:
                # Tentar obter JSON
                data = request.get_json(force=True) or {}
            except Exception as json_error:
                # Se não conseguir fazer parse do JSON, retornar erro específico
                return jsonify({
                    "success": False,
                    "data": [],
                    "message": f"Erro no JSON: {str(json_error)}. Certifique-se de enviar um JSON válido ou deixe o body vazio para usar valores padrão."
                }), 400
        
        # Se não há body ou está vazio, usar valores padrão
        if not data:
            data = {}
        
        # Log para debug
        print(f"DEBUG - Dados recebidos: {data}")
        print(f"DEBUG - Content-Type: {request.content_type}")
        print(f"DEBUG - Content-Length: {request.content_length}")
        
        # Validar tipos dos parâmetros
        pedido_cotacao = data.get('pedido_cotacao')
        if pedido_cotacao is not None:
            try:
                pedido_cotacao = int(pedido_cotacao)
            except (ValueError, TypeError):
                return jsonify({
                    "success": False,
                    "data": [],
                    "message": "pedido_cotacao deve ser um número inteiro"
                }), 400
        
        marca_descricao = data.get('marca_descricao')
        if marca_descricao is not None:
            marca_descricao = str(marca_descricao)
            
        dias_compra = data.get('dias_compra', 30)
        try:
            dias_compra = int(dias_compra)
            if dias_compra <= 0:
                raise ValueError("Deve ser positivo")
        except (ValueError, TypeError):
            return jsonify({
                "success": False,
                "data": [],
                "message": "dias_compra deve ser um número inteiro positivo"
            }), 400
        
        print(f"DEBUG - Parâmetros processados: pedido={pedido_cotacao}, marca={marca_descricao}, dias={dias_compra}")
        
        # Executar sugestão
        df_resultado = executar_sugestao(pedido_cotacao, marca_descricao, dias_compra)
        
        if df_resultado is None or df_resultado.empty:
            return jsonify({
                "success": False,
                "data": [],
                "message": "Nenhum resultado encontrado"
            }), 404
        
        # Filtrar apenas PRO_CODIGO e QTD_SUGERIDA
        resultado = []
        for _, row in df_resultado.iterrows():
            resultado.append({
                "PRO_CODIGO": str(row.get("PRO_CODIGO", "")),
                "QTD_SUGERIDA": int(row.get("QTD_SUGERIDA", 0))
            })
        
        return jsonify({
            "success": True,
            "data": resultado,
            "message": f"Sugestão gerada com sucesso. {len(resultado)} produtos."
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "data": [],
            "message": f"Erro interno: {str(e)}"
        }), 500


# Adicionar endpoint de teste para verificar se a API está funcionando
@app.route('/diagnostico', methods=['GET'])
def diagnostico():
    """Endpoint para diagnóstico de conectividade"""
    diagnostico_result = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "sql_server": {"status": "error", "message": "", "drivers": []},
        "postgresql": {"status": "error", "message": ""},
        "odbc_dsns": {}
    }
    
    # Teste SQL Server
    try:
        # Verificar drivers disponíveis
        all_drivers = pyodbc.drivers()
        sql_server_drivers = [d for d in all_drivers if 'sql server' in d.lower()]
        diagnostico_result["sql_server"]["drivers"] = sql_server_drivers
        
        # Tentar conexão
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        diagnostico_result["sql_server"]["status"] = "ok"
        diagnostico_result["sql_server"]["message"] = f"Conexão SQL Server OK - {SQL_SERVER_HOST}:{SQL_SERVER_PORT}"
        
    except Exception as e:
        diagnostico_result["sql_server"]["message"] = f"Erro SQL Server: {str(e)}"
    
    # Teste PostgreSQL
    try:
        engine = get_postgres_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        diagnostico_result["postgresql"]["status"] = "ok"
        diagnostico_result["postgresql"]["message"] = "Conexão PostgreSQL OK"
        
    except Exception as e:
        diagnostico_result["postgresql"]["message"] = f"Erro PostgreSQL: {str(e)}"
    
    # Listar DSNs disponíveis
    try:
        diagnostico_result["odbc_dsns"] = dict(pyodbc.dataSources())
    except Exception as e:
        diagnostico_result["odbc_dsns"] = {"error": str(e)}
    
    return jsonify(diagnostico_result)


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "success": True,
        "message": "API está funcionando",
        "timestamp": pd.Timestamp.now().isoformat()
    })


# Endpoint para verificar dados disponíveis no PostgreSQL
@app.route('/dados-disponiveis', methods=['GET'])
def verificar_dados_postgres():
    """
    Verifica quais dados estão disponíveis no PostgreSQL
    """
    try:
        engine = get_postgres_engine()
        
        # Consulta para verificar dados disponíveis
        query = """
        SELECT 
            tipo_dados,
            DATE(data_processamento) as data_processamento,
            COUNT(*) as quantidade_registros,
            MIN(pro_codigo) as primeiro_produto,
            MAX(pro_codigo) as ultimo_produto
        FROM com_fifo_completo 
        GROUP BY tipo_dados, DATE(data_processamento)
        ORDER BY data_processamento DESC, tipo_dados
        LIMIT 10
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return jsonify({
                "success": False,
                "message": "Nenhum dado encontrado na tabela com_fifo_completo",
                "dados": []
            })
        
        # Converte DataFrame para lista de dicionários
        dados = df.to_dict('records')
        
        # Consulta específica para ANALISE_ATUAL mais recente
        query_atual = """
        SELECT 
            COUNT(*) as total_produtos,
            MAX(data_processamento) as ultima_analise
        FROM com_fifo_completo 
        WHERE tipo_dados = 'ANALISE_ATUAL'
        AND data_processamento = (
            SELECT MAX(data_processamento) 
            FROM com_fifo_completo 
            WHERE tipo_dados = 'ANALISE_ATUAL'
        )
        """
        
        df_atual = pd.read_sql(query_atual, engine)
        analise_atual = df_atual.to_dict('records')[0] if not df_atual.empty else None
        
        return jsonify({
            "success": True,
            "message": "Dados encontrados no PostgreSQL",
            "analise_atual_disponivel": analise_atual,
            "historico_processamentos": dados
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"Erro ao verificar dados PostgreSQL: {str(e)}",
            "dados": []
        }), 500


# Endpoint para receber requisições GET com query parameters
@app.route('/sugestao-compra', methods=['GET'])
def api_sugestao_get():
    """
    Endpoint alternativo usando query parameters
    Exemplo: GET /sugestao-compra?pedido_cotacao=123&marca_descricao=marca&dias_compra=30
    """
    try:
        # Obter parâmetros da query string
        pedido_cotacao = request.args.get('pedido_cotacao')
        if pedido_cotacao:
            try:
                pedido_cotacao = int(pedido_cotacao)
            except ValueError:
                return jsonify({
                    "success": False,
                    "data": [],
                    "message": "pedido_cotacao deve ser um número inteiro"
                }), 400
        
        marca_descricao = request.args.get('marca_descricao')
        
        dias_compra = request.args.get('dias_compra', '30')
        try:
            dias_compra = int(dias_compra)
            if dias_compra <= 0:
                raise ValueError("Deve ser positivo")
        except ValueError:
            return jsonify({
                "success": False,
                "data": [],
                "message": "dias_compra deve ser um número inteiro positivo"
            }), 400
        
        # Executar sugestão
        df_resultado = executar_sugestao(pedido_cotacao, marca_descricao, dias_compra)
        
        if df_resultado is None or df_resultado.empty:
            return jsonify({
                "success": False,
                "data": [],
                "message": "Nenhum resultado encontrado"
            }), 404
        
        # Filtrar apenas PRO_CODIGO e QTD_SUGERIDA
        resultado = []
        for _, row in df_resultado.iterrows():
            resultado.append({
                "PRO_CODIGO": str(row.get("PRO_CODIGO", "")),
                "QTD_SUGERIDA": int(row.get("QTD_SUGERIDA", 0))
            })
        
        return jsonify({
            "success": True,
            "data": resultado,
            "message": f"Sugestão gerada com sucesso. {len(resultado)} produtos."
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "data": [],
            "message": f"Erro interno: {str(e)}"
        }), 500


def executar_sugestao(pedido_cotacao=None, marca_descricao=None, dias_compra=30):
    global MARCA_DESCRICAO, ANALISA_PEDIDO, DIAS_COMPRA_USER
    
    MARCA_DESCRICAO = marca_descricao
    DIAS_COMPRA_USER = dias_compra
    ANALISA_PEDIDO = pedido_cotacao is not None

    # Carrega dados do PostgreSQL ao invés do Excel
    df_met = carregar_analise_atual_postgres()
    
    if df_met is None or df_met.empty:
        print("Não foi possível carregar dados do PostgreSQL")
        return None

    # Filtro por marca se especificado
    if MARCA_DESCRICAO:
        if "MAR_DESCRICAO" in df_met.columns:
            mask_marca = df_met["MAR_DESCRICAO"].astype(str).str.contains(
                MARCA_DESCRICAO, case=False, na=False
            )
            df_met = df_met.loc[mask_marca].copy()
        
    if df_met.empty:
        return None

    # Se está analisando pedido, faz merge com dados do pedido
    if ANALISA_PEDIDO:
        df_ped = carregar_itens_pedido(pedido_cotacao, EMPRESA_PEDIDO, MARCA_DESCRICAO)
        
        if df_ped.empty:
            return None
        
        # Debug: verificar tipos antes do merge
        print(f"DEBUG - df_met PRO_CODIGO tipo: {df_met['PRO_CODIGO'].dtype}")
        print(f"DEBUG - df_ped PRO_CODIGO tipo: {df_ped['PRO_CODIGO'].dtype}")
        print(f"DEBUG - df_met PRO_CODIGO amostra: {df_met['PRO_CODIGO'].head().tolist()}")
        print(f"DEBUG - df_ped PRO_CODIGO amostra: {df_ped['PRO_CODIGO'].head().tolist()}")
        
        # Garante que ambos sejam string
        df_met['PRO_CODIGO'] = df_met['PRO_CODIGO'].astype(str)
        df_ped['PRO_CODIGO'] = df_ped['PRO_CODIGO'].astype(str)
            
        df = df_met.merge(df_ped, on="PRO_CODIGO", how="inner")
        if df.empty:
            return None
    else:
        df = df_met.copy()
        
    # Aplica a lógica de sugestão
    sug = df.apply(sugerir_compra, axis=1)
    df_sug = df.join(sug)
    
    return df_sug


def apply_rounding(value, curve):
    """
    Aplica regra de arredondamento:
      - Curva A ou B: sempre para cima (ceil)
      - Curva C ou D (e outras): arredondamento tradicional (>= 0.5 sobe, < 0.5 desce)
    """
    # Garante que curva seja string e maiúscula
    c = str(curve).upper() if curve else ""
    
    if c in ["A", "B"]:
        return int(np.ceil(value))
    else:
        # Curva C, D ou outras (arredondamento normal)
        # Para positivos, floor(x + 0.5) arredonda corretamente (0.5->1, 0.4->0)
        return int(np.floor(value + 0.5))


def calcular_sugestao_pura(est, est_min_calc, est_max_calc, tipo, alerta, curva):
    """
    SUGESTAO: quanto eu compraria hoje,
    ignorando completamente o pedido já feito.

    Regra:
      - Sob_Demanda -> 0
      - se max <= 0 -> 0
      - se estoque >= max -> 0
      - caso contrário -> completar até o máximo alvo
        (com reforço leve se for A/B com tendência alta)
    """
    # 1) Sob demanda: não sugere automático
    if tipo == "Sob_Demanda":
        return 0

    # 2) Se nem máximo faz sentido (0 ou negativo), não sugere
    if est_max_calc is None or est_max_calc <= 0:
        return 0

    # 3) Se já está no máximo ou acima, não sugere
    if est >= est_max_calc:
        return 0

    # 4) Complementar até o máximo alvo
    base = est_max_calc - est
    if base < 0:
        base = 0

    fator = 1.0
    # Refórcinho só para itens importantes com tendência alta
    if alerta == "Sim" and curva in ["A", "B"]:
        fator = 1.2

    return apply_rounding(base * fator, curva)


def sugerir_compra(row):
    # Dados básicos
    est      = row.get("ESTOQUE_DISPONIVEL")
    est_min0 = row.get("ESTOQUE_MIN_SUGERIDO")
    est_max0 = row.get("ESTOQUE_MAX_SUGERIDO")
    tipo     = row.get("TIPO_PLANEJAMENTO")
    alerta   = row.get("ALERTA_TENDENCIA_ALTA")
    curva    = row.get("CURVA_ABC")
    tipo     = row.get("TIPO_PLANEJAMENTO")
    alerta   = row.get("ALERTA_TENDENCIA_ALTA")
    curva    = row.get("CURVA_ABC")
    dem = row.get("DEMANDA_MEDIA_DIA_AJUSTADA", 0)
    if pd.isna(dem) or dem == 0:
        dem = row.get("DEMANDA_MEDIA_DIA", 0)
    qtd_ped  = row.get("QTD_PEDIDO", 0) or 0  # quantidade colocada no pedido (se houver)
    desc     = row.get("PRO_DESCRICAO", "")
    # num_vend = row.get("NUM_VENDAS", np.nan)

    # Se o estoque atual estiver em branco, trata como 0 (sem estoque)
    if pd.isna(est):
        est = 0

    # Se os parâmetros de política (min/máx) estiverem faltando, aí sim é "Sem dados"
    if pd.isna(est_min0) or pd.isna(est_max0):
        # Em modo sem pedido, ESTOQUE_APOS_PEDIDO = estoque atual
        estoque_pos_ped = est + (qtd_ped if ANALISA_PEDIDO else 0)
        return pd.Series({
            "ESTOQUE_MIN_ALVO": est_min0,
            "ESTOQUE_MAX_ALVO": est_max0,
            "ESTOQUE_APOS_PEDIDO": estoque_pos_ped,
            "SUGESTAO": 0,
            "QTD_SUGERIDA": 0,
            "STATUS_PEDIDO": "Sem dados" if ANALISA_PEDIDO else "Sem dados",
            "PRIORIDADE": "Sem dados",
            "MOTIVO_SUGESTAO": (
                "Sem dados suficientes de estoque mínimo/máximo para sugerir compra automática."
            ),
        })

    # ============================
    # Cálculo de estoque alvo (min/max)
    # ============================

    # Portanto, não recalculamos com "LEAD_TIME_DIAS" nem "DIAS_ESTOQUE_DESEJADO" extra.
    # Usamos estritamente o que veio da análise, A MENOS QUE o usuário tenha definido DIAS_COMPRA_USER > 0
    est_min_calc = int(est_min0)
    
    if DIAS_COMPRA_USER and DIAS_COMPRA_USER > 0 and dem > 0:
        # Sobrescreve o Maximo com base na demanda diaria * dias solicitados
        est_max_calc = int(np.ceil(dem * DIAS_COMPRA_USER))
    else:
        est_max_calc = int(est_max0)

    # Garantir coerência min <= max
    if est_max_calc < est_min_calc:
        est_max_calc = est_min_calc

    # Estoque projetado:
    # - se ANÁLISE DE PEDIDO: estoque atual + QTD_PEDIDO
    # - se PLANEJAMENTO GERAL: igual ao estoque atual (não faz sentido falar "após pedido")
    estoque_pos_ped = est + (qtd_ped if ANALISA_PEDIDO else 0)

    # SUGESTAO pura (sem considerar o pedido)
    sugestao_pura = calcular_sugestao_pura(
        est=est,
        est_min_calc=est_min_calc,
        est_max_calc=est_max_calc,
        tipo=tipo,
        alerta=alerta,
        curva=curva,
    )

    # ============================
    # Casos especiais de política
    # ============================

    # 1) Sob Demanda
    if tipo == "Sob_Demanda":
        if ANALISA_PEDIDO:
            motivo = (
                f"Sob Demanda. Est: {est:.0f} + Ped: {qtd_ped:.0f} = {estoque_pos_ped:.0f}. Sem sugestão auto."
            )
        else:
            motivo = (
                f"Sob Demanda. Est: {est:.0f}. Sem sugestão auto."
            )

        return pd.Series({
            "ESTOQUE_MIN_ALVO": est_min_calc,
            "ESTOQUE_MAX_ALVO": est_max_calc,
            "ESTOQUE_APOS_PEDIDO": estoque_pos_ped,
            "SUGESTAO": sugestao_pura,
            "QTD_SUGERIDA": 0 if ANALISA_PEDIDO else sugestao_pura,
            "STATUS_PEDIDO": "Sob Demanda" if ANALISA_PEDIDO else "Sob Demanda",
            "PRIORIDADE": "Sob Demanda",
            "MOTIVO_SUGESTAO": motivo,
        })

    # 2) Sem política de estoque (mesmo após ajuste)
    if est_min_calc == 0 and est_max_calc == 0:
        if ANALISA_PEDIDO:
            motivo = (
                f"Sem política (Min/Max=0). Est: {est:.0f} + Ped: {qtd_ped:.0f} = {estoque_pos_ped:.0f}. Avaliar manual."
            )
        else:
            motivo = (
                f"Sem política. Est: {est:.0f}. Avaliar manual."
            )

        return pd.Series({
            "ESTOQUE_MIN_ALVO": est_min_calc,
            "ESTOQUE_MAX_ALVO": est_max_calc,
            "ESTOQUE_APOS_PEDIDO": estoque_pos_ped,
            "SUGESTAO": sugestao_pura,
            "QTD_SUGERIDA": 0 if ANALISA_PEDIDO else sugestao_pura,
            "STATUS_PEDIDO": "Sem política" if ANALISA_PEDIDO else "Sem política",
            "PRIORIDADE": "Sem política",
            "MOTIVO_SUGESTAO": motivo,
        })

    # ============================
    # RAMO 1: MODO PLANEJAMENTO GERAL (SEM PEDIDO)
    # ============================
    if not ANALISA_PEDIDO:
        # Aqui vamos usar somente estoque atual vs alvo e sugestao_pura
        est_atual = est
        qtd_sug = sugestao_pura

        # Estoque abaixo do mínimo
        if est_atual < est_min_calc:
            prioridade = "Crítico"
            motivo = (
                f"Est: {est_atual:.0f} < Min: {est_min_calc:.0f}. "
            )
            if qtd_sug > 0:
                motivo += (
                    f"Sugere-se +{qtd_sug:.0f} p/ atingir Max: {est_max_calc:.0f}."
                )
            else:
                motivo += "Sem compra auto sugerida."

        # Estoque entre mínimo e máximo
        elif est_min_calc <= est_atual < est_max_calc:
            if alerta == "Sim" and curva in ["A", "B"] and qtd_sug > 0:
                prioridade = "Oportunidade Tendência"
                motivo = (
                    f"Est: {est_atual:.0f} (Alvo: {est_min_calc:.0f}-{est_max_calc:.0f}). "
                    f"Curva {curva} c/ tendência alta: sugere-se +{qtd_sug:.0f}."
                )
            else:
                prioridade = "OK"
                motivo = (
                    f"Est: {est_atual:.0f} ok (Alvo: {est_min_calc:.0f}-{est_max_calc:.0f})."
                )

        # Estoque no máximo ou acima
        else:  # est_atual >= est_max_calc
            prioridade = "Excedente ou cheio"
            excedente = est_atual - est_max_calc
            motivo = (
                f"Est: {est_atual:.0f} > Max: {est_max_calc:.0f} (Exced: {excedente:.0f}). "
                "Sem compra."
            )

        return pd.Series({
            "ESTOQUE_MIN_ALVO": est_min_calc,
            "ESTOQUE_MAX_ALVO": est_max_calc,
            "ESTOQUE_APOS_PEDIDO": est_atual,   # aqui só repetimos o atual
            "SUGESTAO": qtd_sug,
            "QTD_SUGERIDA": qtd_sug,           # no modo geral QTD_SUGERIDA = SUGESTAO
            "STATUS_PEDIDO": "Planejamento",   # apenas para manter a coluna
            "PRIORIDADE": prioridade,
            "MOTIVO_SUGESTAO": motivo,
        })

    # ============================
    # RAMO 2: MODO ANÁLISE DE PEDIDO
    # ============================

    # Análise do pedido vs estoque alvo
    falta_min_pos = max(0, est_min_calc - estoque_pos_ped)
    falta_max_pos = max(0, est_max_calc - estoque_pos_ped)

    # Estoque projetado abaixo do mínimo alvo
    if estoque_pos_ped < est_min_calc:
        base = falta_max_pos  # quanto falta para chegar no máximo

        fator = 1.0
        motivo_extra = []

        if alerta == "Sim" and curva in ["A", "B"]:
            fator = 1.2
            motivo_extra.append("produto curva alta (A/B) com tendência de alta nos últimos 12 meses")

        qtd_sug = apply_rounding(base * fator, curva)

        motivo = (
            f"EstProj: {estoque_pos_ped:.0f} < Min: {est_min_calc:.0f} (após pedido {qtd_ped:.0f}). "
            f"Falta +{qtd_sug:.0f} p/ atingir Max {est_max_calc:.0f}."
        )
        if motivo_extra:
            motivo += " Também foi considerado que " + " e ".join(motivo_extra) + "."

        prioridade = "Crítico"

        return pd.Series({
            "ESTOQUE_MIN_ALVO": est_min_calc,
            "ESTOQUE_MAX_ALVO": est_max_calc,
            "ESTOQUE_APOS_PEDIDO": estoque_pos_ped,
            "SUGESTAO": sugestao_pura,
            "QTD_SUGERIDA": qtd_sug,
            "STATUS_PEDIDO": "Compra insuficiente",
            "PRIORIDADE": prioridade,
            "MOTIVO_SUGESTAO": motivo,
        })

    # Estoque projetado entre mínimo e máximo alvo
    if est_min_calc <= estoque_pos_ped <= est_max_calc:
        if alerta == "Sim" and curva in ["A", "B"]:
            qtd_sug = apply_rounding(falta_max_pos, curva)
            prioridade = "Oportunidade Tendência" if qtd_sug > 0 else "OK"

            if qtd_sug > 0:
                motivo = (
                    f"EstProj: {estoque_pos_ped:.0f} ok (Alvo: {est_min_calc:.0f}-{est_max_calc:.0f}). "
                    f"Curva {curva} tendência alta: sugere-se +{qtd_sug:.0f}."
                )
            else:
                motivo = (
                    f"EstProj: {estoque_pos_ped:.0f} ok (Alvo: {est_min_calc:.0f}-{est_max_calc:.0f})."
                )
        else:
            qtd_sug = 0
            prioridade = "OK"
            motivo = (
                f"EstProj: {estoque_pos_ped:.0f} ok (Alvo: {est_min_calc:.0f}-{est_max_calc:.0f})."
            )

        return pd.Series({
            "ESTOQUE_MIN_ALVO": est_min_calc,
            "ESTOQUE_MAX_ALVO": est_max_calc,
            "ESTOQUE_APOS_PEDIDO": estoque_pos_ped,
            "SUGESTAO": sugestao_pura,
            "QTD_SUGERIDA": qtd_sug,
            "STATUS_PEDIDO": "Compra adequada",
            "PRIORIDADE": prioridade,
            "MOTIVO_SUGESTAO": motivo,
        })

    # Estoque projetado acima do máximo alvo
    if estoque_pos_ped > est_max_calc:
        excedente = estoque_pos_ped - est_max_calc

        motivo = (
            f"EstProj: {estoque_pos_ped:.0f} > Max: {est_max_calc:.0f} (Exced: {excedente:.0f}). "
            "Avaliar redução."
        )

        prioridade = "Excedente"

        return pd.Series({
            "ESTOQUE_MIN_ALVO": est_min_calc,
            "ESTOQUE_MAX_ALVO": est_max_calc,
            "ESTOQUE_APOS_PEDIDO": estoque_pos_ped,
            "SUGESTAO": sugestao_pura,
            "QTD_SUGERIDA": 0,
            "STATUS_PEDIDO": "Compra excedente",
            "PRIORIDADE": prioridade,
            "MOTIVO_SUGESTAO": motivo,
        })

    # fallback (teoricamente não chega aqui)
    return pd.Series({
        "ESTOQUE_MIN_ALVO": est_min_calc,
        "ESTOQUE_MAX_ALVO": est_max_calc,
        "ESTOQUE_APOS_PEDIDO": estoque_pos_ped,
        "SUGESTAO": sugestao_pura,
        "QTD_SUGERIDA": 0,
        "STATUS_PEDIDO": "Indefinido",
        "PRIORIDADE": "Indefinido",
        "MOTIVO_SUGESTAO": "Condição não mapeada explicitamente.",
    })


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)