from flask import Flask, request, jsonify
from flask_cors import CORS
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

# Configurar CORS para permitir requisições do frontend
CORS(app, origins=[
    "http://localhost:3000",
    "http://localhost:3001", 
    "http://localhost:8080",
    "http://localhost:5173",  # Vite dev server
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
    "http://127.0.0.1:8080",
    "http://127.0.0.1:5173"
])


# ============================
# CONEXÃO SQL SERVER
# ============================

def get_connection():
    """Conecta ao SQL Server com tratamento de erro melhorado"""
    conn_str = (
        f"DRIVER={{FreeTDS}};"
        f"SERVER={SQL_SERVER_HOST};"
        f"PORT={SQL_SERVER_PORT};"
        f"DATABASE={SQL_SERVER_DATABASE};"
        f"UID={SQL_SERVER_USER};"
        f"PWD={SQL_SERVER_PASSWORD};"
        f"TDS_Version=8.0;"
    )
    
    try:
        print(f"Tentando conectar com SQL Server: {SQL_SERVER_HOST}:{SQL_SERVER_PORT}")
        
        # Verificar se drivers estão disponíveis
        drivers = [driver for driver in pyodbc.drivers() if 'freetds' in driver.lower() or 'tds' in driver.lower()]
        print(f"Drivers FreeTDS disponíveis: {drivers}")
        
        if not drivers:
            print("ATENÇÃO: Nenhum driver FreeTDS encontrado!")
            print(f"Todos os drivers: {pyodbc.drivers()}")
        
        conn = pyodbc.connect(conn_str)
        print("Conexão SQL Server estabelecida com sucesso!")
        return conn
        
    except pyodbc.Error as e:
        print(f"Erro de conexão SQL Server: {e}")
        print(f"Host: {SQL_SERVER_HOST}:{SQL_SERVER_PORT}")
        print(f"Database: {SQL_SERVER_DATABASE}")
        print(f"User: {SQL_SERVER_USER}")
        print(f"String de conexão: {conn_str.replace(SQL_SERVER_PASSWORD, '***')}")
        
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
    Carrega todos os dados da tabela com_fifo_completo do PostgreSQL
    """
    try:
        engine = get_postgres_engine()
        
        # Query simples para pegar todos os dados
        query = """
        SELECT * FROM com_fifo_completo 
        ORDER BY pro_codigo
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("Nenhum dado encontrado na tabela com_fifo_completo")
            return None
        
        print(f"Carregados {len(df)} registros da análise FIFO do PostgreSQL")
        
        # Mapeia as colunas do banco para o formato esperado pela API
        column_mapping = {
            'pro_codigo': 'PRO_CODIGO',
            'pro_descricao': 'PRO_DESCRICAO',
            'mar_descricao': 'MAR_DESCRICAO',
            'fornecedor1': 'FORNECEDOR1',
            'fornecedor2': 'FORNECEDOR2',
            'fornecedor3': 'FORNECEDOR3',
            'estoque_disponivel': 'ESTOQUE_DISPONIVEL',
            'qtd_vendida': 'QTD_VENDIDA_PERIODO',
            'valor_vendido': 'VALOR_VENDIDO_PERIODO',
            'data_max_venda': 'DATA_ULTIMA_VENDA',
            'curva_abc': 'ABC_VENDAS',
            'demanda_media_dia': 'DEMANDA_MEDIA_DIA',
            'demanda_media_dia_ajustada': 'DEMANDA_MEDIA_DIA_AJUSTADA',
            'estoque_min_sugerido': 'ESTOQUE_MIN_SUGERIDO',
            'estoque_max_sugerido': 'ESTOQUE_MAX_SUGERIDO',
            'tipo_planejamento': 'TIPO_PLANEJAMENTO',
            'alerta_tendencia_alta': 'ALERTA_TENDENCIA_ALTA',
            'num_vendas': 'NUM_VENDAS'
        }
        
        # Renomeia as colunas
        df = df.rename(columns=column_mapping)
        
        # Remove colunas de controle que não são necessárias para a análise (se existirem)
        columns_to_remove = ['id', 'data_processamento', 'tipo_dados', 'created_at']
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
        
        # Converte PRO_CODIGO para string para evitar problemas de merge
        if 'PRO_CODIGO' in df.columns:
            df['PRO_CODIGO'] = df['PRO_CODIGO'].astype(str)
        
        # Garantir que temos as colunas essenciais preenchidas
        if 'DEMANDA_MEDIA_DIA' not in df.columns:
            df['DEMANDA_MEDIA_DIA'] = df.get('demanda_media_dia', 0)
        
        if 'DEMANDA_MEDIA_DIA_AJUSTADA' not in df.columns:
            df['DEMANDA_MEDIA_DIA_AJUSTADA'] = df.get('demanda_media_dia_ajustada', df.get('DEMANDA_MEDIA_DIA', 0))
        
        if 'ESTOQUE_MIN_SUGERIDO' not in df.columns:
            df['ESTOQUE_MIN_SUGERIDO'] = df.get('estoque_min_sugerido', 0)
        
        if 'ESTOQUE_MAX_SUGERIDO' not in df.columns:
            df['ESTOQUE_MAX_SUGERIDO'] = df.get('estoque_max_sugerido', 0)
        
        if 'TIPO_PLANEJAMENTO' not in df.columns:
            df['TIPO_PLANEJAMENTO'] = df.get('tipo_planejamento', 'Normal')
        
        if 'ALERTA_TENDENCIA_ALTA' not in df.columns:
            df['ALERTA_TENDENCIA_ALTA'] = df.get('alerta_tendencia_alta', 'Não')
        
        if 'NUM_VENDAS' not in df.columns:
            df['NUM_VENDAS'] = df.get('num_vendas', 0)
        
        # Mapear classificação ABC se disponível
        if 'ABC_VENDAS' not in df.columns and 'curva_abc' in df.columns:
            df['ABC_VENDAS'] = df['curva_abc']
        
        if 'CURVA_ABC' not in df.columns:
            df['CURVA_ABC'] = df.get('ABC_VENDAS', 'C').fillna('C')
            print("Mapeado CURVA_ABC baseado em ABC_VENDAS")
        
        print(f"Estrutura final: {len(df)} registros com {len(df.columns)} colunas")
        print(f"Colunas principais: {list(df.columns)[:10]}...")  # Mostra primeiras 10 colunas
        
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
        
        # Logs removidos para melhor performance
        
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
        
        # Debug removido para melhor performance
        
        # Executar sugestão
        df_resultado = executar_sugestao(pedido_cotacao, marca_descricao, dias_compra)
        
        if df_resultado is None or df_resultado.empty:
            return jsonify({
                "success": False,
                "data": [],
                "message": "Nenhum resultado encontrado"
            }), 404
        
        # Filtrar apenas produtos com sugestão de compra > 0
        produtos_com_sugestao = df_resultado[df_resultado['QTD_SUGERIDA'] > 0]
        
        if produtos_com_sugestao.empty:
            return jsonify({
                "success": True,
                "data": [],
                "message": "Nenhum produto necessita de compra no momento"
            })

        # Filtrar apenas PRO_CODIGO e QTD_SUGERIDA dos produtos que precisam de compra
        resultado = []
        for _, row in produtos_com_sugestao.iterrows():
            # Debug: imprimir valores para verificar
            pro_codigo = str(row.get("PRO_CODIGO", ""))
            qtd_sugerida = row.get("QTD_SUGERIDA", 0)
            
            # Verificar se é NaN ou None
            if pd.isna(qtd_sugerida):
                qtd_sugerida = 0
            
            try:
                qtd_sugerida_int = int(qtd_sugerida)
            except (ValueError, TypeError):
                print(f"ERRO: Não foi possível converter QTD_SUGERIDA para int: {qtd_sugerida} (tipo: {type(qtd_sugerida)})")
                qtd_sugerida_int = 0
                
            # Debug removido para melhor performance
            
            resultado.append({
                "PRO_CODIGO": pro_codigo,
                "QTD_SUGERIDA": qtd_sugerida_int,
                # Campos adicionais para debug
                "ESTOQUE_DISPONIVEL": float(row.get("ESTOQUE_DISPONIVEL", 0)) if not pd.isna(row.get("ESTOQUE_DISPONIVEL", 0)) else 0,
                "ESTOQUE_MIN_ALVO": float(row.get("ESTOQUE_MIN_ALVO", 0)) if not pd.isna(row.get("ESTOQUE_MIN_ALVO", 0)) else 0,
                "ESTOQUE_MAX_ALVO": float(row.get("ESTOQUE_MAX_ALVO", 0)) if not pd.isna(row.get("ESTOQUE_MAX_ALVO", 0)) else 0,
                "PRIORIDADE": str(row.get("PRIORIDADE", "")),
                "MOTIVO_SUGESTAO": str(row.get("MOTIVO_SUGESTAO", ""))
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


# Adicionar endpoint de teste para verificar se a A
# PI está funcionando
@app.route('/diagnostico', methods=['GET'])
def diagnostico():
    """Endpoint para diagnóstico de conectividade"""
    diagnostico_result = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "sql_server": {"status": "error", "message": "", "drivers": []},
        "postgresql": {"status": "error", "message": ""},
        "odbc_dsns": {}
    }
    
    # Teste SQL Server (apenas drivers, sem tentar conexão)
    try:
        # Verificar drivers disponíveis
        all_drivers = pyodbc.drivers()
        sql_server_drivers = [d for d in all_drivers if 'freetds' in d.lower() or 'tds' in d.lower()]
        diagnostico_result["sql_server"]["drivers"] = sql_server_drivers
        
        if sql_server_drivers:
            diagnostico_result["sql_server"]["status"] = "drivers_ok"
            diagnostico_result["sql_server"]["message"] = f"Drivers disponíveis para {SQL_SERVER_HOST}:{SQL_SERVER_PORT} (conexão não testada)"
        else:
            diagnostico_result["sql_server"]["message"] = "Nenhum driver FreeTDS encontrado"
        
    except Exception as e:
        diagnostico_result["sql_server"]["message"] = f"Erro ao verificar drivers: {str(e)}"
    
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


@app.route('/diagnostico-sql', methods=['POST'])
def diagnostico_sql_server():
    """Endpoint específico para testar SQL Server apenas quando necessário"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "ok",
            "message": f"Conexão SQL Server OK - {SQL_SERVER_HOST}:{SQL_SERVER_PORT}"
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro SQL Server: {str(e)}"
        }), 500


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
    
    try:
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

        # Se está analisando pedido, faz merge com dados do pedido (REQUER SQL SERVER)
        if ANALISA_PEDIDO:
            try:
                df_ped = carregar_itens_pedido(pedido_cotacao, EMPRESA_PEDIDO, MARCA_DESCRICAO)
                
                if df_ped.empty:
                    print("Nenhum item encontrado no pedido especificado")
                    return None
                
                # Garante que ambos sejam string
                df_met['PRO_CODIGO'] = df_met['PRO_CODIGO'].astype(str)
                df_ped['PRO_CODIGO'] = df_ped['PRO_CODIGO'].astype(str)
                    
                df = df_met.merge(df_ped, on="PRO_CODIGO", how="inner")
                if df.empty:
                    print("Nenhum produto do pedido encontrado na análise FIFO")
                    return None
            except Exception as sql_error:
                print(f"Erro ao acessar dados do pedido (SQL Server indisponível): {sql_error}")
                print("Convertendo para análise geral sem pedido específico...")
                # Se SQL Server não estiver disponível, faz análise geral
                ANALISA_PEDIDO = False
                df = df_met.copy()
        else:
            df = df_met.copy()
            
        # Aplica a lógica de sugestão
        sug = df.apply(sugerir_compra, axis=1)
        df_sug = df.join(sug)
        
        return df_sug
        
    except Exception as e:
        print(f"Erro geral em executar_sugestao: {e}")
        return None


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

    resultado = apply_rounding(base * fator, curva)
    return resultado


def sugerir_compra(row):
    # Dados básicos
    est      = row.get("ESTOQUE_DISPONIVEL")
    est_min0 = row.get("ESTOQUE_MIN_SUGERIDO")
    est_max0 = row.get("ESTOQUE_MAX_SUGERIDO")
    tipo     = row.get("TIPO_PLANEJAMENTO")
    alerta   = row.get("ALERTA_TENDENCIA_ALTA")
    curva    = row.get("CURVA_ABC")
    pro_codigo = row.get("PRO_CODIGO", "")
    
    # Debug removido para melhor performance
    
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