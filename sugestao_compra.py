import argparse
import pyodbc
import pandas as pd
import numpy as np
import os

# ============================
# CONFIGURAÇÕES
# ============================

ARQ_ENTRADA = "resultado_fifo_completo.xlsx"
ARQ_SAIDA   = "resultado_fifo_sugestao.xlsx"

# Quantos dias de estoque você quer manter ALÉM do lead time
DIAS_ESTOQUE_DESEJADO = 90

# Prazo logístico (dias entre pedido e chegada da mercadoria)
LEAD_TIME_DIAS = 17

# Pedido de cotação a analisar (pode ser None)
PEDIDO_COTACAO  = None
# Empresa fixa (travada em 3)
EMPRESA_PEDIDO  = 3
MARCA_DESCRICAO = None


def parse_filtros():
    """
    Filtros dinamicos via linha de comando:
      --pedido-cotacao   -> opcional (pode ser omitido para pegar apenas planejamento)
      --marca            -> opcional (usa MAR_DESCRICAO do arquivo de métricas, case-insensitive)
    """
    parser = argparse.ArgumentParser(
        description="Gera sugestao de compra com filtros dinamicos."
    )
    parser.add_argument(
        "--pedido-cotacao",
        type=int,
        help="Numero do pedido de cotacao (opcional). Se nao informado, faz apenas sugestao de compra por produto.",
    )
    parser.add_argument(
        "--marca",
        type=str,
        help="Filtro opcional por MAR_DESCRICAO no arquivo de métricas (case-insensitive).",
    )
    return parser.parse_args()


# ============================
# CONEXÃO ODBC FIREBIRD
# ============================

def get_connection():
    conn_str = (
        "DSN=CONSULTA;"
        "UID=USER_CONSULTA;"
        "PWD=Ac@2025acesso;"
    )
    return pyodbc.connect(conn_str)


def carregar_itens_pedido(pedido_cotacao, empresa, marca_descricao=None):
    """
    Carrega os itens do Firebird aplicando filtros dinâmicos:
      - pedido_cotacao: opcional (None = sem filtro)
      - empresa: obrigatório
      - marca_descricao: opcional, filtra mar.mar_descricao com LIKE case-insensitive
    """
    conn = get_connection()

    sql = """
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
        WHERE pedi.empresa = ?
    """

    params = [empresa]

    if pedido_cotacao is not None:
        sql += "\n          AND pedi.pedido_cotacao = ?"
        params.append(pedido_cotacao)

    if marca_descricao:
        sql += "\n          AND UPPER(mar.mar_descricao) LIKE ?"
        params.append(f"%{marca_descricao.upper()}%")

    df_ped = pd.read_sql(sql, conn, params=params)
    conn.close()

    # Normalizar nomes de colunas
    df_ped = df_ped.rename(columns={
        "pro_codigo": "PRO_CODIGO",
        "PRO_CODIGO": "PRO_CODIGO",
        "quantidade": "QTD_PEDIDO",
        "QUANTIDADE": "QTD_PEDIDO",
        "mar_descricao": "MAR_DESCRICAO",
        "MAR_DESCRICAO": "MAR_DESCRICAO",
    })

    # Evitar duplicar MAR_DESCRICAO no merge com métricas (que já tem marca)
    if "MAR_DESCRICAO" in df_ped.columns:
        df_ped = df_ped.drop(columns=["MAR_DESCRICAO"])

    return df_ped

# ============================
# LÓGICA DE SUGESTÃO
# ============================

def executar_sugestao(pedido_cotacao=None, marca_descricao=None, dias_compra=30):
    global PEDIDO_COTACAO, MARCA_DESCRICAO, ANALISA_PEDIDO, DIAS_COMPRA_USER
    
    PEDIDO_COTACAO = pedido_cotacao
    MARCA_DESCRICAO = marca_descricao
    DIAS_COMPRA_USER = dias_compra
    ANALISA_PEDIDO = PEDIDO_COTACAO is not None

    print("Lendo arquivo de métricas:", ARQ_ENTRADA)
    if not os.path.exists(ARQ_ENTRADA):
        print("Arquivo de entrada não encontrado.")
        return None

    df_met = pd.read_excel(ARQ_ENTRADA, sheet_name="ANALISE_ATUAL")

    if MARCA_DESCRICAO:
        if "MAR_DESCRICAO" in df_met.columns:
            mask_marca = df_met["MAR_DESCRICAO"].astype(str).str.contains(
                MARCA_DESCRICAO, case=False, na=False
            )
            df_met = df_met.loc[mask_marca].copy()
            print(f"Aplicando filtro opcional de marca: {len(df_met)} produtos.")
        
    if df_met.empty:
        print("Nenhum produto restante após aplicar filtros.")
        return None

    if ANALISA_PEDIDO:
        print(f"Lendo itens do pedido {PEDIDO_COTACAO}...")
        df_ped = carregar_itens_pedido(PEDIDO_COTACAO, EMPRESA_PEDIDO, MARCA_DESCRICAO)
        
        if df_ped.empty:
            print("Nenhum item encontrado no pedido.")
            return None
            
        df = df_met.merge(df_ped, on="PRO_CODIGO", how="inner")
        if df.empty:
            print("Produtos do pedido não encontrados nas métricas.")
            return None
    else:
        df = df_met.copy()
        
    print("Calculando sugestão...")
    sug = df.apply(sugerir_compra, axis=1)
    df_sug = df.join(sug)
    
    print("Gravando:", ARQ_SAIDA)
    df_sug.to_excel(ARQ_SAIDA, index=False)
    print("Concluído.")
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
    dem      = row.get("DEMANDA_MEDIA_DIA_AJUSTADA", 0)
    sgr      = row.get("SGR_CODIGO")
    
    if pd.isna(dem) or dem == 0:
        dem = row.get("DEMANDA_MEDIA_DIA", 0)
    qtd_ped  = row.get("QTD_PEDIDO", 0) or 0  # quantidade colocada no pedido (se houver)
    desc     = row.get("PRO_DESCRICAO", "")

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
    
    est_min_orig = int(est_min0)
    est_max_orig = int(est_max0)
    
    est_min_calc = est_min_orig
    est_max_calc = est_max_orig

    # Exceção 1: Sob Demanda
    if ml_tipo := str(tipo).strip() == "Sob_Demanda":
        # Mantém originais, ignora DIAS_COMPRA_USER
        pass
    else:
        # Lógica de Escala
        if DIAS_COMPRA_USER and DIAS_COMPRA_USER > 0:
            
            # Determinar dias de referência da curva (Max Days)
            # Default: A=60, B=90, C=120, D=45
            # 154: A=120, B=180, C=240, D=120
            
            if sgr == 154:
                ref_dias_map = {"A": 120, "B": 180, "C": 240, "D": 120}
            else:
                ref_dias_map = {"A": 60, "B": 90, "C": 120, "D": 45}
            
            ref_dias = ref_dias_map.get(curva, 60) # fallback A=60
            
            fator_escala = DIAS_COMPRA_USER / ref_dias
            
            est_min_calc = int(np.ceil(est_min_orig * fator_escala))
            est_max_calc = int(np.ceil(est_max_orig * fator_escala))
            
            # Exceção 2: Pouco Histórico -> divide pela metade
            if str(tipo).strip() == "Pouco_Historico":
                est_min_calc = int(np.ceil(est_min_calc / 2.0))
                est_max_calc = int(np.ceil(est_max_calc / 2.0))

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
    print("--- Configuração de Execução ---")
    p_input = input("Informe o número do pedido de cotação (ou pressione ENTER para analisar todos/sem pedido): ").strip()
    p_cotacao = int(p_input) if p_input.isdigit() else None
    
    executar_sugestao(p_cotacao)