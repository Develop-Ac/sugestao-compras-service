import streamlit as st
import pandas as pd
import os
import sys
import time

# Limita o sys.path para importarmos os scripts locais
sys.path.append(os.getcwd())

# Tenta importar as funcoes dos scripts
try:
    from index import run_job
    from sugestao_compra import executar_sugestao, ARQ_SAIDA as ARQ_SUGESTAO_NOME
except ImportError as e:
    st.error(f"Erro ao importar scripts: {e}. Certifique-se que index.py e sugestao_compra.py estao na mesma pasta.")
    st.stop()
    
# Configura pagina
st.set_page_config(page_title="Controle de Estoque & Compras", layout="wide")

st.title("Sistema de Controle de Estoque & Compras")

tab1, tab2 = st.tabs(["📊 Análise FIFO", "🛒 Sugestão de Compra"])

# --- TAB 1: Executar FIFO ---
with tab1:
    st.header("Execução da Análise FIFO")
    st.markdown("""
    Esta funcionalidade roda o script `index.py` que:
    1. Lê dados do banco via ODBC.
    2. Calcula o FIFO de saídas e entradas.
    3. Define métricas ABC, tendências e níveis de estoque.
    4. Gera o arquivo `resultado_fifo_completo.xlsx`.
    """)
    
    if st.button("Executar Análise Completa", type="primary"):
        st.info("Iniciando execução... (Acompanhe os logs no terminal se necessário)")
        
        # Como o script usa print, nao veremos output real-time facil aqui no streamlit
        # sem capturar stdout, mas vamos apenas rodar e avisar.
        placeholder = st.empty()
        placeholder.text("Rodando... Isso pode levar alguns minutos.")
        
        try:
            start_time = time.time()
            # Rodar o job
            run_job()
            end_time = time.time()
            
            placeholder.success(f"Análise concluída com sucesso em {end_time - start_time:.1f} segundos!")
            
            # Verificar se o arquivo foi gerado
            ARQ_RESULTADO = "resultado_fifo_completo.xlsx"
            if os.path.exists(ARQ_RESULTADO):
                st.success("Arquivo gerado: resultado_fifo_completo.xlsx")
                
                # Ler para mostrar preview
                df_res = pd.read_excel(ARQ_RESULTADO, sheet_name="ANALISE_ATUAL")
                st.dataframe(df_res.head(50))
                
                # Download button
                with open(ARQ_RESULTADO, "rb") as f:
                    st.download_button(
                        label="Download Excel Completo",
                        data=f,
                        file_name="resultado_fifo_completo.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.warning("O arquivo de resultado não foi encontrado após a execução.")
                
        except Exception as e:
            placeholder.error(f"Erro durante a execução: {e}")

# --- TAB 2: Sugestão de Compra ---
with tab2:
    st.header("Gerar Sugestão de Compra")
    st.markdown("""
    Esta funcionalidade usa o resultado da análise FIFO para sugerir compras.
    Você pode especificar um pedido de cotação para cruzar os dados ou gerar uma análise geral.
    """)
    
    col1, col2 = st.columns(2)
    with col1:
        usar_pedido = st.checkbox("Analizar um Pedido de Cotação Específico?", value=False)
        pedido_val = None
        if usar_pedido:
            pedido_val = st.number_input("Número do Pedido de Cotação", min_value=1, step=1)
            
    with col2:
        st.info("Se não marcar 'Pedido de Cotação', será feita uma análise geral de todos os produtos.")
        
        # Novo input: Dias de Cobertura
        dias_compra = st.number_input("Dias de Cobertura (Compra para quantos dias?)", min_value=0, value=30, step=1)

    if st.button("Gerar Sugestão", type="primary"):
        # Verifica se existe o arquivo input
        ARQ_INPUT_FIFO = "resultado_fifo_completo.xlsx"
        if not os.path.exists(ARQ_INPUT_FIFO):
            st.error(f"Arquivo de entrada '{ARQ_INPUT_FIFO}' não encontrado. Por favor, execute a Análise FIFO primeiro.")
        else:
            try:
                st.text("Calculando sugestão...")
                # Chama a funcao refatorada
                df_sug = executar_sugestao(
                    pedido_cotacao=int(pedido_val) if usar_pedido and pedido_val else None,
                    dias_compra=dias_compra
                )
                
                if df_sug is not None and not df_sug.empty:
                    st.success("Sugestão calculada com sucesso!")
                    
                    cols_to_show = ["PRO_CODIGO", "PRO_DESCRICAO", "ESTOQUE_DISPONIVEL", "SUGESTAO", "MOTIVO_SUGESTAO"]
                    if "QTD_PEDIDO" in df_sug.columns:
                        cols_to_show.insert(3, "QTD_PEDIDO") # Mostra antes da sugestao
                        
                    st.dataframe(df_sug[cols_to_show].head(100))
                    
                    # Download
                    if os.path.exists(ARQ_SUGESTAO_NOME):
                        with open(ARQ_SUGESTAO_NOME, "rb") as f:
                            st.download_button(
                                label="Download Sugestão (Excel)",
                                data=f,
                                file_name=ARQ_SUGESTAO_NOME,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                else:
                    st.warning("Nenhum dado retornado ou erro na execução (verifique logs).")
                    
            except Exception as e:
                st.error(f"Erro ao gerar sugestão: {e}")
