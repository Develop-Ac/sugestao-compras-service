"""
Script para testar a conexão PostgreSQL e criar/verificar a estrutura da tabela
"""
import psycopg2
from sqlalchemy import create_engine, text
import sys

# Configurações PostgreSQL
POSTGRES_URL = "postgresql://intranet:Ac%402025acesso@panel-teste.acacessorios.local:5555/intranet"
TABELA_FIFO = "com_fifo_completo"

def testar_conexao():
    """Testa a conexão com o PostgreSQL"""
    print("Testando conexão com PostgreSQL...")
    
    try:
        # Teste com psycopg2
        conn = psycopg2.connect(
            host="panel-teste.acacessorios.local",
            port=5555,
            database="intranet",
            user="intranet",
            password="Ac@2025acesso"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Conexão bem-sucedida! PostgreSQL Version: {version[0]}")
        cursor.close()
        conn.close()
        
        # Teste com SQLAlchemy
        engine = create_engine(POSTGRES_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database(), current_user"))
            db_info = result.fetchone()
            print(f"✅ SQLAlchemy OK! Database: {db_info[0]}, User: {db_info[1]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")
        return False

def verificar_tabela():
    """Verifica se a tabela existe e sua estrutura"""
    print(f"\nVerificando tabela {TABELA_FIFO}...")
    
    try:
        engine = create_engine(POSTGRES_URL)
        
        # Verifica se a tabela existe
        check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = :table_name
        );
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(check_table_query), {"table_name": TABELA_FIFO})
            table_exists = result.scalar()
            
            if table_exists:
                print(f"✅ Tabela {TABELA_FIFO} existe!")
                
                # Mostra estrutura da tabela
                structure_query = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = :table_name
                ORDER BY ordinal_position;
                """
                
                result = conn.execute(text(structure_query), {"table_name": TABELA_FIFO})
                columns = result.fetchall()
                
                print("\nEstrutura da tabela:")
                print(f"{'Coluna':<25} {'Tipo':<20} {'Null?':<10} {'Padrão'}")
                print("-" * 70)
                for col in columns:
                    default = col[3] if col[3] else ""
                    print(f"{col[0]:<25} {col[1]:<20} {col[2]:<10} {default}")
                
                # Conta registros
                count_query = f"SELECT COUNT(*) FROM {TABELA_FIFO}"
                result = conn.execute(text(count_query))
                count = result.scalar()
                print(f"\nTotal de registros: {count}")
                
                if count > 0:
                    # Mostra últimos processamentos
                    last_query = """
                    SELECT tipo_dados, COUNT(*) as qtd, MAX(data_processamento) as ultimo
                    FROM com_fifo_completo 
                    GROUP BY tipo_dados 
                    ORDER BY ultimo DESC
                    """
                    result = conn.execute(text(last_query))
                    processamentos = result.fetchall()
                    
                    print("\nÚltimos processamentos por tipo:")
                    for proc in processamentos:
                        print(f"  {proc[0]}: {proc[1]} registros - último: {proc[2]}")
                
            else:
                print(f"❌ Tabela {TABELA_FIFO} não existe!")
                return False
                
        return True
        
    except Exception as e:
        print(f"❌ Erro ao verificar tabela: {e}")
        return False

def criar_tabela():
    """Cria a tabela com_fifo_completo"""
    print(f"\nCriando tabela {TABELA_FIFO}...")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS com_fifo_completo (
        id SERIAL PRIMARY KEY,
        data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        tipo_dados VARCHAR(50) NOT NULL,
        pro_codigo VARCHAR(50),
        pro_descricao TEXT,
        subgrp_codigo VARCHAR(50),
        mar_descricao VARCHAR(100),
        fornecedor1 VARCHAR(200),
        fornecedor2 VARCHAR(200),
        fornecedor3 VARCHAR(200),
        estoque_disponivel DECIMAL(15,4),
        valor_custo_unitario DECIMAL(15,4),
        valor_total_custo DECIMAL(15,4),
        qtd_vendida_periodo DECIMAL(15,4),
        valor_vendido_periodo DECIMAL(15,4),
        data_ultima_venda DATE,
        margem_lucro DECIMAL(10,4),
        giro_estoque DECIMAL(10,4),
        abc_vendas VARCHAR(10),
        abc_estoque VARCHAR(10),
        abc_margem VARCHAR(10),
        classificacao_geral VARCHAR(50),
        recomendacao TEXT,
        data_compra DATE,
        nfe VARCHAR(50),
        nfs VARCHAR(50),
        lancto INTEGER,
        preco_custo DECIMAL(15,4),
        total_liquido DECIMAL(15,4),
        data_movimentacao DATE,
        origem VARCHAR(10),
        quantidade DECIMAL(15,4),
        indice INTEGER,
        qtd_acumulada DECIMAL(15,4),
        qtd_acum_saida DECIMAL(15,4),
        saldo_fifo DECIMAL(15,4),
        valor_fifo DECIMAL(15,4),
        data_entrada DATE,
        dias_estoque INTEGER,
        percentual_saldo DECIMAL(10,4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Índices para melhorar performance
    CREATE INDEX IF NOT EXISTS idx_com_fifo_data_processamento ON com_fifo_completo (data_processamento);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_tipo_dados ON com_fifo_completo (tipo_dados);
    CREATE INDEX IF NOT EXISTS idx_com_fifo_pro_codigo ON com_fifo_completo (pro_codigo);
    """
    
    try:
        engine = create_engine(POSTGRES_URL)
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print(f"✅ Tabela {TABELA_FIFO} criada com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro ao criar tabela: {e}")
        return False

def limpar_dados_teste():
    """Remove dados de teste se necessário"""
    print(f"\nDeseja limpar dados de teste da tabela {TABELA_FIFO}? (s/n): ", end="")
    resposta = input().lower()
    
    if resposta == 's':
        try:
            engine = create_engine(POSTGRES_URL)
            with engine.connect() as conn:
                result = conn.execute(text(f"DELETE FROM {TABELA_FIFO}"))
                conn.commit()
                print(f"✅ Dados removidos. Linhas afetadas: {result.rowcount}")
        except Exception as e:
            print(f"❌ Erro ao limpar dados: {e}")

if __name__ == "__main__":
    print("=== TESTE DE CONEXÃO POSTGRESQL ===\n")
    
    # 1. Testa conexão
    if not testar_conexao():
        print("\n❌ Não foi possível conectar ao banco. Verifique as configurações.")
        sys.exit(1)
    
    # 2. Verifica/cria tabela
    if not verificar_tabela():
        print("\n⚠️  Tabela não existe. Criando...")
        if not criar_tabela():
            sys.exit(1)
        # Verifica novamente
        verificar_tabela()
    
    # 3. Opção de limpeza
    if len(sys.argv) > 1 and sys.argv[1] == "--limpar":
        limpar_dados_teste()
    
    print("\n✅ Teste concluído! O sistema está pronto para salvar dados no PostgreSQL.")