"""
Script para testar a API de sugestão de compra com PostgreSQL
"""
import requests
import json
from datetime import datetime

# URL base da API (ajuste se necessário)
BASE_URL = "http://localhost:5000"

def testar_health():
    """Testa se a API está funcionando"""
    try:
        response = requests.get(f"{BASE_URL}/health")
        print("=== TESTE HEALTH ===")
        print(f"Status: {response.status_code}")
        print(f"Resposta: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Erro no teste health: {e}")
        return False

def testar_dados_disponiveis():
    """Verifica se há dados no PostgreSQL"""
    try:
        response = requests.get(f"{BASE_URL}/dados-disponiveis")
        print("\n=== TESTE DADOS DISPONÍVEIS ===")
        print(f"Status: {response.status_code}")
        data = response.json()
        print(f"Resposta: {json.dumps(data, indent=2, ensure_ascii=False)}")
        
        if data.get("success"):
            analise = data.get("analise_atual_disponivel")
            if analise and analise.get("total_produtos", 0) > 0:
                print(f"✅ Análise atual disponível com {analise['total_produtos']} produtos")
                print(f"✅ Última análise: {analise['ultima_analise']}")
                return True
            else:
                print("❌ Nenhuma análise atual disponível")
                return False
        else:
            print(f"❌ Erro: {data.get('message')}")
            return False
            
    except Exception as e:
        print(f"Erro ao verificar dados: {e}")
        return False

def testar_sugestao_geral():
    """Testa sugestão geral (sem pedido específico)"""
    try:
        payload = {
            "dias_compra": 30
        }
        
        response = requests.post(
            f"{BASE_URL}/sugestao-compra",
            json=payload,
            headers={'Content-Type': 'application/json'}
        )
        
        print("\n=== TESTE SUGESTÃO GERAL ===")
        print(f"Status: {response.status_code}")
        data = response.json()
        
        if data.get("success"):
            produtos = data.get("data", [])
            print(f"✅ Sugestão gerada com sucesso para {len(produtos)} produtos")
            
            # Mostra os primeiros 5 produtos como exemplo
            if produtos:
                print("Primeiros 5 produtos:")
                for i, produto in enumerate(produtos[:5]):
                    print(f"  {i+1}. {produto['PRO_CODIGO']}: {produto['QTD_SUGERIDA']} unidades")
            
            return True
        else:
            print(f"❌ Erro: {data.get('message')}")
            return False
            
    except Exception as e:
        print(f"Erro no teste de sugestão geral: {e}")
        return False

def testar_sugestao_por_marca():
    """Testa sugestão filtrada por marca"""
    try:
        payload = {
            "marca_descricao": "BOSCH",  # Ajuste conforme suas marcas
            "dias_compra": 45
        }
        
        response = requests.post(
            f"{BASE_URL}/sugestao-compra",
            json=payload,
            headers={'Content-Type': 'application/json'}
        )
        
        print("\n=== TESTE SUGESTÃO POR MARCA ===")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        print(f"Status: {response.status_code}")
        data = response.json()
        
        if data.get("success"):
            produtos = data.get("data", [])
            print(f"✅ Sugestão por marca gerada com sucesso para {len(produtos)} produtos")
            
            # Mostra alguns produtos como exemplo
            if produtos:
                print("Produtos encontrados:")
                for i, produto in enumerate(produtos[:3]):
                    print(f"  {i+1}. {produto['PRO_CODIGO']}: {produto['QTD_SUGERIDA']} unidades")
            
            return True
        else:
            print(f"ℹ️ Resposta: {data.get('message')}")
            return data.get("message") == "Nenhum resultado encontrado"  # OK se não há produtos da marca
            
    except Exception as e:
        print(f"Erro no teste por marca: {e}")
        return False

def testar_sugestao_get():
    """Testa endpoint GET com query parameters"""
    try:
        params = {
            "dias_compra": 60
        }
        
        response = requests.get(f"{BASE_URL}/sugestao-compra", params=params)
        
        print("\n=== TESTE SUGESTÃO GET ===")
        print(f"URL: {response.url}")
        print(f"Status: {response.status_code}")
        data = response.json()
        
        if data.get("success"):
            produtos = data.get("data", [])
            print(f"✅ Sugestão GET gerada com sucesso para {len(produtos)} produtos")
            return True
        else:
            print(f"❌ Erro: {data.get('message')}")
            return False
            
    except Exception as e:
        print(f"Erro no teste GET: {e}")
        return False

def main():
    print("=== TESTE COMPLETO DA API DE SUGESTÃO DE COMPRA ===")
    print(f"Testando API em: {BASE_URL}")
    print(f"Data/Hora: {datetime.now()}")
    
    resultados = []
    
    # 1. Teste básico de saúde
    resultados.append(("Health Check", testar_health()))
    
    # 2. Verificar dados no PostgreSQL
    resultados.append(("Dados PostgreSQL", testar_dados_disponiveis()))
    
    # Se os dados estão disponíveis, continua com os testes
    if resultados[-1][1]:  # Se o teste de dados passou
        
        # 3. Teste de sugestão geral
        resultados.append(("Sugestão Geral", testar_sugestao_geral()))
        
        # 4. Teste por marca
        resultados.append(("Sugestão por Marca", testar_sugestao_por_marca()))
        
        # 5. Teste GET
        resultados.append(("Sugestão GET", testar_sugestao_get()))
    
    # Resumo dos resultados
    print("\n" + "="*50)
    print("RESUMO DOS TESTES:")
    print("="*50)
    
    passed = 0
    total = len(resultados)
    
    for teste, resultado in resultados:
        status = "✅ PASSOU" if resultado else "❌ FALHOU"
        print(f"{teste:<25} {status}")
        if resultado:
            passed += 1
    
    print(f"\nResultado final: {passed}/{total} testes passaram")
    
    if passed == total:
        print("🎉 Todos os testes passaram! A API está funcionando corretamente com PostgreSQL.")
    else:
        print("⚠️ Alguns testes falharam. Verifique os logs acima.")
        
        if not resultados[1][1]:  # Se dados PostgreSQL falharam
            print("\n💡 DICA: Execute primeiro o script principal (index_db.py) para gerar dados no PostgreSQL:")
            print("   python index_db.py run")

if __name__ == "__main__":
    main()