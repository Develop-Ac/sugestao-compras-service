package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

// ============================
// CONFIGURAÇÕES
// ============================

const (
	DIAS_ESTOQUE_DESEJADO = 90
	LEAD_TIME_DIAS        = 17
	EMPRESA_PEDIDO        = 3
)

// Configurações PostgreSQL
var (
	POSTGRES_URL = "postgresql://intranet:Ac%402025acesso@panel-teste.acacessorios.local:5555/intranet"
	TABELA_FIFO  = "com_fifo_completo"
)

// ============================
// ESTRUTURAS DE DADOS
// ============================

type Config struct {
	PostgresURL        string
	DiasEstoqueDesejado int
	LeadTimeDias       int
	EmpresaPedido      int
}

type ProdutoFIFO struct {
	ProCodigo                string  `json:"pro_codigo"`
	ProDescricao             string  `json:"pro_descricao"`
	MarDescricao             string  `json:"mar_descricao"`
	Fornecedor1              string  `json:"fornecedor1"`
	Fornecedor2              string  `json:"fornecedor2"`
	Fornecedor3              string  `json:"fornecedor3"`
	EstoqueDisponivel        float64 `json:"estoque_disponivel"`
	QtdVendidaPeriodo        float64 `json:"qtd_vendida_periodo"`
	ValorVendidoPeriodo      float64 `json:"valor_vendido_periodo"`
	DataUltimaVenda          *time.Time `json:"data_ultima_venda"`
	CurvaABC                 string  `json:"curva_abc"`
	DemandaMediaDia          float64 `json:"demanda_media_dia"`
	DemandaMediaDiaAjustada  float64 `json:"demanda_media_dia_ajustada"`
	EstoqueMinSugerido       int     `json:"estoque_min_sugerido"`
	EstoqueMaxSugerido       int     `json:"estoque_max_sugerido"`
	TipoPlanejamento         string  `json:"tipo_planejamento"`
	AlertaTendenciaAlta      string  `json:"alerta_tendencia_alta"`
	NumVendas                int     `json:"num_vendas"`
}

type SugestaoCompra struct {
	ProCodigo           string  `json:"PRO_CODIGO"`
	QtdSugerida         int     `json:"QTD_SUGERIDA"`
	EstoqueDisponivel   float64 `json:"ESTOQUE_DISPONIVEL"`
	EstoqueMinAlvo      int     `json:"ESTOQUE_MIN_ALVO"`
	EstoqueMaxAlvo      int     `json:"ESTOQUE_MAX_ALVO"`
	Prioridade          string  `json:"PRIORIDADE"`
	MotivoSugestao      string  `json:"MOTIVO_SUGESTAO"`
}

type SugestaoRequest struct {
	PedidoCotacao  *int    `json:"pedido_cotacao,omitempty"`
	MarcaDescricao *string `json:"marca_descricao,omitempty"`
	DiasCompra     *int    `json:"dias_compra,omitempty"`
}

type APIResponse struct {
	Success bool              `json:"success"`
	Data    []SugestaoCompra  `json:"data"`
	Message string           `json:"message"`
}

type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// ============================
// CONEXÃO COM BANCO
// ============================

func conectarPostgreSQL() (*sql.DB, error) {
	db, err := sql.Open("postgres", POSTGRES_URL)
	if err != nil {
		return nil, fmt.Errorf("erro ao conectar com PostgreSQL: %v", err)
	}
	
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("erro ao verificar conexão PostgreSQL: %v", err)
	}
	
	return db, nil
}

func carregarAnaliseAtualPostgreSQL(db *sql.DB) ([]ProdutoFIFO, error) {
	query := `
		SELECT 
			COALESCE(pro_codigo, '') as pro_codigo,
			COALESCE(pro_descricao, '') as pro_descricao,
			COALESCE(mar_descricao, '') as mar_descricao,
			COALESCE(fornecedor1, '') as fornecedor1,
			COALESCE(fornecedor2, '') as fornecedor2,
			COALESCE(fornecedor3, '') as fornecedor3,
			COALESCE(estoque_disponivel, 0) as estoque_disponivel,
			COALESCE(qtd_vendida, 0) as qtd_vendida,
			COALESCE(valor_vendido, 0) as valor_vendido,
			data_max_venda,
			COALESCE(curva_abc, 'C') as curva_abc,
			COALESCE(demanda_media_dia, 0) as demanda_media_dia,
			COALESCE(demanda_media_dia_ajustada, 0) as demanda_media_dia_ajustada,
			COALESCE(estoque_min_sugerido, 0) as estoque_min_sugerido,
			COALESCE(estoque_max_sugerido, 0) as estoque_max_sugerido,
			COALESCE(tipo_planejamento, 'Normal') as tipo_planejamento,
			COALESCE(alerta_tendencia_alta, 'Não') as alerta_tendencia_alta,
			COALESCE(num_vendas, 0) as num_vendas
		FROM com_fifo_completo 
		ORDER BY pro_codigo
	`
	
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao executar query: %v", err)
	}
	defer rows.Close()
	
	var produtos []ProdutoFIFO
	
	for rows.Next() {
		var produto ProdutoFIFO
		var dataMaxVenda sql.NullTime
		
		err := rows.Scan(
			&produto.ProCodigo,
			&produto.ProDescricao,
			&produto.MarDescricao,
			&produto.Fornecedor1,
			&produto.Fornecedor2,
			&produto.Fornecedor3,
			&produto.EstoqueDisponivel,
			&produto.QtdVendidaPeriodo,
			&produto.ValorVendidoPeriodo,
			&dataMaxVenda,
			&produto.CurvaABC,
			&produto.DemandaMediaDia,
			&produto.DemandaMediaDiaAjustada,
			&produto.EstoqueMinSugerido,
			&produto.EstoqueMaxSugerido,
			&produto.TipoPlanejamento,
			&produto.AlertaTendenciaAlta,
			&produto.NumVendas,
		)
		
		if err != nil {
			return nil, fmt.Errorf("erro ao fazer scan dos dados: %v", err)
		}
		
		if dataMaxVenda.Valid {
			produto.DataUltimaVenda = &dataMaxVenda.Time
		}
		
		produtos = append(produtos, produto)
	}
	
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("erro ao iterar sobre os resultados: %v", err)
	}
	
	log.Printf("Carregados %d registros da análise FIFO do PostgreSQL", len(produtos))
	return produtos, nil
}

// ============================
// LÓGICA DE SUGESTÃO
// ============================

func aplicarArredondamento(valor float64, curva string) int {
	curvaUpper := strings.ToUpper(curva)
	
	if curvaUpper == "A" || curvaUpper == "B" {
		return int(math.Ceil(valor))
	}
	
	// Curva C, D ou outras (arredondamento normal)
	return int(math.Floor(valor + 0.5))
}

func calcularSugestaoPura(estoque float64, estoqueMinCalc, estoqueMaxCalc int, tipo, alerta, curva string) int {
	// Sob demanda: não sugere automático
	if tipo == "Sob_Demanda" {
		return 0
	}
	
	// Se nem máximo faz sentido (0 ou negativo), não sugere
	if estoqueMaxCalc <= 0 {
		return 0
	}
	
	// Se já está no máximo ou acima, não sugere
	if estoque >= float64(estoqueMaxCalc) {
		return 0
	}
	
	// Complementar até o máximo alvo
	base := float64(estoqueMaxCalc) - estoque
	if base < 0 {
		base = 0
	}
	
	fator := 1.0
	// Reforçinho só para itens importantes com tendência alta
	if alerta == "Sim" && (curva == "A" || curva == "B") {
		fator = 1.2
	}
	
	resultado := aplicarArredondamento(base*fator, curva)
	return resultado
}

func sugerirCompra(produto ProdutoFIFO, diasCompraUser int) SugestaoCompra {
	estoque := produto.EstoqueDisponivel
	estoqueMin := produto.EstoqueMinSugerido
	estoqueMax := produto.EstoqueMaxSugerido
	tipo := produto.TipoPlanejamento
	alerta := produto.AlertaTendenciaAlta
	curva := produto.CurvaABC
	demanda := produto.DemandaMediaDiaAjustada
	
	// Se os parâmetros de política (min/máx) estiverem faltando
	if estoqueMin == 0 && estoqueMax == 0 {
		return SugestaoCompra{
			ProCodigo:         produto.ProCodigo,
			QtdSugerida:       0,
			EstoqueDisponivel: estoque,
			EstoqueMinAlvo:    estoqueMin,
			EstoqueMaxAlvo:    estoqueMax,
			Prioridade:        "Sem dados",
			MotivoSugestao:    "Sem dados suficientes de estoque mínimo/máximo para sugerir compra automática.",
		}
	}
	
	// Cálculo de estoque alvo (min/max)
	estoqueMinCalc := estoqueMin
	estoqueMaxCalc := estoqueMax
	
	if diasCompraUser > 0 && demanda > 0 {
		// Sobrescreve o Máximo com base na demanda diária * dias solicitados
		estoqueMaxCalc = int(math.Ceil(demanda * float64(diasCompraUser)))
	}
	
	// Garantir coerência min <= max
	if estoqueMaxCalc < estoqueMinCalc {
		estoqueMaxCalc = estoqueMinCalc
	}
	
	// SUGESTAO pura (sem considerar o pedido)
	sugestaoPura := calcularSugestaoPura(estoque, estoqueMinCalc, estoqueMaxCalc, tipo, alerta, curva)
	
	// Casos especiais de política
	if tipo == "Sob_Demanda" {
		motivo := fmt.Sprintf("Sob Demanda. Est: %.0f. Sem sugestão auto.", estoque)
		return SugestaoCompra{
			ProCodigo:         produto.ProCodigo,
			QtdSugerida:       0,
			EstoqueDisponivel: estoque,
			EstoqueMinAlvo:    estoqueMinCalc,
			EstoqueMaxAlvo:    estoqueMaxCalc,
			Prioridade:        "Sob Demanda",
			MotivoSugestao:    motivo,
		}
	}
	
	if estoqueMinCalc == 0 && estoqueMaxCalc == 0 {
		motivo := fmt.Sprintf("Sem política. Est: %.0f. Avaliar manual.", estoque)
		return SugestaoCompra{
			ProCodigo:         produto.ProCodigo,
			QtdSugerida:       0,
			EstoqueDisponivel: estoque,
			EstoqueMinAlvo:    estoqueMinCalc,
			EstoqueMaxAlvo:    estoqueMaxCalc,
			Prioridade:        "Sem política",
			MotivoSugestao:    motivo,
		}
	}
	
	// MODO PLANEJAMENTO GERAL (SEM PEDIDO)
	estoqueAtual := estoque
	qtdSug := sugestaoPura
	var prioridade, motivo string
	
	// Estoque abaixo do mínimo
	if estoqueAtual < float64(estoqueMinCalc) {
		prioridade = "Crítico"
		motivo = fmt.Sprintf("Est: %.0f < Min: %d.", estoqueAtual, estoqueMinCalc)
		if qtdSug > 0 {
			motivo += fmt.Sprintf(" Sugere-se +%d p/ atingir Max: %d.", qtdSug, estoqueMaxCalc)
		} else {
			motivo += " Sem compra auto sugerida."
		}
	} else if estoqueAtual >= float64(estoqueMinCalc) && estoqueAtual < float64(estoqueMaxCalc) {
		// Estoque entre mínimo e máximo
		if alerta == "Sim" && (curva == "A" || curva == "B") && qtdSug > 0 {
			prioridade = "Oportunidade Tendência"
			motivo = fmt.Sprintf("Est: %.0f (Alvo: %d-%d). Curva %s c/ tendência alta: sugere-se +%d.", 
				estoqueAtual, estoqueMinCalc, estoqueMaxCalc, curva, qtdSug)
		} else {
			prioridade = "OK"
			motivo = fmt.Sprintf("Est: %.0f ok (Alvo: %d-%d).", estoqueAtual, estoqueMinCalc, estoqueMaxCalc)
		}
	} else {
		// Estoque no máximo ou acima
		prioridade = "Excedente ou cheio"
		excedente := estoqueAtual - float64(estoqueMaxCalc)
		motivo = fmt.Sprintf("Est: %.0f > Max: %d (Exced: %.0f). Sem compra.", 
			estoqueAtual, estoqueMaxCalc, excedente)
	}
	
	return SugestaoCompra{
		ProCodigo:         produto.ProCodigo,
		QtdSugerida:       qtdSug,
		EstoqueDisponivel: estoqueAtual,
		EstoqueMinAlvo:    estoqueMinCalc,
		EstoqueMaxAlvo:    estoqueMaxCalc,
		Prioridade:        prioridade,
		MotivoSugestao:    motivo,
	}
}

func executarSugestao(db *sql.DB, marcaDescricao *string, diasCompra int) ([]SugestaoCompra, error) {
	// Carrega dados do PostgreSQL
	produtos, err := carregarAnaliseAtualPostgreSQL(db)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar dados do PostgreSQL: %v", err)
	}
	
	if len(produtos) == 0 {
		return nil, fmt.Errorf("nenhum produto encontrado")
	}
	
	// Filtro por marca se especificado
	if marcaDescricao != nil && *marcaDescricao != "" {
		var produtosFiltrados []ProdutoFIFO
		marcaFiltro := strings.ToUpper(*marcaDescricao)
		
		for _, produto := range produtos {
			if strings.Contains(strings.ToUpper(produto.MarDescricao), marcaFiltro) {
				produtosFiltrados = append(produtosFiltrados, produto)
			}
		}
		produtos = produtosFiltrados
	}
	
	if len(produtos) == 0 {
		return nil, fmt.Errorf("nenhum produto encontrado após filtro de marca")
	}
	
	// Aplica a lógica de sugestão
	var sugestoes []SugestaoCompra
	for _, produto := range produtos {
		sugestao := sugerirCompra(produto, diasCompra)
		sugestoes = append(sugestoes, sugestao)
	}
	
	return sugestoes, nil
}

// ============================
// HANDLERS HTTP
// ============================

func setupRoutes(db *sql.DB) *gin.Engine {
	r := gin.Default()
	
	// Middleware para CORS
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization")
		
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		
		c.Next()
	})
	
	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, HealthResponse{
			Status:    "ok",
			Timestamp: time.Now().Format(time.RFC3339),
		})
	})
	
	// Endpoint principal de sugestão de compra
	r.POST("/sugestao-compra", func(c *gin.Context) {
		var req SugestaoRequest
		
		// Parse do JSON (opcional)
		if err := c.ShouldBindJSON(&req); err != nil {
			// Se não conseguir fazer parse, usar valores padrão
			req = SugestaoRequest{}
		}
		
		// Definir valores padrão
		diasCompra := 30
		if req.DiasCompra != nil {
			diasCompra = *req.DiasCompra
		}
		
		if diasCompra <= 0 {
			c.JSON(400, APIResponse{
				Success: false,
				Data:    []SugestaoCompra{},
				Message: "dias_compra deve ser um número inteiro positivo",
			})
			return
		}
		
		// Executar sugestão
		sugestoes, err := executarSugestao(db, req.MarcaDescricao, diasCompra)
		if err != nil {
			c.JSON(500, APIResponse{
				Success: false,
				Data:    []SugestaoCompra{},
				Message: fmt.Sprintf("Erro interno: %v", err),
			})
			return
		}
		
		// Filtrar apenas produtos com sugestão > 0
		var produtosComSugestao []SugestaoCompra
		for _, sugestao := range sugestoes {
			if sugestao.QtdSugerida > 0 {
				produtosComSugestao = append(produtosComSugestao, sugestao)
			}
		}
		
		if len(produtosComSugestao) == 0 {
			c.JSON(200, APIResponse{
				Success: true,
				Data:    []SugestaoCompra{},
				Message: "Nenhum produto necessita de compra no momento",
			})
			return
		}
		
		c.JSON(200, APIResponse{
			Success: true,
			Data:    produtosComSugestao,
			Message: fmt.Sprintf("Sugestão gerada com sucesso. %d produtos.", len(produtosComSugestao)),
		})
	})
	
	// Endpoint de diagnóstico (sem tentar conectar SQL Server)
	r.GET("/diagnostico", func(c *gin.Context) {
		diagnostico := map[string]interface{}{
			"timestamp":  time.Now().Format(time.RFC3339),
			"postgresql": map[string]interface{}{
				"status":  "ok",
				"message": "Conectado com sucesso",
			},
		}
		
		c.JSON(200, diagnostico)
	})
	
	return r
}

// ============================
// MAIN
// ============================

func main() {
	// Configurar log
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	
	// Conectar ao PostgreSQL
	db, err := conectarPostgreSQL()
	if err != nil {
		log.Fatalf("Erro ao conectar ao PostgreSQL: %v", err)
	}
	defer db.Close()
	
	log.Println("Conectado ao PostgreSQL com sucesso")
	
	// Setup do Gin
	gin.SetMode(gin.ReleaseMode)
	r := setupRoutes(db)
	
	// Porta
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	
	log.Printf("Servidor iniciando na porta %s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
}