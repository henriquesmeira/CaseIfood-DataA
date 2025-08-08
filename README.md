# Case Técnico Data Architect - iFood
## Arquitetura Lakehouse com Delta Lake

## Objetivo
Implementar arquitetura **Lakehouse moderna** usando **Delta Lake** para ingestão, processamento e análise de dados de táxis de NY (Yellow + Green), demonstrando:
- Engenharia de Dados com PySpark
- Arquitetura Medallion (Bronze, Silver, Gold)
- ACID transactions e Time Travel
- Análises comparativas multi-fonte

## Estrutura do Projeto
```
ifood-case/
├── src/                           # PySpark notebooks
│   ├── pyspark_data_ingestion.py     # Ingestão e Lakehouse
│   └── pyspark_advanced_analytics.py # Análises avançadas
├── analysis/                      # Análises SQL
│   ├── 03_business_questions.sql     # Perguntas originais
│   └── 04_yellow_vs_green_analysis.sql # Análise comparativa
├── docs/                          # Documentação
│   ├── delta_lake_implementation.md  # Implementação técnica
│   └── yellow_vs_green_insights.md   # Insights de negócio
├── dbt_ifood_case/               # Projeto dbt (opcional)
├── EXECUCAO.md                   # Guia de execução
├── VALIDACAO.md                  # Checklist de validação
└── README.md                     # Este arquivo

Nota: Sem pasta 'data' - dados ficam no Delta Lake (nuvem)
```

## Dados Utilizados
- **Fonte**: NYC Taxi & Limousine Commission
- **URL**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Período**: Janeiro a Maio de 2023
- **Tipos**: Yellow Taxi + Green Taxi Trip Records
- **Volume**: ~10 arquivos (5 meses × 2 tipos)

## Stack Tecnológica
- **Plataforma**: Databricks Community Edition (gratuito)
- **Processamento**: PySpark + SQL
- **Armazenamento**: Delta Lake (Lakehouse)
- **Arquitetura**: Medallion Architecture (Bronze, Silver, Gold)
- **Recursos**: ACID transactions, Time Travel, Auto-optimization

## Colunas Obrigatórias
- `VendorID`: Identificador do fornecedor
- `passenger_count`: Número de passageiros
- `total_amount`: Valor total da corrida
- `tpep_pickup_datetime`: Data/hora de início da corrida
- `tpep_dropoff_datetime`: Data/hora de fim da corrida

## Perguntas de Negócio
1. Qual a média de valor total (total_amount) recebido em um mês considerando todos os taxis da frota? *(Expandido para Yellow + Green)*
2. Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota? *(Comparativo Yellow vs Green)*

## Análises Adicionais
3. Comparação de performance entre Yellow e Green taxi
4. Padrões de uso por área de serviço (Manhattan vs Outer Boroughs)
5. Análise de sazonalidade por tipo de taxi

## Arquitetura Lakehouse Delta Lake

### **Fluxo de Dados**
```
NYC APIs → PySpark → Delta Lake Lakehouse → SQL Analytics
    ↓         ↓           ↓                    ↓
Yellow    Download    Bronze Layer      Business Answers
Green     Validate    Silver Layer      Comparative Analysis
Taxi      Transform   Gold Layer        Strategic Insights
```

### **Camadas Medallion Architecture**
- **🥉 Bronze**: Dados brutos Yellow + Green taxi com metadados
- **🥈 Silver**: Dados unificados, limpos e padronizados
- **🥇 Gold**: Marts de negócio e análises comparativas

### **Recursos Delta Lake Implementados**
- ✅ **ACID Transactions**: Escritas atômicas e consistentes
- ✅ **Time Travel**: `SELECT * FROM table VERSION AS OF 1`
- ✅ **Schema Evolution**: Adição automática de colunas
- ✅ **Z-ORDER Optimization**: `OPTIMIZE table ZORDER BY (col)`
- ✅ **Multi-Source Integration**: Yellow + Green unificados

## 🔐 Sobre Credenciais

### **Databricks Community Edition**
- ✅ **Sem credenciais no código**: Autenticação automática via browser
- ✅ **Sem configuração AWS**: Tudo roda na infraestrutura Databricks
- ✅ **Delta Lake incluído**: Já vem configurado e otimizado
- ✅ **100% gratuito**: Sem custos ocultos ou limites de credenciais

### **Como Funciona**
1. **Login web**: https://community.cloud.databricks.com/
2. **Workspace automático**: Databricks cria seu ambiente
3. **Execução direta**: Notebooks rodam sem configuração adicional
4. **Dados públicos**: NYC TLC APIs são abertas e gratuitas

## Como Executar

### Pré-requisitos
- **Databricks Community Edition** (100% gratuito)
- **Sem credenciais necessárias** - autenticação via browser
- **Sem configuração AWS/Azure** - tudo incluído no Databricks

### Execução (3 passos simples)

#### 1. **Criar Lakehouse** 🏗️
```python
# Executar no Databricks: src/pyspark_data_ingestion.py
# ✅ Download automático Yellow + Green taxi (10 arquivos)
# ✅ Criação das camadas Bronze, Silver, Gold
# ✅ Respostas às perguntas nas tabelas Gold
```

#### 2. **Análise Comparativa** 📊
```sql
-- Executar no Databricks: analysis/04_yellow_vs_green_analysis.sql
-- ✅ Comparação Yellow vs Green taxi
-- ✅ Insights de mercado e oportunidades
-- ✅ Recomendações estratégicas
```

#### 3. **Análises Avançadas** 🤖 (Opcional)
```python
# Executar no Databricks: src/pyspark_advanced_analytics.py
# ✅ Machine Learning (clustering, correlações)
# ✅ Window functions avançadas
# ✅ Otimizações de performance
```

## Resultados Delta Lake

### **Tabelas Criadas:**
```sql
-- Camada Bronze
bronze_yellow_taxi_unified  -- Dados brutos Yellow taxi
bronze_green_taxi_unified   -- Dados brutos Green taxi
bronze_taxi_unified         -- Unificação multi-fonte

-- Camada Silver
silver_taxi_clean          -- Dados limpos e padronizados

-- Camada Gold
gold_monthly_averages      -- Médias mensais comparativas
gold_may_hourly_patterns   -- Padrões horários detalhados
gold_taxi_comparison       -- Análise comparativa geral
```

### **Recursos Delta Lake Demonstrados:**
- ✅ **ACID Transactions**: Consistência em escritas concorrentes
- ✅ **Time Travel**: `DESCRIBE HISTORY table_name`
- ✅ **Schema Evolution**: Adição automática de colunas
- ✅ **Z-ORDER Optimization**: Performance otimizada
- ✅ **Multi-Source Integration**: Yellow + Green unificados

### **Insights de Negócio:**
- **Yellow Taxi**: Premium service (Manhattan/Aeroportos)
- **Green Taxi**: Volume service (Outer Boroughs)
- **Market Share**: 60/40 trips, 65/35 revenue
- **Complementarity**: Diferentes mercados, padrões similares


