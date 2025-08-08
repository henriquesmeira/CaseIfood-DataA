# dbt Project - Case iFood

## Arquitetura de Dados

Este projeto implementa uma arquitetura moderna de Data Lake com dbt para transformação de dados:

```
S3 (Data Lake)
├── raw/           # Dados originais (ingestão)
├── staging/       # dbt staging (limpeza)
├── intermediate/  # dbt intermediate (transformações)
└── marts/         # dbt marts (análises finais)
```

## Estrutura do Projeto dbt

```
dbt_ifood_case/
├── models/
│   ├── staging/           # Camada de limpeza e padronização
│   │   ├── _sources.yml   # Definição das fontes de dados
│   │   ├── _models.yml    # Testes e documentação
│   │   └── stg_yellow_taxi_trips.sql
│   ├── intermediate/      # Camada de transformações de negócio
│   │   └── int_taxi_trips_quality_filtered.sql
│   └── marts/            # Camada de análises finais
│       ├── mart_monthly_averages.sql
│       └── mart_hourly_patterns_may.sql
├── macros/               # Funções reutilizáveis
│   └── data_quality_checks.sql
├── tests/               # Testes customizados
├── dbt_project.yml      # Configuração do projeto
└── profiles.yml         # Configuração de conexão
```

## Setup

### 1. Instalar dbt
```bash
pip install dbt-databricks
```

### 2. Configurar Variáveis de Ambiente
```bash
cp .env.example .env
# Editar .env com suas credenciais
```

### 3. Testar Conexão
```bash
dbt debug
```

### 4. Executar Transformações
```bash
# Executar todos os modelos
dbt run

# Executar apenas staging
dbt run --select staging

# Executar testes
dbt test

# Gerar documentação
dbt docs generate
dbt docs serve
```

## Modelos

### Staging Layer
- **stg_yellow_taxi_trips**: Dados limpos e padronizados de todos os meses

### Intermediate Layer  
- **int_taxi_trips_quality_filtered**: Dados com filtros de qualidade aplicados

### Marts Layer
- **mart_monthly_averages**: Resposta à Pergunta 1 (médias mensais)
- **mart_hourly_patterns_may**: Resposta à Pergunta 2 (padrões por hora em maio)

## Perguntas de Negócio

### Pergunta 1: Média de valor total por mês
```sql
select * from {{ ref('mart_monthly_averages') }}
```

### Pergunta 2: Média de passageiros por hora em maio
```sql
select * from {{ ref('mart_hourly_patterns_may') }}
```

## Testes de Qualidade

O projeto inclui testes abrangentes:
- Testes de unicidade e não-nulidade
- Testes de valores aceitos
- Testes de integridade referencial
- Testes de regras de negócio

## Lineage

O dbt gera automaticamente a linhagem dos dados:
```
raw_data → staging → intermediate → marts
```

## Comandos Úteis

```bash
# Executar modelo específico
dbt run --select mart_monthly_averages

# Executar com full-refresh
dbt run --full-refresh

# Executar testes específicos
dbt test --select stg_yellow_taxi_trips

# Compilar sem executar
dbt compile

# Verificar freshness dos dados
dbt source freshness
```
