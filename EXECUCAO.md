# Guia de Execução - Case iFood Delta Lake

## 🚀 Execução Rápida (3 passos)

### **Pré-requisito**
- Conta no [Databricks Community Edition](https://community.cloud.databricks.com/) (gratuito)
- **Sem credenciais necessárias** - autenticação via browser

### **Passo 1: Criar Lakehouse** 🏗️
1. **Login**: Acesse https://community.cloud.databricks.com/
2. **Cluster**: Use "Serverless SQL Warehouse" (recomendado)
3. **Import**: Faça upload do arquivo `src/pyspark_data_ingestion.py`
4. **Execute**: Rode todas as células (sem configurar credenciais!)
5. ✅ **Resultado**: Lakehouse Delta criado com camadas Bronze, Silver, Gold

### **Passo 2: Análise Comparativa** 📊
1. Importe o notebook: `analysis/04_yellow_vs_green_analysis.sql`
2. Execute todas as células
3. ✅ **Resultado**: Análise comparativa Yellow vs Green taxi

### **Passo 3: Verificar Resultados** ✅
```sql
-- Pergunta 1: Médias mensais
SELECT * FROM ifood_case.gold_monthly_averages;

-- Pergunta 2: Padrões horários maio
SELECT * FROM ifood_case.gold_may_hourly_patterns;

-- Comparação geral
SELECT * FROM ifood_case.gold_taxi_comparison;
```

## 📋 O que será criado

### **Tabelas Delta Lake**
```
ifood_case/
├── bronze_taxi_unified         # Dados brutos Yellow + Green
├── silver_taxi_clean          # Dados limpos e padronizados  
├── gold_monthly_averages      # Resposta Pergunta 1
├── gold_may_hourly_patterns   # Resposta Pergunta 2
└── gold_taxi_comparison       # Análise comparativa
```

### **Recursos Delta Lake Demonstrados**
- ✅ ACID Transactions
- ✅ Time Travel: `DESCRIBE HISTORY table_name`
- ✅ Schema Evolution
- ✅ Auto Optimization
- ✅ Multi-Source Integration

## 🎯 Resultados Esperados

### **Pergunta 1**: Média de valor total por mês
- **Yellow**: $16-20 por viagem
- **Green**: $12-15 por viagem
- **Insight**: Yellow tem 25-30% maior receita

### **Pergunta 2**: Média de passageiros por hora (maio)
- **Yellow**: Picos em rush hours (1.8-2.2)
- **Green**: Mais uniforme (1.5-1.8)
- **Insight**: Padrões diferentes por área

### **Análise Extra**: Comparação de mercados
- **Yellow**: Manhattan/Aeroportos (premium)
- **Green**: Outer Boroughs (volume)
- **Market Share**: 60/40 trips, 65/35 revenue

## 🔧 Opcional: Análises Avançadas

### **Machine Learning** (Opcional)
```python
# Executar: src/pyspark_advanced_analytics.py
# ✅ Clustering para detecção de outliers
# ✅ Análise de correlações
# ✅ Window functions avançadas
```

### **dbt Integration** (Opcional)
```bash
# Demonstração de integração dbt + Delta Lake
cd dbt_ifood_case
dbt run
```

## ⏱️ Tempo Estimado
- **Setup**: 5 minutos
- **Execução**: 15-20 minutos
- **Análise**: 10 minutos
- **Total**: ~30-35 minutos

## 🔐 Sobre Credenciais

### **Databricks Community Edition**
- ✅ **Sem credenciais no código**: Autenticação via browser
- ✅ **Sem AWS/Azure**: Tudo roda na nuvem Databricks
- ✅ **Sem configuração**: Delta Lake já incluído
- ✅ **Gratuito**: Sem custos adicionais

### **Diferença para Produção**
- **Community**: Login web, sem credenciais no código
- **Production**: Tokens, service principals, secrets
- **Nosso case**: Community é perfeito para demonstração

## 🆘 Troubleshooting

### **Erro de cluster**
- Use Serverless SQL Warehouse (recomendado)
- Ou crie cluster com runtime mais recente

### **Erro de download**
- Verifique conexão com internet
- URLs dos dados são públicas (NYC TLC)

### **Erro de Delta Lake**
- Databricks Community já tem Delta Lake
- Não precisa instalar nada adicional

### **"Onde estão as credenciais?"**
- **Resposta**: Não precisa! Community Edition usa autenticação web
- **Produção**: Usaria tokens/secrets, mas não é necessário aqui

---
**Demonstração completa de Lakehouse moderno com Delta Lake!** 🎉
