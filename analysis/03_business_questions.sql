-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Case iFood - Respostas às Perguntas de Negócio
-- MAGIC ## Arquitetura: PySpark + SQL + dbt
-- MAGIC
-- MAGIC ## Objetivo
-- MAGIC Responder às duas perguntas específicas do case usando:
-- MAGIC - **PySpark**: Para processamento inicial e análises complexas
-- MAGIC - **SQL**: Para queries de negócio e agregações
-- MAGIC - **dbt**: Para transformações e documentação
-- MAGIC
-- MAGIC ### Perguntas:
-- MAGIC 1. Qual a média de valor total recebido em um mês considerando todos os yellow taxis da frota?
-- MAGIC 2. Qual a média de passageiros por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dados Disponíveis no Lakehouse Delta
-- MAGIC
-- MAGIC Após o processamento com PySpark, temos:
-- MAGIC - **Bronze**: `bronze_taxi_unified` (dados brutos Yellow + Green)
-- MAGIC - **Silver**: `silver_taxi_clean` (dados limpos e padronizados)
-- MAGIC - **Gold**: Marts de negócio prontos para análise
-- MAGIC - **Período**: Janeiro a Maio 2023

-- COMMAND ----------

-- Usar o database do projeto
USE ifood_case;

-- Verificar tabelas Delta Lake criadas
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 1: Média de Valor Total por Mês
-- MAGIC 
-- MAGIC **Pergunta**: Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow taxis da frota?

-- COMMAND ----------

-- Resposta 1: Usando tabela Gold (recomendado)
SELECT
  taxi_type,
  month_name,
  avg_amount_per_trip,
  total_trips,
  FORMAT_NUMBER(total_revenue, 2) as total_revenue_formatted
FROM gold_monthly_averages
ORDER BY month, taxi_type;

-- COMMAND ----------

-- Resposta 1 Alternativa: Consulta direta na Silver layer
SELECT
  taxi_type,
  source_month,
  CASE source_month
    WHEN 1 THEN 'Janeiro'
    WHEN 2 THEN 'Fevereiro'
    WHEN 3 THEN 'Março'
    WHEN 4 THEN 'Abril'
    WHEN 5 THEN 'Maio'
  END as month_name,
  COUNT(*) as total_trips,
  ROUND(AVG(total_amount), 2) as avg_amount_per_trip,
  ROUND(SUM(total_amount), 2) as total_revenue
FROM silver_taxi_clean
GROUP BY taxi_type, source_month
ORDER BY source_month, taxi_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Interpretação da Pergunta 1
-- MAGIC 
-- MAGIC A pergunta pode ser interpretada de duas formas:
-- MAGIC 1. **Média do valor por corrida em cada mês** (avg_amount_per_trip)
-- MAGIC 2. **Média da receita diária em cada mês** (avg_daily_revenue)
-- MAGIC 
-- MAGIC Ambas as métricas estão calculadas acima para fornecer uma visão completa.

-- COMMAND ----------

-- Análise adicional: Variação mensal
WITH monthly_stats AS (
  SELECT 
    pickup_month,
    CASE pickup_month
      WHEN 1 THEN 'Janeiro'
      WHEN 2 THEN 'Fevereiro'
      WHEN 3 THEN 'Março'
      WHEN 4 THEN 'Abril'
      WHEN 5 THEN 'Maio'
    END as month_name,
    ROUND(AVG(total_amount), 2) as avg_amount
  FROM yellow_taxi_clean
  GROUP BY pickup_month
)
SELECT 
  *,
  ROUND(avg_amount - LAG(avg_amount) OVER (ORDER BY pickup_month), 2) as month_over_month_change,
  ROUND(((avg_amount - LAG(avg_amount) OVER (ORDER BY pickup_month)) / LAG(avg_amount) OVER (ORDER BY pickup_month)) * 100, 2) as pct_change
FROM monthly_stats
ORDER BY pickup_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 2: Média de Passageiros por Hora no Mês de Maio
-- MAGIC 
-- MAGIC **Pergunta**: Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

-- COMMAND ----------

-- Resposta 2: Usando tabela Gold (recomendado)
SELECT
  taxi_type,
  hour_range,
  avg_passengers_per_trip,
  total_trips,
  time_period
FROM gold_may_hourly_patterns
ORDER BY taxi_type, pickup_hour;

-- COMMAND ----------

-- Resposta 2 Alternativa: Consulta direta na Silver layer
SELECT
  taxi_type,
  pickup_hour,
  CONCAT(LPAD(pickup_hour, 2, '0'), ':00 - ', LPAD(pickup_hour + 1, 2, '0'), ':00') as hour_range,
  COUNT(*) as total_trips,
  ROUND(AVG(passenger_count), 3) as avg_passengers_per_trip,
  SUM(passenger_count) as total_passengers,
  time_period
FROM silver_taxi_clean
WHERE source_month = 5  -- Maio
GROUP BY taxi_type, pickup_hour, time_period
ORDER BY taxi_type, pickup_hour;

-- COMMAND ----------

-- Visualização dos padrões por hora em maio
SELECT 
  pickup_hour,
  ROUND(AVG(passenger_count), 3) as avg_passengers,
  COUNT(*) as trip_count,
  -- Criar um gráfico simples com asteriscos
  REPEAT('*', CAST(ROUND(AVG(passenger_count) * 10) AS INT)) as visual_bar
FROM yellow_taxi_clean
WHERE pickup_month = 5
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Análises Complementares

-- COMMAND ----------

-- Comparação de maio com outros meses (média de passageiros por hora)
SELECT 
  pickup_month,
  CASE pickup_month
    WHEN 1 THEN 'Janeiro'
    WHEN 2 THEN 'Fevereiro'
    WHEN 3 THEN 'Março'
    WHEN 4 THEN 'Abril'
    WHEN 5 THEN 'Maio'
  END as month_name,
  pickup_hour,
  ROUND(AVG(passenger_count), 2) as avg_passengers
FROM yellow_taxi_clean
GROUP BY pickup_month, pickup_hour
ORDER BY pickup_month, pickup_hour;

-- COMMAND ----------

-- Horários de pico para passageiros em maio
SELECT 
  pickup_hour,
  ROUND(AVG(passenger_count), 2) as avg_passengers,
  COUNT(*) as trip_count,
  CASE 
    WHEN pickup_hour BETWEEN 6 AND 9 THEN 'Manhã (Rush)'
    WHEN pickup_hour BETWEEN 10 AND 16 THEN 'Meio do Dia'
    WHEN pickup_hour BETWEEN 17 AND 20 THEN 'Tarde (Rush)'
    WHEN pickup_hour BETWEEN 21 AND 23 THEN 'Noite'
    ELSE 'Madrugada'
  END as period_of_day
FROM yellow_taxi_clean
WHERE pickup_month = 5
GROUP BY pickup_hour
ORDER BY avg_passengers DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validação e Qualidade das Respostas

-- COMMAND ----------

-- Verificar dados utilizados nas análises
SELECT 
  'Total de registros analisados' as metric,
  COUNT(*) as value
FROM yellow_taxi_clean

UNION ALL

SELECT 
  'Registros de maio analisados',
  COUNT(*)
FROM yellow_taxi_clean
WHERE pickup_month = 5

UNION ALL

SELECT 
  'Período de dados',
  CONCAT(MIN(DATE(tpep_pickup_datetime)), ' a ', MAX(DATE(tpep_pickup_datetime)))
FROM yellow_taxi_clean

UNION ALL

SELECT 
  'Média geral de passageiros',
  CAST(ROUND(AVG(passenger_count), 2) AS STRING)
FROM yellow_taxi_clean

UNION ALL

SELECT 
  'Média geral de valor',
  CONCAT('$', CAST(ROUND(AVG(total_amount), 2) AS STRING))
FROM yellow_taxi_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Resumo das Respostas
-- MAGIC 
-- MAGIC ### Pergunta 1: Média de Valor Total por Mês
-- MAGIC - **Janeiro**: Média de valor por corrida
-- MAGIC - **Fevereiro**: Média de valor por corrida  
-- MAGIC - **Março**: Média de valor por corrida
-- MAGIC - **Abril**: Média de valor por corrida
-- MAGIC - **Maio**: Média de valor por corrida
-- MAGIC 
-- MAGIC ### Pergunta 2: Média de Passageiros por Hora em Maio
-- MAGIC - **Horário com maior média**: [Será preenchido após execução]
-- MAGIC - **Horário com menor média**: [Será preenchido após execução]
-- MAGIC - **Padrão observado**: [Análise dos padrões de uso]
-- MAGIC 
-- MAGIC ### Metodologia
-- MAGIC 1. **Limpeza de dados**: Removidos outliers e registros inconsistentes
-- MAGIC 2. **Filtros aplicados**: Valores positivos, duração razoável, período correto
-- MAGIC 3. **Agregações**: Utilizadas funções SQL padrão (AVG, COUNT, SUM)
-- MAGIC 4. **Validação**: Verificada qualidade e completeness dos dados
-- MAGIC 
-- MAGIC ### Justificativas Técnicas
-- MAGIC - **SQL vs PySpark**: SQL escolhido por simplicidade e clareza nas agregações
-- MAGIC - **Databricks**: Plataforma robusta para processamento de big data
-- MAGIC - **Filtros de qualidade**: Garantem resultados confiáveis
-- MAGIC - **Views intermediárias**: Facilitam manutenção e reutilização
