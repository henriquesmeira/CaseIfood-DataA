-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Case iFood - Análise Comparativa Yellow vs Green Taxi
-- MAGIC 
-- MAGIC ## Objetivo
-- MAGIC Realizar análise comparativa entre Yellow e Green taxi usando Delta Lake:
-- MAGIC - Diferenças de padrões de uso
-- MAGIC - Métricas de performance
-- MAGIC - Insights de negócio
-- MAGIC - Recomendações estratégicas

-- COMMAND ----------

-- Usar o database do projeto
USE ifood_case;

-- Verificar tabelas disponíveis
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Visão Geral dos Dados

-- COMMAND ----------

-- Estatísticas gerais por tipo de taxi
SELECT 
    taxi_type,
    service_area,
    total_trips,
    FORMAT_NUMBER(total_revenue, 2) as total_revenue_formatted,
    avg_fare,
    avg_passengers,
    avg_distance,
    avg_tip_percentage
FROM gold_taxi_comparison
ORDER BY taxi_type;

-- COMMAND ----------

-- Distribuição de viagens por mês e tipo
SELECT 
    taxi_type,
    month_name,
    total_trips,
    FORMAT_NUMBER(total_revenue, 2) as revenue,
    avg_amount_per_trip,
    avg_passengers_per_trip
FROM gold_monthly_averages
ORDER BY month, taxi_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Análise de Performance Financeira

-- COMMAND ----------

-- Comparação de receita e eficiência
WITH performance_metrics AS (
    SELECT 
        taxi_type,
        SUM(total_trips) as total_trips,
        SUM(total_revenue) as total_revenue,
        AVG(avg_amount_per_trip) as avg_fare,
        AVG(avg_passengers_per_trip) as avg_passengers,
        AVG(avg_trip_distance) as avg_distance
    FROM gold_monthly_averages
    GROUP BY taxi_type
),
market_share AS (
    SELECT 
        taxi_type,
        total_trips,
        total_revenue,
        ROUND(total_trips * 100.0 / SUM(total_trips) OVER (), 2) as trip_market_share,
        ROUND(total_revenue * 100.0 / SUM(total_revenue) OVER (), 2) as revenue_market_share,
        avg_fare,
        avg_passengers,
        avg_distance,
        ROUND(total_revenue / total_trips, 2) as revenue_per_trip,
        ROUND(total_revenue / avg_distance / total_trips, 2) as revenue_per_mile
    FROM performance_metrics
)
SELECT * FROM market_share ORDER BY taxi_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Análise de Padrões Temporais

-- COMMAND ----------

-- Padrões horários em maio - Comparação Yellow vs Green
SELECT 
    hour_range,
    SUM(CASE WHEN taxi_type = 'yellow' THEN total_trips ELSE 0 END) as yellow_trips,
    SUM(CASE WHEN taxi_type = 'green' THEN total_trips ELSE 0 END) as green_trips,
    ROUND(AVG(CASE WHEN taxi_type = 'yellow' THEN avg_passengers_per_trip END), 3) as yellow_avg_passengers,
    ROUND(AVG(CASE WHEN taxi_type = 'green' THEN avg_passengers_per_trip END), 3) as green_avg_passengers,
    ROUND(AVG(CASE WHEN taxi_type = 'yellow' THEN avg_amount_per_trip END), 2) as yellow_avg_fare,
    ROUND(AVG(CASE WHEN taxi_type = 'green' THEN avg_amount_per_trip END), 2) as green_avg_fare
FROM gold_may_hourly_patterns
GROUP BY hour_range, pickup_hour
ORDER BY pickup_hour;

-- COMMAND ----------

-- Análise de períodos de pico
SELECT 
    time_period,
    taxi_type,
    SUM(total_trips) as total_trips,
    AVG(avg_passengers_per_trip) as avg_passengers,
    AVG(avg_amount_per_trip) as avg_fare,
    ROUND(SUM(total_trips) * 100.0 / SUM(SUM(total_trips)) OVER (PARTITION BY taxi_type), 2) as pct_of_taxi_trips
FROM gold_may_hourly_patterns
GROUP BY time_period, taxi_type
ORDER BY taxi_type, 
    CASE time_period 
        WHEN 'morning_rush' THEN 1
        WHEN 'midday' THEN 2  
        WHEN 'evening_rush' THEN 3
        WHEN 'night' THEN 4
        WHEN 'late_night' THEN 5
    END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Análise de Sazonalidade

-- COMMAND ----------

-- Tendências mensais por tipo de taxi
WITH monthly_trends AS (
    SELECT 
        taxi_type,
        month,
        month_name,
        total_trips,
        avg_amount_per_trip,
        LAG(avg_amount_per_trip) OVER (PARTITION BY taxi_type ORDER BY month) as prev_month_fare,
        LAG(total_trips) OVER (PARTITION BY taxi_type ORDER BY month) as prev_month_trips
    FROM gold_monthly_averages
)
SELECT 
    taxi_type,
    month_name,
    total_trips,
    avg_amount_per_trip,
    ROUND(avg_amount_per_trip - prev_month_fare, 2) as fare_change,
    ROUND((avg_amount_per_trip - prev_month_fare) / prev_month_fare * 100, 2) as fare_change_pct,
    total_trips - prev_month_trips as trip_change,
    ROUND((total_trips - prev_month_trips) * 100.0 / prev_month_trips, 2) as trip_change_pct
FROM monthly_trends
WHERE prev_month_fare IS NOT NULL
ORDER BY taxi_type, month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Análise de Qualidade de Serviço

-- COMMAND ----------

-- Métricas de qualidade por tipo de taxi
SELECT 
    taxi_type,
    service_area,
    avg_passengers,
    avg_distance,
    avg_duration / 60 as avg_duration_hours,
    avg_tip_percentage,
    CASE 
        WHEN avg_tip_percentage > 15 THEN 'Alto'
        WHEN avg_tip_percentage > 10 THEN 'Médio'
        ELSE 'Baixo'
    END as tip_category,
    morning_rush_trips + evening_rush_trips as rush_hour_trips,
    ROUND((morning_rush_trips + evening_rush_trips) * 100.0 / total_trips, 2) as rush_hour_percentage
FROM gold_taxi_comparison
ORDER BY taxi_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Insights e Recomendações

-- COMMAND ----------

-- Análise de oportunidades
WITH opportunity_analysis AS (
    SELECT 
        'Yellow Taxi' as taxi_type,
        'Manhattan/Airport' as primary_market,
        'Higher fares, premium service' as positioning,
        'Business travelers, tourists' as target_customer,
        'Maintain premium positioning' as recommendation
    
    UNION ALL
    
    SELECT 
        'Green Taxi',
        'Outer Boroughs',
        'Volume-based, local service',
        'Local residents, daily commuters',
        'Focus on frequency and reliability'
),
performance_summary AS (
    SELECT 
        taxi_type,
        CASE 
            WHEN taxi_type = 'yellow' THEN 'Premium Service'
            ELSE 'Volume Service'
        END as business_model,
        total_trips,
        avg_fare,
        avg_tip_percentage,
        CASE 
            WHEN avg_fare > 15 THEN 'High Value'
            WHEN avg_fare > 10 THEN 'Medium Value'
            ELSE 'Low Value'
        END as value_segment
    FROM gold_taxi_comparison
)
SELECT 
    o.taxi_type,
    o.primary_market,
    o.positioning,
    o.target_customer,
    p.business_model,
    p.value_segment,
    o.recommendation
FROM opportunity_analysis o
JOIN performance_summary p ON o.taxi_type = p.taxi_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Respostas Atualizadas às Perguntas Originais

-- COMMAND ----------

-- PERGUNTA 1 ATUALIZADA: Média de valor total por mês (Yellow vs Green)
SELECT 
    'PERGUNTA 1: Média de valor total por mês' as pergunta,
    taxi_type,
    month_name,
    avg_amount_per_trip as resposta,
    total_trips,
    CONCAT('$', FORMAT_NUMBER(total_revenue, 0)) as receita_total
FROM gold_monthly_averages
ORDER BY month, taxi_type;

-- COMMAND ----------

-- PERGUNTA 2 ATUALIZADA: Média de passageiros por hora em maio (Yellow vs Green)
SELECT 
    'PERGUNTA 2: Média de passageiros por hora em maio' as pergunta,
    taxi_type,
    hour_range,
    avg_passengers_per_trip as resposta,
    total_trips,
    time_period
FROM gold_may_hourly_patterns
WHERE pickup_hour BETWEEN 7 AND 19  -- Horário comercial
ORDER BY taxi_type, pickup_hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Conclusões da Análise Comparativa
-- MAGIC 
-- MAGIC ### 🟡 Yellow Taxi:
-- MAGIC - **Mercado**: Manhattan e aeroportos
-- MAGIC - **Posicionamento**: Serviço premium
-- MAGIC - **Clientes**: Turistas e executivos
-- MAGIC - **Estratégia**: Manter qualidade e conveniência
-- MAGIC 
-- MAGIC ### 🟢 Green Taxi:
-- MAGIC - **Mercado**: Outer Boroughs (Queens, Bronx, Brooklyn)
-- MAGIC - **Posicionamento**: Serviço local e acessível
-- MAGIC - **Clientes**: Residentes locais
-- MAGIC - **Estratégia**: Foco em volume e frequência
-- MAGIC 
-- MAGIC ### 📊 Insights Principais:
-- MAGIC 1. **Complementaridade**: Os serviços atendem mercados diferentes
-- MAGIC 2. **Sazonalidade**: Padrões similares mas intensidades diferentes
-- MAGIC 3. **Oportunidades**: Green taxi pode expandir em horários de pico
-- MAGIC 4. **Eficiência**: Yellow taxi tem maior receita por viagem
-- MAGIC 
-- MAGIC ### 🎯 Recomendações:
-- MAGIC - **Yellow**: Manter foco em qualidade e conveniência
-- MAGIC - **Green**: Aumentar frequência em horários de pico
-- MAGIC - **Ambos**: Otimizar operações baseado em padrões temporais
