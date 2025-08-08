# Databricks notebook source
# MAGIC %md
# MAGIC # Case iFood - Análises Avançadas com PySpark
# MAGIC 
# MAGIC ## Objetivo
# MAGIC Usar PySpark para análises mais complexas que complementam o dbt:
# MAGIC 1. Machine Learning para detecção de outliers
# MAGIC 2. Análises estatísticas avançadas
# MAGIC 3. Processamento de janelas temporais
# MAGIC 4. Otimizações de performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Configurar para melhor performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("✓ Imports e configurações concluídas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento dos Dados

# COMMAND ----------

# Carregar dados da camada Silver do Delta Lake
df = spark.table("ifood_case.silver_taxi_clean")

print(f"📊 Dataset carregado: {df.count():,} registros")
print(f"📅 Período: {df.select(min('pickup_date'), max('pickup_date')).collect()[0]}")

# Cache para performance
df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análise de Outliers com PySpark ML

# COMMAND ----------

# Preparar dados para análise de outliers
features_df = df.select(
    "total_amount",
    "passenger_count", 
    "trip_duration_minutes",
    "trip_distance"
).filter(
    (col("total_amount") > 0) & 
    (col("passenger_count") > 0) &
    (col("trip_duration_minutes") > 0) &
    (col("trip_distance") > 0)
)

# Criar vetor de features
assembler = VectorAssembler(
    inputCols=["total_amount", "passenger_count", "trip_duration_minutes", "trip_distance"],
    outputCol="features"
)

features_vector = assembler.transform(features_df)

# Aplicar K-Means para detectar outliers
kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster")
model = kmeans.fit(features_vector)

# Adicionar clusters ao dataset
clustered_df = model.transform(features_vector)

# Analisar clusters
cluster_analysis = clustered_df.groupBy("cluster").agg(
    count("*").alias("count"),
    avg("total_amount").alias("avg_amount"),
    avg("passenger_count").alias("avg_passengers"),
    avg("trip_duration_minutes").alias("avg_duration"),
    avg("trip_distance").alias("avg_distance")
)

print("🎯 Análise de Clusters (Detecção de Outliers):")
cluster_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Análises de Janela Temporal

# COMMAND ----------

# Definir janelas temporais
hourly_window = Window.partitionBy("pickup_date", "pickup_hour").orderBy("tpep_pickup_datetime")
daily_window = Window.partitionBy("pickup_date").orderBy("tpep_pickup_datetime")

# Análise de padrões temporais com window functions
temporal_analysis = df.withColumn(
    "trips_in_hour", count("*").over(hourly_window)
).withColumn(
    "running_daily_total", sum("total_amount").over(daily_window)
).withColumn(
    "hour_rank_by_volume", dense_rank().over(
        Window.partitionBy("pickup_date").orderBy(desc("trips_in_hour"))
    )
).withColumn(
    "prev_trip_amount", lag("total_amount").over(
        Window.partitionBy("VendorID").orderBy("tpep_pickup_datetime")
    )
)

# Encontrar horários de pico por dia
peak_hours = temporal_analysis.filter(col("hour_rank_by_volume") == 1) \
    .select("pickup_date", "pickup_hour", "trips_in_hour") \
    .distinct() \
    .orderBy("pickup_date")

print("🕐 Horários de Pico por Dia:")
peak_hours.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resposta à Pergunta 1 com PySpark

# COMMAND ----------

# Pergunta 1: Média de valor total por mês usando PySpark
print("💰 PERGUNTA 1: Média de valor total por mês")
print("="*50)

monthly_averages = df.filter(col("total_amount") > 0) \
    .groupBy("source_year", "source_month") \
    .agg(
        count("*").alias("total_trips"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_amount_per_trip"),
        stddev("total_amount").alias("stddev_amount"),
        expr("percentile_approx(total_amount, 0.5)").alias("median_amount"),
        countDistinct("pickup_date").alias("active_days")
    ) \
    .withColumn(
        "avg_daily_revenue", 
        round(col("total_revenue") / col("active_days"), 2)
    ) \
    .withColumn(
        "month_name",
        when(col("source_month") == 1, "Janeiro")
        .when(col("source_month") == 2, "Fevereiro") 
        .when(col("source_month") == 3, "Março")
        .when(col("source_month") == 4, "Abril")
        .when(col("source_month") == 5, "Maio")
    ) \
    .orderBy("source_year", "source_month")

monthly_averages.show()

# Calcular variação mês a mês
window_spec = Window.orderBy("source_year", "source_month")
monthly_with_change = monthly_averages.withColumn(
    "prev_month_avg", lag("avg_amount_per_trip").over(window_spec)
).withColumn(
    "month_over_month_change", 
    round(col("avg_amount_per_trip") - col("prev_month_avg"), 2)
).withColumn(
    "month_over_month_pct", 
    round(((col("avg_amount_per_trip") - col("prev_month_avg")) / col("prev_month_avg")) * 100, 2)
)

print("\n📈 Variação Mês a Mês:")
monthly_with_change.select(
    "month_name", "avg_amount_per_trip", "month_over_month_change", "month_over_month_pct"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resposta à Pergunta 2 com PySpark

# COMMAND ----------

# Pergunta 2: Média de passageiros por hora em maio usando PySpark
print("👥 PERGUNTA 2: Média de passageiros por hora em maio")
print("="*50)

may_hourly_patterns = df.filter(col("source_month") == 5) \
    .groupBy("pickup_hour") \
    .agg(
        count("*").alias("total_trips"),
        avg("passenger_count").alias("avg_passengers_per_trip"),
        sum("passenger_count").alias("total_passengers"),
        stddev("passenger_count").alias("stddev_passengers"),
        expr("percentile_approx(passenger_count, 0.5)").alias("median_passengers"),
        min("passenger_count").alias("min_passengers"),
        max("passenger_count").alias("max_passengers"),
        avg("total_amount").alias("avg_amount_per_trip")
    ) \
    .withColumn(
        "hour_range",
        concat(
            lpad(col("pickup_hour"), 2, "0"),
            lit(":00 - "),
            lpad(col("pickup_hour") + 1, 2, "0"),
            lit(":00")
        )
    ) \
    .withColumn(
        "period_classification",
        when((col("pickup_hour") >= 6) & (col("pickup_hour") <= 9), "Manhã (Rush)")
        .when((col("pickup_hour") >= 10) & (col("pickup_hour") <= 16), "Meio do Dia")
        .when((col("pickup_hour") >= 17) & (col("pickup_hour") <= 20), "Tarde (Rush)")
        .when((col("pickup_hour") >= 21) & (col("pickup_hour") <= 23), "Noite")
        .otherwise("Madrugada")
    ) \
    .orderBy("pickup_hour")

may_hourly_patterns.show(24)

# Ranking por média de passageiros
hourly_ranked = may_hourly_patterns.withColumn(
    "rank_by_avg_passengers", 
    dense_rank().over(Window.orderBy(desc("avg_passengers_per_trip")))
).withColumn(
    "rank_by_trip_volume",
    dense_rank().over(Window.orderBy(desc("total_trips")))
)

print("\n🏆 Top 5 horários por média de passageiros:")
hourly_ranked.filter(col("rank_by_avg_passengers") <= 5) \
    .select("hour_range", "avg_passengers_per_trip", "total_trips", "period_classification") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Análise de Correlações

# COMMAND ----------

# Análise de correlação entre variáveis
correlation_data = df.select(
    "total_amount",
    "passenger_count", 
    "trip_duration_minutes",
    "pickup_hour"
).filter(
    (col("total_amount") > 0) & 
    (col("passenger_count") > 0) &
    (col("trip_duration_minutes") > 0)
)

# Preparar para análise de correlação
assembler = VectorAssembler(
    inputCols=["total_amount", "passenger_count", "trip_duration_minutes", "pickup_hour"],
    outputCol="features"
)

correlation_vector = assembler.transform(correlation_data)

# Calcular matriz de correlação
correlation_matrix = Correlation.corr(correlation_vector, "features").head()
correlation_array = correlation_matrix[0].toArray()

print("🔗 Matriz de Correlação:")
print("Variables: total_amount, passenger_count, trip_duration_minutes, pickup_hour")
print(correlation_array)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Otimizações e Performance

# COMMAND ----------

# Análise de particionamento e performance
print("⚡ ANÁLISE DE PERFORMANCE")
print("="*50)

# Verificar distribuição de dados
partition_analysis = df.groupBy("source_year", "source_month") \
    .agg(
        count("*").alias("record_count"),
        approx_count_distinct("VendorID").alias("unique_vendors"),
        (sum(col("total_amount")) / 1024 / 1024).alias("revenue_mb_equivalent")
    )

print("📊 Distribuição por partição:")
partition_analysis.show()

# Sugestões de otimização
print("\n💡 Sugestões de Otimização:")
print("1. Particionar por ano/mês para queries temporais")
print("2. Usar Z-ORDER por VendorID para queries por fornecedor") 
print("3. Aplicar VACUUM regularmente para limpeza")
print("4. Considerar bucketing por pickup_hour para análises horárias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Exportar Resultados para Visualização

# COMMAND ----------

# Converter resultados principais para Pandas para visualização
monthly_pandas = monthly_averages.toPandas()
hourly_pandas = may_hourly_patterns.toPandas()

# Salvar resultados processados
monthly_pandas.to_csv("/tmp/monthly_averages.csv", index=False)
hourly_pandas.to_csv("/tmp/hourly_patterns_may.csv", index=False)

print("💾 Resultados exportados:")
print("- /tmp/monthly_averages.csv")
print("- /tmp/hourly_patterns_may.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Análises PySpark Concluídas
# MAGIC 
# MAGIC ### Análises Realizadas:
# MAGIC 1. **Detecção de Outliers** com K-Means clustering
# MAGIC 2. **Window Functions** para análises temporais
# MAGIC 3. **Respostas às Perguntas** usando PySpark DataFrame API
# MAGIC 4. **Análise de Correlações** com PySpark ML
# MAGIC 5. **Otimizações de Performance** e sugestões
# MAGIC 
# MAGIC ### Vantagens do PySpark:
# MAGIC - **Escalabilidade**: Processa grandes volumes de dados
# MAGIC - **ML Integration**: Algoritmos de machine learning nativos
# MAGIC - **Window Functions**: Análises temporais complexas
# MAGIC - **Performance**: Otimizações automáticas (Catalyst, Tungsten)
# MAGIC 
# MAGIC ### Integração com dbt:
# MAGIC - PySpark para processamento pesado e ML
# MAGIC - dbt para transformações SQL e documentação
# MAGIC - Melhor dos dois mundos!
