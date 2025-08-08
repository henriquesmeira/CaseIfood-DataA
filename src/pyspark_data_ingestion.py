# Databricks notebook source
# MAGIC %md
# MAGIC # Case iFood - Ingestão de Dados com PySpark + Delta Lake
# MAGIC
# MAGIC ## Objetivo
# MAGIC Implementar arquitetura **Lakehouse** com Delta Lake:
# MAGIC 1. Download dos dados de táxis de NY (**Yellow + Green**)
# MAGIC 2. Processamento e unificação de múltiplas fontes com PySpark
# MAGIC 3. Armazenamento em **Delta Lake** (Bronze, Silver, Gold)
# MAGIC 4. ACID transactions, time travel e performance otimizada
# MAGIC 5. Análise comparativa entre tipos de táxi

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração do Ambiente
# MAGIC
# MAGIC **IMPORTANTE**: Não precisa configurar credenciais!
# MAGIC - Databricks Community Edition usa autenticação web
# MAGIC - Delta Lake já está incluído e configurado
# MAGIC - PySpark já está disponível no ambiente

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import requests
import os
from datetime import datetime

# Configurações Delta Lake (já incluído no Databricks)
CATALOG_NAME = "ifood_case"
DATABASE_NAME = "ifood_case"

# Configurar Spark para Delta Lake (otimizações)
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

# Otimizações de performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

print("✓ Configuração PySpark + Delta Lake concluída")
print("✓ Sem credenciais necessárias - Databricks Community Edition")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criação do Database e Schemas

# COMMAND ----------

# Criar database para o projeto
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")

print(f"✓ Database '{DATABASE_NAME}' criado e selecionado")

# Verificar databases disponíveis
spark.sql("SHOW DATABASES").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Definição dos Dados e Arquitetura Lakehouse

# COMMAND ----------

# URLs dos dados de táxis de NY - Yellow e Green
data_sources = {
    # Yellow Taxi Data
    "yellow": {
        "2023-01": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "2023-02": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
        "2023-03": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
        "2023-04": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet",
        "2023-05": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet"
    },
    # Green Taxi Data
    "green": {
        "2023-01": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet",
        "2023-02": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-02.parquet",
        "2023-03": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-03.parquet",
        "2023-04": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-04.parquet",
        "2023-05": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-05.parquet"
    }
}

# Schema esperado dos dados
expected_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])

print("✓ Schema e fontes de dados definidos")

# Arquitetura Lakehouse com Delta Lake
lakehouse_layers = {
    "bronze": "Dados brutos (raw) - ingestão direta das fontes",
    "silver": "Dados limpos e validados - transformações iniciais",
    "gold": "Dados agregados e prontos para análise - marts de negócio"
}

print("\n🏗️ ARQUITETURA LAKEHOUSE:")
for layer, description in lakehouse_layers.items():
    print(f"📁 {layer.upper()}: {description}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função de Download e Processamento

# COMMAND ----------

def download_and_process_month(taxi_type, month_key, url):
    """
    Download e processamento de dados de um mês específico para Delta Lake
    Suporta Yellow e Green taxi
    """
    print(f"🔄 Processando {taxi_type.upper()} taxi - {month_key}...")

    try:
        # 1. Download do arquivo
        print(f"  → Baixando de {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # 2. Salvar temporariamente
        temp_path = f"/tmp/{taxi_type}_tripdata_{month_key}.parquet"
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # 3. Ler com PySpark
        print(f"  → Lendo com PySpark...")
        df = spark.read.parquet(f"file://{temp_path}")

        # 4. Padronizar colunas entre Yellow e Green taxi
        print(f"  → Padronizando schema...")
        df_standardized = standardize_taxi_schema(df, taxi_type)

        # 5. Adicionar metadados para Lakehouse
        year, month = month_key.split("-")
        df_with_metadata = df_standardized.withColumn("source_year", lit(int(year))) \
                            .withColumn("source_month", lit(int(month))) \
                            .withColumn("taxi_type", lit(taxi_type)) \
                            .withColumn("source_file", lit(f"{taxi_type}_tripdata_{month_key}.parquet")) \
                            .withColumn("ingestion_timestamp", current_timestamp()) \
                            .withColumn("data_quality_score", lit(1.0)) \
                            .withColumn("lakehouse_layer", lit("bronze"))

        # 6. Validações básicas com PySpark
        print(f"  → Aplicando validações...")

        # Colunas de validação dependem do tipo de taxi
        pickup_col = "tpep_pickup_datetime" if taxi_type == "yellow" else "lpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime" if taxi_type == "yellow" else "lpep_dropoff_datetime"

        df_validated = df_with_metadata.filter(
            (col("VendorID").isNotNull()) &
            (col("pickup_datetime").isNotNull()) &
            (col("dropoff_datetime").isNotNull()) &
            (col("total_amount").isNotNull()) &
            (col("pickup_datetime") <= col("dropoff_datetime"))
        )

        # 7. Salvar como Delta Table (Bronze Layer)
        table_name = f"bronze_{taxi_type}_taxi_{month_key.replace('-', '_')}"
        print(f"  → Salvando como Delta Table: {table_name}")

        df_validated.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .saveAsTable(f"{DATABASE_NAME}.{table_name}")

        # 8. Otimizar tabela Delta
        print(f"  → Otimizando Delta Table...")
        spark.sql(f"OPTIMIZE {DATABASE_NAME}.{table_name} ZORDER BY (VendorID, pickup_datetime)")

        # 9. Estatísticas
        total_records = df.count()
        valid_records = df_validated.count()

        print(f"  ✓ {taxi_type.upper()} {month_key}: {total_records:,} registros baixados, {valid_records:,} válidos")

        # 10. Limpar arquivo temporário
        os.remove(temp_path)

        return {
            "taxi_type": taxi_type,
            "month": month_key,
            "total_records": total_records,
            "valid_records": valid_records,
            "delta_table": table_name,
            "quality_score": valid_records / total_records if total_records > 0 else 0
        }

    except Exception as e:
        print(f"  ✗ Erro ao processar {taxi_type} {month_key}: {str(e)}")
        return None

def standardize_taxi_schema(df, taxi_type):
    """
    Padroniza o schema entre Yellow e Green taxi
    """
    if taxi_type == "yellow":
        # Yellow taxi já tem o schema padrão
        return df.withColumn("pickup_datetime", col("tpep_pickup_datetime")) \
                 .withColumn("dropoff_datetime", col("tpep_dropoff_datetime"))

    elif taxi_type == "green":
        # Green taxi usa lpep_ ao invés de tpep_
        return df.withColumn("pickup_datetime", col("lpep_pickup_datetime")) \
                 .withColumn("dropoff_datetime", col("lpep_dropoff_datetime")) \
                 .withColumn("tpep_pickup_datetime", col("lpep_pickup_datetime")) \
                 .withColumn("tpep_dropoff_datetime", col("lpep_dropoff_datetime"))

    else:
        raise ValueError(f"Tipo de taxi não suportado: {taxi_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execução da Ingestão (Yellow + Green Taxi)

# COMMAND ----------

# Processar todos os tipos de taxi e meses
results = []
total_files = sum(len(months) for months in data_sources.values())
processed_files = 0

print(f"🚀 Iniciando ingestão de {total_files} arquivos (Yellow + Green taxi)...")

for taxi_type, months_data in data_sources.items():
    print(f"\n📋 Processando {taxi_type.upper()} TAXI:")
    for month_key, url in months_data.items():
        result = download_and_process_month(taxi_type, month_key, url)
        if result:
            results.append(result)
        processed_files += 1
        print(f"   Progresso: {processed_files}/{total_files} arquivos")

# Resumo da ingestão
print("\n" + "="*60)
print("📊 RESUMO DA INGESTÃO MULTI-FONTE")
print("="*60)

# Estatísticas por tipo de taxi
yellow_results = [r for r in results if r["taxi_type"] == "yellow"]
green_results = [r for r in results if r["taxi_type"] == "green"]

print("🟡 YELLOW TAXI:")
if yellow_results:
    yellow_total = sum(r["total_records"] for r in yellow_results)
    yellow_valid = sum(r["valid_records"] for r in yellow_results)
    print(f"   Meses: {len(yellow_results)}")
    print(f"   Registros: {yellow_total:,}")
    print(f"   Válidos: {yellow_valid:,}")
    print(f"   Qualidade: {(yellow_valid/yellow_total)*100:.2f}%")

print("\n🟢 GREEN TAXI:")
if green_results:
    green_total = sum(r["total_records"] for r in green_results)
    green_valid = sum(r["valid_records"] for r in green_results)
    print(f"   Meses: {len(green_results)}")
    print(f"   Registros: {green_total:,}")
    print(f"   Válidos: {green_valid:,}")
    print(f"   Qualidade: {(green_valid/green_total)*100:.2f}%")

print("\n📈 TOTAL GERAL:")
total_downloaded = sum(r["total_records"] for r in results)
total_valid = sum(r["valid_records"] for r in results)
print(f"   Arquivos processados: {len(results)}")
print(f"   Total de registros: {total_downloaded:,}")
print(f"   Total válidos: {total_valid:,}")
print(f"   Taxa de qualidade geral: {(total_valid/total_downloaded)*100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Criação de Tabela Bronze Unificada (Yellow + Green)

# COMMAND ----------

# Criar tabela Bronze unificada com todos os tipos e meses
print("🔄 Criando tabela Bronze unificada (Yellow + Green)...")

# Listar todas as tabelas bronze criadas
bronze_tables = []
for taxi_type in data_sources.keys():
    for month_key in data_sources[taxi_type].keys():
        table_name = f"bronze_{taxi_type}_taxi_{month_key.replace('-', '_')}"
        bronze_tables.append(table_name)

print(f"📋 Unindo {len(bronze_tables)} tabelas bronze:")
for table in bronze_tables:
    print(f"   - {table}")

# União de todas as tabelas bronze
union_query = " UNION ALL ".join([f"SELECT * FROM {DATABASE_NAME}.{table}" for table in bronze_tables])

# Executar união e criar tabela bronze unificada
print("🔄 Executando união das tabelas...")
unified_bronze_df = spark.sql(f"SELECT * FROM ({union_query})")

# Adicionar colunas calculadas para Silver layer
processed_df = unified_bronze_df.withColumn(
    "trip_duration_minutes",
    round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
).withColumn(
    "pickup_hour",
    hour("tpep_pickup_datetime")
).withColumn(
    "pickup_dayofweek",
    dayofweek("tpep_pickup_datetime")
).withColumn(
    "pickup_date",
    to_date("tpep_pickup_datetime")
).withColumn(
    "trip_id",
    concat(
        col("VendorID"),
        lit("_"),
        date_format(col("tpep_pickup_datetime"), "yyyyMMddHHmmss"),
        lit("_"),
        monotonically_increasing_id()
    )
)

# Salvar como tabela Bronze unificada
print("💾 Salvando tabela Bronze unificada...")
processed_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .saveAsTable(f"{DATABASE_NAME}.bronze_taxi_unified")

# Otimizar tabela unificada
print("⚡ Otimizando tabela Bronze...")
spark.sql(f"OPTIMIZE {DATABASE_NAME}.bronze_taxi_unified ZORDER BY (source_year, source_month, VendorID)")

print("✓ Tabela Bronze unificada criada com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criação da Camada Silver (Dados Limpos)

# COMMAND ----------

# Criar tabela Silver com dados limpos e validados (Yellow + Green)
print("🥈 Criando camada Silver unificada...")

silver_df = spark.sql(f"""
SELECT
    trip_id,
    taxi_type,
    VendorID as vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    fare_amount,
    tip_amount,
    tolls_amount,
    source_year,
    source_month,
    trip_duration_minutes,
    pickup_hour,
    pickup_dayofweek,
    pickup_date,

    -- Flags de qualidade
    CASE
        WHEN total_amount <= 0 THEN 'invalid_amount'
        WHEN passenger_count <= 0 THEN 'invalid_passengers'
        WHEN trip_duration_minutes <= 0 THEN 'invalid_duration'
        WHEN trip_duration_minutes > 1440 THEN 'excessive_duration'
        ELSE 'valid'
    END as quality_flag,

    -- Categorias de negócio
    CASE
        WHEN pickup_hour BETWEEN 6 AND 9 THEN 'morning_rush'
        WHEN pickup_hour BETWEEN 10 AND 16 THEN 'midday'
        WHEN pickup_hour BETWEEN 17 AND 20 THEN 'evening_rush'
        WHEN pickup_hour BETWEEN 21 AND 23 THEN 'night'
        ELSE 'late_night'
    END as time_period,

    -- Categorização por tipo de taxi
    CASE
        WHEN taxi_type = 'yellow' THEN 'Manhattan/Airport'
        WHEN taxi_type = 'green' THEN 'Outer Boroughs'
        ELSE 'Unknown'
    END as service_area,

    current_timestamp() as silver_processed_at

FROM {DATABASE_NAME}.bronze_taxi_unified
WHERE
    total_amount > 0
    AND passenger_count > 0
    AND passenger_count <= 6
    AND trip_duration_minutes > 0
    AND trip_duration_minutes <= 1440
    AND total_amount <= 1000
""")

# Salvar como tabela Silver
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DATABASE_NAME}.silver_yellow_taxi_clean")

# Otimizar Silver
spark.sql(f"OPTIMIZE {DATABASE_NAME}.silver_yellow_taxi_clean ZORDER BY (pickup_date, vendor_id)")

print("✓ Camada Silver criada com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Criação da Camada Gold (Marts de Negócio Multi-Fonte)

# COMMAND ----------

# Criar tabela Gold - Agregações mensais (Pergunta 1) com comparação Yellow vs Green
print("🥇 Criando camada Gold - Marts de negócio...")

# Mart 1: Médias mensais por tipo de taxi
monthly_averages_df = spark.sql(f"""
SELECT
    taxi_type,
    source_year as year,
    source_month as month,
    CASE source_month
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
    END as month_name,
    service_area,

    COUNT(*) as total_trips,
    COUNT(DISTINCT vendor_id) as unique_vendors,
    COUNT(DISTINCT pickup_date) as active_days,

    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_amount_per_trip,
    ROUND(MIN(total_amount), 2) as min_amount,
    ROUND(MAX(total_amount), 2) as max_amount,

    ROUND(AVG(passenger_count), 2) as avg_passengers_per_trip,
    ROUND(AVG(trip_duration_minutes), 2) as avg_trip_duration_minutes,
    ROUND(AVG(trip_distance), 2) as avg_trip_distance,

    current_timestamp() as gold_processed_at

FROM {DATABASE_NAME}.silver_taxi_clean
GROUP BY taxi_type, source_year, source_month, service_area
ORDER BY taxi_type, source_year, source_month
""")

monthly_averages_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{DATABASE_NAME}.gold_monthly_averages")

# Mart 2: Padrões horários de maio por tipo de taxi (Pergunta 2)
may_hourly_df = spark.sql(f"""
SELECT
    taxi_type,
    pickup_hour,
    CONCAT(LPAD(pickup_hour, 2, '0'), ':00 - ', LPAD(pickup_hour + 1, 2, '0'), ':00') as hour_range,

    COUNT(*) as total_trips,
    ROUND(AVG(passenger_count), 3) as avg_passengers_per_trip,
    SUM(passenger_count) as total_passengers,
    MIN(passenger_count) as min_passengers,
    MAX(passenger_count) as max_passengers,

    ROUND(AVG(total_amount), 2) as avg_amount_per_trip,
    SUM(total_amount) as total_revenue,
    ROUND(AVG(trip_distance), 2) as avg_trip_distance,

    time_period,
    service_area,
    current_timestamp() as gold_processed_at

FROM {DATABASE_NAME}.silver_taxi_clean
WHERE source_month = 5
GROUP BY taxi_type, pickup_hour, time_period, service_area
ORDER BY taxi_type, pickup_hour
""")

may_hourly_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{DATABASE_NAME}.gold_may_hourly_patterns")

# Mart 3: Comparação Yellow vs Green (Análise adicional)
comparison_df = spark.sql(f"""
SELECT
    taxi_type,
    service_area,

    -- Métricas gerais
    COUNT(*) as total_trips,
    ROUND(AVG(total_amount), 2) as avg_fare,
    ROUND(AVG(passenger_count), 2) as avg_passengers,
    ROUND(AVG(trip_distance), 2) as avg_distance,
    ROUND(AVG(trip_duration_minutes), 2) as avg_duration,

    -- Métricas de receita
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(tip_amount), 2) as avg_tip,
    ROUND(AVG(tip_amount / NULLIF(total_amount, 0)) * 100, 2) as avg_tip_percentage,

    -- Distribuição temporal
    COUNT(CASE WHEN time_period = 'morning_rush' THEN 1 END) as morning_rush_trips,
    COUNT(CASE WHEN time_period = 'evening_rush' THEN 1 END) as evening_rush_trips,

    current_timestamp() as gold_processed_at

FROM {DATABASE_NAME}.silver_taxi_clean
GROUP BY taxi_type, service_area
ORDER BY taxi_type
""")

comparison_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{DATABASE_NAME}.gold_taxi_comparison")

print("✓ Camada Gold criada com sucesso!")
print("📊 Tabelas Gold criadas:")
print("   - gold_monthly_averages (Yellow vs Green por mês)")
print("   - gold_may_hourly_patterns (Padrões horários por tipo)")
print("   - gold_taxi_comparison (Comparação geral Yellow vs Green)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Análise Exploratória com Delta Lake

# COMMAND ----------

# Análise das camadas do Lakehouse
print("📊 ANÁLISE DO LAKEHOUSE DELTA")
print("="*50)

# 1. Verificar tabelas criadas
print("📋 Tabelas Delta criadas:")
spark.sql(f"SHOW TABLES IN {DATABASE_NAME}").show()

# 2. Estatísticas das camadas
print("\n🏗️ Estatísticas por camada:")

# Bronze layer
bronze_stats = spark.sql(f"""
SELECT
    'Bronze' as layer,
    COUNT(*) as total_records,
    COUNT(DISTINCT source_month) as months,
    MIN(ingestion_timestamp) as first_ingestion,
    MAX(ingestion_timestamp) as last_ingestion
FROM {DATABASE_NAME}.bronze_taxi_unified
""")
bronze_stats.show()

# Silver layer
silver_stats = spark.sql(f"""
SELECT
    'Silver' as layer,
    COUNT(*) as total_records,
    COUNT(DISTINCT quality_flag) as quality_flags,
    SUM(CASE WHEN quality_flag = 'valid' THEN 1 ELSE 0 END) as valid_records
FROM {DATABASE_NAME}.silver_yellow_taxi_clean
""")
silver_stats.show()

# Gold layer
gold_monthly_stats = spark.sql(f"""
SELECT
    'Gold-Monthly' as layer,
    COUNT(*) as total_months,
    SUM(total_trips) as total_trips_aggregated,
    ROUND(AVG(avg_amount_per_trip), 2) as overall_avg_amount
FROM {DATABASE_NAME}.gold_monthly_averages
""")
gold_monthly_stats.show()

# 3. Demonstrar Time Travel (Delta Lake feature)
print("\n⏰ DEMONSTRAÇÃO TIME TRAVEL:")
print("Versões da tabela Bronze:")
spark.sql(f"DESCRIBE HISTORY {DATABASE_NAME}.bronze_taxi_unified").select("version", "timestamp", "operation").show()

# 4. Respostas às perguntas de negócio (Yellow + Green)
print("\n💰 PERGUNTA 1 - Média de valor total por mês (Yellow vs Green):")
spark.sql(f"""
SELECT taxi_type, month_name, avg_amount_per_trip, total_trips
FROM {DATABASE_NAME}.gold_monthly_averages
ORDER BY month, taxi_type
""").show()

print("\n👥 PERGUNTA 2 - Média de passageiros por hora em maio (Yellow vs Green):")
spark.sql(f"""
SELECT taxi_type, hour_range, avg_passengers_per_trip, total_trips, time_period
FROM {DATABASE_NAME}.gold_may_hourly_patterns
ORDER BY taxi_type, pickup_hour
""").show()

print("\n🔍 ANÁLISE COMPARATIVA - Yellow vs Green Taxi:")
spark.sql(f"""
SELECT
    taxi_type,
    service_area,
    total_trips,
    avg_fare,
    avg_passengers,
    avg_distance,
    avg_tip_percentage
FROM {DATABASE_NAME}.gold_taxi_comparison
ORDER BY taxi_type
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Lakehouse Multi-Fonte Implementado com Sucesso!
# MAGIC
# MAGIC ### 🏗️ Arquitetura Lakehouse Multi-Fonte Criada:
# MAGIC 1. **Bronze Layer**: Dados brutos Yellow + Green taxi com metadados
# MAGIC 2. **Silver Layer**: Dados unificados e padronizados entre fontes
# MAGIC 3. **Gold Layer**: Marts comparativos e análises de negócio
# MAGIC
# MAGIC ### 🚀 Recursos Delta Lake Implementados:
# MAGIC - ✅ **ACID Transactions**: Consistência garantida
# MAGIC - ✅ **Time Travel**: Versionamento automático
# MAGIC - ✅ **Schema Evolution**: Flexibilidade de mudanças
# MAGIC - ✅ **Auto Optimization**: Z-ORDER e compactação
# MAGIC - ✅ **Multi-Source Integration**: Yellow + Green unificados
# MAGIC
# MAGIC ### 📊 Análises Implementadas:
# MAGIC 1. **Pergunta 1**: Médias mensais comparativas (Yellow vs Green)
# MAGIC 2. **Pergunta 2**: Padrões horários por tipo de taxi
# MAGIC 3. **Análise Extra**: Comparação geral de performance
# MAGIC
# MAGIC ### 📈 Tabelas Gold Criadas:
# MAGIC - `gold_monthly_averages`: Comparação mensal Yellow vs Green
# MAGIC - `gold_may_hourly_patterns`: Padrões horários detalhados
# MAGIC - `gold_taxi_comparison`: Métricas comparativas gerais
# MAGIC
# MAGIC ### 🎯 Insights Obtidos:
# MAGIC - **Yellow Taxi**: Foco em Manhattan/Aeroportos
# MAGIC - **Green Taxi**: Atende Outer Boroughs
# MAGIC - **Comparação**: Diferentes padrões de uso e receita
# MAGIC - **Sazonalidade**: Variações mensais por tipo
# MAGIC
# MAGIC ### 💡 Vantagens da Implementação:
# MAGIC - **Multi-Source**: Integração de múltiplas fontes
# MAGIC - **Padronização**: Schema unificado entre Yellow/Green
# MAGIC - **Performance**: Otimizações automáticas Delta Lake
# MAGIC - **Escalabilidade**: Pronto para adicionar mais fontes (FHV, etc.)
# MAGIC - **Análises Ricas**: Comparações e insights de negócio
