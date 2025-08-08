{{
  config(
    materialized='view',
    description='Exemplo de integração dbt com Delta Lake Silver layer'
  )
}}

-- Demonstração: dbt lendo dados da camada Silver do Delta Lake
select
    taxi_type,
    pickup_datetime,
    total_amount,
    passenger_count,
    time_period,

    -- Transformação adicional com dbt
    case
        when total_amount > 50 then 'Premium'
        when total_amount > 20 then 'Standard'
        else 'Economy'
    end as fare_tier,

    current_timestamp() as dbt_processed_at

from ifood_case.silver_taxi_clean
where quality_flag = 'valid'
limit 1000  -- Amostra para demonstração
