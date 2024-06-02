{{ config(materialized='table') }}
SELECT
    DATE_TRUNC(v.data_venda, month) AS mes_venda,
    COUNT(v.venda_id) AS numero_vendas,
    SUM(v.valor_venda) AS total_vendas,
    AVG(v.valor_venda) AS valor_medio_venda
FROM {{ ref('fct_vendas') }} v
GROUP BY DATE_TRUNC(v.data_venda, month)
ORDER BY mes_venda
