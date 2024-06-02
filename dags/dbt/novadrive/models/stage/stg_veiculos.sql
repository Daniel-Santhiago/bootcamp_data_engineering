{{ config(materialized='view') }}
WITH source AS (
    SELECT
        id_veiculos,
        nome,
        tipo,
        CAST(valor as FLOAT64) AS valor,
        COALESCE(data_atualizacao, CURRENT_TIMESTAMP()) AS data_atualizacao,
        data_inclusao
    FROM {{ source('sources', 'veiculos') }}
)

SELECT
    id_veiculos,
    nome,
    tipo,
    valor,
    data_atualizacao,
    data_inclusao
FROM source
