{{ config(materialized='table') }}

SELECT *
FROM {{ source('mci_data','DIM_BRAND') }} 