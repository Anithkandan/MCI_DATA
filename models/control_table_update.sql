{{ config(materialized='table') }}

SELECT 'CSAT' AS TABLE_NAME, MAX(TO_DATE(_ab_source_file_last_modified)) AS MAX_CREATIONDATE
FROM {{ source('mci_data','CSAT') }}

UNION ALL

SELECT 'IVR', MAX(TO_DATE(_ab_source_file_last_modified))
FROM {{ source('mci_data','IVR') }}

UNION ALL

SELECT 'SUBEASE', MAX(TO_DATE(_ab_source_file_last_modified))
FROM {{ source('mci_data','SUBEASE') }}