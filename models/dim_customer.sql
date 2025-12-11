{{ config(
    materialized='incremental',
    unique_key='customer_id',
    pre_hook="{{ macros.soft_delete_active_customers() }}"
) }}

WITH csat AS (
    SELECT
        "CUSTOMER ID" AS customer_id,
        b.brand_id,
        LEFT("CUSTOMER ACCOUNT NUMBER",100) AS customer_account_number,
        emailaddress AS customer_email,
        '' AS customer_segment,
        '' AS first_name,
        '' AS last_name,
        '' AS phone_number,
        '' AS gender,
        Age AS age,
        state,
        zipcode,
        "MEMBERSHIP DURATION" AS membership_duration_days,
        CASE WHEN "DO YOU HAVE AUTO DELIVERY" = 'Yes' THEN 1 ELSE 0 END AS is_auto_delivery,
        c.brand_code,
        1 AS Active_Flag,
        CURRENT_TIMESTAMP() AS Created_At,
        CURRENT_TIMESTAMP() AS Modified_At,
        'CSAT' AS DataSource
    FROM {{ source('mci_data','CSAT') }} c
    LEFT JOIN {{ ref('dim_brand') }} b ON c.brand_code = b.brand_code
    WHERE TO_DATE(c.date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='CSAT'
    )
),

ivr AS (
    SELECT
        '' AS customer_id,
        NULL AS brand_id,
        LEFT("ACCOUNT",100) AS customer_account_number,
        email AS customer_email,
        '' AS customer_segment,
        "FIRST-NAME" AS first_name,
        "LAST-NAME" AS last_name,
        "PHONENUMBER-ENTERED" AS phone_number,
        '' AS gender,
        NULL AS age,
        NULL AS state,
        NULL AS zipcode,
        NULL AS membership_duration_days,
        0 AS is_auto_delivery,
        brand AS brand_code,
        1 AS Active_Flag,
        CURRENT_TIMESTAMP() AS Created_At,
        CURRENT_TIMESTAMP() AS Modified_At,
        'IVR' AS DataSource
    FROM {{ source('mci_data','IVR') }}
    WHERE TO_DATE(date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='IVR'
    )
),

subease AS (
    SELECT
        '' AS customer_id,
        NULL AS brand_id,
        '' AS customer_account_number,
        customer_email,
        '' AS customer_segment,
        customer_first_name AS first_name,
        customer_last_name AS last_name,
        IsContact_Phone AS phone_number,
        '' AS gender,
        NULL AS age,
        NULL AS state,
        NULL AS zipcode,
        NULL AS membership_duration_days,
        0 AS is_auto_delivery,
        brand_code,
        1 AS Active_Flag,
        CURRENT_TIMESTAMP() AS Created_At,
        CURRENT_TIMESTAMP() AS Modified_At,
        'SUBEASE' AS DataSource
    FROM {{ source('mci_data','SUBEASE') }}
    WHERE TO_DATE(date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='SUBEASE'
    )
)

SELECT * FROM csat
UNION ALL
SELECT * FROM ivr
UNION ALL
SELECT * FROM subease