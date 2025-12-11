{{ config(materialized='ephemeral') }}
BEGIN;

-- 1. UPDATE existing customers
UPDATE DIM_CUSTOMER dim
SET Active_Flag = 'N',
    Modified_At = CURRENT_TIMESTAMP()
WHERE dim.Active_Flag = 'Y'
  AND (dim.customer_account_number, dim.customer_email) IN (
    SELECT LEFT("CUSTOMER ACCOUNT NUMBER",100), emailaddress
    FROM {{ source('MCI_DATA', 'CSAT') }} c
    WHERE TO_TIMESTAMP(c.date) > (
        SELECT MAX_CREATIONDATE FROM {{ ref('CONTROL_TABLE') }} WHERE TABLE_NAME='CSAT'
    )
    UNION
    SELECT LEFT("ACCOUNT",100), email
    FROM {{ source('MCI_DATA', 'IVR') }} i
    WHERE TO_TIMESTAMP(i.date) > (
        SELECT MAX_CREATIONDATE FROM {{ ref('CONTROL_TABLE') }} WHERE TABLE_NAME='IVR'
    )
    UNION
    SELECT '', customer_email
    FROM {{ source('MCI_DATA', 'SUBEASE') }} s
    WHERE TO_TIMESTAMP(s.date) > (
        SELECT MAX_CREATIONDATE FROM {{ ref('CONTROL_TABLE') }} WHERE TABLE_NAME='SUBEASE'
    )
);

-- 2. INSERT new incremental records
INSERT INTO DIM_CUSTOMER (
    customer_id, brand_id, customer_account_number, customer_email,
    customer_segment, first_name, last_name, phone_number, gender, age,
    state, zipcode, membership_duration_days, is_auto_delivery, brand_code, DataSource
)
SELECT * FROM (
    SELECT
        "CUSTOMER ID",
        b.brand_id,
        LEFT("CUSTOMER ACCOUNT NUMBER",256),
        emailaddress,
        '' AS customer_segment,
        '' AS first_name,
        '' AS last_name,
        '' AS phone_number,
        '' AS gender,
        Age,
        state,
        zipcode,
        "MEMBERSHIP DURATION",
        CASE WHEN "DO YOU HAVE AUTO DELIVERY" = 'Yes' THEN 1 ELSE 0 END as is_auto_delivery,
        c.brand_code,
        'CSAT'
    FROM {{ source('MCI_DATA', 'CSAT') }} c
    LEFT JOIN {{ ref('DIM_BRAND') }} b ON c.brand_code = b.brand_code
    WHERE TO_TIMESTAMP(c.date) > (
        SELECT MAX_CREATIONDATE FROM {{ ref('CONTROL_TABLE') }} WHERE TABLE_NAME='CSAT'
    )
    -- Repeat UNION ALL for IVR and SUBEASE
);

-- 3. UPSERT CONTROL_TABLE
MERGE INTO {{ ref('CONTROL_TABLE') }} ctrl
USING (
    SELECT 'CSAT' AS TABLE_NAME, MAX(TO_TIMESTAMP(_ab_source_file_last_modified)) AS MAX_CREATIONDATE FROM {{ source('MCI_DATA', 'CSAT') }}
    UNION ALL
    SELECT 'IVR', MAX(TO_TIMESTAMP(_ab_source_file_last_modified)) FROM {{ source('MCI_DATA', 'IVR') }}
    UNION ALL
    SELECT 'SUBEASE', MAX(TO_TIMESTAMP(_ab_source_file_last_modified)) FROM {{ source('MCI_DATA', 'SUBEASE') }}
) src
ON ctrl.TABLE_NAME = src.TABLE_NAME
WHEN MATCHED THEN
    UPDATE SET MAX_CREATIONDATE = src.MAX_CREATIONDATE, MODIFYDATE = CURRENT_TIMESTAMP();

END;