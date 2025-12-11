-- -- macros/update_dim_customer.sql
-- {% macro update_dim_customer() %}

 
-- UPDATE DIM_CUSTOMER dim
-- SET 
--     Active_Flag = 'N',
--     Modified_At = CURRENT_TIMESTAMP()
-- WHERE dim.Active_Flag = 'Y'
--   AND (dim.customer_account_number, dim.customer_email) IN (
 
--         -------------------------------------------------------
--         -- CSAT
--         -------------------------------------------------------
--         SELECT 
--             LEFT("CUSTOMER ACCOUNT NUMBER",100),
--             emailaddress
--         FROM KS_DEV.MCI_DATA.CSAT c
--         WHERE TO_DATE(c.date) > (
--             SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='CSAT'
--         )
 
--         UNION
 
--         -------------------------------------------------------
--         -- IVR
--         -------------------------------------------------------
--         SELECT 
--             LEFT("ACCOUNT",100),
--             email
--         FROM KS_DEV.MCI_DATA.IVR i
--         WHERE TO_DATE(i.date) > (
--             SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='IVR'
--         )
 
--         UNION
 
--         -------------------------------------------------------
--         -- SUBEASE
--         -------------------------------------------------------
--         SELECT 
--             '',
--             customer_email
--         FROM KS_DEV.MCI_DATA.SUBEASE s
--         WHERE TO_DATE(s.date) > (
--             SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='SUBEASE'
--         )
--    );
 
 
-- -------------------------------------------------------------
-- -- 2. INSERT NEW INCREMENTAL CUSTOMER RECORDS
-- -------------------------------------------------------------
 
-- --select * from dim_customer;
-- INSERT INTO DIM_CUSTOMER (
--     customer_id,
--     brand_id,
--     customer_account_number,
--     customer_email,
--     customer_segment,
--     first_name,
--     last_name,
--     phone_number,
--     gender,
--     age,
--     state,
--     zipcode,
--     membership_duration_days,
--     is_auto_delivery,
--     brand_code,
--     Active_Flag,
--     Created_At,
--     Modified_At,
--     DataSource
-- )
-- SELECT * FROM (
--     -------------------------------------------------------------
--     -- CSAT
--     -------------------------------------------------------------
--     SELECT
--         "CUSTOMER ID",
--         b.brand_id,
--         LEFT("CUSTOMER ACCOUNT NUMBER",100),
--         emailaddress,
--         '' AS customer_segment,
--         '' AS first_name,
--         '' AS last_name,
--         '' AS phone_number,
--         '' AS gender,
--         Age,
--         state,
--         zipcode,
--         "MEMBERSHIP DURATION",
--         CASE WHEN "DO YOU HAVE AUTO DELIVERY" = 'Yes' THEN 1 ELSE 0 END,
--         c.brand_code,
--         1 AS Active_Flag,
--         CURRENT_TIMESTAMP(),
--         CURRENT_TIMESTAMP(),
--         'CSAT'
--     FROM KS_DEV.MCI_DATA.CSAT c
--     LEFT JOIN dim_brand b ON c.brand_code = b.brand_code
--     WHERE TO_DATE(c.date) > (
--         SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='CSAT'
--     )
 
--     UNION ALL
 
--     -------------------------------------------------------------
--     -- IVR
--     -------------------------------------------------------------
--     SELECT
--         '',
--         NULL,
--         LEFT("ACCOUNT",100),
--         email,
--         '',
--         "FIRST-NAME",
--         "LAST-NAME",
--         "PHONENUMBER-ENTERED",
--         '',
--         '',
--         '',
--         '',
--         NULL,
--         0,
--         brand,
--         1,
--         CURRENT_TIMESTAMP(),
--         CURRENT_TIMESTAMP(),
--         'IVR'
--     FROM KS_DEV.MCI_DATA.IVR i
--     WHERE TO_DATE(i.date) > (
--         SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='IVR'
--     )
 
--     UNION ALL
 
--     -------------------------------------------------------------
--     -- SUBEASE
--     -------------------------------------------------------------
--     SELECT
--         '',
--         NULL,
--         '',
--         customer_email,
--         '',
--         customer_first_name,
--         customer_last_name,
--         IsContact_Phone,
--         '',
--         '',
--         '',
--         '',
--         NULL,
--         0,
--         brand_code,
--         1,
--         CURRENT_TIMESTAMP(),
--         CURRENT_TIMESTAMP(),
--         'SUBEASE'
--     FROM KS_DEV.MCI_DATA.SUBEASE s
--     WHERE TO_DATE(s.date) > (
--         SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='SUBEASE'
--     )
-- ) src;
 
 
-- -------------------------------------------------------------
-- -- 3. UPSERT INTO CONTROL_TABLE
-- -------------------------------------------------------------
-- MERGE INTO CONTROL_TABLE ctrl
-- USING (
--     SELECT 'CSAT' AS TABLE_NAME, MAX(TO_DATE(_ab_source_file_last_modified)) AS MAX_CREATIONDATE FROM KS_DEV.MCI_DATA.CSAT
--     UNION ALL
--     SELECT 'IVR', MAX(TO_DATE(_ab_source_file_last_modified)) FROM KS_DEV.MCI_DATA.IVR
--     UNION ALL
--     SELECT 'SUBEASE', MAX(TO_DATE(_ab_source_file_last_modified)) FROM KS_DEV.MCI_DATA.SUBEASE
-- ) src
-- ON ctrl.TABLE_NAME = src.TABLE_NAME
-- WHEN MATCHED THEN
--     UPDATE 
--     SET MAX_CREATIONDATE = src.MAX_CREATIONDATE,
--         MODIFYDATE = CURRENT_TIMESTAMP();
 

-- {% endmacro %}




{% macro update_dim_customer() %}

-- 1. Deactivate existing active customers
UPDATE {{ ref('dim_customer') }} AS dim
SET 
    Active_Flag = 'N',
    Modified_At = CURRENT_TIMESTAMP()
WHERE dim.Active_Flag = 'Y'
  AND (dim.customer_account_number, dim.customer_email) IN (

    -- CSAT
    SELECT LEFT("CUSTOMER ACCOUNT NUMBER", 100), emailaddress
    FROM {{ source('MCI_DATA', 'CSAT') }} c
    WHERE TO_DATE(c.date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='CSAT'
    )

    UNION

    -- IVR
    SELECT LEFT("ACCOUNT", 100), email
    FROM {{ source('MCI_DATA', 'IVR') }} i
    WHERE TO_DATE(i.date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='IVR'
    )

    UNION

    -- SUBEASE
    SELECT '', customer_email
    FROM {{ source('MCI_DATA', 'SUBEASE') }} s
    WHERE TO_DATE(s.date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='SUBEASE'
    )
);

-- 2. Insert new incremental customer records
INSERT INTO {{ ref('dim_customer') }} (
    customer_id, brand_id, customer_account_number, customer_email,
    customer_segment, first_name, last_name, phone_number,
    gender, age, state, zipcode, membership_duration_days,
    is_auto_delivery, brand_code, Active_Flag, Created_At, Modified_At,
    DataSource
)
SELECT * FROM (

    -- CSAT
    SELECT
        "CUSTOMER ID",
        b.brand_id,
        LEFT("CUSTOMER ACCOUNT NUMBER",100),
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
        CASE WHEN "DO YOU HAVE AUTO DELIVERY" = 'Yes' THEN 1 ELSE 0 END,
        c.brand_code,
        1 AS Active_Flag,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'CSAT'
    FROM {{ source('MCI_DATA', 'CSAT') }} c
    LEFT JOIN {{ ref('dim_brand') }} b ON c.brand_code = b.brand_code
    WHERE TO_DATE(c.date) > (
        SELECT MAX_CREATIONDATE FROM {{ ref('control_table') }} WHERE TABLE_NAME='CSAT'
    )

    UNION ALL

    -- IVR
    SELECT
        '',
        NULL,
        LEFT("ACCOUNT",100),
        email,
        '',
        "FIRST-NAME",
        "LAST-NAME",
        "PHONENUMBER-ENTERED",
        '',
        '',
        '',
        '',
        NULL,
        0,
        brand,
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'IVR'
    FROM {{ source('MCI_DATA', 'IVR') }} i
    WHERE TO_DATE(i.date) > (
        SELECT MAX_CREATIONDATE FROM {{ ref('control_table') }} WHERE TABLE_NAME='IVR'
    )

    UNION ALL

    -- SUBEASE
    SELECT
        '',
        NULL,
        '',
        customer_email,
        '',
        customer_first_name,
        customer_last_name,
        IsContact_Phone,
        '',
        '',
        '',
        '',
        NULL,
        0,
        brand_code,
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'SUBEASE'
    FROM {{ source('MCI_DATA', 'SUBEASE') }} s
    WHERE TO_DATE(s.date) > (
        SELECT MAX_CREATIONDATE FROM {{ ref('control_table') }} WHERE TABLE_NAME='SUBEASE'
    )

) src;

-- 3. Merge into CONTROL_TABLE
MERGE INTO {{ ref('control_table') }} AS ctrl
USING (
    SELECT 'CSAT' AS TABLE_NAME, MAX(TO_DATE(_ab_source_file_last_modified)) AS MAX_CREATIONDATE FROM {{ source('MCI_DATA', 'CSAT') }}
    UNION ALL
    SELECT 'IVR', MAX(TO_DATE(_ab_source_file_last_modified)) FROM {{ source('MCI_DATA', 'IVR') }}
    UNION ALL
    SELECT 'SUBEASE', MAX(TO_DATE(_ab_source_file_last_modified)) FROM {{ source('MCI_DATA', 'SUBEASE') }}
) AS src
ON ctrl.TABLE_NAME = src.TABLE_NAME
WHEN MATCHED THEN
UPDATE SET MAX_CREATIONDATE = src.MAX_CREATIONDATE,
           MODIFYDATE = CURRENT_TIMESTAMP();

{% endmacro %}