-- macros/update_dim_customer.sql
{% macro update_dim_customer() %}

BEGIN
-- 1️⃣ Deactivate existing active customers
UPDATE DIM_CUSTOMER dim
SET 
    Active_Flag = 'N',
    Modified_At = CURRENT_TIMESTAMP()
WHERE dim.Active_Flag = 'Y'
  AND (dim.customer_account_number, dim.customer_email) IN (
        SELECT LEFT("CUSTOMER ACCOUNT NUMBER",100), emailaddress
        FROM KS_DEV.MCI_DATA.CSAT
        WHERE TO_DATE("date") > (SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='CSAT')
        UNION
        SELECT LEFT("ACCOUNT",100), email
        FROM KS_DEV.MCI_DATA.IVR
        WHERE TO_DATE("date") > (SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='IVR')
        UNION
        SELECT '' , customer_email
        FROM KS_DEV.MCI_DATA.SUBEASE
        WHERE TO_DATE("date") > (SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='SUBEASE')
);

-- 2️⃣ Insert new customers
INSERT INTO DIM_CUSTOMER (
    customer_id,
    brand_id,
    customer_account_number,
    customer_email,
    customer_segment,
    first_name,
    last_name,
    phone_number,
    gender,
    age,
    state,
    zipcode,
    membership_duration_days,
    is_auto_delivery,
    brand_code,
    Active_Flag,
    Created_At,
    Modified_At,
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
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'CSAT'
    FROM KS_DEV.MCI_DATA.CSAT c
    LEFT JOIN dim_brand b ON c.brand_code = b.brand_code
    WHERE TO_DATE(c."date") > (SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='CSAT')

    UNION ALL

    -- IVR
    SELECT
        '' AS customer_id,
        NULL AS brand_id,
        LEFT("ACCOUNT",100),
        email,
        '' AS customer_segment,
        "FIRST-NAME",
        "LAST-NAME",
        "PHONENUMBER-ENTERED",
        '' AS gender,
        '' AS age,
        '' AS state,
        '' AS zipcode,
        NULL AS membership_duration_days,
        0,
        brand,
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'IVR'
    FROM KS_DEV.MCI_DATA.IVR i
    WHERE TO_DATE(i."date") > (SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='IVR')

    UNION ALL

    -- SUBEASE
    SELECT
        '' AS customer_id,
        NULL AS brand_id,
        '' AS customer_account_number,
        customer_email,
        '' AS customer_segment,
        customer_first_name,
        customer_last_name,
        IsContact_Phone,
        '' AS gender,
        '' AS age,
        '' AS state,
        '' AS zipcode,
        NULL AS membership_duration_days,
        0,
        brand_code,
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'SUBEASE'
    FROM KS_DEV.MCI_DATA.SUBEASE s
    WHERE TO_DATE(s."date") > (SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='SUBEASE')
) src;

-- 3️⃣ Update CONTROL_TABLE
MERGE INTO CONTROL_TABLE ctrl
USING (
    SELECT 'CSAT' AS TABLE_NAME, MAX(TO_DATE(_ab_source_file_last_modified)) AS MAX_CREATIONDATE FROM KS_DEV.MCI_DATA.CSAT
    UNION ALL
    SELECT 'IVR', MAX(TO_DATE(_ab_source_file_last_modified)) FROM KS_DEV.MCI_DATA.IVR
    UNION ALL
    SELECT 'SUBEASE', MAX(TO_DATE(_ab_source_file_last_modified)) FROM KS_DEV.MCI_DATA.SUBEASE
) src
ON ctrl.TABLE_NAME = src.TABLE_NAME
WHEN MATCHED THEN
    UPDATE SET MAX_CREATIONDATE = src.MAX_CREATIONDATE,
               MODIFYDATE = CURRENT_TIMESTAMP();
END;
{% endmacro %}