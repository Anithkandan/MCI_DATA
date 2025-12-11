{{ config(
    materialized='incremental',
    unique_key='customer_account_number',
    post_hook="{{ soft_delete_active_customers() }}"
) }}

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
    DataSource
)
SELECT * FROM (
    -------------------------------------------------------------
    -- CSAT
    -------------------------------------------------------------
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
    FROM KS_DEV.MCI_DATA.CSAT c
    LEFT JOIN dim_brand b ON c.brand_code = b.brand_code
    WHERE TO_TIMESTAMP(c.date) > (
        SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='CSAT'
    )
 
    UNION ALL
 
    -------------------------------------------------------------
    -- IVR
    -------------------------------------------------------------
    SELECT
        '0' as Customer_ID,
         b.brand_id,
        LEFT("ACCOUNT",256) as ACCOUNT_NUMBER,
        email,
        '' CUSTOMER__SEGMENT,
        "FIRST-NAME" as FIRST_NAME,
        "LAST-NAME" as LAST_NAME,
        "PHONENUMBER-ENTERED" as PHONE_NO,
        '' as GENDER,
        '' as AGE,
        '' as STATE,
        '' as ZIPCODE,
        0 as MEMBERSHIP_DURATION ,
        0 as IS_AUTO_DELIVERY,
        brand as BRANDCODE,
        'IVR'
    FROM KS_DEV.MCI_DATA.IVR i
    LEFT JOIN dim_brand b ON i.brand = b.brand_code
    WHERE TO_TIMESTAMP(i.date) > (
        SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='IVR'
    )
 
    UNION ALL
 
    -------------------------------------------------------------
    -- SUBEASE
    -------------------------------------------------------------
    SELECT
        '0' as Customer_ID,
         b.brand_id,
        '' as ACCOUNT_NUMBER,
        customer_email,
        '' as CUSTOMER__SEGMENT,
        customer_first_name,
        customer_last_name,
        IsContact_Phone,
        '' as GENDER,
        '' as AGE,
        '' as STATE,
        '' as ZIPCODE,
        0 as MEMBERSHIP_DUR,
        0 as IS_AUTO_DELIVERY,
        s.brand_code,
        'SUBEASE'
    FROM KS_DEV.MCI_DATA.SUBEASE s
    LEFT JOIN dim_brand b ON s.brand_code = b.brand_code
    WHERE TO_TIMESTAMP(s.date) > (
        SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='SUBEASE'
    )
) src;
 
 