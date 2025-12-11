{% macro soft_delete_active_customers() %}

-- Soft delete existing active customers that appear in new incremental data
UPDATE {{ ref('dim_customer') }} AS dim
SET 
    Active_Flag = 'N',
    Modified_At = CURRENT_TIMESTAMP()
WHERE dim.Active_Flag = 'Y'
  AND (dim.customer_account_number, dim.customer_email) IN (

    -- CSAT
    SELECT LEFT("CUSTOMER ACCOUNT NUMBER",100), emailaddress
    FROM {{ source('mci_data','CSAT') }}
    WHERE TO_DATE(date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='CSAT'
    )

    UNION

    -- IVR
    SELECT LEFT("ACCOUNT",100), email
    FROM {{ source('mci_data','IVR') }}
    WHERE TO_DATE(date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='IVR'
    )

    UNION

    -- SUBEASE
    SELECT '', customer_email
    FROM {{ source('mci_data','SUBEASE') }}
    WHERE TO_DATE(date) > (
        SELECT MAX_CREATIONDATE 
        FROM {{ ref('control_table') }} 
        WHERE TABLE_NAME='SUBEASE'
    )
);

{% endmacro %}