{% macro soft_delete_active_customers() %}

UPDATE DIM_CUSTOMER dim
SET 
    Active_Flag = 'N',
    Modified_At = CURRENT_TIMESTAMP()
WHERE dim.Active_Flag = 'Y'
  AND (dim.customer_account_number, dim.customer_email) IN (
 
        -------------------------------------------------------
        -- CSAT
        -------------------------------------------------------
        SELECT 
            LEFT("CUSTOMER ACCOUNT NUMBER",100),
            emailaddress
        FROM KS_DEV.MCI_DATA.CSAT c
        WHERE TO_TIMESTAMP(c.date) > (
            SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='CSAT'
        )
 
        UNION
 
        -------------------------------------------------------
        -- IVR
        -------------------------------------------------------
        SELECT 
            LEFT("ACCOUNT",100),
            email
        FROM KS_DEV.MCI_DATA.IVR i
        WHERE TO_TIMESTAMP(i.date) > (
            SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='IVR'
        )
 
        UNION
 
        -------------------------------------------------------
        -- SUBEASE
        -------------------------------------------------------
        SELECT 
            '',
            customer_email
        FROM KS_DEV.MCI_DATA.SUBEASE s
        WHERE TO_TIMESTAMP(s.date) > (
            SELECT MAX_CREATIONDATE FROM CONTROL_TABLE WHERE TABLE_NAME='SUBEASE'
        )
   );
{% endmacro %}