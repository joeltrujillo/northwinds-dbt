WITH source as (
    SELECT * FROM {{ source('RDS', 'CUSTOMERS')}}
),
renamed as (
    SELECT 
    CONCAT ('rds-', customer_id) as customer_id, country,
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name,
    CASE
        WHEN LEN(translate(phone,'(,), ,-,.', '')) = 10
        THEN CONCAT('(',SUBSTRING(translate(phone,'(,), ,-,.', ''),1,3),')',' ',SUBSTRING(translate(phone,'(,), ,-,.', ''),4,3),'-',SUBSTRING(translate(phone,'(,), ,-,.', ''),7,4))
    END
    as phone
    FROM source
)
select * FROM renamed