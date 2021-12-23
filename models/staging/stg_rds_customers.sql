WITH source as (
    SELECT * FROM "PAGILA_INC"."NORTHWINDS_RDS_PUBLIC"."CUSTOMERS"
),
renamed as (
    SELECT customer_id, country,
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name
    FROM source
)
select * FROM renamed