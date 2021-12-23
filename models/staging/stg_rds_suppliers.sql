WITH source as (
    SELECT * FROM "PAGILA_INC"."NORTHWINDS_RDS_PUBLIC"."SUPPLIERS"
),
renamed as (
    SELECT supplier_id, company_name,
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name,
    contact_title,
    CASE
        WHEN LEN(translate(phone,'(,), ,-,.', '')) = 10
        THEN CONCAT('(',SUBSTRING(translate(phone,'(,), ,-,.', ''),1,3),')',' ',SUBSTRING(translate(phone,'(,), ,-,.', ''),4,3),'-',SUBSTRING(translate(phone,'(,), ,-,.', ''),7,4))
    END
    as phone, address, city, 
    postal_code, region,
    fax, homepage,
    _FIVETRAN_DELETED
    FROM source
   )
select * FROM renamed