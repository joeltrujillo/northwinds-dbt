WITH contacts as (
    SELECT * FROM {{ source('HUBSPOT', 'CONTACTS')}}
),
companies as (
    SELECT * FROM {{ ref('stg_hubspot_companies')}}
),
renamed as (
    SELECT CONCAT('hubspot-', HUBSPOT_ID) as contact_ID, 
    first_name,
    last_name,
    CASE
        WHEN LEN(translate(phone,'(,), ,-,.', '')) = 10
        THEN CONCAT('(',SUBSTRING(translate(phone,'(,), ,-,.', ''),1,3),')',' ',SUBSTRING(translate(phone,'(,), ,-,.', ''),4,3),'-',SUBSTRING(translate(phone,'(,), ,-,.', ''),7,4))
    END
    as phone, 
    contacts.business_name,
    COMPANY_ID
    FROM contacts JOIN companies ON companies.business_name = contacts.business_name
   )
select * FROM renamed