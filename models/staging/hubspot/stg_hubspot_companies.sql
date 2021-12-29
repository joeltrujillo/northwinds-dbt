WITH source as (
    SELECT * FROM {{ source('HUBSPOT', 'CONTACTS')}}
),
renamed as (
    SELECT 
    CONCAT('hubspot-', translate(lower(business_name), ' ,','--')) as COMPANY_ID,
    business_name as BUSINESS_NAME
    FROM source
    GROUP BY BUSINESS_NAME
)
select * FROM renamed