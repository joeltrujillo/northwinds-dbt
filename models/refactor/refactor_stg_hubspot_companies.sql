WITH source as (
    SELECT * FROM {{ source('HUBSPOT', 'CONTACTS')}}
),
renamed as (
    SELECT 
    CONCAT('hubspot-', translate(lower(business_name), ' ,','--')) as COMPANY_ID,
    business_name as NAME
    FROM source
    GROUP BY NAME
)
select * FROM renamed