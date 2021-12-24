WITH source as (
    SELECT * FROM {{ source('RDS', 'CUSTOMERS')}}
),
renamed as (
    SELECT 
    CONCAT('rds-', replace(lower(company_name), ' ','-')) as company_id,
    company_name,
    MAX(address) as address,
    MAX(city) as city,
    MAX(postal_code) as postal_code,
    MAX(country) as country
    FROM source
    GROUP BY company_name
)
select * FROM renamed