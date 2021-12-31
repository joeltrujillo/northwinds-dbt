WITH 
hubspot_companies AS (
    SELECT * FROM {{ ref('stg_hubspot_companies') }}
),
rds_companies AS (
    SELECT * FROM {{ ref('stg_rds_companies') }}
),
merged_companies AS (
    SELECT null AS RDS_COMPANY_ID, COMPANY_ID AS HUBSPOT_COMPANY_ID, BUSINESS_NAME AS NAME FROM hubspot_companies
    UNION ALL
    SELECT company_id AS RDS_COMPANY_ID, null AS HUBSPOT_COMPANY_ID, company_name AS NAME FROM rds_companies
),
deduped AS (
    SELECT MAX(HUBSPOT_COMPANY_ID) AS HUBSPOT_COMPANY_ID,
    MAX(RDS_COMPANY_ID) AS RDS_COMPANY_ID,
    NAME
    FROM merged_companies
    GROUP BY
    NAME
    ORDER BY
    NAME
)
SELECT
HUBSPOT_COMPANY_ID,
RDS_COMPANY_ID,
NAME,
rds_companies.address AS ADDRESS,
rds_companies.city AS CITY, 
rds_companies.postal_code AS POSTAL_CODE,
rds_companies.country AS COUNTRY 
FROM deduped
JOIN stg_rds_companies AS rds_companies 
ON
rds_companies.company_id = deduped.RDS_COMPANY_ID
ORDER BY
NAME