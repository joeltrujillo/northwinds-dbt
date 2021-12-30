WITH 
hubspot_contacts as (
    SELECT * FROM {{ ref('stg_hubspot_contacts') }}
),
rds_customers as (
    SELECT * FROM {{ ref('stg_rds_customers') }}
),
merged_contacts as (
    SELECT first_name, last_name, phone, customer_id AS rds_customer_id, null AS hubspot_contact_id FROM rds_customers 
    UNION ALL
    SELECT first_name, last_name, phone, null AS rds_customer_id, company_id AS hubspot_contact_id FROM hubspot_contacts
),
deduped AS (
    SELECT first_name, last_name, MAX(phone) AS phone, MAX(rds_customer_id) AS rds_customer_id,
    MAX(hubspot_contact_id) AS hubspot_contact_id
    FROM merged_contacts
    GROUP BY
    first_name, last_name
    ORDER BY
    first_name, last_name
)
SELECT * FROM deduped
ORDER BY
first_name, last_name



