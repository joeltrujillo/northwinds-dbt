{% set sources = ["refactor_stg_hubspot_contacts", "refactor_stg_rds_customers"] %}

with merged_contacts as (
    {% for source in sources %}
        select 
        {{ 'contact_ID' if 'hubspot' in source else 'null' }} as HUBSPOT_CONTACT_ID,
        {{ 'customer_id' if 'rds' in source else 'null' }} as RDS_CONTACT_ID,
        first_name as FIRST_NAME,
        last_name as LAST_NAME,
        {{ 'PHONE' if 'hubspot' in source else 'null' }} as PHONE,
        {{ 'COMPANY_ID' if 'hubspot' in source else 'null' }} as HUBSPOT_COMPANY_ID,
        {{ 'BUSINESS_NAME' if 'hubspot' in source else 'null' }} as BUSINESS_NAME
        from {{ ref(source) }} 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
 ),
deduped as (
    select 
    max(HUBSPOT_CONTACT_ID) as HUBSPOT_CONTACT_ID, 
    max(RDS_CONTACT_ID) as RDS_CONTACT_ID, 
    FIRST_NAME,
    LAST_NAME,
    max(PHONE) as PHONE,
    max(HUBSPOT_COMPANY_ID) as HUBSPOT_COMPANY_ID,
    max(BUSINESS_NAME) as BUSINESS_NAME
    from merged_contacts 
    GROUP BY
    FIRST_NAME, LAST_NAME
    ORDER BY
    FIRST_NAME, LAST_NAME
 )

select HUBSPOT_CONTACT_ID, RDS_CONTACT_ID, FIRST_NAME, LAST_NAME, PHONE, HUBSPOT_COMPANY_ID, COMPANY_ID as RDS_COMPANY_ID
from deduped
join {{ ref('refactor_stg_rds_companies')}} as rds_companies on rds_companies.NAME = deduped.BUSINESS_NAME


