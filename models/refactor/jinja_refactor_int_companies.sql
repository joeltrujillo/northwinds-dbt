{% set sources = ["refactor_stg_hubspot_companies", "refactor_stg_rds_companies"] %}

with merged_companies as (
    {% for source in sources %}
        select name, 
        {{ 'company_id' if 'hubspot' in source else 'null' }} as HUBSPOT_COMPANY_ID,
        {{ 'company_id' if 'rds' in source else 'null' }} as RDS_COMPANY_ID
        from {{ ref(source) }} 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}

 ),

 deduped as (
    select max(hubspot_company_id) as HUBSPOT_COMPANY_ID, 
    max(rds_company_id) as RDS_COMPANY_ID, 
    name 
    from merged_companies 
    group by name
 )

select HUBSPOT_COMPANY_ID, RDS_COMPANY_ID, deduped.name, address, postal_code, city, country
from deduped
join {{ ref('refactor_stg_rds_companies')}} as rds_companies on rds_companies.company_id = deduped.rds_company_id