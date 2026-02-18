-- Dimension table for taxi technology vendors
-- Small static dimension defining vendor codes and their company names

with trips as (
    select * from {{ ref('fct_trips') }}
),

vendors as (
    select distinct
        vendor_id,
       case
        when vendor_id = 1 then 'Creative Mobile Technologies'
        when vendor_id = 2 then 'VeriFone Inc.'
        when vendor_id = 4 then 'Unknown/Other'
        end as vendor_name
    from trips
)

select * from vendors