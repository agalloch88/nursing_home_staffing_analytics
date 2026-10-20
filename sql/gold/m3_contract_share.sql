-- sql/gold/m3_contract_share.sql: M3, contract staff's share of nurse hours, facility x month, on days that count.
with d as (
    select s.*,
           census > 0 and not (${exclude_suspect_days} and (is_no_nurse_hours or is_above_24_hprd)) as counts
    from staffing_daily s)
select d.ccn,
       d.work_month                                                                   as month,
       coalesce(max(f.facility_name), max(d.facility_name))                           as facility_name,
       coalesce(max(f.state), max(d.state))                                           as state,
       max(f.ownership_type)                                                          as ownership_type,
       max(f.ownership_group)                                                         as ownership_group,
       max(f.provider_type)                                                           as provider_type,
       sum(if(d.counts, d.census, 0))                                                 as resident_days,
       sum(if(d.counts, d.total_nurse_hours, 0))                                      as total_nurse_hours,
       sum(if(d.counts, d.contract_hours, 0))                                         as contract_hours,
       sum(if(d.counts, d.rn_contract_hours, 0))                                      as rn_contract_hours,
       sum(if(d.counts, d.lpn_contract_hours, 0))                                     as lpn_contract_hours,
       sum(if(d.counts, d.aide_contract_hours, 0))                                    as aide_contract_hours,
       sum(if(d.counts, d.contract_hours, 0)) / nullif(sum(if(d.counts, d.total_nurse_hours, 0)), 0) as contract_share,
       sum(if(d.counts, d.rn_contract_hours, 0)) / nullif(sum(if(d.counts, d.rn_hours, 0)), 0)       as rn_contract_share,
       sum(if(d.counts, d.lpn_contract_hours, 0)) / nullif(sum(if(d.counts, d.lpn_hours, 0)), 0)     as lpn_contract_share,
       sum(if(d.counts, d.aide_contract_hours, 0)) / nullif(sum(if(d.counts, d.aide_hours, 0)), 0)   as aide_contract_share
from d left join facility f on f.ccn = d.ccn
group by d.ccn, d.work_month