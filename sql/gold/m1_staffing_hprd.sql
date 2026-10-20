-- sql/gold/m1_staffing_hprd.sql: M1, nurse staffing hours per resident day (HPRD), facility x month.
-- Keeps numerators and denominators, so any group (state, ownership, quarter) is sum(hours) / sum(resident_days).
-- "Days that count": days with residents, minus the suspect days when ${exclude_suspect_days} is true.
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
       count(*)                                                                       as days_reported,
       count_if(d.counts)                                                             as days_counted,
       sum(if(d.counts, d.census, 0))                                                 as resident_days,
       sum(if(d.counts, d.rn_hours, 0))                                               as rn_hours,
       sum(if(d.counts, d.lpn_hours, 0))                                              as lpn_hours,
       sum(if(d.counts, d.aide_hours, 0))                                             as aide_hours,
       sum(if(d.counts, d.total_nurse_hours, 0))                                      as total_nurse_hours,
       sum(if(d.counts, d.total_nurse_hours, 0)) / nullif(sum(if(d.counts, d.census, 0)), 0) as hprd_total,
       sum(if(d.counts, d.rn_hours, 0))   / nullif(sum(if(d.counts, d.census, 0)), 0)        as hprd_rn,
       sum(if(d.counts, d.lpn_hours, 0))  / nullif(sum(if(d.counts, d.census, 0)), 0)        as hprd_lpn,
       sum(if(d.counts, d.aide_hours, 0)) / nullif(sum(if(d.counts, d.census, 0)), 0)        as hprd_aide,
       -- residents per nurse on duty: resident days over nurse days (24 nurse hours = one nurse on duty all day)
       sum(if(d.counts, d.census, 0)) * 24 / nullif(sum(if(d.counts, d.total_nurse_hours, 0)), 0) as residents_per_nurse_on_duty
from d left join facility f on f.ccn = d.ccn        -- keeps the 17 facilities missing from provider info
group by d.ccn, d.work_month