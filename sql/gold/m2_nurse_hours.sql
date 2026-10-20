-- sql/gold/m2_nurse_hours.sql: M2, every nurse hour worked, by role and by employed or contract staff.
-- Grain: facility x month x role x employment type. Every reported hour counts, suspect days included.
with m as (
    select ccn, work_month,
           max(facility_name)                    as pbj_facility_name,
           max(state)                            as pbj_state,
           sum(census)                           as resident_days,
           sum(rn_hours - rn_contract_hours)     as rn_employed,   sum(rn_contract_hours)   as rn_contract,
           sum(lpn_hours - lpn_contract_hours)   as lpn_employed,  sum(lpn_contract_hours)  as lpn_contract,
           sum(aide_hours - aide_contract_hours) as aide_employed, sum(aide_contract_hours) as aide_contract
    from staffing_daily group by ccn, work_month),
long as (                                           -- six columns of hours become six rows
    select m.ccn, m.work_month, m.pbj_facility_name, m.pbj_state, m.resident_days,
           r.role, r.employment_type, r.hours
    from m
    lateral view stack(6,
        'RN',   'employed', rn_employed,   'RN',   'contract', rn_contract,
        'LPN',  'employed', lpn_employed,  'LPN',  'contract', lpn_contract,
        'Aide', 'employed', aide_employed, 'Aide', 'contract', aide_contract) r as role, employment_type, hours)
select l.ccn,
       l.work_month                                        as month,
       coalesce(f.facility_name, l.pbj_facility_name)      as facility_name,
       coalesce(f.state, l.pbj_state)                      as state,
       f.ownership_type, f.ownership_group, f.provider_type,
       l.role, l.employment_type,
       round(l.hours, 2)                                   as hours,
       l.resident_days
from long l left join facility f on f.ccn = l.ccn