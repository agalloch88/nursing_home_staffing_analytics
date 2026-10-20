-- sql/gold/m4_occupancy.sql: M4, occupancy beside the day's staffing, one row per facility-day.
select s.ccn, s.work_date, s.work_month as month,
       coalesce(f.facility_name, s.facility_name) as facility_name,
       coalesce(f.state, s.state)                 as state,
       f.ownership_type, f.ownership_group, f.provider_type,
       s.census, f.certified_beds,
       s.census / nullif(f.certified_beds, 0)     as occupancy_rate,
       s.census > f.certified_beds                as is_over_capacity,
       s.total_nurse_hours,
       s.census > 0 and not (${exclude_suspect_days} and (s.is_no_nurse_hours or s.is_above_24_hprd)) as counts_for_hprd
from staffing_daily s
left join facility f on f.ccn = s.ccn