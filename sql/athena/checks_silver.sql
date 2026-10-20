-- Phase 4d checkpoint: one row per key in every silver table
select 'staffing_daily' as silver_table, count(*) as row_count,
       count(distinct ccn || '|' || cast(work_date as varchar)) as distinct_keys from nh_silver.staffing_daily
union all select 'facility', count(*), count(distinct ccn) from nh_silver.facility
union all select 'state_benchmarks', count(*), count(distinct state) from nh_silver.state_benchmarks
union all select 'facility_quality', count(*), count(distinct ccn || '|' || source || '|' || measure_code) from nh_silver.facility_quality
order by 1;
-- The day flags per loaded month (the quarter totals match the Phase 2 findings)
select work_month, count(*) as facility_days, count_if(is_zero_census) as zero_census,
       count_if(is_no_nurse_hours) as residents_no_hours, count_if(is_above_24_hprd) as above_24_hprd,
       count_if(is_no_rn_hours) as residents_no_rn
from nh_silver.staffing_daily group by 1 order by 1;