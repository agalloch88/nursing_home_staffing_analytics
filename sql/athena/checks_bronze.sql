-- Phase 4c checkpoint: rows and source files per bronze table
select 'pbj_daily_staffing' as bronze_table, count(*) as row_count, count(distinct _source_file) as files from nh_bronze.pbj_daily_staffing
union all select 'provider_info', count(*), count(distinct _source_file) from nh_bronze.provider_info
union all select 'state_us_averages', count(*), count(distinct _source_file) from nh_bronze.state_us_averages
union all select 'quality_claims', count(*), count(distinct _source_file) from nh_bronze.quality_claims
union all select 'snf_qrp', count(*), count(distinct _source_file) from nh_bronze.snf_qrp
union all select 'snf_vbp', count(*), count(distinct _source_file) from nh_bronze.snf_vbp
order by 1;
-- CCNs are text: leading zeros and letters survive
select count(distinct ccn) as facilities, count(distinct case when regexp_like(ccn, '[A-Z]') then ccn end) as ccns_with_a_letter,
       min(ccn) as lowest_ccn, min(work_date) as first_day, max(work_date) as last_day
from nh_bronze.pbj_daily_staffing;