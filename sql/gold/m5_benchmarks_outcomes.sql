-- sql/gold/m5_benchmarks_outcomes.sql: M5, staffing against benchmarks and outcomes, facility x quarter.
-- hprd_cms_rule uses CMS's rule (every day with residents, suspect days kept), for the reconciliation
-- with CMS's published value; every other HPRD uses the days that count.
with d as (
    select s.*,
           concat(year(work_date), '-Q', quarter(work_date))                            as quarter,
           dayofweek(work_date) in (1, 7)                                                as is_weekend,   -- Sunday, Saturday
           census > 0 and not (${exclude_suspect_days} and (is_no_nurse_hours or is_above_24_hprd)) as counts
    from staffing_daily s),
fac as (
    select ccn, quarter,
           max(facility_name)                                                            as pbj_facility_name,
           max(state)                                                                    as pbj_state,
           count(distinct work_month)                                                    as months_loaded,
           sum(if(counts, census, 0))                                                    as resident_days,
           sum(if(counts, total_nurse_hours, 0))                                         as total_nurse_hours,
           sum(if(counts, total_nurse_hours, 0)) / nullif(sum(if(counts, census, 0)), 0) as hprd_total,
           sum(if(counts, rn_hours, 0)) / nullif(sum(if(counts, census, 0)), 0)          as hprd_rn,
           sum(if(counts and is_weekend, total_nurse_hours, 0))
             / nullif(sum(if(counts and is_weekend, census, 0)), 0)                      as hprd_weekend,
           sum(if(counts and not is_weekend, total_nurse_hours, 0))
             / nullif(sum(if(counts and not is_weekend, census, 0)), 0)                  as hprd_weekday,
           sum(if(census > 0, total_nurse_hours, 0)) / nullif(sum(census), 0)            as hprd_cms_rule,
           count_if(census > 0)                                                          as days_with_residents,
           count_if(is_no_rn_hours)                                                      as days_without_rn
    from d group by ccn, quarter),
q as (
    select ccn,
           max(if(measure_code = 'claims_521', score, null))                             as rehosp_short_stay_pct,
           max(if(measure_code = 'qrp_S_004_01_PPR_PD_RSRR', score, null))               as ppr_readmission_pct,
           max(if(measure_code = 'qrp_S_005_02_DTC_RS_RATE', score, null))               as discharge_to_community_pct,
           max(if(measure_code = 'vbp_rsrr', score, null))                               as vbp_readmission_rate
    from facility_quality group by ccn)
select fac.ccn, fac.quarter,
       coalesce(f.facility_name, fac.pbj_facility_name)                                  as facility_name,
       coalesce(f.state, fac.pbj_state)                                                  as state,
       f.ownership_type, f.ownership_group, f.provider_type,
       fac.months_loaded, fac.resident_days, fac.total_nurse_hours,
       fac.hprd_total, fac.hprd_rn, fac.hprd_weekend, fac.hprd_weekday,
       fac.hprd_weekday - fac.hprd_weekend                                               as weekend_gap,
       fac.hprd_cms_rule,
       f.reported_total_hprd                                                             as cms_reported_hprd,
       sb.avg_total_hprd                                                                 as state_avg_hprd,
       fac.hprd_total < sb.avg_total_hprd                                                as is_below_state_avg,
       fac.days_with_residents, fac.days_without_rn,
       fac.days_without_rn / nullif(fac.days_with_residents, 0)                          as rn_coverage_gap_share,
       q.rehosp_short_stay_pct, q.ppr_readmission_pct, q.discharge_to_community_pct, q.vbp_readmission_rate
from fac
left join facility f          on f.ccn = fac.ccn
left join state_benchmarks sb on sb.state = coalesce(f.state, fac.pbj_state)
left join q                   on q.ccn = fac.ccn