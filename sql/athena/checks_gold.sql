-- Phase 4e checkpoint: rows per gold table
select 'm1_staffing_hprd' as gold_table, count(*) as row_count from nh_gold.m1_staffing_hprd
union all select 'm2_nurse_hours', count(*) from nh_gold.m2_nurse_hours
union all select 'm3_contract_share', count(*) from nh_gold.m3_contract_share
union all select 'm4_occupancy', count(*) from nh_gold.m4_occupancy
union all select 'm5_benchmarks_outcomes', count(*) from nh_gold.m5_benchmarks_outcomes
order by 1;
-- National HPRD and contract share by month: sums over sums, never an average of facility ratios
select m1.month, count(*) as facilities,
       round(sum(m1.total_nurse_hours) / sum(m1.resident_days), 2) as national_hprd,
       round(sum(m1.rn_hours) / sum(m1.resident_days), 2) as national_rn_hprd,
       round(100 * sum(m3.contract_hours) / sum(m3.total_nurse_hours), 1) as contract_pct
from nh_gold.m1_staffing_hprd m1 join nh_gold.m3_contract_share m3 on m3.ccn = m1.ccn and m3.month = m1.month
group by 1 order by 1;
-- Reconciliation with CMS's published HPRD: meaningful only once the whole quarter is loaded
select quarter, max(months_loaded) as months_loaded, count_if(cms_reported_hprd > 0) as comparable,
       count_if(abs(hprd_cms_rule / cms_reported_hprd - 1) <= 0.001) as within_0_1_pct
from nh_gold.m5_benchmarks_outcomes group by 1;