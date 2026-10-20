-- sql/silver/staffing_daily.sql: one typed row per facility-day, CMS's role definitions, day flags.
-- Runs on each micro-batch of new bronze rows, registered as the view `batch`.
with t as (
    select ccn,
           to_date(work_date, 'yyyyMMdd')                                                    as work_date,
           date_format(to_date(work_date, 'yyyyMMdd'), 'yyyy-MM')                            as work_month,
           facility_name, state, county_name, county_fips,
           cast(census as int)                                                               as census,
           cast(hrs_rndon as double) + cast(hrs_rnadmin as double) + cast(hrs_rn as double)  as rn_hours,
           cast(hrs_lpnadmin as double) + cast(hrs_lpn as double)                            as lpn_hours,
           cast(hrs_cna as double) + cast(hrs_natrn as double) + cast(hrs_medaide as double) as aide_hours,
           cast(hrs_rndon_ctr as double) + cast(hrs_rnadmin_ctr as double)
             + cast(hrs_rn_ctr as double)                                                    as rn_contract_hours,
           cast(hrs_lpnadmin_ctr as double) + cast(hrs_lpn_ctr as double)                    as lpn_contract_hours,
           cast(hrs_cna_ctr as double) + cast(hrs_natrn_ctr as double)
             + cast(hrs_medaide_ctr as double)                                               as aide_contract_hours,
           _source_file, _drive_file_id, _md5, _release, _ingested_at
    from batch)
select t.*,
       rn_hours + lpn_hours + aide_hours                                    as total_nurse_hours,
       rn_contract_hours + lpn_contract_hours + aide_contract_hours         as contract_hours,
       census = 0                                                           as is_zero_census,
       census > 0 and rn_hours + lpn_hours + aide_hours = 0                 as is_no_nurse_hours,
       census > 0 and (rn_hours + lpn_hours + aide_hours) / census > 24     as is_above_24_hprd,
       census > 0 and rn_hours = 0                                          as is_no_rn_hours,
       sha2(concat_ws('|', facility_name, state, county_name, county_fips, census, rn_hours, lpn_hours,
                      aide_hours, rn_contract_hours, lpn_contract_hours, aide_contract_hours), 256) as _row_hash
from t