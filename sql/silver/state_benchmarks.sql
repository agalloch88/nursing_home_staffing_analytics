-- sql/silver/state_benchmarks.sql: CMS's own averages, one row per state plus NATION. They are
-- simple means of facility values, not weighted by residents (see the Phase 2 findings).
select state,
       cast(average_number_of_residents_per_day as double)                                   as avg_residents_per_day,
       cast(reported_total_nurse_staffing_hours_per_resident_per_day as double)              as avg_total_hprd,
       cast(reported_rn_staffing_hours_per_resident_per_day as double)                       as avg_rn_hprd,
       cast(reported_lpn_staffing_hours_per_resident_per_day as double)                      as avg_lpn_hprd,
       cast(reported_nurse_aide_staffing_hours_per_resident_per_day as double)               as avg_aide_hprd,
       cast(total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend as double) as avg_weekend_hprd,
       cast(total_nursing_staff_turnover as double)                                          as avg_nurse_turnover,
       cast(percentage_of_short_stay_residents_who_were_rehospitalized_after_a_nursing_home_admission as double)
                                                                                              as avg_rehosp_short_stay_pct,
       _source_file, _drive_file_id, _md5, _release, _ingested_at,
       sha2(concat_ws('|', average_number_of_residents_per_day,
                      reported_total_nurse_staffing_hours_per_resident_per_day,
                      reported_rn_staffing_hours_per_resident_per_day,
                      reported_lpn_staffing_hours_per_resident_per_day,
                      reported_nurse_aide_staffing_hours_per_resident_per_day,
                      total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend,
                      total_nursing_staff_turnover,
                      percentage_of_short_stay_residents_who_were_rehospitalized_after_a_nursing_home_admission), 256) as _row_hash
from batch