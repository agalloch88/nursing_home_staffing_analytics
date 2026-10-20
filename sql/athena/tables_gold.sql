-- Register the five gold Delta tables
CREATE EXTERNAL TABLE IF NOT EXISTS nh_gold.m1_staffing_hprd
LOCATION 's3://${bucket}/gold/m1_staffing_hprd/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_gold.m2_nurse_hours
LOCATION 's3://${bucket}/gold/m2_nurse_hours/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_gold.m3_contract_share
LOCATION 's3://${bucket}/gold/m3_contract_share/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_gold.m4_occupancy
LOCATION 's3://${bucket}/gold/m4_occupancy/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_gold.m5_benchmarks_outcomes
LOCATION 's3://${bucket}/gold/m5_benchmarks_outcomes/' TBLPROPERTIES ('table_type' = 'DELTA');