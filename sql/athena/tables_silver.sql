-- Register the four silver Delta tables
CREATE EXTERNAL TABLE IF NOT EXISTS nh_silver.staffing_daily
LOCATION 's3://${bucket}/silver/staffing_daily/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_silver.facility
LOCATION 's3://${bucket}/silver/facility/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_silver.state_benchmarks
LOCATION 's3://${bucket}/silver/state_benchmarks/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_silver.facility_quality
LOCATION 's3://${bucket}/silver/facility_quality/' TBLPROPERTIES ('table_type' = 'DELTA');