-- Register the six bronze Delta tables in the Glue Data Catalog. No column list: Athena
-- reads the columns from each table's Delta log, so the location must already hold a _delta_log/.
CREATE EXTERNAL TABLE IF NOT EXISTS nh_bronze.pbj_daily_staffing
LOCATION 's3://${bucket}/bronze/pbj_daily_staffing/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_bronze.provider_info
LOCATION 's3://${bucket}/bronze/provider_info/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_bronze.state_us_averages
LOCATION 's3://${bucket}/bronze/state_us_averages/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_bronze.quality_claims
LOCATION 's3://${bucket}/bronze/quality_claims/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_bronze.snf_qrp
LOCATION 's3://${bucket}/bronze/snf_qrp/' TBLPROPERTIES ('table_type' = 'DELTA');
CREATE EXTERNAL TABLE IF NOT EXISTS nh_bronze.snf_vbp
LOCATION 's3://${bucket}/bronze/snf_vbp/' TBLPROPERTIES ('table_type' = 'DELTA');