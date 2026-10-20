-- sql/silver/fq_claims.sql: the four claims-based measures as long rows. fq_qrp.sql and fq_vbp.sql
-- return the same columns, so all three MERGE into silver.facility_quality.
select ccn,
       'claims'                                                            as source,
       concat('claims_', measure_code)                                     as measure_code,   -- claims_521: short-stay rehospitalization
       measure_description                                                 as measure_name,
       cast(adjusted_score as double)                                      as score,
       case when cast(adjusted_score as double) is null then nullif(trim(adjusted_score), '') end as score_label,
       nullif(trim(footnote_for_score), '')                                as footnote,
       concat(date_format(to_date(substr(measure_period, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd'), ' to ',
              date_format(to_date(substr(measure_period, 10, 8), 'yyyyMMdd'), 'yyyy-MM-dd')) as period,
       _source_file, _drive_file_id, _md5, _release, _ingested_at,
       sha2(concat_ws('|', measure_description, adjusted_score, footnote_for_score, measure_period), 256) as _row_hash
from batch