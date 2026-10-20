-- sql/silver/fq_qrp.sql: the SNF Quality Reporting Program, already one row per facility and measure.
-- qrp_S_004_01_PPR_PD_RSRR is the risk-standardized potentially preventable readmission rate and
-- qrp_S_005_02_DTC_RS_RATE the risk-standardized discharge-to-community rate.
select ccn,
       'qrp'                                                               as source,
       concat('qrp_', measure_code)                                        as measure_code,
       cast(null as string)                                                as measure_name,   -- the file has codes only
       cast(score as double)                                               as score,
       case when cast(score as double) is null then nullif(trim(score), '') end as score_label,  -- e.g. "Not Available"
       nullif(nullif(trim(footnote), '-'), '')                             as footnote,
       concat(date_format(to_date(start_date, 'MM/dd/yyyy'), 'yyyy-MM-dd'), ' to ',
              date_format(to_date(end_date, 'MM/dd/yyyy'), 'yyyy-MM-dd'))  as period,
       _source_file, _drive_file_id, _md5, _release, _ingested_at,
       sha2(concat_ws('|', score, footnote, start_date, end_date), 256)    as _row_hash
from batch