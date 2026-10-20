-- sql/silver/fq_vbp.sql: value-based purchasing has one wide row per facility; stack() turns its
-- seven measures into long rows. vbp_rsrr is the FY 2022 risk-standardized readmission rate (a fraction).
select ccn,
       'vbp'                                                               as source,
       concat('vbp_', m.code)                                              as measure_code,
       m.name                                                              as measure_name,
       cast(m.value as double)                                             as score,
       case when cast(m.value as double) is null then nullif(trim(m.value), '') end as score_label,
       nullif(trim(m.footnote), '')                                        as footnote,
       m.period,
       _source_file, _drive_file_id, _md5, _release, _ingested_at,
       sha2(concat_ws('|', m.name, m.value, m.footnote, m.period), 256)    as _row_hash
from batch
lateral view stack(7,
    'ranking', 'SNF VBP program ranking',
        snf_vbp_program_ranking, footnote_snf_vbp_program_ranking, 'FY 2024',
    'baseline_rsrr', 'Risk-standardized readmission rate, baseline period',
        baseline_period_fy_2019_risk_standardized_readmission_rate,
        footnote_baseline_period_fy_2019_risk_standardized_readmission_rate, 'FY 2019',
    'rsrr', 'Risk-standardized readmission rate, performance period',
        performance_period_fy_2022_risk_standardized_readmission_rate,
        footnote_performance_period_fy_2022_risk_standardized_readmission_rate, 'FY 2022',
    'achievement_score', 'Achievement score',
        achievement_score, footnote_achievement_score, 'FY 2024',
    'improvement_score', 'Improvement score',
        improvement_score, footnote_improvement_score, 'FY 2024',
    'performance_score', 'Performance score',
        performance_score, footnote_performance_score, 'FY 2024',
    'incentive_multiplier', 'Incentive payment multiplier',
        incentive_payment_multiplier, footnote_incentive_payment_multiplier, 'FY 2024'
) m as code, name, value, footnote, period