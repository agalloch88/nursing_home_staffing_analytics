## 10. Findings

**Master file**

- **Encoding.** Windows-1252 with CRLF line endings; 455 rows at 5 facilities break a UTF-8 reader. All 20
  supporting files are UTF-8. *Phase 4c: bronze tries UTF-8, then Windows-1252.*
- **A complete, clean panel.** 1,325,324 rows = 14,564 facilities × 91 days. No duplicate facility-days, blanks
  or negatives, and employed + contract hours equal the total for every role on every row.
  *Phase 4d: `IsPrimaryKey "ccn" "work_date"` and non-negative rules.*
- **CMS's HPRD rule.** CMS leaves out hours worked on days with zero census. With that rule, our Q2 HPRD matches
  the published value within 0.1% for 14,414 of 14,415 facilities (165383 is 11% off). Counting every day
  instead pushes 6 more facilities, all with zero-census days that report hours, more than 1% off, up to 81%. *Phase 4e: the reconciliation warning on M5.*
- **Implausible days.** 75 facility-days at 17 facilities exceed 24 nurse hours per resident; facility 145446
  reports 13,946 LPN hours for 83 residents on May 25, 2024. CMS keeps such days: its published HPRD for 145446
  is 4.57, against 2.73 without that day. *Silver flags them; gold leaves them out by default.*
- **Gaps.** 320 facility-days at 12 facilities have zero census, 165 of them with hours. 2,522 facility-days at
  95 facilities have residents but no nurse hours. 10,817 facility-days at 1,670 facilities have residents but no
  RN hours of any kind. *M5's RN coverage gap.*
- **Coverage.** 17 master-file facilities are missing from provider info. 267 provider-info facilities have no
  staffing rows, 254 of them with footnote 6. Guam appears only in provider info, so the master file covers 52
  states and territories. *Left joins in gold keep both sets visible.*
- **Occupancy.** Average daily census matches provider info's residents per day (mean absolute difference 0.03 residents), but 161
  facilities exceed 100% of certified beds: 54 above 110%, the highest at 332%. *Flagged in M4, left out of
  occupancy charts.*
- **Weighting.** CMS's averages are simple means of facility HPRD (national 3.84; our facility mean 3.83).
  Weighted by resident days, national HPRD is 3.71. *Dashboard labels which one each chart shows.*
- **Early signals for Phase 5, not conclusions.** Weekend HPRD 3.25 against 3.90 on weekdays; contract staff work
  7.0% of nurse hours, and 5,750 facilities use none; the correlation between occupancy and HPRD is about −0.06.

**Supporting files**

- 261 provider-info CCNs contain a letter, so CCN is text everywhere.
- All 10 other facility-level files match provider info on CCN; the value-based purchasing file matches 10,688
  of 10,858; the swing-bed file matches none.
- Provider info's staffing period is April 1 to June 30, 2024, the same quarter as the master file.
- Blanks in provider info: reported total HPRD 386, nursing turnover 1,473, staffing rating 201, residents per
  day 48.
- Quality-program scores mix numbers with text such as "Not Available" (95,258 rows). *Silver keeps a
  `score_label` column.*