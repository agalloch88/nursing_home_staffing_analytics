# Google Drive source folders

Source folders for the ingest job (nh_ingest_gdrive).

| Folder | Path in My Drive | Folder ID | Contents |
| --- | --- | --- | --- |
| Parent | NH_Source | 1SBNrw02KaNqaTJIFtv0TK4tD_HGQuEAt | The two folders below; shared with the service account in Phase 4b |
| Staffing data | NH_Source/Nursing_Data | 1Akgf_5R-zjEHMieWyH2fEy6UEwwjaqiV | Monthly PBJ files, one per drop |
| Supporting files | NH_Source/Supporting_CSV | 1aaGcQrvbt23nimZ9k7SecGGLCaA47GDR | 20 supporting CSVs and NH_Data_Dictionary.pdf |

- Owner: ryankirsch88@gmail.com
- Service account: <email, added in Phase 4b> (Viewer on NH_Source)
- Upload conversion: off ("Convert uploads to Google Docs editor format" cleared)

cdk.json entry (Phase 4a):

    "driveFolderIds": ["1Akgf_5R-zjEHMieWyH2fEy6UEwwjaqiV", "1aaGcQrvbt23nimZ9k7SecGGLCaA47GDR"]

## Drop log

| Drop | Date | Folder | What was uploaded | Workflow run ID (Phase 4g) |
| --- | --- | --- | --- | --- |
| 1 | Oct 18, 2026 | Both | PBJ_Daily_Nurse_Staffing_2024_04.csv; 20 supporting CSVs; NH_Data_Dictionary.pdf | |
| 2 | | Nursing_Data | PBJ_Daily_Nurse_Staffing_2024_05.csv | |
| 3 | | Nursing_Data | PBJ_Daily_Nurse_Staffing_2024_04.csv again, as a new file | |
| 4 | | Supporting_CSV | NH_ProviderInfo_Oct2024.csv replaced through Manage versions | |
| 5 | | Nursing_Data | PBJ_Daily_Nurse_Staffing_2024_06.csv | |