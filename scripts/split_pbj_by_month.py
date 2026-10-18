"""Split the quarterly PBJ master file into one file per month.

The CMS file is Windows-1252 with CRLF line endings; the monthly files are written as UTF-8
with LF line endings. Every value is copied as text, so PROVNUM, COUNTY_FIPS and WorkDate keep
their leading zeros and letters.

Usage:
  python scripts/split_pbj_by_month.py ~/Downloads/Nursing_Homes_data/PBJ_Daily_Nurse_Staffing_Q2_2024.csv \
      --out-dir ~/Downloads/Nursing_Homes_data/monthly
"""
import argparse
import csv
from collections import Counter
from pathlib import Path

EXPECTED = {"202404": 436_920, "202405": 451_484, "202406": 436_920}

ap = argparse.ArgumentParser()
ap.add_argument("src", type=Path)
ap.add_argument("--out-dir", type=Path, default=Path("."))
args = ap.parse_args()
out_dir = args.out_dir.expanduser()
out_dir.mkdir(parents=True, exist_ok=True)

with open(args.src.expanduser(), newline="", encoding="cp1252") as fh:
    reader = csv.reader(fh)
    header = next(reader)
    assert len(header) == 33, f"expected 33 columns, got {len(header)}"
    work_date = header.index("WorkDate")
    outs, writers, counts = {}, {}, Counter()
    for row in reader:
        assert len(row) == 33, f"ragged row at data line {sum(counts.values()) + 1}"
        month = row[work_date][:6]
        if month not in writers:
            path = out_dir / f"PBJ_Daily_Nurse_Staffing_{month[:4]}_{month[4:]}.csv"
            outs[month] = open(path, "w", newline="", encoding="utf-8")
            writers[month] = csv.writer(outs[month], lineterminator="\n")
            writers[month].writerow(header)
        writers[month].writerow(row)
        counts[month] += 1
    for f in outs.values():
        f.close()

assert dict(counts) == EXPECTED, f"unexpected row counts: {dict(counts)}"
for month, n in sorted(counts.items()):
    print(f"PBJ_Daily_Nurse_Staffing_{month[:4]}_{month[4:]}.csv  {n:,} rows")
print(f"{sum(counts.values()):,} rows split into {len(counts)} monthly UTF-8 files in {out_dir}")