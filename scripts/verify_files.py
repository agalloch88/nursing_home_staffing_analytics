"""Inventory and verify the project's source files.

For every file in the folder it records size and MD5. For every CSV it also records the
encoding, line endings, column count, data-row count and ragged rows, and compares them and every
MD5 with the copies profiled for this project. With --drive-listing it also compares each MD5 with the
md5Checksum Google Drive reports, which proves an upload arrived byte for byte.

Usage:
  python scripts/verify_files.py ~/Downloads/Nursing_Homes_data
  python scripts/verify_files.py ~/Downloads/Nursing_Homes_data \
      --drive-listing docs/drive_nursing_data.json docs/drive_supporting_csv.json

Writes docs/source_inventory.csv (change with --out) and exits with status 1 if any check fails.
"""
import argparse
import codecs
import csv
import hashlib
import json
import sys
from pathlib import Path

CHUNK = 8 * 1024 * 1024

# file name -> (columns, data rows excluding the header, MD5), as profiled in October 2026.
# The three monthly files are what split_pbj_by_month.py writes from the master file.
EXPECTED = {
    "PBJ_Daily_Nurse_Staffing_Q2_2024.csv": (33, 1_325_324, "a160f1f952205e5f8339ea3f24316b57"),
    "PBJ_Daily_Nurse_Staffing_2024_04.csv": (33, 436_920, "aeee1f70b3197caa5f8b4af692cc4f17"),
    "PBJ_Daily_Nurse_Staffing_2024_05.csv": (33, 451_484, "21c9b35d99b3caa6a573155371d9f8b1"),
    "PBJ_Daily_Nurse_Staffing_2024_06.csv": (33, 436_920, "babd610b1f10a9cc8af5831f7bc99725"),
    "NH_HealthCitations_Oct2024.csv": (23, 402_424, "6760b9b75dbcc403a08bd1b0745d8704"),
    "Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv":
        (16, 711_072, "81ba9e7e766760d1730afa07535696b9"),
    "NH_QualityMsr_MDS_Oct2024.csv": (23, 266_652, "7a99d954042c2c52b17a21e40b0d50bd"),
    "NH_FireSafetyCitations_Oct2024.csv": (24, 200_003, "4c9f7144a58628026b70b8a45f1dfceb"),
    "NH_Ownership_Oct2024.csv": (13, 145_354, "1d210bd19a0d445a55c65ebb0a7f99d8"),
    "NH_QualityMsr_Claims_Oct2024.csv": (17, 59_256, "c03b68f2167b3fb3a1cde0cc2fe5c2a7"),
    "NH_SurveySummary_Oct2024.csv": (41, 44_243, "99de5f4044795d77a0e45271c33108cd"),
    "NH_ProviderInfo_Oct2024.csv": (103, 14_814, "0592154a26645c98e95c3f735b7a213c"),
    "NH_SurveyDates_Oct2024.csv": (5, 159_723, "7457076b2b619926c5b17b39e39ad017"),
    "Swing_Bed_SNF_data_Oct2024.csv": (16, 38_064, "cd0d283fba2ad5ea44e9cbfe8533f82e"),
    "NH_Penalties_Oct2024.csv": (13, 28_505, "ecd685dec0361b9e83eff801ee36a48d"),
    "FY_2024_SNF_VBP_Facility_Performance.csv": (20, 10_858, "a5e32b07f18f290fb7c95dbbf3a588d4"),
    "NH_CovidVaxProvider_20241027.csv": (5, 14_814, "7e288ccf5f11651f48b306fd2c682565"),
    "NH_CitationDescriptions_Oct2024.csv": (5, 641, "32c9093942a3ffc0de9afc62662e44e7"),
    "NH_StateUSAverages_Oct2024.csv": (48, 54, "6d52b8c8ea38af2de2f7204dde0791e4"),
    "NH_DataCollectionIntervals_Oct2024.csv": (6, 45, "fade6bd236b37eedb1806e9641d05736"),
    "NH_HlthInspecCutpointsState_Oct2024.csv": (6, 53, "8d608a67c2048cc1837da7f28b70e07f"),
    "NH_CovidVaxAverages_20241027.csv": (4, 54, "ed6334063cf13f5fd0feccbce9162d98"),
    "Skilled_Nursing_Facility_Quality_Reporting_Program_National_Data_Oct2024.csv":
        (7, 24, "4c8e90a84a8a935c138fc0b913e42796"),
    "FY_2024_SNF_VBP_Aggregate_Performance.csv": (9, 1, "61a66231e85ab829c191701a37458ec4"),
    "NH_Data_Dictionary.pdf": (None, None, "376364923660ad1548c94b1d71a4b959"),
}


def scan_bytes(path):
    """One pass over the raw bytes: MD5, strict UTF-8 check, line endings, byte-order mark."""
    md5 = hashlib.md5()
    utf8 = codecs.getincrementaldecoder("utf-8")(errors="strict")
    is_utf8, head = True, b""
    with open(path, "rb") as fh:
        while chunk := fh.read(CHUNK):
            md5.update(chunk)
            if not head:
                head = chunk[:65536]
            if is_utf8:
                try:
                    utf8.decode(chunk)
                except UnicodeDecodeError:
                    is_utf8 = False
    if is_utf8:
        try:
            utf8.decode(b"", final=True)
        except UnicodeDecodeError:
            is_utf8 = False
    return {
        "md5": md5.hexdigest(),
        "encoding": "utf-8" if is_utf8 else "cp1252",   # cp1252 is then checked strictly below
        "line_endings": "CRLF" if b"\r\n" in head else "LF",
        "bom": head.startswith(codecs.BOM_UTF8),
    }


def scan_csv(path, encoding):
    """Parse every record with the csv module (handles quoted commas, doubled quotes, newlines)."""
    reader_encoding = "utf-8-sig" if encoding == "utf-8" else encoding
    with open(path, newline="", encoding=reader_encoding, errors="strict") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = ragged = 0
        first_ragged = None
        for row in reader:
            rows += 1
            if len(row) != len(header):
                ragged += 1
                first_ragged = first_ragged or rows
    return {"columns": len(header), "rows": rows, "ragged_rows": ragged, "first_ragged_row": first_ragged}


def load_drive_listings(paths):
    """Merge files.list responses saved from the Drive API Explorer into {name: file}."""
    files = {}
    for p in paths or []:
        for f in json.loads(Path(p).read_text()).get("files", []):
            files[f["name"]] = f
    return files


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", type=Path)
    ap.add_argument("--out", type=Path, default=Path("docs/source_inventory.csv"))
    ap.add_argument("--drive-listing", nargs="*", help="files.list JSON responses from the Drive API Explorer")
    args = ap.parse_args()

    drive = load_drive_listings(args.drive_listing)
    paths = sorted(p for p in args.folder.expanduser().rglob("*")
                   if p.is_file() and p.suffix.lower() in (".csv", ".pdf") and "SME_package" not in p.parts)
    results, failures = [], 0

    for path in paths:
        print(f"checking {path.name} ...", end="", flush=True)
        r = {"file": path.name, "folder": str(path.parent), "bytes": path.stat().st_size}
        scanned = scan_bytes(path)
        r["md5"] = scanned["md5"]
        problems = []
        expected = EXPECTED.get(path.name)
        if expected is None:
            problems.append("not in the expected file list")
        elif r["md5"] != expected[2]:
            problems.append("MD5 differs from the profiled copy")

        if path.suffix.lower() == ".csv":
            r.update({k: scanned[k] for k in ("encoding", "line_endings", "bom")})
            try:
                r.update(scan_csv(path, r["encoding"]))
            except UnicodeDecodeError as err:
                problems.append(f"not decodable as UTF-8 or Windows-1252: {err}")
            if expected and "rows" in r:
                r["expected_columns"], r["expected_rows"] = expected[:2]
                if r["columns"] != expected[0]:
                    problems.append(f"{r['columns']} columns, expected {expected[0]}")
                if r["rows"] != expected[1]:
                    problems.append(f"{r['rows']:,} rows, expected {expected[1]:,}")
            if r.get("ragged_rows"):
                problems.append(f"{r['ragged_rows']} ragged rows, first at data row {r['first_ragged_row']}")

        if drive:
            f = drive.get(path.name)
            if f is None:
                r["drive"] = "not in Drive"
            elif "md5Checksum" not in f:
                r["drive"] = f"no md5 in Drive ({f.get('mimeType')}): converted to a Google file?"
                problems.append(r["drive"])
            elif f["md5Checksum"] != r["md5"]:
                r["drive"] = "MD5 differs from Drive"
                problems.append(r["drive"])
            else:
                r["drive"] = "match"

        r["status"] = "OK" if not problems else "FAIL: " + "; ".join(problems)
        failures += bool(problems)
        results.append(r)
        print(" " + r["status"])

    for name in sorted(set(drive) - {p.name for p in paths}):
        print(f"in Drive but not in the local folder: {name}")
    monthly = {n for n in EXPECTED if "_2024_0" in n}          # exist only after step 4
    missing = sorted(set(EXPECTED) - monthly - {p.name for p in paths})
    for name in missing:
        print(f"MISSING locally: {name}")
        failures += 1

    fields = ["file", "folder", "bytes", "md5", "encoding", "line_endings", "bom", "columns", "rows",
              "expected_columns", "expected_rows", "ragged_rows", "first_ragged_row", "drive", "status"]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\n{len(results)} files checked, {failures} problem(s). Inventory written to {args.out}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()