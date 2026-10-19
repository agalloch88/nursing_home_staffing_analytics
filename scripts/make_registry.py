"""Write config/datasets.yml, the dataset registry, from the real file headers.

The registry is the contract for every source file. The ingest job uses each dataset's `pattern`
to decide which dataset a Drive file belongs to; the bronze jobs check each file's header against
`columns` and decode it with the first `encoding` that works. Only the six `bronze: true` datasets
need columns; the other 15 are copied to landing and stop there.

Usage (from the repo root):
  python scripts/make_registry.py ~/Downloads/Nursing_Homes_data --out config/datasets.yml
"""
import argparse
import csv
import json
import re
from pathlib import Path

# dataset -> (file name pattern, bronze?, encodings to try in order, column renames)
CCN = "CMS Certification Number (CCN)"
SPEC = {
    # The six datasets the pipeline models
    "pbj_daily_staffing": (r"^PBJ_Daily_Nurse_Staffing_.*\.csv$", True, ["utf-8", "cp1252"],
                           {"PROVNUM": "ccn", "PROVNAME": "facility_name", "WorkDate": "work_date",
                            "MDScensus": "census"}),
    "provider_info": (r"^NH_ProviderInfo_.*\.csv$", True, ["utf-8"], {CCN: "ccn", "Provider Name": "facility_name"}),
    "state_us_averages": (r"^NH_StateUSAverages_.*\.csv$", True, ["utf-8"], {"State or Nation": "state"}),
    "quality_claims": (r"^NH_QualityMsr_Claims_.*\.csv$", True, ["utf-8"], {CCN: "ccn"}),
    "snf_qrp": (r"^Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_.*\.csv$", True, ["utf-8"],
                {CCN: "ccn"}),
    "snf_vbp": (r"^FY_\d{4}_SNF_VBP_Facility_Performance\.csv$", True, ["utf-8"], {CCN: "ccn"}),
    # Landing only: copied to S3 and kept, not modelled (SME-approved, design doc section on scope)
    "quality_mds": (r"^NH_QualityMsr_MDS_.*\.csv$", False, None, None),
    "health_citations": (r"^NH_HealthCitations_.*\.csv$", False, None, None),
    "fire_safety_citations": (r"^NH_FireSafetyCitations_.*\.csv$", False, None, None),
    "citation_descriptions": (r"^NH_CitationDescriptions_.*\.csv$", False, None, None),
    "ownership": (r"^NH_Ownership_.*\.csv$", False, None, None),
    "penalties": (r"^NH_Penalties_.*\.csv$", False, None, None),
    "survey_summary": (r"^NH_SurveySummary_.*\.csv$", False, None, None),
    "survey_dates": (r"^NH_SurveyDates_.*\.csv$", False, None, None),
    "covid_vax_provider": (r"^NH_CovidVaxProvider_.*\.csv$", False, None, None),
    "covid_vax_averages": (r"^NH_CovidVaxAverages_.*\.csv$", False, None, None),
    "data_collection_intervals": (r"^NH_DataCollectionIntervals_.*\.csv$", False, None, None),
    "inspection_cutpoints": (r"^NH_HlthInspecCutpointsState_.*\.csv$", False, None, None),
    "snf_qrp_national": (r"^Skilled_Nursing_Facility_Quality_Reporting_Program_National_Data_.*\.csv$",
                         False, None, None),
    "snf_vbp_aggregate": (r"^FY_\d{4}_SNF_VBP_Aggregate_Performance\.csv$", False, None, None),
    "swing_bed": (r"^Swing_Bed_SNF_data_.*\.csv$", False, None, None),   # hospitals, none in provider info
}


def header(path):
    for enc in ("utf-8-sig", "cp1252"):
        try:
            with open(path, newline="", encoding=enc) as fh:
                return next(csv.reader(fh))
        except UnicodeDecodeError:
            continue
    raise ValueError(f"can't decode the header of {path}")


def q(value):
    return json.dumps(value)        # a JSON string is a valid double-quoted YAML string


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", type=Path)
    ap.add_argument("--out", type=Path, default=Path("config/datasets.yml"))
    args = ap.parse_args()

    files = sorted(p for p in args.folder.expanduser().rglob("*.csv") if "SME_package" not in p.parts)
    matches = {}
    for p in files:                                  # the same rule the ingest job applies
        hits = [d for d, (pattern, *_) in SPEC.items() if re.match(pattern, p.name)]
        if len(hits) != 1:
            raise SystemExit(f"{p.name} matches {hits}; every file must match exactly one pattern")
        matches.setdefault(hits[0], []).append(p)
    missing = [d for d in SPEC if d not in matches]
    if missing:
        raise SystemExit(f"no file found for {missing}")

    lines = ["# Dataset registry: written by scripts/make_registry.py (build guide Phase 4b). Edit by hand",
             "# only to change skip_files (Phase 4d). Bronze keeps every column as text; silver casts.",
             "datasets:"]
    for ds, (pattern, bronze, encodings, rename) in SPEC.items():
        lines += [f"  {ds}:", f"    pattern: '{pattern}'", f"    bronze: {str(bronze).lower()}"]
        if not bronze:
            continue
        cols = header(matches[ds][0])
        for other in matches[ds][1:]:                # e.g. the three monthly PBJ files and the quarterly one
            if header(other) != cols:
                raise SystemExit(f"{other.name} has a different header from {matches[ds][0].name}")
        lines += [f"    encoding: [{', '.join(encodings)}]",
                  "    rename: {" + ", ".join(f"{q(k)}: {v}" for k, v in rename.items()) + "}",
                  "    skip_files: []",
                  f"    columns:                       # {len(cols)}, in file order, from {matches[ds][0].name}"]
        lines += [f"      - {q(c)}" for c in cols]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines) + "\n")
    n_bronze = sum(1 for s in SPEC.values() if s[1])
    print(f"{len(SPEC)} datasets ({n_bronze} bronze, {len(SPEC) - n_bronze} landing only) "
          f"from {len(files)} CSV files; written to {args.out}")


if __name__ == "__main__":
    main()