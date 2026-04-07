
from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd


# =========================
# USER SETTINGS
# =========================

# Folder containing the mobile air-quality CSV files
MOBILE_INPUT_FOLDER = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\gps_records\Google\Interpolation output\individual_interpolated_csvs"

# Output CSV path
OUTPUT_CSV = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\Vergleichs Messtation\mobile station passesver2.csv"

# Air-quality station CSV files
SILBER_STATION_CSV = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\Vergleichs Messtation\Airq\Silbersteinstraße Stundenwerte ber_mc144_20251104-20260304.csv"
MARIENDORF_STATION_CSV = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\Vergleichs Messtation\Airq\Mariendorfer Damm Stundenwerte ber_mc124_20260101-20260328.csv"

# Station coordinates and extraction radii (WGS84 / EPSG:4326)
STATIONS = [
    {
        "station_id": "silbersteinstrasse",
        "station_label": "Silbersteinstraße",
        "lat": 52.4675198,
        "lon": 13.4415632,
        "radius_m": 50.0,
        "station_csv": SILBER_STATION_CSV,
    },
    {
        "station_id": "mariendorfer_damm",
        "station_label": "Mariendorfer Damm",
        "lat": 52.43849910,
        "lon": 13.38767323,
        "radius_m": 140.0,
        "station_csv": MARIENDORF_STATION_CSV,
    },
]

# How large a time gap still counts as the same pass-by event
PASS_GAP_SECONDS = 120

# If True, each station gets at most one pass_id per local calendar day.
ONE_PASS_PER_DAY = True

# If True, keep only one preferred interpolated variant per base enviro log.
PREFER_ONE_VARIANT_PER_BASE_LOG = True

# Candidate column names in the mobile CSVs
TIMESTAMP_CANDIDATES = [
    "timestamp_iso",
    "timestamp",
    "datetime",
    "date_time",
    "time",
    "timestamp_utc",
    "timestamp_local",
    "datetime_utc",
    "datetime_local",
    "recorded_at",
    "created_at",
]
LAT_CANDIDATES = ["gps_lat", "gps_latitude", "latitude", "lat", "y"]
LON_CANDIDATES = ["gps_lon", "gps_longitude", "longitude", "lon", "lng", "x"]

# Mapping files created by the interpolation pipeline (output_file -> source_file)
INTERPOLATION_INDEX_GLOB = "individual_interpolated_files*.csv"

# Merge tolerance when attaching source mobile AQ values to interpolated rows
MOBILE_AQ_MERGE_TOLERANCE_SECONDS = 10

# =========================
# HELPERS
# =========================

def normalize_col_name(text: str) -> str:
    text = str(text).replace("\ufeff", "").strip().lower()
    text = re.sub(r"[^\w]+", "_", text, flags=re.UNICODE)
    return re.sub(r"_+", "_", text).strip("_")


def find_existing_column(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    # Keep original column names, but match in a normalized/case-insensitive way.
    normalized_to_original: dict[str, str] = {}
    for col in df.columns:
        key = normalize_col_name(col)
        if key and key not in normalized_to_original:
            normalized_to_original[key] = col

    for c in candidates:
        if c in df.columns:
            return c
        c_norm = normalize_col_name(c)
        if c_norm in normalized_to_original:
            return normalized_to_original[c_norm]

    raise ValueError(
        f"Could not find a {label} column. Tried: {candidates}. "
        f"Available columns: {list(df.columns)}"
    )


def slugify(text: str) -> str:
    repl = (
        text.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("Ä", "Ae")
        .replace("Ö", "Oe")
        .replace("Ü", "Ue")
        .replace("ß", "ss")
    )
    repl = re.sub(r"[^\w]+", "_", repl, flags=re.UNICODE)
    repl = re.sub(r"_+", "_", repl).strip("_").lower()
    return repl


def is_interpolation_index_file(path: Path) -> bool:
    return path.match(INTERPOLATION_INDEX_GLOB)


def base_log_id_from_filename(path: str | Path) -> str:
    """
    Map interpolation variants back to one base enviro log id.
    """
    stem = Path(path).stem
    base = re.sub(r"_(?:cut|to_uni|from_uni|interp).*$", "", stem, flags=re.IGNORECASE)
    return base or stem


def file_preference_key(path: Path) -> tuple[float, int, str]:
    """
    Newest and largest file wins within one base log id.
    """
    try:
        stat = path.stat()
        return (stat.st_mtime, stat.st_size, path.name.lower())
    except OSError:
        return (0.0, 0, path.name.lower())


def select_preferred_mobile_variants(
    mobile_files: list[Path],
) -> tuple[list[Path], list[tuple[str, str, str]]]:
    """
    Keep one preferred mobile CSV per base log id and return (selected, dropped_report).
    dropped_report rows are (base_log_id, dropped_file_name, kept_file_name).
    """
    by_base: dict[str, list[Path]] = {}
    for path in mobile_files:
        by_base.setdefault(base_log_id_from_filename(path.name), []).append(path)

    selected: list[Path] = []
    dropped: list[tuple[str, str, str]] = []

    for base_id, variants in by_base.items():
        kept = max(variants, key=file_preference_key)
        selected.append(kept)
        for candidate in variants:
            if candidate != kept:
                dropped.append((base_id, candidate.name, kept.name))

    selected = sorted(selected, key=lambda p: p.name.lower())
    dropped = sorted(dropped, key=lambda row: (row[0], row[1]))
    return selected, dropped


def read_csv_flexible(path: str | Path) -> pd.DataFrame:
    """
    Read CSVs that may differ in delimiter/encoding.
    """
    path = Path(path)
    parse_attempts = [
        {"sep": ",", "encoding": "utf-8-sig"},
        {"sep": ";", "encoding": "utf-8-sig"},
        {"sep": "\t", "encoding": "utf-8-sig"},
        {"sep": None, "encoding": "utf-8-sig", "engine": "python"},
        {"sep": ",", "encoding": "latin-1"},
        {"sep": ";", "encoding": "latin-1"},
        {"sep": "\t", "encoding": "latin-1"},
        {"sep": None, "encoding": "latin-1", "engine": "python"},
    ]

    df = None
    parse_errors = []
    for kwargs in parse_attempts:
        try:
            read_kwargs = dict(kwargs)
            if read_kwargs.get("engine", "c") != "python":
                # Avoid mixed-type dtype warnings in large CSVs.
                read_kwargs["low_memory"] = False
            parsed = pd.read_csv(path, **read_kwargs)
        except Exception as exc:
            parse_errors.append(f"{kwargs}: {exc}")
            continue

        # Wrong delimiter often yields one giant column that still contains separators.
        if parsed.shape[1] == 1:
            only_col = str(parsed.columns[0])
            if any(sep in only_col for sep in [",", ";", "\t"]):
                continue

        df = parsed
        break

    if df is None:
        raise ValueError(
            f"Could not parse CSV: {path}. Tried {len(parse_attempts)} parser settings. "
            f"Last error: {parse_errors[-1] if parse_errors else 'n/a'}"
        )

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    return df


def extract_mobile_aq_columns(df: pd.DataFrame) -> list[str]:
    """
    Select source-measurement columns (all non-GPS numeric sensor fields).
    """
    exclude_norm = {normalize_col_name(c) for c in TIMESTAMP_CANDIDATES + LAT_CANDIDATES + LON_CANDIDATES}
    exclude_norm.update(
        {
            "source_file",
            "source_name",
            "output_file",
            "status",
            "status_message",
            "original_filename",
            "output_filename",
            "rows_before",
            "rows_after",
            "window_start_utc",
            "window_end_utc",
            "unix_time_s",
            "gps_time_utc",
            "gps_time_iso",
        }
    )

    candidate_cols = []
    for col in df.columns:
        n = normalize_col_name(col)
        if n in exclude_norm or n.startswith("gps_"):
            continue
        candidate_cols.append(col)

    aq_cols = []
    for col in candidate_cols:
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().any():
            aq_cols.append(col)

    return aq_cols


def read_mobile_source_aq_csv(path: str | Path) -> pd.DataFrame:
    """
    Read source mobile-air file and keep AQ columns with normalized names.
    """
    src = read_csv_flexible(path)
    timestamp_col = find_existing_column(src, TIMESTAMP_CANDIDATES, "timestamp")
    aq_cols = extract_mobile_aq_columns(src)

    if not aq_cols:
        return pd.DataFrame(columns=["mobile_aq_timestamp"])

    out = src[[timestamp_col] + aq_cols].copy()
    out["mobile_aq_timestamp"] = pd.to_datetime(out[timestamp_col], errors="coerce", utc=True)

    if out["mobile_aq_timestamp"].isna().all():
        # Fallback for naive timestamps: treat them as Berlin local time.
        ts_naive = pd.to_datetime(out[timestamp_col], errors="coerce")
        out["mobile_aq_timestamp"] = (
            ts_naive.dt.tz_localize("Europe/Berlin", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
        )

    rename_map = {col: f"mobile_aq__{slugify(col)}" for col in aq_cols}
    out = out.rename(columns=rename_map)

    for col in rename_map.values():
        out[col] = pd.to_numeric(out[col], errors="coerce")

    keep_cols = ["mobile_aq_timestamp"] + list(rename_map.values())
    out = out[keep_cols].dropna(subset=["mobile_aq_timestamp"]).sort_values("mobile_aq_timestamp").reset_index(drop=True)
    return out


def load_interpolation_source_map(mobile_folder: str | Path) -> dict[str, Path]:
    """
    Read interpolation index CSV(s) and map interpolated output filename -> source mobile-air file.
    """
    mobile_folder = Path(mobile_folder)
    mapping: dict[str, Path] = {}

    for idx_path in sorted(mobile_folder.glob(INTERPOLATION_INDEX_GLOB)):
        try:
            idx = read_csv_flexible(idx_path)
        except Exception:
            continue

        if not {"source_file", "output_file"}.issubset(idx.columns):
            continue

        for row in idx.itertuples(index=False):
            source_file = str(getattr(row, "source_file", "")).strip()
            output_file = str(getattr(row, "output_file", "")).strip()

            if not source_file or not output_file or source_file.lower() == "nan" or output_file.lower() == "nan":
                continue

            mapping[Path(output_file).name] = Path(source_file)

    return mapping


def enrich_mobile_with_source_aq(
    mobile_df: pd.DataFrame,
    source_aq_df: pd.DataFrame,
    tolerance_seconds: int = MOBILE_AQ_MERGE_TOLERANCE_SECONDS,
) -> pd.DataFrame:
    """
    Attach source mobile AQ measurements to interpolated mobile rows via nearest timestamp merge.
    """
    if mobile_df.empty or source_aq_df.empty:
        return mobile_df

    # Make sure both keys are timezone-aware UTC.
    left = mobile_df.copy()
    if not isinstance(left["mobile_timestamp"].dtype, pd.DatetimeTZDtype):
        left["mobile_timestamp"] = (
            pd.to_datetime(left["mobile_timestamp"], errors="coerce")
            .dt.tz_localize("Europe/Berlin", ambiguous="NaT", nonexistent="NaT")
            .dt.tz_convert("UTC")
        )
    else:
        left["mobile_timestamp"] = left["mobile_timestamp"].dt.tz_convert("UTC")
    left = left.dropna(subset=["mobile_timestamp"]).sort_values("mobile_timestamp").reset_index(drop=True)

    right = source_aq_df.copy()
    if not isinstance(right["mobile_aq_timestamp"].dtype, pd.DatetimeTZDtype):
        right["mobile_aq_timestamp"] = (
            pd.to_datetime(right["mobile_aq_timestamp"], errors="coerce")
            .dt.tz_localize("Europe/Berlin", ambiguous="NaT", nonexistent="NaT")
            .dt.tz_convert("UTC")
        )
    else:
        right["mobile_aq_timestamp"] = right["mobile_aq_timestamp"].dt.tz_convert("UTC")
    right = right.dropna(subset=["mobile_aq_timestamp"]).sort_values("mobile_aq_timestamp").reset_index(drop=True)

    if left.empty or right.empty:
        return mobile_df

    merged = pd.merge_asof(
        left,
        right,
        left_on="mobile_timestamp",
        right_on="mobile_aq_timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=tolerance_seconds),
    )

    merged["mobile_aq_time_delta_s"] = (
        merged["mobile_timestamp"] - merged["mobile_aq_timestamp"]
    ).abs().dt.total_seconds()

    # Re-create local wall time after forcing mobile_timestamp to UTC above.
    merged["mobile_timestamp_local"] = merged["mobile_timestamp"].dt.tz_convert("Europe/Berlin").dt.tz_localize(None)
    merged["station_hour_local"] = merged["mobile_timestamp_local"].dt.floor("h")
    return merged


def haversine_m(lat1, lon1, lat2, lon2):
    """
    Vectorized haversine distance in meters.
    Inputs can be scalars or numpy/pandas arrays.
    """
    r = 6371000.0
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def read_station_csv(path: str | Path, station_id: str) -> pd.DataFrame:
    """
    Reads the semicolon-separated station CSV with metadata rows.
    Produces one row per station hour.
    """
    raw = pd.read_csv(path, sep=";", header=None, dtype=str, encoding="utf-8", engine="python")
    raw = raw.replace(r"^\s*$", np.nan, regex=True)

    if raw.shape[1] < 2:
        raise ValueError(f"Station file has too few columns: {path}")

    # Measurement names are typically in row 1, columns 1:...
    component_row = raw.iloc[1].tolist() if len(raw) > 1 else []
    component_names = [str(x).strip() if pd.notna(x) else f"value_{i}" for i, x in enumerate(component_row[1:], start=1)]

    # Find first actual data row by matching dd.mm.yyyy HH:MM in column 0
    date_mask = raw[0].astype(str).str.match(r"^\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}$", na=False)
    if not date_mask.any():
        raise ValueError(f"No station data rows found in {path}")

    first_data_idx = raw.index[date_mask][0]
    data = raw.loc[first_data_idx:, :].copy().reset_index(drop=True)

    data.columns = ["station_hour_local"] + component_names

    data["station_hour_local"] = pd.to_datetime(
        data["station_hour_local"], format="%d.%m.%Y %H:%M", errors="coerce"
    )

    data = data.loc[data["station_hour_local"].notna()].copy()

    # Convert all measurement columns to numeric where possible
    measurement_cols = [c for c in data.columns if c != "station_hour_local"]
    for col in measurement_cols:
        data[col] = pd.to_numeric(
            data[col].astype(str).str.replace(",", ".", regex=False),
            errors="coerce"
        )

    # Prefix measurement columns with station id
    rename_map = {col: f"{station_id}__{slugify(col)}" for col in measurement_cols}
    data = data.rename(columns=rename_map)

    return data.sort_values("station_hour_local").reset_index(drop=True)


def read_mobile_csv(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    df = read_csv_flexible(path)

    timestamp_col = find_existing_column(df, TIMESTAMP_CANDIDATES, "timestamp")
    lat_col = find_existing_column(df, LAT_CANDIDATES, "latitude")
    lon_col = find_existing_column(df, LON_CANDIDATES, "longitude")

    # Parse timezone-aware timestamps if present, then convert to Europe/Berlin local time.
    # The station files are in local civil time as well.
    df["mobile_timestamp"] = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True)
    if df["mobile_timestamp"].isna().all():
        # Fallback for naive timestamps
        df["mobile_timestamp"] = pd.to_datetime(df[timestamp_col], errors="coerce")

    # If timezone-aware, convert to Berlin and drop timezone for local-hour matching
    if isinstance(df["mobile_timestamp"].dtype, pd.DatetimeTZDtype):
        df["mobile_timestamp_local"] = df["mobile_timestamp"].dt.tz_convert("Europe/Berlin").dt.tz_localize(None)
    else:
        df["mobile_timestamp_local"] = df["mobile_timestamp"]

    df["station_hour_local"] = df["mobile_timestamp_local"].dt.floor("h")

    df["mobile_lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    df["mobile_lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    df["source_file"] = Path(path).name

    df = df.loc[
        df["mobile_timestamp_local"].notna() &
        df["mobile_lat"].notna() &
        df["mobile_lon"].notna()
    ].copy()

    return df


def assign_pass_ids(
    df: pd.DataFrame,
    gap_seconds: int = 120,
    one_pass_per_day: bool = True,
) -> pd.DataFrame:
    """
    Create pass-by IDs per station.
    If one_pass_per_day is True, each station/day is one pass.
    Otherwise, split by gap_seconds.
    """
    if df.empty:
        df["pass_id"] = pd.Series(dtype="Int64")
        df["pass_row_index"] = pd.Series(dtype="Int64")
        return df

    df = df.sort_values(["station_id", "mobile_timestamp_local", "source_file"]).copy()

    if one_pass_per_day:
        df["pass_day_local"] = df["mobile_timestamp_local"].dt.date
        unique_days = (
            df[["station_id", "pass_day_local"]]
            .drop_duplicates()
            .sort_values(["station_id", "pass_day_local"])
            .reset_index(drop=True)
        )
        unique_days["pass_id"] = unique_days.groupby("station_id").cumcount() + 1

        df = df.merge(unique_days, on=["station_id", "pass_day_local"], how="left")
        df["pass_id"] = df["pass_id"].astype("int64")
        df["pass_row_index"] = df.groupby(["station_id", "pass_id"]).cumcount() + 1
        return df.drop(columns=["pass_day_local"])

    time_diff_s = (
        df.groupby("station_id")["mobile_timestamp_local"]
        .diff()
        .dt.total_seconds()
    )

    new_pass = time_diff_s.isna() | (time_diff_s > gap_seconds)
    df["pass_id"] = new_pass.groupby(df["station_id"]).cumsum().astype("int64")
    df["pass_row_index"] = df.groupby(["station_id", "pass_id"]).cumcount() + 1

    return df


def process_mobile_folder(
    mobile_folder: str | Path,
    stations: list[dict],
    output_csv: str | Path,
    pass_gap_seconds: int = 120,
    one_pass_per_day: bool = True,
    prefer_one_variant_per_base_log: bool = True,
    skip_invalid_mobile_files: bool = True,
) -> pd.DataFrame:
    mobile_folder = Path(mobile_folder)
    output_csv = Path(output_csv)

    all_csv_files = sorted(mobile_folder.glob("*.csv"))
    if not all_csv_files:
        raise FileNotFoundError(f"No CSV files found in {mobile_folder}")

    mobile_files = [p for p in all_csv_files if not is_interpolation_index_file(p)]
    if not mobile_files:
        raise FileNotFoundError(
            f"No mobile trajectory CSV files found in {mobile_folder} after excluding index files."
        )

    if prefer_one_variant_per_base_log:
        original_count = len(mobile_files)
        mobile_files, dropped_variants = select_preferred_mobile_variants(mobile_files)
        if dropped_variants:
            collapsed_count = len(dropped_variants)
            collapsed_bases = len({base_id for base_id, _, _ in dropped_variants})
            print(
                f"Collapsed {collapsed_count} variant mobile CSV file(s) across "
                f"{collapsed_bases} base log id(s); kept {len(mobile_files)} of {original_count}."
            )

    # Read station tables once
    station_tables = {
        s["station_id"]: read_station_csv(s["station_csv"], s["station_id"])
        for s in stations
    }
    source_map = load_interpolation_source_map(mobile_folder)
    source_aq_cache: dict[Path, pd.DataFrame] = {}

    extracted = []
    skipped_files = []
    parsed_file_count = 0
    aq_columns_added = set()

    for mobile_file in mobile_files:
        try:
            mobile_df = read_mobile_csv(mobile_file)
        except Exception as exc:
            if skip_invalid_mobile_files:
                skipped_files.append((mobile_file.name, str(exc).splitlines()[0]))
                continue
            raise

        if mobile_df.empty:
            parsed_file_count += 1
            continue
        parsed_file_count += 1

        # If this is an interpolated output, enrich it with AQ columns from its source mobile-air file.
        source_aq_path = source_map.get(mobile_file.name)
        if source_aq_path is not None:
            if source_aq_path not in source_aq_cache:
                if source_aq_path.exists():
                    try:
                        source_aq_cache[source_aq_path] = read_mobile_source_aq_csv(source_aq_path)
                    except Exception as exc:
                        skipped_files.append(
                            (
                                f"{mobile_file.name} (source: {source_aq_path.name})",
                                f"Could not merge source AQ data: {str(exc).splitlines()[0]}",
                            )
                        )
                        source_aq_cache[source_aq_path] = pd.DataFrame(columns=["mobile_aq_timestamp"])
                else:
                    skipped_files.append(
                        (
                            f"{mobile_file.name} (source: {source_aq_path.name})",
                            f"Mapped source file not found: {source_aq_path}",
                        )
                    )
                    source_aq_cache[source_aq_path] = pd.DataFrame(columns=["mobile_aq_timestamp"])

            source_aq_df = source_aq_cache[source_aq_path]
            if not source_aq_df.empty:
                mobile_df = enrich_mobile_with_source_aq(
                    mobile_df=mobile_df,
                    source_aq_df=source_aq_df,
                    tolerance_seconds=MOBILE_AQ_MERGE_TOLERANCE_SECONDS,
                )
                aq_columns_added.update([c for c in mobile_df.columns if c.startswith("mobile_aq__")])

        for s in stations:
            tmp = mobile_df.copy()
            tmp["station_id"] = s["station_id"]
            tmp["station_label"] = s["station_label"]
            tmp["station_lat"] = s["lat"]
            tmp["station_lon"] = s["lon"]
            tmp["station_radius_m"] = s["radius_m"]

            tmp["distance_to_station_m"] = haversine_m(
                tmp["mobile_lat"].to_numpy(),
                tmp["mobile_lon"].to_numpy(),
                s["lat"],
                s["lon"],
            )

            tmp = tmp.loc[tmp["distance_to_station_m"] <= s["radius_m"]].copy()

            if tmp.empty:
                continue

            tmp = tmp.merge(
                station_tables[s["station_id"]],
                on="station_hour_local",
                how="left",
            )

            extracted.append(tmp)

    if skipped_files:
        print(f"Skipped {len(skipped_files)} incompatible mobile CSV file(s):")
        for file_name, reason in skipped_files:
            print(f"  - {file_name}: {reason}")

    if aq_columns_added:
        print(f"Merged {len(aq_columns_added)} mobile AQ column(s) from source files.")
        pm_cols_added = sorted([c for c in aq_columns_added if c.startswith("mobile_aq__pm")])
        if pm_cols_added:
            print(f"Detected {len(pm_cols_added)} mobile PM column(s): {pm_cols_added}")
        elif source_map:
            print("Note: no PM columns were found in the mapped source mobile files.")

    if parsed_file_count == 0:
        raise ValueError(
            f"No valid mobile CSV files found in {mobile_folder}. "
            f"Found {len(mobile_files)} file(s), skipped {len(skipped_files)}."
        )

    if not extracted:
        # Build an empty but valid output with expected columns
        station_value_cols = []
        for tbl in station_tables.values():
            station_value_cols.extend([c for c in tbl.columns if c != "station_hour_local"])
        empty_cols = [
            "mobile_timestamp_local",
            "station_hour_local",
            "station_id",
            "station_label",
            "pass_id",
            "pass_row_index",
            "source_file",
            "distance_to_station_m",
            "station_radius_m",
            "station_lat",
            "station_lon",
            "mobile_lat",
            "mobile_lon",
        ] + station_value_cols
        out = pd.DataFrame(columns=list(dict.fromkeys(empty_cols)))
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(output_csv, index=False, encoding="utf-8")
        return out

    out = pd.concat(extracted, ignore_index=True)
    out = assign_pass_ids(
        out,
        gap_seconds=pass_gap_seconds,
        one_pass_per_day=one_pass_per_day,
    )

    # Final sort
    out = out.sort_values(
        ["mobile_timestamp_local", "station_id", "distance_to_station_m", "source_file"]
    ).reset_index(drop=True)

    # Helpful leading columns
    leading_cols = [
        "mobile_timestamp_local",
        "station_hour_local",
        "station_id",
        "station_label",
        "pass_id",
        "pass_row_index",
        "source_file",
        "distance_to_station_m",
        "station_radius_m",
        "station_lat",
        "station_lon",
        "mobile_lat",
        "mobile_lon",
    ]
    other_cols = [c for c in out.columns if c not in leading_cols]
    out = out[leading_cols + other_cols]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False, encoding="utf-8")

    return out


if __name__ == "__main__":
    result = process_mobile_folder(
        mobile_folder=MOBILE_INPUT_FOLDER,
        stations=STATIONS,
        output_csv=OUTPUT_CSV,
        pass_gap_seconds=PASS_GAP_SECONDS,
        one_pass_per_day=ONE_PASS_PER_DAY,
        prefer_one_variant_per_base_log=PREFER_ONE_VARIANT_PER_BASE_LOG,
    )

    print(f"Done. Wrote {len(result):,} matched rows to:")
    print(OUTPUT_CSV)
