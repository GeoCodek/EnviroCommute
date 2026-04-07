"""
Simplified data cropping script.
Rules:
1. Keep only files with > 30 rows
2. Crop start only from the beginning section (never from the middle)
3. Crop end only from the ending section (never from the middle)
4. Do not normalize/coalesce/drop sensor columns; keep original row values unchanged
5. Never output files longer than one hour
"""

import pandas as pd
from pathlib import Path
import logging
import re
from datetime import datetime, timezone

# Configuration
INPUT_FOLDER = Path(r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\air_records\Enviro_CSV_All")
OUTPUT_FOLDER = Path(r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\air_records\Enviro_Out_Cut")
TEMPERATURE_COL = "temperature_compensated_C"
TEMPERATURE_FALLBACK_COLS = (
    "temperature_C",
    "temperature",
    "Temperature",
)
MIN_ROWS = 30
DROP_WINDOW = 10  # rows
EDGE_SEARCH_FRACTION = 0.20
EDGE_SEARCH_MAX_ROWS = 600
TIMESTAMP_CANDIDATE_COLS = (
    "timestamp_iso",
    "timestamp",
    "Timestamp",
    "datetime",
    "DateTime",
    "time",
)
INDOOR_FILTER_MONTHS = {1, 2}  # January, February
INDOOR_TEMP_THRESHOLD_C = 19.0
INDOOR_SHORT_DROP_MAX_SECONDS = 10 * 60  # 10 minutes
INDOOR_MIN_ABOVE_THRESHOLD_FRACTION = 0.80
INDOOR_DEFAULT_SAMPLE_SECONDS = 5.0
PM_COUNT_KEEP_COLS = (
    "pm0_3_count",
    "pm0_5_count",
    "pm1_0_count",
    "pm2_5_count",
    "pm5_0_count",
    "pm10_count",
)
MAX_OUTPUT_DURATION_SECONDS = 60 * 60
TIMESTAMP_ONLY_NAME_PATTERN = re.compile(r"^(?:enviro(?:_log)?_)?(?P<unix_ts>\d{10})$")
DATED_LOG_NAME_PATTERN = re.compile(r"^enviro_log_\d{4}-\d{2}-\d{2}_\d{10}$")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def detect_delimiter(path: Path) -> str:
    """Auto-detect CSV delimiter."""
    try:
        sample = path.read_text(encoding="utf-8", errors="ignore")[:8192]
        dialect = __import__("csv").Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        return ","


def read_csv_safe(path: Path, nrows: int | None = None) -> pd.DataFrame | None:
    """Read CSV with multiple encoding attempts."""
    delimiter = detect_delimiter(path)
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(
                path,
                sep=delimiter,
                encoding=encoding,
                nrows=nrows,
                index_col=False,  # Never let pandas silently move first column to index.
            )
        except Exception:
            continue
    logger.warning(f"Could not read: {path.name}")
    return None


def trim_fully_empty_edge_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Trim fully empty rows only at the beginning/end, never in the middle."""
    if df.empty:
        return df

    normalized = df.replace(r"^\s*$", pd.NA, regex=True)
    non_empty_idx = normalized.index[~normalized.isna().all(axis=1)]
    if len(non_empty_idx) == 0:
        return df.iloc[0:0]

    first, last = non_empty_idx[0], non_empty_idx[-1]
    return df.loc[first:last].reset_index(drop=True)


def normalize_col_name(name: str) -> str:
    """Case/whitespace-insensitive header normalization for matching."""
    return re.sub(r"\s+", "", str(name)).lower()


def keep_only_pm_count_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only requested particle-count columns among PM-related fields.
    Non-PM columns are left untouched.
    """
    keep_normalized = {normalize_col_name(col) for col in PM_COUNT_KEEP_COLS}
    drop_cols: list[str] = []

    for col in df.columns:
        normalized = normalize_col_name(col)
        if not normalized.startswith("pm"):
            continue
        if normalized in keep_normalized:
            continue
        drop_cols.append(col)

    if drop_cols:
        return df.drop(columns=drop_cols)
    return df


def pick_temperature_column(df: pd.DataFrame) -> str | None:
    """Pick the best available temperature column for cropping."""
    if TEMPERATURE_COL in df.columns:
        return TEMPERATURE_COL

    for col in TEMPERATURE_FALLBACK_COLS:
        if col in df.columns:
            return col

    return None


def normalize_output_filename(input_path: Path) -> str:
    """
    Normalize output filename to enviro_log_YYYY-MM-DD_<unix_ts>.csv
    when the input name only contains a Unix timestamp.
    """
    stem = input_path.stem
    if DATED_LOG_NAME_PATTERN.match(stem):
        return input_path.name

    match = TIMESTAMP_ONLY_NAME_PATTERN.match(stem)
    if not match:
        return input_path.name

    unix_ts = int(match.group("unix_ts"))
    date_part = datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return f"enviro_log_{date_part}_{unix_ts}{input_path.suffix}"


def extract_time_seconds(df: pd.DataFrame) -> pd.Series:
    """Extract best-effort epoch seconds from unix or timestamp columns."""
    time_s = pd.Series(float("nan"), index=df.index, dtype="float64")

    if "unix_time_s" in df.columns:
        time_s = pd.to_numeric(df["unix_time_s"], errors="coerce")

    for col in TIMESTAMP_CANDIDATE_COLS:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        parsed_s = (parsed - pd.Timestamp("1970-01-01", tz="UTC")).dt.total_seconds()
        time_s = time_s.fillna(parsed_s)
        if not time_s.isna().any():
            break

    return time_s


def max_below_threshold_run_seconds(
    temperature: pd.Series,
    time_s: pd.Series,
    threshold_c: float,
) -> float:
    """Return max continuous duration where temperature is below threshold."""
    frame = pd.DataFrame(
        {
            "temperature": pd.to_numeric(temperature, errors="coerce"),
            "time_s": pd.to_numeric(time_s, errors="coerce"),
        }
    ).dropna(subset=["temperature"])
    if frame.empty:
        return 0.0

    timed = frame.dropna(subset=["time_s"]).sort_values("time_s").reset_index(drop=True)
    if len(timed) >= 2:
        analysis_frame = timed
        positive_diffs = analysis_frame["time_s"].diff()
        positive_diffs = positive_diffs[positive_diffs > 0]
        if positive_diffs.empty:
            sample_seconds = INDOOR_DEFAULT_SAMPLE_SECONDS
        else:
            sample_seconds = float(positive_diffs.median())
    else:
        # Fallback when timestamps are missing: estimate by row cadence.
        analysis_frame = frame.reset_index(drop=True).copy()
        analysis_frame["time_s"] = (
            analysis_frame.index.to_series().astype("float64") * INDOOR_DEFAULT_SAMPLE_SECONDS
        )
        sample_seconds = INDOOR_DEFAULT_SAMPLE_SECONDS

    below_mask = analysis_frame["temperature"] < threshold_c
    run_ids = below_mask.ne(below_mask.shift(fill_value=False)).cumsum()

    max_duration = 0.0
    for _, run in analysis_frame.groupby(run_ids, sort=False):
        if not (run["temperature"].iloc[0] < threshold_c):
            continue
        duration = float(run["time_s"].iloc[-1] - run["time_s"].iloc[0] + sample_seconds)
        if duration > max_duration:
            max_duration = duration

    return max_duration


def is_jan_feb_indoor_like(df: pd.DataFrame, temp_col: str) -> tuple[bool, dict[str, float | int]]:
    """
    Identify likely indoor-only files in Jan/Feb:
    mostly >= 19C, with any cooler dips shorter than 10 minutes.
    """
    time_s = extract_time_seconds(df)
    valid_time = time_s.dropna()
    if valid_time.empty:
        return False, {}

    months = pd.to_datetime(valid_time, unit="s", errors="coerce", utc=True).dt.month.dropna()
    if months.empty:
        return False, {}
    dominant_month = int(months.mode().iloc[0])
    if dominant_month not in INDOOR_FILTER_MONTHS:
        return False, {"month": dominant_month}

    temperature = pd.to_numeric(df[temp_col], errors="coerce")
    valid_temp = temperature.dropna()
    if valid_temp.empty:
        return False, {"month": dominant_month}

    above_fraction = float((valid_temp >= INDOOR_TEMP_THRESHOLD_C).mean())
    median_temp = float(valid_temp.median())
    max_below_seconds = max_below_threshold_run_seconds(temperature, time_s, INDOOR_TEMP_THRESHOLD_C)

    is_indoor = (
        above_fraction >= INDOOR_MIN_ABOVE_THRESHOLD_FRACTION
        and median_temp >= INDOOR_TEMP_THRESHOLD_C
        and max_below_seconds < INDOOR_SHORT_DROP_MAX_SECONDS
    )
    return is_indoor, {
        "month": dominant_month,
        "above_fraction": above_fraction,
        "median_temp_c": median_temp,
        "max_below_seconds": max_below_seconds,
    }


def find_start_crop_idx(df: pd.DataFrame, temp_col: str) -> int:
    """
    Find first row where temperature starts dropping for 10+ consecutive rows.
    Returns the index of that first row (inclusive).
    """
    temp = pd.to_numeric(df[temp_col], errors="coerce")
    
    # Calculate temperature differences
    temp_diff = temp.diff()
    
    # Find rows where temperature drops (< 0)
    is_dropping = temp_diff < 0
    
    search_rows = max(DROP_WINDOW, int(len(df) * EDGE_SEARCH_FRACTION))
    search_rows = min(search_rows, EDGE_SEARCH_MAX_ROWS, len(df))

    # Find consecutive drops of 10+ rows near the beginning only.
    consecutive_count = 0
    for idx in range(search_rows):
        is_drop = bool(is_dropping.iloc[idx])
        if is_drop:
            consecutive_count += 1
            if consecutive_count >= DROP_WINDOW:
                # Return the index where the 10-row drop started
                return idx - DROP_WINDOW + 1
        else:
            consecutive_count = 0
    
    # No long drop found, start from beginning
    return 0


def find_end_crop_idx(df: pd.DataFrame, temp_col: str) -> int:
    """
    Find where temperature starts rising and remove everything up to and including that row.
    Returns the index after which to keep data (exclusive).
    """
    temp = pd.to_numeric(df[temp_col], errors="coerce")
    
    # Calculate temperature differences
    temp_diff = temp.diff()
    
    search_rows = max(DROP_WINDOW, int(len(df) * EDGE_SEARCH_FRACTION))
    search_rows = min(search_rows, EDGE_SEARCH_MAX_ROWS, len(df))
    search_start = max(1, len(temp_diff) - search_rows)

    # Find where temperature starts rising (first positive difference from the end),
    # but only in the ending section.
    for idx in range(len(temp_diff) - 1, search_start - 1, -1):
        if temp_diff.iloc[idx] > 0:
            # Found rising temperature, remove up to and including this row
            # Return the index to start keeping from
            return idx + 1
    
    # No rise found, keep everything from start
    return 0


def crop_dataframe(df: pd.DataFrame, temp_col: str) -> pd.DataFrame | None:
    """Apply both cropping rules."""
    if len(df) <= MIN_ROWS:
        return None
    
    start_idx = find_start_crop_idx(df, temp_col)
    end_idx = find_end_crop_idx(df, temp_col)
    
    # end_idx is where we start removing from the end
    if end_idx <= start_idx:
        end_idx = len(df)
    
    cropped = df.iloc[start_idx:end_idx].reset_index(drop=True)
    
    if len(cropped) > 0:
        return cropped
    return None


def cap_dataframe_duration(df: pd.DataFrame, max_seconds: float) -> pd.DataFrame:
    """
    Keep only the leading part of the frame up to max_seconds from first valid timestamp.
    Falls back to row-count capping when time columns are unavailable.
    """
    if df.empty or len(df) == 1:
        return df

    time_s = extract_time_seconds(df)
    valid_positions = [pos for pos, value in enumerate(time_s) if pd.notna(value)]

    if valid_positions:
        first_valid_pos = valid_positions[0]
        start_time = float(time_s.iloc[first_valid_pos])
        cutoff_time = start_time + max_seconds
        end_pos = len(df) - 1

        for pos in range(first_valid_pos, len(df)):
            value = time_s.iloc[pos]
            if pd.notna(value) and float(value) > cutoff_time:
                end_pos = pos - 1
                break

        if end_pos < 0:
            return df.iloc[0:0]
        return df.iloc[: end_pos + 1].reset_index(drop=True)

    # Fallback: estimate by row cadence if no timestamp information exists.
    max_rows = int(max_seconds / INDOOR_DEFAULT_SAMPLE_SECONDS) + 1
    if len(df) <= max_rows:
        return df
    return df.iloc[:max_rows].reset_index(drop=True)


def process_files() -> None:
    """Process all CSV files in input folder."""
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    
    input_files = list(INPUT_FOLDER.glob("*.csv"))
    logger.info(f"Found {len(input_files)} CSV files to process")

    processed = 0
    skipped = 0
    
    for input_path in input_files:
        # Read file
        df = read_csv_safe(input_path)
        if df is None:
            skipped += 1
            continue
        
        df = trim_fully_empty_edge_rows(df)
        if df.empty:
            logger.info(f"Skipped {input_path.name}: only empty/malformed rows")
            skipped += 1
            continue

        temp_col = pick_temperature_column(df)
        if temp_col is None:
            logger.info(
                f"Skipped {input_path.name}: missing temperature column "
                f"({TEMPERATURE_COL} or fallbacks)"
            )
            skipped += 1
            continue

        indoor_like, indoor_metrics = is_jan_feb_indoor_like(df, temp_col)
        if indoor_like:
            logger.info(
                "Skipped %s: likely indoor-only Jan/Feb data "
                "(month=%s, >=19C fraction=%.3f, median=%.2fC, longest drop=%.1fs)",
                input_path.name,
                int(indoor_metrics.get("month", -1)),
                float(indoor_metrics.get("above_fraction", 0.0)),
                float(indoor_metrics.get("median_temp_c", float("nan"))),
                float(indoor_metrics.get("max_below_seconds", 0.0)),
            )
            skipped += 1
            continue

        # Crop data
        cropped = crop_dataframe(df, temp_col)
        if cropped is None:
            logger.info(f"Skipped {input_path.name}: <= {MIN_ROWS} rows or no valid data")
            skipped += 1
            continue

        cropped = keep_only_pm_count_columns(cropped)
        before_cap_rows = len(cropped)
        cropped = cap_dataframe_duration(cropped, MAX_OUTPUT_DURATION_SECONDS)
        if cropped.empty:
            logger.info(f"Skipped {input_path.name}: empty after applying one-hour cap")
            skipped += 1
            continue
        if len(cropped) < before_cap_rows:
            logger.info(
                "Applied one-hour cap to %s: %d rows -> %d rows",
                input_path.name,
                before_cap_rows,
                len(cropped),
            )

        # Write output
        output_filename = normalize_output_filename(input_path)
        output_path = OUTPUT_FOLDER / output_filename
        legacy_output_path = OUTPUT_FOLDER / input_path.name
        try:
            cropped.to_csv(output_path, index=False)
            if output_path != legacy_output_path and legacy_output_path.exists():
                legacy_output_path.unlink()
                logger.info(f"Removed legacy output name: {legacy_output_path.name}")
            logger.info(
                f"Processed {input_path.name} -> {output_filename}: "
                f"{len(df)} rows -> {len(cropped)} rows"
            )
            processed += 1
        except Exception as e:
            logger.error(f"Failed to write {input_path.name}: {e}")
            skipped += 1
    
    logger.info(f"Completed: {processed} processed, {skipped} skipped")


if __name__ == "__main__":
    process_files()
