import argparse
import ast
import heapq
import json
import math
import os
import re
import shutil
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import requests

# ============================================================
# USER SETTINGS
# ============================================================
GOOGLE_TIMELINE_CSV = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\gps_records\Google\Zeitachse_flat.csv"

# Optional context / correction input from the Enviro+ logger.
# Set to None to run Google-only interpolation.
# SENSOR_GPS_CSV can be:
# - a single Enviro CSV file, or
# - a folder containing many Enviro CSV files (enviro_log*.csv),
#   which will be auto-matched to Google Timeline dates/timestamps.
# SENSOR_GPS_CSV = None
SENSOR_GPS_CSV = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\air_records\Enviro_Out_Cut"
SENSOR_GPS_FILE_GLOB = "enviro_log*.csv"
SENSOR_FILE_HINT_PADDING_HOURS = 24.0

OUTPUT_DIR = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\gps_records\Google\Interpolation output"
OUTPUT_PREFIX = "google_timeline_fused"
OUTPUT_INTERVAL_SECONDS = 5
WIPE_OUTPUT_DIR_BEFORE_RUN = True
SORTED_OUT_OUTPUT_SUBDIR = "sorted_out_sensor_files"
SORTED_OUT_FILENAME_TAG = "sorted_out"
INDIVIDUAL_INTERP_OUTPUT_SUBDIR = "individual_interpolated_csvs"
INDIVIDUAL_INTERP_TIME_PADDING_MINUTES = 2.0

# UNI route configuration (classification+route library)
GOOGLE_UNI_CLASSIFICATION_CSV = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\gps_records\Google\google_timeline_uni_route_classification.csv"
UNI_ROUTE_SHAPE_DIR = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\Code\GIS Model Routes"
NO_GOOGLE_UNI_CSV = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\gps_records\Google\No_google_uni.csv"
GPS_TRAINING_DIR = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\gps_records\GPS Training"
OPNV_MODE_GPKG = r"C:\Users\janek\OneDrive\Desktop\FU\enviro_pi_csv-main\Code\GIS Model Routes\opnv_mode.gpkg"
USE_CLASSIFICATION_CSV_FOR_UNI_ROUTES = False
USE_ONLY_UNI_ROUTE_SHAPES = True
UNI_ROUTE_REQUIRE_ENDPOINT_MATCH = False
UNI_ROUTE_ENDPOINT_MAX_DISTANCE_M = 1100.0
WALK_SEGMENT_END_DWELL_S = 120.0
MIN_ROUTE_CLASS_SPAN_MINUTES_PER_FILE = 25.0
INDIVIDUAL_EXPORT_INCLUDE_LOADED_FILES = True
AUDIT_REQUIRE_ACCEPTED_FIXES = False
ALLOWED_UNI_ROUTE_CLASSES = {
    "to_uni_bike",
    "from_uni_bike",
    "to_uni_opnv",
    "from_uni_opnv",
}
OPNV_AUX_MODE_STEM_RE = re.compile(r"(?:^|_)(walk|train|bus|rail|tram|subway|metro|mode)(?:_|$)")
USE_PRIMITIVE_SENSOR_ROUTE_ASSIGNMENT = True
PRIMITIVE_ROUTE_STRICT_NO_GOOGLE_MODE = True
PRIMITIVE_COMMUTE_MIN_MINUTES = 25.0
PRIMITIVE_COMMUTE_MAX_MINUTES = 50.0
PRIMITIVE_TEMP_SWING_DELTA_C = 5.0
PRIMITIVE_TEMP_SWING_WINDOW_SECONDS = 300.0

# Training-derived intersection/stop behavior model.
ENABLE_TRAINING_INTERSECTION_MODEL = False
TRAINING_INTERSECTION_MIN_GPS_MODE = 2.0
TRAINING_INTERSECTION_MIN_FILE_POINTS = 15
TRAINING_INTERSECTION_MIN_FILE_DISTANCE_M = 800.0
TRAINING_INTERSECTION_STOP_SPEED_Q = 0.20
TRAINING_INTERSECTION_STOP_SPEED_MIN_MPS = 0.45
TRAINING_INTERSECTION_STOP_SPEED_MAX_MPS = 1.60
TRAINING_INTERSECTION_MIN_STOP_DURATION_S = 8.0
TRAINING_INTERSECTION_MAX_STOP_DURATION_S = 240.0
TRAINING_INTERSECTION_REL_BIN = 0.035
TRAINING_INTERSECTION_MIN_CLUSTER_EVENTS = 2
TRAINING_INTERSECTION_MAX_HOTSPOTS = 18
TRAINING_INTERSECTION_OPNV_DWELL_SCALE = 0.60
TRAINING_INTERSECTION_MAX_SLACK_USAGE = 0.95

# Optional time-zone tag for convenience columns.
LOCAL_TIMEZONE = "Europe/Berlin"

# Legacy external-routing options (forced off in this workflow).
USE_OSRM = False
OSRM_BASE_URL = "https://router.project-osrm.org"
OSRM_TIMEOUT = 20
OSRM_SLEEP_SECONDS = 0.10
MAX_ROUTING_DISTANCE_KM = 80.0
OSRM_HEALTHCHECK_TIMEOUT = 3
OSRM_MAX_CONSECUTIVE_FAILURES = 5
# Optional: OSRM nearest-road snapping for each accepted sensor fix.
# Keep False for large multi-file runs to avoid very slow network-bound execution.
USE_OSRM_SENSOR_SNAP = False

# Google-mode default speeds [m/s]. Used when timing between anchors is sparse.
MODE_SPEED_MPS: Dict[str, float] = {
    "WALKING": 1.4,
    "RUNNING": 2.8,
    "CYCLING": 5.5,
    "IN_BUS": 7.5,
    "IN_PASSENGER_VEHICLE": 11.5,
    "IN_TRAM": 8.0,
    "IN_SUBWAY": 13.5,
    "IN_TRAIN": 18.0,
}
ROUTE_MODE_SPEED_MPS: Dict[str, float] = {
    "walk": MODE_SPEED_MPS["WALKING"],
    "bus": MODE_SPEED_MPS["IN_BUS"],
    "train": MODE_SPEED_MPS["IN_TRAIN"],
    "bike": MODE_SPEED_MPS["CYCLING"],
}
DEFAULT_SPEED_MPS = 4.0

ROUTABLE_MODES = {
    "WALKING": "foot",
    "RUNNING": "foot",
    "CYCLING": "bike",
}

MODE_ALIASES = {
    "IN_VEHICLE": "IN_PASSENGER_VEHICLE",
    "ON_FOOT": "WALKING",
    "IN_RAIL_VEHICLE": "IN_TRAIN",
}

# Transit assumptions for this workflow:
# - no car routing
# - motorized uncertain labels are treated as rail transit context
FORCE_RAIL_TRANSIT = True
MOTORIZED_TO_RAIL_MODE = {
    "IN_BUS": "IN_TRAIN",
    "IN_PASSENGER_VEHICLE": "IN_TRAIN",
    "IN_VEHICLE": "IN_TRAIN",
}
RAIL_TRANSIT_MODES = {"IN_TRAIN", "IN_SUBWAY", "IN_TRAM"}

# Legacy GTFS rail-shape support (disabled for UNI-route-only workflow).
GTFS_RAIL_ZIP: Optional[str] = None
AUTO_FIND_GTFS_RAIL_ZIP = False
MAX_RAIL_ENDPOINT_SNAP_M = 900.0
MAX_RAIL_MEAN_SNAP_M = 700.0
MAX_RAIL_TIMELINE_POINT_SNAP_M = 1200.0
MAX_RAIL_SHAPE_CANDIDATES = 180
RAIL_BBOX_PREFILTER_FACTOR = 4.0
# Pairwise fallback when a full segment cannot be represented by one GTFS shape
# (e.g., transfer-heavy rail journeys). This avoids "air-route" straight lines.
RAIL_PAIRWISE_ENDPOINT_SNAP_M = 1800.0
RAIL_PAIRWISE_MEAN_SNAP_M = 1400.0
RAIL_PAIRWISE_TIMELINE_POINT_SNAP_M = 2200.0
RAIL_PAIRWISE_MAX_CANDIDATES = 300
RAIL_DIRECT_CORRIDOR_SNAP_M = 1500.0

# Optional source enhancement (official Berlin/VBB + optional OSM fallback).
VBB_GTFS_STATIC_URL = (
    "https://unternehmen.vbb.de/fileadmin/user_upload/VBB/Dokumente/API-Datensaetze/gtfs-mastscharf/GTFS.zip"
)
BERLIN_DETAILNETZ_WFS = "https://gdi.berlin.de/services/wfs/detailnetz"
BERLIN_RADNETZ_WFS = "https://gdi.berlin.de/services/wfs/radverkehrsnetz"
BERLIN_FOOTNET_WFS = "https://gdi.berlin.de/services/wfs/fussgaengernetz"
BERLIN_BVG_UNDISTURBED_WFS = "https://gdi.berlin.de/services/wfs/oepnv_ungestoert"
GEOFABRIK_BERLIN_PBF_URL = "https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf"
GEOFABRIK_BERLIN_GPKG_ZIP_URL = "https://download.geofabrik.de/europe/germany/berlin-latest-free.gpkg.zip"

SOURCE_CACHE_DIR = None  # None -> next to GOOGLE_TIMELINE_CSV as "_source_cache"
AUTO_DOWNLOAD_VBB_GTFS = True
AUTO_DOWNLOAD_OSM_BERLIN = False
SOURCE_HTTP_TIMEOUT = 60
SOURCE_MAX_AGE_HOURS_GTFS = 24.0 * 3.0
SOURCE_MAX_AGE_HOURS_WFS = 24.0 * 30.0
SOURCE_MAX_AGE_HOURS_OSM = 24.0 * 7.0
SOURCE_RETRY_COUNT = 3
SOURCE_RETRY_SLEEP_S = 1.0

WFS_PAGE_SIZE = 50000
WFS_PREFER_EPSG = "EPSG:4326"
WFS_FORCE_REFRESH = False
WFS_TYPENAME_KEYWORDS = {
    "detailnetz": ["detail", "strassen", "straße", "abschnitt", "netz", "kante"],
    "radnetz": ["rad", "verkehr", "netz"],
    "footnetz": ["fuss", "fuß", "gaenger", "gänger", "netz", "kante"],
    "bvg_oepnv": ["linie", "linien", "tram", "strassenbahn", "halte", "oepnv", "bus"],
}

# Station-gap routing:
# if a rail segment starts/ends near stations but has sparse points,
# use shortest-path over GTFS stop graph, then materialize on rail shapes.
ENABLE_STATION_SHORTEST_RAIL_PATH = False
STATION_PATH_MODES = {"IN_SUBWAY", "IN_TRAIN"}
STATION_GAP_MAX_TIMELINE_POINTS = 2
STATION_ENDPOINT_SNAP_M = 1000.0
STATION_CANDIDATES_PER_SIDE = 4
STATION_DIRECT_SAME_STATION_M = 250.0
STATION_PATH_MIN_DURATION_S = 180.0

# Input-range subsetting + progress telemetry.
ENABLE_SENSOR_FILENAME_TIME_WINDOW = False
SENSOR_FILENAME_WINDOW_PADDING_HOURS = 24.0
SENSOR_RUNTIME_WINDOW_PADDING_MINUTES = 30.0
USE_SENSOR_FILENAME_HINT_FILTER = False
PROGRESS_LOG_EVERY_SEGMENTS = 25
PROGRESS_LOG_EVERY_SECONDS = 20.0

# Sensor-GPS heuristics.
# gps_mode >= 2 is treated as a usable fix; 3 is usually the stronger case.
SENSOR_FIX_MIN_MODE = 2
SENSOR_HARD_MAX_EPH_M = 250.0
SENSOR_SOFT_MAX_EPH_M = 120.0
SENSOR_MIN_TIME_SEPARATION_S = 15.0
SENSOR_MIN_SPATIAL_SEPARATION_M = 10.0
SENSOR_BASELINE_DIST_FLOOR_M = 55.0
SENSOR_BASELINE_DIST_EPH_FACTOR = 3.5
SENSOR_NEAR_BASELINE_KEEP_RAW_M = 20.0
SENSOR_STATIONARY_RADIUS_M = 140.0
SENSOR_BLEND_MAX_SHIFT_M = 60.0
BACKTRACK_TOLERANCE_M = 60.0

# Long dwell inside a time window is handled as slack, not continuous movement.
SPEED_TOLERANCE_FACTOR = 1.20

_OSRM_RUNTIME_ENABLED = USE_OSRM
_OSRM_HEALTHCHECK_DONE = False
_OSRM_CONSECUTIVE_FAILURES = 0
_RAIL_SHAPES_CACHE = None
_RAIL_SHAPES_SOURCE = None
_GTFS_STATION_GRAPH_CACHE = None
_GTFS_STATION_GRAPH_SOURCE = None
_STATION_CANDIDATE_CACHE = {}
_TRAINING_INTERSECTION_MODEL_CACHE = None
_TRAINING_INTERSECTION_MODEL_SOURCE = None
_OPNV_MODE_PROFILE_CACHE = None
_OPNV_MODE_PROFILE_SOURCE = None

try:
    import geopandas as gpd
except Exception:
    gpd = None

try:
    import psutil
except Exception:
    psutil = None


# ============================================================
# DATA STRUCTURES
# ============================================================
@dataclass
class PointTime:
    lat: float
    lon: float
    t: pd.Timestamp


@dataclass
class Anchor:
    lat: float
    lon: float
    t: pd.Timestamp
    source: str
    chainage_m: Optional[float] = None
    speed_mps: Optional[float] = None
    eph_m: Optional[float] = None
    raw_lat: Optional[float] = None
    raw_lon: Optional[float] = None
    fix_id: Optional[int] = None


@dataclass
class SegmentSpec:
    segment_id: int
    google_row_index: int
    kind: str
    mode: Optional[str]
    start_t: pd.Timestamp
    end_t: pd.Timestamp
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    timeline_points: List[PointTime]
    activity_distance_m: Optional[float]
    route_class_hint: Optional[str] = None


@dataclass
class RailShape:
    shape_id: str
    route_type: Optional[int]
    coords_latlon: List[Tuple[float, float]]
    cumdist: np.ndarray
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


# ============================================================
# GEOMETRY HELPERS
# ============================================================
def parse_latlng(text: Optional[str]) -> Optional[Tuple[float, float]]:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return None
    s = str(text).replace("°", "").strip()
    if not s:
        return None
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 2:
        return None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def haversine_m_vectorized(
    lat1: np.ndarray,
    lon1: np.ndarray,
    lat2: np.ndarray,
    lon2: np.ndarray,
) -> np.ndarray:
    r = 6371000.0
    p1 = np.radians(lat1)
    p2 = np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2.0) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2.0) ** 2
    a = np.clip(a, 0.0, 1.0)
    return 2.0 * r * np.arcsin(np.sqrt(a))


def cumulative_lengths(coords_latlon: List[Tuple[float, float]]) -> np.ndarray:
    if len(coords_latlon) == 0:
        return np.array([], dtype=float)
    d = [0.0]
    for i in range(1, len(coords_latlon)):
        a = coords_latlon[i - 1]
        b = coords_latlon[i]
        d.append(d[-1] + haversine_m(a[0], a[1], b[0], b[1]))
    return np.array(d, dtype=float)


def interpolate_on_polyline(
    coords_latlon: List[Tuple[float, float]],
    cumdist: np.ndarray,
    target_distance_m: float,
) -> Tuple[float, float]:
    if len(coords_latlon) == 1:
        return coords_latlon[0]
    total = float(cumdist[-1])
    if total <= 0:
        return coords_latlon[-1]
    x = min(max(target_distance_m, 0.0), total)
    idx = np.searchsorted(cumdist, x, side="right") - 1
    idx = max(0, min(idx, len(coords_latlon) - 2))
    d0, d1 = cumdist[idx], cumdist[idx + 1]
    p0, p1 = coords_latlon[idx], coords_latlon[idx + 1]
    if d1 <= d0:
        return p1
    w = (x - d0) / (d1 - d0)
    lat = p0[0] + w * (p1[0] - p0[0])
    lon = p0[1] + w * (p1[1] - p0[1])
    return lat, lon


def load_uni_route_classification(classification_csv: str) -> pd.DataFrame:
    if not classification_csv:
        return pd.DataFrame()
    try:
        df = pd.read_csv(classification_csv)
    except Exception as exc:
        print(f"[WARN] Could not read uni classification csv '{classification_csv}': {exc}")
        return pd.DataFrame()

    if "final_class" not in df.columns:
        print(f"[WARN] classification file does not contain 'final_class' column")
        df["final_class"] = "unknown"

    df["final_class"] = df["final_class"].fillna("unknown").astype(str)
    for col in [
        "final_direction",
        "final_transport",
        "best_reference_direction",
        "best_reference_transport",
        "best_reference_name",
        "best_reference_file",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = ""
    df["route_class_key"] = df.apply(infer_uni_route_class_from_classification_row, axis=1)
    return df


def normalize_uni_direction(value: Optional[str]) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    token = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    if not token:
        return None
    if "from_uni" in token or ("from" in token and "uni" in token):
        return "from_uni"
    if "to_uni" in token or ("to" in token and "uni" in token):
        return "to_uni"
    return None


def normalize_uni_transport(value: Optional[str]) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    token = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    if not token:
        return None
    if "bike" in token or "cycling" in token:
        return "bike"
    if "opnv" in token or "oepnv" in token or "public" in token or "pt" in token:
        return "opnv"
    return None


def canonical_uni_route_class(value: Optional[str]) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    token = Path(raw).stem.lower()
    token = token.replace("__", "_")
    token = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    if not token:
        return None

    direction = normalize_uni_direction(token)
    transport = normalize_uni_transport(token)
    if direction is not None and transport is not None:
        return f"{direction}_{transport}"
    return None


def infer_uni_route_class_from_classification_row(row: pd.Series) -> Optional[str]:
    for col in ("final_class", "best_reference_name", "best_reference_file"):
        key = canonical_uni_route_class(row.get(col))
        if key is not None:
            return key

    direction = (
        normalize_uni_direction(row.get("final_direction"))
        or normalize_uni_direction(row.get("best_reference_direction"))
    )
    transport = (
        normalize_uni_transport(row.get("final_transport"))
        or normalize_uni_transport(row.get("best_reference_transport"))
    )
    if direction is not None and transport is not None:
        return f"{direction}_{transport}"
    return None


def resolve_uni_route_shape_dir(route_dir: str) -> str:
    requested = Path(route_dir).expanduser()
    if requested.exists():
        return str(requested.resolve())

    fallback_candidates: List[Path] = []
    parent = requested.parent if requested.parent != requested else None
    if parent is not None:
        fallback_candidates.extend([
            parent / "GIS",
            parent / "GIS Model Routes",
        ])
    # Project-level fallback for common layouts.
    if parent is not None and parent.parent is not None:
        fallback_candidates.extend([
            parent.parent / "Code" / "GIS",
            parent.parent / "Code" / "GIS Model Routes",
        ])

    seen: Set[str] = set()
    for cand in fallback_candidates:
        key = str(cand).lower()
        if key in seen:
            continue
        seen.add(key)
        if cand.exists():
            print(f"[WARN] Requested route folder '{requested}' not found. Using '{cand}' instead.")
            return str(cand.resolve())

    print(f"[WARN] Requested route folder '{requested}' not found and no fallback existed.")
    return str(requested)


def prepare_uni_classification_lookup(classification_df: pd.DataFrame) -> pd.DataFrame:
    if classification_df.empty:
        return pd.DataFrame(columns=["trip_start_utc", "trip_end_utc", "trip_mid_utc", "route_class_key"])

    df = classification_df.copy()

    def parse_to_utc(column_name: str, assume_local_tz: bool = False) -> pd.Series:
        if column_name not in df.columns:
            return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
        raw = pd.to_datetime(df[column_name], errors="coerce")
        if assume_local_tz:
            try:
                return raw.dt.tz_localize(LOCAL_TIMEZONE, ambiguous="infer", nonexistent="shift_forward").dt.tz_convert("UTC")
            except Exception:
                return pd.to_datetime(df[column_name], utc=True, errors="coerce")
        try:
            return pd.to_datetime(df[column_name], utc=True, errors="coerce")
        except Exception:
            try:
                return raw.dt.tz_localize("UTC")
            except Exception:
                return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

    start_utc = parse_to_utc("start_time_utc", assume_local_tz=False)
    end_utc = parse_to_utc("end_time_utc", assume_local_tz=False)
    start_berlin = parse_to_utc("start_time_berlin", assume_local_tz=True)
    end_berlin = parse_to_utc("end_time_berlin", assume_local_tz=True)

    start = start_utc.fillna(start_berlin)
    end = end_utc.fillna(end_berlin)

    lookup = pd.DataFrame({
        "trip_start_utc": start,
        "trip_end_utc": end,
        "route_class_key": df.get("route_class_key"),
    })
    lookup["route_class_key"] = lookup["route_class_key"].apply(canonical_uni_route_class)
    lookup = lookup[
        lookup["trip_start_utc"].notna()
        & lookup["trip_end_utc"].notna()
        & lookup["route_class_key"].notna()
    ].copy()
    lookup = lookup[lookup["route_class_key"].isin(ALLOWED_UNI_ROUTE_CLASSES)].copy()
    lookup = lookup[lookup["trip_end_utc"] >= lookup["trip_start_utc"]].copy()
    lookup["trip_mid_utc"] = lookup["trip_start_utc"] + (lookup["trip_end_utc"] - lookup["trip_start_utc"]) / 2
    lookup = lookup.sort_values(["trip_start_utc", "trip_end_utc"]).reset_index(drop=True)
    return lookup


def infer_segment_route_class(segment: SegmentSpec, classification_lookup: pd.DataFrame) -> Optional[str]:
    if classification_lookup is None or classification_lookup.empty:
        return None
    if pd.isna(segment.start_t) or pd.isna(segment.end_t):
        return None

    seg_start = segment.start_t
    seg_end = segment.end_t
    seg_mid = seg_start + (seg_end - seg_start) / 2

    best_overlap = 0.0
    best_span = float("inf")
    best_key = None
    for row in classification_lookup.itertuples(index=False):
        overlap_start = max(seg_start, row.trip_start_utc)
        overlap_end = min(seg_end, row.trip_end_utc)
        overlap_s = float((overlap_end - overlap_start).total_seconds())
        if overlap_s <= 0:
            continue
        span_s = float((row.trip_end_utc - row.trip_start_utc).total_seconds())
        if overlap_s > best_overlap or (overlap_s == best_overlap and span_s < best_span):
            best_overlap = overlap_s
            best_span = span_s
            best_key = row.route_class_key

    if best_key is not None:
        return canonical_uni_route_class(best_key)
    return None


def infer_transport_bucket_from_google_mode(mode: Optional[str]) -> Optional[str]:
    mode_norm = normalize_mode(mode)
    if mode_norm == "CYCLING":
        return "bike"
    if mode_norm in (RAIL_TRANSIT_MODES | {"IN_BUS", "IN_PASSENGER_VEHICLE", "IN_RAIL_VEHICLE"}):
        return "opnv"
    if mode_norm in {"WALKING", "RUNNING"}:
        # Walking portions are commonly part of OPNV commutes.
        return "opnv"
    return None


def assign_time_mode_route_hints(segments: List[SegmentSpec]) -> int:
    groups: Dict[Tuple[date, str], List[SegmentSpec]] = {}
    for seg in segments:
        if pd.isna(seg.start_t):
            continue
        transport = infer_transport_bucket_from_google_mode(seg.mode)
        if transport is None:
            continue
        try:
            local_day = seg.start_t.tz_convert(LOCAL_TIMEZONE).date()
        except Exception:
            continue
        groups.setdefault((local_day, transport), []).append(seg)

    assigned_n = 0
    for (_, transport), segs in groups.items():
        segs = sorted(segs, key=lambda s: s.start_t)
        if not segs:
            continue
        t_first = segs[0].start_t
        t_last = segs[-1].start_t
        for seg in segs:
            if t_first == t_last:
                try:
                    hour = seg.start_t.tz_convert(LOCAL_TIMEZONE).hour
                except Exception:
                    hour = 12
                direction = "to_uni" if hour < 14 else "from_uni"
            else:
                dt_to_first = abs(float((seg.start_t - t_first).total_seconds()))
                dt_to_last = abs(float((t_last - seg.start_t).total_seconds()))
                direction = "to_uni" if dt_to_first <= dt_to_last else "from_uni"
            class_hint = canonical_uni_route_class(f"{direction}_{transport}")
            if class_hint in ALLOWED_UNI_ROUTE_CLASSES:
                seg.route_class_hint = class_hint
                assigned_n += 1
    return assigned_n


def _truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return False
    token = str(value).strip().lower()
    return token in {"1", "true", "yes", "y", "on"}


def has_temperature_jump_signature(air_metrics: pd.DataFrame) -> bool:
    if air_metrics.empty or "temperature_C" not in air_metrics.columns:
        return False
    work = air_metrics[["timestamp", "temperature_C"]].copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    work["temperature_C"] = pd.to_numeric(work["temperature_C"], errors="coerce")
    work = work.dropna(subset=["timestamp", "temperature_C"]).sort_values("timestamp").reset_index(drop=True)
    if len(work) < 2:
        return False

    dt_s = work["timestamp"].diff().dt.total_seconds()
    dtemp = work["temperature_C"].diff().abs()
    mask = (
        dt_s.notna()
        & dtemp.notna()
        & (dt_s > 0)
        & (dt_s <= PRIMITIVE_TEMP_SWING_WINDOW_SECONDS)
        & (dtemp >= PRIMITIVE_TEMP_SWING_DELTA_C)
    )
    return bool(mask.any())


def build_primitive_sensor_route_lookup(sensor_file_diag: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    empty_lookup = pd.DataFrame(columns=["trip_start_utc", "trip_end_utc", "trip_mid_utc", "route_class_key", "source_file"])
    if sensor_file_diag is None or sensor_file_diag.empty or "source_file" not in sensor_file_diag.columns:
        return empty_lookup, {}

    records: List[Dict[str, object]] = []
    for row in sensor_file_diag.itertuples(index=False):
        source_file = str(getattr(row, "source_file", "") or "")
        if not source_file:
            continue
        selected_for_loading = _truthy(getattr(row, "selected_for_loading", True))
        if not selected_for_loading:
            continue
        path = Path(source_file)
        if not path.exists() or not path.is_file():
            continue

        ts_df = parse_sensor_timestamps_only(path)
        if ts_df.empty:
            continue
        start_utc = pd.to_datetime(ts_df["timestamp"], utc=True, errors="coerce").min()
        end_utc = pd.to_datetime(ts_df["timestamp"], utc=True, errors="coerce").max()
        if pd.isna(start_utc) or pd.isna(end_utc) or end_utc <= start_utc:
            continue
        duration_min = float((end_utc - start_utc).total_seconds() / 60.0)
        if duration_min < PRIMITIVE_COMMUTE_MIN_MINUTES or duration_min > PRIMITIVE_COMMUTE_MAX_MINUTES:
            continue

        air_df = parse_sensor_air_metrics(path)
        temp_jump = has_temperature_jump_signature(air_df)
        try:
            local_day = start_utc.tz_convert(LOCAL_TIMEZONE).date()
        except Exception:
            continue

        records.append({
            "source_file": source_file,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "duration_min": duration_min,
            "temp_jump_flag": bool(temp_jump),
            "local_day": local_day,
        })

    if not records:
        return empty_lookup, {}

    candidates = pd.DataFrame(records)
    lookup_rows: List[Dict[str, object]] = []
    file_route_class_map: Dict[str, str] = {}

    for _, grp in candidates.groupby("local_day"):
        g = grp.sort_values("start_utc").reset_index(drop=True)
        first_start = g["start_utc"].iloc[0]
        last_start = g["start_utc"].iloc[-1]
        midpoint = first_start + (last_start - first_start) / 2 if len(g) >= 2 else first_start

        for rec in g.itertuples(index=False):
            mode_token = "opnv" if bool(rec.temp_jump_flag) else "bike"
            if len(g) >= 2:
                direction = "to_uni" if rec.start_utc <= midpoint else "from_uni"
            else:
                try:
                    hour_local = int(rec.start_utc.tz_convert(LOCAL_TIMEZONE).hour)
                except Exception:
                    hour_local = 12
                direction = "to_uni" if hour_local < 14 else "from_uni"
            class_key = canonical_uni_route_class(f"{direction}_{mode_token}")
            if class_key not in ALLOWED_UNI_ROUTE_CLASSES:
                continue
            lookup_rows.append({
                "trip_start_utc": rec.start_utc,
                "trip_end_utc": rec.end_utc,
                "trip_mid_utc": rec.start_utc + (rec.end_utc - rec.start_utc) / 2,
                "route_class_key": class_key,
                "source_file": rec.source_file,
            })
            file_route_class_map[str(rec.source_file)] = class_key

    if not lookup_rows:
        return empty_lookup, {}

    lookup = pd.DataFrame(lookup_rows)
    lookup["trip_start_utc"] = pd.to_datetime(lookup["trip_start_utc"], utc=True, errors="coerce")
    lookup["trip_end_utc"] = pd.to_datetime(lookup["trip_end_utc"], utc=True, errors="coerce")
    lookup["route_class_key"] = lookup["route_class_key"].apply(canonical_uni_route_class)
    lookup = lookup[
        lookup["trip_start_utc"].notna()
        & lookup["trip_end_utc"].notna()
        & lookup["route_class_key"].isin(ALLOWED_UNI_ROUTE_CLASSES)
    ].copy()
    lookup["trip_mid_utc"] = lookup["trip_start_utc"] + (lookup["trip_end_utc"] - lookup["trip_start_utc"]) / 2
    lookup = lookup.sort_values(["trip_start_utc", "trip_end_utc"]).reset_index(drop=True)
    return lookup, file_route_class_map


def assign_route_hints_from_lookup(segments: List[SegmentSpec], lookup: pd.DataFrame) -> int:
    if lookup is None or lookup.empty:
        return 0
    assigned_n = 0
    for seg in segments:
        hint = infer_segment_route_class(seg, lookup)
        if hint is None:
            continue
        seg.route_class_hint = hint
        assigned_n += 1
    return assigned_n


def assign_duration_order_route_hints(
    segments: List[SegmentSpec],
    mode_token: str,
) -> int:
    mode_token = str(mode_token).strip().lower()
    if mode_token not in {"bike", "opnv"}:
        mode_token = "bike"

    by_day: Dict[date, List[SegmentSpec]] = {}
    for seg in segments:
        if pd.isna(seg.start_t) or pd.isna(seg.end_t):
            continue
        duration_min = float((seg.end_t - seg.start_t).total_seconds() / 60.0)
        if duration_min < PRIMITIVE_COMMUTE_MIN_MINUTES or duration_min > PRIMITIVE_COMMUTE_MAX_MINUTES:
            continue
        try:
            d = seg.start_t.tz_convert(LOCAL_TIMEZONE).date()
        except Exception:
            continue
        by_day.setdefault(d, []).append(seg)

    assigned_n = 0
    for _, day_segs in by_day.items():
        if len(day_segs) < 2:
            continue
        ordered = sorted(day_segs, key=lambda s: s.start_t)
        first_t = ordered[0].start_t
        last_t = ordered[-1].start_t
        midpoint = first_t + (last_t - first_t) / 2
        for seg in ordered:
            direction = "to_uni" if seg.start_t <= midpoint else "from_uni"
            class_hint = canonical_uni_route_class(f"{direction}_{mode_token}")
            if class_hint in ALLOWED_UNI_ROUTE_CLASSES:
                seg.route_class_hint = class_hint
                assigned_n += 1
    return assigned_n


def list_valid_uni_route_shape_paths(route_root: Path) -> List[Path]:
    out: List[Path] = []
    for shp in sorted(route_root.rglob("*.shp")):
        class_key = canonical_uni_route_class(shp.stem)
        if class_key is None or class_key not in ALLOWED_UNI_ROUTE_CLASSES:
            continue
        stem_norm = re.sub(r"[^a-z0-9]+", "_", shp.stem.lower()).strip("_")
        if class_key in {"to_uni_opnv", "from_uni_opnv"} and OPNV_AUX_MODE_STEM_RE.search(stem_norm):
            continue
        out.append(shp)
    return out


def select_uni_route_shapes_from_classification(
    classification_df: pd.DataFrame,
    route_dir: str,
    no_google_uni_csv: str,
) -> List[Path]:
    route_root = Path(route_dir)
    if not route_root.exists():
        print(f"[WARN] Route shape directory does not exist: {route_root}")
        return []

    candidate_shapes = list_valid_uni_route_shape_paths(route_root)
    if not candidate_shapes:
        print(
            "[WARN] No valid UNI route shapefiles found. "
            f"Expected classes: {sorted(ALLOWED_UNI_ROUTE_CLASSES)}"
        )
        return []

    if classification_df.empty:
        print(
            "[INFO] No classification data; using only valid UNI route shapefiles "
            f"({len(candidate_shapes)} found)."
        )
        return candidate_shapes

    selected_filenames = set()
    for col in ["best_reference_file", "best_reference_name"]:
        if col in classification_df.columns:
            for x in classification_df[col].dropna().astype(str).unique():
                p = Path(x.strip())
                selected_filenames.add(p.name.lower())
                selected_filenames.add(p.stem.lower())

    if not selected_filenames:
        print("[INFO] No reference route filenames found in classification; keeping valid UNI route shapes.")
        return candidate_shapes

    selected_shapes = []
    excluded_shapes = []

    for shp in candidate_shapes:
        name = shp.name.lower()
        stem = shp.stem.lower()
        if name in selected_filenames or stem in selected_filenames:
            selected_shapes.append(shp)
        else:
            excluded_shapes.append(shp)

    if excluded_shapes:
        try:
            df = pd.DataFrame({"excluded_route": [str(x) for x in excluded_shapes]})
            df.to_csv(no_google_uni_csv, index=False, encoding="utf-8-sig")
            print(f"[INFO] Wrote excluded route shapes to {no_google_uni_csv}")
        except Exception as exc:
            print(f"[WARN] Could not write excluded route file list: {exc}")

    if not selected_shapes:
        print("[WARN] No classification filename matched route shapes; using valid UNI route shapes.")
        selected_shapes = candidate_shapes

    print(f"[INFO] Using {len(selected_shapes)} route shapefiles from classification (excluded {len(excluded_shapes)}).")
    return selected_shapes


def _iter_coords_latlon_from_geometry(geom) -> List[Tuple[float, float]]:
    if geom is None or getattr(geom, "is_empty", False):
        return []
    geom_type = str(getattr(geom, "geom_type", "")).lower()
    out: List[Tuple[float, float]] = []
    try:
        if geom_type == "linestring":
            out.extend([(float(y), float(x)) for x, y in geom.coords])
        elif geom_type == "multilinestring":
            for line in geom.geoms:
                out.extend([(float(y), float(x)) for x, y in line.coords])
        elif geom_type == "point":
            out.append((float(geom.y), float(geom.x)))
        elif geom_type == "multipoint":
            for p in geom.geoms:
                out.append((float(p.y), float(p.x)))
        elif hasattr(geom, "geoms"):
            for part in geom.geoms:
                out.extend(_iter_coords_latlon_from_geometry(part))
        elif hasattr(geom, "coords"):
            out.extend([(float(y), float(x)) for x, y in geom.coords])
    except Exception:
        return out
    return out


def load_uni_route_shapes(route_paths: List[Path]) -> Dict[str, List[Tuple[float, float]]]:
    shapes: Dict[str, List[Tuple[float, float]]] = {}
    class_token_score: Dict[str, int] = {}
    class_source: Dict[str, str] = {}
    if gpd is None:
        print("[WARN] geopandas not available; cannot load uni route shapefiles.")
        return shapes

    for path in route_paths:
        try:
            gdf = gpd.read_file(path)
            if gdf.empty:
                continue
            if gdf.crs is None:
                gdf = gdf.set_crs(epsg=4326, allow_override=True)
            gdf = gdf.to_crs(epsg=4326)

            coords: List[Tuple[float, float]] = []
            for geom in gdf.geometry:
                coords.extend(_iter_coords_latlon_from_geometry(geom))

            if not coords:
                continue

            class_key = canonical_uni_route_class(path.stem)
            if class_key is None or class_key not in ALLOWED_UNI_ROUTE_CLASSES:
                continue

            # Ignore auxiliary OPNV mode shapefiles (walk/train/bus); mode comes from opnv_mode.gpkg.
            stem_norm = re.sub(r"[^a-z0-9]+", "_", path.stem.lower()).strip("_")
            if class_key in {"to_uni_opnv", "from_uni_opnv"} and OPNV_AUX_MODE_STEM_RE.search(stem_norm):
                continue

            token_score = len([t for t in stem_norm.split("_") if t])
            if class_key in shapes and class_token_score.get(class_key, 10**9) <= token_score:
                continue
            shapes[class_key] = coords
            class_token_score[class_key] = token_score
            class_source[class_key] = str(path)
        except Exception as exc:
            print(f"[WARN] Failed to load route shape {path}: {exc}")
    for class_key in sorted(shapes.keys()):
        print(f"[INFO] UNI route '{class_key}' loaded from: {class_source.get(class_key, '')}")
    return shapes


def crop_route_to_segment(
    segment: SegmentSpec,
    coords_latlon: List[Tuple[float, float]],
) -> List[Tuple[float, float]]:
    if len(coords_latlon) < 2:
        return coords_latlon
    seg_start = (segment.start_lat, segment.start_lon)
    seg_end = (segment.end_lat, segment.end_lon)
    start_idx = min(
        range(len(coords_latlon)),
        key=lambda i: haversine_m(seg_start[0], seg_start[1], coords_latlon[i][0], coords_latlon[i][1]),
    )
    end_idx = min(
        range(len(coords_latlon)),
        key=lambda i: haversine_m(seg_end[0], seg_end[1], coords_latlon[i][0], coords_latlon[i][1]),
    )
    if start_idx <= end_idx:
        cropped = coords_latlon[start_idx:end_idx + 1]
    else:
        cropped = list(reversed(coords_latlon[end_idx:start_idx + 1]))
    if len(cropped) < 2:
        return coords_latlon
    return cropped


def segment_endpoint_distances_to_route(
    segment: SegmentSpec,
    coords_latlon: List[Tuple[float, float]],
) -> Tuple[float, float]:
    if not coords_latlon:
        return float("inf"), float("inf")
    start_d = min(haversine_m(segment.start_lat, segment.start_lon, lat, lon) for lat, lon in coords_latlon)
    end_d = min(haversine_m(segment.end_lat, segment.end_lon, lat, lon) for lat, lon in coords_latlon)
    return float(start_d), float(end_d)


def infer_allowed_uni_route_classes_from_mode(mode: Optional[str]) -> Set[str]:
    mode_norm = normalize_mode(mode)
    if mode_norm == "CYCLING":
        return {"to_uni_bike", "from_uni_bike"}
    if mode_norm in (RAIL_TRANSIT_MODES | {"IN_BUS", "IN_PASSENGER_VEHICLE", "IN_RAIL_VEHICLE"}):
        return {"to_uni_opnv", "from_uni_opnv"}
    # Ambiguous labels (e.g., WALKING/unknown) are resolved via endpoint fit.
    return set(ALLOWED_UNI_ROUTE_CLASSES)


def endpoint_alignment_for_route(
    segment: SegmentSpec,
    coords_latlon: List[Tuple[float, float]],
) -> Optional[Dict[str, object]]:
    if not coords_latlon:
        return None
    if len(coords_latlon) == 1:
        d_start = haversine_m(segment.start_lat, segment.start_lon, coords_latlon[0][0], coords_latlon[0][1])
        d_end = haversine_m(segment.end_lat, segment.end_lon, coords_latlon[0][0], coords_latlon[0][1])
        return {
            "coords_oriented_latlon": coords_latlon,
            "start_endpoint_distance_m": float(d_start),
            "end_endpoint_distance_m": float(d_end),
            "reversed": False,
            "endpoint_score_m": float(d_start + d_end),
        }

    start_pt = (segment.start_lat, segment.start_lon)
    end_pt = (segment.end_lat, segment.end_lon)
    first_pt = coords_latlon[0]
    last_pt = coords_latlon[-1]

    direct_start = haversine_m(start_pt[0], start_pt[1], first_pt[0], first_pt[1])
    direct_end = haversine_m(end_pt[0], end_pt[1], last_pt[0], last_pt[1])
    reverse_start = haversine_m(start_pt[0], start_pt[1], last_pt[0], last_pt[1])
    reverse_end = haversine_m(end_pt[0], end_pt[1], first_pt[0], first_pt[1])

    direct_score = direct_start + direct_end
    reverse_score = reverse_start + reverse_end

    if reverse_score + 1e-6 < direct_score:
        return {
            "coords_oriented_latlon": list(reversed(coords_latlon)),
            "start_endpoint_distance_m": float(reverse_start),
            "end_endpoint_distance_m": float(reverse_end),
            "reversed": True,
            "endpoint_score_m": float(reverse_score),
        }
    return {
        "coords_oriented_latlon": coords_latlon,
        "start_endpoint_distance_m": float(direct_start),
        "end_endpoint_distance_m": float(direct_end),
        "reversed": False,
        "endpoint_score_m": float(direct_score),
    }


def infer_uni_route_for_segment(
    segment: SegmentSpec,
    route_shapes: Dict[str, List[Tuple[float, float]]],
    classification_lookup: Optional[pd.DataFrame] = None,
) -> Optional[Dict[str, object]]:
    if not route_shapes:
        return None

    # Primary assignment: externally assigned route hint (file-time primitive lookup).
    route_hint = canonical_uni_route_class(getattr(segment, "route_class_hint", None))
    if route_hint and route_hint in route_shapes:
        return {
            "coords_latlon": route_shapes[route_hint],
            "route_class": route_hint,
            "route_shape_key": route_hint,
            "selection_method": "primitive_time_hint",
            "endpoint_score_m": np.nan,
        }

    allowed_classes = infer_allowed_uni_route_classes_from_mode(segment.mode)

    def accepted_alignment(
        class_key: str,
        coords: List[Tuple[float, float]],
    ) -> Optional[Dict[str, object]]:
        class_key = canonical_uni_route_class(class_key)
        if class_key is None:
            return None
        if class_key not in ALLOWED_UNI_ROUTE_CLASSES:
            return None
        if class_key not in allowed_classes:
            return None
        align = endpoint_alignment_for_route(segment, coords)
        if align is None:
            return None
        if UNI_ROUTE_REQUIRE_ENDPOINT_MATCH:
            if float(align["start_endpoint_distance_m"]) > UNI_ROUTE_ENDPOINT_MAX_DISTANCE_M:
                return None
            if float(align["end_endpoint_distance_m"]) > UNI_ROUTE_ENDPOINT_MAX_DISTANCE_M:
                return None
        return align

    # 1) Time-aligned classification match has highest priority.
    class_key = infer_segment_route_class(segment, classification_lookup) if classification_lookup is not None else None
    if class_key and class_key in route_shapes:
        coords = route_shapes[class_key]
        align = accepted_alignment(class_key, coords)
        if align is not None:
            return {
                "coords_latlon": crop_route_to_segment(segment, align["coords_oriented_latlon"]),
                "route_class": class_key,
                "route_shape_key": class_key,
                "selection_method": "classification_time_match",
                "endpoint_score_m": float(align["endpoint_score_m"]),
            }

    if USE_PRIMITIVE_SENSOR_ROUTE_ASSIGNMENT and PRIMITIVE_ROUTE_STRICT_NO_GOOGLE_MODE:
        return None

    # 2) Fallback: mode-appropriate shape, selected by endpoint distance.
    unique_candidates: List[Tuple[str, List[Tuple[float, float]], Dict[str, object]]] = []
    seen_ids: Set[int] = set()
    for key, coords in route_shapes.items():
        if not coords:
            continue
        if id(coords) in seen_ids:
            continue
        class_key = canonical_uni_route_class(key)
        if class_key is None:
            continue
        align = accepted_alignment(class_key, coords)
        if align is not None:
            unique_candidates.append((class_key, coords, align))
            seen_ids.add(id(coords))

    if not unique_candidates:
        return None

    best_aligned_coords: Optional[List[Tuple[float, float]]] = None
    best_key: Optional[str] = None
    best_score = float("inf")
    for class_key, _, align in unique_candidates:
        score = float(align["endpoint_score_m"])
        if score < best_score:
            best_score = score
            best_aligned_coords = align["coords_oriented_latlon"]
            best_key = class_key

    if best_aligned_coords is None or best_key is None:
        return None
    return {
        "coords_latlon": crop_route_to_segment(segment, best_aligned_coords),
        "route_class": best_key,
        "route_shape_key": best_key,
        "selection_method": "mode_endpoint_fallback",
        "endpoint_score_m": float(best_score),
    }


def normalize_route_mode_label(value: Optional[str]) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    token = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    if not token:
        return None
    if token in {"walk", "walking", "foot"}:
        return "walk"
    if token in {"train", "subway", "tram", "rail"}:
        return "train"
    if token in {"bus"}:
        return "bus"
    return None


def build_mode_spans_rel_from_segments(
    mode_segments: List[Tuple[str, List[Tuple[float, float]]]],
) -> List[Dict[str, float]]:
    spans_abs: List[Dict[str, float]] = []
    cursor_m = 0.0
    for mode, coords in mode_segments:
        if len(coords) < 2:
            continue
        length_m = float(cumulative_lengths(coords)[-1])
        if length_m <= 0:
            continue
        start_m = cursor_m
        end_m = cursor_m + length_m
        if spans_abs and spans_abs[-1]["mode"] == mode:
            spans_abs[-1]["end_m"] = end_m
        else:
            spans_abs.append({"mode": mode, "start_m": start_m, "end_m": end_m})
        cursor_m = end_m

    total_m = cursor_m
    if total_m <= 0 or not spans_abs:
        return []
    spans_rel = []
    for s in spans_abs:
        spans_rel.append({
            "mode": s["mode"],
            "start_rel": float(s["start_m"] / total_m),
            "end_rel": float(s["end_m"] / total_m),
        })
    return spans_rel


def reverse_mode_spans_rel(spans_rel: List[Dict[str, float]]) -> List[Dict[str, float]]:
    rev: List[Dict[str, float]] = []
    for s in reversed(spans_rel):
        rev.append({
            "mode": s["mode"],
            "start_rel": float(1.0 - float(s["end_rel"])),
            "end_rel": float(1.0 - float(s["start_rel"])),
        })
    rev = sorted(rev, key=lambda x: float(x["start_rel"]))
    return rev


def load_opnv_mode_profiles(opnv_mode_gpkg: str) -> Dict[str, Dict]:
    profiles: Dict[str, Dict] = {}
    if not opnv_mode_gpkg:
        return profiles
    gpkg_path = Path(opnv_mode_gpkg).expanduser()
    if not gpkg_path.exists():
        print(f"[WARN] OPNV mode GeoPackage not found: {gpkg_path}")
        return profiles
    if gpd is None:
        print("[WARN] geopandas not available; cannot read OPNV mode GeoPackage.")
        return profiles

    try:
        gdf = gpd.read_file(gpkg_path)
        if gdf.empty:
            print(f"[WARN] OPNV mode GeoPackage has no features: {gpkg_path}")
            return profiles
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326, allow_override=True)
        gdf = gdf.to_crs(epsg=4326)
    except Exception as exc:
        print(f"[WARN] Failed to read OPNV mode GeoPackage '{gpkg_path}': {exc}")
        return profiles

    if "mode" not in gdf.columns:
        print(f"[WARN] OPNV mode GeoPackage missing 'mode' column: {gpkg_path}")
        return profiles

    sort_col = None
    for c in ("id", "fid"):
        if c in gdf.columns:
            sort_col = c
            break
    if sort_col is not None:
        try:
            gdf = gdf.sort_values(sort_col)
        except Exception:
            pass

    mode_segments: List[Tuple[str, List[Tuple[float, float]]]] = []
    for row in gdf.itertuples(index=False):
        mode = normalize_route_mode_label(getattr(row, "mode", None))
        if mode is None:
            continue
        geom = getattr(row, "geometry", None)
        coords = _iter_coords_latlon_from_geometry(geom)
        if len(coords) < 2:
            continue
        mode_segments.append((mode, coords))

    if not mode_segments:
        print(f"[WARN] No usable mode segments found in {gpkg_path}")
        return profiles

    spans_rel = build_mode_spans_rel_from_segments(mode_segments)
    if not spans_rel:
        return profiles

    profiles["to_uni_opnv"] = {
        "spans_rel": spans_rel,
        "source": str(gpkg_path),
    }
    profiles["from_uni_opnv"] = {
        "spans_rel": reverse_mode_spans_rel(spans_rel),
        "source": f"{gpkg_path}::reversed",
    }
    return profiles


def get_opnv_mode_profiles(opnv_mode_gpkg: str) -> Dict[str, Dict]:
    global _OPNV_MODE_PROFILE_CACHE, _OPNV_MODE_PROFILE_SOURCE
    source_key = str(Path(opnv_mode_gpkg).expanduser()) if opnv_mode_gpkg else ""
    if _OPNV_MODE_PROFILE_CACHE is not None and _OPNV_MODE_PROFILE_SOURCE == source_key:
        return _OPNV_MODE_PROFILE_CACHE
    profiles = load_opnv_mode_profiles(opnv_mode_gpkg)
    _OPNV_MODE_PROFILE_CACHE = profiles
    _OPNV_MODE_PROFILE_SOURCE = source_key
    return profiles


# Local planar approximation around Berlin-scale extents.
def latlon_to_local_xy(lat: float, lon: float, lat0: float, lon0: float) -> Tuple[float, float]:
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(math.radians(lat0))
    return (lon - lon0) * m_per_deg_lon, (lat - lat0) * m_per_deg_lat


def local_xy_to_latlon(x: float, y: float, lat0: float, lon0: float) -> Tuple[float, float]:
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(math.radians(lat0))
    lon = lon0 + x / max(m_per_deg_lon, 1e-9)
    lat = lat0 + y / m_per_deg_lat
    return lat, lon


def project_point_to_segment(
    lat: float,
    lon: float,
    a_lat: float,
    a_lon: float,
    b_lat: float,
    b_lon: float,
) -> Tuple[float, float, float, float]:
    lat0 = (a_lat + b_lat + lat) / 3.0
    lon0 = (a_lon + b_lon + lon) / 3.0
    px, py = latlon_to_local_xy(lat, lon, lat0, lon0)
    ax, ay = latlon_to_local_xy(a_lat, a_lon, lat0, lon0)
    bx, by = latlon_to_local_xy(b_lat, b_lon, lat0, lon0)
    abx = bx - ax
    aby = by - ay
    ab2 = abx * abx + aby * aby
    if ab2 <= 1e-12:
        projx, projy = ax, ay
        frac = 0.0
    else:
        frac = ((px - ax) * abx + (py - ay) * aby) / ab2
        frac = min(max(frac, 0.0), 1.0)
        projx = ax + frac * abx
        projy = ay + frac * aby
    dist = math.hypot(px - projx, py - projy)
    proj_lat, proj_lon = local_xy_to_latlon(projx, projy, lat0, lon0)
    seg_len = math.hypot(abx, aby)
    return proj_lat, proj_lon, dist, frac * seg_len


def project_point_to_polyline(
    lat: float,
    lon: float,
    coords_latlon: List[Tuple[float, float]],
    cumdist: Optional[np.ndarray] = None,
) -> Dict[str, float]:
    if not coords_latlon:
        return {
            "lat": lat,
            "lon": lon,
            "distance_m": np.nan,
            "chainage_m": np.nan,
            "seg_index": -1,
        }
    if len(coords_latlon) == 1:
        return {
            "lat": coords_latlon[0][0],
            "lon": coords_latlon[0][1],
            "distance_m": haversine_m(lat, lon, coords_latlon[0][0], coords_latlon[0][1]),
            "chainage_m": 0.0,
            "seg_index": 0,
        }
    if cumdist is None:
        cumdist = cumulative_lengths(coords_latlon)

    best = None
    for i in range(len(coords_latlon) - 1):
        a = coords_latlon[i]
        b = coords_latlon[i + 1]
        proj_lat, proj_lon, dist_m, offset_m = project_point_to_segment(lat, lon, a[0], a[1], b[0], b[1])
        chainage_m = float(cumdist[i] + offset_m)
        cand = {
            "lat": proj_lat,
            "lon": proj_lon,
            "distance_m": dist_m,
            "chainage_m": chainage_m,
            "seg_index": i,
        }
        if best is None or cand["distance_m"] < best["distance_m"]:
            best = cand
    return best


def extract_subpolyline(
    coords_latlon: List[Tuple[float, float]],
    cumdist: np.ndarray,
    start_chainage_m: float,
    end_chainage_m: float,
) -> List[Tuple[float, float]]:
    if not coords_latlon:
        return []
    total = float(cumdist[-1]) if len(cumdist) else 0.0
    s = min(max(start_chainage_m, 0.0), total)
    e = min(max(end_chainage_m, 0.0), total)
    if e < s:
        s, e = e, s
    if total <= 0.0:
        return [coords_latlon[0], coords_latlon[-1]]

    pts = [interpolate_on_polyline(coords_latlon, cumdist, s)]
    inside = [coords_latlon[i] for i, d in enumerate(cumdist) if s < d < e]
    pts.extend(inside)
    pts.append(interpolate_on_polyline(coords_latlon, cumdist, e))

    out = [pts[0]]
    for p in pts[1:]:
        if haversine_m(out[-1][0], out[-1][1], p[0], p[1]) > 0.05:
            out.append(p)
    if len(out) == 1:
        out.append(out[0])
    return out


def weighted_centroid(latlon_weight: List[Tuple[float, float, float]]) -> Tuple[float, float]:
    if not latlon_weight:
        raise ValueError("weighted_centroid() needs at least one point")
    lat0 = sum(lat for lat, _, _ in latlon_weight) / len(latlon_weight)
    lon0 = sum(lon for _, lon, _ in latlon_weight) / len(latlon_weight)
    sw = 0.0
    sx = 0.0
    sy = 0.0
    for lat, lon, w in latlon_weight:
        x, y = latlon_to_local_xy(lat, lon, lat0, lon0)
        sx += x * w
        sy += y * w
        sw += w
    if sw <= 0:
        return lat0, lon0
    return local_xy_to_latlon(sx / sw, sy / sw, lat0, lon0)


# ============================================================
# GOOGLE INPUT NORMALIZATION
# ============================================================
def normalize_colname(col: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(col).strip().lower())


def find_column_case_insensitive(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    normalized = {normalize_colname(c): c for c in df.columns}
    for cand in candidates:
        hit = normalized.get(normalize_colname(cand))
        if hit is not None:
            return hit
    return None


def flatten_nested_mapping(node, prefix: str = "", out: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    if out is None:
        out = {}
    if isinstance(node, dict):
        for k, v in node.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            flatten_nested_mapping(v, key, out)
    elif isinstance(node, list):
        # Keep timelinePath parse-compatible with existing parser.
        if prefix.endswith("timelinePath"):
            try:
                out[prefix] = json.dumps(node, ensure_ascii=False)
            except Exception:
                out[prefix] = str(node)
        else:
            try:
                out[prefix] = json.dumps(node, ensure_ascii=False)
            except Exception:
                out[prefix] = str(node)
    else:
        out[prefix] = node
    return out


def parse_semantic_segments_cell(text: object) -> List[Dict]:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return []
    s = str(text).strip()
    if not s:
        return []
    # Google CSV often stores python-literal style with single quotes.
    try:
        obj = ast.literal_eval(s)
    except Exception:
        try:
            obj = json.loads(s)
        except Exception:
            return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []


def expand_semantic_segments_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    semantic_col = find_column_case_insensitive(df, ["semanticSegments", "semantic_segments"])
    if semantic_col is None:
        return pd.DataFrame()

    rows: List[Dict[str, object]] = []
    for _, row in df.iterrows():
        segments = parse_semantic_segments_cell(row.get(semantic_col))
        for seg in segments:
            flat = flatten_nested_mapping(seg)
            rows.append(flat)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    return out


def read_google_timeline_table(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, low_memory=False)
    start_col = find_column_case_insensitive(df, ["startTime", "start_time", "start_time_utc"])
    end_col = find_column_case_insensitive(df, ["endTime", "end_time", "end_time_utc"])
    if start_col is not None and end_col is not None:
        # Canonicalize expected column names for downstream parser logic.
        renamed = df.copy()
        if start_col != "startTime":
            renamed = renamed.rename(columns={start_col: "startTime"})
        if end_col != "endTime":
            renamed = renamed.rename(columns={end_col: "endTime"})
        return renamed

    expanded = expand_semantic_segments_dataframe(df)
    if expanded.empty:
        raise ValueError(
            "Google timeline CSV is missing start/end columns and semanticSegments could not be expanded. "
            f"Columns found: {list(df.columns)[:20]}"
        )

    start_col2 = find_column_case_insensitive(expanded, ["startTime", "start_time", "start_time_utc"])
    end_col2 = find_column_case_insensitive(expanded, ["endTime", "end_time", "end_time_utc"])
    if start_col2 is None or end_col2 is None:
        raise ValueError(
            "Expanded semanticSegments table still lacks start/end columns. "
            f"Columns found after expansion: {list(expanded.columns)[:30]}"
        )
    if start_col2 != "startTime" or end_col2 != "endTime":
        expanded = expanded.rename(columns={start_col2: "startTime", end_col2: "endTime"})
    print(
        "[INFO] Expanded semanticSegments into flat timeline rows: "
        f"{len(expanded):,} rows parsed from {Path(csv_path).name}."
    )
    return expanded


# ============================================================
# GOOGLE TIMELINE PARSING
# ============================================================
def parse_timeline_path(path_text: Optional[str]) -> List[PointTime]:
    if path_text is None or (isinstance(path_text, float) and np.isnan(path_text)):
        return []
    if isinstance(path_text, list):
        items = path_text
    else:
        try:
            items = json.loads(path_text)
        except Exception:
            try:
                items = ast.literal_eval(str(path_text))
            except Exception:
                return []
    if not isinstance(items, list):
        return []

    pts = []
    for item in items:
        latlon = parse_latlng(item.get("point"))
        t = pd.to_datetime(item.get("time"), utc=True, errors="coerce")
        if latlon is None or pd.isna(t):
            continue
        pts.append(PointTime(lat=latlon[0], lon=latlon[1], t=t))

    pts = sorted(pts, key=lambda x: x.t)
    out = []
    for p in pts:
        if not out or p.t != out[-1].t or haversine_m(p.lat, p.lon, out[-1].lat, out[-1].lon) > 0.5:
            out.append(p)
    return out


def get_mode_speed_mps(mode: Optional[str]) -> float:
    if mode is None or (isinstance(mode, float) and np.isnan(mode)):
        return DEFAULT_SPEED_MPS
    return MODE_SPEED_MPS.get(str(mode), DEFAULT_SPEED_MPS)


def get_osrm_profile(mode: Optional[str]) -> Optional[str]:
    if mode is None or (isinstance(mode, float) and np.isnan(mode)):
        return None
    return ROUTABLE_MODES.get(str(mode))


# Override helpers with more robust parsing/normalization behavior.
def parse_latlng(text: Optional[str]) -> Optional[Tuple[float, float]]:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return None
    s = str(text).replace("Â°", "").replace("°", "").strip()
    if not s:
        return None
    numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", s)
    if len(numbers) < 2:
        return None
    try:
        return float(numbers[0]), float(numbers[1])
    except ValueError:
        return None


def normalize_mode(mode: Optional[str]) -> Optional[str]:
    if mode is None or (isinstance(mode, float) and np.isnan(mode)):
        return None
    mode_norm = str(mode).strip().upper()
    if not mode_norm:
        return None
    mode_norm = MODE_ALIASES.get(mode_norm, mode_norm)
    if FORCE_RAIL_TRANSIT and mode_norm in MOTORIZED_TO_RAIL_MODE:
        return MOTORIZED_TO_RAIL_MODE[mode_norm]
    return mode_norm


def get_mode_speed_mps(mode: Optional[str]) -> float:
    mode_norm = normalize_mode(mode)
    if mode_norm is None:
        return DEFAULT_SPEED_MPS
    return MODE_SPEED_MPS.get(mode_norm, DEFAULT_SPEED_MPS)


def get_osrm_profile(mode: Optional[str]) -> Optional[str]:
    mode_norm = normalize_mode(mode)
    if mode_norm is None:
        return None
    return ROUTABLE_MODES.get(mode_norm)


# ============================================================
# OSRM
# ============================================================
def osrm_runtime_enabled() -> bool:
    global _OSRM_RUNTIME_ENABLED, _OSRM_HEALTHCHECK_DONE
    if not USE_OSRM:
        return False
    if _OSRM_HEALTHCHECK_DONE:
        return _OSRM_RUNTIME_ENABLED

    _OSRM_HEALTHCHECK_DONE = True
    probe_url = f"{OSRM_BASE_URL}/nearest/v1/driving/13.4000000,52.5000000?number=1"
    try:
        resp = requests.get(probe_url, timeout=OSRM_HEALTHCHECK_TIMEOUT)
        if resp.status_code == 200 and (resp.json() or {}).get("code") == "Ok":
            _OSRM_RUNTIME_ENABLED = True
        else:
            _OSRM_RUNTIME_ENABLED = False
            print("[WARN] OSRM health check failed; routing disabled for this run.")
    except Exception:
        _OSRM_RUNTIME_ENABLED = False
        print("[WARN] OSRM unreachable; falling back to non-routed interpolation.")
    return _OSRM_RUNTIME_ENABLED


def mark_osrm_failure() -> None:
    global _OSRM_RUNTIME_ENABLED, _OSRM_CONSECUTIVE_FAILURES
    _OSRM_CONSECUTIVE_FAILURES += 1
    if _OSRM_CONSECUTIVE_FAILURES >= OSRM_MAX_CONSECUTIVE_FAILURES:
        _OSRM_RUNTIME_ENABLED = False
        print("[WARN] OSRM disabled after repeated failures; using fallback interpolation.")


def mark_osrm_success() -> None:
    global _OSRM_CONSECUTIVE_FAILURES
    _OSRM_CONSECUTIVE_FAILURES = 0


def osrm_route(
    profile: str,
    coords_latlon: List[Tuple[float, float]],
) -> Optional[Dict]:
    if not osrm_runtime_enabled() or len(coords_latlon) < 2:
        return None

    straight_km = sum(
        haversine_m(a[0], a[1], b[0], b[1]) for a, b in zip(coords_latlon[:-1], coords_latlon[1:])
    ) / 1000.0
    if straight_km > MAX_ROUTING_DISTANCE_KM:
        return None

    coords_str = ";".join([f"{lon:.7f},{lat:.7f}" for lat, lon in coords_latlon])
    url = (
        f"{OSRM_BASE_URL}/route/v1/{profile}/{coords_str}"
        f"?overview=full&geometries=geojson&steps=false"
    )
    try:
        r = requests.get(url, timeout=OSRM_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "Ok":
            mark_osrm_failure()
            return None
        routes = data.get("routes") or []
        if not routes:
            mark_osrm_failure()
            return None
        geom = routes[0]["geometry"]["coordinates"]
        coords = [(latlon[1], latlon[0]) for latlon in geom]
        mark_osrm_success()
        return {
            "coords_latlon": coords,
            "distance_m": float(routes[0].get("distance", np.nan)),
            "duration_s": float(routes[0].get("duration", np.nan)),
            "method": f"osrm_route_{profile}",
        }
    except Exception:
        mark_osrm_failure()
        return None
    finally:
        time.sleep(OSRM_SLEEP_SECONDS)


def osrm_nearest(profile: str, lat: float, lon: float) -> Optional[Dict]:
    if not osrm_runtime_enabled():
        return None
    url = f"{OSRM_BASE_URL}/nearest/v1/{profile}/{lon:.7f},{lat:.7f}?number=1"
    try:
        r = requests.get(url, timeout=OSRM_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "Ok":
            mark_osrm_failure()
            return None
        waypoints = data.get("waypoints") or []
        if not waypoints:
            mark_osrm_failure()
            return None
        wp = waypoints[0]
        loc = wp.get("location")
        if not loc or len(loc) != 2:
            mark_osrm_failure()
            return None
        mark_osrm_success()
        return {
            "lat": float(loc[1]),
            "lon": float(loc[0]),
            "distance_m": float(wp.get("distance", np.nan)),
            "name": wp.get("name"),
        }
    except Exception:
        mark_osrm_failure()
        return None
    finally:
        time.sleep(OSRM_SLEEP_SECONDS)


# ============================================================
# SOURCE CACHE / DOWNLOAD
# ============================================================
def get_source_cache_dir() -> Path:
    if SOURCE_CACHE_DIR:
        base = Path(SOURCE_CACHE_DIR).expanduser()
    else:
        base = Path(GOOGLE_TIMELINE_CSV).expanduser().resolve().parent / "_source_cache"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _source_file_is_fresh(path: Path, max_age_hours: float) -> bool:
    if not path.exists():
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600.0
    return age_hours <= max_age_hours


def _http_get(url: str, timeout: int = SOURCE_HTTP_TIMEOUT, stream: bool = False) -> requests.Response:
    last_exc = None
    for attempt in range(1, SOURCE_RETRY_COUNT + 1):
        try:
            resp = requests.get(url, timeout=timeout, stream=stream, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < SOURCE_RETRY_COUNT:
                time.sleep(SOURCE_RETRY_SLEEP_S * attempt)
    raise last_exc


def _download_to_path(url: str, target_path: Path, max_age_hours: float, force_refresh: bool = False) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and not force_refresh and _source_file_is_fresh(target_path, max_age_hours):
        return target_path

    tmp_fd, tmp_name = tempfile.mkstemp(prefix="srcdl_", suffix=target_path.suffix)
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        with _http_get(url, timeout=SOURCE_HTTP_TIMEOUT, stream=True) as resp:
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        shutil.move(str(tmp_path), str(target_path))
        return target_path
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


# ============================================================
# RAIL SHAPES (GTFS STATIC + OFFICIAL SOURCE SUPPORT)
# ============================================================
def _resolve_gtfs_zip_local_only(google_csv: Optional[str] = None) -> Optional[Path]:
    if GTFS_RAIL_ZIP:
        direct = Path(GTFS_RAIL_ZIP).expanduser()
        if direct.exists():
            return direct
        print(f"[WARN] GTFS_RAIL_ZIP not found: {GTFS_RAIL_ZIP}")

    if not AUTO_FIND_GTFS_RAIL_ZIP:
        return None

    roots: List[Path] = []
    if google_csv:
        g = Path(google_csv).expanduser().resolve()
        roots.extend([g.parent, g.parent.parent])
        if len(g.parents) >= 3:
            project_root = g.parents[2]
            roots.extend([
                project_root,
                project_root / "data",
                project_root / "input",
                project_root / "newest csvs",
            ])

    names = [
        "vbb_gtfs.zip",
        "vbb_gtfs_static.zip",
        "VBB_GTFS.zip",
        "google_transit.zip",
        "GTFS.zip",
    ]

    seen = set()
    for root in roots:
        if not root or root in seen:
            continue
        seen.add(root)
        for name in names:
            cand = root / name
            if cand.exists():
                return cand

    for root in roots:
        if not root or not root.exists():
            continue
        try:
            for cand in root.rglob("*.zip"):
                lower = cand.name.lower()
                if "gtfs" in lower or "google_transit" in lower or "vbb" in lower:
                    return cand
        except Exception:
            continue
    return None


def ensure_vbb_gtfs_zip(force_refresh: bool = False, google_csv: Optional[str] = None) -> Optional[Path]:
    local_found = _resolve_gtfs_zip_local_only(google_csv=google_csv)
    if local_found is not None and local_found.exists():
        return local_found

    if not AUTO_DOWNLOAD_VBB_GTFS:
        return None

    target = get_source_cache_dir() / "gtfs" / "vbb_gtfs_static.zip"
    try:
        return _download_to_path(
            url=VBB_GTFS_STATIC_URL,
            target_path=target,
            max_age_hours=SOURCE_MAX_AGE_HOURS_GTFS,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        print(f"[WARN] Could not download VBB GTFS: {exc}")
        return None


def resolve_gtfs_zip_path(google_csv: Optional[str] = None) -> Optional[Path]:
    return ensure_vbb_gtfs_zip(force_refresh=False, google_csv=google_csv)


def parse_route_type(value) -> Optional[int]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def is_rail_route_type(route_type: Optional[int]) -> bool:
    if route_type is None:
        return False
    if route_type in {0, 1, 2}:  # tram, subway, rail
        return True
    # Extended GTFS route types: 100-499 cover rail/metro families.
    return 100 <= route_type < 500


def route_type_matches_mode(route_type: Optional[int], mode: Optional[str]) -> bool:
    if route_type is None:
        return True
    if mode == "IN_SUBWAY":
        return route_type in {1, 109} or 400 <= route_type < 500
    if mode == "IN_TRAM":
        return route_type == 0 or 900 <= route_type < 1000
    if mode == "IN_TRAIN":
        return route_type in {2, 109} or 100 <= route_type < 200
    return is_rail_route_type(route_type)


def point_bbox_distance_m(lat: float, lon: float, shape: RailShape) -> float:
    clamped_lat = min(max(lat, shape.min_lat), shape.max_lat)
    clamped_lon = min(max(lon, shape.min_lon), shape.max_lon)
    return haversine_m(lat, lon, clamped_lat, clamped_lon)


def load_rail_shapes_from_gtfs(gtfs_zip: Path) -> List[RailShape]:
    with zipfile.ZipFile(gtfs_zip, "r") as zf:
        names = {name.lower(): name for name in zf.namelist()}
        if "shapes.txt" not in names:
            raise ValueError("shapes.txt not found in GTFS ZIP")

        shapes = pd.read_csv(
            zf.open(names["shapes.txt"]),
            usecols=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
            dtype={"shape_id": str},
        )
        shapes["shape_pt_lat"] = pd.to_numeric(shapes["shape_pt_lat"], errors="coerce")
        shapes["shape_pt_lon"] = pd.to_numeric(shapes["shape_pt_lon"], errors="coerce")
        shapes["shape_pt_sequence"] = pd.to_numeric(shapes["shape_pt_sequence"], errors="coerce")
        shapes = shapes.dropna(subset=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"])

        route_type_by_shape: Dict[str, Optional[int]] = {}
        if "trips.txt" in names and "routes.txt" in names:
            trips = pd.read_csv(
                zf.open(names["trips.txt"]),
                usecols=["route_id", "shape_id"],
                dtype={"route_id": str, "shape_id": str},
            )
            routes = pd.read_csv(
                zf.open(names["routes.txt"]),
                usecols=["route_id", "route_type"],
                dtype={"route_id": str},
            )
            routes["route_type"] = routes["route_type"].apply(parse_route_type)
            tr = trips.merge(routes, on="route_id", how="left")
            tr = tr.dropna(subset=["shape_id"])
            grouped = tr.groupby("shape_id")["route_type"].agg(
                lambda x: x.dropna().mode().iloc[0] if not x.dropna().empty else np.nan
            )
            route_type_by_shape = {
                str(k): parse_route_type(v) for k, v in grouped.to_dict().items()
            }

    rail_shapes: List[RailShape] = []
    for shape_id, group in shapes.groupby("shape_id", sort=False):
        group = group.sort_values("shape_pt_sequence")
        coords = list(zip(group["shape_pt_lat"].astype(float), group["shape_pt_lon"].astype(float)))
        if len(coords) < 2:
            continue
        compact = [coords[0]]
        for p in coords[1:]:
            if haversine_m(compact[-1][0], compact[-1][1], p[0], p[1]) > 0.5:
                compact.append(p)
        if len(compact) < 2:
            continue
        route_type = route_type_by_shape.get(str(shape_id))
        if route_type is not None and not is_rail_route_type(route_type):
            continue
        lats = [p[0] for p in compact]
        lons = [p[1] for p in compact]
        rail_shapes.append(
            RailShape(
                shape_id=str(shape_id),
                route_type=route_type,
                coords_latlon=compact,
                cumdist=cumulative_lengths(compact),
                min_lat=float(np.min(lats)),
                max_lat=float(np.max(lats)),
                min_lon=float(np.min(lons)),
                max_lon=float(np.max(lons)),
            )
        )
    return rail_shapes


def get_rail_shapes(google_csv: Optional[str] = None) -> List[RailShape]:
    global _RAIL_SHAPES_CACHE, _RAIL_SHAPES_SOURCE
    if _RAIL_SHAPES_CACHE is not None:
        return _RAIL_SHAPES_CACHE

    gtfs_zip = resolve_gtfs_zip_path(google_csv=google_csv)
    if gtfs_zip is None:
        _RAIL_SHAPES_CACHE = []
        print("[INFO] No GTFS rail ZIP found; rail modes will use fallback interpolation.")
        return _RAIL_SHAPES_CACHE

    try:
        shapes = load_rail_shapes_from_gtfs(gtfs_zip)
        _RAIL_SHAPES_CACHE = shapes
        _RAIL_SHAPES_SOURCE = str(gtfs_zip)
        print(f"[INFO] Loaded {len(shapes):,} rail shapes from GTFS: {gtfs_zip}")
    except Exception as exc:
        print(f"[WARN] Failed to load GTFS rail shapes from {gtfs_zip}: {exc}")
        _RAIL_SHAPES_CACHE = []
    return _RAIL_SHAPES_CACHE


# ============================================================
# OFFICIAL BERLIN NETWORK SOURCE HELPERS (WFS/OSM)
# ============================================================
def _wfs_capabilities_url(base_url: str) -> str:
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}service=WFS&request=GetCapabilities"


def _wfs_getfeature_url(
    base_url: str,
    typename: str,
    count: Optional[int] = None,
    start_index: Optional[int] = None,
    srs_name: str = WFS_PREFER_EPSG,
) -> str:
    sep = "&" if "?" in base_url else "?"
    params = [
        "service=WFS",
        "version=2.0.0",
        "request=GetFeature",
        f"typeNames={typename}",
        "outputFormat=application/json",
        f"srsName={srs_name}",
    ]
    if count is not None:
        params.append(f"count={int(count)}")
    if start_index is not None:
        params.append(f"startIndex={int(start_index)}")
    return f"{base_url}{sep}{'&'.join(params)}"


def _parse_wfs_capabilities(base_url: str) -> List[Dict[str, str]]:
    resp = _http_get(_wfs_capabilities_url(base_url), timeout=SOURCE_HTTP_TIMEOUT, stream=False)
    root = ET.fromstring(resp.content)
    ns = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "ows": "http://www.opengis.net/ows/1.1",
    }
    out: List[Dict[str, str]] = []
    for ft in root.findall(".//wfs:FeatureType", ns):
        name = ft.findtext("wfs:Name", default="", namespaces=ns) or ""
        title = ft.findtext("wfs:Title", default="", namespaces=ns) or ""
        abstract = ft.findtext("wfs:Abstract", default="", namespaces=ns) or ""
        default_crs = ft.findtext("wfs:DefaultCRS", default="", namespaces=ns) or ""
        if not default_crs:
            default_crs = ft.findtext("DefaultCRS", default="") or ""
        if name:
            out.append({
                "name": name,
                "title": title,
                "abstract": abstract,
                "default_crs": default_crs,
            })
    return out


def _score_typename(meta: Dict[str, str], keywords: List[str]) -> int:
    text = " ".join([
        meta.get("name", ""),
        meta.get("title", ""),
        meta.get("abstract", ""),
    ]).lower()
    score = 0
    for kw in keywords:
        if kw.lower() in text:
            score += 10
    return score


def discover_wfs_typename(base_url: str, logical_name: str) -> Optional[str]:
    metas = _parse_wfs_capabilities(base_url)
    if not metas:
        return None
    keywords = WFS_TYPENAME_KEYWORDS.get(logical_name, [])
    scored = sorted(
        ((_score_typename(meta, keywords), meta["name"]) for meta in metas),
        key=lambda x: (-x[0], x[1]),
    )
    if scored and scored[0][0] > 0:
        return scored[0][1]
    return metas[0]["name"]


def ensure_wfs_geojson(base_url: str, logical_name: str, force_refresh: bool = False) -> Optional[Path]:
    cache_dir = get_source_cache_dir() / "wfs"
    cache_dir.mkdir(parents=True, exist_ok=True)
    out_path = cache_dir / f"{logical_name}.geojson"
    if out_path.exists() and not force_refresh and _source_file_is_fresh(out_path, SOURCE_MAX_AGE_HOURS_WFS):
        return out_path
    try:
        typename = discover_wfs_typename(base_url, logical_name)
        if not typename:
            raise RuntimeError(f"No WFS typename discovered for '{logical_name}'")
        url = _wfs_getfeature_url(base_url=base_url, typename=typename, srs_name=WFS_PREFER_EPSG)
        data = _http_get(url, timeout=SOURCE_HTTP_TIMEOUT, stream=False).json()
        if not (isinstance(data, dict) and "features" in data):
            raise RuntimeError("Unexpected WFS payload")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        return out_path
    except Exception as exc:
        print(f"[WARN] Could not fetch WFS layer '{logical_name}' from {base_url}: {exc}")
        return None


def load_cached_vector(path: Optional[Path]):
    if path is None or not Path(path).exists() or gpd is None:
        return None
    try:
        return gpd.read_file(path)
    except Exception as exc:
        print(f"[WARN] Could not read vector layer {path}: {exc}")
        return None


def get_berlin_official_network_paths(force_refresh: bool = False) -> Dict[str, Optional[Path]]:
    refresh = force_refresh or WFS_FORCE_REFRESH
    return {
        "detailnetz": ensure_wfs_geojson(BERLIN_DETAILNETZ_WFS, "detailnetz", force_refresh=refresh),
        "radnetz": ensure_wfs_geojson(BERLIN_RADNETZ_WFS, "radnetz", force_refresh=refresh),
        "footnetz": ensure_wfs_geojson(BERLIN_FOOTNET_WFS, "footnetz", force_refresh=refresh),
        "bvg_oepnv": ensure_wfs_geojson(BERLIN_BVG_UNDISTURBED_WFS, "bvg_oepnv", force_refresh=refresh),
    }


def load_berlin_official_networks(force_refresh: bool = False) -> Dict[str, object]:
    paths = get_berlin_official_network_paths(force_refresh=force_refresh)
    return {k: load_cached_vector(v) for k, v in paths.items()}


def ensure_geofabrik_berlin_pbf(force_refresh: bool = False) -> Optional[Path]:
    if not AUTO_DOWNLOAD_OSM_BERLIN:
        return None
    target = get_source_cache_dir() / "osm" / "berlin-latest.osm.pbf"
    try:
        return _download_to_path(
            url=GEOFABRIK_BERLIN_PBF_URL,
            target_path=target,
            max_age_hours=SOURCE_MAX_AGE_HOURS_OSM,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        print(f"[WARN] Could not download Geofabrik Berlin PBF: {exc}")
        return None


def ensure_geofabrik_berlin_gpkg(force_refresh: bool = False) -> Optional[Path]:
    if not AUTO_DOWNLOAD_OSM_BERLIN:
        return None

    cache_dir = get_source_cache_dir() / "osm"
    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_target = cache_dir / "berlin-latest-free.gpkg.zip"
    gpkg_target = cache_dir / "berlin-latest-free.gpkg"
    if gpkg_target.exists() and not force_refresh and _source_file_is_fresh(gpkg_target, SOURCE_MAX_AGE_HOURS_OSM):
        return gpkg_target

    try:
        zip_path = _download_to_path(
            url=GEOFABRIK_BERLIN_GPKG_ZIP_URL,
            target_path=zip_target,
            max_age_hours=SOURCE_MAX_AGE_HOURS_OSM,
            force_refresh=force_refresh,
        )
        with zipfile.ZipFile(zip_path, "r") as zf:
            gpkg_names = [name for name in zf.namelist() if name.lower().endswith(".gpkg")]
            if not gpkg_names:
                raise RuntimeError("No .gpkg found inside Geofabrik ZIP")
            tmp_dir = cache_dir / "_tmp_extract_gpkg"
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
            tmp_dir.mkdir(parents=True, exist_ok=True)
            zf.extract(gpkg_names[0], path=tmp_dir)
            extracted = tmp_dir / gpkg_names[0]
            shutil.move(str(extracted), str(gpkg_target))
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return gpkg_target
    except Exception as exc:
        print(f"[WARN] Could not prepare Geofabrik Berlin GPKG: {exc}")
        return None


def get_source_manifest(force_refresh: bool = False) -> Dict[str, Dict[str, object]]:
    gtfs_path = ensure_vbb_gtfs_zip(force_refresh=force_refresh, google_csv=GOOGLE_TIMELINE_CSV)
    wfs_paths = get_berlin_official_network_paths(force_refresh=force_refresh)
    osm_pbf = ensure_geofabrik_berlin_pbf(force_refresh=force_refresh)
    osm_gpkg = ensure_geofabrik_berlin_gpkg(force_refresh=force_refresh)
    return {
        "vbb_gtfs": {
            "url": VBB_GTFS_STATIC_URL,
            "local_path": str(gtfs_path) if gtfs_path else None,
            "license": "CC BY 4.0 (verify provider metadata)",
            "provider": "VBB",
        },
        "detailnetz": {
            "url": BERLIN_DETAILNETZ_WFS,
            "local_path": str(wfs_paths.get("detailnetz")) if wfs_paths.get("detailnetz") else None,
            "license": "dl-de-zero-2.0",
            "provider": "Land Berlin",
        },
        "radnetz": {
            "url": BERLIN_RADNETZ_WFS,
            "local_path": str(wfs_paths.get("radnetz")) if wfs_paths.get("radnetz") else None,
            "license": "dl-de-zero-2.0",
            "provider": "Land Berlin",
        },
        "footnetz": {
            "url": BERLIN_FOOTNET_WFS,
            "local_path": str(wfs_paths.get("footnetz")) if wfs_paths.get("footnetz") else None,
            "license": "dl-de-zero-2.0",
            "provider": "Land Berlin",
        },
        "bvg_oepnv": {
            "url": BERLIN_BVG_UNDISTURBED_WFS,
            "local_path": str(wfs_paths.get("bvg_oepnv")) if wfs_paths.get("bvg_oepnv") else None,
            "license": "dl-de-zero-2.0",
            "provider": "Land Berlin / BVG",
        },
        "osm_berlin_pbf": {
            "url": GEOFABRIK_BERLIN_PBF_URL,
            "local_path": str(osm_pbf) if osm_pbf else None,
            "license": "ODbL 1.0",
            "provider": "Geofabrik / OpenStreetMap",
        },
        "osm_berlin_gpkg": {
            "url": GEOFABRIK_BERLIN_GPKG_ZIP_URL,
            "local_path": str(osm_gpkg) if osm_gpkg else None,
            "license": "ODbL 1.0",
            "provider": "Geofabrik / OpenStreetMap",
        },
    }


def maybe_prepare_official_sources(force_refresh: bool = False) -> Dict[str, Dict[str, object]]:
    manifest = get_source_manifest(force_refresh=force_refresh)
    gtfs_path = manifest.get("vbb_gtfs", {}).get("local_path")
    if gtfs_path:
        print(f"[INFO] VBB GTFS ready: {gtfs_path}")
    for key in ["detailnetz", "radnetz", "footnetz", "bvg_oepnv"]:
        p = manifest.get(key, {}).get("local_path")
        if p:
            print(f"[INFO] {key} cached: {p}")
    return manifest


# ============================================================
# GTFS STATION GRAPH (for station-gap shortest-path routing)
# ============================================================
def _clean_gtfs_text(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    text = str(value).strip()
    return text or None


def _add_station_edge(
    adj: Dict[str, Dict[str, Dict[str, object]]],
    a: str,
    b: str,
    distance_m: float,
    route_type: Optional[int],
) -> None:
    for src, dst in ((a, b), (b, a)):
        nbrs = adj.setdefault(src, {})
        edge = nbrs.get(dst)
        if edge is None:
            nbrs[dst] = {
                "weight_m": float(distance_m),
                "route_types": set() if route_type is None else {int(route_type)},
            }
        else:
            edge["weight_m"] = min(float(edge.get("weight_m", distance_m)), float(distance_m))
            if route_type is not None:
                edge.setdefault("route_types", set()).add(int(route_type))


def load_gtfs_station_graph(gtfs_zip: Path) -> Dict[str, object]:
    with zipfile.ZipFile(gtfs_zip, "r") as zf:
        names = {name.lower(): name for name in zf.namelist()}
        required = {"stops.txt", "trips.txt", "routes.txt", "stop_times.txt"}
        missing = [name for name in required if name not in names]
        if missing:
            raise ValueError(f"Missing GTFS files for station graph: {', '.join(missing)}")

        stops = pd.read_csv(
            zf.open(names["stops.txt"]),
            usecols=lambda c: c in {"stop_id", "stop_name", "stop_lat", "stop_lon", "parent_station"},
            dtype=str,
        )
        stops["stop_lat"] = pd.to_numeric(stops.get("stop_lat"), errors="coerce")
        stops["stop_lon"] = pd.to_numeric(stops.get("stop_lon"), errors="coerce")
        stops = stops.dropna(subset=["stop_id", "stop_lat", "stop_lon"]).copy()
        stops["stop_id"] = stops["stop_id"].astype(str)
        stops["stop_name"] = stops.get("stop_name", pd.Series(index=stops.index, dtype=object)).fillna("").astype(str)
        stops["parent_station"] = stops.get(
            "parent_station", pd.Series(index=stops.index, dtype=object)
        ).map(_clean_gtfs_text)
        stops["station_id"] = np.where(stops["parent_station"].notna(), stops["parent_station"], stops["stop_id"])

        stop_to_station: Dict[str, str] = {
            str(stop_id): str(station_id)
            for stop_id, station_id in zip(stops["stop_id"], stops["station_id"])
        }

        station_rows = stops.groupby("station_id", as_index=False).agg(
            station_lat=("stop_lat", "mean"),
            station_lon=("stop_lon", "mean"),
            station_name=("stop_name", lambda s: next((x for x in s if str(x).strip()), "station")),
        )
        station_rows["station_id"] = station_rows["station_id"].astype(str)
        stations: Dict[str, Dict[str, object]] = {
            str(row["station_id"]): {
                "lat": float(row["station_lat"]),
                "lon": float(row["station_lon"]),
                "name": str(row["station_name"]),
            }
            for _, row in station_rows.iterrows()
        }

        routes = pd.read_csv(
            zf.open(names["routes.txt"]),
            usecols=lambda c: c in {"route_id", "route_type"},
            dtype=str,
        )
        routes["route_type"] = routes.get("route_type").apply(parse_route_type)
        trips = pd.read_csv(
            zf.open(names["trips.txt"]),
            usecols=lambda c: c in {"trip_id", "route_id"},
            dtype=str,
        )
        trip_meta = trips.merge(routes[["route_id", "route_type"]], on="route_id", how="left")

        stop_times = pd.read_csv(
            zf.open(names["stop_times.txt"]),
            usecols=lambda c: c in {"trip_id", "stop_id", "stop_sequence"},
            dtype=str,
        )
        stop_times["stop_sequence"] = pd.to_numeric(stop_times.get("stop_sequence"), errors="coerce")
        stop_times = stop_times.dropna(subset=["trip_id", "stop_id", "stop_sequence"]).copy()
        stop_times["trip_id"] = stop_times["trip_id"].astype(str)
        stop_times["stop_id"] = stop_times["stop_id"].astype(str)
        stop_times["station_id"] = stop_times["stop_id"].map(stop_to_station)
        stop_times = stop_times[stop_times["station_id"].notna()].copy()

    st = stop_times.merge(trip_meta[["trip_id", "route_type"]], on="trip_id", how="left")
    st["route_type"] = st["route_type"].apply(parse_route_type)
    st = st.sort_values(["trip_id", "stop_sequence"])

    adj: Dict[str, Dict[str, Dict[str, object]]] = {}
    for _, grp in st.groupby("trip_id", sort=False):
        grp = grp.sort_values("stop_sequence")
        route_types = grp["route_type"].dropna().tolist()
        route_type = parse_route_type(route_types[0]) if route_types else None
        if not is_rail_route_type(route_type):
            continue
        station_ids = grp["station_id"].astype(str).tolist()
        for a, b in zip(station_ids[:-1], station_ids[1:]):
            if a == b or a not in stations or b not in stations:
                continue
            distance_m = haversine_m(
                float(stations[a]["lat"]), float(stations[a]["lon"]),
                float(stations[b]["lat"]), float(stations[b]["lon"]),
            )
            if not np.isfinite(distance_m) or distance_m <= 0:
                continue
            _add_station_edge(adj, a, b, float(distance_m), route_type)

    return {
        "stations": stations,
        "adj": adj,
    }


def get_gtfs_station_graph(google_csv: Optional[str] = None) -> Optional[Dict[str, object]]:
    global _GTFS_STATION_GRAPH_CACHE, _GTFS_STATION_GRAPH_SOURCE
    if _GTFS_STATION_GRAPH_CACHE is not None:
        return _GTFS_STATION_GRAPH_CACHE

    gtfs_zip = resolve_gtfs_zip_path(google_csv=google_csv)
    if gtfs_zip is None:
        return None

    try:
        graph = load_gtfs_station_graph(gtfs_zip)
        _GTFS_STATION_GRAPH_CACHE = graph
        _GTFS_STATION_GRAPH_SOURCE = str(gtfs_zip)
        print(
            f"[INFO] Loaded GTFS station graph: {len(graph.get('stations', {})):,} stations, "
            f"{sum(len(v) for v in graph.get('adj', {}).values()) // 2:,} edges."
        )
    except Exception as exc:
        print(f"[WARN] Failed to load GTFS station graph from {gtfs_zip}: {exc}")
        _GTFS_STATION_GRAPH_CACHE = None
    return _GTFS_STATION_GRAPH_CACHE


def _edge_supports_mode(edge: Dict[str, object], mode: Optional[str]) -> bool:
    route_types = edge.get("route_types", set())
    if not route_types:
        return True
    for route_type in route_types:
        if route_type_matches_mode(parse_route_type(route_type), mode):
            return True
    return False


def _station_has_mode_edge(
    adj: Dict[str, Dict[str, Dict[str, object]]],
    station_id: str,
    mode: Optional[str],
) -> bool:
    for edge in adj.get(station_id, {}).values():
        if _edge_supports_mode(edge, mode):
            return True
    return False


def _nearest_station_candidates(
    lat: float,
    lon: float,
    graph: Dict[str, object],
    mode: Optional[str],
    max_dist_m: float,
    max_candidates: int,
) -> List[Tuple[float, str]]:
    global _STATION_CANDIDATE_CACHE
    cache_key = (
        round(float(lat), 4),
        round(float(lon), 4),
        str(mode),
        int(round(float(max_dist_m))),
        int(max_candidates),
    )
    cached = _STATION_CANDIDATE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    stations = graph.get("stations", {})
    adj = graph.get("adj", {})
    out: List[Tuple[float, str]] = []
    for station_id, meta in stations.items():
        if station_id not in adj:
            continue
        if mode and not _station_has_mode_edge(adj, station_id, mode):
            continue
        dist = haversine_m(lat, lon, float(meta["lat"]), float(meta["lon"]))
        if dist <= max_dist_m:
            out.append((float(dist), str(station_id)))
    out.sort(key=lambda x: x[0])
    result = out[:max(1, int(max_candidates))]
    _STATION_CANDIDATE_CACHE[cache_key] = result
    return result


def _shortest_station_path(
    graph: Dict[str, object],
    start_station_id: str,
    end_station_id: str,
    mode: Optional[str],
) -> Optional[Dict[str, object]]:
    adj: Dict[str, Dict[str, Dict[str, object]]] = graph.get("adj", {})
    if start_station_id not in adj or end_station_id not in adj:
        return None

    dist: Dict[str, float] = {start_station_id: 0.0}
    prev: Dict[str, str] = {}
    heap: List[Tuple[float, str]] = [(0.0, start_station_id)]

    while heap:
        cur_dist, u = heapq.heappop(heap)
        if cur_dist != dist.get(u):
            continue
        if u == end_station_id:
            break
        for v, edge in adj.get(u, {}).items():
            if mode and not _edge_supports_mode(edge, mode):
                continue
            weight_m = float(edge.get("weight_m", np.nan))
            if not np.isfinite(weight_m) or weight_m <= 0:
                continue
            nd = cur_dist + weight_m
            if nd + 1e-9 < dist.get(v, float("inf")):
                dist[v] = nd
                prev[v] = u
                heapq.heappush(heap, (nd, v))

    if end_station_id not in dist:
        return None

    station_ids = [end_station_id]
    while station_ids[-1] != start_station_id:
        station_ids.append(prev[station_ids[-1]])
    station_ids.reverse()
    return {
        "station_ids": station_ids,
        "distance_m": float(dist[end_station_id]),
    }


def build_rail_path_from_shapes(
    segment: SegmentSpec,
    anchor_coords: List[Tuple[float, float]],
    rail_shapes: List[RailShape],
    endpoint_snap_m: float = MAX_RAIL_ENDPOINT_SNAP_M,
    mean_snap_m: float = MAX_RAIL_MEAN_SNAP_M,
    timeline_point_snap_m: float = MAX_RAIL_TIMELINE_POINT_SNAP_M,
    max_candidates: int = MAX_RAIL_SHAPE_CANDIDATES,
    strict_mode_filter: bool = True,
) -> Optional[Dict]:
    if not rail_shapes or len(anchor_coords) < 2:
        return None

    mode = normalize_mode(segment.mode)
    start_lat, start_lon = anchor_coords[0]
    end_lat, end_lon = anchor_coords[-1]
    bbox_limit = endpoint_snap_m * RAIL_BBOX_PREFILTER_FACTOR

    candidate_scores: List[Tuple[float, RailShape]] = []
    for shape in rail_shapes:
        if strict_mode_filter and not route_type_matches_mode(shape.route_type, mode):
            continue
        d0 = point_bbox_distance_m(start_lat, start_lon, shape)
        d1 = point_bbox_distance_m(end_lat, end_lon, shape)
        if d0 > bbox_limit or d1 > bbox_limit:
            continue
        candidate_scores.append((d0 + d1, shape))

    if not candidate_scores:
        return None
    candidate_scores.sort(key=lambda x: x[0])
    shortlisted = [s for _, s in candidate_scores[:max(1, int(max_candidates))]]

    best = None
    best_score = float("inf")

    for shape in shortlisted:
        coords = shape.coords_latlon
        if len(coords) < 2:
            continue
        cumdist = shape.cumdist
        projections = [
            project_point_to_polyline(lat, lon, coords, cumdist) for lat, lon in anchor_coords
        ]
        dists = np.array([float(p["distance_m"]) for p in projections], dtype=float)
        if np.any(~np.isfinite(dists)):
            continue
        if dists[0] > endpoint_snap_m or dists[-1] > endpoint_snap_m:
            continue
        if float(np.nanmean(dists)) > mean_snap_m:
            continue

        chainages = [float(p["chainage_m"]) for p in projections]
        inversions = 0
        for a, b in zip(chainages[:-1], chainages[1:]):
            if b + BACKTRACK_TOLERANCE_M < a:
                inversions += 1

        timeline_penalty = float(np.mean(np.minimum(dists, timeline_point_snap_m)))
        score = timeline_penalty + (200.0 * inversions)
        if score < best_score:
            best_score = score
            best = {
                "shape": shape,
                "cumdist": cumdist,
                "projections": projections,
            }

    if best is None:
        return None

    coords = best["shape"].coords_latlon
    cumdist = best["cumdist"]
    projections = best["projections"]
    start_chainage = float(projections[0]["chainage_m"])
    end_chainage = float(projections[-1]["chainage_m"])

    sub = extract_subpolyline(coords, cumdist, start_chainage, end_chainage)
    if end_chainage < start_chainage:
        sub = list(reversed(sub))
    if len(sub) < 2:
        return None

    out_coords = [(float(projections[0]["lat"]), float(projections[0]["lon"]))]
    for p in sub:
        if haversine_m(out_coords[-1][0], out_coords[-1][1], p[0], p[1]) > 0.5:
            out_coords.append(p)
    end_proj = (float(projections[-1]["lat"]), float(projections[-1]["lon"]))
    if haversine_m(out_coords[-1][0], out_coords[-1][1], end_proj[0], end_proj[1]) > 0.5:
        out_coords.append(end_proj)

    if len(out_coords) < 2:
        return None

    return {
        "coords_latlon": out_coords,
        "distance_m": float(cumulative_lengths(out_coords)[-1]),
        "duration_s": float((segment.end_t - segment.start_t).total_seconds()),
        "method": f"gtfs_rail_shape_{best['shape'].shape_id}",
    }


def build_pairwise_rail_piece(
    segment: SegmentSpec,
    a: Tuple[float, float],
    b: Tuple[float, float],
    rail_shapes: List[RailShape],
) -> Optional[Dict]:
    # Pass 1: mode-consistent rail family.
    piece = build_rail_path_from_shapes(
        segment=segment,
        anchor_coords=[a, b],
        rail_shapes=rail_shapes,
        endpoint_snap_m=RAIL_PAIRWISE_ENDPOINT_SNAP_M,
        mean_snap_m=RAIL_PAIRWISE_MEAN_SNAP_M,
        timeline_point_snap_m=RAIL_PAIRWISE_TIMELINE_POINT_SNAP_M,
        max_candidates=RAIL_PAIRWISE_MAX_CANDIDATES,
        strict_mode_filter=True,
    )
    if piece is not None:
        piece["method"] = f"{piece['method']}::pairwise_mode"
        return piece

    # Pass 2: relaxed family filter (still rail-only shapes), useful for noisy labels.
    piece = build_rail_path_from_shapes(
        segment=segment,
        anchor_coords=[a, b],
        rail_shapes=rail_shapes,
        endpoint_snap_m=RAIL_PAIRWISE_ENDPOINT_SNAP_M,
        mean_snap_m=RAIL_PAIRWISE_MEAN_SNAP_M,
        timeline_point_snap_m=RAIL_PAIRWISE_TIMELINE_POINT_SNAP_M,
        max_candidates=RAIL_PAIRWISE_MAX_CANDIDATES,
        strict_mode_filter=False,
    )
    if piece is not None:
        piece["method"] = f"{piece['method']}::pairwise_relaxed"
    return piece


def build_station_shortest_rail_path(
    segment: SegmentSpec,
    anchor_coords: List[Tuple[float, float]],
    rail_shapes: List[RailShape],
) -> Optional[Dict]:
    if not ENABLE_STATION_SHORTEST_RAIL_PATH:
        return None
    if not rail_shapes or len(anchor_coords) < 2:
        return None

    mode = normalize_mode(segment.mode)
    if mode not in STATION_PATH_MODES:
        return None

    # Trigger for sparse rail gaps only (signal vanished between known points).
    if len(anchor_coords) > STATION_GAP_MAX_TIMELINE_POINTS:
        return None
    segment_duration_s = float((segment.end_t - segment.start_t).total_seconds())
    if segment_duration_s < STATION_PATH_MIN_DURATION_S:
        return None

    graph = get_gtfs_station_graph(google_csv=GOOGLE_TIMELINE_CSV)
    if not graph:
        return None

    start_anchor = anchor_coords[0]
    end_anchor = anchor_coords[-1]
    stations: Dict[str, Dict[str, object]] = graph.get("stations", {})

    if haversine_m(start_anchor[0], start_anchor[1], end_anchor[0], end_anchor[1]) <= STATION_DIRECT_SAME_STATION_M:
        return None

    start_candidates = _nearest_station_candidates(
        lat=start_anchor[0],
        lon=start_anchor[1],
        graph=graph,
        mode=mode,
        max_dist_m=STATION_ENDPOINT_SNAP_M,
        max_candidates=STATION_CANDIDATES_PER_SIDE,
    )
    end_candidates = _nearest_station_candidates(
        lat=end_anchor[0],
        lon=end_anchor[1],
        graph=graph,
        mode=mode,
        max_dist_m=STATION_ENDPOINT_SNAP_M,
        max_candidates=STATION_CANDIDATES_PER_SIDE,
    )
    if not start_candidates or not end_candidates:
        return None

    best = None
    for start_dist, start_station in start_candidates:
        for end_dist, end_station in end_candidates:
            if start_station == end_station:
                continue
            station_path = _shortest_station_path(
                graph=graph,
                start_station_id=start_station,
                end_station_id=end_station,
                mode=mode,
            )
            if station_path is None:
                continue
            total_cost = float(start_dist + station_path["distance_m"] + end_dist)
            if best is None or total_cost < best["cost_m"]:
                best = {
                    "cost_m": total_cost,
                    "start_station": start_station,
                    "end_station": end_station,
                    "station_ids": station_path["station_ids"],
                    "station_path_m": float(station_path["distance_m"]),
                }

    if best is None:
        return None

    def append_coord(coords: List[Tuple[float, float]], pt: Tuple[float, float], eps_m: float = 0.5) -> None:
        if not coords:
            coords.append((float(pt[0]), float(pt[1])))
            return
        if haversine_m(coords[-1][0], coords[-1][1], float(pt[0]), float(pt[1])) > eps_m:
            coords.append((float(pt[0]), float(pt[1])))

    coords_out: List[Tuple[float, float]] = []
    append_coord(coords_out, start_anchor)

    start_station_meta = stations.get(best["start_station"])
    end_station_meta = stations.get(best["end_station"])
    if start_station_meta is None or end_station_meta is None:
        return None

    start_station_pt = (float(start_station_meta["lat"]), float(start_station_meta["lon"]))
    end_station_pt = (float(end_station_meta["lat"]), float(end_station_meta["lon"]))
    append_coord(coords_out, start_station_pt)

    piece_methods: List[str] = []
    station_ids = best["station_ids"]
    for a_station, b_station in zip(station_ids[:-1], station_ids[1:]):
        a_meta = stations.get(a_station)
        b_meta = stations.get(b_station)
        if a_meta is None or b_meta is None:
            continue
        a_pt = (float(a_meta["lat"]), float(a_meta["lon"]))
        b_pt = (float(b_meta["lat"]), float(b_meta["lon"]))
        piece = build_pairwise_rail_piece(segment, a_pt, b_pt, rail_shapes)
        if piece is None:
            piece = build_path_for_points(segment.mode, [a_pt, b_pt])
            piece_method = f"{piece.get('method', 'direct_polyline')}::station_pair_fallback"
        else:
            piece_method = f"{piece.get('method', 'unknown')}::station_pair"
        piece_methods.append(piece_method)
        for pt in piece.get("coords_latlon", []):
            append_coord(coords_out, pt)

    append_coord(coords_out, end_station_pt)
    append_coord(coords_out, end_anchor)

    if len(coords_out) < 2:
        return None

    method_suffix = "station_shortest"
    if piece_methods:
        unique_methods = sorted(set(piece_methods))
        method_suffix = "+".join(unique_methods[:4]) if len(unique_methods) <= 4 else "station_shortest_mixed"
    return {
        "coords_latlon": coords_out,
        "distance_m": float(cumulative_lengths(coords_out)[-1]),
        "duration_s": float((segment.end_t - segment.start_t).total_seconds()),
        "method": f"gtfs_station_shortest::{method_suffix}",
    }


# ============================================================
# PATH BUILDING
# ============================================================
def fallback_line(start_lat: float, start_lon: float, end_lat: float, end_lon: float) -> Dict:
    coords = [(start_lat, start_lon), (end_lat, end_lon)]
    return {
        "coords_latlon": coords,
        "distance_m": haversine_m(start_lat, start_lon, end_lat, end_lon),
        "duration_s": np.nan,
        "method": "direct_line",
    }


def build_path_for_points(mode: Optional[str], coords_latlon: List[Tuple[float, float]]) -> Dict:
    if len(coords_latlon) == 0:
        return {"coords_latlon": [], "distance_m": 0.0, "duration_s": np.nan, "method": "empty"}
    if len(coords_latlon) == 1:
        return {"coords_latlon": coords_latlon, "distance_m": 0.0, "duration_s": 0.0, "method": "single"}

    profile = get_osrm_profile(mode)
    if profile is not None:
        routed = osrm_route(profile, coords_latlon)
        if routed is not None:
            return routed

    # Direct-line fallback, segment by segment if there are intermediate anchors.
    out = [coords_latlon[0]]
    for pt in coords_latlon[1:]:
        if haversine_m(out[-1][0], out[-1][1], pt[0], pt[1]) > 0.05:
            out.append(pt)
    return {
        "coords_latlon": out,
        "distance_m": float(cumulative_lengths(out)[-1]),
        "duration_s": np.nan,
        "method": "direct_polyline",
    }


def build_baseline_path(
    segment: SegmentSpec,
    rail_shapes: Optional[List[RailShape]] = None,
    uni_route_shapes: Optional[Dict[str, List[Tuple[float, float]]]] = None,
    uni_classification_lookup: Optional[pd.DataFrame] = None,
    training_motion_model: Optional[Dict] = None,
    opnv_mode_profiles: Optional[Dict[str, Dict]] = None,
) -> Dict:
    if segment.kind == "visit_stationary":
        coords = [(segment.start_lat, segment.start_lon)]
        return {
            "coords_latlon": coords,
            "distance_m": 0.0,
            "duration_s": float((segment.end_t - segment.start_t).total_seconds()),
            "method": "stationary",
            "route_class": None,
            "route_shape_key": None,
            "route_motion_profile": None,
            "route_mode_profile": None,
        }

    anchor_coords = [(p.lat, p.lon) for p in segment.timeline_points]
    if len(anchor_coords) < 2:
        anchor_coords = [(segment.start_lat, segment.start_lon), (segment.end_lat, segment.end_lon)]

    if uni_route_shapes:
        uni_path_info = infer_uni_route_for_segment(
            segment,
            uni_route_shapes,
            classification_lookup=uni_classification_lookup,
        )
        if uni_path_info is not None:
            uni_path = uni_path_info["coords_latlon"]
            dist = float(cumulative_lengths(uni_path)[-1]) if len(uni_path) > 1 else 0.0
            route_class = canonical_uni_route_class(uni_path_info.get("route_class"))
            motion_profile = get_route_motion_profile(route_class, training_motion_model)
            mode_profile = None
            if route_class in {"to_uni_opnv", "from_uni_opnv"} and isinstance(opnv_mode_profiles, dict):
                mode_profile = opnv_mode_profiles.get(route_class)
            selection_method = uni_path_info.get("selection_method", "unknown")
            shape_key = uni_path_info.get("route_shape_key")
            return {
                "coords_latlon": uni_path,
                "distance_m": dist,
                "duration_s": float((segment.end_t - segment.start_t).total_seconds()),
                "method": f"uni_route_shape::{selection_method}",
                "route_class": route_class,
                "route_shape_key": shape_key,
                "route_motion_profile": motion_profile,
                "route_mode_profile": mode_profile,
            }

    if USE_ONLY_UNI_ROUTE_SHAPES:
        # User-requested behavior: do not apply external routing/snap systems.
        out = [anchor_coords[0]]
        for pt in anchor_coords[1:]:
            if haversine_m(out[-1][0], out[-1][1], pt[0], pt[1]) > 0.05:
                out.append(pt)
        if len(out) == 1:
            out.append(out[0])
        return {
            "coords_latlon": out,
            "distance_m": float(cumulative_lengths(out)[-1]) if len(out) > 1 else 0.0,
            "duration_s": float((segment.end_t - segment.start_t).total_seconds()),
            "method": "timeline_polyline_no_route_match",
            "route_class": None,
            "route_shape_key": None,
            "route_motion_profile": None,
            "route_mode_profile": None,
        }

    rail_mode = normalize_mode(segment.mode) in RAIL_TRANSIT_MODES
    if rail_mode and rail_shapes:
        station_shortest = build_station_shortest_rail_path(segment, anchor_coords, rail_shapes)
        if station_shortest is not None:
            return station_shortest
        rail_path = build_rail_path_from_shapes(segment, anchor_coords, rail_shapes)
        if rail_path is not None:
            rail_path["route_class"] = None
            rail_path["route_shape_key"] = None
            rail_path["route_motion_profile"] = None
            rail_path["route_mode_profile"] = None
            return rail_path

    pieces = []
    methods = []
    total_distance = 0.0
    for a, b in zip(anchor_coords[:-1], anchor_coords[1:]):
        piece = None
        if rail_mode and rail_shapes:
            piece = build_pairwise_rail_piece(segment, a, b, rail_shapes)
        if piece is None:
            piece = build_path_for_points(segment.mode, [a, b])
        coords = piece["coords_latlon"]
        if not pieces:
            pieces.extend(coords)
        else:
            pieces.extend(coords[1:])
        total_distance += float(piece.get("distance_m", 0.0) or 0.0)
        methods.append(piece.get("method", "unknown"))

    if not pieces:
        pieces = anchor_coords
    method = methods[0] if methods and len(set(methods)) == 1 else "+".join(sorted(set(methods))) if methods else "unknown"
    return {
        "coords_latlon": pieces,
        "distance_m": total_distance,
        "duration_s": float((segment.end_t - segment.start_t).total_seconds()),
        "method": method,
        "route_class": None,
        "route_shape_key": None,
        "route_motion_profile": None,
        "route_mode_profile": None,
    }


# ============================================================
# SENSOR GPS PARSING
# ============================================================
def infer_google_time_context(google_csv: Optional[str]) -> Optional[Dict]:
    if not google_csv:
        return None
    try:
        df = pd.read_csv(google_csv, usecols=["startTime", "endTime"])
    except Exception:
        return None
    start_series = pd.to_datetime(df.get("startTime"), utc=True, errors="coerce")
    end_series = pd.to_datetime(df.get("endTime"), utc=True, errors="coerce")
    ts = pd.concat([start_series, end_series], ignore_index=True).dropna()
    if ts.empty:
        return None
    local_dates = set(ts.dt.tz_convert(LOCAL_TIMEZONE).dt.date.tolist())
    return {
        "start_utc": ts.min(),
        "end_utc": ts.max(),
        "local_dates": local_dates,
    }


def extract_file_time_hints(path: Path) -> Tuple[List[pd.Timestamp], List[pd.Timestamp]]:
    stem = path.stem
    unix_matches = re.findall(r"(?<!\d)(\d{10}|\d{13})(?!\d)", stem)
    date_matches = re.findall(r"(20\d{2}-\d{2}-\d{2})", stem)

    unix_hints: List[pd.Timestamp] = []
    for token in unix_matches:
        try:
            if len(token) == 13:
                ts = pd.to_datetime(int(token), unit="ms", utc=True, errors="coerce")
            else:
                ts = pd.to_datetime(int(token), unit="s", utc=True, errors="coerce")
            if pd.notna(ts) and 2015 <= ts.year <= 2100:
                unix_hints.append(ts)
        except Exception:
            continue

    date_hints: List[pd.Timestamp] = []
    for token in date_matches:
        try:
            d = pd.Timestamp(token).date()
            date_hints.append(pd.Timestamp(d))
        except Exception:
            continue

    return unix_hints, date_hints


def sensor_file_matches_google_context(path: Path, google_ctx: Optional[Dict]) -> bool:
    if google_ctx is None:
        return True

    unix_hints, date_hints = extract_file_time_hints(path)
    has_any_hint = bool(unix_hints or date_hints)
    if not has_any_hint:
        return True

    start_utc = google_ctx["start_utc"] - pd.Timedelta(hours=SENSOR_FILE_HINT_PADDING_HOURS)
    end_utc = google_ctx["end_utc"] + pd.Timedelta(hours=SENSOR_FILE_HINT_PADDING_HOURS)
    local_dates = google_ctx["local_dates"]

    unix_match = any(start_utc <= ts <= end_utc for ts in unix_hints) if unix_hints else False
    date_match = any(d.date() in local_dates for d in date_hints) if date_hints else False
    return unix_match or date_match


def discover_sensor_csv_files(csv_path: Optional[str], google_csv: Optional[str] = None) -> List[Path]:
    if not csv_path:
        return []

    direct = Path(csv_path).expanduser()
    files: List[Path] = []

    if direct.exists():
        if direct.is_file():
            return [direct.resolve()]
        if direct.is_dir():
            by_glob = sorted([p.resolve() for p in direct.glob(SENSOR_GPS_FILE_GLOB) if p.is_file()])
            if by_glob:
                return by_glob
            by_rglob = sorted([p.resolve() for p in direct.rglob(SENSOR_GPS_FILE_GLOB) if p.is_file()])
            if by_rglob:
                return by_rglob
            return sorted([p.resolve() for p in direct.glob("*.csv") if p.is_file()])

    search_roots: List[Path] = []
    if google_csv:
        g = Path(google_csv).expanduser().resolve()
        search_roots.extend([g.parent, g.parent.parent])
        if len(g.parents) >= 3:
            project_root = g.parents[2]
            search_roots.extend([
                project_root,
                project_root / "newest csvs",
                project_root / "gps_records",
                project_root / "air_records",
            ])

    # File-like unresolved input: try by name in known roots.
    if direct.suffix.lower() == ".csv":
        for root in search_roots:
            cand = (root / direct.name)
            if cand.exists() and cand.is_file():
                files.append(cand.resolve())
        if files:
            return sorted(list({p: None for p in files}.keys()), key=lambda x: str(x))

    # Directory-like unresolved input: search folder by that name and glob.
    if direct.name:
        for root in search_roots:
            cand_dir = root / direct.name
            if cand_dir.exists() and cand_dir.is_dir():
                files.extend([p.resolve() for p in cand_dir.glob(SENSOR_GPS_FILE_GLOB) if p.is_file()])

    # Fallback: search for enviro logs in roots.
    if not files:
        for root in search_roots:
            try:
                files.extend([p.resolve() for p in root.rglob(SENSOR_GPS_FILE_GLOB) if p.is_file()])
            except Exception:
                continue

    dedup_paths = list({p: None for p in files}.keys())

    # Many exports are duplicated across "newest csvs"/archive folders.
    # Keep one file per (name, size) signature to avoid redundant parsing.
    by_signature: Dict[Tuple[str, int], Path] = {}
    for p in sorted(dedup_paths, key=lambda x: str(x)):
        try:
            sig = (p.name, int(p.stat().st_size))
        except Exception:
            sig = (p.name, -1)
        if sig not in by_signature:
            by_signature[sig] = p

    return sorted(by_signature.values(), key=lambda x: str(x))


def infer_sensor_filename_time_context(files: List[Path]) -> Optional[Dict]:
    if not files:
        return None

    unix_hints_all: List[pd.Timestamp] = []
    local_dates: Set[date] = set()
    hinted_files = 0
    for path in files:
        unix_hints, date_hints = extract_file_time_hints(path)
        if unix_hints or date_hints:
            hinted_files += 1
        for ts in unix_hints:
            if pd.notna(ts):
                unix_hints_all.append(ts)
                try:
                    local_dates.add(ts.tz_convert(LOCAL_TIMEZONE).date())
                except Exception:
                    pass
        for d in date_hints:
            try:
                local_dates.add(pd.Timestamp(d).date())
            except Exception:
                continue

    if not unix_hints_all and not local_dates:
        return None

    if unix_hints_all:
        start_utc = min(unix_hints_all) - pd.Timedelta(hours=SENSOR_FILENAME_WINDOW_PADDING_HOURS)
        end_utc = max(unix_hints_all) + pd.Timedelta(hours=SENSOR_FILENAME_WINDOW_PADDING_HOURS)
    else:
        min_date = min(local_dates)
        max_date = max(local_dates)
        start_utc = pd.Timestamp(min_date).tz_localize(LOCAL_TIMEZONE).tz_convert("UTC")
        end_utc = (
            pd.Timestamp(max_date).tz_localize(LOCAL_TIMEZONE).tz_convert("UTC")
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )
        start_utc -= pd.Timedelta(hours=SENSOR_FILENAME_WINDOW_PADDING_HOURS)
        end_utc += pd.Timedelta(hours=SENSOR_FILENAME_WINDOW_PADDING_HOURS)

    if not local_dates:
        cur = start_utc.tz_convert(LOCAL_TIMEZONE).date()
        end_date = end_utc.tz_convert(LOCAL_TIMEZONE).date()
        while cur <= end_date:
            local_dates.add(cur)
            cur = (pd.Timestamp(cur) + pd.Timedelta(days=1)).date()

    return {
        "start_utc": start_utc,
        "end_utc": end_utc,
        "local_dates": local_dates,
        "hinted_files": hinted_files,
        "total_files": len(files),
        "source": "sensor_filename_hints",
    }


def infer_segment_time_context(segments: List[SegmentSpec]) -> Optional[Dict]:
    if not segments:
        return None
    starts = [s.start_t for s in segments if pd.notna(s.start_t)]
    ends = [s.end_t for s in segments if pd.notna(s.end_t)]
    if not starts or not ends:
        return None
    start_utc = min(starts)
    end_utc = max(ends)
    local_dates: Set[date] = set()
    for s in segments:
        try:
            local_dates.add(s.start_t.tz_convert(LOCAL_TIMEZONE).date())
            local_dates.add(s.end_t.tz_convert(LOCAL_TIMEZONE).date())
        except Exception:
            continue
    return {
        "start_utc": start_utc,
        "end_utc": end_utc,
        "local_dates": local_dates,
        "source": "segments",
    }


def overlaps_time_window(
    start_t: pd.Timestamp,
    end_t: pd.Timestamp,
    window_start: Optional[pd.Timestamp],
    window_end: Optional[pd.Timestamp],
) -> bool:
    if pd.isna(start_t) or pd.isna(end_t):
        return False
    if window_start is not None and end_t < window_start:
        return False
    if window_end is not None and start_t > window_end:
        return False
    return True


def parse_sensor_csv_single(path: Path) -> pd.DataFrame:
    wanted = {
        "timestamp_iso",
        "timestamp",
        "gps_time_utc",
        "gps_mode",
        "gps_lat",
        "gps_lon",
        "gps_speed_m_s",
        "gps_track_deg",
        "gps_eph_m",
    }
    try:
        df = pd.read_csv(path, usecols=lambda c: c in wanted)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception:
        # Fallback for unusual CSV dialects/headers.
        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    ts_raw = df.get("timestamp_iso")
    if ts_raw is None:
        ts_raw = df.get("timestamp")
    if ts_raw is None:
        ts_raw = df.get("gps_time_utc")
    ts = pd.to_datetime(ts_raw, utc=True, errors="coerce")
    if not isinstance(ts, pd.Series):
        ts = pd.Series(pd.NaT, index=df.index)

    out = pd.DataFrame({
        "timestamp": ts,
        "gps_mode": pd.to_numeric(df.get("gps_mode", pd.Series(np.nan, index=df.index)), errors="coerce"),
        "gps_lat": pd.to_numeric(df.get("gps_lat", pd.Series(np.nan, index=df.index)), errors="coerce"),
        "gps_lon": pd.to_numeric(df.get("gps_lon", pd.Series(np.nan, index=df.index)), errors="coerce"),
        "gps_speed_m_s": pd.to_numeric(df.get("gps_speed_m_s", pd.Series(np.nan, index=df.index)), errors="coerce"),
        "gps_track_deg": pd.to_numeric(df.get("gps_track_deg", pd.Series(np.nan, index=df.index)), errors="coerce"),
        "gps_eph_m": pd.to_numeric(df.get("gps_eph_m", pd.Series(np.nan, index=df.index)), errors="coerce"),
        "gps_time_utc": pd.to_datetime(df.get("gps_time_utc"), utc=True, errors="coerce"),
        "source_file": str(path),
        "source_name": path.name,
    })
    out = out[out["timestamp"].notna()].copy()
    out = out[out["gps_lat"].notna() & out["gps_lon"].notna()].copy()
    return out


def parse_sensor_timestamps_only(path: Path) -> pd.DataFrame:
    wanted = {"timestamp_iso", "timestamp", "gps_time_utc"}
    try:
        df = pd.read_csv(path, usecols=lambda c: c in wanted)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=["timestamp"])
    except Exception:
        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=["timestamp"])

    ts_raw = df.get("timestamp_iso")
    if ts_raw is None:
        ts_raw = df.get("timestamp")
    if ts_raw is None:
        ts_raw = df.get("gps_time_utc")
    ts = pd.to_datetime(ts_raw, utc=True, errors="coerce")
    if not isinstance(ts, pd.Series):
        ts = pd.Series(pd.NaT, index=df.index)
    out = pd.DataFrame({"timestamp": ts})
    out = out[out["timestamp"].notna()].copy()
    return out


def parse_sensor_air_metrics(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=["timestamp"])
    if df.empty:
        return pd.DataFrame(columns=["timestamp"])

    ts_raw = df.get("timestamp_iso")
    if ts_raw is None:
        ts_raw = df.get("timestamp")
    if ts_raw is None:
        ts_raw = df.get("gps_time_utc")
    if ts_raw is None and "unix_time_s" in df.columns:
        ts_raw = pd.to_datetime(pd.to_numeric(df["unix_time_s"], errors="coerce"), unit="s", utc=True, errors="coerce")
    ts = pd.to_datetime(ts_raw, utc=True, errors="coerce")
    if not isinstance(ts, pd.Series):
        ts = pd.Series(pd.NaT, index=df.index)

    work = df.copy()
    work["timestamp"] = ts
    work = work[work["timestamp"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=["timestamp"])

    excluded = {
        "timestamp",
        "timestamp_iso",
        "gps_time_utc",
        "gps_lat",
        "gps_lon",
        "gps_mode",
        "gps_speed_m_s",
        "gps_track_deg",
        "gps_eph_m",
        "source_file",
        "source_name",
        "fix_id",
    }
    metric_cols = [c for c in work.columns if c not in excluded]
    if not metric_cols:
        return pd.DataFrame(columns=["timestamp"])
    out = work[["timestamp", *metric_cols]].copy()
    out = out.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    return out


def merge_air_metrics_onto_track_rows(track_subset: pd.DataFrame, air_metrics: pd.DataFrame) -> pd.DataFrame:
    if track_subset.empty or air_metrics.empty:
        return track_subset
    if "timestamp" not in track_subset.columns or "timestamp" not in air_metrics.columns:
        return track_subset

    left = track_subset.copy()
    left["timestamp"] = pd.to_datetime(left["timestamp"], utc=True, errors="coerce")
    left["timestamp"] = left["timestamp"].astype("datetime64[ns, UTC]")
    left = left[left["timestamp"].notna()].copy()
    if left.empty:
        return track_subset

    right = air_metrics.copy()
    right["timestamp"] = pd.to_datetime(right["timestamp"], utc=True, errors="coerce")
    right["timestamp"] = right["timestamp"].astype("datetime64[ns, UTC]")
    right = right[right["timestamp"].notna()].copy()
    if right.empty:
        return track_subset

    rename_map: Dict[str, str] = {}
    for c in right.columns:
        if c == "timestamp":
            continue
        if c in left.columns:
            rename_map[c] = f"air_{c}"
    if rename_map:
        right = right.rename(columns=rename_map)

    left = left.sort_values("timestamp").reset_index(drop=True)
    right = right.sort_values("timestamp").reset_index(drop=True)

    left["timestamp_ns"] = left["timestamp"].astype("int64")
    right["timestamp_ns"] = right["timestamp"].astype("int64")
    right = right.drop(columns=["timestamp"])
    merged = pd.merge_asof(left, right, on="timestamp_ns", direction="nearest")
    merged = merged.drop(columns=["timestamp_ns"])
    return merged


def compute_progress_and_speed_from_sensor_points(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "progress_rel", "speed_m_s"])

    work = df[["timestamp", "gps_lat", "gps_lon", "gps_speed_m_s"]].copy()
    work = work.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    if len(work) < 2:
        return pd.DataFrame(columns=["timestamp", "progress_rel", "speed_m_s"])

    lat = pd.to_numeric(work["gps_lat"], errors="coerce").to_numpy(dtype=float)
    lon = pd.to_numeric(work["gps_lon"], errors="coerce").to_numpy(dtype=float)
    ts = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    ts_s = (ts.astype("int64") / 1e9).to_numpy(dtype=float)
    dt = np.diff(ts_s)
    dt = np.where(dt > 0, dt, np.nan)
    step_dist = haversine_m_vectorized(lat[:-1], lon[:-1], lat[1:], lon[1:])
    step_speed = np.divide(step_dist, dt, out=np.full_like(step_dist, np.nan), where=np.isfinite(dt))

    cumulative = np.concatenate(([0.0], np.nancumsum(np.where(np.isfinite(step_dist), step_dist, 0.0))))
    total_dist = float(cumulative[-1]) if len(cumulative) else 0.0
    if total_dist <= 0:
        progress = np.zeros_like(cumulative)
    else:
        progress = cumulative / total_dist

    gps_speed = pd.to_numeric(work["gps_speed_m_s"], errors="coerce").to_numpy(dtype=float)
    speed = np.concatenate(([np.nan], step_speed))
    use_gps = np.isfinite(gps_speed) & (gps_speed >= 0) & (gps_speed <= 35.0)
    speed[use_gps] = gps_speed[use_gps]

    out = pd.DataFrame({
        "timestamp": ts,
        "progress_rel": np.clip(progress, 0.0, 1.0),
        "speed_m_s": speed,
    })
    return out.dropna(subset=["timestamp"]).reset_index(drop=True)


def detect_stop_events_from_progress(
    progress_df: pd.DataFrame,
    stand_speed_threshold_mps: float,
) -> Tuple[List[float], List[float]]:
    if progress_df.empty or len(progress_df) < 3:
        return [], []

    ts = pd.to_datetime(progress_df["timestamp"], utc=True, errors="coerce")
    speed = pd.to_numeric(progress_df["speed_m_s"], errors="coerce").to_numpy(dtype=float)
    rel = pd.to_numeric(progress_df["progress_rel"], errors="coerce").to_numpy(dtype=float)

    valid = np.isfinite(speed) & np.isfinite(rel) & ts.notna().to_numpy()
    if valid.sum() < 3:
        return [], []

    ts_s = (ts.astype("int64") / 1e9).to_numpy(dtype=float)
    dt = np.diff(ts_s)
    dt = dt[np.isfinite(dt) & (dt > 0)]
    median_dt = float(np.nanmedian(dt)) if len(dt) else np.nan
    if not np.isfinite(median_dt) or median_dt <= 0:
        median_dt = 0.25
    window_pts = int(round(5.0 / max(median_dt, 1e-3)))
    window_pts = max(21, min(window_pts, 401))
    if window_pts % 2 == 0:
        window_pts += 1
    smooth_speed = (
        pd.Series(speed)
        .rolling(window=window_pts, min_periods=max(7, window_pts // 6), center=True)
        .median()
        .to_numpy(dtype=float)
    )

    detection_threshold = max(float(stand_speed_threshold_mps), 0.8)
    stand_mask = valid & (
        ((np.isfinite(smooth_speed)) & (smooth_speed <= detection_threshold))
        | (speed <= stand_speed_threshold_mps)
    )

    events_rel: List[float] = []
    events_dwell_s: List[float] = []
    n = len(stand_mask)
    i = 0
    while i < n:
        if not stand_mask[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and stand_mask[j + 1]:
            j += 1
        start_t = ts_s[i]
        end_t = ts_s[j]
        dwell_s = float(max(0.0, end_t - start_t))
        if (
            dwell_s >= TRAINING_INTERSECTION_MIN_STOP_DURATION_S
            and dwell_s <= TRAINING_INTERSECTION_MAX_STOP_DURATION_S
        ):
            rel_window = rel[i:j + 1]
            rel_center = float(np.nanmedian(rel_window))
            # Ignore start/end parking behavior to keep intersection-like events.
            if 0.03 <= rel_center <= 0.97:
                events_rel.append(rel_center)
                events_dwell_s.append(dwell_s)
        i = j + 1

    return events_rel, events_dwell_s


def cluster_stop_events(
    event_rel: List[float],
    event_dwell_s: List[float],
) -> Tuple[List[float], List[float], int]:
    if not event_rel:
        return [], [], 0
    pairs = sorted(
        (float(r), float(d))
        for r, d in zip(event_rel, event_dwell_s)
        if np.isfinite(r) and np.isfinite(d)
    )
    if not pairs:
        return [], [], 0

    clusters: List[Dict[str, List[float]]] = []
    for rel, dwell in pairs:
        if not clusters:
            clusters.append({"rel": [rel], "dwell": [dwell]})
            continue
        center = float(np.mean(clusters[-1]["rel"]))
        if abs(rel - center) <= TRAINING_INTERSECTION_REL_BIN:
            clusters[-1]["rel"].append(rel)
            clusters[-1]["dwell"].append(dwell)
        else:
            clusters.append({"rel": [rel], "dwell": [dwell]})

    hotspot_rel: List[float] = []
    hotspot_dwell: List[float] = []
    for c in clusters:
        if len(c["rel"]) < TRAINING_INTERSECTION_MIN_CLUSTER_EVENTS:
            continue
        hotspot_rel.append(float(np.mean(c["rel"])))
        hotspot_dwell.append(float(np.median(c["dwell"])))

    # Fallback for sparse training data.
    if not hotspot_rel and pairs:
        dwell_sorted = sorted(pairs, key=lambda x: x[1], reverse=True)
        top = dwell_sorted[: min(len(dwell_sorted), 4)]
        hotspot_rel = sorted(float(x[0]) for x in top)
        hotspot_dwell = [float(np.median([x[1] for x in top]))] * len(hotspot_rel)

    if len(hotspot_rel) > TRAINING_INTERSECTION_MAX_HOTSPOTS:
        sel = np.linspace(0, len(hotspot_rel) - 1, TRAINING_INTERSECTION_MAX_HOTSPOTS)
        idx = sorted(set(int(round(x)) for x in sel))
        hotspot_rel = [hotspot_rel[i] for i in idx]
        hotspot_dwell = [hotspot_dwell[i] for i in idx]

    return hotspot_rel, hotspot_dwell, len(pairs)


def mirror_hotspot_positions(rel_positions: List[float]) -> List[float]:
    mirrored = [float(1.0 - np.clip(r, 0.0, 1.0)) for r in rel_positions if np.isfinite(r)]
    return sorted(mirrored)


def get_route_motion_profile(route_class: Optional[str], training_model: Optional[Dict]) -> Optional[Dict]:
    if training_model is None or not training_model.get("enabled", False):
        return None
    key = canonical_uni_route_class(route_class)
    if key is None:
        return None
    profile = training_model.get("route_profiles", {}).get(key)
    if not isinstance(profile, dict):
        return None
    hotspots = profile.get("hotspot_rel") or []
    if len(hotspots) == 0:
        return None
    return profile


def build_training_intersection_model(training_dir: str) -> Dict[str, object]:
    model = {
        "enabled": False,
        "stand_speed_threshold_mps": np.nan,
        "stand_speed_median_mps": np.nan,
        "moving_speed_median_mps": np.nan,
        "global_dwell_median_s": np.nan,
        "route_profiles": {},
        "training_files_used_n": 0,
    }
    if not ENABLE_TRAINING_INTERSECTION_MODEL:
        return model
    if not training_dir:
        return model

    root = Path(training_dir).expanduser()
    if not root.exists() or not root.is_dir():
        print(f"[WARN] Training directory not found; skipping stop model: {root}")
        return model

    files = sorted([p for p in root.glob("*.csv") if p.is_file() and "summary" not in p.name.lower()])
    if not files:
        return model

    all_speed_samples: List[float] = []
    per_route_series: Dict[str, List[pd.DataFrame]] = {}
    for path in files:
        route_key = canonical_uni_route_class(path.stem)
        if route_key is None or "bike" not in route_key:
            continue

        part = parse_sensor_csv_single(path)
        if part.empty:
            continue
        part = part[part["gps_mode"].fillna(-1) >= TRAINING_INTERSECTION_MIN_GPS_MODE].copy()
        if len(part) < TRAINING_INTERSECTION_MIN_FILE_POINTS:
            continue

        progress_df = compute_progress_and_speed_from_sensor_points(part)
        if progress_df.empty or len(progress_df) < TRAINING_INTERSECTION_MIN_FILE_POINTS:
            continue

        # Discard tiny traces, they do not represent full route behavior.
        rel = pd.to_numeric(progress_df["progress_rel"], errors="coerce").to_numpy(dtype=float)
        if np.nanmax(rel) - np.nanmin(rel) < 0.2:
            continue
        lat = pd.to_numeric(part["gps_lat"], errors="coerce").to_numpy(dtype=float)
        lon = pd.to_numeric(part["gps_lon"], errors="coerce").to_numpy(dtype=float)
        if len(lat) >= 2 and len(lon) >= 2:
            approx_dist = float(np.nansum(haversine_m_vectorized(lat[:-1], lon[:-1], lat[1:], lon[1:])))
            if approx_dist < TRAINING_INTERSECTION_MIN_FILE_DISTANCE_M:
                continue

        speed_vals = pd.to_numeric(progress_df["speed_m_s"], errors="coerce")
        speed_vals = speed_vals[np.isfinite(speed_vals) & (speed_vals >= 0.0) & (speed_vals <= 35.0)]
        if len(speed_vals) > 0:
            all_speed_samples.extend(speed_vals.astype(float).tolist())

        per_route_series.setdefault(route_key, []).append(progress_df)

    if not all_speed_samples or not per_route_series:
        return model

    q = float(np.nanquantile(np.array(all_speed_samples, dtype=float), TRAINING_INTERSECTION_STOP_SPEED_Q))
    stand_threshold = float(np.clip(q, TRAINING_INTERSECTION_STOP_SPEED_MIN_MPS, TRAINING_INTERSECTION_STOP_SPEED_MAX_MPS))

    speed_arr = np.array(all_speed_samples, dtype=float)
    stand_arr = speed_arr[speed_arr <= stand_threshold]
    moving_arr = speed_arr[speed_arr > stand_threshold]
    stand_median = float(np.nanmedian(stand_arr)) if len(stand_arr) > 0 else float(np.nanmedian(speed_arr))
    moving_median = float(np.nanmedian(moving_arr)) if len(moving_arr) > 0 else float(np.nanmedian(speed_arr))

    route_profiles: Dict[str, Dict] = {}
    global_dwell: List[float] = []
    for route_key, series_list in per_route_series.items():
        route_events: List[float] = []
        route_dwells: List[float] = []
        for progress_df in series_list:
            ev_rel, ev_dwell = detect_stop_events_from_progress(progress_df, stand_threshold)
            route_events.extend(ev_rel)
            route_dwells.extend(ev_dwell)

        hotspots, hotspot_dwell, raw_events_n = cluster_stop_events(route_events, route_dwells)
        if not hotspots:
            continue
        dwell_median = float(np.nanmedian(hotspot_dwell)) if hotspot_dwell else float(np.nanmedian(route_dwells))
        if not np.isfinite(dwell_median):
            dwell_median = 0.0
        global_dwell.append(dwell_median)

        route_profiles[route_key] = {
            "hotspot_rel": [float(np.clip(x, 0.0, 1.0)) for x in hotspots],
            "dwell_s": float(max(0.0, dwell_median)),
            "strength": 1.0,
            "source": "training_route_specific",
            "raw_stop_events_n": int(raw_events_n),
            "files_n": int(len(series_list)),
        }

    # Directional mirroring to cover missing bike direction.
    if "from_uni_bike" in route_profiles and "to_uni_bike" not in route_profiles:
        src = route_profiles["from_uni_bike"]
        route_profiles["to_uni_bike"] = {
            **src,
            "hotspot_rel": mirror_hotspot_positions(src.get("hotspot_rel", [])),
            "source": "training_mirrored_from_from_uni_bike",
        }
    if "to_uni_bike" in route_profiles and "from_uni_bike" not in route_profiles:
        src = route_profiles["to_uni_bike"]
        route_profiles["from_uni_bike"] = {
            **src,
            "hotspot_rel": mirror_hotspot_positions(src.get("hotspot_rel", [])),
            "source": "training_mirrored_from_to_uni_bike",
        }

    # Template fallback for OPNV classes from bike behavior.
    bike_template = sorted(set(
        [float(x) for x in route_profiles.get("to_uni_bike", {}).get("hotspot_rel", [])]
        + [float(x) for x in route_profiles.get("from_uni_bike", {}).get("hotspot_rel", [])]
    ))
    for direction in ("to_uni", "from_uni"):
        opnv_key = f"{direction}_opnv"
        if opnv_key in route_profiles:
            continue
        bike_key = f"{direction}_bike"
        src_profile = route_profiles.get(bike_key)
        hotspot_rel = (
            src_profile.get("hotspot_rel", []) if isinstance(src_profile, dict) else bike_template
        )
        if not hotspot_rel:
            continue
        base_dwell = float(src_profile.get("dwell_s", np.nan)) if isinstance(src_profile, dict) else np.nan
        if not np.isfinite(base_dwell):
            base_dwell = float(np.nanmedian(global_dwell)) if global_dwell else 0.0
        route_profiles[opnv_key] = {
            "hotspot_rel": [float(np.clip(x, 0.0, 1.0)) for x in hotspot_rel],
            "dwell_s": float(max(0.0, base_dwell * TRAINING_INTERSECTION_OPNV_DWELL_SCALE)),
            "strength": float(TRAINING_INTERSECTION_OPNV_DWELL_SCALE),
            "source": "bike_template_for_opnv",
            "raw_stop_events_n": 0,
            "files_n": 0,
        }

    if not route_profiles:
        return model

    model.update({
        "enabled": True,
        "stand_speed_threshold_mps": float(stand_threshold),
        "stand_speed_median_mps": float(stand_median),
        "moving_speed_median_mps": float(moving_median),
        "global_dwell_median_s": float(np.nanmedian(global_dwell)) if global_dwell else np.nan,
        "route_profiles": route_profiles,
        "training_files_used_n": int(sum(len(v) for v in per_route_series.values())),
    })
    return model


def get_training_intersection_model(training_dir: str) -> Dict[str, object]:
    global _TRAINING_INTERSECTION_MODEL_CACHE, _TRAINING_INTERSECTION_MODEL_SOURCE
    source_key = str(Path(training_dir).expanduser()) if training_dir else ""
    if (
        _TRAINING_INTERSECTION_MODEL_CACHE is not None
        and _TRAINING_INTERSECTION_MODEL_SOURCE == source_key
    ):
        return _TRAINING_INTERSECTION_MODEL_CACHE
    model = build_training_intersection_model(training_dir)
    _TRAINING_INTERSECTION_MODEL_CACHE = model
    _TRAINING_INTERSECTION_MODEL_SOURCE = source_key
    return model


def empty_sensor_file_diag_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "source_file",
        "source_name",
        "context_match",
        "selected_for_loading",
        "status",
        "reason",
        "detail",
        "rows_parsed",
        "rows_after_time_window",
        "rows_loaded",
    ])


def parse_sensor_gps(
    csv_path: Optional[str],
    google_csv: Optional[str] = None,
    candidate_files: Optional[List[Path]] = None,
    time_context: Optional[Dict] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    empty = pd.DataFrame(columns=[
        "fix_id", "timestamp", "gps_mode", "gps_lat", "gps_lon", "gps_speed_m_s",
        "gps_track_deg", "gps_eph_m", "gps_time_utc", "source_file", "source_name"
    ])
    diag_empty = empty_sensor_file_diag_df()
    if not csv_path:
        return empty, diag_empty

    candidates = candidate_files if candidate_files is not None else discover_sensor_csv_files(csv_path, google_csv=google_csv)
    if not candidates:
        print(f"[WARN] No sensor CSV files found for input: {csv_path}. Continuing without sensor fusion.")
        return empty, diag_empty

    google_ctx = time_context if time_context is not None else infer_google_time_context(google_csv)
    matched_by_context = [p for p in candidates if sensor_file_matches_google_context(p, google_ctx)]
    fallback_used_all_candidates = False
    if USE_SENSOR_FILENAME_HINT_FILTER:
        matched = list(matched_by_context)
        if not matched:
            source_name = (google_ctx or {}).get("source", "google_context")
            print(f"[WARN] No sensor files matched context ({source_name}); using all discovered sensor files.")
            matched = candidates
            fallback_used_all_candidates = True
    else:
        matched = list(candidates)
        fallback_used_all_candidates = True

    matched_set = {str(p) for p in matched}
    matched_by_context_set = {str(p) for p in matched_by_context}
    diag_by_file: Dict[str, Dict] = {}
    for path in candidates:
        p = str(path)
        selected_for_loading = p in matched_set
        reason = "pending_parse"
        status = "pending"
        if not selected_for_loading:
            status = "sorted_out"
            reason = "excluded_by_filename_time_hints"
        diag_by_file[p] = {
            "source_file": p,
            "source_name": path.name,
            "context_match": (p in matched_by_context_set),
            "selected_for_loading": selected_for_loading,
            "status": status,
            "reason": reason,
            "detail": "",
            "rows_parsed": 0,
            "rows_after_time_window": 0,
            "rows_loaded": 0,
        }

    frames: List[pd.DataFrame] = []
    loaded = 0
    for path in matched:
        key = str(path)
        diag_entry = diag_by_file.get(key)
        if diag_entry is None:
            diag_entry = {
                "source_file": key,
                "source_name": path.name,
                "context_match": (key in matched_by_context_set),
                "selected_for_loading": True,
                "status": "pending",
                "reason": "pending_parse",
                "detail": "",
                "rows_parsed": 0,
                "rows_after_time_window": 0,
                "rows_loaded": 0,
            }
            diag_by_file[key] = diag_entry
        try:
            part = parse_sensor_csv_single(path)
        except Exception as exc:
            print(f"[WARN] Failed to parse sensor CSV {path}: {exc}")
            diag_entry["status"] = "sorted_out"
            diag_entry["reason"] = "parse_error"
            diag_entry["detail"] = str(exc)
            continue
        diag_entry["rows_parsed"] = int(len(part))
        if part.empty:
            ts_only = parse_sensor_timestamps_only(path)
            if not ts_only.empty:
                if google_ctx is not None:
                    start_utc = google_ctx["start_utc"] - pd.Timedelta(hours=SENSOR_FILE_HINT_PADDING_HOURS)
                    end_utc = google_ctx["end_utc"] + pd.Timedelta(hours=SENSOR_FILE_HINT_PADDING_HOURS)
                    ts_only = ts_only[(ts_only["timestamp"] >= start_utc) & (ts_only["timestamp"] <= end_utc)].copy()
                diag_entry["rows_after_time_window"] = int(len(ts_only))
                if not ts_only.empty:
                    diag_entry["status"] = "loaded_for_fusion"
                    diag_entry["reason"] = "loaded_time_only_no_valid_gps_points"
                    continue
            diag_entry["status"] = "sorted_out"
            diag_entry["reason"] = "no_valid_timestamp_or_latlon_rows"
            continue

        if google_ctx is not None:
            start_utc = google_ctx["start_utc"] - pd.Timedelta(hours=SENSOR_FILE_HINT_PADDING_HOURS)
            end_utc = google_ctx["end_utc"] + pd.Timedelta(hours=SENSOR_FILE_HINT_PADDING_HOURS)
            part = part[(part["timestamp"] >= start_utc) & (part["timestamp"] <= end_utc)].copy()
            diag_entry["rows_after_time_window"] = int(len(part))
            if part.empty:
                diag_entry["status"] = "sorted_out"
                diag_entry["reason"] = "outside_google_time_window"
                continue
        else:
            diag_entry["rows_after_time_window"] = int(len(part))

        frames.append(part)
        loaded += 1
        diag_entry["status"] = "loaded_for_fusion"
        if fallback_used_all_candidates and not bool(diag_entry.get("context_match")):
            diag_entry["reason"] = "loaded_via_context_fallback"
        else:
            diag_entry["reason"] = "loaded_for_fusion"

    if not frames:
        print("[WARN] Sensor files found but no rows overlapped Google time window.")
        diag_df = pd.DataFrame(diag_by_file.values()).reindex(columns=diag_empty.columns)
        diag_df = diag_df.sort_values(["status", "source_name"]).reset_index(drop=True)
        return empty, diag_df

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["timestamp", "gps_lat", "gps_lon", "source_name"], keep="first")
    out = out.sort_values("timestamp").reset_index(drop=True)
    out = deduplicate_sensor_fixes(out)
    out["fix_id"] = np.arange(len(out), dtype=int)

    if "source_file" in out.columns:
        loaded_counts = out.groupby("source_file").size().to_dict()
    else:
        loaded_counts = {}
    for key, entry in diag_by_file.items():
        rows_loaded = int(loaded_counts.get(key, 0))
        entry["rows_loaded"] = rows_loaded
        if (
            entry.get("status") == "loaded_for_fusion"
            and rows_loaded <= 0
            and str(entry.get("reason", "")) != "loaded_time_only_no_valid_gps_points"
        ):
            entry["status"] = "sorted_out"
            entry["reason"] = "dropped_during_global_deduplication"

    print(
        f"[INFO] Loaded sensor fixes from {loaded}/{len(matched)} matched files "
        f"({len(candidates)} discovered): {len(out):,} rows after filtering."
    )
    diag_df = pd.DataFrame(diag_by_file.values()).reindex(columns=diag_empty.columns)
    diag_df = diag_df.sort_values(["status", "source_name"]).reset_index(drop=True)
    return out, diag_df


# ============================================================
# SEGMENT EXTRACTION
# ============================================================
def parse_google_segments(csv_path: str, time_context: Optional[Dict] = None) -> List[SegmentSpec]:
    df = read_google_timeline_table(csv_path)
    if "startTime" not in df.columns or "endTime" not in df.columns:
        raise ValueError(
            "Google timeline input must provide start/end timestamps after normalization. "
            f"Available columns: {list(df.columns)[:30]}"
        )
    start_series = pd.to_datetime(df["startTime"], utc=True, errors="coerce")
    end_series = pd.to_datetime(df["endTime"], utc=True, errors="coerce")

    if time_context is not None:
        mask = start_series.notna() & end_series.notna() & (end_series >= start_series)
        window_start = time_context.get("start_utc")
        window_end = time_context.get("end_utc")
        if window_start is not None:
            mask &= (end_series >= window_start)
        if window_end is not None:
            mask &= (start_series <= window_end)

        allowed_local_dates = time_context.get("local_dates")
        if allowed_local_dates:
            try:
                start_local_date = start_series.dt.tz_convert(LOCAL_TIMEZONE).dt.date
                end_local_date = end_series.dt.tz_convert(LOCAL_TIMEZONE).dt.date
                date_mask = start_local_date.isin(allowed_local_dates) | end_local_date.isin(allowed_local_dates)
                mask &= date_mask.fillna(False)
            except Exception:
                pass

        keep_mask = mask.fillna(False)
        keep_n = int(keep_mask.sum())
        total_n = int(len(df))
        if keep_n < total_n:
            print(
                f"[INFO] Subsetting Google rows to sensor-covered window: "
                f"{keep_n:,}/{total_n:,} rows retained."
            )
        df = df.loc[keep_mask].copy()
        start_series = start_series.loc[df.index]
        end_series = end_series.loc[df.index]

    visit_loc_series = df.get(
        "visit.topCandidate.placeLocation.latLng",
        pd.Series(index=df.index, dtype=object),
    )
    visit_prob_series = pd.to_numeric(
        df.get("visit.topCandidate.probability", pd.Series(np.nan, index=df.index)),
        errors="coerce",
    ).fillna(-np.inf)

    tmp = pd.DataFrame({
        "_start": start_series,
        "_end": end_series,
        "_visit_loc": visit_loc_series,
        "_score": visit_prob_series,
    })

    visit_keep_idx = set(
        tmp[tmp["_visit_loc"].notna()]
        .sort_values(["_start", "_score", "_end"], ascending=[True, False, False])
        .groupby("_start", as_index=False)
        .head(1)
        .index
    )

    segs: List[SegmentSpec] = []
    seg_id = 0

    for i, row in df.iterrows():
        start_t = start_series.loc[i]
        end_t = end_series.loc[i]
        if pd.isna(start_t) or pd.isna(end_t) or end_t < start_t:
            continue

        mode = normalize_mode(row.get("activity.topCandidate.type"))
        activity_distance_m = pd.to_numeric(pd.Series([row.get("activity.distanceMeters")]), errors="coerce").iloc[0]
        activity_distance_m = float(activity_distance_m) if pd.notna(activity_distance_m) else None

        visit_loc = parse_latlng(row.get("visit.topCandidate.placeLocation.latLng"))
        act_start = parse_latlng(row.get("activity.start.latLng"))
        act_end = parse_latlng(row.get("activity.end.latLng"))
        tl = parse_timeline_path(row.get("timelinePath"))

        # Skip broad wrappers when specific rows exist inside.
        if len(tl) >= 2 and visit_loc is None and act_start is None and act_end is None:
            contained = (
                (start_series >= start_t)
                & (end_series <= end_t)
                & ((start_series > start_t) | (end_series < end_t))
            )
            if int(contained.fillna(False).sum()) >= 2:
                continue

        if visit_loc is not None:
            if i not in visit_keep_idx:
                continue
            segs.append(SegmentSpec(
                segment_id=seg_id,
                google_row_index=i,
                kind="visit_stationary",
                mode=None,
                start_t=start_t,
                end_t=end_t,
                start_lat=visit_loc[0],
                start_lon=visit_loc[1],
                end_lat=visit_loc[0],
                end_lon=visit_loc[1],
                timeline_points=[PointTime(visit_loc[0], visit_loc[1], start_t), PointTime(visit_loc[0], visit_loc[1], end_t)],
                activity_distance_m=0.0,
            ))
            seg_id += 1
            continue

        if len(tl) >= 2:
            segs.append(SegmentSpec(
                segment_id=seg_id,
                google_row_index=i,
                kind="activity_timeline_segment",
                mode=mode,
                start_t=tl[0].t,
                end_t=tl[-1].t,
                start_lat=tl[0].lat,
                start_lon=tl[0].lon,
                end_lat=tl[-1].lat,
                end_lon=tl[-1].lon,
                timeline_points=tl,
                activity_distance_m=activity_distance_m,
            ))
            seg_id += 1
            continue

        if act_start is not None and act_end is not None:
            segs.append(SegmentSpec(
                segment_id=seg_id,
                google_row_index=i,
                kind="activity_start_end_segment",
                mode=mode,
                start_t=start_t,
                end_t=end_t,
                start_lat=act_start[0],
                start_lon=act_start[1],
                end_lat=act_end[0],
                end_lon=act_end[1],
                timeline_points=[PointTime(act_start[0], act_start[1], start_t), PointTime(act_end[0], act_end[1], end_t)],
                activity_distance_m=activity_distance_m,
            ))
            seg_id += 1
            continue

        fallback_pt = visit_loc or act_start or act_end
        if fallback_pt is not None:
            segs.append(SegmentSpec(
                segment_id=seg_id,
                google_row_index=i,
                kind="single_point_fallback",
                mode=mode,
                start_t=start_t,
                end_t=end_t,
                start_lat=fallback_pt[0],
                start_lon=fallback_pt[1],
                end_lat=fallback_pt[0],
                end_lon=fallback_pt[1],
                timeline_points=[PointTime(fallback_pt[0], fallback_pt[1], start_t), PointTime(fallback_pt[0], fallback_pt[1], end_t)],
                activity_distance_m=activity_distance_m,
            ))
            seg_id += 1

    return segs


# ============================================================
# FIX EVALUATION
# ============================================================
def allowed_fix_distance_from_baseline(eph_m: Optional[float]) -> float:
    eph = eph_m if eph_m is not None and pd.notna(eph_m) else SENSOR_SOFT_MAX_EPH_M
    return max(SENSOR_BASELINE_DIST_FLOOR_M, min(SENSOR_HARD_MAX_EPH_M, SENSOR_BASELINE_DIST_EPH_FACTOR * float(eph)))


def fix_quality_weight(mode: Optional[float], eph_m: Optional[float]) -> float:
    mode_bonus = 1.0
    if pd.notna(mode):
        if mode >= 3:
            mode_bonus = 1.25
        elif mode >= 2:
            mode_bonus = 1.0
        else:
            mode_bonus = 0.0
    eph = float(eph_m) if pd.notna(eph_m) else SENSOR_SOFT_MAX_EPH_M
    eph_weight = 1.0 / (1.0 + max(eph, 1.0) / 25.0)
    return mode_bonus * eph_weight


def filter_segment_sensor_fixes(sensor_df: pd.DataFrame, start_t: pd.Timestamp, end_t: pd.Timestamp) -> pd.DataFrame:
    if sensor_df.empty:
        return sensor_df.copy()
    mask = (
        (sensor_df["timestamp"] >= start_t)
        & (sensor_df["timestamp"] <= end_t)
        & sensor_df["gps_lat"].notna()
        & sensor_df["gps_lon"].notna()
    )
    return sensor_df.loc[mask].copy().sort_values("timestamp").reset_index(drop=True)


def deduplicate_sensor_fixes(fixes: pd.DataFrame) -> pd.DataFrame:
    if fixes.empty:
        return fixes
    keep = []
    last_t = None
    last_lat = None
    last_lon = None
    for idx, row in fixes.iterrows():
        t = row["timestamp"]
        lat = row["gps_lat"]
        lon = row["gps_lon"]
        if last_t is None:
            keep.append(idx)
            last_t, last_lat, last_lon = t, lat, lon
            continue
        dt = float((t - last_t).total_seconds())
        ds = haversine_m(lat, lon, last_lat, last_lon)
        if dt >= SENSOR_MIN_TIME_SEPARATION_S or ds >= SENSOR_MIN_SPATIAL_SEPARATION_M:
            keep.append(idx)
            last_t, last_lat, last_lon = t, lat, lon
    return fixes.loc[keep].copy().reset_index(drop=True)


def evaluate_moving_fixes(
    segment: SegmentSpec,
    sensor_fixes: pd.DataFrame,
    baseline_coords: List[Tuple[float, float]],
    baseline_cumdist: np.ndarray,
) -> Tuple[List[Anchor], List[Dict]]:
    diagnostics: List[Dict] = []
    accepted: List[Anchor] = []
    profile = get_osrm_profile(segment.mode)
    last_chainage = -np.inf

    for _, row in sensor_fixes.iterrows():
        fix_id = int(row["fix_id"])
        ts = row["timestamp"]
        raw_lat = float(row["gps_lat"])
        raw_lon = float(row["gps_lon"])
        source_name = row.get("source_name")
        source_file = row.get("source_file")
        gps_mode = row["gps_mode"]
        gps_speed = row["gps_speed_m_s"]
        eph = row["gps_eph_m"]

        status = "rejected"
        reason = "unknown"
        adjusted_lat = np.nan
        adjusted_lon = np.nan
        adjusted_chainage = np.nan
        baseline_proj = project_point_to_polyline(raw_lat, raw_lon, baseline_coords, baseline_cumdist)
        baseline_dist = float(baseline_proj["distance_m"])
        threshold = allowed_fix_distance_from_baseline(eph)
        snap_dist = np.nan
        snap_method = "none"

        if pd.isna(gps_mode) or gps_mode < SENSOR_FIX_MIN_MODE:
            reason = "gps_mode_below_fix_threshold"
        elif pd.notna(eph) and float(eph) > SENSOR_HARD_MAX_EPH_M:
            reason = "eph_above_hard_threshold"
        elif baseline_dist > threshold:
            reason = "too_far_from_baseline"
        else:
            use_lat, use_lon = raw_lat, raw_lon
            use_chainage = float(baseline_proj["chainage_m"])
            reason = "accepted_raw"
            status = "accepted"

            if profile is not None and USE_OSRM_SENSOR_SNAP:
                nearest = osrm_nearest(profile, raw_lat, raw_lon)
                if nearest is not None:
                    snap_dist = float(nearest["distance_m"])
                    if snap_dist <= threshold:
                        use_lat = float(nearest["lat"])
                        use_lon = float(nearest["lon"])
                        snap_method = "osrm_nearest"
                        reason = "accepted_osrm_snapped"
                    else:
                        snap_method = "osrm_nearest_rejected"

            # If the raw/snap point is still noticeably away from the baseline, use its projection.
            proj2 = project_point_to_polyline(use_lat, use_lon, baseline_coords, baseline_cumdist)
            if float(proj2["distance_m"]) > SENSOR_NEAR_BASELINE_KEEP_RAW_M:
                use_lat = float(proj2["lat"])
                use_lon = float(proj2["lon"])
                use_chainage = float(proj2["chainage_m"])
                if reason == "accepted_raw":
                    reason = "accepted_projected_to_baseline"
                elif reason == "accepted_osrm_snapped":
                    reason = "accepted_osrm_then_projected_to_baseline"
            else:
                use_chainage = float(proj2["chainage_m"])

            if use_chainage + BACKTRACK_TOLERANCE_M < last_chainage:
                status = "rejected"
                reason = "backtracking_against_segment_direction"
            else:
                last_chainage = max(last_chainage, use_chainage)
                adjusted_lat = use_lat
                adjusted_lon = use_lon
                adjusted_chainage = use_chainage
                accepted.append(Anchor(
                    lat=use_lat,
                    lon=use_lon,
                    t=ts,
                    source="sensor_fix",
                    chainage_m=use_chainage,
                    speed_mps=float(gps_speed) if pd.notna(gps_speed) else None,
                    eph_m=float(eph) if pd.notna(eph) else None,
                    raw_lat=raw_lat,
                    raw_lon=raw_lon,
                    fix_id=fix_id,
                ))

        diagnostics.append({
            "segment_id": segment.segment_id,
            "google_row_index": segment.google_row_index,
            "segment_kind": segment.kind,
            "mode": segment.mode,
            "fix_id": fix_id,
            "source_name": source_name,
            "source_file": source_file,
            "timestamp": ts,
            "gps_mode": gps_mode,
            "raw_lat": raw_lat,
            "raw_lon": raw_lon,
            "gps_speed_m_s": gps_speed,
            "gps_eph_m": eph,
            "baseline_distance_m": baseline_dist,
            "baseline_chainage_m": float(baseline_proj["chainage_m"]),
            "allowed_distance_m": threshold,
            "snap_distance_m": snap_dist,
            "snap_method": snap_method,
            "adjusted_lat": adjusted_lat,
            "adjusted_lon": adjusted_lon,
            "adjusted_chainage_m": adjusted_chainage,
            "status": status,
            "reason": reason,
            "quality_weight": fix_quality_weight(gps_mode, eph),
        })

    return accepted, diagnostics


def refine_stationary_point(
    base_lat: float,
    base_lon: float,
    sensor_fixes: pd.DataFrame,
) -> Tuple[float, float, str, List[Dict]]:
    diagnostics: List[Dict] = []
    candidates = []

    for _, row in sensor_fixes.iterrows():
        fix_id = int(row["fix_id"])
        raw_lat = float(row["gps_lat"])
        raw_lon = float(row["gps_lon"])
        source_name = row.get("source_name")
        source_file = row.get("source_file")
        gps_mode = row["gps_mode"]
        eph = row["gps_eph_m"]
        dist_to_base = haversine_m(base_lat, base_lon, raw_lat, raw_lon)
        status = "rejected"
        reason = "unknown"
        if pd.isna(gps_mode) or gps_mode < SENSOR_FIX_MIN_MODE:
            reason = "gps_mode_below_fix_threshold"
        elif pd.notna(eph) and float(eph) > SENSOR_HARD_MAX_EPH_M:
            reason = "eph_above_hard_threshold"
        elif dist_to_base > max(SENSOR_STATIONARY_RADIUS_M, allowed_fix_distance_from_baseline(eph)):
            reason = "too_far_from_stationary_base"
        else:
            status = "accepted"
            reason = "accepted_stationary_fix"
            candidates.append((raw_lat, raw_lon, fix_quality_weight(gps_mode, eph), fix_id))

        diagnostics.append({
            "segment_id": None,
            "google_row_index": None,
            "segment_kind": "visit_stationary",
            "mode": None,
            "fix_id": fix_id,
            "source_name": source_name,
            "source_file": source_file,
            "timestamp": row["timestamp"],
            "gps_mode": gps_mode,
            "raw_lat": raw_lat,
            "raw_lon": raw_lon,
            "gps_speed_m_s": row["gps_speed_m_s"],
            "gps_eph_m": eph,
            "baseline_distance_m": dist_to_base,
            "baseline_chainage_m": 0.0,
            "allowed_distance_m": max(SENSOR_STATIONARY_RADIUS_M, allowed_fix_distance_from_baseline(eph)),
            "snap_distance_m": np.nan,
            "snap_method": "none",
            "adjusted_lat": raw_lat if status == "accepted" else np.nan,
            "adjusted_lon": raw_lon if status == "accepted" else np.nan,
            "adjusted_chainage_m": 0.0 if status == "accepted" else np.nan,
            "status": status,
            "reason": reason,
            "quality_weight": fix_quality_weight(gps_mode, eph),
        })

    if not candidates:
        return base_lat, base_lon, "google_stationary_only", diagnostics

    centroid = weighted_centroid([(lat, lon, w) for lat, lon, w, _ in candidates])
    shift = haversine_m(base_lat, base_lon, centroid[0], centroid[1])
    if shift <= SENSOR_BLEND_MAX_SHIFT_M:
        return centroid[0], centroid[1], "sensor_stationary_centroid", diagnostics

    # Blend if the centroid is plausible but not trusted enough for a full jump.
    alpha = SENSOR_BLEND_MAX_SHIFT_M / max(shift, 1e-9)
    lat = base_lat + alpha * (centroid[0] - base_lat)
    lon = base_lon + alpha * (centroid[1] - base_lon)
    return lat, lon, "google_sensor_blend", diagnostics


# ============================================================
# TIME GRID + INTERPOLATION
# ============================================================
def regular_time_grid(start_t: pd.Timestamp, end_t: pd.Timestamp, step_seconds: int) -> List[pd.Timestamp]:
    if pd.isna(start_t) or pd.isna(end_t) or end_t < start_t:
        return []
    start_ns = start_t.value
    end_ns = end_t.value
    step_ns = int(step_seconds * 1e9)
    first_ns = ((start_ns + step_ns - 1) // step_ns) * step_ns  # ceil to global step
    last_ns = (end_ns // step_ns) * step_ns  # floor to global step
    if first_ns > last_ns:
        return []
    grid_ns = list(range(first_ns, last_ns + 1, step_ns))
    return [pd.Timestamp(x, tz="UTC") for x in grid_ns]


def infer_speed_for_interval(
    mode: Optional[str],
    anchors: List[Anchor],
    training_motion_model: Optional[Dict] = None,
) -> Tuple[float, str]:
    mode_norm = normalize_mode(mode)
    if mode_norm == "CYCLING":
        return MODE_SPEED_MPS["CYCLING"], "mode_bike_fixed"

    speeds = [a.speed_mps for a in anchors if a.speed_mps is not None and pd.notna(a.speed_mps) and a.speed_mps > 0]
    if speeds:
        stand_threshold = np.nan
        if isinstance(training_motion_model, dict):
            stand_threshold = float(training_motion_model.get("stand_speed_threshold_mps", np.nan))

        used = speeds
        source = "sensor_median"
        if (
            np.isfinite(stand_threshold)
            and mode_norm in {"CYCLING", "WALKING", "RUNNING"}
        ):
            moving = [s for s in speeds if s > stand_threshold]
            if moving:
                used = moving
                source = "sensor_median_moving_only"
            else:
                source = "sensor_median_with_stops"

        speed = float(np.nanmedian(used))
        default = get_mode_speed_mps(mode)
        # Avoid pathological sensor spikes dominating the interpolation.
        speed = min(max(speed, 0.4), max(default * 2.5, 6.0))
        return speed, source
    return get_mode_speed_mps(mode), "mode_default"


def mode_speed_mps_from_label(mode_label: Optional[str], fallback_mode: Optional[str]) -> float:
    key = normalize_route_mode_label(mode_label)
    if key is None:
        return get_mode_speed_mps(fallback_mode)
    return float(ROUTE_MODE_SPEED_MPS.get(key, get_mode_speed_mps(fallback_mode)))


def build_interval_route_mode_segments(
    route_mode_profile: Optional[Dict],
    path_length_m: float,
    baseline_length_m: Optional[float],
    interval_start_chainage_m: Optional[float],
    interval_end_chainage_m: Optional[float],
    fallback_mode: Optional[str],
) -> List[Dict]:
    if route_mode_profile is None:
        return []
    spans_rel = route_mode_profile.get("spans_rel") if isinstance(route_mode_profile, dict) else None
    if not spans_rel:
        return []
    if path_length_m <= 0:
        return []
    if baseline_length_m is None or not np.isfinite(baseline_length_m) or baseline_length_m <= 0:
        return []
    if interval_start_chainage_m is None or interval_end_chainage_m is None:
        return []

    s = float(min(interval_start_chainage_m, interval_end_chainage_m))
    e = float(max(interval_start_chainage_m, interval_end_chainage_m))
    interval_chainage_m = e - s
    if interval_chainage_m <= 0:
        return []
    chain_to_path = float(path_length_m / interval_chainage_m)

    out: List[Dict] = []
    tol = 1e-6 * baseline_length_m
    for span in spans_rel:
        try:
            start_rel = float(span.get("start_rel", np.nan))
            end_rel = float(span.get("end_rel", np.nan))
            mode_label = normalize_route_mode_label(span.get("mode"))
        except Exception:
            continue
        if not np.isfinite(start_rel) or not np.isfinite(end_rel):
            continue
        if end_rel <= start_rel:
            continue
        abs_start = start_rel * baseline_length_m
        abs_end = end_rel * baseline_length_m
        inter_start = max(abs_start, s)
        inter_end = min(abs_end, e)
        if inter_end <= inter_start:
            continue

        dist_chainage = inter_end - inter_start
        dist_path = max(0.0, dist_chainage * chain_to_path)
        speed_mps = mode_speed_mps_from_label(mode_label, fallback_mode)
        # Exception: no 2-minute standstill after the final walking segment at route end.
        is_final_walk_of_route = mode_label == "walk" and abs(abs_end - baseline_length_m) <= tol
        walk_ended_inside = (
            mode_label == "walk"
            and abs(abs_end - inter_end) <= tol
            and (abs_end < e - tol)
            and (not is_final_walk_of_route)
        )
        out.append({
            "mode": mode_label or "",
            "distance_m": float(dist_path),
            "speed_mps": float(max(speed_mps, 0.1)),
            "walk_dwell_after_s": float(WALK_SEGMENT_END_DWELL_S if walk_ended_inside else 0.0),
        })
    return out


def build_mode_timing_schedule(
    mode_segments: List[Dict],
    dt_s: float,
) -> Tuple[List[Dict], float, float, float]:
    if not mode_segments:
        return [], 0.0, 0.0, max(0.0, dt_s)

    nominal_move_s = 0.0
    nominal_dwell_s = 0.0
    for seg in mode_segments:
        d = float(seg.get("distance_m", 0.0))
        v = float(seg.get("speed_mps", 0.1))
        nominal_move_s += d / max(v, 0.1)
        nominal_dwell_s += float(seg.get("walk_dwell_after_s", 0.0))

    if dt_s >= nominal_move_s + nominal_dwell_s:
        move_scale = 1.0
        dwell_scale = 1.0
        slack_s = float(dt_s - (nominal_move_s + nominal_dwell_s))
    elif dt_s > nominal_dwell_s:
        move_scale = float((dt_s - nominal_dwell_s) / max(nominal_move_s, 1e-9))
        dwell_scale = 1.0
        slack_s = 0.0
    else:
        move_scale = 0.0
        dwell_scale = float(dt_s / max(nominal_dwell_s, 1e-9)) if nominal_dwell_s > 0 else 0.0
        slack_s = 0.0

    schedule: List[Dict] = []
    total_move_s = 0.0
    total_dwell_s = 0.0
    for seg in mode_segments:
        d = float(seg.get("distance_m", 0.0))
        v = float(seg.get("speed_mps", 0.1))
        move_t = (d / max(v, 0.1)) * move_scale
        dwell_t = float(seg.get("walk_dwell_after_s", 0.0)) * dwell_scale
        schedule.append({
            "distance_m": d,
            "move_time_s": float(max(move_t, 0.0)),
            "dwell_s": float(max(dwell_t, 0.0)),
        })
        total_move_s += max(move_t, 0.0)
        total_dwell_s += max(dwell_t, 0.0)

    return schedule, float(total_move_s), float(total_dwell_s), float(max(slack_s, 0.0))


def distance_along_path_with_mode_schedule(
    elapsed_s: float,
    dt_s: float,
    path_length_m: float,
    slack_start_s: float,
    slack_end_s: float,
    schedule: List[Dict],
) -> float:
    if path_length_m <= 0 or dt_s <= 0:
        return 0.0
    if elapsed_s <= slack_start_s:
        return 0.0
    if elapsed_s >= (dt_s - slack_end_s):
        return float(path_length_m)
    if not schedule:
        return 0.0

    t = max(0.0, elapsed_s - slack_start_s)
    dist_acc = 0.0
    for seg in schedule:
        d = float(seg.get("distance_m", 0.0))
        move_t = float(seg.get("move_time_s", 0.0))
        dwell_t = float(seg.get("dwell_s", 0.0))

        if move_t > 0:
            if t <= move_t:
                return float(dist_acc + d * (t / max(move_t, 1e-9)))
            t -= move_t
            dist_acc += d
        else:
            # zero movement time means no advancement in this segment
            dist_acc += 0.0

        if dwell_t > 0:
            if t <= dwell_t:
                return float(dist_acc)
            t -= dwell_t

    return float(path_length_m)


def build_interval_intersection_dwell_plan(
    route_motion_profile: Optional[Dict],
    path_length_m: float,
    baseline_length_m: Optional[float],
    interval_start_chainage_m: Optional[float],
    interval_end_chainage_m: Optional[float],
    base_slack_s: float,
) -> Dict[str, object]:
    out = {
        "applied": False,
        "event_distance_m": [],
        "event_dwell_s": [],
        "total_dwell_s": 0.0,
        "profile_source": "",
    }
    if route_motion_profile is None:
        return out
    if path_length_m <= 0 or base_slack_s <= 0:
        return out
    if baseline_length_m is None or not np.isfinite(baseline_length_m) or baseline_length_m <= 0:
        return out
    if interval_start_chainage_m is None or interval_end_chainage_m is None:
        return out

    try:
        start = float(interval_start_chainage_m)
        end = float(interval_end_chainage_m)
    except Exception:
        return out
    lo = min(start, end)
    hi = max(start, end)
    span = hi - lo
    if span <= 0.5:
        return out

    hotspot_rel = route_motion_profile.get("hotspot_rel") or []
    if len(hotspot_rel) == 0:
        return out
    dwell_per_event = float(route_motion_profile.get("dwell_s", 0.0) or 0.0)
    if dwell_per_event <= 0:
        return out

    event_distance: List[float] = []
    for rel in hotspot_rel:
        if not np.isfinite(rel):
            continue
        ch = float(np.clip(rel, 0.0, 1.0) * baseline_length_m)
        if lo <= ch <= hi:
            frac = (ch - lo) / max(span, 1e-9)
            d_m = float(np.clip(frac * path_length_m, 0.0, path_length_m))
            event_distance.append(d_m)
    if not event_distance:
        return out

    # Merge near-duplicate event positions.
    event_distance = sorted(event_distance)
    deduped: List[float] = []
    for d in event_distance:
        if not deduped or abs(d - deduped[-1]) > 2.0:
            deduped.append(d)
    event_distance = deduped

    max_dwell_budget_s = base_slack_s * TRAINING_INTERSECTION_MAX_SLACK_USAGE
    candidate_total_s = dwell_per_event * len(event_distance)
    dwell_total_s = float(min(max_dwell_budget_s, candidate_total_s))
    if dwell_total_s < 1.0:
        return out
    dwell_each = dwell_total_s / max(len(event_distance), 1)

    out["applied"] = True
    out["event_distance_m"] = event_distance
    out["event_dwell_s"] = [dwell_each] * len(event_distance)
    out["total_dwell_s"] = dwell_total_s
    out["profile_source"] = str(route_motion_profile.get("source", "training_intersection_profile"))
    return out


def distance_along_path_with_dwell(
    elapsed_s: float,
    dt_s: float,
    path_length_m: float,
    moving_s: float,
    slack_start_s: float,
    slack_end_s: float,
    event_distance_m: List[float],
    event_dwell_s: List[float],
) -> float:
    if path_length_m <= 0 or dt_s <= 0:
        return 0.0
    if elapsed_s <= slack_start_s:
        return 0.0
    if elapsed_s >= (dt_s - slack_end_s):
        return float(path_length_m)
    if moving_s <= 1e-9:
        return 0.0

    moving_speed_mps = path_length_m / moving_s
    travel_clock = max(0.0, elapsed_s - slack_start_s)
    prev_dist = 0.0
    for d_m, dwell_s in zip(event_distance_m, event_dwell_s):
        d_here = float(np.clip(d_m, prev_dist, path_length_m))
        seg_dist = max(0.0, d_here - prev_dist)
        seg_time = seg_dist / max(moving_speed_mps, 1e-9)
        if travel_clock <= seg_time:
            return float(prev_dist + moving_speed_mps * travel_clock)
        travel_clock -= seg_time
        if travel_clock <= dwell_s:
            return d_here
        travel_clock -= dwell_s
        prev_dist = d_here

    rem_dist = max(0.0, path_length_m - prev_dist)
    rem_time = rem_dist / max(moving_speed_mps, 1e-9)
    if travel_clock <= rem_time:
        return float(prev_dist + moving_speed_mps * travel_clock)
    return float(path_length_m)


def classify_position_source(
    ts: pd.Timestamp,
    start_anchor: Anchor,
    end_anchor: Anchor,
    path_method: str,
    accepted_fix_reason_by_id: Dict[int, str],
) -> str:
    if ts == start_anchor.t and start_anchor.source == "sensor_fix":
        reason = accepted_fix_reason_by_id.get(start_anchor.fix_id or -1, "accepted_raw")
        if "snapped" in reason:
            return "snapped_sensor_anchor"
        if "projected" in reason:
            return "projected_sensor_anchor"
        return "raw_sensor_anchor"
    if ts == end_anchor.t and end_anchor.source == "sensor_fix":
        reason = accepted_fix_reason_by_id.get(end_anchor.fix_id or -1, "accepted_raw")
        if "snapped" in reason:
            return "snapped_sensor_anchor"
        if "projected" in reason:
            return "projected_sensor_anchor"
        return "raw_sensor_anchor"
    if ts == start_anchor.t or ts == end_anchor.t:
        return "google_anchor"
    if "osrm_route" in str(path_method):
        return "routed_interpolation"
    if str(path_method).startswith("baseline_subpath::"):
        return "baseline_interpolation"
    return "direct_interpolation"


def confidence_from_source(position_source: str, mode: Optional[str], path_method: str) -> Tuple[str, float]:
    if position_source in {"raw_sensor_anchor", "snapped_sensor_anchor"}:
        return "high", 0.9
    if position_source == "projected_sensor_anchor":
        return "medium", 0.75
    if position_source == "routed_interpolation":
        return "medium", 0.7
    if "gtfs_rail_shape" in str(path_method):
        return "high", 0.82
    if position_source == "baseline_interpolation":
        if mode in {"IN_TRAIN", "IN_SUBWAY", "IN_TRAM"} and "direct" in str(path_method):
            return "low", 0.4
        return "medium", 0.6
    if position_source == "google_anchor":
        return "medium", 0.6
    return "low", 0.35


def interpolate_path_between_anchors(
    start_anchor: Anchor,
    end_anchor: Anchor,
    path_coords: List[Tuple[float, float]],
    mode: Optional[str],
    segment: SegmentSpec,
    subsegment_id: int,
    path_method: str,
    correction_used: str,
    accepted_fix_reason_by_id: Dict[int, str],
    route_class: Optional[str] = None,
    route_motion_profile: Optional[Dict] = None,
    route_mode_profile: Optional[Dict] = None,
    baseline_length_m: Optional[float] = None,
    interval_start_chainage_m: Optional[float] = None,
    interval_end_chainage_m: Optional[float] = None,
    training_motion_model: Optional[Dict] = None,
) -> List[Dict]:
    if start_anchor.t > end_anchor.t:
        return []
    dt_s = float((end_anchor.t - start_anchor.t).total_seconds())
    if dt_s < 0:
        return []

    if not path_coords:
        path_coords = [(start_anchor.lat, start_anchor.lon), (end_anchor.lat, end_anchor.lon)]
    cumdist = cumulative_lengths(path_coords)
    path_length_m = float(cumdist[-1]) if len(cumdist) else 0.0
    speed_basis_mps = np.nan
    speed_source = "mode_default"
    expected_move_s = 0.0
    moving_s = 0.0
    slack_s = max(0.0, dt_s)
    slack_start_s = slack_s / 2.0
    slack_end_s = slack_s / 2.0
    dwell_total_s = 0.0
    event_distance_m: List[float] = []
    event_dwell_s: List[float] = []
    mode_schedule: List[Dict] = []

    mode_segments = build_interval_route_mode_segments(
        route_mode_profile=route_mode_profile,
        path_length_m=path_length_m,
        baseline_length_m=baseline_length_m,
        interval_start_chainage_m=interval_start_chainage_m,
        interval_end_chainage_m=interval_end_chainage_m,
        fallback_mode=mode,
    )
    if mode_segments:
        mode_schedule, mode_move_s, mode_dwell_s, mode_slack_s = build_mode_timing_schedule(mode_segments, dt_s)
        expected_move_s = mode_move_s
        moving_s = mode_move_s
        dwell_total_s = mode_dwell_s
        slack_s = mode_slack_s
        slack_start_s = slack_s / 2.0
        slack_end_s = slack_s / 2.0
        speed_basis_mps = (path_length_m / max(moving_s, 1e-9)) if moving_s > 0 else 0.0
        speed_source = "opnv_mode_profile"
    else:
        speed_basis_mps, speed_source = infer_speed_for_interval(
            mode,
            [start_anchor, end_anchor],
            training_motion_model=training_motion_model,
        )

        expected_move_s = path_length_m / max(speed_basis_mps, 0.1) if path_length_m > 0 else 0.0
        moving_s = min(dt_s, expected_move_s * SPEED_TOLERANCE_FACTOR) if dt_s > 0 else 0.0
        base_slack_s = max(0.0, dt_s - moving_s)
        dwell_plan = build_interval_intersection_dwell_plan(
            route_motion_profile=route_motion_profile,
            path_length_m=path_length_m,
            baseline_length_m=baseline_length_m,
            interval_start_chainage_m=interval_start_chainage_m,
            interval_end_chainage_m=interval_end_chainage_m,
            base_slack_s=base_slack_s,
        )
        dwell_total_s = float(dwell_plan["total_dwell_s"]) if dwell_plan.get("applied") else 0.0
        slack_s = max(0.0, dt_s - moving_s - dwell_total_s)
        slack_start_s = slack_s / 2.0
        slack_end_s = slack_s / 2.0
        event_distance_m = dwell_plan.get("event_distance_m", []) if dwell_plan.get("applied") else []
        event_dwell_s = dwell_plan.get("event_dwell_s", []) if dwell_plan.get("applied") else []

    path_method_effective = path_method
    correction_used_effective = correction_used
    speed_source_effective = speed_source
    if mode_schedule:
        path_method_effective = f"{path_method}::mode_profile"
        speed_source_effective = f"{speed_source}+walk_dwell_120s"
        if not correction_used_effective:
            correction_used_effective = "opnv_mode_profile"
        elif "opnv_mode_profile" not in correction_used_effective:
            correction_used_effective = f"{correction_used_effective}+opnv_mode_profile"
    elif len(event_distance_m) > 0:
        path_method_effective = f"{path_method}::intersection_dwell"
        speed_source_effective = f"{speed_source}+training_intersection_profile"
        if not correction_used_effective:
            correction_used_effective = "training_intersection_model"
        elif "training_intersection_model" not in correction_used_effective:
            correction_used_effective = f"{correction_used_effective}+training_intersection_model"

    grid = regular_time_grid(start_anchor.t, end_anchor.t, OUTPUT_INTERVAL_SECONDS)
    rows = []
    for ts in grid:
        elapsed_s = float((ts - start_anchor.t).total_seconds())
        if mode_schedule:
            dist_here_m = distance_along_path_with_mode_schedule(
                elapsed_s=elapsed_s,
                dt_s=dt_s,
                path_length_m=path_length_m,
                slack_start_s=slack_start_s,
                slack_end_s=slack_end_s,
                schedule=mode_schedule,
            )
        else:
            dist_here_m = distance_along_path_with_dwell(
                elapsed_s=elapsed_s,
                dt_s=dt_s,
                path_length_m=path_length_m,
                moving_s=moving_s,
                slack_start_s=slack_start_s,
                slack_end_s=slack_end_s,
                event_distance_m=event_distance_m,
                event_dwell_s=event_dwell_s,
            )
        lat, lon = interpolate_on_polyline(path_coords, cumdist, dist_here_m)
        position_source = classify_position_source(
            ts=ts,
            start_anchor=start_anchor,
            end_anchor=end_anchor,
            path_method=path_method_effective,
            accepted_fix_reason_by_id=accepted_fix_reason_by_id,
        )
        confidence_label, confidence_score = confidence_from_source(
            position_source=position_source,
            mode=segment.mode,
            path_method=path_method_effective,
        )
        rows.append({
            "timestamp": ts,
            "lat": lat,
            "lon": lon,
            "segment_id": segment.segment_id,
            "subsegment_id": subsegment_id,
            "google_row_index": segment.google_row_index,
            "segment_kind": segment.kind,
            "mode": segment.mode,
            "route_class": route_class,
            "anchor_start_source": start_anchor.source,
            "anchor_end_source": end_anchor.source,
            "anchor_start_fix_id": start_anchor.fix_id,
            "anchor_end_fix_id": end_anchor.fix_id,
            "path_method": path_method_effective,
            "correction_used": correction_used_effective,
            "path_length_m": path_length_m,
            "observed_duration_s": dt_s,
            "speed_basis_mps": speed_basis_mps,
            "speed_basis_source": speed_source_effective,
            "expected_move_duration_s": expected_move_s,
            "slack_duration_s": slack_s,
            "intersection_dwell_s": dwell_total_s,
            "position_source": position_source,
            "confidence_label": confidence_label,
            "confidence_score": confidence_score,
        })
    return rows


def snap_rail_direct_rows_to_gtfs_corridor(
    track: pd.DataFrame,
    rail_shapes: List[RailShape],
) -> pd.DataFrame:
    if track.empty or not rail_shapes:
        return track
    required = {"mode", "path_method", "lat", "lon"}
    if not required.issubset(set(track.columns)):
        return track

    mode_norm = track["mode"].astype(str).str.upper()
    direct_mask = track["path_method"].astype(str).str.contains("direct", na=False)
    rail_direct_mask = mode_norm.isin(RAIL_TRANSIT_MODES) & direct_mask
    idxs = np.where(rail_direct_mask.to_numpy())[0]
    if len(idxs) == 0:
        return track

    bbox_limit = RAIL_DIRECT_CORRIDOR_SNAP_M * RAIL_BBOX_PREFILTER_FACTOR
    corrected = 0
    for idx in idxs:
        lat = float(track.at[idx, "lat"])
        lon = float(track.at[idx, "lon"])

        best_shape = None
        best_bbox_dist = float("inf")
        for shape in rail_shapes:
            dbox = point_bbox_distance_m(lat, lon, shape)
            if dbox <= bbox_limit and dbox < best_bbox_dist:
                best_bbox_dist = dbox
                best_shape = shape
        if best_shape is None:
            continue

        best = project_point_to_polyline(lat, lon, best_shape.coords_latlon, best_shape.cumdist)
        if float(best["distance_m"]) > RAIL_DIRECT_CORRIDOR_SNAP_M:
            continue
        if best is None:
            continue

        track.at[idx, "lat"] = float(best["lat"])
        track.at[idx, "lon"] = float(best["lon"])
        if "position_source" in track.columns:
            track.at[idx, "position_source"] = "rail_corridor_snap"
        if "path_method" in track.columns:
            pm = str(track.at[idx, "path_method"])
            if "::corridor_snap" not in pm:
                track.at[idx, "path_method"] = f"{pm}::corridor_snap"
        if "confidence_label" in track.columns:
            track.at[idx, "confidence_label"] = "medium"
        if "confidence_score" in track.columns:
            score = pd.to_numeric(pd.Series([track.at[idx, "confidence_score"]]), errors="coerce").iloc[0]
            if pd.isna(score):
                score = 0.0
            track.at[idx, "confidence_score"] = float(max(score, 0.58))
        corrected += 1

    if corrected > 0:
        print(f"[INFO] Snapped {corrected:,} rail-direct rows to GTFS corridor.")
    return track


def format_duration_hms(seconds: float) -> str:
    if not np.isfinite(seconds) or seconds < 0:
        return "n/a"
    sec = int(round(seconds))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def format_bytes_mib(num_bytes: Optional[float]) -> str:
    if num_bytes is None or not np.isfinite(num_bytes):
        return "n/a"
    return f"{float(num_bytes) / (1024.0 * 1024.0):.1f} MiB"


def get_process_memory_bytes() -> Optional[float]:
    if psutil is None:
        return None
    try:
        return float(psutil.Process(os.getpid()).memory_info().rss)
    except Exception:
        return None


# ============================================================
# MAIN FUSION
# ============================================================
def process_tracks(
    google_csv: str,
    sensor_csv: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    run_started = time.time()

    sensor_candidates: List[Path] = []
    sensor_filename_ctx = None
    if sensor_csv:
        sensor_candidates = discover_sensor_csv_files(sensor_csv, google_csv=google_csv)
        if ENABLE_SENSOR_FILENAME_TIME_WINDOW and sensor_candidates:
            sensor_filename_ctx = infer_sensor_filename_time_context(sensor_candidates)
            if sensor_filename_ctx is not None:
                print(
                    "[INFO] Air-data filename coverage window: "
                    f"{sensor_filename_ctx['start_utc']} to {sensor_filename_ctx['end_utc']} UTC "
                    f"({len(sensor_filename_ctx.get('local_dates', []))} local dates from "
                    f"{sensor_filename_ctx.get('hinted_files', 0)}/{sensor_filename_ctx.get('total_files', 0)} hinted files)."
                )

    t_parse_google = time.time()
    segments = parse_google_segments(google_csv, time_context=sensor_filename_ctx)
    parse_google_elapsed = time.time() - t_parse_google
    print(f"[INFO] Parsed Google segments: {len(segments):,} in {format_duration_hms(parse_google_elapsed)}.")

    segment_ctx = infer_segment_time_context(segments)
    t_parse_sensor = time.time()
    sensor_df, sensor_file_diag = parse_sensor_gps(
        sensor_csv,
        google_csv=google_csv,
        candidate_files=sensor_candidates if sensor_candidates else None,
        time_context=segment_ctx,
    )
    parse_sensor_elapsed = time.time() - t_parse_sensor
    print(f"[INFO] Parsed sensor fixes in {format_duration_hms(parse_sensor_elapsed)}.")

    if sensor_csv and not sensor_df.empty and segments and (not USE_PRIMITIVE_SENSOR_ROUTE_ASSIGNMENT):
        pad = pd.Timedelta(minutes=SENSOR_RUNTIME_WINDOW_PADDING_MINUTES)
        sensor_start = sensor_df["timestamp"].min() - pad
        sensor_end = sensor_df["timestamp"].max() + pad
        before_n = len(segments)
        segments = [s for s in segments if overlaps_time_window(s.start_t, s.end_t, sensor_start, sensor_end)]
        if len(segments) < before_n:
            print(
                "[INFO] Refined interpolation window from sensor timestamps: "
                f"{len(segments):,}/{before_n:,} segments retained "
                f"({sensor_start} to {sensor_end} UTC)."
            )

    primitive_route_lookup = pd.DataFrame(columns=["trip_start_utc", "trip_end_utc", "trip_mid_utc", "route_class_key", "source_file"])
    file_route_class_map: Dict[str, str] = {}
    if USE_PRIMITIVE_SENSOR_ROUTE_ASSIGNMENT:
        primitive_route_lookup, file_route_class_map = build_primitive_sensor_route_lookup(sensor_file_diag)
        hinted_n = assign_route_hints_from_lookup(segments, primitive_route_lookup)
        fallback_mode_token = "opnv" if any("opnv" in str(v) for v in file_route_class_map.values()) else "bike"
        if hinted_n == 0:
            hinted_n = assign_duration_order_route_hints(segments, fallback_mode_token)
            if hinted_n > 0:
                print(
                    "[INFO] Primitive overlap produced no direct matches; "
                    f"assigned {hinted_n:,} segment hints via duration-order fallback ({fallback_mode_token})."
                )
        print(
            "[INFO] Primitive route assignment active: "
            f"{len(primitive_route_lookup):,} file windows -> {hinted_n:,} hinted segments."
        )
        if not primitive_route_lookup.empty:
            class_counts = primitive_route_lookup["route_class_key"].astype(str).value_counts().to_dict()
            print(f"[INFO] Primitive route class windows: {class_counts}")
    else:
        hinted_n = assign_time_mode_route_hints(segments)
        if hinted_n > 0:
            print(f"[INFO] Assigned rough route class hints from Google mode/time for {hinted_n:,} segments.")

    classification = (
        load_uni_route_classification(GOOGLE_UNI_CLASSIFICATION_CSV)
        if USE_CLASSIFICATION_CSV_FOR_UNI_ROUTES
        else pd.DataFrame()
    )
    classification_lookup = prepare_uni_classification_lookup(classification)
    if USE_PRIMITIVE_SENSOR_ROUTE_ASSIGNMENT:
        classification_lookup = primitive_route_lookup.copy()
    resolved_uni_route_dir = resolve_uni_route_shape_dir(UNI_ROUTE_SHAPE_DIR)
    uni_route_paths = select_uni_route_shapes_from_classification(
        classification_df=classification,
        route_dir=resolved_uni_route_dir,
        no_google_uni_csv=NO_GOOGLE_UNI_CSV,
    )
    uni_route_shapes = load_uni_route_shapes(uni_route_paths)
    opnv_mode_profiles = get_opnv_mode_profiles(OPNV_MODE_GPKG)
    training_motion_model: Dict[str, object] = {"enabled": False, "route_profiles": {}}
    if ENABLE_TRAINING_INTERSECTION_MODEL:
        training_motion_model = get_training_intersection_model(GPS_TRAINING_DIR)

    if USE_ONLY_UNI_ROUTE_SHAPES and not uni_route_shapes:
        raise RuntimeError(
            "No UNI route shapes could be loaded. "
            "Please ensure geopandas is installed and shapefiles exist in UNI_ROUTE_SHAPE_DIR."
        )
    if USE_ONLY_UNI_ROUTE_SHAPES:
        has_opnv_shapes = any(k in {"to_uni_opnv", "from_uni_opnv"} for k in uni_route_shapes.keys())
        if has_opnv_shapes and not opnv_mode_profiles:
            print(
                "[WARN] OPNV route shapes are present but no mode profile was loaded from opnv_mode.gpkg; "
                "falling back to Google mode labels/default speeds for OPNV timing."
            )

    rail_shapes: List[RailShape] = []
    if not USE_ONLY_UNI_ROUTE_SHAPES:
        t_load_shapes = time.time()
        rail_shapes = get_rail_shapes(google_csv=google_csv)
        load_shapes_elapsed = time.time() - t_load_shapes
        print(f"[INFO] Loaded rail shapes in {format_duration_hms(load_shapes_elapsed)}.")
    else:
        print("[INFO] USE_ONLY_UNI_ROUTE_SHAPES is enabled; skipping GTFS/rail shape loading.")

    if uni_route_shapes:
        print(f"[INFO] Loaded {len(uni_route_shapes)} uni route shapes for interpolation.")
    if not classification_lookup.empty:
        print(f"[INFO] Loaded {len(classification_lookup)} uni route classification intervals.")
    if opnv_mode_profiles:
        print(f"[INFO] Loaded OPNV mode profiles for {len(opnv_mode_profiles)} route classes from {OPNV_MODE_GPKG}.")
    if training_motion_model.get("enabled", False):
        print(
            "[INFO] Training intersection model active: "
            f"{training_motion_model.get('training_files_used_n', 0)} files, "
            f"stand threshold {training_motion_model.get('stand_speed_threshold_mps', np.nan):.2f} m/s, "
            f"{len(training_motion_model.get('route_profiles', {}))} route profiles."
        )

    if (not USE_ONLY_UNI_ROUTE_SHAPES) and ENABLE_STATION_SHORTEST_RAIL_PATH and any(
        normalize_mode(seg.mode) in STATION_PATH_MODES and len(seg.timeline_points) <= STATION_GAP_MAX_TIMELINE_POINTS
        for seg in segments
    ):
        t_station_graph = time.time()
        print("[INFO] Preloading GTFS station graph for station-gap routing...")
        _ = get_gtfs_station_graph(google_csv=google_csv)
        print(f"[INFO] Station graph ready in {format_duration_hms(time.time() - t_station_graph)}.")

    track_rows: List[Dict] = []
    fix_diag_rows: List[Dict] = []
    seg_diag_rows: List[Dict] = []

    total_segments = len(segments)
    progress_started = time.time()
    last_log_time = progress_started
    last_cpu_time = time.process_time()
    last_wall_time = progress_started
    print(f"[INFO] Starting interpolation loop for {total_segments:,} segments...")

    for i, seg in enumerate(segments, start=1):
        baseline = build_baseline_path(
            seg,
            rail_shapes=rail_shapes,
            uni_route_shapes=uni_route_shapes,
            uni_classification_lookup=classification_lookup,
            training_motion_model=training_motion_model,
            opnv_mode_profiles=opnv_mode_profiles,
        )
        baseline_coords = baseline["coords_latlon"]
        baseline_cumdist = cumulative_lengths(baseline_coords)
        baseline_length_m = float(baseline_cumdist[-1]) if len(baseline_cumdist) else 0.0
        baseline_route_class = baseline.get("route_class")
        baseline_route_shape_key = baseline.get("route_shape_key")
        baseline_motion_profile = baseline.get("route_motion_profile")
        baseline_mode_profile = baseline.get("route_mode_profile")
        seg_output_rows = 0

        if USE_ONLY_UNI_ROUTE_SHAPES and baseline_route_class is None:
            seg_diag_rows.append({
                "segment_id": seg.segment_id,
                "google_row_index": seg.google_row_index,
                "segment_kind": seg.kind,
                "mode": seg.mode,
                "start_time": seg.start_t,
                "end_time": seg.end_t,
                "baseline_method": baseline["method"],
                "baseline_length_m": baseline_length_m,
                "baseline_route_class": baseline_route_class,
                "baseline_route_shape_key": baseline_route_shape_key,
                "sensor_fix_candidates_n": 0,
                "sensor_fix_accepted_n": 0,
                "output_points_n": 0,
                "correction_summary": "discarded_no_matching_uni_route",
            })
            continue

        seg_sensor = filter_segment_sensor_fixes(sensor_df, seg.start_t, seg.end_t)
        seg_sensor = deduplicate_sensor_fixes(seg_sensor)

        if seg.kind in {"visit_stationary", "single_point_fallback"}:
            base_lat = seg.start_lat
            base_lon = seg.start_lon
            refined_lat, refined_lon, stationary_method, diag = refine_stationary_point(base_lat, base_lon, seg_sensor)
            for d in diag:
                d["segment_id"] = seg.segment_id
                d["google_row_index"] = seg.google_row_index
            fix_diag_rows.extend(diag)

            grid = regular_time_grid(seg.start_t, seg.end_t, OUTPUT_INTERVAL_SECONDS)
            for ts in grid:
                track_rows.append({
                    "timestamp": ts,
                    "lat": refined_lat,
                    "lon": refined_lon,
                    "segment_id": seg.segment_id,
                    "subsegment_id": 0,
                    "google_row_index": seg.google_row_index,
                    "segment_kind": seg.kind,
                    "mode": seg.mode,
                    "route_class": baseline_route_class,
                    "anchor_start_source": stationary_method,
                    "anchor_end_source": stationary_method,
                    "anchor_start_fix_id": np.nan,
                    "anchor_end_fix_id": np.nan,
                    "path_method": stationary_method,
                    "correction_used": stationary_method,
                    "path_length_m": 0.0,
                    "observed_duration_s": float((seg.end_t - seg.start_t).total_seconds()),
                    "speed_basis_mps": 0.0,
                    "speed_basis_source": "stationary",
                    "expected_move_duration_s": 0.0,
                    "slack_duration_s": float((seg.end_t - seg.start_t).total_seconds()),
                    "intersection_dwell_s": 0.0,
                    "position_source": "stationary_point",
                    "confidence_label": "medium" if stationary_method != "google_stationary_only" else "low",
                    "confidence_score": 0.75 if stationary_method != "google_stationary_only" else 0.45,
                })
            seg_output_rows = len(grid)

            seg_diag_rows.append({
                "segment_id": seg.segment_id,
                "google_row_index": seg.google_row_index,
                "segment_kind": seg.kind,
                "mode": seg.mode,
                "start_time": seg.start_t,
                "end_time": seg.end_t,
                "baseline_method": baseline["method"],
                "baseline_length_m": baseline_length_m,
                "baseline_route_class": baseline_route_class,
                "baseline_route_shape_key": baseline_route_shape_key,
                "sensor_fix_candidates_n": len(seg_sensor),
                "sensor_fix_accepted_n": sum(d["status"] == "accepted" for d in diag),
                "output_points_n": seg_output_rows,
                "correction_summary": stationary_method,
            })
        else:
            accepted_fixes, diag = evaluate_moving_fixes(seg, seg_sensor, baseline_coords, baseline_cumdist)
            fix_diag_rows.extend(diag)
            accepted_fix_reason_by_id = {
                int(d["fix_id"]): str(d.get("reason", "accepted_raw"))
                for d in diag
                if d.get("status") == "accepted" and pd.notna(d.get("fix_id"))
            }

            anchors: List[Anchor] = [
                Anchor(seg.start_lat, seg.start_lon, seg.start_t, source="google_start", chainage_m=0.0),
                *accepted_fixes,
                Anchor(seg.end_lat, seg.end_lon, seg.end_t, source="google_end", chainage_m=baseline_length_m),
            ]
            anchors = sorted(anchors, key=lambda a: (a.t, a.chainage_m if a.chainage_m is not None else np.inf))

            # Collapse exact duplicates in time by keeping sensor fixes over Google anchors.
            collapsed: List[Anchor] = []
            priority = {"sensor_fix": 3, "google_end": 2, "google_start": 1}
            for a in anchors:
                if collapsed and a.t == collapsed[-1].t:
                    if priority.get(a.source, 0) >= priority.get(collapsed[-1].source, 0):
                        collapsed[-1] = a
                else:
                    collapsed.append(a)
            anchors = collapsed

            # Enforce non-decreasing chainage if available.
            monotonic: List[Anchor] = []
            last_chainage = -np.inf
            for a in anchors:
                ch = a.chainage_m if a.chainage_m is not None and pd.notna(a.chainage_m) else last_chainage
                if ch + BACKTRACK_TOLERANCE_M < last_chainage and a.source == "sensor_fix":
                    fix_diag_rows.append({
                        "segment_id": seg.segment_id,
                        "google_row_index": seg.google_row_index,
                        "segment_kind": seg.kind,
                        "mode": seg.mode,
                        "fix_id": a.fix_id,
                        "timestamp": a.t,
                        "gps_mode": np.nan,
                        "raw_lat": a.raw_lat,
                        "raw_lon": a.raw_lon,
                        "gps_speed_m_s": a.speed_mps,
                        "gps_eph_m": a.eph_m,
                        "baseline_distance_m": np.nan,
                        "baseline_chainage_m": ch,
                        "allowed_distance_m": np.nan,
                        "snap_distance_m": np.nan,
                        "snap_method": "post_filter",
                        "adjusted_lat": np.nan,
                        "adjusted_lon": np.nan,
                        "adjusted_chainage_m": np.nan,
                        "status": "rejected",
                        "reason": "post_anchor_monotonicity_filter",
                        "quality_weight": fix_quality_weight(3.0, a.eph_m),
                    })
                    continue
                last_chainage = max(last_chainage, ch)
                monotonic.append(a)
            anchors = monotonic

            subsegment_count = 0
            correction_used = "google_only"
            if any(a.source == "sensor_fix" for a in anchors):
                correction_used = "google_plus_sensor_fixes"

            for a, b in zip(anchors[:-1], anchors[1:]):
                if b.t < a.t:
                    continue
                interval_start_chainage = None
                interval_end_chainage = None
                if baseline_length_m > 0 and a.chainage_m is not None and b.chainage_m is not None and b.chainage_m >= a.chainage_m:
                    subpath = extract_subpolyline(baseline_coords, baseline_cumdist, float(a.chainage_m), float(b.chainage_m))
                    path_method = f"baseline_subpath::{baseline['method']}"
                    interval_start_chainage = float(a.chainage_m)
                    interval_end_chainage = float(b.chainage_m)
                else:
                    sub = build_path_for_points(seg.mode, [(a.lat, a.lon), (b.lat, b.lon)])
                    subpath = sub["coords_latlon"]
                    path_method = sub["method"]

                rows = interpolate_path_between_anchors(
                    start_anchor=a,
                    end_anchor=b,
                    path_coords=subpath,
                    mode=seg.mode,
                    segment=seg,
                    subsegment_id=subsegment_count,
                    path_method=path_method,
                    correction_used=correction_used,
                    accepted_fix_reason_by_id=accepted_fix_reason_by_id,
                    route_class=baseline_route_class,
                    route_motion_profile=baseline_motion_profile,
                    route_mode_profile=baseline_mode_profile,
                    baseline_length_m=baseline_length_m,
                    interval_start_chainage_m=interval_start_chainage,
                    interval_end_chainage_m=interval_end_chainage,
                    training_motion_model=training_motion_model,
                )
                track_rows.extend(rows)
                seg_output_rows += len(rows)
                subsegment_count += 1

            accepted_n = sum(d["status"] == "accepted" for d in diag)
            seg_diag_rows.append({
                "segment_id": seg.segment_id,
                "google_row_index": seg.google_row_index,
                "segment_kind": seg.kind,
                "mode": seg.mode,
                "start_time": seg.start_t,
                "end_time": seg.end_t,
                "baseline_method": baseline["method"],
                "baseline_length_m": baseline_length_m,
                "baseline_route_class": baseline_route_class,
                "baseline_route_shape_key": baseline_route_shape_key,
                "sensor_fix_candidates_n": len(seg_sensor),
                "sensor_fix_accepted_n": accepted_n,
                "output_points_n": seg_output_rows,
                "correction_summary": correction_used,
            })

        now = time.time()
        should_log = (
            i == 1
            or i == total_segments
            or (PROGRESS_LOG_EVERY_SEGMENTS > 0 and i % PROGRESS_LOG_EVERY_SEGMENTS == 0)
            or (now - last_log_time) >= PROGRESS_LOG_EVERY_SECONDS
        )
        if should_log:
            elapsed = now - progress_started
            rate = (i / elapsed) if elapsed > 0 else np.nan
            remaining = max(0, total_segments - i)
            eta_s = (remaining / rate) if rate and np.isfinite(rate) and rate > 0 else np.nan

            cpu_now = time.process_time()
            wall_delta = max(now - last_wall_time, 1e-9)
            cpu_delta = max(cpu_now - last_cpu_time, 0.0)
            cpu_pct = 100.0 * (cpu_delta / wall_delta)
            last_cpu_time = cpu_now
            last_wall_time = now
            last_log_time = now

            mem_text = format_bytes_mib(get_process_memory_bytes())
            print(
                f"[PROGRESS] Segments {i:,}/{total_segments:,} ({(100.0 * i / max(total_segments, 1)):.1f}%) | "
                f"elapsed {format_duration_hms(elapsed)} | eta {format_duration_hms(eta_s)} | "
                f"rate {rate:.2f} seg/s | cpu~{cpu_pct:.0f}% | mem {mem_text} | "
                f"track_rows {len(track_rows):,}"
            )

    track = pd.DataFrame(track_rows)
    fixes = pd.DataFrame(fix_diag_rows)
    segs = pd.DataFrame(seg_diag_rows)

    if not track.empty:
        source_priority = {
            "raw_sensor_anchor": 6,
            "snapped_sensor_anchor": 5,
            "projected_sensor_anchor": 4,
            "stationary_point": 4,
            "routed_interpolation": 3,
            "baseline_interpolation": 2,
            "google_anchor": 1,
            "direct_interpolation": 0,
        }
        track["confidence_score"] = pd.to_numeric(track.get("confidence_score"), errors="coerce").fillna(0.0)
        track["position_priority"] = track.get("position_source", pd.Series(index=track.index)).map(source_priority).fillna(0)
        track = track.sort_values(
            ["timestamp", "confidence_score", "position_priority", "path_length_m", "segment_id", "subsegment_id"],
            ascending=[True, False, False, False, True, True],
        )
        track = track.drop_duplicates(subset=["timestamp"], keep="first")
        track = track.sort_values(["timestamp"]).reset_index(drop=True)
        if not USE_ONLY_UNI_ROUTE_SHAPES:
            track = snap_rail_direct_rows_to_gtfs_corridor(track, rail_shapes)
        track["timestamp_local"] = track["timestamp"].dt.tz_convert(LOCAL_TIMEZONE)
        track["date_local"] = track["timestamp_local"].dt.date
        track["time_local"] = track["timestamp_local"].dt.strftime("%H:%M:%S")
        track["unix_time_s"] = (track["timestamp"].astype("int64") // 10**9).astype("int64")
        track["is_exact_grid"] = (track["unix_time_s"] % OUTPUT_INTERVAL_SECONDS == 0)
        track["delta_s"] = track["timestamp"].diff().dt.total_seconds()

        step_distance = [np.nan]
        for j in range(1, len(track)):
            step_distance.append(
                haversine_m(
                    float(track.at[j - 1, "lat"]),
                    float(track.at[j - 1, "lon"]),
                    float(track.at[j, "lat"]),
                    float(track.at[j, "lon"]),
                )
            )
        track["step_distance_m"] = step_distance
        track["step_speed_m_s"] = track["step_distance_m"] / track["delta_s"].replace(0, np.nan)
        track["jump_flag"] = track["step_speed_m_s"] > 70.0
        track.loc[track["jump_flag"], "confidence_label"] = "low"
        track.loc[track["jump_flag"], "confidence_score"] = np.minimum(
            pd.to_numeric(track.loc[track["jump_flag"], "confidence_score"], errors="coerce").fillna(0.0),
            0.45,
        )
        track = track.drop(columns=["position_priority"])

    if not fixes.empty:
        fixes = fixes.sort_values(["timestamp", "segment_id", "fix_id"]).reset_index(drop=True)
        fixes["timestamp_local"] = fixes["timestamp"].dt.tz_convert(LOCAL_TIMEZONE)

    if not segs.empty:
        segs = segs.sort_values(["segment_id"]).reset_index(drop=True)
        segs["start_time_local"] = pd.to_datetime(segs["start_time"], utc=True).dt.tz_convert(LOCAL_TIMEZONE)
        segs["end_time_local"] = pd.to_datetime(segs["end_time"], utc=True).dt.tz_convert(LOCAL_TIMEZONE)

    sensor_file_audit = summarize_sensor_file_audit(sensor_file_diag=sensor_file_diag, fixes=fixes)

    print(
        f"[INFO] Fusion completed in {format_duration_hms(time.time() - run_started)} "
        f"for {len(segments):,} segments -> {len(track):,} track rows."
    )
    return track, fixes, segs, sensor_file_audit, file_route_class_map


# ============================================================
# SAVE OUTPUTS
# ============================================================
def resolve_output_dir(google_csv: str, output_dir: Optional[str]) -> Path:
    if output_dir:
        out_dir = Path(output_dir)
    else:
        out_dir = Path(google_csv).expanduser().resolve().parent
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _wipe_path_recursive(path: Path, remove_self: bool = True) -> int:
    removed = 0
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
            return 1
        if path.is_dir():
            for child in path.iterdir():
                removed += _wipe_path_recursive(child, remove_self=True)
            if remove_self:
                path.rmdir()
                removed += 1
            return removed
    except Exception as exc:
        print(f"[WARN] Could not remove '{path}': {exc}")
    return removed


def wipe_output_directory_contents(output_dir: Path) -> int:
    out_dir = output_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    if len(out_dir.parts) < 3:
        raise ValueError(f"Refusing to wipe unsafe output path: {out_dir}")

    removed = 0
    for child in list(out_dir.iterdir()):
        if child.is_dir():
            removed += _wipe_path_recursive(child, remove_self=False)
        else:
            removed += _wipe_path_recursive(child, remove_self=True)

    leftover_files = []
    for p in out_dir.rglob("*"):
        if p.is_file():
            leftover_files.append(p)
    if leftover_files:
        print(
            "[WARN] Output directory not fully empty before run; "
            f"{len(leftover_files)} file(s) remain locked/inaccessible."
        )
    return removed


def save_csv_with_fallback(df: pd.DataFrame, target_path: Path) -> Path:
    try:
        df.to_csv(target_path, index=False)
        return target_path
    except PermissionError:
        stamp = pd.Timestamp.now("UTC").strftime("%Y%m%d_%H%M%S")
        fallback = target_path.with_name(f"{target_path.stem}_{stamp}{target_path.suffix}")
        df.to_csv(fallback, index=False)
        print(f"[WARN] File locked, wrote fallback output instead: {fallback}")
        return fallback


def summarize_sensor_file_audit(sensor_file_diag: pd.DataFrame, fixes: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "source_file",
        "source_name",
        "context_match",
        "selected_for_loading",
        "status",
        "reason",
        "detail",
        "rows_parsed",
        "rows_after_time_window",
        "rows_loaded",
        "fixes_considered_n",
        "accepted_fixes_n",
        "rejected_fixes_n",
        "top_rejection_reason",
        "final_status",
        "final_reason",
    ]
    if sensor_file_diag.empty:
        return pd.DataFrame(columns=base_cols)

    audit = sensor_file_diag.copy()
    if "source_file" not in audit.columns:
        audit["source_file"] = ""
    if "source_name" not in audit.columns:
        audit["source_name"] = audit["source_file"].apply(lambda x: Path(str(x)).name if str(x) else "")

    audit["source_file"] = audit["source_file"].astype(str)
    audit["source_name"] = audit["source_name"].astype(str)

    if not fixes.empty and {"source_file", "status", "reason"}.issubset(set(fixes.columns)):
        fx = fixes.copy()
        fx = fx[fx["source_file"].notna()].copy()
        fx["source_file"] = fx["source_file"].astype(str)

        grouped = fx.groupby("source_file")
        stats = grouped["status"].agg(
            fixes_considered_n="size",
            accepted_fixes_n=lambda s: int((s.astype(str) == "accepted").sum()),
        ).reset_index()
        stats["rejected_fixes_n"] = stats["fixes_considered_n"] - stats["accepted_fixes_n"]

        rejected = fx[fx["status"].astype(str) != "accepted"].copy()
        if rejected.empty:
            top_reject = pd.DataFrame(columns=["source_file", "top_rejection_reason"])
        else:
            top_reject = (
                rejected.groupby("source_file")["reason"]
                .agg(lambda s: str(s.value_counts(dropna=True).index[0]) if len(s.value_counts(dropna=True)) else "")
                .reset_index(name="top_rejection_reason")
            )

        stats = stats.merge(top_reject, on="source_file", how="left")
        overlap = [c for c in ["fixes_considered_n", "accepted_fixes_n", "rejected_fixes_n", "top_rejection_reason"] if c in audit.columns]
        if overlap:
            audit = audit.drop(columns=overlap)
        audit = audit.merge(stats, on="source_file", how="left")

        for c in ["fixes_considered_n", "accepted_fixes_n", "rejected_fixes_n"]:
            if c not in audit.columns:
                audit[c] = 0
            audit[c] = pd.to_numeric(audit[c], errors="coerce").fillna(0).astype(int)
        if "top_rejection_reason" not in audit.columns:
            audit["top_rejection_reason"] = ""
        audit["top_rejection_reason"] = audit["top_rejection_reason"].fillna("").astype(str)
    else:
        if "fixes_considered_n" not in audit.columns:
            audit["fixes_considered_n"] = 0
        if "accepted_fixes_n" not in audit.columns:
            audit["accepted_fixes_n"] = 0
        if "rejected_fixes_n" not in audit.columns:
            audit["rejected_fixes_n"] = 0
        if "top_rejection_reason" not in audit.columns:
            audit["top_rejection_reason"] = ""
        audit["fixes_considered_n"] = pd.to_numeric(audit["fixes_considered_n"], errors="coerce").fillna(0).astype(int)
        audit["accepted_fixes_n"] = pd.to_numeric(audit["accepted_fixes_n"], errors="coerce").fillna(0).astype(int)
        audit["rejected_fixes_n"] = pd.to_numeric(audit["rejected_fixes_n"], errors="coerce").fillna(0).astype(int)
        audit["top_rejection_reason"] = audit["top_rejection_reason"].fillna("").astype(str)

    audit["final_status"] = np.where(audit["status"].astype(str) == "loaded_for_fusion", "processed", "sorted_out")
    audit["final_reason"] = audit["reason"].astype(str)

    if AUDIT_REQUIRE_ACCEPTED_FIXES:
        loaded_mask = audit["status"].astype(str) == "loaded_for_fusion"
        no_fix_rows_mask = loaded_mask & (audit["fixes_considered_n"] <= 0)
        all_rejected_mask = loaded_mask & (audit["fixes_considered_n"] > 0) & (audit["accepted_fixes_n"] <= 0)
        accepted_mask = loaded_mask & (audit["accepted_fixes_n"] > 0)

        audit.loc[no_fix_rows_mask, "final_status"] = "sorted_out"
        audit.loc[no_fix_rows_mask, "final_reason"] = "no_fix_rows_reached_segment_evaluation"

        audit.loc[all_rejected_mask, "final_status"] = "sorted_out"
        audit.loc[all_rejected_mask, "final_reason"] = (
            "all_fixes_rejected::" + audit.loc[all_rejected_mask, "top_rejection_reason"].replace("", "unknown")
        )

        audit.loc[accepted_mask, "final_status"] = "processed"
        audit.loc[accepted_mask, "final_reason"] = "accepted_fixes_used_in_fusion"
    else:
        loaded_mask = audit["status"].astype(str) == "loaded_for_fusion"
        audit.loc[loaded_mask, "final_status"] = "processed"
        audit.loc[loaded_mask, "final_reason"] = "loaded_for_fusion_used_for_route_interpolation"

    audit = audit.reindex(columns=base_cols)
    audit = audit.sort_values(["final_status", "source_name", "source_file"]).reset_index(drop=True)
    return audit


def build_sorted_out_filename(source_name: str, run_date_tag: str) -> str:
    p = Path(str(source_name))
    stem = p.stem if p.stem else "sorted_out"
    return f"{stem}_{SORTED_OUT_FILENAME_TAG}_{run_date_tag}.csv"


def save_sorted_out_sensor_file_reports(
    sensor_file_audit: pd.DataFrame,
    output_dir: Path,
) -> Tuple[List[Path], Optional[Path]]:
    if sensor_file_audit.empty:
        return [], None
    if "final_status" not in sensor_file_audit.columns:
        return [], None

    sorted_rows = sensor_file_audit[sensor_file_audit["final_status"].astype(str) == "sorted_out"].copy()
    sorted_out_dir = output_dir / SORTED_OUT_OUTPUT_SUBDIR
    sorted_out_dir.mkdir(parents=True, exist_ok=True)

    run_date_tag = pd.Timestamp.now(tz=LOCAL_TIMEZONE).strftime("%Y%m%d")
    summary_name = f"sorted_out_sensor_files_{run_date_tag}.csv"
    summary_path = save_csv_with_fallback(sorted_rows, sorted_out_dir / summary_name)

    saved_paths: List[Path] = []
    for idx, row in sorted_rows.reset_index(drop=True).iterrows():
        source_name = str(row.get("source_name", "")) or str(row.get("source_file", f"sorted_{idx+1:04d}"))
        file_name = f"{idx + 1:04d}_{build_sorted_out_filename(source_name, run_date_tag)}"
        row_df = pd.DataFrame([row.to_dict()])
        saved = save_csv_with_fallback(row_df, sorted_out_dir / file_name)
        saved_paths.append(saved)

    return saved_paths, summary_path


def build_individual_interp_filename(source_name: str, route_class: Optional[str] = None) -> str:
    stem = Path(str(source_name)).stem
    if not stem:
        stem = "unknown_source"
    class_token = canonical_uni_route_class(route_class)
    if class_token is None:
        class_token = "unclassified"
    return f"{stem}_{class_token}_interp.csv"


def infer_route_class_for_subset(subset: pd.DataFrame) -> Tuple[Optional[str], float, Dict[str, float]]:
    if subset.empty or "route_class" not in subset.columns:
        return None, 0.0, {}
    route_series = subset["route_class"].apply(canonical_uni_route_class)
    route_series = route_series[route_series.isin(ALLOWED_UNI_ROUTE_CLASSES)]
    if route_series.empty:
        return None, 0.0, {}

    counts = route_series.value_counts(dropna=True)
    duration_s_by_class = {str(k): float(v) * float(OUTPUT_INTERVAL_SECONDS) for k, v in counts.to_dict().items()}
    if not duration_s_by_class:
        return None, 0.0, {}
    best_class = max(duration_s_by_class.items(), key=lambda kv: kv[1])[0]
    best_duration_s = float(duration_s_by_class.get(best_class, 0.0))
    if best_duration_s < float(MIN_ROUTE_CLASS_SPAN_MINUTES_PER_FILE) * 60.0:
        return None, best_duration_s, duration_s_by_class
    return best_class, best_duration_s, duration_s_by_class


def infer_mode_from_route_class(route_class: Optional[str]) -> Optional[str]:
    key = canonical_uni_route_class(route_class)
    if key is None:
        return None
    if key.endswith("_bike"):
        return "CYCLING"
    if key.endswith("_opnv"):
        return "IN_TRAIN"
    return None


def synthesize_track_rows_from_route_shape(
    start_t: pd.Timestamp,
    end_t: pd.Timestamp,
    route_class: Optional[str],
    route_shapes: Dict[str, List[Tuple[float, float]]],
    template_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    key = canonical_uni_route_class(route_class)
    if key is None or key not in ALLOWED_UNI_ROUTE_CLASSES:
        return pd.DataFrame(columns=template_columns or [])

    coords = route_shapes.get(key, [])
    if not coords:
        return pd.DataFrame(columns=template_columns or [])

    start_ts = pd.to_datetime(start_t, utc=True, errors="coerce")
    end_ts = pd.to_datetime(end_t, utc=True, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts) or end_ts < start_ts:
        return pd.DataFrame(columns=template_columns or [])

    grid = regular_time_grid(start_ts, end_ts, OUTPUT_INTERVAL_SECONDS)
    if not grid:
        step_ns = int(OUTPUT_INTERVAL_SECONDS * 1e9)
        snapped_ns = (int(start_ts.value) // step_ns) * step_ns
        grid = [pd.Timestamp(snapped_ns, tz="UTC")]

    cumdist = cumulative_lengths(coords)
    total_m = float(cumdist[-1]) if len(cumdist) else 0.0
    duration_s = max(float((end_ts - start_ts).total_seconds()), float(OUTPUT_INTERVAL_SECONDS))
    mode = infer_mode_from_route_class(key)

    rows: List[Dict[str, object]] = []
    n = len(grid)
    for i, ts in enumerate(grid):
        rel = (float(i) / float(n - 1)) if n > 1 else 0.0
        target_m = rel * total_m
        lat, lon = interpolate_on_polyline(coords, cumdist, target_m) if len(cumdist) else coords[0]
        rows.append({
            "timestamp": ts,
            "lat": float(lat),
            "lon": float(lon),
            "segment_id": np.nan,
            "subsegment_id": 0,
            "google_row_index": np.nan,
            "segment_kind": "synthetic_file_window_route",
            "mode": mode,
            "route_class": key,
            "anchor_start_source": "synthetic_shape_start",
            "anchor_end_source": "synthetic_shape_end",
            "anchor_start_fix_id": np.nan,
            "anchor_end_fix_id": np.nan,
            "path_method": "synthetic_shape_stretch",
            "correction_used": "shape_only_no_track_rows",
            "path_length_m": total_m,
            "observed_duration_s": duration_s,
            "speed_basis_mps": (total_m / duration_s) if duration_s > 0 else 0.0,
            "speed_basis_source": "window_length_shape",
            "expected_move_duration_s": duration_s,
            "slack_duration_s": 0.0,
            "intersection_dwell_s": 0.0,
            "position_source": "synthetic_route_shape",
            "confidence_label": "low",
            "confidence_score": 0.35,
        })

    out = pd.DataFrame(rows)
    if template_columns:
        for col in template_columns:
            if col not in out.columns:
                out[col] = np.nan
        out = out[[*template_columns, *[c for c in out.columns if c not in template_columns]]]
    return out


def save_individual_interpolated_outputs(
    track: pd.DataFrame,
    fixes: pd.DataFrame,
    sensor_file_audit: pd.DataFrame,
    output_dir: Path,
    file_route_class_map: Optional[Dict[str, str]] = None,
) -> Tuple[List[Path], Optional[Path]]:
    if track.empty or sensor_file_audit.empty:
        return [], None
    if "timestamp" not in track.columns:
        return [], None

    audit = sensor_file_audit.copy()
    if "source_file" not in audit.columns:
        return [], None
    if "source_name" not in audit.columns:
        audit["source_name"] = audit["source_file"].apply(lambda x: Path(str(x)).name if str(x) else "")

    if INDIVIDUAL_EXPORT_INCLUDE_LOADED_FILES and "selected_for_loading" in audit.columns:
        sel = audit["selected_for_loading"]
        if sel.dtype == bool:
            mask = sel.fillna(False)
        else:
            mask = sel.astype(str).str.lower().isin({"true", "1", "yes", "y"})
        eligible = audit[mask].copy()
    elif INDIVIDUAL_EXPORT_INCLUDE_LOADED_FILES and "status" in audit.columns:
        eligible = audit[audit["status"].astype(str).isin({"loaded_for_fusion", "sorted_out"})].copy()
    elif "final_status" in audit.columns:
        eligible = audit[audit["final_status"].astype(str) == "processed"].copy()
    elif "status" in audit.columns:
        eligible = audit[audit["status"].astype(str) == "loaded_for_fusion"].copy()
    else:
        eligible = audit.copy()
    if eligible.empty:
        return [], None

    interp_dir = output_dir / INDIVIDUAL_INTERP_OUTPUT_SUBDIR
    interp_dir.mkdir(parents=True, exist_ok=True)

    track_ts = pd.to_datetime(track["timestamp"], utc=True, errors="coerce")
    track_idx = track_ts.notna()
    track_valid = track.loc[track_idx].copy()
    track_valid["timestamp"] = track_ts.loc[track_idx]
    if track_valid.empty:
        return [], None
    track_cols = list(track_valid.columns)

    route_shape_dir = Path(resolve_uni_route_shape_dir(UNI_ROUTE_SHAPE_DIR))
    route_shape_paths = list_valid_uni_route_shape_paths(route_shape_dir) if route_shape_dir.exists() else []
    route_shapes_for_synth = load_uni_route_shapes(route_shape_paths) if route_shape_paths else {}
    require_file_route_class = bool(USE_PRIMITIVE_SENSOR_ROUTE_ASSIGNMENT)

    fix_windows: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {}
    if (
        not fixes.empty
        and "source_file" in fixes.columns
        and "timestamp" in fixes.columns
    ):
        fx = fixes.copy()
        fx["timestamp"] = pd.to_datetime(fx["timestamp"], utc=True, errors="coerce")
        fx = fx[fx["timestamp"].notna() & fx["source_file"].notna()].copy()
        if not fx.empty:
            agg = fx.groupby(fx["source_file"].astype(str))["timestamp"].agg(["min", "max"]).reset_index()
            for row in agg.itertuples(index=False):
                fix_windows[str(row.source_file)] = (row.min, row.max)

    pad = pd.Timedelta(minutes=INDIVIDUAL_INTERP_TIME_PADDING_MINUTES)
    used_names: Set[str] = set()
    saved_paths: List[Path] = []
    summary_rows: List[Dict] = []
    air_metrics_cache: Dict[str, pd.DataFrame] = {}

    for row in eligible.itertuples(index=False):
        source_file = str(getattr(row, "source_file", "") or "")
        source_name = str(getattr(row, "source_name", "") or Path(source_file).name)
        if not source_file and not source_name:
            continue

        window = fix_windows.get(source_file)
        if window is None and source_file:
            p = Path(source_file)
            if p.exists() and p.is_file():
                part_ts = parse_sensor_timestamps_only(p)
                if not part_ts.empty and "timestamp" in part_ts.columns:
                    ts = pd.to_datetime(part_ts["timestamp"], utc=True, errors="coerce").dropna()
                    if not ts.empty:
                        window = (ts.min(), ts.max())

        file_duration_s = np.nan
        start_t = pd.NaT
        end_t = pd.NaT
        if window is not None:
            file_duration_s = float((window[1] - window[0]).total_seconds())
            if file_duration_s < float(MIN_ROUTE_CLASS_SPAN_MINUTES_PER_FILE) * 60.0:
                continue
            start_t = window[0] - pad
            end_t = window[1] + pad
            subset = track_valid[
                (track_valid["timestamp"] >= start_t)
                & (track_valid["timestamp"] <= end_t)
            ].copy()
        else:
            subset = pd.DataFrame(columns=track_valid.columns)

        known_route_class = None
        if isinstance(file_route_class_map, dict) and source_file:
            known_route_class = canonical_uni_route_class(file_route_class_map.get(source_file))
        if require_file_route_class and known_route_class not in ALLOWED_UNI_ROUTE_CLASSES:
            continue

        route_durations_s: Dict[str, float] = {}
        assigned_route_span_s = 0.0
        route_geometry_source = "track_window"
        if known_route_class in ALLOWED_UNI_ROUTE_CLASSES:
            assigned_route_class = known_route_class
        else:
            assigned_route_class, assigned_route_span_s, route_durations_s = infer_route_class_for_subset(subset)
            if assigned_route_class is None:
                continue

        if not subset.empty and "route_class" in subset.columns:
            route_series = subset["route_class"].apply(canonical_uni_route_class)
            counts = route_series.value_counts(dropna=True)
            route_durations_s = {
                str(k): float(v) * float(OUTPUT_INTERVAL_SECONDS)
                for k, v in counts.to_dict().items()
            }
            subset_match = subset[route_series == assigned_route_class].copy()
            if not subset_match.empty:
                subset = subset_match

        if subset.empty and window is not None:
            subset = synthesize_track_rows_from_route_shape(
                start_t=start_t,
                end_t=end_t,
                route_class=assigned_route_class,
                route_shapes=route_shapes_for_synth,
                template_columns=track_cols,
            )
            if not subset.empty:
                route_geometry_source = "synthetic_shape"

        if subset.empty:
            continue

        if assigned_route_span_s <= 0.0:
            assigned_route_span_s = float(len(subset) * OUTPUT_INTERVAL_SECONDS)
        if not route_durations_s and assigned_route_class in ALLOWED_UNI_ROUTE_CLASSES:
            route_durations_s = {str(assigned_route_class): assigned_route_span_s}

        air_cols_added = 0
        if source_file:
            if source_file not in air_metrics_cache:
                p_air = Path(source_file)
                air_metrics_cache[source_file] = (
                    parse_sensor_air_metrics(p_air) if p_air.exists() and p_air.is_file() else pd.DataFrame(columns=["timestamp"])
                )
            air_df = air_metrics_cache.get(source_file, pd.DataFrame(columns=["timestamp"]))
            if not air_df.empty:
                before_cols = set(subset.columns)
                subset = merge_air_metrics_onto_track_rows(subset, air_df)
                after_cols = set(subset.columns)
                air_cols_added = int(len(after_cols - before_cols))

        base_name = build_individual_interp_filename(source_name, route_class=assigned_route_class)
        file_name = base_name
        if file_name.lower() in used_names:
            k = 2
            while True:
                alt = f"{Path(base_name).stem}_{k:02d}{Path(base_name).suffix}"
                if alt.lower() not in used_names:
                    file_name = alt
                    break
                k += 1
        used_names.add(file_name.lower())

        target = interp_dir / file_name
        saved = save_csv_with_fallback(subset, target)
        saved_paths.append(saved)

        summary_rows.append({
            "source_file": source_file,
            "source_name": source_name,
            "output_file": str(saved),
            "track_rows_written": int(len(subset)),
            "window_start_utc": subset["timestamp"].min(),
            "window_end_utc": subset["timestamp"].max(),
            "assigned_route_class": assigned_route_class or "",
            "assigned_route_span_min": float(assigned_route_span_s / 60.0),
            "route_class_threshold_min": float(MIN_ROUTE_CLASS_SPAN_MINUTES_PER_FILE),
            "route_class_duration_s_by_class": json.dumps(route_durations_s, ensure_ascii=False),
            "file_duration_min": float(file_duration_s / 60.0) if window is not None else np.nan,
            "air_metric_columns_added_n": int(air_cols_added),
            "route_geometry_source": route_geometry_source,
        })

    summary_path: Optional[Path] = None
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        run_date_tag = pd.Timestamp.now(tz=LOCAL_TIMEZONE).strftime("%Y%m%d")
        summary_name = f"individual_interpolated_files_{run_date_tag}.csv"
        summary_path = save_csv_with_fallback(summary_df, interp_dir / summary_name)

    return saved_paths, summary_path


def parse_cli_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Interpolate Google Timeline tracks along local UNI route shapefiles.",
    )
    parser.add_argument(
        "--google-timeline-csv",
        default=GOOGLE_TIMELINE_CSV,
        help="Path to flattened Google Timeline CSV.",
    )
    parser.add_argument(
        "--sensor-gps-csv",
        default=SENSOR_GPS_CSV,
        help="Path to Enviro sensor CSV or folder. Use 'none' to disable sensor fusion.",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help="Output directory. Default: same folder as Google CSV.",
    )
    return parser.parse_args(argv)


def _normalize_optional_path(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "null", "off", "false"}:
        return None
    return text


def apply_cli_overrides(args: argparse.Namespace) -> None:
    global GOOGLE_TIMELINE_CSV, SENSOR_GPS_CSV, OUTPUT_DIR, GTFS_RAIL_ZIP
    global USE_OSRM, USE_OSRM_SENSOR_SNAP
    global USE_ONLY_UNI_ROUTE_SHAPES, USE_CLASSIFICATION_CSV_FOR_UNI_ROUTES, ENABLE_TRAINING_INTERSECTION_MODEL
    global AUTO_FIND_GTFS_RAIL_ZIP
    global _OSRM_RUNTIME_ENABLED, _OSRM_HEALTHCHECK_DONE, _OSRM_CONSECUTIVE_FAILURES
    global _RAIL_SHAPES_CACHE, _RAIL_SHAPES_SOURCE
    global _GTFS_STATION_GRAPH_CACHE, _GTFS_STATION_GRAPH_SOURCE
    global _STATION_CANDIDATE_CACHE
    global _TRAINING_INTERSECTION_MODEL_CACHE, _TRAINING_INTERSECTION_MODEL_SOURCE
    global _OPNV_MODE_PROFILE_CACHE, _OPNV_MODE_PROFILE_SOURCE

    GOOGLE_TIMELINE_CSV = str(args.google_timeline_csv).strip()
    SENSOR_GPS_CSV = _normalize_optional_path(args.sensor_gps_csv)
    OUTPUT_DIR = _normalize_optional_path(args.output_dir)
    GTFS_RAIL_ZIP = None

    # Forced by user workflow: only custom UNI route shapefiles; no external map/rail routing.
    USE_ONLY_UNI_ROUTE_SHAPES = True
    USE_CLASSIFICATION_CSV_FOR_UNI_ROUTES = False
    ENABLE_TRAINING_INTERSECTION_MODEL = False
    GTFS_RAIL_ZIP = None
    AUTO_FIND_GTFS_RAIL_ZIP = False
    USE_OSRM = False
    USE_OSRM_SENSOR_SNAP = False

    # Reset run-scoped caches/state after CLI overrides.
    _OSRM_RUNTIME_ENABLED = USE_OSRM
    _OSRM_HEALTHCHECK_DONE = False
    _OSRM_CONSECUTIVE_FAILURES = 0
    _RAIL_SHAPES_CACHE = None
    _RAIL_SHAPES_SOURCE = None
    _GTFS_STATION_GRAPH_CACHE = None
    _GTFS_STATION_GRAPH_SOURCE = None
    _STATION_CANDIDATE_CACHE = {}
    _TRAINING_INTERSECTION_MODEL_CACHE = None
    _TRAINING_INTERSECTION_MODEL_SOURCE = None
    _OPNV_MODE_PROFILE_CACHE = None
    _OPNV_MODE_PROFILE_SOURCE = None


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_cli_args(argv)
    apply_cli_overrides(args)

    out_dir = resolve_output_dir(GOOGLE_TIMELINE_CSV, OUTPUT_DIR)
    if WIPE_OUTPUT_DIR_BEFORE_RUN:
        removed_n = wipe_output_directory_contents(out_dir)
        print(f"[INFO] Wiped output directory: {out_dir} ({removed_n} entries removed).")

    track, fixes, segs, sensor_file_audit, file_route_class_map = process_tracks(GOOGLE_TIMELINE_CSV, SENSOR_GPS_CSV)

    track_path = out_dir / f"{OUTPUT_PREFIX}_track_{OUTPUT_INTERVAL_SECONDS}s.csv"
    fixes_path = out_dir / f"{OUTPUT_PREFIX}_sensor_fix_diagnostics.csv"
    segs_path = out_dir / f"{OUTPUT_PREFIX}_segment_diagnostics.csv"
    audit_path = out_dir / f"{OUTPUT_PREFIX}_sensor_file_audit.csv"

    track_saved = save_csv_with_fallback(track, track_path)
    fixes_saved = save_csv_with_fallback(fixes, fixes_path)
    segs_saved = save_csv_with_fallback(segs, segs_path)
    audit_saved = save_csv_with_fallback(sensor_file_audit, audit_path)
    sorted_out_saved, sorted_out_summary = save_sorted_out_sensor_file_reports(
        sensor_file_audit=sensor_file_audit,
        output_dir=out_dir,
    )
    individual_interp_saved, individual_interp_summary = save_individual_interpolated_outputs(
        track=track,
        fixes=fixes,
        sensor_file_audit=sensor_file_audit,
        output_dir=out_dir,
        file_route_class_map=file_route_class_map,
    )

    print(f"Saved track: {track_saved}")
    print(f"Saved fix diagnostics: {fixes_saved}")
    print(f"Saved segment diagnostics: {segs_saved}")
    print(f"Saved sensor-file audit: {audit_saved}")
    if sorted_out_summary is not None:
        print(f"Saved sorted-out summary: {sorted_out_summary}")
    print(
        f"Saved sorted-out per-file reports: {len(sorted_out_saved):,} "
        f"in {out_dir / SORTED_OUT_OUTPUT_SUBDIR}"
    )
    if individual_interp_summary is not None:
        print(f"Saved individual interpolation summary: {individual_interp_summary}")
    print(
        f"Saved individual interpolated files: {len(individual_interp_saved):,} "
        f"in {out_dir / INDIVIDUAL_INTERP_OUTPUT_SUBDIR}"
    )
    print(f"Track rows: {len(track):,}")
    print(f"Fix diagnostics rows: {len(fixes):,}")
    print(f"Segment diagnostics rows: {len(segs):,}")
    print(f"Sensor-file audit rows: {len(sensor_file_audit):,}")


if __name__ == "__main__":
    main()
