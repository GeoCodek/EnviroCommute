#!/usr/bin/env Rscript
# Route heatmaps from interpolated GPS CSV files.
# The script mirrors the Python workflow:
# - batch CSV processing
# - coordinate/time/value column auto-detection
# - smoothing, subroute splitting, and segment aggregation
# - per-route and combined heatmap PNG generation
# - summary/error CSV outputs
required_packages <- c(
  "data.table",
  "sf",
  "zoo",
  "RColorBrewer",
  "viridisLite",
  "scales"
)
missing_packages <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)
]
if (length(missing_packages) > 0) {
  stop(
    sprintf(
      "Missing required package(s): %s. Install them before running this script.",
      paste(missing_packages, collapse = ", ")
    ),
    call. = FALSE
  )
}
HAS_ROSM <- requireNamespace("rosm", quietly = TRUE)
HAS_MAPTILES <- requireNamespace("maptiles", quietly = TRUE)
LAST_BASEMAP_EXTENT <- NULL
DEFAULT_INPUT_DIR <- paste0(
  "C:/Users/janek/OneDrive/Desktop/FU/enviro_pi_csv-main/",
  "gps_records/Google/Interpolation output/individual_interpolated_csvs"
)
OUTPUT_BASE_DIR <- "C:/Users/janek/OneDrive/Desktop/FU/enviro_pi_csv-main/Python Maps"
PREFERRED_DISPLAY_COLUMNS <- c(
  "pm2_5_ug_m3",
  "pm2_5_count",
  "gas_reducing_ohms",
  "gas_oxidising_ohms"
)
FALLBACK_DISPLAY_COLUMNS <- c(
  "confidence_score",
  "delta_s",
  "path_length_m"
)
PM25_CANDIDATES <- c(
  "pm2_5_ug_m3",
  "pm2_5_count",
  "pm2_5_atm_ug_m3"
)
COLUMN_LABELS <- c(
  pm2_5_ug_m3 = "Particulate Matter (PM2.5\u00B5m)",
  pm2_5_count = "Particulate Matter (PM2.5\u00B5m)",
  gas_reducing_ohms = "Reducing gas (k\u03A9)",
  gas_oxidising_ohms = "Oxidising gas (k\u03A9)",
  noise_total = "Noise total",
  step_speed_m_s = "Step speed (m/s)",
  speed_basis_mps = "Speed basis (m/s)",
  confidence_score = "Confidence score",
  delta_s = "Time delta (s)",
  step_distance_m = "Step distance (m)",
  path_length_m = "Path length (m)"
)
COLUMN_UNITS <- c(
  pm2_5_ug_m3 = "ug/m3",
  pm2_5_count = "count",
  gas_reducing_ohms = "k\u03A9",
  gas_oxidising_ohms = "k\u03A9",
  noise_total = "",
  step_speed_m_s = "m/s",
  speed_basis_mps = "m/s",
  confidence_score = "score",
  delta_s = "s",
  step_distance_m = "m",
  path_length_m = "m",
  temperature_c = "C"
)
COLUMN_CMAPS <- c(
  pm2_5_ug_m3 = "plasma_yellow_low",
  pm2_5_count = "plasma_yellow_low",
  gas_reducing_ohms = "blues",
  gas_oxidising_ohms = "viridis_yellow_low",
  noise_total = "heat_yellow_low",
  step_speed_m_s = "viridis",
  speed_basis_mps = "viridis",
  confidence_score = "greens",
  delta_s = "plasma_yellow_low",
  step_distance_m = "oranges",
  path_length_m = "ylgnbu",
  temperature_c = "inferno_yellow_low"
)
FALLBACK_CMAP_SEQUENCE <- c(
  "plasma_yellow_low",
  "magma_yellow_low",
  "viridis_yellow_low",
  "heat_yellow_low"
)
REFERENCE_MARKERS_WGS84 <- data.frame(
  label = c("144", "124"),
  lat = c(52.4675198, 52.43849910),
  lon = c(13.4415632, 13.38767323),
  stringsAsFactors = FALSE
)
OFFSET_SLOT_SPACING <- 1.15
DEFAULTS <- list(
  input_dir = DEFAULT_INPUT_DIR,
  output_base_dir = OUTPUT_BASE_DIR,
  output_dir = "parallel_heatmap_r",
  input_crs_epsg = 0L,
  crs_epsg = 32633L,
  map_epsg = 3857L,
  wgs84_epsg = 4326L,
  create_wgs84_maps = FALSE,
  create_route_maps = TRUE,
  create_column_maps = TRUE,
  create_dot_maps = FALSE,
  basemap_engine = "auto",
  osm_background = TRUE,
  osm_type = "osm",
  osm_provider = "CartoDB.Voyager",
  osm_zoom = NA_integer_,
  osm_zoom_bias = 2L,
  osm_cache_dir = "",
  debug_basemap = FALSE,
  osm_res = 170,
  osm_zoomin = -1L,
  osm_allow_large_requests = TRUE,
  segment_length_m = 20.0,
  smoothing_window = 5L,
  line_offset_m = 3.0,
  line_offset_screen_px = 9.0,
  line_width = 5.8,
  line_alpha = 1.0,
  line_outline_width = 0.0,
  line_outline_alpha = 0.0,
  line_outline_color = "#101010",
  legend_label_cex = 1.10,
  legend_value_cex = 1.10,
  legend_bar_width_frac = 0.021,
  legend_bar_gap_frac = 0.020,
  legend_inside = FALSE,
  legend_bg_alpha = 0.68,
  bbox_pad_frac = 0.15,
  png_width_px = 4400L,
  png_height_px = 3600L,
  combined_png_width_px = 4300L,
  combined_png_height_px = 3800L,
  png_res_dpi = 360L,
  merged_snap_m = 18.0,
  point_gap_break_m = 80.0,
  min_points_per_route = 2L,
  max_display_cols = 4L,
  max_points_per_route = 60000L,
  auto_install_osm_deps = TRUE,
  display_cols = ""
)
normalize_slashes <- function(path) {
  gsub("\\\\", "/", path)
}
is_absolute_path <- function(path) {
  grepl("^[A-Za-z]:[/\\\\]", path) || startsWith(path, "/")
}
coalesce_num <- function(x, default = NA_real_) {
  if (length(x) == 0 || is.na(x) || !is.finite(x)) {
    return(default)
  }
  x
}
safe_mean <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  mean(x)
}
safe_median <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  stats::median(x)
}
safe_min <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  min(x)
}
safe_max <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) {
    return(NA_real_)
  }
  max(x)
}
parse_requested_display_cols <- function(raw_cols) {
  if (is.null(raw_cols) || !nzchar(trimws(raw_cols))) {
    return(character(0))
  }
  cols <- trimws(unlist(strsplit(raw_cols, ",", fixed = TRUE)))
  cols <- cols[nzchar(cols)]
  unique(cols)
}
pick_existing_column <- function(col_names, candidates) {
  hits <- candidates[candidates %in% col_names]
  if (length(hits) == 0) {
    return(NA_character_)
  }
  hits[[1]]
}
cast_cli_value <- function(template, value) {
  if (is.integer(template)) {
    num <- suppressWarnings(as.numeric(value))
    if (!is.finite(num)) {
      stop(sprintf("Invalid integer argument value: %s", value), call. = FALSE)
    }
    return(as.integer(round(num)))
  }
  if (is.numeric(template)) {
    num <- suppressWarnings(as.numeric(value))
    if (!is.finite(num)) {
      stop(sprintf("Invalid numeric argument value: %s", value), call. = FALSE)
    }
    return(num)
  }
  if (is.logical(template)) {
    return(tolower(value) %in% c("1", "true", "yes", "y", "t"))
  }
  value
}
resolve_osm_plot_epsg <- function(cfg) {
  epsg <- as.integer(cfg$map_epsg)
  engine <- tolower(as.character(cfg$basemap_engine)[1])
  if (!engine %in% c("auto", "maptiles", "rosm")) {
    engine <- "auto"
  }
  if (!isTRUE(cfg$osm_background)) {
    return(as.integer(cfg$crs_epsg))
  }
  if (identical(epsg, as.integer(cfg$wgs84_epsg))) {
    return(epsg)
  }
  if (identical(epsg, 3857L)) {
    if (engine %in% c("auto", "maptiles") && isTRUE(HAS_MAPTILES)) {
      return(epsg)
    }
    if (engine %in% c("auto", "rosm") && requireNamespace("sp", quietly = TRUE)) {
      return(epsg)
    }
    message("EPSG:3857 basemap not available for current backend/deps. Falling back to EPSG:4326.")
    return(as.integer(cfg$wgs84_epsg))
  }
  message(sprintf("OSM plotting for EPSG:%d is not supported. Falling back to EPSG:%d.", epsg, as.integer(cfg$wgs84_epsg)))
  as.integer(cfg$wgs84_epsg)
}
ensure_osm_runtime_deps <- function(auto_install = TRUE, engine = "auto") {
  engine <- tolower(as.character(engine)[1])
  if (!engine %in% c("auto", "maptiles", "rosm")) {
    engine <- "auto"
  }

  has_all <- function(pkgs) {
    all(vapply(pkgs, requireNamespace, logical(1), quietly = TRUE))
  }
  missing_of <- function(pkgs) {
    pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  }

  maptiles_pkgs <- c("maptiles", "terra")
  rosm_pkgs <- c("rosm", "sp", "raster", "plyr")

  if (engine == "maptiles") {
    missing <- missing_of(maptiles_pkgs)
    if (length(missing) == 0) {
      return(TRUE)
    }
    if (isTRUE(auto_install)) {
      message(sprintf("Installing missing OSM package(s): %s", paste(missing, collapse = ", ")))
      tryCatch(
        utils::install.packages(missing, repos = "https://cloud.r-project.org"),
        error = function(e) {
          message(sprintf("OSM dependency install failed: %s", conditionMessage(e)))
        }
      )
      missing <- missing_of(maptiles_pkgs)
    }
    if (length(missing) > 0) {
      message(sprintf(
        "OSM background disabled for maptiles backend. Missing package(s): %s",
        paste(missing, collapse = ", ")
      ))
      return(FALSE)
    }
    return(TRUE)
  }

  if (engine == "rosm") {
    missing <- missing_of(rosm_pkgs)
    if (length(missing) == 0) {
      return(TRUE)
    }
    if (isTRUE(auto_install)) {
      message(sprintf("Installing missing OSM package(s): %s", paste(missing, collapse = ", ")))
      tryCatch(
        utils::install.packages(missing, repos = "https://cloud.r-project.org"),
        error = function(e) {
          message(sprintf("OSM dependency install failed: %s", conditionMessage(e)))
        }
      )
      missing <- missing_of(rosm_pkgs)
    }
    if (length(missing) > 0) {
      message(sprintf(
        "OSM background disabled for rosm backend. Missing package(s): %s",
        paste(missing, collapse = ", ")
      ))
      return(FALSE)
    }
    return(TRUE)
  }

  # auto mode: accept either backend
  if (has_all(maptiles_pkgs) || has_all(rosm_pkgs)) {
    return(TRUE)
  }

  install_target <- unique(c(missing_of(maptiles_pkgs), missing_of(rosm_pkgs)))
  if (isTRUE(auto_install) && length(install_target) > 0) {
    message(sprintf("Installing missing OSM package(s): %s", paste(install_target, collapse = ", ")))
    tryCatch(
      utils::install.packages(install_target, repos = "https://cloud.r-project.org"),
      error = function(e) {
        message(sprintf("OSM dependency install failed: %s", conditionMessage(e)))
      }
    )
  }

  if (has_all(maptiles_pkgs) || has_all(rosm_pkgs)) {
    return(TRUE)
  }

  message(sprintf(
    paste(
      "OSM background disabled in auto mode.",
      "Need maptiles+terra OR rosm+sp+raster+plyr.",
      "Missing maptiles stack: %s.",
      "Missing rosm stack: %s."
    ),
    paste(missing_of(maptiles_pkgs), collapse = ", "),
    paste(missing_of(rosm_pkgs), collapse = ", ")
  ))
  FALSE
}
log_basemap <- function(cfg, msg) {
  if (isTRUE(cfg$debug_basemap)) {
    message(msg)
  }
}
build_plot_bbox_sfc <- function(xlim, ylim, plot_epsg) {
  sf::st_as_sfc(
    sf::st_bbox(c(
      xmin = xlim[1],
      xmax = xlim[2],
      ymin = ylim[1],
      ymax = ylim[2]
    ), crs = as.integer(plot_epsg))
  )
}
expand_limits <- function(xlim, ylim, pad_frac = 0.08) {
  pad_x <- diff(xlim) * pad_frac
  pad_y <- diff(ylim) * pad_frac
  list(
    xlim = c(xlim[1] - pad_x, xlim[2] + pad_x),
    ylim = c(ylim[1] - pad_y, ylim[2] + pad_y)
  )
}
compute_tile_zoom_from_bbox <- function(bbox_sfc, cfg) {
  bbox_3857 <- tryCatch(
    sf::st_bbox(sf::st_transform(bbox_sfc, 3857)),
    error = function(e) NULL
  )
  bbox_ll <- tryCatch(
    sf::st_bbox(sf::st_transform(bbox_sfc, 4326)),
    error = function(e) NULL
  )
  if (is.null(bbox_3857) || is.null(bbox_ll)) {
    return(NA_integer_)
  }
  width_m <- as.numeric(bbox_3857$xmax - bbox_3857$xmin)
  height_m <- as.numeric(bbox_3857$ymax - bbox_3857$ymin)
  span_m <- max(width_m, height_m, na.rm = TRUE)
  if (!is.finite(span_m) || span_m <= 0) {
    return(NA_integer_)
  }
  center_lat <- mean(c(as.numeric(bbox_ll$ymin), as.numeric(bbox_ll$ymax)))
  if (!is.finite(center_lat)) {
    center_lat <- 0
  }
  target_px <- max(
    1600,
    round(max(
      as.numeric(cfg$png_width_px),
      as.numeric(cfg$png_height_px),
      as.numeric(cfg$combined_png_width_px),
      as.numeric(cfg$combined_png_height_px)
    ) * 0.58)
  )
  meters_per_pixel_z0 <- 156543.03392804097 * cos(center_lat * pi / 180)
  if (!is.finite(meters_per_pixel_z0) || meters_per_pixel_z0 <= 0) {
    return(NA_integer_)
  }
  zoom_raw <- log((meters_per_pixel_z0 * target_px) / span_m, base = 2)
  if (!is.finite(zoom_raw)) {
    return(NA_integer_)
  }
  zoom_bias <- suppressWarnings(as.integer(cfg$osm_zoom_bias))
  if (!is.finite(zoom_bias)) {
    zoom_bias <- 0L
  }
  zoom_val <- floor(zoom_raw) + zoom_bias
  as.integer(min(19L, max(1L, zoom_val)))
}
try_plot_maptiles_background <- function(xlim, ylim, cfg, plot_epsg) {
  LAST_BASEMAP_EXTENT <<- NULL
  if (!isTRUE(HAS_MAPTILES)) {
    return(FALSE)
  }
  if (!requireNamespace("terra", quietly = TRUE)) {
    return(FALSE)
  }
  bbox_sfc <- tryCatch(
    {
      build_plot_bbox_sfc(
        xlim = xlim,
        ylim = ylim,
        plot_epsg = plot_epsg
      )
    },
    error = function(e) NULL
  )
  if (is.null(bbox_sfc)) {
    return(FALSE)
  }
  provider <- if (!is.null(cfg$osm_provider) && nzchar(trimws(cfg$osm_provider))) {
    as.character(cfg$osm_provider)
  } else {
    "OpenStreetMap"
  }
  zoom_val <- suppressWarnings(as.numeric(cfg$osm_zoom))
  zoom_arg <- if (is.finite(zoom_val)) as.integer(round(zoom_val)) else NA_integer_
  cache_dir <- if (!is.null(cfg$osm_cache_dir) && nzchar(trimws(cfg$osm_cache_dir))) {
    normalize_slashes(cfg$osm_cache_dir)
  } else {
    file.path(tempdir(), "maptiles_cache")
  }
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  ok <- tryCatch(
    {
      tile_args <- list(
        x = bbox_sfc,
        provider = provider,
        crop = TRUE,
        project = TRUE,
        verbose = FALSE,
        cachedir = cache_dir,
        forceDownload = FALSE,
        retina = TRUE
      )
      zoom_used <- zoom_arg
      if (is.finite(zoom_arg)) {
        tile_args$zoom <- zoom_arg
      } else {
        zoom_used <- compute_tile_zoom_from_bbox(bbox_sfc, cfg)
        if (is.finite(zoom_used)) {
          tile_args$zoom <- zoom_used
        }
      }
      tiles <- do.call(maptiles::get_tiles, tile_args)
      maptiles::plot_tiles(tiles, add = FALSE)
      tile_usr <- graphics::par("usr")
      LAST_BASEMAP_EXTENT <<- list(
        xlim = c(tile_usr[1], tile_usr[2]),
        ylim = c(tile_usr[3], tile_usr[4])
      )
      log_basemap(
        cfg,
        sprintf(
          "maptiles success provider=%s epsg=%d xlim=[%.6f, %.6f] ylim=[%.6f, %.6f] zoom=%s",
          provider,
          as.integer(plot_epsg),
          xlim[1], xlim[2], ylim[1], ylim[2],
          if (is.finite(zoom_used)) as.character(zoom_used) else "auto"
        )
      )
      TRUE
    },
    error = function(e) {
      LAST_BASEMAP_EXTENT <<- NULL
      log_basemap(cfg, sprintf("maptiles failed (%s): %s", provider, conditionMessage(e)))
      FALSE
    }
  )
  ok
}
parse_cli_args <- function(defaults) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    return(defaults)
  }
  out <- defaults
  for (arg in args) {
    if (!startsWith(arg, "--")) {
      next
    }
    kv <- strsplit(sub("^--", "", arg), "=", fixed = TRUE)[[1]]
    key <- kv[1]
    value <- if (length(kv) > 1) {
      paste(kv[-1], collapse = "=")
    } else {
      "TRUE"
    }
    if (!key %in% names(out)) {
      next
    }
    out[[key]] <- cast_cli_value(out[[key]], value)
  }
  out
}
resolve_output_dir <- function(output_dir, output_base_dir) {
  out <- normalize_slashes(output_dir)
  base <- normalize_slashes(output_base_dir)
  if (is_absolute_path(out)) {
    out_norm <- normalizePath(out, winslash = "/", mustWork = FALSE)
    base_norm <- normalizePath(base, winslash = "/", mustWork = FALSE)
    if (startsWith(tolower(out_norm), tolower(base_norm))) {
      return(out_norm)
    }
    return(file.path(base_norm, basename(out_norm)))
  }
  file.path(base, out)
}
ensure_output_dirs <- function(output_root) {
  dirs <- list(
    root = output_root,
    maps_per_route = file.path(output_root, "maps_per_route"),
    maps_per_route_columns = file.path(output_root, "maps_per_route_columns"),
    maps_per_route_dots = file.path(output_root, "maps_per_route_dots"),
    maps_combined = file.path(output_root, "maps_combined"),
    merged_layers = file.path(output_root, "merged_layers"),
    aggregated_segments = file.path(output_root, "aggregated_segments"),
    summaries = file.path(output_root, "summaries")
  )
  invisible(lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE))
  dirs
}
read_csv_header <- function(csv_path) {
  tryCatch(
    names(data.table::fread(
      csv_path,
      nrows = 0L,
      showProgress = FALSE,
      encoding = "UTF-8"
    )),
    error = function(e) {
      tryCatch(
        names(utils::read.csv(
          csv_path,
          nrows = 1L,
          check.names = FALSE,
          stringsAsFactors = FALSE
        )),
        error = function(e2) character(0)
      )
    }
  )
}
read_csv_safely <- function(csv_path) {
  tryCatch(
    data.table::fread(
      csv_path,
      data.table = FALSE,
      showProgress = FALSE,
      encoding = "UTF-8"
    ),
    error = function(e) {
      utils::read.csv(
        csv_path,
        check.names = FALSE,
        stringsAsFactors = FALSE
      )
    }
  )
}
auto_select_display_columns <- function(csv_files, max_display_cols) {
  headers <- lapply(csv_files, read_csv_header)
  unique_headers <- lapply(headers, unique)
  flat <- unlist(unique_headers, use.names = FALSE)
  if (length(flat) == 0) {
    return(character(0))
  }
  counts <- sort(table(flat), decreasing = TRUE)
  chosen <- character(0)
  for (col in c(PREFERRED_DISPLAY_COLUMNS, FALLBACK_DISPLAY_COLUMNS)) {
    if (col %in% names(counts) && !(col %in% chosen)) {
      chosen <- c(chosen, col)
    }
    if (length(chosen) >= max_display_cols) {
      return(chosen[seq_len(max_display_cols)])
    }
  }
  if (length(chosen) < max_display_cols) {
    tokens <- c(
      "speed", "distance", "score", "delta", "noise",
      "pm", "gas", "temp", "humid"
    )
    extras <- names(counts)
    for (col in extras) {
      if (col %in% chosen) {
        next
      }
      col_l <- tolower(col)
      if (any(vapply(tokens, grepl, logical(1), x = col_l, fixed = TRUE))) {
        chosen <- c(chosen, col)
      }
      if (length(chosen) >= max_display_cols) {
        break
      }
    }
  }
  chosen[seq_len(min(length(chosen), max_display_cols))]
}
resolve_display_columns <- function(csv_files, display_cols_raw, max_display_cols) {
  requested <- parse_requested_display_cols(display_cols_raw)
  if (length(requested) > 0) {
    return(requested[seq_len(min(length(requested), max_display_cols))])
  }
  auto_select_display_columns(csv_files, max_display_cols = max_display_cols)
}
build_column_style_maps <- function(display_cols) {
  labels <- list()
  cmaps <- list()
  offsets <- numeric(0)
  if (length(display_cols) == 0) {
    return(list(labels = labels, cmaps = cmaps, offsets = offsets))
  }
  center_idx <- 0.5 * (length(display_cols) - 1)
  for (i in seq_along(display_cols)) {
    col <- display_cols[i]
    label <- if (col %in% names(COLUMN_LABELS)) unname(COLUMN_LABELS[col]) else ""
    if (!nzchar(label)) {
      label <- tools::toTitleCase(gsub("_", " ", col))
    }
    cmap <- if (col %in% names(COLUMN_CMAPS)) unname(COLUMN_CMAPS[col]) else ""
    if (!nzchar(cmap)) {
      cmap <- FALLBACK_CMAP_SEQUENCE[((i - 1) %% length(FALLBACK_CMAP_SEQUENCE)) + 1]
    }
    labels[[col]] <- label
    cmaps[[col]] <- cmap
    offsets[col] <- (i - 1 - center_idx) * OFFSET_SLOT_SPACING
  }
  list(labels = labels, cmaps = cmaps, offsets = offsets)
}
detect_lat_lon_columns <- function(col_names) {
  candidates <- list(
    c("lat", "lon"),
    c("latitude", "longitude"),
    c("Latitude", "Longitude"),
    c("LAT", "LON"),
    c("gps_lat_deg", "gps_lon_deg"),
    c("gps_latitude", "gps_longitude"),
    c("Breite", "L\u00e4nge"),
    c("Breite", "Laenge")
  )
  for (pair in candidates) {
    if (all(pair %in% col_names)) {
      return(list(lat_col = pair[1], lon_col = pair[2]))
    }
  }
  stop(
    paste(
      "No coordinate columns found. Expected one of:",
      "(lat, lon), (latitude, longitude), (Latitude, Longitude),",
      "(LAT, LON), (gps_lat_deg, gps_lon_deg), (gps_latitude, gps_longitude),",
      "(Breite, L\\u00e4nge/Laenge)."
    ),
    call. = FALSE
  )
}
detect_time_column <- function(col_names) {
  for (candidate in c("timestamp", "timestamp_iso", "timestamp_local", "unix_time_s")) {
    if (candidate %in% col_names) {
      return(candidate)
    }
  }
  NULL
}
build_sort_time <- function(df, time_col) {
  if (is.null(time_col) || !time_col %in% names(df)) {
    return(seq_len(nrow(df)))
  }
  x <- df[[time_col]]
  if (identical(time_col, "unix_time_s")) {
    return(suppressWarnings(as.numeric(x)))
  }
  if (inherits(x, "POSIXct")) {
    return(as.numeric(x))
  }
  x_char <- as.character(x)
  parsed <- suppressWarnings(as.POSIXct(
    x_char,
    tz = "UTC",
    tryFormats = c(
      "%Y-%m-%d %H:%M:%OS",
      "%Y-%m-%d %H:%M:%S",
      "%Y-%m-%dT%H:%M:%OS",
      "%Y-%m-%dT%H:%M:%S",
      "%Y-%m-%dT%H:%M:%OSZ",
      "%Y-%m-%dT%H:%M:%SZ",
      "%Y-%m-%d %H:%M:%S%z",
      "%Y-%m-%dT%H:%M:%S%z",
      "%d.%m.%Y %H:%M:%OS",
      "%m/%d/%Y %H:%M:%OS"
    )
  ))
  sort_time <- as.numeric(parsed)
  numeric_guess <- suppressWarnings(as.numeric(x_char))
  fill_idx <- is.na(sort_time) & is.finite(numeric_guess)
  sort_time[fill_idx] <- numeric_guess[fill_idx]
  sort_time
}
infer_input_crs_epsg <- function(
  lat_vals,
  lon_vals,
  forced_epsg = 0L,
  default_projected_epsg = 32633L
) {
  if (is.finite(forced_epsg) && as.integer(forced_epsg) > 0L) {
    return(as.integer(forced_epsg))
  }
  lat <- suppressWarnings(as.numeric(lat_vals))
  lon <- suppressWarnings(as.numeric(lon_vals))
  idx <- which(is.finite(lat) & is.finite(lon))
  if (length(idx) < 2) {
    return(4326L)
  }
  lat <- lat[idx]
  lon <- lon[idx]
  q_lat <- suppressWarnings(as.numeric(stats::quantile(abs(lat), 0.98, na.rm = TRUE)))
  q_lon <- suppressWarnings(as.numeric(stats::quantile(abs(lon), 0.98, na.rm = TRUE)))
  if (is.finite(q_lat) && is.finite(q_lon) && q_lat <= 90 && q_lon <= 180) {
    return(4326L)
  }
  candidates <- unique(as.integer(c(
    default_projected_epsg,
    32633L,
    25833L,
    32632L,
    25832L,
    3857L
  )))
  sample_n <- min(length(lat), 300L)
  sidx <- unique(round(seq(1, length(lat), length.out = sample_n)))
  x <- lon[sidx]
  y <- lat[sidx]
  best_epsg <- as.integer(default_projected_epsg)
  best_score <- -Inf
  for (epsg in candidates) {
    coords_4326 <- tryCatch(
      {
        pts <- sf::st_as_sf(
          data.frame(x = x, y = y),
          coords = c("x", "y"),
          crs = as.integer(epsg),
          remove = FALSE
        )
        pts_ll <- sf::st_transform(pts, 4326)
        sf::st_coordinates(pts_ll)
      },
      error = function(e) NULL
    )
    if (is.null(coords_4326)) {
      next
    }
    lon_t <- coords_4326[, 1]
    lat_t <- coords_4326[, 2]
    valid <- is.finite(lon_t) & is.finite(lat_t) & abs(lon_t) <= 180 & abs(lat_t) <= 90
    valid_ratio <- mean(valid)
    if (!is.finite(valid_ratio) || valid_ratio < 0.90) {
      next
    }
    lon_v <- lon_t[valid]
    lat_v <- lat_t[valid]
    lon_range <- diff(range(lon_v))
    lat_range <- diff(range(lat_v))
    bbox_area <- lon_range * lat_range
    if (!is.finite(bbox_area) || bbox_area <= 0) {
      bbox_area <- 1e6
    }
    penalty <- 0
    if (bbox_area > 25) {
      penalty <- penalty + 2
    }
    if (bbox_area < 1e-8) {
      penalty <- penalty + 1
    }
    score <- valid_ratio - log1p(bbox_area) * 0.05 - penalty
    if (identical(as.integer(epsg), as.integer(default_projected_epsg))) {
      score <- score + 0.02
    }
    if (score > best_score) {
      best_score <- score
      best_epsg <- as.integer(epsg)
    }
  }
  best_epsg
}
downsample_dataframe <- function(df, max_points) {
  if (max_points <= 0L || nrow(df) <= max_points) {
    return(list(df = df, downsampled = FALSE))
  }
  idx <- unique(round(seq(1, nrow(df), length.out = max_points)))
  idx <- idx[idx >= 1 & idx <= nrow(df)]
  list(df = df[idx, , drop = FALSE], downsampled = TRUE)
}
rolling_median_center <- function(x, window) {
  x_num <- suppressWarnings(as.numeric(x))
  if (window <= 1L) {
    return(x_num)
  }
  zoo::rollapply(
    x_num,
    width = window,
    align = "center",
    fill = NA_real_,
    partial = TRUE,
    FUN = function(v) {
      vv <- v[is.finite(v)]
      if (length(vv) == 0) {
        return(NA_real_)
      }
      stats::median(vv)
    }
  )
}
compute_segmented_route_ids <- function(points_sf, point_gap_break_m) {
  coords <- sf::st_coordinates(points_sf)
  n <- nrow(coords)
  step_distance <- rep(NA_real_, n)
  if (n > 1) {
    dx <- coords[2:n, 1] - coords[1:(n - 1), 1]
    dy <- coords[2:n, 2] - coords[1:(n - 1), 2]
    step_distance[2:n] <- sqrt(dx^2 + dy^2)
  }
  finite_steps <- step_distance[is.finite(step_distance) & step_distance > 0]
  effective_gap_break_m <- point_gap_break_m
  if (length(finite_steps) >= 8) {
    p90 <- as.numeric(stats::quantile(finite_steps, probs = 0.90, na.rm = TRUE, names = FALSE))
    med <- as.numeric(stats::median(finite_steps, na.rm = TRUE))
    adaptive <- max(point_gap_break_m, p90 * 3.0, med * 8.0)
    # Avoid over-connecting truly disjoint traces.
    effective_gap_break_m <- min(adaptive, point_gap_break_m * 8.0)
  }
  subroute_break <- ifelse(is.na(step_distance), FALSE, step_distance > effective_gap_break_m)
  subroute_id <- as.integer(cumsum(subroute_break))
  points_sf$step_distance_m <- step_distance
  points_sf$subroute_break <- subroute_break
  points_sf$subroute_id <- subroute_id
  points_sf$effective_gap_break_m <- effective_gap_break_m
  points_sf
}
points_sf_to_df <- function(points_sf) {
  coords <- sf::st_coordinates(points_sf)
  out <- sf::st_drop_geometry(points_sf)
  out$x <- coords[, 1]
  out$y <- coords[, 2]
  out
}
build_subroute_lines <- function(points_df, crs_epsg, min_points) {
  if (nrow(points_df) < min_points) {
    return(NULL)
  }
  line_geoms <- list()
  line_lengths <- numeric(0)
  subroutes <- split(points_df, points_df$subroute_id, drop = TRUE)
  for (sub in subroutes) {
    if (nrow(sub) < min_points) {
      next
    }
    xy <- as.matrix(sub[, c("x", "y"), drop = FALSE])
    if (nrow(unique(xy)) < 2) {
      next
    }
    line_geom <- sf::st_linestring(xy)
    line_sfc <- sf::st_sfc(line_geom, crs = crs_epsg)
    line_len <- as.numeric(sf::st_length(line_sfc))
    if (!is.finite(line_len) || line_len <= 0) {
      next
    }
    line_geoms[[length(line_geoms) + 1]] <- line_geom
    line_lengths <- c(line_lengths, line_len)
  }
  if (length(line_geoms) == 0) {
    return(NULL)
  }
  longest_idx <- which.max(line_lengths)
  list(
    lines = sf::st_sfc(line_geoms, crs = crs_epsg),
    lengths_m = line_lengths,
    longest_line = sf::st_sfc(line_geoms[[longest_idx]], crs = crs_epsg),
    longest_length_m = line_lengths[longest_idx]
  )
}
project_points_onto_line <- function(points_sf, route_line_sfc) {
  n <- nrow(points_sf)
  line_length <- as.numeric(sf::st_length(route_line_sfc))
  if (n == 0 || !is.finite(line_length) || line_length <= 0) {
    return(rep(NA_real_, n))
  }
  if ("st_line_project" %in% getNamespaceExports("sf")) {
    measures <- suppressWarnings(as.numeric(
      sf::st_line_project(route_line_sfc, sf::st_geometry(points_sf))
    ))
    return(pmin(pmax(measures, 0), line_length))
  }
  step <- suppressWarnings(as.numeric(points_sf$step_distance_m))
  step[!is.finite(step)] <- 0
  cum <- cumsum(step)
  if (!is.finite(max(cum, na.rm = TRUE)) || max(cum, na.rm = TRUE) <= 0) {
    return(rep(0, n))
  }
  cum / max(cum, na.rm = TRUE) * line_length
}
sample_line_segments <- function(line_length_m, segment_length_m) {
  if (!is.finite(line_length_m) || line_length_m <= 0 || segment_length_m <= 0) {
    return(data.frame())
  }
  starts <- seq(0, line_length_m, by = segment_length_m)
  if (length(starts) == 0) {
    starts <- 0
  }
  out <- data.frame(
    segment_index = seq_along(starts) - 1L,
    m_start = starts,
    m_end = pmin(starts + segment_length_m, line_length_m),
    stringsAsFactors = FALSE
  )
  out <- out[out$m_end > out$m_start, , drop = FALSE]
  out$m_mid <- 0.5 * (out$m_start + out$m_end)
  out
}
aggregate_along_route <- function(
  points_sf,
  route_line_sfc,
  segment_length_m,
  display_cols,
  route_name
) {
  line_length <- as.numeric(sf::st_length(route_line_sfc))
  seg_defs <- sample_line_segments(line_length, segment_length_m)
  if (nrow(seg_defs) == 0) {
    return(data.frame())
  }
  measures <- project_points_onto_line(points_sf, route_line_sfc)
  rows <- vector("list", nrow(seg_defs))
  row_idx <- 0L
  for (i in seq_len(nrow(seg_defs))) {
    m0 <- seg_defs$m_start[i]
    m1 <- seg_defs$m_end[i]
    is_last <- i == nrow(seg_defs)
    sel <- which(
      is.finite(measures) &
        measures >= m0 &
        (measures < m1 | (is_last & measures <= m1))
    )
    if (length(sel) == 0) {
      next
    }
    row <- list(
      route_name = route_name,
      segment_index = as.integer(seg_defs$segment_index[i]),
      m_start = m0,
      m_end = m1,
      m_mid = seg_defs$m_mid[i],
      n_points = as.integer(length(sel))
    )
    for (col in display_cols) {
      sm_col <- paste0(col, "_smoothed")
      vals <- suppressWarnings(as.numeric(points_sf[[sm_col]][sel]))
      row[[paste0(col, "_mean")]] <- safe_mean(vals)
      row[[paste0(col, "_median")]] <- safe_median(vals)
      row[[paste0(col, "_min")]] <- safe_min(vals)
      row[[paste0(col, "_max")]] <- safe_max(vals)
    }
    row_idx <- row_idx + 1L
    rows[[row_idx]] <- row
  }
  if (row_idx == 0L) {
    return(data.frame())
  }
  agg <- data.table::rbindlist(rows[seq_len(row_idx)], fill = TRUE)
  if (is.finite(line_length) && line_length > 0) {
    fracs <- pmin(pmax(agg$m_mid / line_length, 0), 1)
    mid_geom <- sf::st_line_sample(route_line_sfc, sample = fracs)
    mid_xy <- sf::st_coordinates(mid_geom)
    if (nrow(mid_xy) >= nrow(agg)) {
      agg$x_mid <- mid_xy[seq_len(nrow(agg)), 1]
      agg$y_mid <- mid_xy[seq_len(nrow(agg)), 2]
    } else {
      agg$x_mid <- NA_real_
      agg$y_mid <- NA_real_
    }
  } else {
    agg$x_mid <- NA_real_
    agg$y_mid <- NA_real_
  }
  as.data.frame(agg)
}
build_norm_from_values <- function(values) {
  arr <- values[is.finite(values)]
  if (length(arr) == 0) {
    return(list(vmin = 0.0, vmax = 1.0))
  }
  vmin <- suppressWarnings(min(arr, na.rm = TRUE))
  vmax <- suppressWarnings(max(arr, na.rm = TRUE))
  if (!is.finite(vmin) || !is.finite(vmax) || vmin >= vmax) {
    center <- as.numeric(stats::median(arr))
    spread <- suppressWarnings(stats::sd(arr, na.rm = TRUE))
    if (!is.finite(spread) || spread <= 0) {
      spread <- max(abs(center) * 0.05, 1e-6)
    }
    spread <- max(spread, 1e-6)
    return(list(vmin = center - spread, vmax = center + spread))
  }
  list(vmin = vmin, vmax = vmax)
}
choose_visual_value_col <- function(df, base_col, prefer_smoothed = TRUE) {
  sm_col <- paste0(base_col, "_smoothed")
  if (isTRUE(prefer_smoothed) && sm_col %in% names(df)) {
    sm_vals <- suppressWarnings(as.numeric(df[[sm_col]]))
    sm_vals <- sm_vals[is.finite(sm_vals)]
    if (length(sm_vals) > 1) {
      sm_range <- diff(range(sm_vals, na.rm = TRUE))
      if (is.finite(sm_range) && sm_range > 1e-12) {
        return(sm_col)
      }
    }
  }
  if (base_col %in% names(df)) {
    return(base_col)
  }
  sm_col
}
get_palette <- function(cmap_name, n = 256L) {
  n <- as.integer(max(2L, n))
  nm <- tolower(cmap_name)
  if (nm == "blue_red") {
    return(grDevices::colorRampPalette(
      c("#2c7bb6", "#74add1", "#abd9e9", "#ffffbf", "#fdae61", "#f46d43", "#d73027")
    )(n))
  }
  if (nm == "temperature_blue_red") {
    return(grDevices::colorRampPalette(
      c("#2b83ba", "#74add1", "#abd9e9", "#ffffbf", "#fdae61", "#f46d43", "#d7191c")
    )(n))
  }
  if (nm == "viridis_yellow_low") {
    return(rev(viridisLite::viridis(n, begin = 0.06, end = 0.98)))
  }
  if (nm == "magma_yellow_low") {
    return(rev(viridisLite::magma(n, begin = 0.03, end = 0.98)))
  }
  if (nm == "plasma_yellow_low") {
    return(rev(viridisLite::plasma(n, begin = 0.03, end = 0.98)))
  }
  if (nm == "inferno_yellow_low") {
    return(rev(viridisLite::inferno(n, begin = 0.03, end = 0.98)))
  }
  if (nm == "heat_yellow_low") {
    return(grDevices::colorRampPalette(
      c("#fff7bc", "#fee391", "#fec44f", "#fe9929", "#ec7014", "#cc4c02", "#8c2d04")
    )(n))
  }
  if (nm == "yellow_violet") {
    return(grDevices::colorRampPalette(
      c("#f9f871", "#f6d4a2", "#d4b9da", "#9e6db5", "#5e3c99")
    )(n))
  }
  if (nm %in% c("reds", "blues", "greens", "purples", "oranges", "ylgnbu")) {
    base_name <- switch(
      nm,
      reds = "Reds",
      blues = "Blues",
      greens = "Greens",
      purples = "Purples",
      oranges = "Oranges",
      ylgnbu = "YlGnBu"
    )
    base <- RColorBrewer::brewer.pal(9, base_name)
    # Skip the palest tones so lines remain visible on light basemaps.
    return(grDevices::colorRampPalette(base[4:9])(n))
  }
  if (nm == "magma") {
    return(viridisLite::magma(n, begin = 0.14, end = 1.0))
  }
  if (nm == "inferno") {
    return(viridisLite::inferno(n, begin = 0.14, end = 1.0))
  }
  if (nm == "plasma") {
    return(viridisLite::plasma(n, begin = 0.14, end = 1.0))
  }
  if (nm == "cividis") {
    return(viridisLite::cividis(n, begin = 0.14, end = 1.0))
  }
  viridisLite::viridis(n, begin = 0.14, end = 1.0)
}
map_values_to_colors <- function(values, palette, norm) {
  vmin <- norm$vmin
  vmax <- norm$vmax
  if (!is.finite(vmin) || !is.finite(vmax) || vmin >= vmax) {
    vmin <- 0
    vmax <- 1
  }
  scaled <- (values - vmin) / (vmax - vmin)
  scaled <- pmin(pmax(scaled, 0), 1)
  idx <- floor(scaled * (length(palette) - 1)) + 1L
  idx[!is.finite(idx)] <- NA_integer_
  out <- rep(NA_character_, length(values))
  valid <- is.finite(values) & !is.na(idx)
  out[valid] <- palette[idx[valid]]
  out
}
format_num <- function(x, digits = 4) {
  if (!is.finite(x)) {
    return("NA")
  }
  formatC(x, format = "fg", digits = digits)
}
needs_kohm_scaling <- function(unit = "") {
  if (length(unit) == 0 || is.na(unit) || !nzchar(unit)) {
    return(FALSE)
  }
  grepl("ohm|\u03A9", as.character(unit), ignore.case = TRUE, perl = TRUE)
}
normalize_display_unit <- function(unit = "") {
  if (needs_kohm_scaling(unit)) {
    return("k\u03A9")
  }
  unit
}
format_display_num <- function(x, unit = "", digits = 4) {
  if (needs_kohm_scaling(unit) && is.finite(x)) {
    x <- x / 1000
  }
  format_num(x, digits = digits)
}
lookup_display_unit <- function(column_name = NULL, label = NULL) {
  if (!is.null(column_name) && length(column_name) == 1 && !is.na(column_name) &&
      column_name %in% names(COLUMN_UNITS)) {
    return(unname(COLUMN_UNITS[column_name]))
  }
  if (!is.null(label) && length(label) == 1 && !is.na(label)) {
    match <- regmatches(label, regexec("\\(([^)]+)\\)", label))[[1]]
    if (length(match) >= 2) {
      return(match[2])
    }
  }
  ""
}
format_num_with_unit <- function(x, unit = "", digits = 3) {
  value_txt <- format_display_num(x, unit = unit, digits = digits)
  unit <- normalize_display_unit(unit)
  if (!nzchar(unit)) {
    return(value_txt)
  }
  paste0(value_txt, "\n", unit)
}
estimate_meters_per_x_unit <- function(plot_epsg, xlim, ylim) {
  if (!identical(as.integer(plot_epsg), 4326L)) {
    return(1.0)
  }
  center_lat <- mean(ylim)
  meters <- 111320 * cos(center_lat * pi / 180)
  if (!is.finite(meters) || meters <= 0) {
    return(111320)
  }
  meters
}
choose_scale_bar_length_m <- function(plot_epsg, xlim, ylim, target_frac = 0.18, max_length_m = Inf) {
  span_units <- diff(xlim)
  meters_per_x_unit <- estimate_meters_per_x_unit(plot_epsg, xlim, ylim)
  span_m <- span_units * meters_per_x_unit
  if (!is.finite(span_m) || span_m <= 0) {
    return(NA_real_)
  }
  target_m <- min(span_m * target_frac, max_length_m)
  if (!is.finite(target_m) || target_m <= 0) {
    return(NA_real_)
  }
  candidates <- c(
    25, 50, 100, 200, 250, 500,
    1000, 2000, 2500, 5000,
    10000, 20000, 25000, 50000
  )
  chosen <- candidates[candidates <= target_m]
  if (length(chosen) == 0) {
    if (is.finite(max_length_m) && max_length_m < min(candidates)) {
      return(NA_real_)
    }
    return(candidates[1])
  }
  max(chosen)
}
format_scale_distance <- function(length_m) {
  if (!is.finite(length_m)) {
    return("")
  }
  if (length_m >= 1000) {
    km <- length_m / 1000
    if (abs(km - round(km)) < 1e-8) {
      return(sprintf("%d km", as.integer(round(km))))
    }
    return(sprintf("%.1f km", km))
  }
  sprintf("%d m", as.integer(round(length_m)))
}
draw_north_arrow <- function(plot_epsg, xlim, ylim) {
  visible_limits <- get_visible_map_extent(xlim = xlim, ylim = ylim)
  xlim <- visible_limits$xlim
  ylim <- visible_limits$ylim
  x_span <- diff(xlim)
  y_span <- diff(ylim)
  left <- xlim[1] + x_span * 0.068
  right <- left + x_span * 0.0242
  top <- ylim[2] - y_span * 0.145
  bottom <- top - y_span * 0.0572
  x_mid <- (left + right) / 2
  graphics::polygon(
    x = c(x_mid, left, right),
    y = c(top, bottom, bottom),
    col = "#111111",
    border = "#111111",
    xpd = NA
  )
  graphics::text(
    x = x_mid,
    y = top + y_span * 0.0035,
    labels = "N",
    cex = 0.84,
    font = 2,
    col = "#111111",
    adj = c(0.5, 0),
    xpd = NA
  )
}
draw_scale_bar <- function(
  plot_epsg,
  xlim,
  ylim,
  right = NULL,
  center_x = NULL,
  bottom = NULL,
  left_limit = NULL,
  preferred_length_m = NA_real_
) {
  x_span <- diff(xlim)
  y_span <- diff(ylim)
  meters_per_x_unit <- estimate_meters_per_x_unit(plot_epsg, xlim, ylim)
  has_right <- length(right) == 1 && is.finite(right)
  has_center_x <- length(center_x) == 1 && is.finite(center_x)
  has_left_limit <- length(left_limit) == 1 && is.finite(left_limit)
  max_length_m <- Inf
  if (has_right && has_left_limit && right > left_limit) {
    max_length_m <- (right - left_limit) * meters_per_x_unit * 0.94
  }
  if (is.finite(preferred_length_m) &&
      preferred_length_m > 0 &&
      (is.infinite(max_length_m) || preferred_length_m <= max_length_m)) {
    scale_len_m <- preferred_length_m
  } else {
    scale_len_m <- choose_scale_bar_length_m(
      plot_epsg,
      xlim,
      ylim,
      target_frac = 0.18,
      max_length_m = max_length_m
    )
  }
  if (!is.finite(scale_len_m) || scale_len_m <= 0) {
    return(invisible(NULL))
  }
  scale_len_units <- scale_len_m / meters_per_x_unit
  if (has_center_x) {
    left <- center_x - scale_len_units * 0.5
    right <- center_x + scale_len_units * 0.5
  } else {
    if (!has_right) {
      right <- xlim[2] - x_span * 0.04
    }
    left <- right - scale_len_units
    if (has_left_limit && left < left_limit) {
      left <- left_limit
      right <- left + scale_len_units
    }
  }
  if (!(length(bottom) == 1 && is.finite(bottom))) {
    bottom <- ylim[1] + y_span * 0.035
  }
  top <- bottom + y_span * 0.018
  mid <- (left + right) / 2
  label_y <- top + y_span * 0.012
  usr <- graphics::par("usr")
  pin <- graphics::par("pin")
  graphics::rect(left, bottom, mid, top, col = "#111111", border = "#111111", xpd = NA)
  graphics::rect(mid, bottom, right, top, col = "white", border = "#111111", xpd = NA)
  graphics::segments(left, bottom, left, top, lwd = 1.1, col = "#111111", xpd = NA)
  graphics::segments(mid, bottom, mid, top, lwd = 1.1, col = "#111111", xpd = NA)
  graphics::segments(right, bottom, right, top, lwd = 1.1, col = "#111111", xpd = NA)
  label_box <- compute_text_box(
    label = format_scale_distance(scale_len_m),
    cex = 0.76,
    usr = usr,
    pin = pin,
    pad_in_x = 0.035,
    pad_in_y = 0.022
  )
  graphics::rect(
    xleft = mid - label_box$half_width_x,
    ybottom = label_y - label_box$half_height_y,
    xright = mid + label_box$half_width_x,
    ytop = label_y + label_box$half_height_y,
    col = grDevices::adjustcolor("white", alpha.f = 0.88),
    border = grDevices::adjustcolor("#666666", alpha.f = 0.35),
    lwd = 0.45,
    xpd = NA
  )
  graphics::text(
    x = mid,
    y = label_y,
    labels = format_scale_distance(scale_len_m),
    cex = 0.76,
    font = 2,
    col = "#111111",
    adj = c(0.5, 0.5),
    xpd = NA
  )
}
draw_map_navigation <- function(plot_epsg, xlim, ylim, include_scale_bar = TRUE) {
  draw_north_arrow(plot_epsg = plot_epsg, xlim = xlim, ylim = ylim)
  if (isTRUE(include_scale_bar)) {
    draw_scale_bar(plot_epsg = plot_epsg, xlim = xlim, ylim = ylim)
  }
}
get_effective_map_extent <- function(default_xlim, default_ylim) {
  if (!is.null(LAST_BASEMAP_EXTENT) &&
      is.list(LAST_BASEMAP_EXTENT) &&
      all(c("xlim", "ylim") %in% names(LAST_BASEMAP_EXTENT))) {
    xlim <- suppressWarnings(as.numeric(LAST_BASEMAP_EXTENT$xlim))
    ylim <- suppressWarnings(as.numeric(LAST_BASEMAP_EXTENT$ylim))
    if (length(xlim) == 2 && length(ylim) == 2 &&
        all(is.finite(xlim)) && all(is.finite(ylim)) &&
        diff(xlim) > 0 && diff(ylim) > 0) {
      return(list(xlim = xlim, ylim = ylim))
    }
  }
  list(xlim = default_xlim, ylim = default_ylim)
}
get_visible_map_extent <- function(xlim, ylim) {
  pin <- graphics::par("pin")
  x_span <- diff(xlim)
  y_span <- diff(ylim)
  if (length(pin) < 2 ||
      any(!is.finite(pin)) ||
      pin[1] <= 0 || pin[2] <= 0 ||
      !is.finite(x_span) || !is.finite(y_span) ||
      x_span <= 0 || y_span <= 0) {
    return(list(xlim = xlim, ylim = ylim))
  }
  unit_scale <- min(pin[1] / x_span, pin[2] / y_span)
  if (!is.finite(unit_scale) || unit_scale <= 0) {
    return(list(xlim = xlim, ylim = ylim))
  }
  pad_x <- max(0, (pin[1] / unit_scale - x_span) / 2)
  pad_y <- max(0, (pin[2] / unit_scale - y_span) / 2)
  list(
    xlim = c(xlim[1] + pad_x, xlim[2] - pad_x),
    ylim = c(ylim[1] + pad_y, ylim[2] - pad_y)
  )
}
format_lon_label <- function(x) {
  sprintf("%.2f\u00B0%s", abs(x), ifelse(x < 0, "W", "E"))
}
format_lat_label <- function(x) {
  sprintf("%.2f\u00B0%s", abs(x), ifelse(x < 0, "S", "N"))
}
transform_axis_points <- function(x, y, from_epsg, to_epsg = 4326L) {
  pts <- tryCatch(
    sf::st_as_sf(
      data.frame(x = x, y = y),
      coords = c("x", "y"),
      crs = as.integer(from_epsg)
    ),
    error = function(e) NULL
  )
  if (is.null(pts)) {
    return(NULL)
  }
  pts_t <- tryCatch(sf::st_transform(pts, as.integer(to_epsg)), error = function(e) NULL)
  if (is.null(pts_t)) {
    return(NULL)
  }
  sf::st_coordinates(pts_t)
}
compute_plot_x_delta_for_lon_degrees <- function(plot_epsg, xlim, ylim, lon_delta_deg = 0.02) {
  lon_delta_deg <- suppressWarnings(as.numeric(lon_delta_deg))
  if (!is.finite(lon_delta_deg) || lon_delta_deg <= 0) {
    return(NA_real_)
  }
  if (identical(as.integer(plot_epsg), 4326L)) {
    return(lon_delta_deg)
  }
  center_ll <- transform_axis_points(
    x = mean(xlim),
    y = mean(ylim),
    from_epsg = plot_epsg,
    to_epsg = 4326L
  )
  if (is.null(center_ll) || nrow(center_ll) == 0 || !all(is.finite(center_ll[1, ]))) {
    return(NA_real_)
  }
  center_lon <- center_ll[1, 1]
  center_lat <- center_ll[1, 2]
  target_xy <- transform_axis_points(
    x = c(center_lon, center_lon + lon_delta_deg),
    y = c(center_lat, center_lat),
    from_epsg = 4326L,
    to_epsg = plot_epsg
  )
  if (is.null(target_xy) || nrow(target_xy) < 2 || !all(is.finite(target_xy[, 1]))) {
    return(NA_real_)
  }
  abs(target_xy[2, 1] - target_xy[1, 1])
}
draw_geo_axes <- function(plot_epsg, xlim, ylim) {
  effective_limits <- get_effective_map_extent(default_xlim = xlim, default_ylim = ylim)
  visible_limits <- get_visible_map_extent(
    xlim = effective_limits$xlim,
    ylim = effective_limits$ylim
  )
  xlim <- visible_limits$xlim
  ylim <- visible_limits$ylim
  tick_col <- "#505050"
  label_cex <- 0.76
  usr <- graphics::par("usr")
  pin <- graphics::par("pin")
  x_span <- diff(xlim)
  y_span <- diff(ylim)
  graphics::rect(
    xleft = usr[1],
    ybottom = usr[3],
    xright = usr[2],
    ytop = ylim[1] + y_span * 0.030,
    col = "white",
    border = NA,
    xpd = NA
  )
  x_ticks <- pretty(xlim, n = 5)
  y_ticks <- pretty(ylim, n = 5)
  if (identical(as.integer(plot_epsg), 4326L)) {
    x_labels <- format_lon_label(x_ticks)
    y_labels <- format_lat_label(y_ticks)
  } else {
    x_coords <- transform_axis_points(
      x = x_ticks,
      y = rep(mean(ylim), length(x_ticks)),
      from_epsg = plot_epsg
    )
    y_coords <- transform_axis_points(
      x = rep(mean(xlim), length(y_ticks)),
      y = y_ticks,
      from_epsg = plot_epsg
    )
    if (is.null(x_coords) || is.null(y_coords)) {
      return(invisible(NULL))
    }
    x_labels <- format_lon_label(x_coords[, 1])
    y_labels <- format_lat_label(y_coords[, 2])
  }
  keep_x <- x_ticks >= xlim[1] & x_ticks <= xlim[2]
  keep_y <- y_ticks >= ylim[1] & y_ticks <= ylim[2]
  x_ticks <- x_ticks[keep_x]
  y_ticks <- y_ticks[keep_y]
  x_labels <- x_labels[keep_x]
  y_labels <- y_labels[keep_y]
  x_tick_top <- ylim[1] + y_span * 0.015
  x_label_y <- ylim[1] + y_span * 0.012
  y_tick_right <- xlim[1] + x_span * 0.015
  y_label_x <- xlim[1] + x_span * 0.015
  graphics::segments(
    x0 = xlim[1],
    y0 = ylim[1],
    x1 = xlim[2],
    y1 = ylim[1],
    col = grDevices::adjustcolor(tick_col, alpha.f = 0.72),
    lwd = 0.95,
    xpd = NA
  )
  graphics::segments(
    x0 = xlim[1],
    y0 = ylim[1],
    x1 = xlim[1],
    y1 = ylim[2],
    col = grDevices::adjustcolor(tick_col, alpha.f = 0.72),
    lwd = 0.95,
    xpd = NA
  )
  if (length(x_ticks) > 0) {
    graphics::segments(
      x0 = x_ticks,
      y0 = ylim[1],
      x1 = x_ticks,
      y1 = x_tick_top,
      col = grDevices::adjustcolor(tick_col, alpha.f = 0.98),
      lwd = 1.15,
      xpd = NA
    )
    for (i in seq_along(x_ticks)) {
      draw_text_box(
        x = x_ticks[i],
        y = x_label_y,
        label = x_labels[i],
        cex = label_cex,
        usr = usr,
        pin = pin,
        fill = grDevices::adjustcolor("white", alpha.f = 0.60),
        border = grDevices::adjustcolor("#888888", alpha.f = 0.10),
        col = tick_col,
        lwd = 0.22
      )
    }
  }
  if (length(y_ticks) > 0) {
    graphics::segments(
      x0 = xlim[1],
      y0 = y_ticks,
      x1 = y_tick_right,
      y1 = y_ticks,
      col = grDevices::adjustcolor(tick_col, alpha.f = 0.98),
      lwd = 1.15,
      xpd = NA
    )
    for (i in seq_along(y_ticks)) {
      draw_text_box(
        x = y_label_x,
        y = y_ticks[i],
        label = y_labels[i],
        cex = label_cex,
        usr = usr,
        pin = pin,
        fill = grDevices::adjustcolor("white", alpha.f = 0.60),
        border = grDevices::adjustcolor("#888888", alpha.f = 0.10),
        col = tick_col,
        lwd = 0.22
      )
    }
  }
}
draw_source_footer <- function(cfg, xlim = NULL, ylim = NULL) {
  provider <- if (!is.null(cfg$osm_provider) && nzchar(cfg$osm_provider)) {
    as.character(cfg$osm_provider)
  } else {
    "OpenStreetMap"
  }
  pin <- graphics::par("pin")
  if (length(xlim) == 2 && length(ylim) == 2 &&
      all(is.finite(xlim)) && all(is.finite(ylim))) {
    visible_limits <- get_visible_map_extent(xlim = xlim, ylim = ylim)
    xlim <- visible_limits$xlim
    ylim <- visible_limits$ylim
  } else {
    usr <- graphics::par("usr")
    xlim <- usr[1:2]
    ylim <- usr[3:4]
  }
  x_span <- diff(xlim)
  y_span <- diff(ylim)
  footer_label <- sprintf(
    "Author: Janek Wuigk | Basemap: %s / OpenStreetMap contributors",
    provider
  )
  footer_cex <- 0.68
  footer_x <- xlim[1] + x_span * 0.003
  footer_y <- ylim[1] + y_span * 0.014
  x_per_in <- x_span / pin[1]
  y_per_in <- y_span / pin[2]
  label_w <- graphics::strwidth(footer_label, cex = footer_cex, units = "inches") * x_per_in
  label_h <- graphics::strheight(footer_label, cex = footer_cex, units = "inches") * y_per_in
  pad_x <- x_per_in * 0.08
  pad_y <- y_per_in * 0.05
  graphics::rect(
    xleft = xlim[1],
    ybottom = footer_y - label_h * 0.5 - pad_y,
    xright = min(xlim[2], footer_x + label_w + pad_x * 6.0),
    ytop = footer_y + label_h * 0.5 + pad_y,
    col = grDevices::adjustcolor("white", alpha.f = 0.96),
    border = grDevices::adjustcolor("#777777", alpha.f = 0.25),
    lwd = 0.5,
    xpd = NA
  )
  graphics::text(
    x = footer_x,
    y = footer_y,
    adj = c(0, 0.5),
    cex = footer_cex,
    col = "#404040",
    labels = footer_label,
    xpd = NA
  )
}
draw_map_title <- function(title_text, xlim, ylim) {
  visible_limits <- get_visible_map_extent(xlim = xlim, ylim = ylim)
  xlim <- visible_limits$xlim
  ylim <- visible_limits$ylim
  usr <- graphics::par("usr")
  pin <- graphics::par("pin")
  y_span <- diff(ylim)
  target_cex <- 2.85
  available_width_in <- pin[1] * (diff(xlim) / max(usr[2] - usr[1], 1e-9))
  title_width_in <- graphics::strwidth(title_text, cex = target_cex, units = "inches")
  fit_scale <- if (is.finite(title_width_in) && title_width_in > 0) {
    min(1, max(0.55, (available_width_in - 0.18) / title_width_in))
  } else {
    1
  }
  title_cex <- target_cex * fit_scale
  title_box <- compute_text_box(
    label = title_text,
    cex = title_cex,
    usr = usr,
    pin = pin,
    pad_in_x = 0.014,
    pad_in_y = 0.018
  )
  title_y <- ylim[2] - title_box$half_height_y + y_span * 0.040
  graphics::rect(
    xleft = xlim[1],
    ybottom = title_y - title_box$half_height_y,
    xright = xlim[2],
    ytop = title_y + title_box$half_height_y + y_span * 0.120,
    col = grDevices::adjustcolor("white", alpha.f = 0.98),
    border = grDevices::adjustcolor("#777777", alpha.f = 0.30),
    lwd = 0.5,
    xpd = NA
  )
  graphics::text(
    x = mean(xlim),
    y = title_y,
    labels = title_text,
    cex = title_cex,
    font = 2,
    col = "#111111",
    adj = c(0.5, 0.5),
    xpd = NA
  )
}
draw_reference_station_legend <- function(layout = NULL) {
  usr <- graphics::par("usr")
  visible_limits <- get_visible_map_extent(xlim = usr[1:2], ylim = usr[3:4])
  x_span <- usr[2] - usr[1]
  y_span <- usr[4] - usr[3]
  visible_x_span <- diff(visible_limits$xlim)
  visible_y_span <- diff(visible_limits$ylim)
  use_layout <- !is.null(layout) && all(c("panel_left", "panel_bottom") %in% names(layout))
  if (use_layout) {
    box_left <- visible_limits$xlim[1] + visible_x_span * 0.59
    box_right <- layout$panel_left - visible_x_span * 0.028
    min_width <- visible_x_span * 0.27
    if (!is.finite(box_right) || box_right <= box_left + min_width) {
      box_right <- box_left + min_width
    }
    legend_width <- box_right - box_left
    box_bottom <- visible_limits$ylim[1] + visible_y_span * 0.090
    box_top <- box_bottom + visible_y_span * 0.116
    entry_y <- c(
      box_top - (box_top - box_bottom) * 0.34,
      box_top - (box_top - box_bottom) * 0.76
    )
    circle_x <- box_left + legend_width * 0.09
    text_x <- box_left + legend_width * 0.21
  } else {
    entry_y <- c(
      usr[4] - y_span * 0.220,
      usr[4] - y_span * 0.285
    )
    circle_x <- usr[1] + x_span * 0.046
    text_x <- usr[1] + x_span * 0.076
    box_left <- usr[1] + x_span * 0.024
    box_right <- usr[1] + x_span * 0.255
    box_bottom <- usr[4] - y_span * 0.335
    box_top <- usr[4] - y_span * 0.175
  }
  entries <- c(
    "144 - Air quality measurement station\nSilbersteinstra\u00DFe, Neuk\u00F6lln",
    "124 - Air quality measurement station\nMariendorfer Damm"
  )
  labels <- c("144", "124")
  graphics::rect(
    xleft = box_left,
    ybottom = box_bottom,
    xright = box_right,
    ytop = box_top,
    col = grDevices::adjustcolor("white", alpha.f = 0.72),
    border = grDevices::adjustcolor("#777777", alpha.f = 0.40),
    lwd = 0.6,
    xpd = NA
  )
  for (i in seq_along(entries)) {
    graphics::points(
      x = circle_x,
      y = entry_y[i],
      pch = 21,
      cex = if (use_layout) 3.75 else 3.75,
      bg = grDevices::adjustcolor("white", alpha.f = 0.95),
      col = "#202020",
      lwd = 1.0,
      xpd = NA
    )
    graphics::text(
      x = circle_x,
      y = entry_y[i],
      labels = labels[i],
      cex = if (use_layout) 1.10 else 1.08,
      font = 2,
      col = "#202020",
      xpd = NA
    )
    graphics::text(
      x = text_x,
      y = entry_y[i],
      labels = entries[i],
      adj = c(0, 0.5),
      cex = if (use_layout) 1.20 else 0.84,
      col = "#222222",
      xpd = NA
    )
  }
  invisible(list(
    box_left = box_left,
    box_right = box_right,
    box_bottom = box_bottom,
    box_top = box_top
  ))
}
compute_vertex_offset_path <- function(x, y, offset_m) {
  n <- length(x)
  if (n == 0 || !is.finite(offset_m) || abs(offset_m) < 1e-9) {
    return(list(x = x, y = y))
  }
  normals <- matrix(0, nrow = n, ncol = 2)
  for (i in seq_len(n)) {
    tx <- 0
    ty <- 0
    if (i > 1) {
      dx_prev <- x[i] - x[i - 1]
      dy_prev <- y[i] - y[i - 1]
      len_prev <- sqrt(dx_prev^2 + dy_prev^2)
      if (is.finite(len_prev) && len_prev > 0) {
        tx <- tx + dx_prev / len_prev
        ty <- ty + dy_prev / len_prev
      }
    }
    if (i < n) {
      dx_next <- x[i + 1] - x[i]
      dy_next <- y[i + 1] - y[i]
      len_next <- sqrt(dx_next^2 + dy_next^2)
      if (is.finite(len_next) && len_next > 0) {
        tx <- tx + dx_next / len_next
        ty <- ty + dy_next / len_next
      }
    }
    tan_len <- sqrt(tx^2 + ty^2)
    if (!is.finite(tan_len) || tan_len <= 0) {
      if (i < n) {
        dx <- x[i + 1] - x[i]
        dy <- y[i + 1] - y[i]
      } else if (i > 1) {
        dx <- x[i] - x[i - 1]
        dy <- y[i] - y[i - 1]
      } else {
        next
      }
      d <- sqrt(dx^2 + dy^2)
      if (!is.finite(d) || d <= 0) {
        next
      }
      tx <- dx / d
      ty <- dy / d
    } else {
      tx <- tx / tan_len
      ty <- ty / tan_len
    }
    normals[i, 1] <- -ty
    normals[i, 2] <- tx
  }
  list(
    x = x + normals[, 1] * offset_m,
    y = y + normals[, 2] * offset_m
  )
}
build_colored_segments_from_points <- function(points_df, value_col, offset_m) {
  if (!value_col %in% names(points_df)) {
    return(data.frame())
  }
  if (nrow(points_df) < 2) {
    return(data.frame())
  }
  step_vals <- suppressWarnings(as.numeric(points_df$step_distance_m))
  step_vals <- step_vals[is.finite(step_vals) & step_vals > 0]
  if (length(step_vals) > 0) {
    typical_step <- stats::median(step_vals)
    # Keep parallel rails distinct without letting them drift too far from the path.
    max_safe_offset <- max(30.0, typical_step * 20.0)
    effective_offset <- sign(offset_m) * min(abs(offset_m), max_safe_offset)
  } else {
    effective_offset <- sign(offset_m) * min(abs(offset_m), 35.0)
  }
  rows <- vector("list", nrow(points_df))
  row_idx <- 0L
  for (sub in split(points_df, points_df$subroute_id, drop = TRUE)) {
    if (nrow(sub) < 2) {
      next
    }
    sub_x <- suppressWarnings(as.numeric(sub$x))
    sub_y <- suppressWarnings(as.numeric(sub$y))
    shifted <- compute_vertex_offset_path(sub_x, sub_y, effective_offset)
    vals <- suppressWarnings(as.numeric(sub[[value_col]]))
    for (i in seq_len(nrow(sub) - 1)) {
      x0 <- shifted$x[i]
      y0 <- shifted$y[i]
      x1 <- shifted$x[i + 1]
      y1 <- shifted$y[i + 1]
      dx <- x1 - x0
      dy <- y1 - y0
      seg_len <- sqrt(dx^2 + dy^2)
      if (!is.finite(seg_len) || seg_len <= 0) {
        next
      }
      v0 <- vals[i]
      v1 <- vals[i + 1]
      if (!is.finite(v0) && !is.finite(v1)) {
        next
      }
      seg_value <- mean(c(v0, v1), na.rm = TRUE)
      row_idx <- row_idx + 1L
      rows[[row_idx]] <- list(
        x = x0,
        y = y0,
        xend = x1,
        yend = y1,
        value = seg_value
      )
    }
  }
  if (row_idx == 0L) {
    return(data.frame())
  }
  as.data.frame(data.table::rbindlist(rows[seq_len(row_idx)], fill = TRUE))
}
transform_points_df_xy <- function(df, from_epsg, to_epsg, x_col = "x", y_col = "y") {
  if (nrow(df) == 0 || identical(as.integer(from_epsg), as.integer(to_epsg))) {
    return(df)
  }
  out <- df
  idx <- which(is.finite(out[[x_col]]) & is.finite(out[[y_col]]))
  if (length(idx) == 0) {
    return(out)
  }
  pts <- sf::st_as_sf(
    out[idx, , drop = FALSE],
    coords = c(x_col, y_col),
    crs = as.integer(from_epsg),
    remove = FALSE
  )
  pts_t <- sf::st_transform(pts, as.integer(to_epsg))
  coords_t <- sf::st_coordinates(pts_t)
  out[[x_col]][idx] <- coords_t[, 1]
  out[[y_col]][idx] <- coords_t[, 2]
  out
}
transform_segment_df_xy <- function(seg_df, from_epsg, to_epsg) {
  if (nrow(seg_df) == 0 || identical(as.integer(from_epsg), as.integer(to_epsg))) {
    return(seg_df)
  }
  out <- seg_df
  p0 <- data.frame(x = out$x, y = out$y)
  p1 <- data.frame(x = out$xend, y = out$yend)
  p0_t <- transform_points_df_xy(p0, from_epsg = from_epsg, to_epsg = to_epsg, x_col = "x", y_col = "y")
  p1_t <- transform_points_df_xy(p1, from_epsg = from_epsg, to_epsg = to_epsg, x_col = "x", y_col = "y")
  out$x <- p0_t$x
  out$y <- p0_t$y
  out$xend <- p1_t$x
  out$yend <- p1_t$y
  out
}
build_bbox_matrix <- function(xlim, ylim) {
  box <- rbind(
    c(xlim[1], xlim[2]),
    c(ylim[1], ylim[2])
  )
  rownames(box) <- c("x", "y")
  colnames(box) <- c("min", "max")
  box
}
try_plot_osm_background <- function(xlim, ylim, cfg, plot_epsg) {
  LAST_BASEMAP_EXTENT <<- NULL
  if (!isTRUE(cfg$osm_background)) {
    return(FALSE)
  }
  plot_epsg <- as.integer(plot_epsg)
  engine <- tolower(as.character(cfg$basemap_engine)[1])
  if (!engine %in% c("auto", "maptiles", "rosm")) {
    engine <- "auto"
  }
  if (engine %in% c("auto", "maptiles")) {
    ok_tiles <- try_plot_maptiles_background(
      xlim = xlim,
      ylim = ylim,
      cfg = cfg,
      plot_epsg = plot_epsg
    )
    if (ok_tiles) {
      log_basemap(cfg, sprintf("Basemap backend used: maptiles (EPSG:%d)", plot_epsg))
      return(TRUE)
    }
    if (engine == "maptiles") {
      return(FALSE)
    }
  }
  if (!isTRUE(HAS_ROSM)) {
    return(FALSE)
  }
  if (identical(plot_epsg, 3857L) && !requireNamespace("sp", quietly = TRUE)) {
    return(FALSE)
  }
  bbox_ll <- tryCatch(
    {
      if (identical(plot_epsg, as.integer(cfg$wgs84_epsg))) {
        list(xlim = xlim, ylim = ylim)
      } else if (identical(plot_epsg, 3857L)) {
        corners <- expand.grid(
          x = c(xlim[1], xlim[2]),
          y = c(ylim[1], ylim[2])
        )
        pts <- sf::st_as_sf(corners, coords = c("x", "y"), crs = 3857, remove = FALSE)
        pts_ll <- sf::st_transform(pts, as.integer(cfg$wgs84_epsg))
        ll_xy <- sf::st_coordinates(pts_ll)
        list(
          xlim = range(ll_xy[, 1], na.rm = TRUE),
          ylim = range(ll_xy[, 2], na.rm = TRUE)
        )
      } else {
        NULL
      }
    },
    error = function(e) NULL
  )
  if (is.null(bbox_ll)) {
    return(FALSE)
  }
  bbox <- build_bbox_matrix(xlim = bbox_ll$xlim, ylim = bbox_ll$ylim)
  use_projected_tiles <- identical(plot_epsg, 3857L)
  ok <- tryCatch(
    {
      rosm::osm.plot(
        bbox = bbox,
        zoomin = as.integer(cfg$osm_zoomin),
        type = cfg$osm_type,
        stoponlargerequest = !isTRUE(cfg$osm_allow_large_requests),
        fusetiles = TRUE,
        res = as.numeric(cfg$osm_res),
        project = use_projected_tiles,
        progress = "none",
        quiet = TRUE,
        asp = 1,
        axes = FALSE,
        xlab = "",
        ylab = ""
      )
      LAST_BASEMAP_EXTENT <<- list(xlim = xlim, ylim = ylim)
      TRUE
    },
    error = function(e) FALSE
  )
  if (ok) {
    log_basemap(cfg, sprintf("Basemap backend used: rosm (EPSG:%d)", plot_epsg))
  } else {
    log_basemap(cfg, sprintf("Basemap backend failed: rosm (EPSG:%d)", plot_epsg))
  }
  isTRUE(ok)
}
compute_xy_limits <- function(route_xy, points_df, segment_layers, min_pad = 1.0, pad_frac = 0.08) {
  x_vals <- c(route_xy[, 1], points_df$x)
  y_vals <- c(route_xy[, 2], points_df$y)
  if (length(segment_layers) > 0) {
    for (seg_df in segment_layers) {
      x_vals <- c(x_vals, seg_df$x, seg_df$xend)
      y_vals <- c(y_vals, seg_df$y, seg_df$yend)
    }
  }
  x_vals <- x_vals[is.finite(x_vals)]
  y_vals <- y_vals[is.finite(y_vals)]
  if (length(x_vals) == 0 || length(y_vals) == 0) {
    return(list(xlim = c(0, 1), ylim = c(0, 1)))
  }
  xlim <- range(x_vals)
  ylim <- range(y_vals)
  if (!is.finite(diff(xlim)) || diff(xlim) <= 0) {
    xlim <- xlim + c(-0.5, 0.5)
  }
  if (!is.finite(diff(ylim)) || diff(ylim) <= 0) {
    ylim <- ylim + c(-0.5, 0.5)
  }
  pad_x <- max(diff(xlim) * pad_frac, min_pad)
  pad_y <- max(diff(ylim) * pad_frac, min_pad)
  list(
    xlim = c(xlim[1] - pad_x, xlim[2] + pad_x),
    ylim = c(ylim[1] - pad_y, ylim[2] + pad_y)
  )
}
compute_offset_units_from_screen <- function(xlim, png_width, bar_count, line_offset_screen_px) {
  if (!is.finite(line_offset_screen_px) || line_offset_screen_px <= 0) {
    return(0)
  }
  x_span <- diff(xlim)
  if (!is.finite(x_span) || x_span <= 0) {
    return(0)
  }
  # Approximate horizontal data area after margins/colorbars.
  right_frac <- min(0.50, 0.12 + 0.045 * max(0, bar_count))
  data_frac <- max(0.35, 1 - right_frac)
  data_px <- max(200, png_width * data_frac)
  units_per_px <- x_span / data_px
  as.numeric(line_offset_screen_px * units_per_px)
}
get_reference_markers_plot <- function(plot_epsg) {
  pts <- tryCatch(
    sf::st_as_sf(
      REFERENCE_MARKERS_WGS84,
      coords = c("lon", "lat"),
      crs = 4326,
      remove = FALSE
    ),
    error = function(e) NULL
  )
  if (is.null(pts)) {
    return(data.frame())
  }
  pts_plot <- tryCatch(
    {
      if (identical(as.integer(plot_epsg), 4326L)) pts else sf::st_transform(pts, as.integer(plot_epsg))
    },
    error = function(e) NULL
  )
  if (is.null(pts_plot)) {
    return(data.frame())
  }
  xy <- sf::st_coordinates(pts_plot)
  out <- sf::st_drop_geometry(pts_plot)
  out$x <- xy[, 1]
  out$y <- xy[, 2]
  out
}
draw_reference_markers <- function(plot_epsg) {
  markers <- get_reference_markers_plot(plot_epsg)
  if (nrow(markers) == 0) {
    return(invisible(NULL))
  }
  usr <- graphics::par("usr")
  visible_limits <- get_visible_map_extent(xlim = usr[1:2], ylim = usr[3:4])
  visible_x_span <- diff(visible_limits$xlim)
  visible_y_span <- diff(visible_limits$ylim)
  y_offset <- (usr[4] - usr[3]) * 0.013
  move_124 <- markers$label == "124"
  if (any(move_124)) {
    markers$x[move_124] <- markers$x[move_124] - visible_x_span * 0.048
    markers$y[move_124] <- markers$y[move_124] - visible_y_span * 0.032
  }
  graphics::points(
    x = markers$x,
    y = markers$y - y_offset,
    pch = 21,
    cex = 5.35,
    bg = grDevices::adjustcolor("white", alpha.f = 0.92),
    col = "#202020",
    lwd = 1.25,
    xpd = NA
  )
  graphics::text(
    x = markers$x,
    y = markers$y - y_offset,
    labels = markers$label,
    cex = 1.36,
    font = 2,
    col = "#202020",
    xpd = NA
  )
}
draw_route_annotations <- function(xlim, ylim) {
  visible_limits <- get_visible_map_extent(xlim = xlim, ylim = ylim)
  usr <- graphics::par("usr")
  pin <- graphics::par("pin")
  x_span <- diff(visible_limits$xlim)
  y_span <- diff(visible_limits$ylim)
  annotation_defs <- list(
    list(
      label = "Public transport route",
      x = visible_limits$xlim[1] + x_span * 0.33,
      y = visible_limits$ylim[1] + y_span * 0.72
    ),
    list(
      label = "Bike route",
      x = visible_limits$xlim[1] + x_span * 0.60,
      y = visible_limits$ylim[1] + y_span * 0.48
    )
  )
  for (def in annotation_defs) {
    draw_text_box(
      x = def$x,
      y = def$y,
      label = def$label,
      cex = 1.64,
      usr = usr,
      pin = pin,
      fill = grDevices::adjustcolor("white", alpha.f = 0.86),
      border = grDevices::adjustcolor("#666666", alpha.f = 0.34),
      col = "#202020",
      font = 2,
      xpd = NA,
      pad_in_x = 0.040,
      pad_in_y = 0.024
    )
  }
}
palette_luminance <- function(cols) {
  rgb <- grDevices::col2rgb(cols) / 255
  0.2126 * rgb[1, ] + 0.7152 * rgb[2, ] + 0.0722 * rgb[3, ]
}
compute_text_box <- function(label, cex, usr, pin, pad_in_x = 0.04, pad_in_y = 0.022) {
  x_per_in <- (usr[2] - usr[1]) / pin[1]
  y_per_in <- (usr[4] - usr[3]) / pin[2]
  label_w_in <- graphics::strwidth(label, cex = cex, units = "inches")
  label_h_in <- graphics::strheight(label, cex = cex, units = "inches")
  list(
    half_width_x = (label_w_in / 2 + pad_in_x) * x_per_in,
    half_height_y = (label_h_in / 2 + pad_in_y) * y_per_in
  )
}
draw_text_box <- function(
  x,
  y,
  label,
  cex,
  usr,
  pin,
  fill = grDevices::adjustcolor("white", alpha.f = 0.72),
  border = grDevices::adjustcolor("#666666", alpha.f = 0.32),
  col = "#303030",
  font = 1,
  xpd = NA,
  pad_in_x = 0.04,
  pad_in_y = 0.022,
  lwd = 0.45
) {
  box <- compute_text_box(
    label = label,
    cex = cex,
    usr = usr,
    pin = pin,
    pad_in_x = pad_in_x,
    pad_in_y = pad_in_y
  )
  graphics::rect(
    xleft = x - box$half_width_x,
    ybottom = y - box$half_height_y,
    xright = x + box$half_width_x,
    ytop = y + box$half_height_y,
    col = fill,
    border = border,
    lwd = lwd,
    xpd = xpd
  )
  graphics::text(
    x = x,
    y = y,
    labels = label,
    cex = cex,
    font = font,
    col = col,
    adj = c(0.5, 0.5),
    xpd = xpd
  )
  invisible(box)
}
compute_rotated_label_box <- function(label, cex, usr, pin, pad_in = 0.045) {
  x_per_in <- (usr[2] - usr[1]) / pin[1]
  y_per_in <- (usr[4] - usr[3]) / pin[2]
  label_w_in <- graphics::strwidth(label, cex = cex, units = "inches")
  label_h_in <- graphics::strheight(label, cex = cex, units = "inches")
  list(
    half_width_x = (label_h_in / 2 + pad_in) * x_per_in,
    half_height_y = (label_w_in / 2 + pad_in) * y_per_in
  )
}
draw_colorbars <- function(colorbar_defs, cfg = NULL) {
  if (length(colorbar_defs) == 0) {
    return(invisible(NULL))
  }
  label_cex <- if (!is.null(cfg$legend_label_cex)) as.numeric(cfg$legend_label_cex) else 1.08
  value_cex <- if (!is.null(cfg$legend_value_cex)) as.numeric(cfg$legend_value_cex) else 0.92
  label_cex <- label_cex * 1.2
  value_cex <- value_cex * 1.2
  bar_width_frac <- if (!is.null(cfg$legend_bar_width_frac)) as.numeric(cfg$legend_bar_width_frac) else 0.026
  bar_gap_frac <- if (!is.null(cfg$legend_bar_gap_frac)) as.numeric(cfg$legend_bar_gap_frac) else 0.016
  bar_gap_frac <- bar_gap_frac * 1.55
  legend_inside <- if (!is.null(cfg$legend_inside)) isTRUE(cfg$legend_inside) else TRUE
  legend_bg_alpha <- if (!is.null(cfg$legend_bg_alpha)) as.numeric(cfg$legend_bg_alpha) else 0.68
  bar_width_frac <- min(max(bar_width_frac, 0.010), 0.080)
  bar_gap_frac <- min(max(bar_gap_frac, 0.006), 0.080)
  usr <- graphics::par("usr")
  pin <- graphics::par("pin")
  x_span <- usr[2] - usr[1]
  y_span <- usr[4] - usr[3]
  bar_width <- x_span * bar_width_frac
  bar_gap <- x_span * bar_gap_frac
  y_bottom <- usr[3] + y_span * 0.14
  y_top <- usr[4] - y_span * 0.14
  n_bars <- length(colorbar_defs)
  total_width <- n_bars * bar_width + (n_bars - 1) * bar_gap
  if (legend_inside) {
    panel_pad_x <- x_span * 0.012
    label_room <- x_span * 0.032
    panel_right <- usr[2] - x_span * 0.008
    bar_x_start <- panel_right - panel_pad_x - label_room - total_width
    panel_left <- bar_x_start - panel_pad_x
    panel_bottom <- y_bottom - y_span * 0.11
    panel_top <- y_top + y_span * 0.07
    aux_left <- panel_left
    aux_right <- panel_left
  } else {
    panel_pad_x <- x_span * 0.012
    label_room <- x_span * 0.032
    panel_left <- usr[2] + x_span * 0.230
    bar_x_start <- panel_left + panel_pad_x
    panel_right <- bar_x_start + total_width + panel_pad_x + label_room
    panel_bottom <- y_bottom - y_span * 0.11
    panel_top <- y_top + y_span * 0.07
    aux_left <- usr[2] + x_span * 0.012
    aux_right <- panel_left - x_span * 0.010
  }
  graphics::rect(
    xleft = panel_left,
    ybottom = panel_bottom,
    xright = panel_right,
    ytop = panel_top,
    col = grDevices::adjustcolor("white", alpha.f = legend_bg_alpha),
    border = grDevices::adjustcolor("#555555", alpha.f = 0.55),
    lwd = 0.7,
    xpd = NA
  )
  for (i in seq_along(colorbar_defs)) {
    def <- colorbar_defs[[i]]
    palette <- def$palette
    norm <- def$norm
    unit <- if (!is.null(def$unit)) as.character(def$unit) else ""
    x_left <- bar_x_start + (i - 1) * (bar_width + bar_gap)
    x_right <- x_left + bar_width
    y_breaks <- seq(y_bottom, y_top, length.out = length(palette) + 1)
    for (j in seq_len(length(palette))) {
      graphics::rect(
        xleft = x_left,
        ybottom = y_breaks[j],
        xright = x_right,
        ytop = y_breaks[j + 1],
        col = palette[j],
        border = NA,
        xpd = NA
      )
    }
    graphics::rect(
      xleft = x_left,
      ybottom = y_bottom,
      xright = x_right,
      ytop = y_top,
      border = "#444444",
      lwd = 0.8,
      xpd = NA
    )
    tick_vals <- pretty(c(norm$vmin, norm$vmax), n = 5)
    tick_vals <- tick_vals[tick_vals >= norm$vmin & tick_vals <= norm$vmax]
    if (length(tick_vals) > 0 && is.finite(norm$vmin) && is.finite(norm$vmax) && norm$vmax > norm$vmin) {
      tick_frac <- (tick_vals - norm$vmin) / (norm$vmax - norm$vmin)
      tick_y <- y_bottom + tick_frac * (y_top - y_bottom)
      graphics::segments(
        x0 = x_right,
        y0 = tick_y,
        x1 = x_right + x_span * 0.005,
        y1 = tick_y,
        col = grDevices::adjustcolor("#4A4A4A", alpha.f = 0.8),
        lwd = 0.8,
        xpd = NA
      )
      graphics::text(
        x = x_right + x_span * 0.008,
        y = tick_y,
        labels = vapply(tick_vals, function(x) format_display_num(x, unit = unit, digits = 3), character(1)),
        adj = c(0, 0.5),
        cex = value_cex * 0.90,
        col = "#4A4A4A",
        xpd = NA
      )
    }
    label_y <- y_bottom + (y_top - y_bottom) * 0.5
    label_box <- compute_rotated_label_box(
      label = def$label,
      cex = label_cex,
      usr = usr,
      pin = pin
    )
    graphics::rect(
      xleft = (x_left + x_right) / 2 - label_box$half_width_x,
      ybottom = label_y - label_box$half_height_y,
      xright = (x_left + x_right) / 2 + label_box$half_width_x,
      ytop = label_y + label_box$half_height_y,
      col = grDevices::adjustcolor("white", alpha.f = 0.68),
      border = grDevices::adjustcolor("#666666", alpha.f = 0.45),
      lwd = 0.5,
      xpd = NA
    )
    graphics::text(
      x = (x_left + x_right) / 2,
      y = label_y,
      labels = def$label,
      srt = 90,
      adj = c(0.5, 0.5),
      cex = label_cex,
      font = 2,
      col = "#1A1A1A",
      xpd = NA
    )
    draw_text_box(
      x = (x_left + x_right) / 2,
      y = y_bottom - y_span * 0.035,
      label = format_num_with_unit(norm$vmin, unit = unit, digits = 3),
      cex = value_cex * 1.04,
      usr = usr,
      pin = pin,
      fill = grDevices::adjustcolor("white", alpha.f = 0.86),
      border = grDevices::adjustcolor("#777777", alpha.f = 0.35),
      col = "#1A1A1A"
    )
    draw_text_box(
      x = (x_left + x_right) / 2,
      y = y_top + y_span * 0.035,
      label = format_num_with_unit(norm$vmax, unit = unit, digits = 3),
      cex = value_cex * 1.04,
      usr = usr,
      pin = pin,
      fill = grDevices::adjustcolor("white", alpha.f = 0.86),
      border = grDevices::adjustcolor("#777777", alpha.f = 0.35),
      col = "#1A1A1A"
    )
  }
  invisible(list(
    panel_left = panel_left,
    panel_right = panel_right,
    panel_bottom = panel_bottom,
    panel_top = panel_top,
    aux_left = aux_left,
    aux_right = aux_right,
    bar_x_start = bar_x_start,
    y_bottom = y_bottom,
    y_top = y_top,
    bar_width = bar_width,
    bar_gap = bar_gap,
    x_span = x_span,
    y_span = y_span
  ))
}
plot_route_heatmap <- function(
  route_line_sfc,
  points_df,
  route_name,
  out_png,
  display_cols,
  style_maps,
  cfg,
  source_crs_epsg = cfg$crs_epsg,
  plot_epsg = cfg$crs_epsg
) {
  route_line_plot <- route_line_sfc
  if (!identical(as.integer(source_crs_epsg), as.integer(plot_epsg))) {
    route_line_plot <- sf::st_transform(route_line_sfc, as.integer(plot_epsg))
  }
  route_xy <- sf::st_coordinates(route_line_plot)[, 1:2, drop = FALSE]
  points_plot <- transform_points_df_xy(
    points_df,
    from_epsg = source_crs_epsg,
    to_epsg = plot_epsg,
    x_col = "x",
    y_col = "y"
  )
  planned_bar_count <- length(display_cols)
  png_width <- as.integer(cfg$png_width_px + planned_bar_count * 520)
  png_height <- as.integer(cfg$png_height_px)
  min_pad <- if (identical(as.integer(plot_epsg), as.integer(cfg$wgs84_epsg))) 0.0005 else 1.0
  base_limits <- compute_xy_limits(
    route_xy,
    points_plot,
    list(),
    min_pad = min_pad,
    pad_frac = cfg$bbox_pad_frac
  )
  line_offset_plot_units <- if (length(display_cols) > 1) {
    offset_units <- compute_offset_units_from_screen(
      xlim = base_limits$xlim,
      png_width = png_width,
      bar_count = planned_bar_count,
      line_offset_screen_px = cfg$line_offset_screen_px
    )
    if (is.finite(offset_units) && offset_units > 0) offset_units else cfg$line_offset_m
  } else {
    cfg$line_offset_m
  }
  segment_layers <- list()
  colorbar_defs <- list()
  for (col in display_cols) {
    value_col <- choose_visual_value_col(points_df, col, prefer_smoothed = TRUE)
    offset_slot <- if (col %in% names(style_maps$offsets)) style_maps$offsets[[col]] else 0
    offset_m <- offset_slot * line_offset_plot_units
    seg_df <- build_colored_segments_from_points(points_df, value_col, offset_m)
    if (nrow(seg_df) == 0) {
      next
    }
    seg_df <- transform_segment_df_xy(
      seg_df,
      from_epsg = source_crs_epsg,
      to_epsg = plot_epsg
    )
    norm <- build_norm_from_values(seg_df$value)
    cmap <- if (!is.null(style_maps$cmaps[[col]])) style_maps$cmaps[[col]] else "viridis"
    palette <- get_palette(cmap, n = 256L)
    seg_df$plot_color <- map_values_to_colors(seg_df$value, palette, norm)
    segment_layers[[col]] <- seg_df
    label <- if (!is.null(style_maps$labels[[col]])) style_maps$labels[[col]] else col
    colorbar_defs[[length(colorbar_defs) + 1]] <- list(
      label = label,
      palette = palette,
      norm = norm,
      unit = lookup_display_unit(column_name = col, label = label)
    )
  }
  limits <- compute_xy_limits(
    route_xy,
    points_plot,
    segment_layers,
    min_pad = min_pad,
    pad_frac = cfg$bbox_pad_frac
  )
  bar_count <- length(colorbar_defs)
  png_width <- as.integer(cfg$png_width_px + bar_count * 520)
  png_height <- as.integer(cfg$png_height_px)
  grDevices::png(
    filename = out_png,
    width = png_width,
    height = png_height,
    res = as.integer(cfg$png_res_dpi),
    bg = "white"
  )
  on.exit(grDevices::dev.off(), add = TRUE)
  old_par <- graphics::par(no.readonly = TRUE)
  on.exit(graphics::par(old_par), add = TRUE)
  graphics::par(
    mar = c(1.5, 1.5, 4.6, max(14, 10 + bar_count * 4.8)),
    lend = "round",
    ljoin = "round"
  )
  graphics::plot(
    NA,
    xlim = limits$xlim,
    ylim = limits$ylim,
    asp = 1,
    axes = FALSE,
    xlab = "",
    ylab = ""
  )
  used_osm <- try_plot_osm_background(
    xlim = limits$xlim,
    ylim = limits$ylim,
    cfg = cfg,
    plot_epsg = plot_epsg
  )
  if (!used_osm) {
    log_basemap(cfg, sprintf("Basemap unavailable -> blank canvas (EPSG:%d)", as.integer(plot_epsg)))
  }
  graphics::title(
    main = sprintf("Parallel route heatmap lines\n%s (EPSG:%d)", route_name, as.integer(plot_epsg))
  )
  graphics::lines(
    route_xy[, 1],
    route_xy[, 2],
    lwd = 0.9,
    col = grDevices::adjustcolor("black", alpha.f = 0.05)
  )
  for (sub in split(points_plot, points_plot$subroute_id, drop = TRUE)) {
    if (nrow(sub) < 2) {
      next
    }
    graphics::lines(
      sub$x,
      sub$y,
      lwd = 0.8,
      col = grDevices::adjustcolor("black", alpha.f = 0.06)
    )
  }
  for (col in names(segment_layers)) {
    seg_df <- segment_layers[[col]]
    draw_lwd <- if (length(segment_layers) > 1) min(cfg$line_width, 3.6) else cfg$line_width
    outline_lwd <- draw_lwd + max(0, cfg$line_outline_width)
    if (outline_lwd > draw_lwd) {
      graphics::segments(
        x0 = seg_df$x,
        y0 = seg_df$y,
        x1 = seg_df$xend,
        y1 = seg_df$yend,
        col = grDevices::adjustcolor(cfg$line_outline_color, alpha.f = cfg$line_outline_alpha),
        lwd = outline_lwd
      )
    }
    graphics::segments(
      x0 = seg_df$x,
      y0 = seg_df$y,
      x1 = seg_df$xend,
      y1 = seg_df$yend,
      col = grDevices::adjustcolor(seg_df$plot_color, alpha.f = cfg$line_alpha),
      lwd = draw_lwd
    )
  }
  draw_colorbars(colorbar_defs, cfg = cfg)
}
plot_route_dot_map <- function(
  points_df,
  route_name,
  out_png,
  value_col,
  value_label,
  cmap_name,
  cfg,
  source_crs_epsg = cfg$crs_epsg,
  plot_epsg = cfg$wgs84_epsg
) {
  points_plot <- transform_points_df_xy(
    points_df,
    from_epsg = source_crs_epsg,
    to_epsg = plot_epsg,
    x_col = "x",
    y_col = "y"
  )
  if (!value_col %in% names(points_plot)) {
    return(invisible(NULL))
  }
  vals <- suppressWarnings(as.numeric(points_plot[[value_col]]))
  finite_idx <- which(is.finite(points_plot$x) & is.finite(points_plot$y) & is.finite(vals))
  if (length(finite_idx) == 0) {
    return(invisible(NULL))
  }
  plot_df <- points_plot[finite_idx, , drop = FALSE]
  plot_vals <- vals[finite_idx]
  norm <- build_norm_from_values(plot_vals)
  palette <- get_palette(cmap_name, n = 256L)
  point_cols <- map_values_to_colors(plot_vals, palette, norm)
  limits <- compute_xy_limits(
    route_xy = as.matrix(plot_df[, c("x", "y"), drop = FALSE]),
    points_df = plot_df,
    segment_layers = list(),
    min_pad = if (identical(as.integer(plot_epsg), as.integer(cfg$wgs84_epsg))) 0.0005 else 1.0,
    pad_frac = cfg$bbox_pad_frac
  )
  grDevices::png(
    filename = out_png,
    width = as.integer(cfg$png_width_px),
    height = as.integer(cfg$png_height_px),
    res = as.integer(cfg$png_res_dpi),
    bg = "white"
  )
  on.exit(grDevices::dev.off(), add = TRUE)
  old_par <- graphics::par(no.readonly = TRUE)
  on.exit(graphics::par(old_par), add = TRUE)
  graphics::par(
    mar = c(1.5, 1.5, 4.6, 14),
    lend = "round",
    ljoin = "round"
  )
  graphics::plot(
    NA,
    xlim = limits$xlim,
    ylim = limits$ylim,
    asp = 1,
    axes = FALSE,
    xlab = "",
    ylab = ""
  )
  used_osm <- try_plot_osm_background(
    xlim = limits$xlim,
    ylim = limits$ylim,
    cfg = cfg,
    plot_epsg = plot_epsg
  )
  if (!used_osm) {
    log_basemap(cfg, sprintf("Basemap unavailable -> blank canvas (dot map EPSG:%d)", as.integer(plot_epsg)))
  }
  graphics::title(
    main = sprintf("Dot map: %s\n%s (EPSG:%d)", value_label, route_name, as.integer(plot_epsg))
  )
  for (sub in split(plot_df, plot_df$subroute_id, drop = TRUE)) {
    if (nrow(sub) < 2) {
      next
    }
    graphics::lines(
      sub$x,
      sub$y,
      lwd = 0.9,
      col = grDevices::adjustcolor("black", alpha.f = 0.2)
    )
  }
  graphics::points(
    x = plot_df$x,
    y = plot_df$y,
    pch = 16,
    cex = 0.35,
    col = grDevices::adjustcolor("#444444", alpha.f = 0.35)
  )
  graphics::points(
    x = plot_df$x,
    y = plot_df$y,
    pch = 21,
    cex = 0.95,
    bg = grDevices::adjustcolor(point_cols, alpha.f = 0.85),
    col = grDevices::adjustcolor("black", alpha.f = 0.45),
    lwd = 0.25
  )
  draw_colorbars(list(list(
    label = value_label,
    palette = palette,
    norm = norm,
    unit = lookup_display_unit(label = value_label)
  )), cfg = cfg)
}
compute_global_norms <- function(points_by_route, display_cols) {
  norms <- list()
  for (col in display_cols) {
    vals <- unlist(
      lapply(points_by_route, function(df) {
        value_col <- choose_visual_value_col(df, col, prefer_smoothed = TRUE)
        if (!value_col %in% names(df)) {
          return(numeric(0))
        }
        suppressWarnings(as.numeric(df[[value_col]]))
      }),
      use.names = FALSE
    )
    norms[[col]] <- build_norm_from_values(vals)
  }
  norms
}
plot_combined_heatmap <- function(
  points_by_route,
  out_png,
  display_cols,
  style_maps,
  cfg,
  global_norms,
  source_crs_epsg = cfg$crs_epsg,
  plot_epsg = cfg$crs_epsg
) {
  if (length(points_by_route) == 0) {
    return(invisible(NULL))
  }
  planned_bar_count <- length(display_cols)
  png_width <- as.integer(cfg$combined_png_width_px + planned_bar_count * 320)
  png_height <- as.integer(cfg$combined_png_height_px)
  points_by_route_plot <- lapply(points_by_route, function(df) {
    transform_points_df_xy(
      df,
      from_epsg = source_crs_epsg,
      to_epsg = plot_epsg,
      x_col = "x",
      y_col = "y"
    )
  })
  reference_markers <- get_reference_markers_plot(plot_epsg)
  all_points <- data.table::rbindlist(points_by_route_plot, idcol = "route_name", fill = TRUE)
  points_for_limits <- data.frame(
    x = suppressWarnings(as.numeric(all_points$x)),
    y = suppressWarnings(as.numeric(all_points$y))
  )
  if (nrow(reference_markers) > 0) {
    points_for_limits <- rbind(
      points_for_limits,
      data.frame(x = reference_markers$x, y = reference_markers$y)
    )
  }
  route_xy <- as.matrix(points_for_limits[, c("x", "y"), drop = FALSE])
  min_pad <- if (identical(as.integer(plot_epsg), as.integer(cfg$wgs84_epsg))) 0.0005 else 1.0
  base_limits <- compute_xy_limits(
    route_xy,
    points_for_limits,
    list(),
    min_pad = min_pad,
    pad_frac = min(cfg$bbox_pad_frac, 0.08)
  )
  line_offset_plot_units <- if (length(display_cols) > 1) {
    offset_units <- compute_offset_units_from_screen(
      xlim = base_limits$xlim,
      png_width = png_width,
      bar_count = planned_bar_count,
      line_offset_screen_px = cfg$line_offset_screen_px
    )
    if (is.finite(offset_units) && offset_units > 0) offset_units else cfg$line_offset_m
  } else {
    cfg$line_offset_m
  }
  segment_layers <- list()
  colorbar_defs <- list()
  for (col in display_cols) {
    offset_slot <- if (col %in% names(style_maps$offsets)) style_maps$offsets[[col]] else 0
    offset_m <- offset_slot * line_offset_plot_units
    seg_parts <- lapply(points_by_route, function(df) {
      value_col <- choose_visual_value_col(df, col, prefer_smoothed = TRUE)
      build_colored_segments_from_points(df, value_col, offset_m)
    })
    seg_parts <- seg_parts[vapply(seg_parts, nrow, integer(1)) > 0]
    if (length(seg_parts) == 0) {
      next
    }
    seg_df <- as.data.frame(data.table::rbindlist(seg_parts, fill = TRUE))
    seg_df <- transform_segment_df_xy(
      seg_df,
      from_epsg = source_crs_epsg,
      to_epsg = plot_epsg
    )
    cmap <- if (!is.null(style_maps$cmaps[[col]])) style_maps$cmaps[[col]] else "viridis"
    palette <- get_palette(cmap, n = 256L)
    norm <- global_norms[[col]]
    seg_df$plot_color <- map_values_to_colors(seg_df$value, palette, norm)
    segment_layers[[col]] <- seg_df
    label <- if (!is.null(style_maps$labels[[col]])) style_maps$labels[[col]] else col
    colorbar_defs[[length(colorbar_defs) + 1]] <- list(
      label = label,
      palette = palette,
      norm = norm,
      unit = lookup_display_unit(column_name = col, label = label)
    )
  }
  limits <- compute_xy_limits(
    route_xy,
    points_for_limits,
    segment_layers,
    min_pad = min_pad,
    pad_frac = min(cfg$bbox_pad_frac, 0.08)
  )
  left_crop_units <- compute_plot_x_delta_for_lon_degrees(
    plot_epsg = plot_epsg,
    xlim = limits$xlim,
    ylim = limits$ylim,
    lon_delta_deg = 0.020
  )
  if (is.finite(left_crop_units) && left_crop_units > 0) {
    limits$xlim[1] <- limits$xlim[1] + min(left_crop_units, diff(limits$xlim) * 0.18)
  }
  bar_count <- length(colorbar_defs)
  png_width <- as.integer(cfg$combined_png_width_px + bar_count * 320)
  png_height <- as.integer(cfg$combined_png_height_px)
  grDevices::png(
    filename = out_png,
    width = png_width,
    height = png_height,
    res = as.integer(cfg$png_res_dpi),
    bg = "white"
  )
  on.exit(grDevices::dev.off(), add = TRUE)
  old_par <- graphics::par(no.readonly = TRUE)
  on.exit(graphics::par(old_par), add = TRUE)
  graphics::par(
    mar = c(6.6, 4.6, 3.6, 3.0),
    plt = c(0.06, 0.90, 0.14, 0.985),
    lend = "round",
    ljoin = "round"
  )
  graphics::plot(
    NA,
    xlim = limits$xlim,
    ylim = limits$ylim,
    asp = 1,
    axes = FALSE,
    xlab = "",
    ylab = ""
  )
  used_osm <- try_plot_osm_background(
    xlim = limits$xlim,
    ylim = limits$ylim,
    cfg = cfg,
    plot_epsg = plot_epsg
  )
  if (!used_osm) {
    log_basemap(cfg, sprintf("Basemap unavailable -> blank canvas (combined EPSG:%d)", as.integer(plot_epsg)))
  }
  nav_limits <- get_effective_map_extent(default_xlim = limits$xlim, default_ylim = limits$ylim)
  visible_nav_limits <- get_visible_map_extent(xlim = nav_limits$xlim, ylim = nav_limits$ylim)
  graphics::par(new = TRUE)
  graphics::plot(
    NA,
    xlim = nav_limits$xlim,
    ylim = nav_limits$ylim,
    asp = 1,
    axes = FALSE,
    xlab = "",
    ylab = ""
  )
  draw_map_title(
    title_text = "Air Quality Measurements Along a Commuting Route",
    xlim = nav_limits$xlim,
    ylim = nav_limits$ylim
  )
  draw_geo_axes(plot_epsg = plot_epsg, xlim = nav_limits$xlim, ylim = nav_limits$ylim)
  draw_map_navigation(plot_epsg = plot_epsg, xlim = nav_limits$xlim, ylim = nav_limits$ylim, include_scale_bar = FALSE)
  for (route_name in names(points_by_route_plot)) {
    route_df <- points_by_route_plot[[route_name]]
    for (sub in split(route_df, route_df$subroute_id, drop = TRUE)) {
      if (nrow(sub) < 2) {
        next
      }
      graphics::lines(
        sub$x,
        sub$y,
        lwd = 1.1,
        col = grDevices::adjustcolor("black", alpha.f = 0.18)
      )
    }
  }
  for (col in names(segment_layers)) {
    seg_df <- segment_layers[[col]]
    draw_lwd <- if (length(segment_layers) > 1) min(cfg$line_width, 6.0) else cfg$line_width
    outline_lwd <- draw_lwd + max(0, cfg$line_outline_width)
    if (outline_lwd > draw_lwd) {
      graphics::segments(
        x0 = seg_df$x,
        y0 = seg_df$y,
        x1 = seg_df$xend,
        y1 = seg_df$yend,
        col = grDevices::adjustcolor(cfg$line_outline_color, alpha.f = cfg$line_outline_alpha),
        lwd = outline_lwd
      )
    }
    graphics::segments(
      x0 = seg_df$x,
      y0 = seg_df$y,
      x1 = seg_df$xend,
      y1 = seg_df$yend,
      col = grDevices::adjustcolor(seg_df$plot_color, alpha.f = cfg$line_alpha),
      lwd = draw_lwd
    )
  }
  draw_reference_markers(plot_epsg = plot_epsg)
  draw_route_annotations(xlim = nav_limits$xlim, ylim = nav_limits$ylim)
  cfg_colorbars <- cfg
  cfg_colorbars$legend_inside <- TRUE
  cfg_colorbars$legend_bg_alpha <- max(0.82, cfg$legend_bg_alpha)
  legend_layout <- draw_colorbars(colorbar_defs, cfg = cfg_colorbars)
  draw_reference_station_legend(layout = legend_layout)
  draw_colorbars(colorbar_defs, cfg = cfg_colorbars)
  draw_scale_bar(
    plot_epsg = plot_epsg,
    xlim = nav_limits$xlim,
    ylim = nav_limits$ylim,
    center_x = mean(visible_nav_limits$xlim),
    bottom = visible_nav_limits$ylim[1] + diff(visible_nav_limits$ylim) * 0.030,
    preferred_length_m = 2500
  )
  draw_source_footer(cfg = cfg, xlim = nav_limits$xlim, ylim = nav_limits$ylim)
}
build_segment_records_from_points <- function(points_df, value_col, route_name = NA_character_) {
  if (!value_col %in% names(points_df) || nrow(points_df) < 2) {
    return(data.frame())
  }
  rows <- vector("list", nrow(points_df))
  row_idx <- 0L
  for (sub in split(points_df, points_df$subroute_id, drop = TRUE)) {
    if (nrow(sub) < 2) {
      next
    }
    vals <- suppressWarnings(as.numeric(sub[[value_col]]))
    for (i in seq_len(nrow(sub) - 1)) {
      x0 <- suppressWarnings(as.numeric(sub$x[i]))
      y0 <- suppressWarnings(as.numeric(sub$y[i]))
      x1 <- suppressWarnings(as.numeric(sub$x[i + 1]))
      y1 <- suppressWarnings(as.numeric(sub$y[i + 1]))
      seg_len <- sqrt((x1 - x0)^2 + (y1 - y0)^2)
      if (!is.finite(seg_len) || seg_len <= 0) {
        next
      }
      seg_value <- mean(c(vals[i], vals[i + 1]), na.rm = TRUE)
      if (!is.finite(seg_value)) {
        next
      }
      row_idx <- row_idx + 1L
      rows[[row_idx]] <- list(
        route_name = route_name,
        x0 = x0,
        y0 = y0,
        x1 = x1,
        y1 = y1,
        value = seg_value,
        length_m = seg_len
      )
    }
  }
  if (row_idx == 0L) {
    return(data.frame())
  }
  as.data.frame(data.table::rbindlist(rows[seq_len(row_idx)], fill = TRUE))
}
build_merged_pm25_layer <- function(points_by_route, cfg) {
  snap_m <- as.numeric(cfg$merged_snap_m)
  if (!is.finite(snap_m) || snap_m <= 0) {
    snap_m <- 18.0
  }
  all_parts <- list()
  part_idx <- 0L
  for (route_name in names(points_by_route)) {
    points_df <- points_by_route[[route_name]]
    pm25_base_col <- pick_existing_column(names(points_df), PM25_CANDIDATES)
    if (is.na(pm25_base_col)) {
      next
    }
    value_col <- choose_visual_value_col(points_df, pm25_base_col, prefer_smoothed = TRUE)
    seg_df <- build_segment_records_from_points(points_df, value_col, route_name = route_name)
    if (nrow(seg_df) == 0) {
      next
    }
    seg_df$gx0 <- round(seg_df$x0 / snap_m) * snap_m
    seg_df$gy0 <- round(seg_df$y0 / snap_m) * snap_m
    seg_df$gx1 <- round(seg_df$x1 / snap_m) * snap_m
    seg_df$gy1 <- round(seg_df$y1 / snap_m) * snap_m
    swap_idx <- with(seg_df, gx0 > gx1 | (gx0 == gx1 & gy0 > gy1))
    if (any(swap_idx)) {
      tmp_x <- seg_df$gx0[swap_idx]
      tmp_y <- seg_df$gy0[swap_idx]
      seg_df$gx0[swap_idx] <- seg_df$gx1[swap_idx]
      seg_df$gy0[swap_idx] <- seg_df$gy1[swap_idx]
      seg_df$gx1[swap_idx] <- tmp_x
      seg_df$gy1[swap_idx] <- tmp_y

      tmp_x <- seg_df$x0[swap_idx]
      tmp_y <- seg_df$y0[swap_idx]
      seg_df$x0[swap_idx] <- seg_df$x1[swap_idx]
      seg_df$y0[swap_idx] <- seg_df$y1[swap_idx]
      seg_df$x1[swap_idx] <- tmp_x
      seg_df$y1[swap_idx] <- tmp_y
    }
    seg_df$key <- sprintf(
      "%.2f|%.2f|%.2f|%.2f",
      seg_df$gx0, seg_df$gy0, seg_df$gx1, seg_df$gy1
    )
    part_idx <- part_idx + 1L
    all_parts[[part_idx]] <- seg_df
  }
  if (part_idx == 0L) {
    return(NULL)
  }
  raw_df <- as.data.frame(data.table::rbindlist(all_parts[seq_len(part_idx)], fill = TRUE))
  grouped <- split(raw_df, raw_df$key, drop = TRUE)
  merged_rows <- vector("list", length(grouped))
  row_idx <- 0L
  for (group_name in names(grouped)) {
    sub <- grouped[[group_name]]
    if (nrow(sub) == 0) {
      next
    }
    row_idx <- row_idx + 1L
    merged_rows[[row_idx]] <- list(
      segment_key = group_name,
      n_observations = as.integer(nrow(sub)),
      n_routes = as.integer(length(unique(sub$route_name))),
      pm25_mean = safe_mean(sub$value),
      pm25_median = safe_median(sub$value),
      pm25_min = safe_min(sub$value),
      pm25_max = safe_max(sub$value),
      segment_length_m = safe_mean(sub$length_m),
      x0 = safe_mean(sub$x0),
      y0 = safe_mean(sub$y0),
      x1 = safe_mean(sub$x1),
      y1 = safe_mean(sub$y1)
    )
  }
  if (row_idx == 0L) {
    return(NULL)
  }
  merged_df <- as.data.frame(data.table::rbindlist(merged_rows[seq_len(row_idx)], fill = TRUE))
  merged_df <- merged_df[is.finite(merged_df$pm25_mean), , drop = FALSE]
  if (nrow(merged_df) == 0) {
    return(NULL)
  }
  geoms <- lapply(seq_len(nrow(merged_df)), function(i) {
    sf::st_linestring(matrix(
      c(
        merged_df$x0[i], merged_df$y0[i],
        merged_df$x1[i], merged_df$y1[i]
      ),
      byrow = TRUE,
      ncol = 2
    ))
  })
  sf::st_sf(
    merged_df,
    geometry = sf::st_sfc(geoms, crs = as.integer(cfg$crs_epsg))
  )
}
plot_segment_sf_heatmap <- function(
  segment_sf,
  value_col,
  value_label,
  out_png,
  cfg,
  plot_epsg = cfg$resolved_map_epsg,
  cmap_name = "blue_red",
  value_unit = ""
) {
  if (is.null(segment_sf) || nrow(segment_sf) == 0 || !value_col %in% names(segment_sf)) {
    return(invisible(NULL))
  }
  segment_plot <- if (identical(as.integer(sf::st_crs(segment_sf)$epsg), as.integer(plot_epsg))) {
    segment_sf
  } else {
    sf::st_transform(segment_sf, as.integer(plot_epsg))
  }
  coords <- sf::st_coordinates(segment_plot)
  if (nrow(coords) == 0) {
    return(invisible(NULL))
  }
  line_id_col <- if ("L1" %in% colnames(coords)) "L1" else tail(colnames(coords), 1)
  coord_df <- as.data.frame(coords)
  seg_groups <- split(coord_df, coord_df[[line_id_col]], drop = TRUE)
  x0 <- vapply(seg_groups, function(df) df$X[1], numeric(1))
  y0 <- vapply(seg_groups, function(df) df$Y[1], numeric(1))
  x1 <- vapply(seg_groups, function(df) df$X[nrow(df)], numeric(1))
  y1 <- vapply(seg_groups, function(df) df$Y[nrow(df)], numeric(1))
  plot_df <- data.frame(
    x = x0,
    y = y0,
    xend = x1,
    yend = y1,
    value = suppressWarnings(as.numeric(segment_plot[[value_col]])),
    stringsAsFactors = FALSE
  )
  plot_df <- plot_df[is.finite(plot_df$value), , drop = FALSE]
  if (nrow(plot_df) == 0) {
    return(invisible(NULL))
  }
  min_pad <- if (identical(as.integer(plot_epsg), as.integer(cfg$wgs84_epsg))) 0.0005 else 1.0
  limits <- compute_xy_limits(
    route_xy = as.matrix(cbind(c(plot_df$x, plot_df$xend), c(plot_df$y, plot_df$yend))),
    points_df = data.frame(x = c(plot_df$x, plot_df$xend), y = c(plot_df$y, plot_df$yend)),
    segment_layers = list(),
    min_pad = min_pad,
    pad_frac = cfg$bbox_pad_frac
  )
  palette <- get_palette(cmap_name, n = 256L)
  norm <- build_norm_from_values(plot_df$value)
  plot_df$plot_color <- map_values_to_colors(plot_df$value, palette, norm)
  grDevices::png(
    filename = out_png,
    width = as.integer(cfg$combined_png_width_px),
    height = as.integer(cfg$combined_png_height_px),
    res = as.integer(cfg$png_res_dpi),
    bg = "white"
  )
  on.exit(grDevices::dev.off(), add = TRUE)
  old_par <- graphics::par(no.readonly = TRUE)
  on.exit(graphics::par(old_par), add = TRUE)
  graphics::par(
    mar = c(1.8, 1.2, 4.6, 8.2),
    plt = c(0.06, 0.88, 0.08, 0.92),
    lend = "round",
    ljoin = "round"
  )
  graphics::plot(
    NA,
    xlim = limits$xlim,
    ylim = limits$ylim,
    asp = 1,
    axes = FALSE,
    xlab = "",
    ylab = ""
  )
  used_osm <- try_plot_osm_background(
    xlim = limits$xlim,
    ylim = limits$ylim,
    cfg = cfg,
    plot_epsg = plot_epsg
  )
  if (!used_osm) {
    log_basemap(cfg, sprintf("Basemap unavailable -> merged layer canvas (EPSG:%d)", as.integer(plot_epsg)))
  }
  nav_limits <- get_effective_map_extent(default_xlim = limits$xlim, default_ylim = limits$ylim)
  graphics::title(main = sprintf("Merged PM2.5 path heatmap (EPSG:%d)", as.integer(plot_epsg)))
  draw_map_navigation(plot_epsg = plot_epsg, xlim = nav_limits$xlim, ylim = nav_limits$ylim)
  graphics::segments(
    x0 = plot_df$x,
    y0 = plot_df$y,
    x1 = plot_df$xend,
    y1 = plot_df$yend,
    col = grDevices::adjustcolor(plot_df$plot_color, alpha.f = 0.98),
    lwd = max(6.5, cfg$line_width + 0.6)
  )
  cfg_colorbars <- cfg
  cfg_colorbars$legend_inside <- TRUE
  cfg_colorbars$legend_bg_alpha <- max(0.78, cfg$legend_bg_alpha)
  draw_colorbars(list(list(
    label = value_label,
    palette = palette,
    norm = norm,
    unit = if (nzchar(value_unit)) value_unit else lookup_display_unit(label = value_label)
  )), cfg = cfg_colorbars)
}
delete_existing_shapefile <- function(shp_path) {
  base <- sub("\\.shp$", "", shp_path)
  suppressWarnings(
    invisible(file.remove(paste0(base, c(".shp", ".shx", ".dbf", ".prj", ".cpg"))))
  )
}
safe_route_name <- function(path) {
  stem <- tools::file_path_sans_ext(basename(path))
  gsub("[^A-Za-z0-9_-]+", "_", stem)
}
create_empty_routes_summary <- function(display_cols) {
  out <- data.frame(
    route_name = character(),
    input_crs_epsg_used = integer(),
    n_raw_points = integer(),
    n_valid_points_before_downsample = integer(),
    n_valid_points_used = integer(),
    downsampled = logical(),
    max_points_per_route = integer(),
    n_valid_points_after_downsample = integer(),
    route_length_m = numeric(),
    n_segments = integer(),
    display_cols_used = character(),
    n_display_cols_used = integer(),
    stringsAsFactors = FALSE
  )
  for (col in display_cols) {
    out[[paste0(col, "_overall_mean")]] <- numeric()
    out[[paste0(col, "_overall_median")]] <- numeric()
  }
  out
}
create_empty_fused_segments <- function(display_cols) {
  out <- data.frame(
    route_name = character(),
    segment_index = integer(),
    m_start = numeric(),
    m_end = numeric(),
    m_mid = numeric(),
    n_points = integer(),
    x_mid = numeric(),
    y_mid = numeric(),
    stringsAsFactors = FALSE
  )
  for (col in display_cols) {
    out[[paste0(col, "_mean")]] <- numeric()
    out[[paste0(col, "_median")]] <- numeric()
    out[[paste0(col, "_min")]] <- numeric()
    out[[paste0(col, "_max")]] <- numeric()
  }
  out
}
compute_global_averages_df <- function(fused_df, display_cols) {
  out <- list(
    n_routes_with_segments = if ("route_name" %in% names(fused_df)) {
      length(unique(fused_df$route_name))
    } else {
      0L
    }
  )
  for (col in display_cols) {
    mean_col <- paste0(col, "_mean")
    vals <- if (mean_col %in% names(fused_df)) {
      suppressWarnings(as.numeric(fused_df[[mean_col]]))
    } else {
      numeric(0)
    }
    vals <- vals[is.finite(vals)]
    out[[paste0(col, "_global_mean_of_segment_means")]] <- safe_mean(vals)
    out[[paste0(col, "_global_median_of_segment_means")]] <- safe_median(vals)
  }
  as.data.frame(out, check.names = FALSE)
}
process_single_csv <- function(
  csv_path,
  cfg,
  output_dirs,
  display_cols,
  style_maps
) {
  route_name <- safe_route_name(csv_path)
  df <- read_csv_safely(csv_path)
  if (nrow(df) == 0) {
    stop("CSV has no rows.", call. = FALSE)
  }
  n_raw_points <- nrow(df)
  coord_cols <- detect_lat_lon_columns(names(df))
  lat_col <- coord_cols$lat_col
  lon_col <- coord_cols$lon_col
  time_col <- detect_time_column(names(df))
  available_display_cols <- display_cols[display_cols %in% names(df)]
  for (col in available_display_cols) {
    df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
  }
  df[[lat_col]] <- suppressWarnings(as.numeric(df[[lat_col]]))
  df[[lon_col]] <- suppressWarnings(as.numeric(df[[lon_col]]))
  df <- df[is.finite(df[[lat_col]]) & is.finite(df[[lon_col]]), , drop = FALSE]
  if (nrow(df) < cfg$min_points_per_route) {
    stop("Not enough valid coordinate rows after filtering lat/lon.", call. = FALSE)
  }
  input_crs_epsg_used <- infer_input_crs_epsg(
    lat_vals = df[[lat_col]],
    lon_vals = df[[lon_col]],
    forced_epsg = cfg$input_crs_epsg,
    default_projected_epsg = cfg$crs_epsg
  )
  log_basemap(
    cfg,
    sprintf(
      "Route %s input CRS detected EPSG:%d (lat_col=%s lon_col=%s)",
      route_name,
      as.integer(input_crs_epsg_used),
      lat_col,
      lon_col
    )
  )
  df$..sort_time <- build_sort_time(df, time_col)
  df <- df[order(df$..sort_time, na.last = TRUE), , drop = FALSE]
  n_valid_before_downsample <- nrow(df)
  ds <- downsample_dataframe(df, cfg$max_points_per_route)
  df <- ds$df
  downsampled <- ds$downsampled
  n_valid_after_downsample <- nrow(df)
  for (col in available_display_cols) {
    df[[paste0(col, "_smoothed")]] <- rolling_median_center(df[[col]], cfg$smoothing_window)
  }
  pm25_base_col <- pick_existing_column(names(df), PM25_CANDIDATES)
  if (!is.na(pm25_base_col)) {
    df[[pm25_base_col]] <- suppressWarnings(as.numeric(df[[pm25_base_col]]))
    pm25_smoothed_col <- paste0(pm25_base_col, "_smoothed")
    if (!pm25_smoothed_col %in% names(df)) {
      df[[pm25_smoothed_col]] <- rolling_median_center(df[[pm25_base_col]], cfg$smoothing_window)
    }
  }
  points_sf <- sf::st_as_sf(
    df,
    coords = c(lon_col, lat_col),
    crs = as.integer(input_crs_epsg_used),
    remove = FALSE
  )
  points_sf <- sf::st_transform(points_sf, as.integer(cfg$crs_epsg))
  points_sf <- compute_segmented_route_ids(points_sf, cfg$point_gap_break_m)
  if (nrow(points_sf) < cfg$min_points_per_route) {
    stop("Not enough projected points to build a route.", call. = FALSE)
  }
  points_df <- points_sf_to_df(points_sf)
  line_info <- build_subroute_lines(
    points_df = points_df,
    crs_epsg = cfg$crs_epsg,
    min_points = cfg$min_points_per_route
  )
  if (is.null(line_info)) {
    stop("Could not build valid line(s) from subroutes.", call. = FALSE)
  }
  route_line_sfc <- line_info$longest_line
  route_length_m <- line_info$longest_length_m
  agg_df <- aggregate_along_route(
    points_sf = points_sf,
    route_line_sfc = route_line_sfc,
    segment_length_m = cfg$segment_length_m,
    display_cols = available_display_cols,
    route_name = route_name
  )
  agg_path <- file.path(
    output_dirs$aggregated_segments,
    paste0(route_name, "_aggregated_segments.csv")
  )
  if (nrow(agg_df) == 0) {
    agg_df <- create_empty_fused_segments(available_display_cols)
  }
  data.table::fwrite(agg_df, agg_path, na = "")
  map_path <- file.path(
    output_dirs$maps_per_route,
    paste0(route_name, "_parallel_heatmap.png")
  )
  main_plot_epsg <- as.integer(cfg$resolved_map_epsg)
  if (isTRUE(cfg$create_route_maps)) {
    plot_route_heatmap(
      route_line_sfc = route_line_sfc,
      points_df = points_df,
      route_name = route_name,
      out_png = map_path,
      display_cols = available_display_cols,
      style_maps = style_maps,
      cfg = cfg,
      source_crs_epsg = cfg$crs_epsg,
      plot_epsg = main_plot_epsg
    )
  }
  if (isTRUE(cfg$create_route_maps) &&
      isTRUE(cfg$create_column_maps) &&
      length(available_display_cols) > 0) {
    for (col in available_display_cols) {
      single_style <- list(
        labels = list(),
        cmaps = list(),
        offsets = numeric(0)
      )
      single_style$labels[[col]] <- if (!is.null(style_maps$labels[[col]])) style_maps$labels[[col]] else col
      single_style$cmaps[[col]] <- if (!is.null(style_maps$cmaps[[col]])) style_maps$cmaps[[col]] else "viridis"
      single_style$offsets[col] <- 0
      col_map_path <- file.path(
        output_dirs$maps_per_route_columns,
        paste0(route_name, "_", col, "_line_heatmap.png")
      )
      plot_route_heatmap(
        route_line_sfc = route_line_sfc,
        points_df = points_df,
        route_name = paste0(route_name, " | ", col),
        out_png = col_map_path,
        display_cols = c(col),
        style_maps = single_style,
        cfg = cfg,
        source_crs_epsg = cfg$crs_epsg,
        plot_epsg = main_plot_epsg
      )
    }
  }
  if (isTRUE(cfg$create_route_maps) &&
      isTRUE(cfg$create_wgs84_maps) &&
      !identical(as.integer(main_plot_epsg), as.integer(cfg$wgs84_epsg))) {
    map_path_wgs84 <- file.path(
      output_dirs$maps_per_route,
      paste0(route_name, "_parallel_heatmap_wgs84_epsg", as.integer(cfg$wgs84_epsg), ".png")
    )
    plot_route_heatmap(
      route_line_sfc = route_line_sfc,
      points_df = points_df,
      route_name = route_name,
      out_png = map_path_wgs84,
      display_cols = available_display_cols,
      style_maps = style_maps,
      cfg = cfg,
      source_crs_epsg = cfg$crs_epsg,
      plot_epsg = cfg$wgs84_epsg
    )
  }
  if (isTRUE(cfg$create_route_maps) &&
      isTRUE(cfg$create_dot_maps) &&
      length(available_display_cols) > 0) {
    for (col in available_display_cols) {
      dot_value_col <- choose_visual_value_col(points_df, col, prefer_smoothed = TRUE)
      dot_label <- if (!is.null(style_maps$labels[[col]])) style_maps$labels[[col]] else col
      dot_cmap <- if (!is.null(style_maps$cmaps[[col]])) style_maps$cmaps[[col]] else "viridis"
      dot_path <- file.path(
        output_dirs$maps_per_route_dots,
        paste0(route_name, "_", col, "_dots_epsg", as.integer(main_plot_epsg), ".png")
      )
      plot_route_dot_map(
        points_df = points_df,
        route_name = route_name,
        out_png = dot_path,
        value_col = dot_value_col,
        value_label = dot_label,
        cmap_name = dot_cmap,
        cfg = cfg,
        source_crs_epsg = cfg$crs_epsg,
        plot_epsg = main_plot_epsg
      )
    }
  }
  summary_row <- data.frame(
    route_name = route_name,
    input_crs_epsg_used = as.integer(input_crs_epsg_used),
    n_raw_points = as.integer(n_raw_points),
    n_valid_points_before_downsample = as.integer(n_valid_before_downsample),
    n_valid_points_used = as.integer(nrow(points_df)),
    downsampled = as.logical(downsampled),
    max_points_per_route = as.integer(cfg$max_points_per_route),
    n_valid_points_after_downsample = as.integer(n_valid_after_downsample),
    route_length_m = coalesce_num(route_length_m),
    n_segments = as.integer(nrow(agg_df)),
    display_cols_used = paste(available_display_cols, collapse = "|"),
    n_display_cols_used = as.integer(length(available_display_cols)),
    stringsAsFactors = FALSE
  )
  for (col in display_cols) {
    if (col %in% names(df)) {
      vals <- suppressWarnings(as.numeric(df[[col]]))
      summary_row[[paste0(col, "_overall_mean")]] <- safe_mean(vals)
      summary_row[[paste0(col, "_overall_median")]] <- safe_median(vals)
    } else {
      summary_row[[paste0(col, "_overall_mean")]] <- NA_real_
      summary_row[[paste0(col, "_overall_median")]] <- NA_real_
    }
  }
  list(
    route_name = route_name,
    points_df = points_df,
    aggregated_df = agg_df,
    summary_df = summary_row
  )
}
main <- function() {
  cfg <- parse_cli_args(DEFAULTS)
  cfg$input_dir <- normalize_slashes(cfg$input_dir)
  cfg$output_base_dir <- normalize_slashes(cfg$output_base_dir)
  cfg$output_dir <- normalize_slashes(cfg$output_dir)
  cfg$input_crs_epsg <- as.integer(cfg$input_crs_epsg)
  cfg$max_display_cols <- as.integer(max(1L, cfg$max_display_cols))
  cfg$smoothing_window <- as.integer(max(1L, cfg$smoothing_window))
  cfg$min_points_per_route <- as.integer(max(2L, cfg$min_points_per_route))
  cfg$max_points_per_route <- as.integer(cfg$max_points_per_route)
  cfg$map_epsg <- as.integer(cfg$map_epsg)
  cfg$wgs84_epsg <- as.integer(cfg$wgs84_epsg)
  cfg$line_offset_screen_px <- as.numeric(cfg$line_offset_screen_px)
  cfg$line_width <- as.numeric(cfg$line_width)
  cfg$line_alpha <- as.numeric(cfg$line_alpha)
  cfg$line_outline_width <- as.numeric(cfg$line_outline_width)
  cfg$line_outline_alpha <- as.numeric(cfg$line_outline_alpha)
  cfg$line_outline_color <- as.character(cfg$line_outline_color)
  if (length(cfg$line_outline_color) == 0 || is.na(cfg$line_outline_color) || !nzchar(cfg$line_outline_color)) {
    cfg$line_outline_color <- "#101010"
  }
  cfg$auto_install_osm_deps <- as.logical(cfg$auto_install_osm_deps)
  cfg$debug_basemap <- as.logical(cfg$debug_basemap)
  cfg$legend_label_cex <- as.numeric(cfg$legend_label_cex)
  cfg$legend_value_cex <- as.numeric(cfg$legend_value_cex)
  cfg$legend_bar_width_frac <- as.numeric(cfg$legend_bar_width_frac)
  cfg$legend_bar_gap_frac <- as.numeric(cfg$legend_bar_gap_frac)
  cfg$legend_inside <- as.logical(cfg$legend_inside)
  cfg$legend_bg_alpha <- as.numeric(cfg$legend_bg_alpha)
  cfg$bbox_pad_frac <- as.numeric(cfg$bbox_pad_frac)
  cfg$png_width_px <- as.integer(cfg$png_width_px)
  cfg$png_height_px <- as.integer(cfg$png_height_px)
  cfg$combined_png_width_px <- as.integer(cfg$combined_png_width_px)
  cfg$combined_png_height_px <- as.integer(cfg$combined_png_height_px)
  cfg$png_res_dpi <- as.integer(cfg$png_res_dpi)
  cfg$merged_snap_m <- as.numeric(cfg$merged_snap_m)
  cfg$osm_zoom <- suppressWarnings(as.integer(cfg$osm_zoom))
  cfg$osm_zoom_bias <- suppressWarnings(as.integer(cfg$osm_zoom_bias))
  cfg$basemap_engine <- tolower(as.character(cfg$basemap_engine)[1])
  if (!cfg$basemap_engine %in% c("auto", "maptiles", "rosm")) {
    cfg$basemap_engine <- "auto"
  }
  cfg$osm_cache_dir <- as.character(cfg$osm_cache_dir)
  if (length(cfg$osm_cache_dir) == 0 || is.na(cfg$osm_cache_dir)) {
    cfg$osm_cache_dir <- ""
  }
  cfg$osm_res <- as.numeric(cfg$osm_res)
  cfg$osm_zoomin <- as.integer(cfg$osm_zoomin)
  if (isTRUE(cfg$osm_background)) {
    deps_ok <- ensure_osm_runtime_deps(
      auto_install = cfg$auto_install_osm_deps,
      engine = cfg$basemap_engine
    )
    HAS_ROSM <<- requireNamespace("rosm", quietly = TRUE)
    HAS_MAPTILES <<- requireNamespace("maptiles", quietly = TRUE)
    if (!isTRUE(deps_ok)) {
      cfg$osm_background <- FALSE
    }
  }
  cfg$resolved_map_epsg <- resolve_osm_plot_epsg(cfg)
  log_basemap(
    cfg,
    sprintf(
      "Basemap config engine=%s provider=%s resolved_map_epsg=%d requested_map_epsg=%d osm_background=%s",
      cfg$basemap_engine,
      cfg$osm_provider,
      as.integer(cfg$resolved_map_epsg),
      as.integer(cfg$map_epsg),
      as.character(isTRUE(cfg$osm_background))
    )
  )
  if (isTRUE(cfg$osm_background) && !isTRUE(HAS_MAPTILES) && !isTRUE(HAS_ROSM)) {
    message("No supported basemap backend is available in this R session. Continuing without OSM background.")
  }
  output_root <- resolve_output_dir(cfg$output_dir, cfg$output_base_dir)
  output_dirs <- ensure_output_dirs(output_root)
  csv_files <- sort(list.files(
    cfg$input_dir,
    pattern = "\\.csv$",
    full.names = TRUE,
    ignore.case = TRUE
  ))
  if (length(csv_files) == 0) {
    stop(sprintf("No CSV files found in input_dir: %s", cfg$input_dir), call. = FALSE)
  }
  requested_display_cols <- parse_requested_display_cols(cfg$display_cols)
  display_cols <- resolve_display_columns(
    csv_files = csv_files,
    display_cols_raw = cfg$display_cols,
    max_display_cols = cfg$max_display_cols
  )
  display_cols <- setdiff(display_cols, c("noise_total", "confidence_score"))
  if (length(requested_display_cols) == 0) {
    available_headers <- unique(unlist(lapply(csv_files, read_csv_header), use.names = FALSE))
    available_metric_cols <- unique(c(PREFERRED_DISPLAY_COLUMNS, FALLBACK_DISPLAY_COLUMNS))
    available_metric_cols <- setdiff(
      available_metric_cols[
        available_metric_cols %in% available_headers &
          available_metric_cols != "noise_total"
      ],
      "confidence_score"
    )
    particle_col <- pick_existing_column(available_metric_cols, PM25_CANDIDATES)
    non_particle_cols <- setdiff(available_metric_cols, PM25_CANDIDATES)
    ordered_non_particle <- intersect(c("gas_reducing_ohms", "gas_oxidising_ohms"), non_particle_cols)
    display_cols <- unique(c(if (!is.na(particle_col)) particle_col, ordered_non_particle))
  } else {
    particle_col <- pick_existing_column(display_cols, PM25_CANDIDATES)
    non_particle_cols <- setdiff(display_cols, PM25_CANDIDATES)
    ordered_non_particle <- c(
      intersect(c("gas_reducing_ohms", "gas_oxidising_ohms"), non_particle_cols),
      setdiff(non_particle_cols, c("gas_reducing_ohms", "gas_oxidising_ohms"))
    )
    display_cols <- unique(c(if (!is.na(particle_col)) particle_col, ordered_non_particle))
  }
  display_cols <- setdiff(display_cols, "confidence_score")
  if (length(display_cols) > 4) {
    display_cols <- display_cols[seq_len(4)]
  }
  style_maps <- build_column_style_maps(display_cols)
  selected_cols_df <- data.frame(
    display_column = display_cols,
    label = if (length(display_cols) > 0) {
      vapply(display_cols, function(col) style_maps$labels[[col]], character(1))
    } else {
      character(0)
    },
    cmap = if (length(display_cols) > 0) {
      vapply(display_cols, function(col) style_maps$cmaps[[col]], character(1))
    } else {
      character(0)
    },
    offset_slot = if (length(display_cols) > 0) {
      as.numeric(style_maps$offsets[display_cols])
    } else {
      numeric(0)
    },
    stringsAsFactors = FALSE
  )
  data.table::fwrite(
    selected_cols_df,
    file.path(output_dirs$summaries, "selected_display_columns.csv"),
    na = ""
  )
  points_by_route <- list()
  aggregated_by_route <- list()
  summary_rows <- list()
  processing_errors <- list()
  for (csv_path in csv_files) {
    result <- tryCatch(
      process_single_csv(
        csv_path = csv_path,
        cfg = cfg,
        output_dirs = output_dirs,
        display_cols = display_cols,
        style_maps = style_maps
      ),
      error = function(e) e
    )
    if (inherits(result, "error")) {
      processing_errors[[length(processing_errors) + 1]] <- data.frame(
        file = basename(csv_path),
        error = conditionMessage(result),
        stringsAsFactors = FALSE
      )
      next
    }
    points_by_route[[result$route_name]] <- result$points_df
    aggregated_by_route[[result$route_name]] <- result$aggregated_df
    summary_rows[[length(summary_rows) + 1]] <- result$summary_df
  }
  if (length(summary_rows) > 0) {
    all_routes_summary <- as.data.frame(data.table::rbindlist(summary_rows, fill = TRUE))
  } else {
    all_routes_summary <- create_empty_routes_summary(display_cols)
  }
  data.table::fwrite(
    all_routes_summary,
    file.path(output_dirs$summaries, "all_routes_summary.csv"),
    na = ""
  )
  if (length(aggregated_by_route) > 0) {
    fused_segments <- as.data.frame(data.table::rbindlist(aggregated_by_route, fill = TRUE))
  } else {
    fused_segments <- create_empty_fused_segments(display_cols)
  }
  data.table::fwrite(
    fused_segments,
    file.path(output_dirs$summaries, "all_routes_fused_segment_values.csv"),
    na = ""
  )
  global_averages <- compute_global_averages_df(fused_segments, display_cols)
  data.table::fwrite(
    global_averages,
    file.path(output_dirs$summaries, "all_routes_global_averages.csv"),
    na = ""
  )
  if (length(processing_errors) > 0) {
    errors_df <- as.data.frame(data.table::rbindlist(processing_errors, fill = TRUE))
  } else {
    errors_df <- data.frame(file = character(), error = character(), stringsAsFactors = FALSE)
  }
  data.table::fwrite(
    errors_df,
    file.path(output_dirs$summaries, "processing_errors.csv"),
    na = ""
  )
  if (length(points_by_route) > 0) {
    global_norms <- compute_global_norms(points_by_route, display_cols)
    combined_main_epsg <- as.integer(cfg$resolved_map_epsg)
    plot_combined_heatmap(
      points_by_route = points_by_route,
      out_png = file.path(output_dirs$maps_combined, "combined_parallel_heatmap.png"),
      display_cols = display_cols,
      style_maps = style_maps,
      cfg = cfg,
      global_norms = global_norms,
      source_crs_epsg = cfg$crs_epsg,
      plot_epsg = combined_main_epsg
    )
    if (isTRUE(cfg$create_wgs84_maps) && !identical(as.integer(combined_main_epsg), as.integer(cfg$wgs84_epsg))) {
      plot_combined_heatmap(
        points_by_route = points_by_route,
        out_png = file.path(
          output_dirs$maps_combined,
          paste0("combined_parallel_heatmap_wgs84_epsg", as.integer(cfg$wgs84_epsg), ".png")
        ),
        display_cols = display_cols,
        style_maps = style_maps,
        cfg = cfg,
        global_norms = global_norms,
        source_crs_epsg = cfg$crs_epsg,
        plot_epsg = cfg$wgs84_epsg
      )
    }

    merged_pm25_sf <- tryCatch(
      build_merged_pm25_layer(points_by_route, cfg),
      error = function(e) {
        message(sprintf("Merged PM2.5 layer failed: %s", conditionMessage(e)))
        NULL
      }
    )
    if (!is.null(merged_pm25_sf) && nrow(merged_pm25_sf) > 0) {
      merged_shp <- file.path(output_dirs$merged_layers, "merged_pm25_segments.shp")
      delete_existing_shapefile(merged_shp)
      merged_pm25_shp <- merged_pm25_sf
      names(merged_pm25_shp)[names(merged_pm25_shp) == "segment_key"] <- "seg_key"
      names(merged_pm25_shp)[names(merged_pm25_shp) == "n_observations"] <- "n_obs"
      names(merged_pm25_shp)[names(merged_pm25_shp) == "n_routes"] <- "n_routes"
      names(merged_pm25_shp)[names(merged_pm25_shp) == "pm25_median"] <- "pm25_med"
      names(merged_pm25_shp)[names(merged_pm25_shp) == "segment_length_m"] <- "seg_len_m"
      tryCatch(
        sf::st_write(merged_pm25_shp, merged_shp, quiet = TRUE, append = FALSE),
        error = function(e) {
          message(sprintf("Writing merged PM2.5 shapefile failed: %s", conditionMessage(e)))
        }
      )
      data.table::fwrite(
        sf::st_drop_geometry(merged_pm25_sf),
        file.path(output_dirs$summaries, "merged_pm25_segments.csv"),
        na = ""
      )
      plot_segment_sf_heatmap(
        segment_sf = merged_pm25_sf,
        value_col = "pm25_mean",
        value_label = "Merged PM2.5 average",
        out_png = file.path(output_dirs$maps_combined, "combined_merged_pm25_heatmap.png"),
        cfg = cfg,
        plot_epsg = combined_main_epsg,
        cmap_name = "plasma_yellow_low",
        value_unit = "count"
      )
    }
  }
  message(
    sprintf(
      "Done. Processed: %d CSV(s). Errors: %d. Output root: %s",
      length(summary_rows),
      length(processing_errors),
      output_dirs$root
    )
  )
}
if (sys.nframe() == 0) {
  main()
}


