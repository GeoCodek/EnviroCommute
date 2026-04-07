#!/usr/bin/env Rscript

# Station vs mobile line graphs.
# - PM2.5: mobile pass means vs station hourly PM2.5.
# - Oxidising gases: station oxidising gases (NO2/O3/SO2 if present) vs
#   mobile oxidising resistance (ohms) on a dual y-axis.

suppressPackageStartupMessages({
  required_packages <- c("dplyr", "tidyr", "readr", "stringr", "lubridate", "ggplot2", "scales")
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

  invisible(lapply(required_packages, library, character.only = TRUE))
})

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
config <- list(
  input_csv = "C:/Users/janek/OneDrive/Desktop/FU/enviro_pi_csv-main/Vergleichs Messtation/mobile station passesver2.csv",
  output_dir = "C:/Users/janek/OneDrive/Desktop/FU/enviro_pi_csv-main/Line Graph Station Comp",
  clean_output_dir = TRUE,
  timezone = "Europe/Berlin",
  silberstein_zoom_start = "2026-01-25",
  silberstein_zoom_end = "2026-08-02",
  silberstein_low_ohm_quantile = 0.15,
  width = 12,
  height = 6,
  dpi = 320
)

dir.create(config$output_dir, recursive = TRUE, showWarnings = FALSE)

if (isTRUE(config$clean_output_dir)) {
  stale_outputs <- list.files(
    config$output_dir,
    pattern = "\\.(jpg|jpeg|svg|csv)$",
    full.names = TRUE,
    ignore.case = TRUE
  )
  if (length(stale_outputs) > 0) {
    invisible(file.remove(stale_outputs))
  }
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
parse_numeric_robust <- function(x) {
  if (is.numeric(x)) {
    return(as.numeric(x))
  }

  x_chr <- as.character(x)
  x_chr <- stringr::str_replace_all(x_chr, "\u00A0", " ")
  x_chr <- stringr::str_trim(x_chr)
  x_chr[x_chr %in% c("", "NA", "NaN", "NULL", "-", "--")] <- NA_character_

  parsed_dot <- suppressWarnings(
    readr::parse_number(
      x_chr,
      locale = readr::locale(decimal_mark = ".", grouping_mark = ",")
    )
  )

  parsed_comma <- suppressWarnings(
    readr::parse_number(
      x_chr,
      locale = readr::locale(decimal_mark = ",", grouping_mark = ".")
    )
  )

  non_missing <- !is.na(x_chr)
  valid_dot <- sum(!is.na(parsed_dot) & non_missing)
  valid_comma <- sum(!is.na(parsed_comma) & non_missing)

  if (valid_comma > valid_dot) as.numeric(parsed_comma) else as.numeric(parsed_dot)
}

parse_timestamp_flexible <- function(x, tz = "Europe/Berlin") {
  if (inherits(x, "POSIXt")) {
    return(lubridate::with_tz(as.POSIXct(x), tzone = tz))
  }

  x_chr <- as.character(x)
  x_chr <- stringr::str_trim(x_chr)
  x_chr[x_chr %in% c("", "NA", "NaN", "NULL")] <- NA_character_

  parsed <- as.POSIXct(rep(NA_real_, length(x_chr)), origin = "1970-01-01", tz = tz)
  formats <- c(
    "%Y-%m-%dT%H:%M:%OS%z", "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%OS%z", "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%OS", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"
  )

  for (fmt in formats) {
    idx <- is.na(parsed) & !is.na(x_chr)
    if (!any(idx)) {
      break
    }

    suppressWarnings({
      candidate <- as.POSIXct(x_chr[idx], format = fmt, tz = tz)
    })
    parsed[idx] <- candidate
  }

  parsed
}

safe_filename <- function(x) {
  x |>
    as.character() |>
    iconv(from = "", to = "ASCII//TRANSLIT", sub = "") |>
    tolower() |>
    stringr::str_replace_all("[^a-z0-9]+", "_") |>
    stringr::str_replace_all("^_+|_+$", "")
}

normalize_key <- function(x) {
  x |>
    as.character() |>
    iconv(from = "", to = "ASCII//TRANSLIT", sub = "") |>
    tolower() |>
    stringr::str_replace_all("[^a-z0-9]+", "")
}

is_silberstein_station <- function(station_id) {
  normalize_key(station_id) == "silbersteinstrasse"
}

compute_lower_ohm_focus_range <- function(values, focus_quantile = 0.15, min_points = 8L) {
  vals <- values[is.finite(values)]
  if (length(vals) < 2) {
    return(NULL)
  }

  probs <- min(max(as.numeric(focus_quantile), 0.02), 0.5)
  upper <- as.numeric(stats::quantile(vals, probs = probs, na.rm = TRUE, names = FALSE))

  low_count <- sum(vals <= upper, na.rm = TRUE)
  if (low_count < min_points) {
    fallback_prob <- min(0.35, max(probs, min_points / length(vals)))
    upper <- as.numeric(stats::quantile(vals, probs = fallback_prob, na.rm = TRUE, names = FALSE))
  }

  lower <- min(vals, na.rm = TRUE)
  if (!is.finite(lower) || !is.finite(upper) || upper <= lower) {
    return(NULL)
  }

  span <- upper - lower
  pad <- max(5, span * 0.02)
  c(max(0, lower - pad), upper + pad)
}

compute_y_limits <- function(values) {
  vals <- values[is.finite(values)]
  if (length(vals) == 0) {
    return(c(0, 1))
  }

  ymin <- min(vals)
  ymax <- max(vals)

  if (isTRUE(all.equal(ymin, ymax))) {
    pad <- max(1, abs(ymin) * 0.1)
    return(c(ymin - pad, ymax + pad))
  }

  pad <- 0.05 * (ymax - ymin)
  c(ymin - pad, ymax + pad)
}

safe_station_hour_mean <- function(x) {
  vals <- x[is.finite(x)]
  if (length(vals) == 0) NA_real_ else mean(vals)
}

first_valid_timestamp <- function(mobile_ts, station_ts) {
  mobile_vals <- mobile_ts[!is.na(mobile_ts)]
  if (length(mobile_vals) > 0) {
    return(min(mobile_vals))
  }

  station_vals <- station_ts[!is.na(station_ts)]
  if (length(station_vals) > 0) {
    return(min(station_vals))
  }

  as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC")
}

pick_best_mobile_column <- function(df, candidates) {
  available <- candidates[candidates %in% names(df)]
  if (length(available) == 0) {
    return(NA_character_)
  }

  non_missing_counts <- vapply(
    available,
    function(col) sum(!is.na(parse_numeric_robust(df[[col]]))),
    numeric(1)
  )

  if (all(non_missing_counts == 0)) {
    return(NA_character_)
  }

  available[which.max(non_missing_counts)]
}

station_columns_for_id <- function(station_id, column_names) {
  prefix <- paste0(tolower(station_id), "__")
  column_names[startsWith(tolower(column_names), prefix)]
}

station_pm25_column_for_id <- function(station_id, column_names) {
  station_cols <- station_columns_for_id(station_id, column_names)
  if (length(station_cols) == 0) {
    return(NA_character_)
  }

  pm_regex <- regex("feinstaub.*pm\\s*2[._, ]?5|\\bpm\\s*2[._, ]?5\\b|pm2_5|pm25", ignore_case = TRUE)
  pm_cols <- station_cols[stringr::str_detect(station_cols, pm_regex)]

  if (length(pm_cols) > 0) pm_cols[[1]] else NA_character_
}

station_no_column_for_id <- function(station_id, column_names) {
  station_cols <- station_columns_for_id(station_id, column_names)
  if (length(station_cols) == 0) {
    return(NA_character_)
  }

  no_regex <- regex("stickstoffmonoxid|\\bno\\b|nitric_oxide", ignore_case = TRUE)
  nox_regex <- regex("stickoxide|\\bnox\\b|nitrogen_oxides", ignore_case = TRUE)
  no_cols <- station_cols[
    stringr::str_detect(station_cols, no_regex) &
      !stringr::str_detect(station_cols, nox_regex)
  ]

  if (length(no_cols) > 0) no_cols[[1]] else NA_character_
}

station_no2_column_for_id <- function(station_id, column_names) {
  station_cols <- station_columns_for_id(station_id, column_names)
  if (length(station_cols) == 0) {
    return(NA_character_)
  }

  no2_regex <- regex("stickstoffdioxid|\\bno2\\b|nitrogen_dioxide", ignore_case = TRUE)
  no2_cols <- station_cols[stringr::str_detect(station_cols, no2_regex)]

  if (length(no2_cols) > 0) no2_cols[[1]] else NA_character_
}

station_nox_column_for_id <- function(station_id, column_names) {
  station_cols <- station_columns_for_id(station_id, column_names)
  if (length(station_cols) == 0) {
    return(NA_character_)
  }

  nox_regex <- regex("stickoxide|\\bnox\\b|nitrogen_oxides", ignore_case = TRUE)
  nox_cols <- station_cols[stringr::str_detect(station_cols, nox_regex)]

  if (length(nox_cols) > 0) nox_cols[[1]] else NA_character_
}

station_oxidising_columns_for_id <- function(station_id, column_names) {
  station_cols <- station_columns_for_id(station_id, column_names)
  if (length(station_cols) == 0) {
    return(character(0))
  }

  # Include requested station gas lines: NO2, NO, NOx (plus O3/SO2 if present).
  gas_regex <- regex(
    paste(
      c(
        "stickstoffdioxid", "\\bno2\\b", "nitrogen_dioxide",
        "stickstoffmonoxid", "\\bno\\b", "nitric_oxide",
        "stickoxide", "\\bnox\\b", "nitrogen_oxides",
        "ozon", "\\bo3\\b",
        "schwefeldioxid", "\\bso2\\b", "sulfur_dioxide", "sulphur_dioxide"
      ),
      collapse = "|"
    ),
    ignore_case = TRUE
  )
  excluded_regex <- regex(
    "kohlenmonoxid|\\bco\\b|ammoniak|\\bnh3\\b|methan|\\bch4\\b|reduzier|reducing",
    ignore_case = TRUE
  )

  keep <- station_cols[stringr::str_detect(station_cols, gas_regex)]
  keep <- keep[!stringr::str_detect(keep, excluded_regex)]

  unique(keep)
}

station_gas_label_from_column <- function(col_name) {
  lower_col <- tolower(col_name)

  if (stringr::str_detect(lower_col, regex("stickoxide|\\bnox\\b|nitrogen_oxides", ignore_case = TRUE))) {
    return("Station NOx")
  }
  if (stringr::str_detect(lower_col, regex("stickstoffmonoxid|\\bno\\b|nitric_oxide", ignore_case = TRUE))) {
    return("Station NO")
  }
  if (stringr::str_detect(lower_col, regex("stickstoffdioxid|\\bno2\\b", ignore_case = TRUE))) {
    return("Station NO2")
  }
  if (stringr::str_detect(lower_col, regex("ozon|\\bo3\\b", ignore_case = TRUE))) {
    return("Station O3")
  }
  if (stringr::str_detect(lower_col, regex("schwefeldioxid|\\bso2\\b", ignore_case = TRUE))) {
    return("Station SO2")
  }

  station_part <- stringr::str_replace(col_name, "^[^_]+__", "")
  paste("Station", station_part)
}

detect_drop_timestamp <- function(pass_summary, threshold_ohms = 18000, min_drop_ohms = 500, threshold_tolerance = 100) {
  df <- pass_summary |>
    filter(is.finite(mobile_oxidising_mean), !is.na(pass_timestamp)) |>
    arrange(pass_timestamp)

  if (nrow(df) < 2) {
    return(as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC"))
  }

  diffs <- c(NA_real_, diff(df$mobile_oxidising_mean))
  strongest_drop_idx <- which.min(diffs)
  if (length(strongest_drop_idx) == 1 && is.finite(diffs[[strongest_drop_idx]]) && diffs[[strongest_drop_idx]] <= -abs(min_drop_ohms)) {
    return(df$pass_timestamp[[strongest_drop_idx]])
  }

  idx_threshold <- which(df$mobile_oxidising_mean <= (threshold_ohms + abs(threshold_tolerance)))
  if (length(idx_threshold) == 0) {
    return(as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC"))
  }

  first_idx <- idx_threshold[[1]]
  if (first_idx > 1) {
    prev_value <- df$mobile_oxidising_mean[[first_idx - 1]]
    current_value <- df$mobile_oxidising_mean[[first_idx]]
    if (is.finite(prev_value) && is.finite(current_value) && (prev_value - current_value) >= min_drop_ohms) {
      return(df$pass_timestamp[[first_idx]])
    }
  }

  drop_candidates <- which(df$mobile_oxidising_mean <= threshold_ohms & diffs <= -abs(min_drop_ohms))
  if (length(drop_candidates) > 0) {
    return(df$pass_timestamp[[drop_candidates[[1]]]])
  }

  df$pass_timestamp[[first_idx]]
}

save_oxidising_dual_axis_plot <- function(
  pass_summary,
  station_long,
  station_label_value,
  station_id,
  mobile_oxidising_col,
  out_file,
  title_suffix = "",
  subtitle_suffix = ""
) {
  if (nrow(pass_summary) == 0 || nrow(station_long) == 0) {
    return(NULL)
  }

  station_long_subset <- station_long |>
    semi_join(pass_summary |> select(pass_timestamp), by = "pass_timestamp")
  if (nrow(station_long_subset) == 0) {
    return(NULL)
  }

  axis_map <- build_linear_transform(
    source_values = pass_summary$mobile_oxidising_mean,
    target_values = station_long_subset$station_value
  )
  if (is.null(axis_map)) {
    return(NULL)
  }

  pass_summary_plot <- pass_summary |>
    mutate(mobile_oxidising_scaled = axis_map$to_target(mobile_oxidising_mean))

  station_plot_df <- station_long_subset |>
    transmute(
      pass_timestamp,
      series,
      value = station_value
    )

  mobile_series_name <- "Mobile oxidising resistance (scaled to station range)"
  mobile_plot_df <- pass_summary_plot |>
    transmute(
      pass_timestamp,
      series = mobile_series_name,
      value = mobile_oxidising_scaled
    )

  plot_df <- dplyr::bind_rows(station_plot_df, mobile_plot_df) |>
    filter(is.finite(value))
  if (nrow(plot_df) == 0) {
    return(NULL)
  }

  station_series <- unique(station_plot_df$series)
  base_station_palette <- c("#C92A2A", "#E67700", "#5F3DC4", "#2B8A3E")
  station_colors <- if (length(station_series) <= length(base_station_palette)) {
    setNames(base_station_palette[seq_along(station_series)], station_series)
  } else {
    setNames(scales::hue_pal()(length(station_series)), station_series)
  }
  color_map <- c(station_colors, setNames("#0B7285", mobile_series_name))

  y_limits <- compute_y_limits(c(station_plot_df$value, mobile_plot_df$value))
  station_series_text <- paste(unique(station_plot_df$series), collapse = ", ")

  has_o3 <- any(stringr::str_detect(tolower(station_long_subset$station_column), regex("ozon|\\bo3\\b", ignore_case = TRUE)))
  has_no2 <- any(stringr::str_detect(tolower(station_long_subset$station_column), regex("stickstoffdioxid|\\bno2\\b", ignore_case = TRUE)))
  notes <- c()
  if (!has_o3) {
    notes <- c(notes, "No ozone station column found in this file.")
  }
  if (!has_no2) {
    notes <- c(notes, "No NO2 station column found in this file.")
  }
  note_text <- if (length(notes) > 0) paste(notes, collapse = " ") else ""

  line_df <- plot_df |>
    dplyr::group_by(series) |>
    dplyr::filter(dplyr::n() > 1) |>
    dplyr::ungroup()

  title_text <- sprintf("%s: Station Gases vs Mobile Oxidising Sensor%s", station_label_value, title_suffix)
  subtitle_text <- sprintf(
    "Station series: %s | Mobile line scaled to station range (right axis = ohms)%s",
    station_series_text,
    subtitle_suffix
  )

  p <- ggplot2::ggplot(
    plot_df,
    ggplot2::aes(x = pass_timestamp, y = value, color = series, group = series)
  )
  if (nrow(line_df) > 0) {
    p <- p + ggplot2::geom_line(data = line_df, linewidth = 0.95, alpha = 0.9)
  }
  p <- p +
    ggplot2::geom_point(size = 2) +
    ggplot2::scale_color_manual(values = color_map) +
    ggplot2::scale_x_datetime(
      breaks = scales::breaks_pretty(n = 10),
      labels = scales::label_date(format = "%Y-%m-%d\n%H:%M")
    ) +
    ggplot2::scale_y_continuous(
      name = "Station gases (station units)",
      limits = y_limits,
      expand = ggplot2::expansion(mult = c(0, 0.02)),
      sec.axis = ggplot2::sec_axis(
        transform = ~ axis_map$to_source(.),
        name = sprintf("Mobile oxidising resistance (ohms) [%s]", mobile_oxidising_col),
        labels = scales::label_number(big.mark = ",")
      )
    ) +
    ggplot2::labs(
      title = title_text,
      subtitle = subtitle_text,
      x = "Measurement date/time (local)",
      color = NULL
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      legend.position = "top",
      panel.grid.minor = ggplot2::element_blank()
    )

  ggplot2::ggsave(
    filename = out_file,
    plot = p,
    width = config$width,
    height = config$height,
    dpi = config$dpi,
    units = "in",
    device = "jpeg"
  )

  list(
    station_columns = paste(unique(station_long_subset$station_column), collapse = ";"),
    station_series_text = station_series_text,
    compared_passes = nrow(pass_summary),
    note_text = note_text
  )
}

build_linear_transform <- function(source_values, target_values) {
  src <- source_values[is.finite(source_values)]
  tgt <- target_values[is.finite(target_values)]

  if (length(src) == 0 || length(tgt) == 0) {
    return(NULL)
  }

  source_min <- min(src)
  source_max <- max(src)
  target_min <- min(tgt)
  target_max <- max(tgt)

  if (isTRUE(all.equal(source_min, source_max))) {
    source_pad <- max(1, abs(source_min) * 0.05)
    source_min <- source_min - source_pad
    source_max <- source_max + source_pad
  }

  if (isTRUE(all.equal(target_min, target_max))) {
    target_pad <- max(1, abs(target_min) * 0.05)
    target_min <- target_min - target_pad
    target_max <- target_max + target_pad
  }

  to_target <- function(x) {
    ((x - source_min) / (source_max - source_min)) * (target_max - target_min) + target_min
  }

  to_source <- function(y) {
    ((y - target_min) / (target_max - target_min)) * (source_max - source_min) + source_min
  }

  list(
    source_min = source_min,
    source_max = source_max,
    target_min = target_min,
    target_max = target_max,
    to_target = to_target,
    to_source = to_source
  )
}

save_target_gas_dual_axis_plot <- function(
  pass_summary,
  station_series_name,
  mobile_series_name,
  left_axis_name,
  right_axis_name,
  title_text,
  subtitle_text,
  out_file,
  mobile_zoom_range = NULL,
  max_line_gap_hours = 12
) {
  if (nrow(pass_summary) == 0) {
    return(NULL)
  }

  pass_summary <- pass_summary |>
    filter(!is.na(pass_timestamp), is.finite(station_value), is.finite(mobile_value)) |>
    arrange(pass_timestamp)

  if (!is.null(mobile_zoom_range) && length(mobile_zoom_range) == 2 && all(is.finite(mobile_zoom_range))) {
    zoom_range <- sort(as.numeric(mobile_zoom_range))
    if (zoom_range[[2]] > zoom_range[[1]]) {
      pass_summary <- pass_summary |>
        filter(mobile_value >= zoom_range[[1]], mobile_value <= zoom_range[[2]]) |>
        arrange(pass_timestamp)
    }
  }

  if (nrow(pass_summary) == 0) {
    return(NULL)
  }

  axis_map <- build_linear_transform(
    source_values = pass_summary$mobile_value,
    target_values = pass_summary$station_value
  )
  if (is.null(axis_map)) {
    return(NULL)
  }

  pass_summary <- pass_summary |>
    mutate(mobile_scaled = axis_map$to_target(mobile_value))

  station_plot_df <- pass_summary |>
    transmute(
      pass_timestamp,
      series = station_series_name,
      value = station_value
    )

  mobile_plot_df <- pass_summary |>
    transmute(
      pass_timestamp,
      series = mobile_series_name,
      value = mobile_scaled
    )

  plot_df <- bind_rows(station_plot_df, mobile_plot_df)
  line_df <- plot_df |>
    arrange(series, pass_timestamp) |>
    group_by(series) |>
    mutate(
      gap_hours = as.numeric(difftime(pass_timestamp, dplyr::lag(pass_timestamp), units = "hours")),
      line_group = cumsum(dplyr::coalesce(gap_hours > max_line_gap_hours, TRUE))
    ) |>
    group_by(series, line_group) |>
    filter(n() > 1) |>
    ungroup()

  y_limits <- compute_y_limits(c(station_plot_df$value, mobile_plot_df$value))

  color_map <- c(
    setNames("#C92A2A", station_series_name),
    setNames("#0B7285", mobile_series_name)
  )

  p <- ggplot2::ggplot(
    plot_df,
    ggplot2::aes(x = pass_timestamp, y = value, color = series, group = series)
  )
  if (nrow(line_df) > 0) {
    p <- p + ggplot2::geom_line(
      data = line_df,
      ggplot2::aes(group = interaction(series, line_group)),
      linewidth = 0.95,
      alpha = 0.9
    )
  }
  p <- p +
    ggplot2::geom_point(size = 2) +
    ggplot2::scale_color_manual(values = color_map) +
    ggplot2::scale_x_datetime(
      breaks = scales::breaks_pretty(n = 10),
      labels = scales::label_date(format = "%Y-%m-%d\n%H:%M")
    ) +
    ggplot2::scale_y_continuous(
      name = left_axis_name,
      limits = y_limits,
      expand = ggplot2::expansion(mult = c(0, 0.02)),
      sec.axis = ggplot2::sec_axis(
        transform = ~ axis_map$to_source(.),
        name = right_axis_name,
        labels = scales::label_number(big.mark = ",")
      )
    ) +
    ggplot2::labs(
      title = title_text,
      subtitle = subtitle_text,
      x = "Measurement date/time (local)",
      color = NULL
    ) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      legend.position = "top",
      panel.grid.minor = ggplot2::element_blank()
    )

  ggplot2::ggsave(
    filename = out_file,
    plot = p,
    width = config$width,
    height = config$height,
    dpi = config$dpi,
    units = "in",
    device = "jpeg"
  )

  list(compared_passes = nrow(pass_summary))
}

identify_edge_time_outliers <- function(timestamps, iqr_mult = 1.5, min_points = 12L) {
  ts_num <- suppressWarnings(as.numeric(timestamps))
  outlier_mask <- rep(FALSE, length(ts_num))
  valid_idx <- which(is.finite(ts_num))

  if (length(valid_idx) < min_points) {
    return(list(mask = outlier_mask, n_left = 0L, n_right = 0L))
  }

  vals <- ts_num[valid_idx]
  q1 <- as.numeric(stats::quantile(vals, probs = 0.25, na.rm = TRUE, names = FALSE))
  q3 <- as.numeric(stats::quantile(vals, probs = 0.75, na.rm = TRUE, names = FALSE))
  iqr <- q3 - q1
  if (!is.finite(iqr) || iqr <= 0) {
    return(list(mask = outlier_mask, n_left = 0L, n_right = 0L))
  }

  lower <- q1 - iqr_mult * iqr
  upper <- q3 + iqr_mult * iqr

  ord <- valid_idx[order(ts_num[valid_idx])]
  left_n <- 0L
  while (left_n < length(ord) && ts_num[[ord[[left_n + 1L]]]] < lower) {
    left_n <- left_n + 1L
  }

  right_n <- 0L
  while ((right_n + left_n) < length(ord) && ts_num[[ord[[length(ord) - right_n]]]] > upper) {
    right_n <- right_n + 1L
  }

  if (left_n == 0L && right_n == 0L && length(ord) >= 40L) {
    lower_q <- as.numeric(stats::quantile(vals, probs = 0.01, na.rm = TRUE, names = FALSE))
    upper_q <- as.numeric(stats::quantile(vals, probs = 0.99, na.rm = TRUE, names = FALSE))

    while (left_n < length(ord) && ts_num[[ord[[left_n + 1L]]]] < lower_q) {
      left_n <- left_n + 1L
    }
    while ((right_n + left_n) < length(ord) && ts_num[[ord[[length(ord) - right_n]]]] > upper_q) {
      right_n <- right_n + 1L
    }
  }

  if (left_n > 0) {
    outlier_mask[ord[seq_len(left_n)]] <- TRUE
  }
  if (right_n > 0) {
    outlier_mask[ord[seq.int(length(ord) - right_n + 1L, length(ord))]] <- TRUE
  }

  list(mask = outlier_mask, n_left = left_n, n_right = right_n)
}

build_compact_date_axis <- function(timestamps, tz = "Europe/Berlin") {
  ts <- as.POSIXct(timestamps, tz = tz)
  pass_dates <- as.Date(ts, tz = tz)
  valid_dates <- sort(unique(pass_dates[!is.na(pass_dates)]))

  if (length(valid_dates) == 0) {
    return(list(
      x = rep(NA_real_, length(ts)),
      breaks = numeric(0),
      labels = character(0),
      dates = as.Date(character(0))
    ))
  }

  date_index <- seq_along(valid_dates)
  lookup <- stats::setNames(date_index, as.character(valid_dates))
  day_pos <- as.numeric(lookup[as.character(pass_dates)])

  day_start <- as.POSIXct(pass_dates, tz = tz)
  sec_of_day <- as.numeric(difftime(ts, day_start, units = "secs"))
  sec_of_day[!is.finite(sec_of_day)] <- 0
  frac <- pmin(pmax(sec_of_day / 86400, 0), 1)

  # Keep each day's points inside a compact slot centered on the date tick.
  x_compact <- day_pos - 0.45 + (frac * 0.9)

  list(
    x = x_compact,
    breaks = date_index,
    labels = format(valid_dates, "%d.%m"),
    dates = valid_dates
  )
}

aggregate_daily_pass_summary <- function(pass_summary, tz = "Europe/Berlin") {
  if (nrow(pass_summary) == 0) {
    return(data.frame())
  }

  daily <- pass_summary |>
    filter(!is.na(pass_timestamp), is.finite(station_value), is.finite(mobile_value)) |>
    mutate(pass_date = as.Date(pass_timestamp, tz = tz)) |>
    filter(!is.na(pass_date)) |>
    group_by(pass_date) |>
    summarise(
      pass_timestamp = as.POSIXct(paste0(as.character(first(pass_date)), " 12:00:00"), tz = tz),
      station_daily_mean = mean(station_value, na.rm = TRUE),
      mobile_daily_mean = mean(mobile_value, na.rm = TRUE),
      n_passes = dplyr::n(),
      .groups = "drop"
    ) |>
    arrange(pass_date)

  as.data.frame(daily)
}

save_daily_dual_axis_line_svg <- function(
  daily_summary,
  station_series_name,
  station_series_source,
  left_axis_name,
  title_text,
  out_file,
  mobile_series_name = "Sensor Resistance",
  plot_width = 18,
  plot_height = 10,
  title_size = 24,
  title_bottom_margin = 6
) {
  if (nrow(daily_summary) == 0) {
    return(NULL)
  }

  daily_summary <- daily_summary |>
    filter(!is.na(pass_timestamp), is.finite(station_daily_mean), is.finite(mobile_daily_mean)) |>
    arrange(pass_date)
  if (nrow(daily_summary) == 0) {
    return(NULL)
  }

  axis_map <- build_linear_transform(
    source_values = daily_summary$mobile_daily_mean,
    target_values = daily_summary$station_daily_mean
  )
  if (is.null(axis_map)) {
    return(NULL)
  }

  daily_summary <- daily_summary |>
    mutate(
      mobile_scaled = axis_map$to_target(mobile_daily_mean),
      diff_abs = abs(station_daily_mean - mobile_scaled)
    )

  compact_axis <- build_compact_date_axis(daily_summary$pass_timestamp, tz = config$timezone)
  daily_summary$x_compact <- compact_axis$x

  station_plot_df <- daily_summary |>
    transmute(
      x_compact,
      series = station_series_name,
      value = station_daily_mean
    )
  mobile_plot_df <- daily_summary |>
    transmute(
      x_compact,
      series = mobile_series_name,
      value = mobile_scaled
    )
  plot_df <- dplyr::bind_rows(station_plot_df, mobile_plot_df)

  color_map <- c(
    setNames("#C92A2A", station_series_name),
    setNames("#0B7285", mobile_series_name)
  )
  y_limits <- compute_y_limits(c(station_plot_df$value, mobile_plot_df$value))

  p <- ggplot2::ggplot(
    plot_df,
    ggplot2::aes(x = x_compact, y = value, color = series, group = series)
  ) +
    ggplot2::geom_line(linewidth = 0.95, alpha = 0.9) +
    ggplot2::geom_point(size = 3.4) +
    ggplot2::scale_color_manual(values = color_map) +
    ggplot2::scale_x_continuous(
      breaks = compact_axis$breaks,
      labels = compact_axis$labels,
      expand = ggplot2::expansion(mult = c(0.01, 0.02))
    ) +
    ggplot2::scale_y_continuous(
      name = left_axis_name,
      limits = y_limits,
      expand = ggplot2::expansion(mult = c(0, 0.02)),
      sec.axis = ggplot2::sec_axis(
        transform = ~ axis_map$to_source(.) / 100000,
        name = "Sensor Resistance in 100 k\u03A9",
        labels = scales::label_number(accuracy = 0.01, big.mark = ",")
      )
    ) +
    ggplot2::labs(
      title = title_text,
      x = "Date (2026)",
      color = NULL
    ) +
    ggplot2::theme_minimal(base_size = 18) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(
        size = title_size,
        face = "bold",
        margin = ggplot2::margin(b = title_bottom_margin)
      ),
      axis.title = ggplot2::element_text(size = 24),
      axis.text = ggplot2::element_text(size = 20),
      legend.text = ggplot2::element_text(size = 20),
      legend.position = "top",
      legend.box.spacing = grid::unit(0.05, "lines"),
      legend.margin = ggplot2::margin(t = -5, r = 0, b = -8, l = 0),
      legend.box.margin = ggplot2::margin(t = -6, r = 0, b = -8, l = 0),
      panel.grid.minor = ggplot2::element_blank(),
      plot.margin = ggplot2::margin(t = 12, r = 40, b = 20, l = 20)
    )

  if (file.exists(out_file)) {
    suppressWarnings(file.remove(out_file))
  }

  ggplot2::ggsave(
    filename = out_file,
    plot = p,
    width = plot_width,
    height = plot_height,
    dpi = config$dpi,
    units = "in",
    device = grDevices::svg
  )

  if (!file.exists(out_file)) {
    stop(sprintf("Failed to write daily SVG output: %s", out_file), call. = FALSE)
  }

  out_csv <- file.path(
    dirname(out_file),
    sprintf("%s_data.csv", tools::file_path_sans_ext(basename(out_file)))
  )
  if (file.exists(out_csv)) {
    suppressWarnings(file.remove(out_csv))
  }
  readr::write_csv(
    daily_summary |>
      transmute(
        pass_date = format(pass_date, "%Y-%m-%d"),
        station_series = station_series_name,
        station_series_source = station_series_source,
        station_daily_mean = station_daily_mean,
        sensor_resistance_ohms = mobile_daily_mean,
        sensor_resistance_100k_ohm = mobile_daily_mean / 100000,
        sensor_scaled_to_station_axis = mobile_scaled,
        abs_axis_difference = diff_abs,
        aggregated_passes = n_passes
      ),
    out_csv,
    na = ""
  )

  list(
    compared_days = nrow(daily_summary),
    input_passes = sum(daily_summary$n_passes, na.rm = TRUE),
    min_date = as.character(min(daily_summary$pass_date, na.rm = TRUE)),
    max_date = as.character(max(daily_summary$pass_date, na.rm = TRUE)),
    mean_abs_diff = mean(daily_summary$diff_abs, na.rm = TRUE),
    data_csv = out_csv
  )
}

save_reducing_no_dumbbell_svg <- function(
  pass_summary,
  station_unit_label,
  out_file
) {
  if (nrow(pass_summary) == 0) {
    return(NULL)
  }

  n_before_trim <- nrow(pass_summary)
  pass_summary <- pass_summary |>
    filter(!is.na(pass_timestamp), is.finite(station_value), is.finite(mobile_value)) |>
    arrange(pass_timestamp)
  if (nrow(pass_summary) == 0) {
    return(NULL)
  }

  edge_trim <- identify_edge_time_outliers(pass_summary$pass_timestamp, iqr_mult = 1.5, min_points = 12L)
  keep_mask <- !edge_trim$mask
  if (sum(keep_mask) >= 8) {
    pass_summary <- pass_summary[keep_mask, , drop = FALSE] |>
      arrange(pass_timestamp)
  }
  if (nrow(pass_summary) == 0) {
    return(NULL)
  }

  axis_map <- build_linear_transform(
    source_values = pass_summary$mobile_value,
    target_values = pass_summary$station_value
  )
  if (is.null(axis_map)) {
    return(NULL)
  }

  pass_summary <- pass_summary |>
    mutate(
      mobile_scaled = axis_map$to_target(mobile_value),
      diff_abs = abs(station_value - mobile_scaled),
      pass_date = as.Date(pass_timestamp, tz = config$timezone)
    )
  compact_axis <- build_compact_date_axis(pass_summary$pass_timestamp, tz = config$timezone)
  pass_summary$x_compact <- compact_axis$x

  color_map <- c(
    "Station NO" = "#C92A2A",
    "Sensor Resistance" = "#0B7285"
  )
  y_limits <- compute_y_limits(c(pass_summary$station_value, pass_summary$mobile_scaled))

  p <- ggplot2::ggplot(pass_summary, ggplot2::aes(x = x_compact)) +
    ggplot2::geom_segment(
      ggplot2::aes(
        xend = x_compact,
        y = station_value,
        yend = mobile_scaled
      ),
      color = "grey40",
      alpha = 0.7,
      linewidth = 0.8
    )

  p <- p +
    ggplot2::geom_point(
      ggplot2::aes(y = station_value, color = "Station NO"),
      size = 3.4
    ) +
    ggplot2::geom_point(
      ggplot2::aes(y = mobile_scaled, color = "Sensor Resistance"),
      size = 3.4
    ) +
    ggplot2::scale_color_manual(values = color_map) +
    ggplot2::scale_x_continuous(
      breaks = compact_axis$breaks,
      labels = compact_axis$labels,
      expand = ggplot2::expansion(mult = c(0.01, 0.02))
    ) +
    ggplot2::scale_y_continuous(
      name = sprintf("Station NO (%s)", station_unit_label),
      limits = y_limits,
      expand = ggplot2::expansion(mult = c(0, 0.02)),
      sec.axis = ggplot2::sec_axis(
        transform = ~ axis_map$to_source(.) / 100000,
        name = "Sensor Resistance in 100 k\u03A9",
        labels = scales::label_number(accuracy = 0.01, big.mark = ",")
      )
    ) +
    ggplot2::labs(
      title = "Reducing Gas sensor comparision with daily passed air quality station 144 (Neuk\u00F6lln).",
      x = "Date (2026)",
      color = NULL
    ) +
    ggplot2::theme_minimal(base_size = 18) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(size = 24, face = "bold"),
      axis.title = ggplot2::element_text(size = 24),
      axis.text = ggplot2::element_text(size = 20),
      legend.text = ggplot2::element_text(size = 20),
      legend.position = "top",
      panel.grid.minor = ggplot2::element_blank(),
      plot.margin = ggplot2::margin(t = 20, r = 40, b = 20, l = 20)
    )

  if (file.exists(out_file)) {
    suppressWarnings(file.remove(out_file))
  }

  ggplot2::ggsave(
    filename = out_file,
    plot = p,
    width = 18,
    height = 10,
    dpi = config$dpi,
    units = "in",
    device = grDevices::svg
  )

  if (!file.exists(out_file)) {
    stop(sprintf("Failed to write SVG output: %s", out_file), call. = FALSE)
  }

  out_csv <- file.path(
    dirname(out_file),
    sprintf("%s_data.csv", tools::file_path_sans_ext(basename(out_file)))
  )
  if (file.exists(out_csv)) {
    suppressWarnings(file.remove(out_csv))
  }
  readr::write_csv(
    pass_summary |>
      transmute(
        pass_timestamp_local = format(pass_timestamp, "%Y-%m-%d %H:%M:%S"),
        pass_date = format(pass_date, "%Y-%m-%d"),
        pass_id = as.character(pass_id),
        station_no_ug_m3 = station_value,
        sensor_resistance_ohms = mobile_value,
        sensor_resistance_100k_ohm = mobile_value / 100000,
        sensor_scaled_to_station_axis = mobile_scaled,
        abs_axis_difference = diff_abs
      ),
    out_csv,
    na = ""
  )

  unique_dates_n <- length(unique(pass_summary$pass_date))
  min_date <- as.character(min(pass_summary$pass_date, na.rm = TRUE))
  max_date <- as.character(max(pass_summary$pass_date, na.rm = TRUE))

  list(
    compared_passes = nrow(pass_summary),
    unique_dates = unique_dates_n,
    min_date = min_date,
    max_date = max_date,
    mean_abs_diff = mean(pass_summary$diff_abs, na.rm = TRUE),
    trimmed_left = edge_trim$n_left,
    trimmed_right = edge_trim$n_right,
    trimmed_total = max(0L, n_before_trim - nrow(pass_summary)),
    data_csv = out_csv
  )
}

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
raw_df <- readr::read_csv(
  config$input_csv,
  show_col_types = FALSE,
  progress = FALSE,
  guess_max = 100000
)

if (nrow(raw_df) == 0) {
  stop("Input CSV has no rows.", call. = FALSE)
}

required_cols <- c("station_id", "station_hour_local", "pass_id", "mobile_timestamp_local")
missing_required <- required_cols[!required_cols %in% names(raw_df)]
if (length(missing_required) > 0) {
  stop(
    sprintf("Input CSV missing required columns: %s", paste(missing_required, collapse = ", ")),
    call. = FALSE
  )
}

mobile_pm25_col <- pick_best_mobile_column(
  raw_df,
  c("mobile_aq__pm2_5_count", "pm2_5_count", "pm2_5_ug_m3", "pm2_5_atm_ug_m3")
)
mobile_oxidising_col <- pick_best_mobile_column(
  raw_df,
  c("mobile_aq__gas_oxidising_ohms", "gas_oxidising_ohms")
)
mobile_reducing_col <- pick_best_mobile_column(
  raw_df,
  c("mobile_aq__gas_reducing_ohms", "gas_reducing_ohms")
)

if (is.na(mobile_pm25_col)) {
  message("No mobile PM2.5 column with values found. PM2.5 plots will be skipped.")
} else {
  message(sprintf("Using mobile PM2.5 column: %s", mobile_pm25_col))
}

if (is.na(mobile_oxidising_col)) {
  message("No mobile oxidising resistance column with values found. Oxidising-gas plots will be skipped.")
} else {
  message(sprintf("Using mobile oxidising column: %s", mobile_oxidising_col))
}

if (is.na(mobile_reducing_col)) {
  message("No mobile reducing resistance column with values found. Reducing-vs-NO plots will be skipped.")
} else {
  message(sprintf("Using mobile reducing column: %s", mobile_reducing_col))
}

if (is.na(mobile_pm25_col) && is.na(mobile_oxidising_col) && is.na(mobile_reducing_col)) {
  stop("No usable mobile PM2.5, oxidising resistance, or reducing resistance columns were found.", call. = FALSE)
}

station_label_source <- if ("station_label" %in% names(raw_df)) raw_df$station_label else raw_df$station_id
mobile_pm25_values <- if (!is.na(mobile_pm25_col)) parse_numeric_robust(raw_df[[mobile_pm25_col]]) else rep(NA_real_, nrow(raw_df))
mobile_oxidising_values <- if (!is.na(mobile_oxidising_col)) parse_numeric_robust(raw_df[[mobile_oxidising_col]]) else rep(NA_real_, nrow(raw_df))
mobile_reducing_values <- if (!is.na(mobile_reducing_col)) parse_numeric_robust(raw_df[[mobile_reducing_col]]) else rep(NA_real_, nrow(raw_df))

prepared_df <- raw_df |>
  mutate(
    station_id = as.character(station_id),
    station_label = dplyr::if_else(
      !is.na(as.character(station_label_source)) & as.character(station_label_source) != "",
      as.character(station_label_source),
      as.character(station_id),
      missing = as.character(station_id)
    ),
    station_hour_dt = parse_timestamp_flexible(station_hour_local, tz = config$timezone),
    mobile_ts_dt = parse_timestamp_flexible(mobile_timestamp_local, tz = config$timezone),
    mobile_pm25 = mobile_pm25_values,
    mobile_oxidising_ohms = mobile_oxidising_values,
    mobile_reducing_ohms = mobile_reducing_values
  )

station_ids <- prepared_df |>
  filter(!is.na(station_id), station_id != "") |>
  distinct(station_id) |>
  pull(station_id)

if (length(station_ids) == 0) {
  stop("No station IDs found in the CSV.", call. = FALSE)
}

plot_manifest <- list()

# ---------------------------------------------------------------------------
# PM2.5 line graphs
# ---------------------------------------------------------------------------
if (!is.na(mobile_pm25_col)) {
  for (sid in station_ids) {
    station_pm25_col <- station_pm25_column_for_id(sid, names(prepared_df))
    if (is.na(station_pm25_col)) {
      message(sprintf("PM2.5: skipping station '%s' (no station PM2.5 column matched from headers).", sid))
      next
    }

    station_df <- prepared_df |>
      filter(station_id == sid) |>
      mutate(station_pm25 = parse_numeric_robust(.data[[station_pm25_col]])) |>
      filter(!is.na(mobile_pm25))

    if (nrow(station_df) == 0) {
      message(sprintf("PM2.5: skipping station '%s' (no successful mobile PM2.5 rows).", sid))
      next
    }

    pass_summary <- station_df |>
      group_by(station_id, station_label, pass_id, station_hour_dt) |>
      summarise(
        pass_timestamp = first_valid_timestamp(mobile_ts_dt, station_hour_dt),
        mobile_pm25_mean = mean(mobile_pm25, na.rm = TRUE),
        station_pm25_hour = safe_station_hour_mean(station_pm25),
        mobile_points = sum(!is.na(mobile_pm25)),
        .groups = "drop"
      ) |>
      filter(!is.na(pass_timestamp), mobile_points > 0, !is.na(station_pm25_hour)) |>
      arrange(pass_timestamp)

    if (nrow(pass_summary) == 0) {
      message(sprintf("PM2.5: skipping station '%s' (no rows with both mobile and station PM2.5).", sid))
      next
    }

    station_label_value <- pass_summary$station_label[[1]]
    y_limits <- compute_y_limits(c(pass_summary$mobile_pm25_mean, pass_summary$station_pm25_hour))

    plot_df <- pass_summary |>
      select(pass_timestamp, mobile_pm25_mean, station_pm25_hour) |>
      tidyr::pivot_longer(
        cols = c(mobile_pm25_mean, station_pm25_hour),
        names_to = "series",
        values_to = "value"
      ) |>
      mutate(
        series = dplyr::recode(
          series,
          mobile_pm25_mean = "Mobile pass mean PM2.5",
          station_pm25_hour = "Station hourly PM2.5"
        )
      )

    line_df <- plot_df |>
      dplyr::group_by(series) |>
      dplyr::filter(dplyr::n() > 1) |>
      dplyr::ungroup()

    p <- ggplot2::ggplot(
      plot_df,
      ggplot2::aes(x = pass_timestamp, y = value, color = series, group = series)
    )
    if (nrow(line_df) > 0) {
      p <- p + ggplot2::geom_line(data = line_df, linewidth = 0.9, alpha = 0.9)
    }
    p <- p +
      ggplot2::geom_point(size = 2) +
      ggplot2::scale_color_manual(
        values = c(
          "Mobile pass mean PM2.5" = "#0B7285",
          "Station hourly PM2.5" = "#C92A2A"
        )
      ) +
      ggplot2::scale_x_datetime(
        breaks = scales::breaks_pretty(n = 10),
        labels = scales::label_date(format = "%Y-%m-%d\n%H:%M")
      ) +
      ggplot2::scale_y_continuous(
        limits = y_limits,
        expand = ggplot2::expansion(mult = c(0, 0.02))
      ) +
      ggplot2::labs(
        title = sprintf("%s: PM2.5 Mobile vs Station", station_label_value),
        subtitle = sprintf(
          "Mobile source: %s | Station source: %s | Pass-level means from bypass rows",
          mobile_pm25_col, station_pm25_col
        ),
        x = "Measurement date/time (local)",
        y = "PM2.5 count",
        color = NULL
      ) +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(
        legend.position = "top",
        panel.grid.minor = ggplot2::element_blank()
      )

    out_file <- file.path(
      config$output_dir,
      sprintf("%s_pm25_mobile_vs_station.jpg", safe_filename(sid))
    )

    ggplot2::ggsave(
      filename = out_file,
      plot = p,
      width = config$width,
      height = config$height,
      dpi = config$dpi,
      units = "in",
      device = "jpeg"
    )

    plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
      plot_type = "pm25",
      station_id = sid,
      station_label = station_label_value,
      mobile_column = mobile_pm25_col,
      station_columns = station_pm25_col,
      compared_passes = nrow(pass_summary),
      output_jpg = out_file,
      notes = ""
    )
  }
}

# ---------------------------------------------------------------------------
# Oxidising gas line graphs (dual axis)
# ---------------------------------------------------------------------------
if (!is.na(mobile_oxidising_col)) {
  for (sid in station_ids) {
    oxid_station_cols <- station_oxidising_columns_for_id(sid, names(prepared_df))
    if (length(oxid_station_cols) == 0) {
      message(sprintf("Oxidising: skipping station '%s' (no oxidising station gas columns found).", sid))
      next
    }

    station_df <- prepared_df |>
      filter(station_id == sid) |>
      filter(!is.na(mobile_oxidising_ohms))

    if (nrow(station_df) == 0) {
      message(sprintf("Oxidising: skipping station '%s' (no mobile oxidising rows).", sid))
      next
    }

    for (col_name in oxid_station_cols) {
      station_df[[col_name]] <- parse_numeric_robust(station_df[[col_name]])
    }

    pass_summary <- station_df |>
      group_by(station_id, station_label, pass_id, station_hour_dt) |>
      summarise(
        pass_timestamp = first_valid_timestamp(mobile_ts_dt, station_hour_dt),
        mobile_oxidising_mean = mean(mobile_oxidising_ohms, na.rm = TRUE),
        mobile_points = sum(!is.na(mobile_oxidising_ohms)),
        across(all_of(oxid_station_cols), safe_station_hour_mean, .names = "{.col}__hour"),
        .groups = "drop"
      ) |>
      filter(!is.na(pass_timestamp), mobile_points > 0) |>
      arrange(pass_timestamp)

    station_hour_cols <- paste0(oxid_station_cols, "__hour")
    pass_summary <- pass_summary |>
      filter(if_any(all_of(station_hour_cols), ~ !is.na(.x)))

    if (nrow(pass_summary) == 0) {
      message(sprintf("Oxidising: skipping station '%s' (no rows with both station oxidising gas and mobile oxidising).", sid))
      next
    }

    station_long <- pass_summary |>
      select(pass_timestamp, all_of(station_hour_cols)) |>
      tidyr::pivot_longer(
        cols = all_of(station_hour_cols),
        names_to = "station_hour_col",
        values_to = "station_value",
        values_drop_na = TRUE
      ) |>
      mutate(
        station_column = stringr::str_remove(station_hour_col, "__hour$"),
        series = vapply(station_column, station_gas_label_from_column, character(1))
      )

    if (nrow(station_long) == 0) {
      message(sprintf("Oxidising: skipping station '%s' (station oxidising values are all NA after aggregation).", sid))
      next
    }

    station_label_value <- pass_summary$station_label[[1]]
    out_file <- file.path(
      config$output_dir,
      sprintf("%s_oxidising_gases_vs_mobile_ohms.jpg", safe_filename(sid))
    )

    full_plot_meta <- save_oxidising_dual_axis_plot(
      pass_summary = pass_summary,
      station_long = station_long,
      station_label_value = station_label_value,
      station_id = sid,
      mobile_oxidising_col = mobile_oxidising_col,
      out_file = out_file
    )
    if (is.null(full_plot_meta)) {
      message(sprintf("Oxidising: skipping station '%s' (insufficient value range for axis mapping).", sid))
      next
    }

    plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
      plot_type = "oxidising_dual_axis",
      station_id = sid,
      station_label = station_label_value,
      mobile_column = mobile_oxidising_col,
      station_columns = full_plot_meta$station_columns,
      compared_passes = full_plot_meta$compared_passes,
      output_jpg = out_file,
      notes = full_plot_meta$note_text
    )

    # Additional Silbersteinstraße zoomed split plots around the major drop near 18k ohms.
    if (tolower(sid) == "silbersteinstrasse") {
      drop_ts <- detect_drop_timestamp(pass_summary, threshold_ohms = 18000, min_drop_ohms = 500)
      if (!is.na(drop_ts)) {
        pre_pass <- pass_summary |>
          filter(pass_timestamp < drop_ts)
        post_pass <- pass_summary |>
          filter(pass_timestamp >= drop_ts)

        if (nrow(pre_pass) > 1) {
          pre_out <- file.path(
            config$output_dir,
            sprintf("%s_oxidising_gases_vs_mobile_ohms_pre_drop_zoom.jpg", safe_filename(sid))
          )
          pre_meta <- save_oxidising_dual_axis_plot(
            pass_summary = pre_pass,
            station_long = station_long,
            station_label_value = station_label_value,
            station_id = sid,
            mobile_oxidising_col = mobile_oxidising_col,
            out_file = pre_out,
            title_suffix = " (Pre-Drop Zoom)",
            subtitle_suffix = sprintf(" | Split before drop at %s", format(drop_ts, "%Y-%m-%d %H:%M"))
          )
          if (!is.null(pre_meta)) {
            plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
              plot_type = "oxidising_dual_axis_zoom_pre_drop",
              station_id = sid,
              station_label = station_label_value,
              mobile_column = mobile_oxidising_col,
              station_columns = pre_meta$station_columns,
              compared_passes = pre_meta$compared_passes,
              output_jpg = pre_out,
              notes = paste("Split before drop timestamp:", format(drop_ts, "%Y-%m-%d %H:%M"), pre_meta$note_text)
            )
          }
        }

        if (nrow(post_pass) > 1) {
          post_out <- file.path(
            config$output_dir,
            sprintf("%s_oxidising_gases_vs_mobile_ohms_post_drop_zoom.jpg", safe_filename(sid))
          )
          post_meta <- save_oxidising_dual_axis_plot(
            pass_summary = post_pass,
            station_long = station_long,
            station_label_value = station_label_value,
            station_id = sid,
            mobile_oxidising_col = mobile_oxidising_col,
            out_file = post_out,
            title_suffix = " (Post-Drop Zoom)",
            subtitle_suffix = sprintf(" | Split from drop at %s", format(drop_ts, "%Y-%m-%d %H:%M"))
          )
          if (!is.null(post_meta)) {
            plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
              plot_type = "oxidising_dual_axis_zoom_post_drop",
              station_id = sid,
              station_label = station_label_value,
              mobile_column = mobile_oxidising_col,
              station_columns = post_meta$station_columns,
              compared_passes = post_meta$compared_passes,
              output_jpg = post_out,
              notes = paste("Split from drop timestamp:", format(drop_ts, "%Y-%m-%d %H:%M"), post_meta$note_text)
            )
          }
        }
      } else {
        message("Silbersteinstraße: no <=18,000 ohm drop point detected; skipped split zoom plots.")
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Targeted NO/NO2 comparison graphs (explicit left-axis units: µg/m³)
# ---------------------------------------------------------------------------
station_unit_label <- "\u00B5g/m\u00B3"
silberstein_zoom_start_dt <- as.POSIXct(
  sprintf("%s 00:00:00", config$silberstein_zoom_start),
  tz = config$timezone
)
silberstein_zoom_end_dt <- as.POSIXct(
  sprintf("%s 23:59:59", config$silberstein_zoom_end),
  tz = config$timezone
)
silberstein_zoom_label <- sprintf(
  "%s to %s",
  format(silberstein_zoom_start_dt, "%d.%m.%Y"),
  format(silberstein_zoom_end_dt, "%d.%m.%Y")
)

if (!is.na(mobile_reducing_col) || !is.na(mobile_oxidising_col)) {
  for (sid in station_ids) {
    if (!is.na(mobile_reducing_col)) {
      no_col <- station_no_column_for_id(sid, names(prepared_df))
      if (!is.na(no_col)) {
        station_df <- prepared_df |>
          filter(station_id == sid) |>
          mutate(station_no = parse_numeric_robust(.data[[no_col]])) |>
          filter(!is.na(mobile_reducing_ohms))

        pass_summary <- station_df |>
          group_by(station_id, station_label, pass_id, station_hour_dt) |>
          summarise(
            pass_timestamp = first_valid_timestamp(mobile_ts_dt, station_hour_dt),
            mobile_value = mean(mobile_reducing_ohms, na.rm = TRUE),
            station_value = safe_station_hour_mean(station_no),
            mobile_points = sum(!is.na(mobile_reducing_ohms)),
            .groups = "drop"
          ) |>
          filter(!is.na(pass_timestamp), mobile_points > 0, !is.na(station_value)) |>
          arrange(pass_timestamp)

        if (nrow(pass_summary) > 0) {
          station_label_value <- pass_summary$station_label[[1]]
          out_file <- file.path(
            config$output_dir,
            sprintf("%s_reducing_ohms_vs_station_no.jpg", safe_filename(sid))
          )

          reduce_meta <- save_target_gas_dual_axis_plot(
            pass_summary = pass_summary,
            station_series_name = "Station NO",
            mobile_series_name = "Mobile reducing resistance (scaled to NO range)",
            left_axis_name = sprintf("Station NO (%s)", station_unit_label),
            right_axis_name = sprintf("Mobile reducing resistance (ohms) [%s]", mobile_reducing_col),
            title_text = sprintf("%s: Mobile Reducing Sensor vs Station NO", station_label_value),
            subtitle_text = "Pass-level mobile means (5-second data) aligned to station hourly resolution.",
            out_file = out_file
          )

          if (!is.null(reduce_meta)) {
            plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
              plot_type = "reducing_ohms_vs_no",
              station_id = sid,
              station_label = station_label_value,
              mobile_column = mobile_reducing_col,
              station_columns = no_col,
              compared_passes = reduce_meta$compared_passes,
              output_jpg = out_file,
              notes = sprintf("Left axis in %s.", station_unit_label)
            )
          }

          reducing_daily_summary <- aggregate_daily_pass_summary(
            pass_summary = pass_summary,
            tz = config$timezone
          )
          if (nrow(reducing_daily_summary) > 0) {
            reducing_daily_svg <- file.path(
              config$output_dir,
              sprintf("%s_reducing_ohms_vs_station_no_daily_line.svg", safe_filename(sid))
            )

            reducing_daily_meta <- save_daily_dual_axis_line_svg(
              daily_summary = reducing_daily_summary,
              station_series_name = "Station NO",
              station_series_source = "NO",
              left_axis_name = sprintf("Station NO (%s)", station_unit_label),
              title_text = sprintf("%s: Mobile Reducing Sensor vs Station NO (Daily Mean)", station_label_value),
              out_file = reducing_daily_svg
            )

            if (!is.null(reducing_daily_meta)) {
              plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
                plot_type = "reducing_ohms_vs_no_daily_line_svg",
                station_id = sid,
                station_label = station_label_value,
                mobile_column = mobile_reducing_col,
                station_columns = no_col,
                compared_passes = reducing_daily_meta$compared_days,
                output_jpg = reducing_daily_svg,
                notes = sprintf(
                  paste(
                    "Daily mean aggregation (1 sample per recorded date), compressed x-axis;",
                    "based on %d pass rows across %d daily points (%s to %s);",
                    "mean |scaled difference| = %.2f (%s axis units); Data CSV: %s"
                  ),
                  reducing_daily_meta$input_passes,
                  reducing_daily_meta$compared_days,
                  reducing_daily_meta$min_date,
                  reducing_daily_meta$max_date,
                  reducing_daily_meta$mean_abs_diff,
                  station_unit_label,
                  reducing_daily_meta$data_csv
                )
              )
            }

            if (is_silberstein_station(sid)) {
              reducing_daily_custom_svg <- file.path(
                config$output_dir,
                "silbersteinstrasse_station_144_no_sensor_vs_mobile_reducing_gas_sensor_daily_line_compact.svg"
              )

              reducing_daily_custom_meta <- save_daily_dual_axis_line_svg(
                daily_summary = reducing_daily_summary,
                station_series_name = "Station NO sensor",
                station_series_source = "NO",
                left_axis_name = sprintf("Station NO (%s)", station_unit_label),
                title_text = "Station 144 NO Sensor\nvs Mobile Reducing Gas Sensor",
                out_file = reducing_daily_custom_svg,
                mobile_series_name = "Mobile Reducing Sensor Resistance",
                plot_width = 9,
                plot_height = 10,
                title_size = 28,
                title_bottom_margin = 8
              )

              if (!is.null(reducing_daily_custom_meta)) {
                plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
                  plot_type = "reducing_ohms_vs_no_daily_line_svg_compact_custom",
                  station_id = sid,
                  station_label = station_label_value,
                  mobile_column = mobile_reducing_col,
                  station_columns = no_col,
                  compared_passes = reducing_daily_custom_meta$compared_days,
                  output_jpg = reducing_daily_custom_svg,
                  notes = sprintf(
                    paste(
                      "Custom compact SVG (half width); daily mean aggregation;",
                      "legend label uses 'Mobile Reducing Sensor Resistance';",
                      "Data CSV: %s"
                    ),
                    reducing_daily_custom_meta$data_csv
                  )
                )
              }
            }
          }

          if (is_silberstein_station(sid)) {
            dumbbell_svg <- file.path(
              config$output_dir,
              sprintf("%s_reducing_ohms_vs_station_no_dumbbell.svg", safe_filename(sid))
            )
            dumbbell_meta <- save_reducing_no_dumbbell_svg(
              pass_summary = pass_summary,
              station_unit_label = station_unit_label,
              out_file = dumbbell_svg
            )
            if (!is.null(dumbbell_meta)) {
              plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
                plot_type = "reducing_ohms_vs_no_dumbbell_svg",
                station_id = sid,
                station_label = station_label_value,
                mobile_column = mobile_reducing_col,
                station_columns = no_col,
                compared_passes = dumbbell_meta$compared_passes,
                output_jpg = dumbbell_svg,
                notes = sprintf(
                  paste(
                    "Dumbbell chart (SVG). Mean |scaled difference| = %.2f (%s axis units).",
                    "Trimmed edge outliers: left=%d, right=%d, total removed=%d.",
                    "Used %d passes across %d recorded dates (%s to %s).",
                    "Data CSV: %s"
                  ),
                  dumbbell_meta$mean_abs_diff,
                  station_unit_label,
                  dumbbell_meta$trimmed_left,
                  dumbbell_meta$trimmed_right,
                  dumbbell_meta$trimmed_total,
                  dumbbell_meta$compared_passes,
                  dumbbell_meta$unique_dates,
                  dumbbell_meta$min_date,
                  dumbbell_meta$max_date,
                  dumbbell_meta$data_csv
                )
              )
            }
          }

          if (is_silberstein_station(sid)) {
            zoom_pass_summary <- pass_summary |>
              filter(
                pass_timestamp >= silberstein_zoom_start_dt,
                pass_timestamp <= silberstein_zoom_end_dt
              ) |>
              arrange(pass_timestamp)

            if (nrow(zoom_pass_summary) > 1) {
              zoom_window_passes <- nrow(zoom_pass_summary)
              zoom_mobile_range <- compute_lower_ohm_focus_range(
                zoom_pass_summary$mobile_value,
                focus_quantile = config$silberstein_low_ohm_quantile
              )

              if (!is.null(zoom_mobile_range)) {
                zoom_out_file <- file.path(
                  config$output_dir,
                  sprintf(
                    "%s_reducing_ohms_vs_station_no_low_ohm_zoom_2026_01_25_to_2026_08_02.jpg",
                    safe_filename(sid)
                  )
                )

                zoom_reduce_meta <- save_target_gas_dual_axis_plot(
                  pass_summary = zoom_pass_summary,
                  station_series_name = "Station NO",
                  mobile_series_name = "Mobile reducing resistance (scaled to NO range)",
                  left_axis_name = sprintf("Station NO (%s)", station_unit_label),
                  right_axis_name = sprintf("Mobile reducing resistance (ohms) [%s]", mobile_reducing_col),
                  title_text = sprintf("%s: Mobile Reducing Sensor vs Station NO (Low-Ohm Zoom)", station_label_value),
                  subtitle_text = sprintf(
                    paste(
                      "Silbersteinstrasse only | Date window: %s |",
                      "Keeping only low-ohm mobile passes (<= approx %.0fth percentile),",
                      "and breaking lines at gaps > 12h."
                    ),
                    silberstein_zoom_label,
                    config$silberstein_low_ohm_quantile * 100
                  ),
                  out_file = zoom_out_file,
                  mobile_zoom_range = zoom_mobile_range,
                  max_line_gap_hours = 12
                )

                if (!is.null(zoom_reduce_meta)) {
                  plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
                    plot_type = "reducing_ohms_vs_no_low_ohm_zoom_window",
                    station_id = sid,
                    station_label = station_label_value,
                    mobile_column = mobile_reducing_col,
                    station_columns = no_col,
                    compared_passes = zoom_reduce_meta$compared_passes,
                    output_jpg = zoom_out_file,
                    notes = sprintf(
                      paste(
                        "Silbersteinstrasse only; date window %s;",
                        "low-ohm window based on %.0fth percentile;",
                        "kept %d of %d passes after low-ohm filtering;"
                      ),
                      silberstein_zoom_label,
                      config$silberstein_low_ohm_quantile * 100,
                      zoom_reduce_meta$compared_passes,
                      zoom_window_passes
                    )
                  )
                }
              } else {
                message("Silbersteinstrasse reducing-vs-NO zoom: unable to derive a low-ohm zoom range.")
              }
            } else {
              message("Silbersteinstrasse reducing-vs-NO zoom: not enough pass rows in requested date window.")
            }
          }
        }
      }
    }

    if (!is.na(mobile_oxidising_col)) {
      nox_col <- station_nox_column_for_id(sid, names(prepared_df))
      nox_target_col <- nox_col
      nox_series_name <- "Station NOx"
      nox_source_label <- "NOx"
      nox_fallback_used <- FALSE

      if (is.na(nox_target_col)) {
        no2_fallback_col <- station_no2_column_for_id(sid, names(prepared_df))
        if (!is.na(no2_fallback_col)) {
          nox_target_col <- no2_fallback_col
          nox_series_name <- "Station NO2"
          nox_source_label <- "NO2 (fallback)"
          nox_fallback_used <- TRUE
        }
      }

      if (is.na(nox_target_col)) {
        plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
          plot_type = "oxidising_ohms_vs_nox_daily_line_svg",
          station_id = sid,
          station_label = sid,
          mobile_column = mobile_oxidising_col,
          station_columns = "",
          compared_passes = 0L,
          output_jpg = "",
          notes = "Skipped daily SVG: no station NOx column and no NO2 fallback column found."
        )
      } else {
        station_df_nox <- prepared_df |>
          filter(station_id == sid) |>
          mutate(station_nox_target = parse_numeric_robust(.data[[nox_target_col]])) |>
          filter(!is.na(mobile_oxidising_ohms))

        pass_summary_nox <- station_df_nox |>
          group_by(station_id, station_label, pass_id, station_hour_dt) |>
          summarise(
            pass_timestamp = first_valid_timestamp(mobile_ts_dt, station_hour_dt),
            mobile_value = mean(mobile_oxidising_ohms, na.rm = TRUE),
            station_value = safe_station_hour_mean(station_nox_target),
            mobile_points = sum(!is.na(mobile_oxidising_ohms)),
            .groups = "drop"
          ) |>
          filter(!is.na(pass_timestamp), mobile_points > 0, !is.na(station_value)) |>
          arrange(pass_timestamp)

        if (nrow(pass_summary_nox) == 0) {
          plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
            plot_type = "oxidising_ohms_vs_nox_daily_line_svg",
            station_id = sid,
            station_label = sid,
            mobile_column = mobile_oxidising_col,
            station_columns = nox_target_col,
            compared_passes = 0L,
            output_jpg = "",
            notes = sprintf(
              "Skipped daily SVG: no valid rows after filters (series source: %s).",
              nox_source_label
            )
          )
        } else {
          station_label_value_nox <- pass_summary_nox$station_label[[1]]
          nox_daily_summary <- aggregate_daily_pass_summary(
            pass_summary = pass_summary_nox,
            tz = config$timezone
          )

          if (nrow(nox_daily_summary) > 0) {
            oxidising_daily_svg <- file.path(
              config$output_dir,
              sprintf("%s_oxidising_ohms_vs_station_nox_daily_line.svg", safe_filename(sid))
            )

            oxidising_daily_meta <- save_daily_dual_axis_line_svg(
              daily_summary = nox_daily_summary,
              station_series_name = nox_series_name,
              station_series_source = nox_source_label,
              left_axis_name = sprintf("%s (%s)", nox_series_name, station_unit_label),
              title_text = sprintf(
                "%s: Mobile Oxidising Sensor vs %s (Daily Mean)",
                station_label_value_nox,
                nox_series_name
              ),
              out_file = oxidising_daily_svg
            )

            if (!is.null(oxidising_daily_meta)) {
              plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
                plot_type = "oxidising_ohms_vs_nox_daily_line_svg",
                station_id = sid,
                station_label = station_label_value_nox,
                mobile_column = mobile_oxidising_col,
                station_columns = nox_target_col,
                compared_passes = oxidising_daily_meta$compared_days,
                output_jpg = oxidising_daily_svg,
                notes = sprintf(
                  paste(
                    "Daily mean aggregation (1 sample per recorded date), compressed x-axis;",
                    "series source: %s; NO2 fallback used: %s;",
                    "based on %d pass rows across %d daily points (%s to %s);",
                    "mean |scaled difference| = %.2f (%s axis units); Data CSV: %s"
                  ),
                  nox_source_label,
                  ifelse(nox_fallback_used, "yes", "no"),
                  oxidising_daily_meta$input_passes,
                  oxidising_daily_meta$compared_days,
                  oxidising_daily_meta$min_date,
                  oxidising_daily_meta$max_date,
                  oxidising_daily_meta$mean_abs_diff,
                  station_unit_label,
                  oxidising_daily_meta$data_csv
                )
              )
            }

            if (normalize_key(sid) == "mariendorferdamm") {
              oxidising_daily_custom_svg <- file.path(
                config$output_dir,
                "mariendorfer_damm_station_124_nox_sensor_vs_mobile_oxidizing_gas_sensor_daily_line_compact.svg"
              )

              oxidising_daily_custom_meta <- save_daily_dual_axis_line_svg(
                daily_summary = nox_daily_summary,
                station_series_name = sprintf("%s sensor", nox_series_name),
                station_series_source = nox_source_label,
                left_axis_name = sprintf("%s (%s)", nox_series_name, station_unit_label),
                title_text = "Station 124 NOx Sensor\nvs. Mobile Oxidizing Gas Sensor.",
                out_file = oxidising_daily_custom_svg,
                mobile_series_name = "Mobile Oxidizing Sensor Resistance",
                plot_width = 9,
                plot_height = 10,
                title_size = 28,
                title_bottom_margin = 8
              )

              if (!is.null(oxidising_daily_custom_meta)) {
                plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
                  plot_type = "oxidising_ohms_vs_nox_daily_line_svg_compact_custom",
                  station_id = sid,
                  station_label = station_label_value_nox,
                  mobile_column = mobile_oxidising_col,
                  station_columns = nox_target_col,
                  compared_passes = oxidising_daily_custom_meta$compared_days,
                  output_jpg = oxidising_daily_custom_svg,
                  notes = sprintf(
                    paste(
                      "Custom compact SVG (half width); daily mean aggregation;",
                      "legend label uses 'Mobile Oxidizing Sensor Resistance';",
                      "series source: %s; Data CSV: %s"
                    ),
                    nox_source_label,
                    oxidising_daily_custom_meta$data_csv
                  )
                )
              }
            }
          }
        }
      }

      no2_col <- station_no2_column_for_id(sid, names(prepared_df))
      if (!is.na(no2_col)) {
        station_df <- prepared_df |>
          filter(station_id == sid) |>
          mutate(station_no2 = parse_numeric_robust(.data[[no2_col]])) |>
          filter(!is.na(mobile_oxidising_ohms))

        pass_summary <- station_df |>
          group_by(station_id, station_label, pass_id, station_hour_dt) |>
          summarise(
            pass_timestamp = first_valid_timestamp(mobile_ts_dt, station_hour_dt),
            mobile_value = mean(mobile_oxidising_ohms, na.rm = TRUE),
            station_value = safe_station_hour_mean(station_no2),
            mobile_points = sum(!is.na(mobile_oxidising_ohms)),
            .groups = "drop"
          ) |>
          filter(!is.na(pass_timestamp), mobile_points > 0, !is.na(station_value)) |>
          arrange(pass_timestamp)

        if (nrow(pass_summary) > 0) {
          station_label_value <- pass_summary$station_label[[1]]
          out_file <- file.path(
            config$output_dir,
            sprintf("%s_oxidising_ohms_vs_station_no2.jpg", safe_filename(sid))
          )

          oxid_no2_meta <- save_target_gas_dual_axis_plot(
            pass_summary = pass_summary,
            station_series_name = "Station NO2",
            mobile_series_name = "Mobile oxidising resistance (scaled to NO2 range)",
            left_axis_name = sprintf("Station NO2 (%s)", station_unit_label),
            right_axis_name = sprintf("Mobile oxidising resistance (ohms) [%s]", mobile_oxidising_col),
            title_text = sprintf("%s: Mobile Oxidising Sensor vs Station NO2", station_label_value),
            subtitle_text = "Pass-level mobile means (5-second data) aligned to station hourly resolution.",
            out_file = out_file
          )

          if (!is.null(oxid_no2_meta)) {
            plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
              plot_type = "oxidising_ohms_vs_no2",
              station_id = sid,
              station_label = station_label_value,
              mobile_column = mobile_oxidising_col,
              station_columns = no2_col,
              compared_passes = oxid_no2_meta$compared_passes,
              output_jpg = out_file,
              notes = sprintf("Left axis in %s.", station_unit_label)
            )
          }

          if (is_silberstein_station(sid)) {
            zoom_pass_summary <- pass_summary |>
              filter(
                pass_timestamp >= silberstein_zoom_start_dt,
                pass_timestamp <= silberstein_zoom_end_dt
              ) |>
              arrange(pass_timestamp)

            if (nrow(zoom_pass_summary) > 1) {
              zoom_window_passes <- nrow(zoom_pass_summary)
              zoom_mobile_range <- compute_lower_ohm_focus_range(
                zoom_pass_summary$mobile_value,
                focus_quantile = config$silberstein_low_ohm_quantile
              )

              if (!is.null(zoom_mobile_range)) {
                zoom_out_file <- file.path(
                  config$output_dir,
                  sprintf(
                    "%s_oxidising_ohms_vs_station_no2_low_ohm_zoom_2026_01_25_to_2026_08_02.jpg",
                    safe_filename(sid)
                  )
                )

                zoom_oxid_meta <- save_target_gas_dual_axis_plot(
                  pass_summary = zoom_pass_summary,
                  station_series_name = "Station NO2",
                  mobile_series_name = "Mobile oxidising resistance (scaled to NO2 range)",
                  left_axis_name = sprintf("Station NO2 (%s)", station_unit_label),
                  right_axis_name = sprintf("Mobile oxidising resistance (ohms) [%s]", mobile_oxidising_col),
                  title_text = sprintf("%s: Mobile Oxidising Sensor vs Station NO2 (Low-Ohm Zoom)", station_label_value),
                  subtitle_text = sprintf(
                    paste(
                      "Silbersteinstrasse only | Date window: %s |",
                      "Keeping only low-ohm mobile passes (<= approx %.0fth percentile),",
                      "and breaking lines at gaps > 12h."
                    ),
                    silberstein_zoom_label,
                    config$silberstein_low_ohm_quantile * 100
                  ),
                  out_file = zoom_out_file,
                  mobile_zoom_range = zoom_mobile_range,
                  max_line_gap_hours = 12
                )

                if (!is.null(zoom_oxid_meta)) {
                  plot_manifest[[length(plot_manifest) + 1L]] <- dplyr::tibble(
                    plot_type = "oxidising_ohms_vs_no2_low_ohm_zoom_window",
                    station_id = sid,
                    station_label = station_label_value,
                    mobile_column = mobile_oxidising_col,
                    station_columns = no2_col,
                    compared_passes = zoom_oxid_meta$compared_passes,
                    output_jpg = zoom_out_file,
                    notes = sprintf(
                      paste(
                        "Silbersteinstrasse only; date window %s;",
                        "low-ohm window based on %.0fth percentile;",
                        "kept %d of %d passes after low-ohm filtering;"
                      ),
                      silberstein_zoom_label,
                      config$silberstein_low_ohm_quantile * 100,
                      zoom_oxid_meta$compared_passes,
                      zoom_window_passes
                    )
                  )
                }
              } else {
                message("Silbersteinstrasse oxidising-vs-NO2 zoom: unable to derive a low-ohm zoom range.")
              }
            } else {
              message("Silbersteinstrasse oxidising-vs-NO2 zoom: not enough pass rows in requested date window.")
            }
          }
        }
      }
    }
  }
}

if (length(plot_manifest) == 0) {
  stop("No station plots were generated. Check mobile/station data availability.", call. = FALSE)
}

manifest_df <- dplyr::bind_rows(plot_manifest)
manifest_path <- file.path(config$output_dir, "linegraph_manifest.csv")
readr::write_csv(manifest_df, manifest_path, na = "")

pm25_manifest <- manifest_df |> filter(plot_type == "pm25")
if (nrow(pm25_manifest) > 0) {
  readr::write_csv(pm25_manifest, file.path(config$output_dir, "linegraph_manifest_pm25.csv"), na = "")
}

oxidising_manifest <- manifest_df |> filter(stringr::str_starts(plot_type, "oxidising"))
if (nrow(oxidising_manifest) > 0) {
  readr::write_csv(oxidising_manifest, file.path(config$output_dir, "linegraph_manifest_oxidising.csv"), na = "")
}

message("Generated linegraphs:")
for (i in seq_len(nrow(manifest_df))) {
  message(sprintf("- [%s] %s -> %s", manifest_df$plot_type[[i]], manifest_df$station_id[[i]], manifest_df$output_jpg[[i]]))
}
message(sprintf("Manifest written: %s", manifest_path))
