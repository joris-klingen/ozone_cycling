# Within-day estimation (Eq. 1) ----

source("R/00_packages.R")

proc_dir <- "data/processed"
tab_dir <- "output/tables"
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

hourly <- as.data.table(read_parquet(file.path(proc_dir, "analysis_hourly.parquet")))

# Prepare variables ----

# Ozone per 10 ppb
hourly[, O3_10 := O3 / 10]

# Wind direction bins (8 compass directions, 45 degrees each)
hourly[, wind_dir_bin := cut(wind_direction_10m %% 360,
                              breaks = seq(0, 360, 45),
                              include.lowest = TRUE, right = FALSE)]

# Temperature lag bins (1 degree C dummies)
for (lag_h in 1:8) {
  lag_col <- paste0("temp_lag", lag_h)
  bin_col <- paste0("temp_lag", lag_h, "_bin")
  hourly[, (bin_col) := floor(get(lag_col))]
}

# Rain lag bins
for (lag_h in 1:8) {
  lag_col <- paste0("rain_lag", lag_h)
  bin_col <- paste0("rain_lag", lag_h, "_bin")
  hourly[, (bin_col) := fifelse(get(lag_col) > 0, 1L, 0L)]
}

# Day-of-week x hour interaction
hourly[, dow_hour := paste0(dow, "_", hour)]

# Year-month for between-day
hourly[, yearmonth := paste0(year, "_", sprintf("%02d", month))]

# Weekdays only (lubridate: Sun=1, Mon=2, ..., Sat=7)
hourly_wd <- hourly[dow %in% 2:6]

# Define sample periods ----

periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L),
  post2017    = c(2018L, 2025L)
)

# Build regression formula ----

weather_controls <- paste0(
  "factor(temp_bin) + surface_pressure + relative_humidity_2m + rain + ",
  "factor(radiation_bin) + shortwave_radiation:temperature_2m + wind_speed_10m + ",
  "factor(wind_dir_bin) + ",
  paste0("factor(temp_lag", 1:8, "_bin)", collapse = " + "), " + ",
  paste0("rain_lag", 1:8, collapse = " + ")
)

pollutant_controls <- "SO2 + factor(nox_bin) + factor(pm25_bin)"

# Within-day formula: speed ~ O3/10 + weather + pollutants | date + dow^hour
fml_speed <- as.formula(paste0(
  "mean_speed ~ O3_10 + ", weather_controls, " + ", pollutant_controls,
  " | date + dow_hour"
))

fml_dist <- as.formula(paste0(
  "mean_dist ~ O3_10 + ", weather_controls, " + ", pollutant_controls,
  " | date + dow_hour"
))

fml_trips <- as.formula(paste0(
  "log(n_trips) ~ O3_10 + ", weather_controls, " + ", pollutant_controls,
  " | date + dow_hour"
))

# Run regressions ----

results <- list()

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- hourly_wd[year >= yr_range[1] & year <= yr_range[2]]
  dt <- dt[!is.na(O3_10) & !is.na(mean_speed) & !is.na(temperature_2m)]

  cat("=== Within-day:", pname, "(", yr_range[1], "-", yr_range[2], ") ===\n")
  cat("  Observations:", nrow(dt), "\n")

  # (1) Speed
  m_speed <- fixest::feols(fml_speed, data = dt, weights = ~n_trips,
                            cluster = ~date, lean = TRUE)
  cat("  Speed coef on O3_10:", coef(m_speed)["O3_10"], "\n")

  # (3) Distance (sorting test)
  m_dist <- fixest::feols(fml_dist, data = dt, weights = ~n_trips,
                           cluster = ~date, lean = TRUE)
  cat("  Distance coef on O3_10:", coef(m_dist)["O3_10"], "\n")

  # (5) log(trips) (sorting test)
  m_trips <- fixest::feols(fml_trips, data = dt, weights = ~n_trips,
                            cluster = ~date, lean = TRUE)
  cat("  log(trips) coef on O3_10:", coef(m_trips)["O3_10"], "\n")

  results[[paste0("speed_", pname)]] <- m_speed
  results[[paste0("dist_", pname)]] <- m_dist
  results[[paste0("trips_", pname)]] <- m_trips
}

# Output Table 2 (within-day columns) ----

# Replication results
etable_rep <- fixest::etable(
  results$speed_replication, results$dist_replication, results$trips_replication,
  keep = "O3_10",
  headers = c("Speed", "Distance", "log(Trips)"),
  tex = TRUE
)

writeLines(etable_rep, file.path(tab_dir, "table2_within_day_replication.tex"))

# Extension results
etable_ext <- fixest::etable(
  results$speed_extension, results$dist_extension, results$trips_extension,
  keep = "O3_10",
  headers = c("Speed", "Distance", "log(Trips)"),
  tex = TRUE
)

writeLines(etable_ext, file.path(tab_dir, "table2_within_day_extension.tex"))

# Combined table
etable_all <- fixest::etable(
  results$speed_replication, results$speed_extension, results$speed_post2017,
  results$dist_replication, results$dist_extension, results$dist_post2017,
  results$trips_replication, results$trips_extension, results$trips_post2017,
  keep = "O3_10",
  headers = c("Speed (R)", "Speed (E)", "Speed (P)",
              "Dist (R)", "Dist (E)", "Dist (P)",
              "Trips (R)", "Trips (E)", "Trips (P)"),
  tex = TRUE
)

writeLines(etable_all, file.path(tab_dir, "table2_within_day_combined.tex"))

cat("\nWithin-day estimation complete.\n")
