# Between-day estimation (Eq. 2) ----

source("R/00_packages.R")

proc_dir <- "data/processed"
tab_dir <- "output/tables"
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

daily <- as.data.table(read_parquet(file.path(proc_dir, "analysis_daily.parquet")))

# Prepare variables ----

daily[, O3_10 := O3 / 10]
daily[, yearmonth := paste0(year, "_", sprintf("%02d", month))]

# Temperature bins (2.5 degree C)
daily[, temp_bin := floor(temp_mean / 2.5) * 2.5]

# Radiation bins (100 W/m2)
daily[, radiation_bin := floor(radiation / 100) * 100]

# NOx bins (10 ppb)
daily[, nox_bin := floor(NOx / 10) * 10]

# PM2.5 bins (10 ug/m3)
daily[, pm25_bin := floor(PM25 / 10) * 10]

# Weekdays only
daily_wd <- daily[dow %in% 2:6]

# Define sample periods ----

periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L),
  post2017    = c(2018L, 2025L)
)

# Build regression formula ----

weather_controls <- paste0(
  "factor(temp_bin) + pressure + humidity + rain_total + ",
  "factor(radiation_bin) + radiation:temp_mean + wind_speed"
)

pollutant_controls <- "SO2 + factor(nox_bin) + factor(pm25_bin)"

# Between-day: speed ~ O3/10 + weather + pollutants | yearmonth + dow
fml_speed <- as.formula(paste0(
  "mean_speed ~ O3_10 + ", weather_controls, " + ", pollutant_controls,
  " | yearmonth + dow"
))

fml_dist <- as.formula(paste0(
  "mean_dist ~ O3_10 + ", weather_controls, " + ", pollutant_controls,
  " | yearmonth + dow"
))

fml_trips <- as.formula(paste0(
  "log(n_trips) ~ O3_10 + ", weather_controls, " + ", pollutant_controls,
  " | yearmonth + dow"
))

# Run regressions ----

results <- list()

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- daily_wd[year >= yr_range[1] & year <= yr_range[2]]
  dt <- dt[!is.na(O3_10) & !is.na(mean_speed) & !is.na(temp_mean)]

  cat("=== Between-day:", pname, "(", yr_range[1], "-", yr_range[2], ") ===\n")
  cat("  Observations:", nrow(dt), "\n")

  m_speed <- fixest::feols(fml_speed, data = dt, weights = ~n_trips,
                            cluster = ~date, lean = TRUE)
  cat("  Speed coef on O3_10:", coef(m_speed)["O3_10"], "\n")

  m_dist <- fixest::feols(fml_dist, data = dt, weights = ~n_trips,
                           cluster = ~date, lean = TRUE)
  cat("  Distance coef on O3_10:", coef(m_dist)["O3_10"], "\n")

  m_trips <- fixest::feols(fml_trips, data = dt, weights = ~n_trips,
                            cluster = ~date, lean = TRUE)
  cat("  log(trips) coef on O3_10:", coef(m_trips)["O3_10"], "\n")

  results[[paste0("speed_", pname)]] <- m_speed
  results[[paste0("dist_", pname)]] <- m_dist
  results[[paste0("trips_", pname)]] <- m_trips
}

# Output Table 2 (between-day columns) ----

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

writeLines(etable_all, file.path(tab_dir, "table2_between_day_combined.tex"))

cat("\nBetween-day estimation complete.\n")
