# Spatio-temporal estimation (Eq. 3) ----

source("R/00_packages.R")

proc_dir <- "data/processed"
tab_dir <- "output/tables"
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

zone_h <- as.data.table(read_parquet(file.path(proc_dir, "analysis_zone_hourly.parquet")))

# Aggregate to zone-day level ----

zone_h[, year := year(date)]
zone_h[, dow := wday(date)]
zone_h[, month := month(date)]
zone_h[, week := isoweek(date)]

zone_daily <- zone_h[, .(
  mean_speed = weighted.mean(mean_speed, n_trips, na.rm = TRUE),
  mean_dist  = weighted.mean(mean_dist, n_trips, na.rm = TRUE),
  n_trips    = sum(n_trips),
  O3         = mean(O3, na.rm = TRUE),
  NOx        = mean(NOx, na.rm = TRUE),
  NO2        = mean(NO2, na.rm = TRUE)
), by = .(zone, date, year, dow, month, week)]

# Replace NaN with NA
for (col in names(zone_daily)) {
  if (is.numeric(zone_daily[[col]])) {
    zone_daily[is.nan(get(col)), (col) := NA_real_]
  }
}

# Prepare variables ----

zone_daily[, O3_10 := O3 / 10]
zone_daily[, nox_bin := floor(NOx / 10) * 10]
zone_daily[, yearmonth := paste0(year, "_", sprintf("%02d", month))]
zone_daily[, yearweek := paste0(year, "_", sprintf("%02d", week))]
zone_daily[, zone_month := paste0(zone, "_", yearmonth)]
zone_daily[, zone_week := paste0(zone, "_", yearweek)]

# Weekdays only
zone_wd <- zone_daily[dow %in% 2:6]

# Define sample periods ----

periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L),
  post2017    = c(2018L, 2025L)
)

# Run regressions ----

results <- list()

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- zone_wd[year >= yr_range[1] & year <= yr_range[2]]
  dt <- dt[!is.na(O3_10) & !is.na(mean_speed)]

  cat("=== Spatio-temporal:", pname, "(", yr_range[1], "-", yr_range[2], ") ===\n")
  cat("  Observations:", nrow(dt), "\n")
  cat("  Zones:", length(unique(dt$zone)), "\n")

  # Spec 1: day + zone + zone-month FE
  m1_speed <- fixest::feols(
    mean_speed ~ O3_10 + factor(nox_bin) | date + zone + zone_month,
    data = dt, weights = ~n_trips, cluster = ~date, lean = TRUE
  )
  cat("  Spec1 speed coef:", coef(m1_speed)["O3_10"], "\n")

  m1_dist <- fixest::feols(
    mean_dist ~ O3_10 + factor(nox_bin) | date + zone + zone_month,
    data = dt, weights = ~n_trips, cluster = ~date, lean = TRUE
  )

  m1_trips <- fixest::feols(
    log(n_trips) ~ O3_10 + factor(nox_bin) | date + zone + zone_month,
    data = dt, weights = ~n_trips, cluster = ~date, lean = TRUE
  )

  # Spec 2: day + zone + zone-week FE
  m2_speed <- fixest::feols(
    mean_speed ~ O3_10 + factor(nox_bin) | date + zone + zone_week,
    data = dt, weights = ~n_trips, cluster = ~date, lean = TRUE
  )
  cat("  Spec2 speed coef:", coef(m2_speed)["O3_10"], "\n")

  m2_dist <- fixest::feols(
    mean_dist ~ O3_10 + factor(nox_bin) | date + zone + zone_week,
    data = dt, weights = ~n_trips, cluster = ~date, lean = TRUE
  )

  m2_trips <- fixest::feols(
    log(n_trips) ~ O3_10 + factor(nox_bin) | date + zone + zone_week,
    data = dt, weights = ~n_trips, cluster = ~date, lean = TRUE
  )

  results[[paste0("s1_speed_", pname)]] <- m1_speed
  results[[paste0("s1_dist_", pname)]]  <- m1_dist
  results[[paste0("s1_trips_", pname)]] <- m1_trips
  results[[paste0("s2_speed_", pname)]] <- m2_speed
  results[[paste0("s2_dist_", pname)]]  <- m2_dist
  results[[paste0("s2_trips_", pname)]] <- m2_trips
}

# Output Table 3 ----

etable_all <- fixest::etable(
  results$s1_speed_replication, results$s2_speed_replication,
  results$s1_speed_extension, results$s2_speed_extension,
  results$s1_speed_post2017, results$s2_speed_post2017,
  keep = "O3_10",
  headers = c("Speed-M(R)", "Speed-W(R)", "Speed-M(E)", "Speed-W(E)",
              "Speed-M(P)", "Speed-W(P)"),
  tex = TRUE
)

writeLines(etable_all, file.path(tab_dir, "table3_spatiotemporal.tex"))

# Sorting tests
etable_sort <- fixest::etable(
  results$s1_dist_replication, results$s1_dist_extension, results$s1_dist_post2017,
  results$s1_trips_replication, results$s1_trips_extension, results$s1_trips_post2017,
  keep = "O3_10",
  headers = c("Dist(R)", "Dist(E)", "Dist(P)",
              "Trips(R)", "Trips(E)", "Trips(P)"),
  tex = TRUE
)

writeLines(etable_sort, file.path(tab_dir, "table3_spatiotemporal_sorting.tex"))

cat("\nSpatio-temporal estimation complete.\n")
