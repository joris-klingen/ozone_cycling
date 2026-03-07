# Cyclist panel estimation (Eq. 4) ----

source("R/00_packages.R")

TEST_RUN <- as.logical(Sys.getenv("TEST_RUN", "FALSE"))
if (TEST_RUN) cat("*** TEST RUN MODE ***\n")

proc_dir <- "data/processed"
tab_dir <- "output/tables"
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

# Load panel and environment data ----

panel <- as.data.table(read_parquet(file.path(proc_dir, "panel_trips.parquet")))
env_h <- as.data.table(read_parquet(file.path(proc_dir, "environment_hourly.parquet")))

cat("Panel trips:", nrow(panel), "\n")
cat("Unique pseudo-cyclists:", uniqueN(panel$cyclist_set), "\n")

# Merge environment data ----

panel <- merge(panel, env_h,
               by.x = c("date", "hour"),
               by.y = c("date", "hour"),
               all.x = TRUE, suffixes = c("", "_env"))

rm(env_h); gc(verbose = FALSE)

# Prepare variables ----

panel[, O3_10 := O3 / 10]
panel[, dow := wday(date)]
panel[, yearmonth := paste0(year, "_", sprintf("%02d", month))]

panel[, wind_dir_bin := cut(wind_direction_10m %% 360,
                             breaks = seq(0, 360, 45),
                             include.lowest = TRUE, right = FALSE)]

for (lag_h in 1:8) {
  lag_col <- paste0("temp_lag", lag_h)
  bin_col <- paste0("temp_lag", lag_h, "_bin")
  if (lag_col %in% names(panel)) {
    panel[, (bin_col) := floor(get(lag_col))]
  }
}

# Weekdays only
panel_wd <- panel[dow %in% 2:6]
rm(panel); gc(verbose = FALSE)

# Define sample periods ----

periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L),
  post2017    = c(2018L, 2025L)
)

# Build formula ----

weather_controls <- paste0(
  "factor(temp_bin) + surface_pressure + relative_humidity_2m + rain + ",
  "factor(radiation_bin) + shortwave_radiation:temperature_2m + wind_speed_10m + ",
  "factor(wind_dir_bin) + ",
  paste0("factor(temp_lag", 1:8, "_bin)", collapse = " + "), " + ",
  paste0("rain_lag", 1:8, collapse = " + ")
)

pollutant_controls <- "factor(nox_bin) + PM25 + SO2"

fml <- as.formula(paste0(
  "speed_kmh ~ O3_10 + ", weather_controls, " + ", pollutant_controls,
  " | cyclist_set + dow"
))

# Run regressions ----
# Core specs only: d>=0 t>=4 and d>=2 t>=4 for each period (6 total)

specs <- data.table(min_d = c(0, 2), min_t = c(4, 4))
results <- list()

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt_base <- panel_wd[year >= yr_range[1] & year <= yr_range[2]]
  dt_base <- dt_base[!is.na(O3_10) & !is.na(speed_kmh) & !is.na(temperature_2m)]

  cat("\n=== Cyclist panel:", pname, "(", yr_range[1], "-", yr_range[2], ") ===\n")
  cat("  Base obs:", nrow(dt_base), "\n")

  for (i in seq_len(nrow(specs))) {
    min_d <- specs$min_d[i]
    min_t <- specs$min_t[i]
    dt <- dt_base[dist_km >= min_d]

    set_counts <- dt[, .N, by = cyclist_set]
    keep <- set_counts[N >= min_t, cyclist_set]
    dt <- dt[cyclist_set %in% keep]

    if (nrow(dt) < 100) {
      cat("  d>=", min_d, " t>=", min_t, ": too few obs (", nrow(dt), ")\n")
      next
    }

    m <- tryCatch(
      fixest::feols(fml, data = dt, cluster = ~hour, lean = TRUE),
      error = function(e) {
        cat("  d>=", min_d, " t>=", min_t, ": ERROR -", conditionMessage(e), "\n")
        NULL
      }
    )

    if (!is.null(m)) {
      cat("  d>=", min_d, " t>=", min_t, ": O3_10 =",
          round(coef(m)["O3_10"], 4), " (n=", nrow(dt), ")\n")
      results[[paste0(pname, "_d", min_d, "_t", min_t)]] <- m
    }
  }
}

# Output tables ----

# Combined table: all 6 specs side by side
all_keys <- c("replication_d0_t4", "replication_d2_t4",
              "extension_d0_t4",   "extension_d2_t4",
              "post2017_d0_t4",    "post2017_d2_t4")

models <- results[intersect(all_keys, names(results))]
if (length(models) > 0) {
  tex <- do.call(fixest::etable, c(models, list(keep = "O3_10", tex = TRUE)))
  writeLines(tex, file.path(tab_dir, "table4_panel_combined.tex"))
  cat("Saved: table4_panel_combined.tex\n")

  # Also save per-period for backwards compat with paper
  for (p in c("replication", "extension", "post2017")) {
    pmodels <- results[intersect(paste0(p, c("_d0_t4", "_d2_t4")), names(results))]
    if (length(pmodels) > 0) {
      ptex <- do.call(fixest::etable, c(pmodels, list(keep = "O3_10", tex = TRUE)))
      writeLines(ptex, file.path(tab_dir, paste0("table4_panel_", p, ".tex")))
    }
  }
}

cat("\nCyclist panel estimation complete.\n")
