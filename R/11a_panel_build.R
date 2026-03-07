# Build cyclist panel dataset ----

source("R/00_packages.R")

TEST_RUN <- as.logical(Sys.getenv("TEST_RUN", "FALSE"))
if (TEST_RUN) cat("*** TEST RUN MODE: using 2013-2014 only ***\n")

proc_dir <- "data/processed"

trip_files <- sort(list.files(proc_dir, pattern = "^trips_\\d{4}\\.parquet$", full.names = TRUE))
if (TEST_RUN) {
  trip_files <- trip_files[grep("201[34]", trip_files)]
}

# Columns needed for panel construction
build_cols <- c("start_station_id", "end_station_id", "start_dt",
                "speed_kmh", "dist_km", "duration_sec", "date", "hour",
                "dow", "month", "year", "rental_id")

# Pass 1: Collect (route, hour, month, date) counts for thin-route filter ----
# Only need 4 integer columns per trip — much lighter than full data

cat("Pass 1: Collecting slot-date counts...\n")
slot_counts_list <- list()

for (f in trip_files) {
  cat("  ", basename(f), "...")
  dt <- as.data.table(read_parquet(f,
    col_select = c("start_station_id", "end_station_id", "hour", "month", "date", "dist_km")))
  dt <- dt[start_station_id != end_station_id & dist_km > 0]
  cat(nrow(dt), "trips\n")

  # Count trips per (route-as-two-ints, hour, month, date)
  chunk <- dt[, .N, by = .(start_station_id, end_station_id, hour, month, date)]
  slot_counts_list[[length(slot_counts_list) + 1L]] <- chunk
  rm(dt, chunk); gc(verbose = FALSE)
}

# Combine across years — a (route, hour, month, date) can only appear in one year
# so N values don't need re-aggregation
slot_counts <- rbindlist(slot_counts_list)
rm(slot_counts_list); gc(verbose = FALSE)
cat("Total slot-date combinations:", nrow(slot_counts), "\n")

# Find bad slots: any (route, hour, month) where some date has N > 1
bad_slots <- unique(slot_counts[N > 1, .(start_station_id, end_station_id, hour, month)])
cat("Slots with duplicates (to exclude):", nrow(bad_slots), "\n")
setkey(bad_slots, start_station_id, end_station_id, hour, month)

# Also count trips per (route, hour, month) across all dates — needed for set_stats
slot_totals <- slot_counts[, .(n_trips = sum(N)), by = .(start_station_id, end_station_id, hour, month)]
rm(slot_counts); gc(verbose = FALSE)

# Pass 2: Load trips, apply thin filter, compute departure sd, apply CDF test ----

cat("Pass 2: Building panel...\n")
panel_list <- list()

for (f in trip_files) {
  cat("  ", basename(f), "...")
  dt <- as.data.table(read_parquet(f, col_select = build_cols))
  dt <- dt[start_station_id != end_station_id & dist_km > 0]

  # Thin-route filter: remove trips in bad slots
  dt[, thin := TRUE]
  if (nrow(bad_slots) > 0) {
    dt[bad_slots, thin := FALSE, on = .(start_station_id, end_station_id, hour, month)]
  }
  dt <- dt[thin == TRUE]
  dt[, thin := NULL]

  if (nrow(dt) == 0) { cat("0 kept\n"); next }

  # Compute departure minute
  dt[, dep_minute := minute(start_dt) + second(start_dt) / 60]

  # Aggregate: sum and sum-of-squares of dep_minute per slot (for streaming sd)
  chunk_stats <- dt[, .(
    n = .N,
    sum_min = sum(dep_minute),
    sum_min2 = sum(dep_minute^2)
  ), by = .(start_station_id, end_station_id, hour, month)]

  cat(nrow(dt), "kept\n")
  panel_list[[length(panel_list) + 1L]] <- list(trips = dt, stats = chunk_stats)
  rm(chunk_stats); gc(verbose = FALSE)
}

# Combine departure-minute sufficient stats across years ----

cat("Computing set statistics...\n")
all_stats <- rbindlist(lapply(panel_list, `[[`, "stats"))
set_stats <- all_stats[, .(
  n = sum(n),
  sum_min = sum(sum_min),
  sum_min2 = sum(sum_min2)
), by = .(start_station_id, end_station_id, hour, month)]
rm(all_stats); gc(verbose = FALSE)

# Compute sd from sufficient statistics: var = (sum_x2 - sum_x^2/n) / (n-1)
set_stats[, var_minute := (sum_min2 - sum_min^2 / n) / (n - 1)]
set_stats[n < 2, var_minute := NA_real_]

# CDF-based threshold: (n-1)*s^2/300 ~ chi-sq(n-1)
set_stats[, chi2_stat := (n - 1) * var_minute / 300]
set_stats[, p_random := pchisq(chi2_stat, df = n - 1)]
set_stats[, keep := p_random < 0.05 & n >= 4]

cat("Candidate sets:", nrow(set_stats), "\n")
cat("Sets passing threshold (p<0.05, n>=4):", sum(set_stats$keep, na.rm = TRUE), "\n")

# Filter: keep only trips in passing sets ----

keep_keys <- set_stats[keep == TRUE, .(start_station_id, end_station_id, hour, month)]
setkey(keep_keys, start_station_id, end_station_id, hour, month)

cat("Filtering trips...\n")
filtered_list <- list()
for (i in seq_along(panel_list)) {
  dt <- panel_list[[i]]$trips
  dt[, cyclist_set := paste0(start_station_id, "_", end_station_id, "_h", hour, "_m", month)]
  # Semi-join on the 4 integer keys
  dt <- dt[keep_keys, on = .(start_station_id, end_station_id, hour, month), nomatch = NULL]
  if (nrow(dt) > 0) filtered_list[[length(filtered_list) + 1L]] <- dt
}
rm(panel_list, keep_keys, set_stats, bad_slots); gc(verbose = FALSE)

panel <- rbindlist(filtered_list, use.names = TRUE)
rm(filtered_list); gc(verbose = FALSE)

# Drop working columns
panel[, dep_minute := NULL]

cat("Panel trips:", nrow(panel), "\n")
cat("Unique pseudo-cyclists:", uniqueN(panel$cyclist_set), "\n")

out_file <- file.path(proc_dir, "panel_trips.parquet")
write_parquet(panel, out_file)
cat("Saved:", out_file, "\n")

rm(panel); gc(verbose = FALSE)
cat("Panel build complete.\n")
