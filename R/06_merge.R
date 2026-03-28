# Merge cycling and environment data into analysis datasets ----

source("R/00_packages.R")

proc_dir <- "data/processed"

# Read environment data ----

env_h <- as.data.table(read_parquet(file.path(proc_dir, "environment_hourly.parquet")))
env_d <- as.data.table(read_parquet(file.path(proc_dir, "environment_daily.parquet")))
aq_site <- as.data.table(read_parquet(file.path(proc_dir, "airquality_by_site.parquet")))

cat("Hourly env:", nrow(env_h), "\n")
cat("Daily env:", nrow(env_d), "\n")

# Monitor locations ----

monitors <- data.table(
  site = c("BL0", "KC1", "RI2", "SK6", "TH4"),
  lat  = c(51.4946, 51.4955, 51.4568, 51.5081, 51.5224),
  lon  = c(-0.1518, -0.2133, -0.1749, -0.0165, -0.0178)
)

haversine <- function(lat1, lon1, lat2, lon2) {
  R <- 6371
  dlat <- (lat2 - lat1) * pi / 180
  dlon <- (lon2 - lon1) * pi / 180
  a <- sin(dlat / 2)^2 + cos(lat1 * pi / 180) * cos(lat2 * pi / 180) * sin(dlon / 2)^2
  2 * R * asin(sqrt(a))
}

# Build station-zone lookup from all trip files ----

trip_files <- sort(list.files(proc_dir, pattern = "^trips_\\d{4}\\.parquet$", full.names = TRUE))

cat("Building station-zone lookup...\n")
station_list <- list()
for (f in trip_files) {
  dt <- as.data.table(read_parquet(f,
    col_select = c("start_station_id", "start_lat", "start_lon",
                    "end_station_id", "end_lat", "end_lon")))
  s1 <- unique(dt[, .(station_id = start_station_id, lat = start_lat, lon = start_lon)])
  s2 <- unique(dt[, .(station_id = end_station_id, lat = end_lat, lon = end_lon)])
  station_list[[length(station_list) + 1L]] <- rbind(s1, s2)
  rm(dt, s1, s2); gc(verbose = FALSE)
}
stations <- unique(rbindlist(station_list))
stations <- stations[, .(lat = lat[1], lon = lon[1]), by = station_id]
rm(station_list); gc(verbose = FALSE)
cat("Unique docking stations:", nrow(stations), "\n")

# Assign each station to nearest monitor within 2km
stations[, zone := NA_character_]
for (i in seq_len(nrow(monitors))) {
  d <- haversine(stations$lat, stations$lon, monitors$lat[i], monitors$lon[i])
  in_range <- d <= 2 & is.na(stations$zone)
  stations[in_range, zone := monitors$site[i]]
}

station_zones <- stations[!is.na(zone), .(station_id, zone)]
setkey(station_zones, station_id)
cat("Stations within 2km of a monitor:", nrow(station_zones), "\n")

# Stream trip files: aggregate to hourly (city + zone) ----

cat("Streaming trip files...\n")
hourly_chunks <- list()
zone_chunks <- list()
total_trips <- 0L
sum_speed_all <- 0
sum_dist_all <- 0
n_all <- 0L

for (f in trip_files) {
  yr <- sub(".*trips_(\\d{4})\\.parquet$", "\\1", f)
  cat("  Processing", yr, "...")
  dt <- as.data.table(read_parquet(f))
  cat(nrow(dt), "trips\n")
  total_trips <- total_trips + nrow(dt)

  # Running totals for summary
  sum_speed_all <- sum_speed_all + sum(dt$speed_kmh, na.rm = TRUE)
  sum_dist_all <- sum_dist_all + sum(dt$dist_km, na.rm = TRUE)
  n_all <- n_all + sum(!is.na(dt$speed_kmh))

  # City-level hourly aggregation
  h <- dt[, .(
    sum_speed    = sum(speed_kmh, na.rm = TRUE),
    sum_speed_sq = sum(speed_kmh^2, na.rm = TRUE),
    sum_dist     = sum(dist_km, na.rm = TRUE),
    sum_duration = sum(duration_sec, na.rm = TRUE),
    n_trips      = .N,
    n_speed      = sum(!is.na(speed_kmh))
  ), by = .(date, hour)]
  # Also compute median per chunk (no way to combine medians across chunks,
  # but since each date-hour falls in exactly one year, this is exact)
  h_med <- dt[, .(median_speed = median(speed_kmh, na.rm = TRUE)), by = .(date, hour)]
  h <- merge(h, h_med, by = c("date", "hour"))
  hourly_chunks[[length(hourly_chunks) + 1L]] <- h


  # Zone-level aggregation
  dt[station_zones, start_zone := i.zone, on = .(start_station_id = station_id)]
  dt[station_zones, end_zone := i.zone, on = .(end_station_id = station_id)]
  dt[, zone := fifelse(start_zone == end_zone & !is.na(start_zone), start_zone, NA_character_)]
  zoned <- dt[!is.na(zone)]
  if (nrow(zoned) > 0) {
    z <- zoned[, .(
      sum_speed = sum(speed_kmh, na.rm = TRUE),
      n_trips   = .N,
      sum_dist  = sum(dist_km, na.rm = TRUE)
    ), by = .(zone, date, hour)]
    zone_chunks[[length(zone_chunks) + 1L]] <- z
  }

  rm(dt, h, h_med, zoned); gc(verbose = FALSE)
}

cat("Total trips:", total_trips, "\n")

# Combine hourly chunks ----

hourly_trips <- rbindlist(hourly_chunks)
rm(hourly_chunks)

# Since each (date, hour) appears in exactly one year file, no need to re-aggregate
hourly_trips[, `:=`(
  mean_speed    = sum_speed / n_speed,
  sd_speed      = sqrt(pmax(0, (sum_speed_sq / n_speed) - (sum_speed / n_speed)^2)),
  mean_dist     = sum_dist / n_trips,
  mean_duration = sum_duration / n_trips
)]

hourly <- merge(hourly_trips, env_h, by = c("date", "hour"), all.x = TRUE)

# Compute city-level cycling volume (total distance, log scale) ----
# Used as control for bicycle traffic / congestion, following original paper
hourly[, log_sum_dist := log(sum_dist + 1)]

cat("Hourly analysis dataset:", nrow(hourly), "obs\n")
write_parquet(hourly, file.path(proc_dir, "analysis_hourly.parquet"))

# Daily analysis dataset ----

daily_trips <- hourly_trips[, .(
  sum_speed    = sum(sum_speed),
  sum_speed_sq = sum(sum_speed_sq),
  sum_dist     = sum(sum_dist),
  sum_duration = sum(sum_duration),
  n_trips      = sum(n_trips),
  n_speed      = sum(n_speed)
), by = .(date)]

daily_trips[, `:=`(
  mean_speed    = sum_speed / n_speed,
  median_speed  = NA_real_,
  sd_speed      = sqrt(pmax(0, (sum_speed_sq / n_speed) - (sum_speed / n_speed)^2)),
  mean_dist     = sum_dist / n_trips,
  mean_duration = sum_duration / n_trips
)]

daily <- merge(daily_trips, env_d, by = "date", all.x = TRUE)

# Daily cycling volume
daily[, log_sum_dist := log(sum_dist + 1)]

cat("Daily analysis dataset:", nrow(daily), "obs\n")
write_parquet(daily, file.path(proc_dir, "analysis_daily.parquet"))

# Zone-hourly analysis dataset ----

zone_hourly <- rbindlist(zone_chunks)
rm(zone_chunks)

# Again, each (zone, date, hour) is unique across year files
zone_hourly[, `:=`(
  mean_speed = sum_speed / n_trips,
  mean_dist  = sum_dist / n_trips
)]

# Merge with site-specific air quality
aq_site[, `:=`(
  date = as.Date(datetime),
  hour = hour(datetime)
)]

zone_hourly <- merge(zone_hourly, aq_site,
                     by.x = c("zone", "date", "hour"),
                     by.y = c("site", "date", "hour"),
                     all.x = TRUE)

# Also merge city-level weather
wx_cols <- c("date", "hour", "temperature_2m", "relative_humidity_2m", "rain",
             "wind_speed_10m", "wind_direction_10m", "surface_pressure",
             "shortwave_radiation", "temp_bin", "radiation_bin")
wx_cols <- intersect(wx_cols, names(env_h))
zone_hourly <- merge(zone_hourly, env_h[, ..wx_cols],
                     by = c("date", "hour"), all.x = TRUE)

cat("Zone-hourly analysis dataset:", nrow(zone_hourly), "obs\n")
write_parquet(zone_hourly, file.path(proc_dir, "analysis_zone_hourly.parquet"))

# Summary ----

cat("\n=== Summary ===\n")
cat("Total trips:", total_trips, "\n")
cat("Mean speed:", round(sum_speed_all / n_all, 2), "km/h\n")
cat("Mean distance:", round(sum_dist_all / total_trips, 2), "km\n")
cat("Hourly obs:", nrow(hourly), "\n")
cat("Daily obs:", nrow(daily), "\n")
cat("Zone-hourly obs:", nrow(zone_hourly), "\n")

if ("O3" %in% names(hourly)) {
  cat("Ozone range:", round(min(hourly$O3, na.rm = TRUE), 1), "-",
      round(max(hourly$O3, na.rm = TRUE), 1), "ppb\n")
}
