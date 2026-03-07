# Clean cycling trip data — process file-by-file, output year parquets ----

TEST_MODE <- FALSE  # Set TRUE to process only 10 files for debugging

source("R/00_packages.R")

raw_dir <- "data/raw/cycling"
out_dir <- "data/processed"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Helper functions ----

name_map <- c(
  "rental_id" = "rental_id", "rental.id" = "rental_id",
  "rentalid" = "rental_id", "number" = "rental_id",
  "duration" = "duration", "duration_seconds" = "duration",
  "duration_(seconds)" = "duration", "total_duration" = "duration",
  "total_duration_(ms)" = "duration_ms",
  "end_date" = "end_date", "enddate" = "end_date", "end.date" = "end_date",
  "end_station_id" = "end_station_id", "endstation_id" = "end_station_id",
  "endstationid" = "end_station_id", "endstation.id" = "end_station_id",
  "end_station_number" = "end_station_id",
  "endstation_logical_terminal" = "end_station_id",
  "end_station_name" = "end_station_name", "endstation_name" = "end_station_name",
  "endstationname" = "end_station_name", "endstation.name" = "end_station_name",
  "end_station" = "end_station_name",
  "start_date" = "start_date", "startdate" = "start_date", "start.date" = "start_date",
  "start_station_id" = "start_station_id", "startstation_id" = "start_station_id",
  "startstationid" = "start_station_id", "startstation.id" = "start_station_id",
  "start_station_number" = "start_station_id",
  "startstation_logical_terminal" = "start_station_id",
  "start_station_name" = "start_station_name", "startstation_name" = "start_station_name",
  "startstationname" = "start_station_name", "startstation.name" = "start_station_name",
  "start_station" = "start_station_name"
)

desired_cols <- c("rental_id", "duration", "duration_ms",
                  "start_date", "end_date",
                  "start_station_id", "end_station_id")

keep_cols <- c("rental_id", "start_station_id", "end_station_id",
               "start_dt", "end_dt", "duration_sec",
               "start_lat", "start_lon", "end_lat", "end_lon",
               "dist_straight_km", "dist_km", "speed_kmh",
               "date", "hour", "dow", "month", "year")

standardize_names <- function(dt) {
  nms <- tolower(names(dt))
  nms <- gsub("\\s+", "_", trimws(nms))
  matched <- match(nms, names(name_map))
  has_match <- !is.na(matched)
  nms[has_match] <- name_map[matched[has_match]]
  setnames(dt, nms)
  dt
}

parse_datetime <- function(x) {
  x <- as.character(x)
  first <- x[!is.na(x)][1]
  if (grepl("^\\d{2}/\\d{2}/\\d{4}", first)) {
    x <- sub("^(\\d{2})/(\\d{2})/(\\d{4})", "\\3-\\2-\\1", x)
  }
  short <- which(nchar(x) == 16L)
  if (length(short)) x[short] <- paste0(x[short], ":00")
  fasttime::fastPOSIXct(x, tz = "GMT")
}

haversine <- function(lat1, lon1, lat2, lon2) {
  R <- 6371
  dlat <- (lat2 - lat1) * pi / 180
  dlon <- (lon2 - lon1) * pi / 180
  a <- sin(dlat / 2)^2 + cos(lat1 * pi / 180) * cos(lat2 * pi / 180) * sin(dlon / 2)^2
  2 * R * asin(sqrt(a))
}

# Read station locations ----

stations <- as.data.table(read_parquet(file.path(raw_dir, "stations.parquet")))
setkey(stations, station_id)

# Process files one by one, accumulate into year buckets ----

trip_files <- list.files(raw_dir, pattern = "\\.parquet$", full.names = TRUE)
trip_files <- trip_files[!grepl("stations", trip_files)]

if (TEST_MODE) {
  set.seed(42)
  trip_files <- sample(trip_files, min(10, length(trip_files)))
  cat("TEST MODE: processing", length(trip_files), "sampled files\n")
}

cat("Processing", length(trip_files), "trip files...\n")

year_buckets <- list()
file_count <- 0L
skip_count <- 0L

# Flush year buckets to disk to manage memory ----
flush_count <- 0L
flush_buckets <- function() {
  flush_count <<- flush_count + 1L
  for (yr_key in names(year_buckets)) {
    chunk <- rbindlist(year_buckets[[yr_key]], fill = TRUE)
    out_path <- file.path(out_dir, sprintf("trips_%s_part%03d.parquet", yr_key, flush_count))
    write_parquet(chunk, out_path)
  }
  year_buckets <<- list()
  gc(verbose = FALSE)
}

for (f in trip_files) {
  tryCatch({
    dt <- tryCatch(as.data.table(read_parquet(f)), error = function(e) NULL)
    if (is.null(dt)) next

    # Check for recognized columns
    if (!"Rental Id" %in% names(dt) && !"Number" %in% names(dt)) {
      skip_count <- skip_count + 1L
      next
    }

    # Standardize and drop unneeded columns in-place
    dt <- standardize_names(dt)
    drop_cols <- setdiff(names(dt), desired_cols)
    if (length(drop_cols)) dt[, (drop_cols) := NULL]

    # Parse dates
    dt[, `:=`(start_dt = parse_datetime(start_date),
              end_dt   = parse_datetime(end_date))]
    dt[, c("start_date", "end_date") := NULL]

    # Drop rows with unparseable dates
    dt <- dt[!is.na(start_dt)]
    if (nrow(dt) == 0L) next

    # Duration
    if ("duration_ms" %in% names(dt)) {
      dt[, duration := as.numeric(duration_ms) / 1000]
      dt[, duration_ms := NULL]
    }
    if ("duration" %in% names(dt)) {
      dt[, duration_sec := as.numeric(duration)]
      dt[, duration := NULL]
    } else {
      dt[, duration_sec := as.numeric(difftime(end_dt, start_dt, units = "secs"))]
    }

    # Station IDs — skip file if missing
    if (!all(c("start_station_id", "end_station_id") %in% names(dt))) {
      cat("  Skipping", basename(f), "- missing station ID columns\n")
      skip_count <- skip_count + 1L
      next
    }
    dt[, `:=`(start_station_id = as.integer(start_station_id),
              end_station_id   = as.integer(end_station_id))]

    # Merge station coordinates (keyed join)
    dt[stations, `:=`(start_lat = i.lat, start_lon = i.lon), on = .(start_station_id = station_id)]
    dt[stations, `:=`(end_lat = i.lat, end_lon = i.lon), on = .(end_station_id = station_id)]

    # Distance and speed (combined)
    dt[, dist_straight_km := haversine(start_lat, start_lon, end_lat, end_lon)]
    dt[, `:=`(dist_km  = dist_straight_km * 1.3,
              speed_kmh = dist_straight_km * 1.3 / (duration_sec / 3600))]

    # Filter
    dt <- dt[!is.na(speed_kmh) & speed_kmh > 0.1 & speed_kmh <= 30 &
               dist_km > 0 & duration_sec > 60]
    if (nrow(dt) == 0L) next

    # Time variables (combined)
    dt[, `:=`(date  = as.Date(start_dt),
              hour  = hour(start_dt),
              dow   = wday(start_dt),
              month = month(start_dt),
              year  = year(start_dt))]

    # Keep only final columns
    out_cols <- intersect(keep_cols, names(dt))
    drop_final <- setdiff(names(dt), out_cols)
    if (length(drop_final)) dt[, (drop_final) := NULL]

    # Append chunks to year bucket lists
    for (y in unique(dt$year)) {
      yr_key <- as.character(y)
      if (is.null(year_buckets[[yr_key]])) year_buckets[[yr_key]] <- list()
      year_buckets[[yr_key]][[length(year_buckets[[yr_key]]) + 1L]] <- dt[year == y]
    }

    file_count <- file_count + 1L
    if (file_count %% 5L == 0L) {
      mem_mb <- round(sum(sapply(year_buckets, function(bl) sum(sapply(bl, object.size)))) / 1e6, 0)
      cat(sprintf("  [%d/%d files | %d skipped | %dMB buffered]\n",
                  file_count, length(trip_files), skip_count, mem_mb))
    }

    # Flush to disk every 100 files to manage memory
    if (file_count %% 100L == 0L) {
      cat("  Flushing year buckets to disk...\n")
      flush_buckets()
    }
  }, error = function(e) {
    cat("  ERROR in", basename(f), ":", conditionMessage(e), "\n")
    skip_count <<- skip_count + 1L
  })
}

cat("Processed", file_count, "files, skipped", skip_count, ".\n")

# Final flush of remaining buckets ----
if (length(year_buckets) > 0L) flush_buckets()

# Combine per-year parts into final year parquets ----

all_parts <- list.files(out_dir, pattern = "^trips_\\d{4}_part\\d+\\.parquet$", full.names = TRUE)
years <- unique(sub("^trips_(\\d{4})_part.*", "\\1", basename(all_parts)))

total <- 0L
for (yr_key in sort(years)) {
  yr_parts <- all_parts[grepl(paste0("trips_", yr_key, "_part"), all_parts)]
  dt <- rbindlist(lapply(yr_parts, function(p) as.data.table(read_parquet(p))), fill = TRUE)
  out_path <- file.path(out_dir, paste0("trips_", yr_key, ".parquet"))
  write_parquet(dt, out_path)
  cat("  trips_", yr_key, ".parquet: ", nrow(dt), " rows\n", sep = "")
  total <- total + nrow(dt)
  file.remove(yr_parts)
  rm(dt); gc(verbose = FALSE)
}

cat("Total cleaned trips:", total, "\n")
