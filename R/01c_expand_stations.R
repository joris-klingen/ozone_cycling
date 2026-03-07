# Expand stations table with new-format station IDs (2023+) ----
# TfL changed station numbering from 1-888 to 959+/200xxx/300xxx.
# Match new IDs to old station coordinates via station name.

source("R/00_packages.R")

raw_dir <- "data/raw/cycling"

# Read existing stations ----
stations <- as.data.table(read_parquet(file.path(raw_dir, "stations.parquet")))
cat("Existing stations:", nrow(stations), "(IDs", min(stations$station_id), "-", max(stations$station_id), ")\n")

# Build name-to-coords lookup
name_lookup <- stations[, .(lat = lat[1], lon = lon[1]), by = station_name]
setkey(name_lookup, station_name)

# Scan trip files for new-format station IDs ----
trip_files <- list.files(raw_dir, pattern = "\\.parquet$", full.names = TRUE)
trip_files <- trip_files[!grepl("stations", trip_files)]

new_map <- data.table()
cat("Scanning", length(trip_files), "files for new station IDs...\n")

for (f in trip_files) {
  nms <- tryCatch(names(read_parquet(f, as_data_frame = FALSE)), error = function(e) NULL)
  if (is.null(nms)) next
  if (!"Start station number" %in% nms || !"Start station" %in% nms) next

  dt <- tryCatch(
    as.data.table(read_parquet(f,
      col_select = c("Start station number", "Start station",
                      "End station number", "End station"))),
    error = function(e) NULL
  )
  if (is.null(dt)) next

  # Start stations
  start <- unique(dt[, .(station_id = `Start station number`, station_name = `Start station`)])
  # End stations
  end <- unique(dt[, .(station_id = `End station number`, station_name = `End station`)])
  new_map <- rbindlist(list(new_map, start, end))
  new_map <- unique(new_map, by = "station_id")
}

# Keep only IDs not already in stations table
new_map <- new_map[!station_id %in% stations$station_id]
new_map <- new_map[!is.na(station_id) & !is.na(station_name)]
cat("New station IDs found:", nrow(new_map), "\n")

# Match to coordinates via station name ----
new_map[name_lookup, `:=`(lat = i.lat, lon = i.lon), on = "station_name"]

matched <- new_map[!is.na(lat)]
unmatched <- new_map[is.na(lat)]
cat("Matched to coords:", nrow(matched), "\n")
cat("Unmatched:", nrow(unmatched), "\n")
if (nrow(unmatched) > 0) {
  cat("  Examples:", paste(head(unmatched$station_name, 10), collapse = ", "), "\n")
}

# Combine and save ----
expanded <- rbindlist(list(
  stations,
  matched[, .(station_id, station_name, lat, lon)]
), fill = TRUE)

out_path <- file.path(raw_dir, "stations.parquet")
write_parquet(expanded, out_path)
cat("Saved expanded stations:", nrow(expanded), "total\n")
