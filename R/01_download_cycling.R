# Download cycling trip data from TfL S3 bucket ----

source("R/00_packages.R")

out_dir <- "data/raw/cycling"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
tmp_dir <- tempdir()

# List all files in the S3 bucket ----

bucket_url <- "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"

list_bucket <- function(prefix = "usage-stats/", marker = NULL) {
  url <- paste0(bucket_url, "?prefix=", prefix)
  if (!is.null(marker)) url <- paste0(url, "&marker=", marker)
  xml_text <- readLines(url, warn = FALSE)
  xml_text <- paste(xml_text, collapse = "")
  # Extract keys using regex
  keys <- regmatches(xml_text, gregexpr("(?<=<Key>)[^<]+", xml_text, perl = TRUE))[[1]]
  truncated <- grepl("<IsTruncated>true</IsTruncated>", xml_text)
  list(keys = keys, truncated = truncated)
}

# Paginate through all keys
all_keys <- character(0)
marker <- NULL
repeat {
  res <- list_bucket(marker = marker)
  all_keys <- c(all_keys, res$keys)
  if (!res$truncated) break
  marker <- res$keys[length(res$keys)]
}

cat("Found", length(all_keys), "total keys in bucket.\n")

# Identify relevant files ----

# Annual zips for 2013-2014
zip_keys <- all_keys[grepl("cyclehireusagestats-201[34]\\.zip$", all_keys, ignore.case = TRUE)]

# Weekly CSVs for 2015-2017 (skip aggregate/summary files)
csv_keys <- all_keys[grepl("\\.csv$", all_keys, ignore.case = TRUE)]
# Keep only files that look like weekly journey data (contain "Journey" or numbered weekly files)
csv_keys <- csv_keys[grepl("JourneyData", csv_keys, ignore.case = TRUE)]
# Filter to 2015-2017 range: files typically named like "01aJourneyDataExtract..." or contain year
# We'll download all JourneyData CSVs and filter by date content later,
# but exclude files clearly outside 2013-2017
cat("Found", length(zip_keys), "annual zips and", length(csv_keys), "CSV files.\n")

# Download and process annual zips (2013-2014) ----

for (key in zip_keys) {
  url <- paste0(bucket_url, "/", curl::curl_escape(key))
  # Simpler: use the key directly since S3 handles it
  url <- paste0(bucket_url, "/", key)
  zip_file <- file.path(tmp_dir, basename(key))

  cat("Downloading", key, "...\n")
  curl::curl_download(url, zip_file)

  # Extract CSVs from zip
  csv_files <- unzip(zip_file, list = TRUE)$Name
  csv_files <- csv_files[grepl("\\.csv$", csv_files, ignore.case = TRUE)]

  unzip(zip_file, files = csv_files, exdir = tmp_dir)

  for (csv_f in csv_files) {
    csv_path <- file.path(tmp_dir, csv_f)
    dt <- fread(csv_path, sep = ",", quote = "\"", fill = TRUE)

    # Standardize column names
    names(dt) <- trimws(names(dt))

    parquet_name <- gsub("\\.csv$", ".parquet", basename(csv_f), ignore.case = TRUE)
    write_parquet(dt, file.path(out_dir, parquet_name))
    cat("  Saved", parquet_name, "-", nrow(dt), "rows\n")

    file.remove(csv_path)
  }
  file.remove(zip_file)
}

# Download individual CSVs ----

for (key in csv_keys) {
  parquet_name <- gsub("\\.csv$", ".parquet", basename(key), ignore.case = TRUE)
  parquet_path <- file.path(out_dir, parquet_name)

  # Skip if already downloaded and valid
  if (file.exists(parquet_path)) {
    existing <- arrow::read_parquet(parquet_path, as_data_frame = FALSE)
    if ("Rental Id" %in% names(existing)) {
      cat("Skipping", basename(key), "(already valid)\n")
      next
    }
    cat("Re-downloading", basename(key), "(bad columns)\n")
  }

  url <- paste0(bucket_url, "/", URLencode(key, reserved = TRUE))
  csv_file <- file.path(tmp_dir, basename(key))

  cat("Downloading", basename(key), "...\n")
  tryCatch({
    curl::curl_download(url, csv_file)
    dt <- fread(csv_file, sep = ",", quote = "\"", fill = TRUE)
    names(dt) <- trimws(names(dt))
    write_parquet(dt, parquet_path)
    cat("  Saved", parquet_name, "-", nrow(dt), "rows\n")
    file.remove(csv_file)
  }, error = function(e) {
    cat("  ERROR:", conditionMessage(e), "\n")
  })
}

# Download station locations from TfL BikePoint API ----

cat("Downloading station locations...\n")
stations_json <- jsonlite::fromJSON("https://api.tfl.gov.uk/BikePoint")
stations <- data.table(
  station_id = as.integer(gsub("BikePoints_", "", stations_json$id)),
  station_name = stations_json$commonName,
  lat = stations_json$lat,
  lon = stations_json$lon
)
write_parquet(stations, file.path(out_dir, "stations.parquet"))
cat("Saved", nrow(stations), "station locations.\n")

cat("Done downloading cycling data.\n")
