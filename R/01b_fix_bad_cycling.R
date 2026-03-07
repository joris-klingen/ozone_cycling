# Fix badly-parsed cycling CSV files ----
# Some CSVs have quoted commas in station names that fread mishandled.
# This script re-downloads and re-parses them with proper quoting.

source("R/00_packages.R")

out_dir <- "data/raw/cycling"
tmp_dir <- tempdir()
bucket_url <- "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"

# Find bad parquet files ----

all_files <- list.files(out_dir, pattern = "\\.parquet$", full.names = TRUE)
all_files <- all_files[!grepl("stations", all_files)]

bad_files <- character(0)
for (f in all_files) {
  schema <- arrow::read_parquet(f, as_data_frame = FALSE)
  if (!"Rental Id" %in% names(schema)) {
    bad_files <- c(bad_files, f)
  }
}
cat("Found", length(bad_files), "bad files to fix.\n")

# Re-download and fix zipped files (2013-2014) ----

# These came from zips, need special handling
zip_files_2013 <- bad_files[grepl("Journey Data Extract.*1[34]\\.", bad_files)]
if (length(zip_files_2013) > 0) {
  cat("Fixing", length(zip_files_2013), "files from 2013-2014 zips...\n")

  for (yr in c(2013, 2014)) {
    zip_key <- paste0("usage-stats/cyclehireusagestats-", yr, ".zip")
    url <- paste0(bucket_url, "/", zip_key)
    zip_file <- file.path(tmp_dir, basename(zip_key))

    # Check if any bad files are from this year
    yr_short <- substr(as.character(yr), 3, 4)
    yr_bad <- bad_files[grepl(paste0(yr_short, "\\.parquet$"), bad_files)]
    if (length(yr_bad) == 0) next

    cat("  Downloading", basename(zip_key), "...\n")
    curl::curl_download(url, zip_file)

    csv_files <- unzip(zip_file, list = TRUE)$Name
    csv_files <- csv_files[grepl("\\.csv$", csv_files, ignore.case = TRUE)]
    unzip(zip_file, files = csv_files, exdir = tmp_dir)

    for (csv_f in csv_files) {
      parquet_name <- gsub("\\.csv$", ".parquet", basename(csv_f), ignore.case = TRUE)
      parquet_path <- file.path(out_dir, parquet_name)

      # Only fix if it's in our bad list
      if (!parquet_path %in% bad_files) next

      csv_path <- file.path(tmp_dir, csv_f)
      dt <- fread(csv_path, sep = ",", quote = "\"", fill = TRUE)
      names(dt) <- trimws(names(dt))

      # Write to temp first, then move (avoids Dropbox lock)
      tmp_parquet <- file.path(tmp_dir, parquet_name)
      write_parquet(dt, tmp_parquet)

      # Remove old file and copy new
      tryCatch({
        if (file.exists(parquet_path)) file.remove(parquet_path)
        file.copy(tmp_parquet, parquet_path)
        file.remove(tmp_parquet)
        cat("  Fixed", parquet_name, "-", nrow(dt), "rows\n")
      }, error = function(e) {
        cat("  ERROR moving", parquet_name, ":", conditionMessage(e), "\n")
      })

      file.remove(csv_path)
    }
    file.remove(zip_file)
  }
}

# Re-download individual CSVs ----

csv_bad <- bad_files[!bad_files %in% zip_files_2013]
cat("Fixing", length(csv_bad), "individual CSV files...\n")

# We need the S3 key for each file. Reconstruct from filename.
for (f in csv_bad) {
  parquet_name <- basename(f)
  csv_name <- gsub("\\.parquet$", ".csv", parquet_name, ignore.case = TRUE)
  key <- paste0("usage-stats/", csv_name)
  url <- paste0(bucket_url, "/", URLencode(key, reserved = TRUE))

  csv_file <- file.path(tmp_dir, csv_name)

  tryCatch({
    curl::curl_download(url, csv_file)
    dt <- fread(csv_file, sep = ",", quote = "\"", fill = TRUE)
    names(dt) <- trimws(names(dt))

    tmp_parquet <- file.path(tmp_dir, parquet_name)
    write_parquet(dt, tmp_parquet)

    if (file.exists(f)) file.remove(f)
    file.copy(tmp_parquet, f)
    file.remove(tmp_parquet)

    cat("  Fixed", parquet_name, "-", nrow(dt), "rows\n")
    file.remove(csv_file)
  }, error = function(e) {
    cat("  ERROR:", parquet_name, "-", conditionMessage(e), "\n")
  })
}

# Verify ----

still_bad <- 0
for (f in bad_files) {
  if (file.exists(f)) {
    schema <- arrow::read_parquet(f, as_data_frame = FALSE)
    if (!"Rental Id" %in% names(schema)) still_bad <- still_bad + 1
  }
}
cat("\nDone. Still bad:", still_bad, "files.\n")
