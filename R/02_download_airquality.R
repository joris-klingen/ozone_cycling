# Download air quality data from London Air API ----

source("R/00_packages.R")

out_dir <- "data/raw/airquality"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

sites <- c("BL0", "KC1", "RI2", "SK6", "TH4")
species <- c("O3", "NO2", "NO", "PM25", "SO2")
years <- 2013:2017

base_url <- "https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies"

# Download data per site ----

for (site in sites) {
  cat("Downloading data for site", site, "...\n")
  site_data <- list()

  for (sp in species) {
    for (yr in years) {
      start_date <- paste0(yr, "-01-01")
      end_date <- paste0(yr, "-12-31")

      url <- paste0(
        base_url,
        "/SiteCode=", site,
        "/SpeciesCode=", sp,
        "/StartDate=", start_date,
        "/EndDate=", end_date,
        "/Json"
      )

      tryCatch({
        json <- jsonlite::fromJSON(url)

        if (is.null(json$RawAQData$Data) || length(json$RawAQData$Data) == 0) {
          cat("  No data for", site, sp, yr, "\n")
          next
        }

        dt <- as.data.table(json$RawAQData$Data)

        if (nrow(dt) == 0 || !("@MeasurementDateGMT" %in% names(dt))) next

        dt[, datetime := as.POSIXct(`@MeasurementDateGMT`, format = "%Y-%m-%d %H:%M:%S", tz = "GMT")]
        dt[, value := as.numeric(`@Value`)]
        dt[, species := sp]
        dt <- dt[, .(datetime, species, value)]

        site_data[[paste(sp, yr)]] <- dt
        cat("  ", site, sp, yr, ":", nrow(dt), "obs\n")

      }, error = function(e) {
        cat("  ERROR:", site, sp, yr, "-", conditionMessage(e), "\n")
      })

      Sys.sleep(0.2)  # rate limit
    }
  }

  if (length(site_data) == 0) {
    cat("  No data for site", site, "\n")
    next
  }

  dt_site <- rbindlist(site_data)

  # Pivot to wide format: one column per species
  dt_wide <- dcast(dt_site, datetime ~ species, value.var = "value", fun.aggregate = mean)
  dt_wide[, site := site]

  write_parquet(dt_wide, file.path(out_dir, paste0(site, ".parquet")))
  cat("  Saved", site, "-", nrow(dt_wide), "hourly obs\n")
}

cat("Done downloading air quality data.\n")
