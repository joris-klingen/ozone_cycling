# Download weather data from Open-Meteo archive API ----

source("R/00_packages.R")

out_dir <- "data/raw/weather"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Central London coordinates
lat <- 51.52
lon <- -0.11

hourly_vars <- paste0(
  "temperature_2m,relative_humidity_2m,rain,",
  "wind_speed_10m,wind_direction_10m,surface_pressure,",
  "shortwave_radiation"
)

years <- 2013:2017
all_data <- list()

for (yr in years) {
  start_date <- paste0(yr, "-01-01")
  end_date <- paste0(yr, "-12-31")

  url <- paste0(
    "https://archive-api.open-meteo.com/v1/archive",
    "?latitude=", lat,
    "&longitude=", lon,
    "&start_date=", start_date,
    "&end_date=", end_date,
    "&hourly=", hourly_vars,
    "&timezone=Europe/London"
  )

  cat("Downloading weather for", yr, "...\n")
  json <- jsonlite::fromJSON(url)

  dt <- as.data.table(json$hourly)
  dt[, time := as.POSIXct(time, format = "%Y-%m-%dT%H:%M", tz = "Europe/London")]

  all_data[[as.character(yr)]] <- dt
  cat("  ", yr, ":", nrow(dt), "hourly obs\n")

  Sys.sleep(0.5)
}

weather <- rbindlist(all_data)

# Convert to GMT to match air quality data
weather[, datetime := lubridate::with_tz(time, tzone = "GMT")]
weather[, time := NULL]

# Reorder columns
setcolorder(weather, "datetime")

write_parquet(weather, file.path(out_dir, "weather.parquet"))
cat("Saved", nrow(weather), "hourly weather observations.\n")
