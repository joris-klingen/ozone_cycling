# Download weather data 2018-2025 from Open-Meteo archive API ----

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

# Read existing weather data
wx_path <- file.path(out_dir, "weather.parquet")
if (file.exists(wx_path)) {
  existing <- as.data.table(read_parquet(wx_path))
  cat("Existing weather data:", nrow(existing), "rows\n")
} else {
  existing <- NULL
}

years <- 2018:2025
new_data <- list()

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
  tryCatch({
    json <- jsonlite::fromJSON(url)
    dt <- as.data.table(json$hourly)
    dt[, time := as.POSIXct(time, format = "%Y-%m-%dT%H:%M", tz = "Europe/London")]
    new_data[[as.character(yr)]] <- dt
    cat("  ", yr, ":", nrow(dt), "hourly obs\n")
  }, error = function(e) {
    cat("  ERROR:", yr, "-", conditionMessage(e), "\n")
  })

  Sys.sleep(0.5)
}

weather_new <- rbindlist(new_data)

# Convert to GMT
weather_new[, datetime := lubridate::with_tz(time, tzone = "GMT")]
weather_new[, time := NULL]
setcolorder(weather_new, "datetime")

# Append to existing
if (!is.null(existing)) {
  combined <- rbindlist(list(existing, weather_new), fill = TRUE)
  combined <- unique(combined, by = "datetime")
  setorder(combined, datetime)
} else {
  combined <- weather_new
}

write_parquet(combined, wx_path)
cat("Saved", nrow(combined), "total hourly weather observations.\n")
