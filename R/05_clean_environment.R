# Clean and merge air quality + weather data ----

source("R/00_packages.R")

aq_dir <- "data/raw/airquality"
wx_dir <- "data/raw/weather"
out_dir <- "data/processed"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Read air quality data ----

sites <- c("BL0", "KC1", "RI2", "SK6", "TH4")

aq_list <- lapply(sites, function(s) {
  f <- file.path(aq_dir, paste0(s, ".parquet"))
  if (!file.exists(f)) {
    cat("Missing:", f, "\n")
    return(NULL)
  }
  dt <- as.data.table(read_parquet(f))
  dt[, site := s]
  dt
})
aq_list <- aq_list[!sapply(aq_list, is.null)]
aq <- rbindlist(aq_list, fill = TRUE)

cat("Air quality data:", nrow(aq), "rows from", length(aq_list), "sites\n")

# Convert concentrations from ug/m3 to ppb ----
# Molecular weights: O3=48, NO2=46, NO=30, SO2=64
# ppb = (ug/m3) * 24.45 / MW  (at 25C, 1atm)
mw <- c(O3 = 48, NO2 = 46, NO = 30, SO2 = 64)

for (sp in names(mw)) {
  if (sp %in% names(aq)) {
    aq[, (sp) := get(sp) * 24.45 / mw[sp]]
  }
}
# PM2.5 stays in ug/m3 (convert to ppb-equivalent 10 ug/m3 bins later)
# But for consistency with paper's "10 ppb dummies", we keep PM25 as is

# Compute NOx = NO + NO2 (both now in ppb)
if (all(c("NO", "NO2") %in% names(aq))) {
  aq[, NOx := fifelse(is.na(NO) & is.na(NO2), NA_real_,
                       fifelse(is.na(NO), NO2,
                               fifelse(is.na(NO2), NO, NO + NO2)))]
}

# City-average air quality (equal weight across stations) ----

aq_avg <- aq[, lapply(.SD, mean, na.rm = TRUE),
             by = .(datetime),
             .SDcols = setdiff(names(aq), c("datetime", "site"))]

# Replace NaN with NA (from all-NA averages)
for (col in names(aq_avg)) {
  if (is.numeric(aq_avg[[col]])) {
    aq_avg[is.nan(get(col)), (col) := NA_real_]
  }
}

cat("City-average AQ:", nrow(aq_avg), "hourly obs\n")

# Read weather data ----

weather <- as.data.table(read_parquet(file.path(wx_dir, "weather.parquet")))
cat("Weather data:", nrow(weather), "hourly obs\n")

# Merge air quality and weather ----

# Round datetimes to hour for merging
aq_avg[, datetime := as.POSIXct(round(as.numeric(datetime) / 3600) * 3600,
                                 origin = "1970-01-01", tz = "GMT")]
weather[, datetime := as.POSIXct(round(as.numeric(datetime) / 3600) * 3600,
                                  origin = "1970-01-01", tz = "GMT")]

env <- merge(aq_avg, weather, by = "datetime", all = TRUE)
cat("Merged environment data:", nrow(env), "hourly obs\n")

# Create dummy indicators ----

# Temperature: 2.5 degree C bins
env[, temp_bin := floor(temperature_2m / 2.5) * 2.5]

# Solar radiation: 100 W/m2 bins
env[, radiation_bin := floor(shortwave_radiation / 100) * 100]

# NOx: 10 ppb bins
if ("NOx" %in% names(env)) {
  env[, nox_bin := floor(NOx / 10) * 10]
}

# PM2.5: 10 ug/m3 bins (paper uses "10 ppb dummies" but PM2.5 is in ug/m3)
if ("PM25" %in% names(env)) {
  env[, pm25_bin := floor(PM25 / 10) * 10]
}

# Compute 8-hour lags of temperature and rain ----

setorder(env, datetime)
for (lag_h in 1:8) {
  lag_name_temp <- paste0("temp_lag", lag_h)
  lag_name_rain <- paste0("rain_lag", lag_h)
  env[, (lag_name_temp) := shift(temperature_2m, n = lag_h, type = "lag")]
  env[, (lag_name_rain) := shift(rain, n = lag_h, type = "lag")]
}

# Add time variables ----

env[, `:=`(
  date = as.Date(datetime),
  hour = hour(datetime),
  dow = wday(datetime),
  month = month(datetime),
  year = year(datetime)
)]

# Save hourly data ----

write_parquet(env, file.path(out_dir, "environment_hourly.parquet"))
cat("Saved hourly environment data:", nrow(env), "rows\n")

# Create daily aggregates ----

daily_env <- env[, .(
  O3 = mean(O3, na.rm = TRUE),
  NO2 = mean(NO2, na.rm = TRUE),
  NOx = if ("NOx" %in% names(.SD)) mean(NOx, na.rm = TRUE) else NA_real_,
  PM25 = if ("PM25" %in% names(.SD)) mean(PM25, na.rm = TRUE) else NA_real_,
  SO2 = if ("SO2" %in% names(.SD)) mean(SO2, na.rm = TRUE) else NA_real_,
  temp_mean = mean(temperature_2m, na.rm = TRUE),
  temp_max = max(temperature_2m, na.rm = TRUE),
  temp_min = min(temperature_2m, na.rm = TRUE),
  humidity = mean(relative_humidity_2m, na.rm = TRUE),
  rain_total = sum(rain, na.rm = TRUE),
  wind_speed = mean(wind_speed_10m, na.rm = TRUE),
  pressure = mean(surface_pressure, na.rm = TRUE),
  radiation = mean(shortwave_radiation, na.rm = TRUE)
), by = .(date)]

daily_env[, `:=`(
  dow = wday(date),
  month = month(date),
  year = year(date)
)]

# Replace NaN/Inf
for (col in names(daily_env)) {
  if (is.numeric(daily_env[[col]])) {
    daily_env[is.nan(get(col)) | is.infinite(get(col)), (col) := NA_real_]
  }
}

write_parquet(daily_env, file.path(out_dir, "environment_daily.parquet"))
cat("Saved daily environment data:", nrow(daily_env), "rows\n")

# Save per-site air quality (needed for spatial analysis) ----

write_parquet(aq, file.path(out_dir, "airquality_by_site.parquet"))
cat("Saved per-site air quality:", nrow(aq), "rows\n")
