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

# Shift AQ timestamps forward by 1 hour ----
# London Air Quality Network reports backward-looking hourly averages:
# the value at hour h is the mean over (h-1, h]. Shifting +1h aligns the
# AQ reading with the hour in which cyclists were actually exposed.
# This matches the original paper's approach.
aq_avg[, datetime := datetime + 3600]

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

# Compute daylight indicator ----
# Central London coordinates for sunrise/sunset calculation
sun_dates <- unique(env[, date])
sun_times <- getSunlightTimes(date = sun_dates, lat = 51.52, lon = -0.11,
                               tz = "GMT", keep = c("sunrise", "sunset"))
sun_dt <- as.data.table(sun_times)
sun_dt[, `:=`(
  sunrise_hour = hour(sunrise) + minute(sunrise) / 60,
  sunset_hour  = hour(sunset) + minute(sunset) / 60
)]
env <- merge(env, sun_dt[, .(date, sunrise_hour, sunset_hour)],
             by = "date", all.x = TRUE)
env[, daylight := fifelse(hour >= sunrise_hour & hour < sunset_hour, 1L, 0L)]
env[, c("sunrise_hour", "sunset_hour") := NULL]

# Bank holiday indicator ----
# UK (England & Wales) bank holidays
uk_bank_holidays <- as.Date(c(
  # 2012
  "2012-01-02", "2012-04-06", "2012-04-09", "2012-06-04", "2012-06-05",
  "2012-08-27", "2012-12-25", "2012-12-26",
  # 2013
  "2013-01-01", "2013-03-29", "2013-04-01", "2013-05-06", "2013-05-27",
  "2013-08-26", "2013-12-25", "2013-12-26",
  # 2014
  "2014-01-01", "2014-04-18", "2014-04-21", "2014-05-05", "2014-05-26",
  "2014-08-25", "2014-12-25", "2014-12-26",
  # 2015
  "2015-01-01", "2015-04-03", "2015-04-06", "2015-05-04", "2015-05-25",
  "2015-08-31", "2015-12-25", "2015-12-28",
  # 2016
  "2016-01-01", "2016-03-25", "2016-03-28", "2016-05-02", "2016-05-30",
  "2016-08-29", "2016-12-26", "2016-12-27",
  # 2017
  "2017-01-02", "2017-04-14", "2017-04-17", "2017-05-01", "2017-05-29",
  "2017-08-28", "2017-12-25", "2017-12-26",
  # 2018
  "2018-01-01", "2018-03-30", "2018-04-02", "2018-05-07", "2018-05-28",
  "2018-08-27", "2018-12-25", "2018-12-26",
  # 2019
  "2019-01-01", "2019-04-19", "2019-04-22", "2019-05-06", "2019-05-27",
  "2019-08-26", "2019-12-25", "2019-12-26",
  # 2020
  "2020-01-01", "2020-04-10", "2020-04-13", "2020-05-08", "2020-05-25",
  "2020-08-31", "2020-12-25", "2020-12-28",
  # 2021
  "2021-01-01", "2021-04-02", "2021-04-05", "2021-05-03", "2021-05-31",
  "2021-08-30", "2021-12-27", "2021-12-28",
  # 2022
  "2022-01-03", "2022-04-15", "2022-04-18", "2022-05-02", "2022-06-02",
  "2022-06-03", "2022-08-29", "2022-09-19", "2022-12-26", "2022-12-27",
  # 2023
  "2023-01-02", "2023-04-07", "2023-04-10", "2023-05-01", "2023-05-08",
  "2023-05-29", "2023-08-28", "2023-12-25", "2023-12-26",
  # 2024
  "2024-01-01", "2024-03-29", "2024-04-01", "2024-05-06", "2024-05-27",
  "2024-08-26", "2024-12-25", "2024-12-26",
  # 2025
  "2025-01-01", "2025-04-18", "2025-04-21", "2025-05-05", "2025-05-26",
  "2025-08-25", "2025-12-25", "2025-12-26"
))
env[, bank_holiday := fifelse(date %in% uk_bank_holidays, 1L, 0L)]

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

# Daily weather lags (previous day) ----
setorder(daily_env, date)
daily_env[, temp_lag1d := shift(temp_mean, n = 1, type = "lag")]
daily_env[, rain_lag1d := shift(rain_total, n = 1, type = "lag")]

# Bank holiday on daily level
daily_env[, bank_holiday := fifelse(date %in% uk_bank_holidays, 1L, 0L)]

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
