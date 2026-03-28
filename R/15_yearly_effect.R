# Ozone effect by year ----
# Within-day specification estimated separately per year

source("R/00_packages.R")

fig_dir <- "output/figures"
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

proc_dir <- "data/processed"
hourly <- as.data.table(read_parquet(file.path(proc_dir, "analysis_hourly.parquet")))

# Prepare variables (same as 08_within_day.R) ----

hourly[, O3_10 := O3 / 10]
hourly[, wind_dir_bin := cut(wind_direction_10m %% 360,
                              breaks = seq(0, 360, 45),
                              include.lowest = TRUE, right = FALSE)]
for (lag_h in 1:8) {
  hourly[, paste0("temp_lag", lag_h, "_bin") := floor(get(paste0("temp_lag", lag_h)))]
}
hourly[, dow_hour_daylight := paste0(dow, "_", hour, "_", daylight)]

hourly_wd <- hourly[dow %in% 2:6 & !is.na(O3_10) & !is.na(mean_speed) & !is.na(temperature_2m)]

# Formula ----

wx <- paste0(
  "factor(temp_bin) + surface_pressure + relative_humidity_2m + rain + ",
  "factor(radiation_bin) + shortwave_radiation:temperature_2m + wind_speed_10m + ",
  "factor(wind_dir_bin) + ",
  paste0("factor(temp_lag", 1:8, "_bin)", collapse = " + "), " + ",
  paste0("rain_lag", 1:8, collapse = " + ")
)
poll <- "SO2 + factor(nox_bin) + factor(pm25_bin)"
vol <- "log_sum_dist"

fml <- as.formula(paste0(
  "mean_speed ~ O3_10 + ", wx, " + ", poll, " + ", vol,
  " | date + dow_hour_daylight"
))

# Estimate per year ----

years <- sort(unique(hourly_wd[, year]))
results <- list()

for (yr in years) {
  dt <- hourly_wd[year == yr]
  if (nrow(dt) < 500) next

  m <- tryCatch(
    fixest::feols(fml, data = dt, weights = ~n_trips, cluster = ~date, lean = TRUE),
    error = function(e) NULL
  )

  if (!is.null(m) && "O3_10" %in% names(coef(m))) {
    cf <- coef(m)["O3_10"]
    se <- sqrt(vcov(m)["O3_10", "O3_10"])
    cat(yr, ": O3_10 =", round(cf, 4), " (SE =", round(se, 4), ", n =", nrow(dt), ")\n")
    results[[as.character(yr)]] <- data.table(year = yr, coef = cf, se = se, n = nrow(dt))
  }
}

res <- rbindlist(results)
res[, ci_lo := coef - 1.96 * se]
res[, ci_hi := coef + 1.96 * se]

# Plot ----
# Cap CI display range for readability (2021 has huge SE from COVID)
y_lo <- -0.20
y_hi <- 0.10
res[, ci_lo_cap := pmax(ci_lo, y_lo)]
res[, ci_hi_cap := pmin(ci_hi, y_hi)]

p <- ggplot(res, aes(x = year, y = coef)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray60", linewidth = 0.4) +
  geom_ribbon(aes(ymin = ci_lo_cap, ymax = ci_hi_cap), alpha = 0.15, fill = "#2166ac") +
  geom_line(color = "#2166ac", linewidth = 0.8) +
  geom_point(color = "#2166ac", size = 2.5) +
  scale_x_continuous(breaks = years) +
  coord_cartesian(ylim = c(y_lo, y_hi)) +
  labs(x = NULL,
       y = "Effect on cycling speed (km/h per 10 ppb ozone)") +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 9),
    plot.margin = margin(10, 15, 5, 10)
  )

pdf(file.path(fig_dir, "yearly_ozone_effect.pdf"), width = 8, height = 4)
print(p)
dev.off()

png(file.path(fig_dir, "yearly_ozone_effect.png"), width = 1600, height = 800, res = 200)
print(p)
dev.off()

cat("Saved yearly_ozone_effect.pdf and .png\n")
