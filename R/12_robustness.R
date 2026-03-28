# Robustness checks ----

source("R/00_packages.R")

proc_dir <- "data/processed"
fig_dir <- "output/figures"
tab_dir <- "output/tables"
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

# Load data ----

hourly <- as.data.table(read_parquet(file.path(proc_dir, "analysis_hourly.parquet")))
daily <- as.data.table(read_parquet(file.path(proc_dir, "analysis_daily.parquet")))

# Prepare variables (same as 08/09) ----

hourly[, O3_10 := O3 / 10]
hourly[, wind_dir_bin := cut(wind_direction_10m %% 360,
                              breaks = seq(0, 360, 45),
                              include.lowest = TRUE, right = FALSE)]
for (lag_h in 1:8) {
  hourly[, paste0("temp_lag", lag_h, "_bin") := floor(get(paste0("temp_lag", lag_h)))]
  hourly[, paste0("rain_lag", lag_h, "_bin") := fifelse(get(paste0("rain_lag", lag_h)) > 0, 1L, 0L)]
}
hourly[, dow_hour_daylight := paste0(dow, "_", hour, "_", daylight)]

daily[, O3_10 := O3 / 10]
daily[, temp_bin := floor(temp_mean / 2.5) * 2.5]
daily[, radiation_bin := floor(radiation / 100) * 100]
daily[, nox_bin := floor(NOx / 10) * 10]
daily[, pm25_bin := floor(PM25 / 10) * 10]
daily[, yearmonth := paste0(year, "_", sprintf("%02d", month))]

# Weekdays only
hourly_wd <- hourly[dow %in% 2:6]
daily_wd <- daily[dow %in% 2:6]

# Sample periods
periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L),
  post2017    = c(2018L, 2025L)
)

# Formulas ----

wx_hourly <- paste0(
  "factor(temp_bin) + surface_pressure + relative_humidity_2m + rain + ",
  "factor(radiation_bin) + shortwave_radiation:temperature_2m + wind_speed_10m + ",
  "factor(wind_dir_bin) + ",
  paste0("factor(temp_lag", 1:8, "_bin)", collapse = " + "), " + ",
  paste0("rain_lag", 1:8, collapse = " + ")
)

poll_hourly <- "SO2 + factor(nox_bin) + factor(pm25_bin)"
vol_hourly <- "log_sum_dist"

wx_daily <- "factor(temp_bin) + pressure + humidity + rain_total + factor(radiation_bin) + radiation:temp_mean + wind_speed + temp_lag1d + rain_lag1d"
poll_daily <- "SO2 + factor(nox_bin) + factor(pm25_bin)"
vol_daily <- "log_sum_dist"

# Robustness 1: Median speed as DV ----

cat("=== Robustness: Median speed ===\n")

fml_median_wd <- as.formula(paste0(
  "median_speed ~ O3_10 + ", wx_hourly, " + ", poll_hourly, " + ", vol_hourly,
  " | date + dow_hour_daylight"
))

fml_median_bd <- as.formula(paste0(
  "median_speed ~ O3_10 + ", wx_daily, " + ", poll_daily, " + ", vol_daily,
  " | yearmonth + dow"
))

for (pname in names(periods)) {
  yr_range <- periods[[pname]]

  dt_h <- hourly_wd[year >= yr_range[1] & year <= yr_range[2] &
                      !is.na(O3_10) & !is.na(median_speed) & !is.na(temperature_2m)]
  dt_d <- daily_wd[year >= yr_range[1] & year <= yr_range[2] &
                     !is.na(O3_10) & !is.na(median_speed) & !is.na(temp_mean)]

  m_wd <- fixest::feols(fml_median_wd, data = dt_h, weights = ~n_trips,
                         cluster = ~date, lean = TRUE)
  cat("  ", pname, "within-day median:", round(coef(m_wd)["O3_10"], 4), "\n")

  if (nrow(dt_d) > 0) {
    m_bd <- fixest::feols(fml_median_bd, data = dt_d, weights = ~n_trips,
                           cluster = ~date, lean = TRUE)
    cat("  ", pname, "between-day median:", round(coef(m_bd)["O3_10"], 4), "\n")
  } else {
    cat("  ", pname, "between-day median: skipped (no valid obs)\n")
  }
}

# Robustness 2: Ozone 5ppb dummies -> Figure 7 ----

cat("\n=== Figure 7: Non-linearity (5ppb dummies) ===\n")

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- hourly_wd[year >= yr_range[1] & year <= yr_range[2] &
                    !is.na(O3) & !is.na(mean_speed) & !is.na(temperature_2m)]

  # Create 5ppb bins
  dt[, o3_5ppb := floor(O3 / 5) * 5]

  # Reference category: 15-20 ppb (near threshold)
  dt[, o3_5ppb := relevel(factor(o3_5ppb), ref = as.character(
    floor(median(dt$O3, na.rm = TRUE) / 5) * 5
  ))]

  fml_nonlin <- as.formula(paste0(
    "mean_speed ~ factor(o3_5ppb) + ", wx_hourly, " + ", poll_hourly, " + ", vol_hourly,
    " | date + dow_hour_daylight"
  ))

  m <- fixest::feols(fml_nonlin, data = dt, weights = ~n_trips,
                      cluster = ~date, lean = TRUE)

  # Extract coefficients for o3 dummies
  cf <- coef(m)
  se <- sqrt(diag(vcov(m)))
  o3_idx <- grep("o3_5ppb", names(cf))

  if (length(o3_idx) > 0) {
    plot_dt <- data.table(
      bin = as.numeric(gsub(".*o3_5ppb\\)", "", names(cf)[o3_idx])),
      coef = cf[o3_idx],
      se = se[o3_idx]
    )
    # Add reference category
    ref_bin <- floor(median(dt$O3, na.rm = TRUE) / 5) * 5
    plot_dt <- rbindlist(list(
      plot_dt,
      data.table(bin = ref_bin, coef = 0, se = 0)
    ))
    setorder(plot_dt, bin)

    p <- ggplot(plot_dt, aes(x = bin, y = coef)) +
      geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
      geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se),
                  alpha = 0.2, fill = "steelblue") +
      geom_line(color = "steelblue", linewidth = 1) +
      geom_point(color = "steelblue", size = 2) +
      labs(x = "Ozone (ppb, 5ppb bins)", y = "Effect on speed (km/h)",
           title = paste0("Non-linear ozone effect (", pname, ")")) +
      theme_minimal()

    pdf(file.path(fig_dir, paste0("figure7_", pname, ".pdf")), width = 8, height = 5)
    print(p)
    dev.off()

    cat("  Saved figure7_", pname, ".pdf\n")
  }
}

# Robustness 3: Lag/lead analysis -> Figure 8 ----

cat("\n=== Figure 8: Lag/lead analysis ===\n")

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- hourly_wd[year >= yr_range[1] & year <= yr_range[2] &
                    !is.na(O3) & !is.na(mean_speed) & !is.na(temperature_2m)]

  # Create lags and leads of O3
  setorder(dt, date, hour)
  for (h in 1:8) {
    dt[, paste0("O3_lag", h) := shift(O3, n = h, type = "lag") / 10]
    dt[, paste0("O3_lead", h) := shift(O3, n = h, type = "lead") / 10]
  }

  # Run model with contemporaneous + lags + leads
  lag_lead_vars <- c("O3_10",
                     paste0("O3_lag", 1:8),
                     paste0("O3_lead", 1:8))
  lag_lead_str <- paste(lag_lead_vars, collapse = " + ")

  fml_ll <- as.formula(paste0(
    "mean_speed ~ ", lag_lead_str, " + ", wx_hourly, " + ", poll_hourly, " + ", vol_hourly,
    " | date + dow_hour_daylight"
  ))

  m <- tryCatch(
    fixest::feols(fml_ll, data = dt, weights = ~n_trips,
                   cluster = ~date, lean = TRUE),
    error = function(e) {
      cat("  ERROR:", conditionMessage(e), "\n")
      NULL
    }
  )

  if (!is.null(m)) {
    cf <- coef(m)
    se <- sqrt(diag(vcov(m)))

    plot_dt <- data.table(
      lag = c(0, 1:8, -(1:8)),
      coef = cf[lag_lead_vars],
      se = se[lag_lead_vars]
    )
    setorder(plot_dt, lag)

    p <- ggplot(plot_dt, aes(x = lag, y = coef)) +
      geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
      geom_vline(xintercept = 0, linetype = "dotted", color = "gray70") +
      geom_ribbon(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se),
                  alpha = 0.2, fill = "steelblue") +
      geom_line(color = "steelblue", linewidth = 1) +
      geom_point(color = "steelblue", size = 2) +
      scale_x_continuous(breaks = -8:8) +
      labs(x = "Hours (negative = lead, positive = lag)",
           y = "Effect on speed (km/h per 10ppb)",
           title = paste0("Lag/lead ozone effects (", pname, ")")) +
      theme_minimal()

    pdf(file.path(fig_dir, paste0("figure8_", pname, ".pdf")), width = 10, height = 5)
    print(p)
    dev.off()

    cat("  Saved figure8_", pname, ".pdf\n")
  }
}

# Robustness 4: Heterogeneity by fitness (panel) ----

cat("\n=== Heterogeneity by fitness ===\n")

# Load panel data if available
panel_path <- file.path(proc_dir, "analysis_hourly.parquet")
# This would require the panel from 11_cyclist_panel.R
# For now, use hourly data to show speed tercile heterogeneity

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- hourly_wd[year >= yr_range[1] & year <= yr_range[2] &
                    !is.na(O3_10) & !is.na(mean_speed) & !is.na(temperature_2m)]

  # Monthly average speed terciles
  dt[, month_key := paste0(year, "_", sprintf("%02d", month))]
  monthly_speed <- dt[, .(monthly_avg_speed = weighted.mean(mean_speed, n_trips)), by = month_key]
  cuts <- quantile(monthly_speed$monthly_avg_speed, probs = c(1/3, 2/3))
  monthly_speed[, fitness := fifelse(monthly_avg_speed <= cuts[1], "low",
                                      fifelse(monthly_avg_speed <= cuts[2], "mid", "high"))]
  dt <- merge(dt, monthly_speed[, .(month_key, fitness)], by = "month_key")

  fml_wd <- as.formula(paste0(
    "mean_speed ~ O3_10 + ", wx_hourly, " + ", poll_hourly, " + ", vol_hourly,
    " | date + dow_hour_daylight"
  ))

  for (fit in c("low", "mid", "high")) {
    dt_sub <- dt[fitness == fit]
    m <- tryCatch(
      fixest::feols(fml_wd, data = dt_sub, weights = ~n_trips,
                     cluster = ~date, lean = TRUE),
      error = function(e) NULL
    )
    if (!is.null(m)) {
      cat("  ", pname, fit, "fitness: O3_10 =", round(coef(m)["O3_10"], 4),
          "(n=", nrow(dt_sub), ")\n")
    }
  }
}

cat("\nRobustness checks complete.\n")
