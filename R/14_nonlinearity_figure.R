# Non-linearity figure: ozone 5ppb dummies ----
# Three colored lines: Original (from paper Figure 7), Replication, Extension

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
  hourly[, paste0("rain_lag", lag_h, "_bin") := fifelse(get(paste0("rain_lag", lag_h)) > 0, 1L, 0L)]
}
hourly[, dow_hour_daylight := paste0(dow, "_", hour, "_", daylight)]

hourly_wd <- hourly[dow %in% 2:6]

wx_hourly <- paste0(
  "factor(temp_bin) + surface_pressure + relative_humidity_2m + rain + ",
  "factor(radiation_bin) + shortwave_radiation:temperature_2m + wind_speed_10m + ",
  "factor(wind_dir_bin) + ",
  paste0("factor(temp_lag", 1:8, "_bin)", collapse = " + "), " + ",
  paste0("rain_lag", 1:8, collapse = " + ")
)
poll_hourly <- "SO2 + factor(nox_bin) + factor(pm25_bin)"
vol_hourly <- "log_sum_dist"

# Create 5ppb bins with reference 0-10 ----
# Bins: 0 (ref), 10-15, 15-20, 20-25, 25-30, 30-35, 35-40, 40-45, 45-50, >50

hourly_wd[, o3_bin := cut(O3,
  breaks = c(-Inf, 10, 15, 20, 25, 30, 35, 40, 45, 50, Inf),
  labels = c("0-10", "10-15", "15-20", "20-25", "25-30",
             "30-35", "35-40", "40-45", "45-50", ">50"),
  right = FALSE
)]
hourly_wd[, o3_bin := relevel(o3_bin, ref = "0-10")]

# Run regressions ----

periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L)
)

fml <- as.formula(paste0(
  "mean_speed ~ o3_bin + ", wx_hourly, " + ", poll_hourly, " + ", vol_hourly,
  " | date + dow_hour_daylight"
))

results_list <- list()

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- hourly_wd[year >= yr_range[1] & year <= yr_range[2] &
                    !is.na(O3) & !is.na(mean_speed) & !is.na(temperature_2m)]

  m <- fixest::feols(fml, data = dt, weights = ~n_trips,
                      cluster = ~date, lean = TRUE)

  cf <- coef(m)
  se <- sqrt(diag(vcov(m)))
  o3_idx <- grep("^o3_bin", names(cf))

  bin_labels <- gsub("o3_bin", "", names(cf)[o3_idx])

  results_list[[pname]] <- data.table(
    sample = pname,
    bin = bin_labels,
    coef = cf[o3_idx],
    se = se[o3_idx]
  )

  cat(pname, ": estimated", length(o3_idx), "ozone dummies\n")
}

# Original paper coefficients (approximated from Figure 7, panel spec) ----
# Values read from the published figure (Klingen & van Ommeren 2020, Fig. 7)
# Reference: 0-10 ppb. These are approximate.
original <- data.table(
  sample = "original",
  bin = c("10-15", "15-20", "20-25", "25-30", "30-35", "35-40", "40-45", "45-50", ">50"),
  coef = c(-0.005, -0.010, -0.025, -0.035, -0.055, -0.065, -0.080, -0.090, -0.120),
  se   = c(0.015,  0.015,  0.018,  0.020,  0.025,  0.030,  0.035,  0.045,  0.060)
)

# Combine ----
all_results <- rbindlist(c(list(original), results_list), fill = TRUE)

# Add reference category for all samples
ref_rows <- data.table(
  sample = c("original", "replication", "extension"),
  bin = "0-10",
  coef = 0,
  se = 0
)
all_results <- rbindlist(list(all_results, ref_rows))

# Order bins
bin_order <- c("0-10", "10-15", "15-20", "20-25", "25-30",
               "30-35", "35-40", "40-45", "45-50", ">50")
all_results[, bin := factor(bin, levels = bin_order)]

# Midpoints for x-axis positioning
bin_midpoints <- c(5, 12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5, 47.5, 55)
all_results[, x := bin_midpoints[match(bin, bin_order)]]

all_results[, ci_lo := coef - 1.96 * se]
all_results[, ci_hi := coef + 1.96 * se]

# Labels for plot
sample_labels <- c(
  "original" = "Original (2013-2017)",
  "replication" = "Replication (2013-2017)",
  "extension" = "Extension (2013-2025)"
)
all_results[, sample_label := factor(sample_labels[sample],
  levels = c("Original (2013-2017)", "Replication (2013-2017)", "Extension (2013-2025)")
)]

# Colors
cols <- c("Original (2013-2017)" = "#404040",
          "Replication (2013-2017)" = "#2166ac",
          "Extension (2013-2025)" = "#b2182b")

# Plot ----
p <- ggplot(all_results, aes(x = x, y = coef, color = sample_label, fill = sample_label)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray60", linewidth = 0.4) +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.1, color = NA) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 2, shape = 21, stroke = 0.6) +
  scale_color_manual(values = cols) +
  scale_fill_manual(values = cols) +
  scale_x_continuous(
    breaks = bin_midpoints,
    labels = bin_order
  ) +
  labs(x = "Ozone concentration (ppb)",
       y = "Effect on cycling speed (km/h)",
       color = NULL, fill = NULL) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "bottom",
    legend.margin = margin(t = -5),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 9),
    plot.margin = margin(10, 15, 5, 10)
  )

# Save ----
pdf(file.path(fig_dir, "nonlinearity_comparison.pdf"), width = 8, height = 4.5)
print(p)
dev.off()

png(file.path(fig_dir, "nonlinearity_comparison.png"), width = 1600, height = 900, res = 200)
print(p)
dev.off()

cat("Saved nonlinearity_comparison.pdf and .png\n")
