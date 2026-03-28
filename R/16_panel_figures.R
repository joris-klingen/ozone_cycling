# Panel-based figures: ozone dummies and yearly effects ----
# Uses d>=4km, t>=4 filter for tractable computation

source("R/00_packages.R")

proc_dir <- "data/processed"
fig_dir <- "output/figures"
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

# Load and prepare panel data ----

panel <- as.data.table(read_parquet(file.path(proc_dir, "panel_trips.parquet")))
env_h <- as.data.table(read_parquet(file.path(proc_dir, "environment_hourly.parquet")))
hourly_city <- as.data.table(read_parquet(file.path(proc_dir, "analysis_hourly.parquet")))
hourly_city <- hourly_city[, .(date, hour, log_sum_dist)]

panel <- merge(panel, env_h, by.x = c("date", "hour"), by.y = c("date", "hour"),
               all.x = TRUE, suffixes = c("", "_env"))
panel <- merge(panel, hourly_city, by = c("date", "hour"), all.x = TRUE)
rm(env_h, hourly_city); gc(verbose = FALSE)

panel[, O3_10 := O3 / 10]
panel[, dow := wday(date)]
panel[, wind_dir_bin := cut(wind_direction_10m %% 360, breaks = seq(0, 360, 45),
                             include.lowest = TRUE, right = FALSE)]
for (lag_h in 1:8) {
  lag_col <- paste0("temp_lag", lag_h)
  bin_col <- paste0("temp_lag", lag_h, "_bin")
  if (lag_col %in% names(panel)) panel[, (bin_col) := floor(get(lag_col))]
}

# Filter: weekdays, d>=4km, t>=4 per cyclist ----

panel_wd <- panel[dow %in% 2:6 & dist_km >= 4 &
                    !is.na(O3_10) & !is.na(speed_kmh) & !is.na(temperature_2m)]
rm(panel); gc(verbose = FALSE)

set_counts <- panel_wd[, .N, by = cyclist_set]
keep_sets <- set_counts[N >= 4, cyclist_set]
panel_wd <- panel_wd[cyclist_set %in% keep_sets]
rm(set_counts, keep_sets); gc(verbose = FALSE)

cat("Panel obs (d>=4, t>=4):", nrow(panel_wd), "\n")
cat("Unique cyclists:", uniqueN(panel_wd[, cyclist_set]), "\n")

# Common controls ----

wx <- paste0(
  "factor(temp_bin) + surface_pressure + relative_humidity_2m + rain + ",
  "factor(radiation_bin) + shortwave_radiation:temperature_2m + wind_speed_10m + ",
  "factor(wind_dir_bin) + ",
  paste0("factor(temp_lag", 1:8, "_bin)", collapse = " + "), " + ",
  paste0("rain_lag", 1:8, collapse = " + ")
)
poll <- "factor(nox_bin) + PM25 + SO2"
vol <- "log_sum_dist + bank_holiday"

# ============================================================
# Part 1: Ozone 5ppb dummies (non-linearity)
# ============================================================

cat("\n=== Ozone dummies (panel) ===\n")

panel_wd[, o3_bin := cut(O3,
  breaks = c(-Inf, 10, 15, 20, 25, 30, 35, 40, 45, 50, Inf),
  labels = c("0-10", "10-15", "15-20", "20-25", "25-30",
             "30-35", "35-40", "40-45", "45-50", ">50"),
  right = FALSE
)]
panel_wd[, o3_bin := relevel(o3_bin, ref = "0-10")]

fml_dummy <- as.formula(paste0(
  "speed_kmh ~ o3_bin + ", wx, " + ", poll, " + ", vol,
  " | cyclist_set + dow"
))

periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L)
)

dummy_results <- list()

for (pname in names(periods)) {
  yr <- periods[[pname]]
  dt <- panel_wd[year >= yr[1] & year <= yr[2]]

  # Re-filter t>=4 within period
  sc <- dt[, .N, by = cyclist_set]
  dt <- dt[cyclist_set %in% sc[N >= 4, cyclist_set]]

  cat(pname, ": n =", nrow(dt), "\n")

  m <- tryCatch(
    fixest::feols(fml_dummy, data = dt, cluster = ~hour, lean = TRUE),
    error = function(e) { cat("  ERROR:", conditionMessage(e), "\n"); NULL }
  )

  if (!is.null(m)) {
    cf <- coef(m)
    se <- sqrt(diag(vcov(m)))
    idx <- grep("^o3_bin", names(cf))
    dummy_results[[pname]] <- data.table(
      sample = pname,
      bin = gsub("o3_bin", "", names(cf)[idx]),
      coef = cf[idx],
      se = se[idx]
    )
  }
}

# Combine with reference rows
all_dummy <- rbindlist(dummy_results)
ref_rows <- data.table(
  sample = names(periods),
  bin = "0-10", coef = 0, se = 0
)
all_dummy <- rbindlist(list(all_dummy, ref_rows))

bin_order <- c("0-10", "10-15", "15-20", "20-25", "25-30",
               "30-35", "35-40", "40-45", "45-50", ">50")
bin_midpoints <- c(5, 12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5, 47.5, 55)
all_dummy[, bin := factor(bin, levels = bin_order)]
all_dummy[, x := bin_midpoints[match(bin, bin_order)]]
all_dummy[, ci_lo := coef - 1.96 * se]
all_dummy[, ci_hi := coef + 1.96 * se]

sample_labels <- c(replication = "Replication (2013-2017)", extension = "Extension (2013-2025)")
all_dummy[, sample_label := factor(sample_labels[sample],
  levels = c("Replication (2013-2017)", "Extension (2013-2025)")
)]

cols <- c("Replication (2013-2017)" = "#2166ac", "Extension (2013-2025)" = "#b2182b")

p1 <- ggplot(all_dummy, aes(x = x, y = coef, color = sample_label, fill = sample_label)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray60", linewidth = 0.4) +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.1, color = NA) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 2, shape = 21, stroke = 0.6) +
  scale_color_manual(values = cols) +
  scale_fill_manual(values = cols) +
  scale_x_continuous(breaks = bin_midpoints, labels = bin_order) +
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

pdf(file.path(fig_dir, "panel_nonlinearity.pdf"), width = 8, height = 4.5)
print(p1)
dev.off()

png(file.path(fig_dir, "panel_nonlinearity.png"), width = 1600, height = 900, res = 200)
print(p1)
dev.off()

cat("Saved panel_nonlinearity.pdf and .png\n")

# ============================================================
# Part 2: Yearly ozone effect (panel)
# ============================================================

cat("\n=== Yearly ozone effect (panel) ===\n")

fml_yr <- as.formula(paste0(
  "speed_kmh ~ O3_10 + ", wx, " + ", poll, " + ", vol,
  " | cyclist_set + dow"
))

years <- sort(unique(panel_wd[, year]))
yr_results <- list()

for (yr in years) {
  dt <- panel_wd[year == yr]

  # Re-filter t>=4 within year
  sc <- dt[, .N, by = cyclist_set]
  dt <- dt[cyclist_set %in% sc[N >= 4, cyclist_set]]

  if (nrow(dt) < 1000) {
    cat(yr, ": too few obs (", nrow(dt), ")\n")
    next
  }

  m <- tryCatch(
    fixest::feols(fml_yr, data = dt, cluster = ~hour, lean = TRUE),
    error = function(e) { cat(yr, ": ERROR -", conditionMessage(e), "\n"); NULL }
  )

  if (!is.null(m) && "O3_10" %in% names(coef(m))) {
    cf <- coef(m)["O3_10"]
    se <- sqrt(vcov(m)["O3_10", "O3_10"])
    cat(yr, ": O3_10 =", round(cf, 4), " (SE =", round(se, 4), ", n =", nrow(dt), ")\n")
    yr_results[[as.character(yr)]] <- data.table(year = yr, coef = cf, se = se, n = nrow(dt))
  }
}

res <- rbindlist(yr_results)
res[, ci_lo := coef - 1.96 * se]
res[, ci_hi := coef + 1.96 * se]

p2 <- ggplot(res, aes(x = year, y = coef)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray60", linewidth = 0.4) +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.15, fill = "#2166ac") +
  geom_line(color = "#2166ac", linewidth = 0.8) +
  geom_point(color = "#2166ac", size = 2.5) +
  scale_x_continuous(breaks = years) +
  labs(x = NULL,
       y = "Effect on cycling speed (km/h per 10 ppb ozone)") +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 9),
    plot.margin = margin(10, 15, 5, 10)
  )

pdf(file.path(fig_dir, "panel_yearly_effect.pdf"), width = 8, height = 4)
print(p2)
dev.off()

png(file.path(fig_dir, "panel_yearly_effect.png"), width = 1600, height = 800, res = 200)
print(p2)
dev.off()

cat("Saved panel_yearly_effect.pdf and .png\n")
