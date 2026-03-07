# Descriptive statistics and figures ----

source("R/00_packages.R")

proc_dir <- "data/processed"
fig_dir <- "output/figures"
tab_dir <- "output/tables"
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

# Load data ----

hourly <- as.data.table(read_parquet(file.path(proc_dir, "analysis_hourly.parquet")))
daily <- as.data.table(read_parquet(file.path(proc_dir, "analysis_daily.parquet")))

# Read trip-level data for distribution plots
trip_files <- list.files(proc_dir, pattern = "^trips_\\d{4}\\.parquet$", full.names = TRUE)

# Define sample periods ----

periods <- list(
  replication = c(2013L, 2017L),
  extension   = c(2013L, 2025L),
  post2017    = c(2018L, 2025L)
)

# Table 1: Hourly descriptive statistics ----

cat("=== Table 1: Descriptive statistics ===\n")

desc_vars <- c("mean_speed", "mean_dist", "mean_duration", "n_trips",
               "O3", "NOx", "PM25", "SO2")

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- hourly[year >= yr_range[1] & year <= yr_range[2] & !is.na(mean_speed)]

  # WLS-weighted stats (weighted by n_trips)
  stats_list <- lapply(desc_vars, function(v) {
    x <- dt[[v]]
    w <- dt[["n_trips"]]
    ok <- !is.na(x) & !is.na(w)
    x <- x[ok]; w <- w[ok]
    if (length(x) == 0) return(data.table(variable = v, mean = NA, sd = NA, min = NA, max = NA, n = 0L))
    wm <- weighted.mean(x, w)
    wsd <- sqrt(sum(w * (x - wm)^2) / sum(w))
    data.table(variable = v, mean = wm, sd = wsd,
               min = min(x), max = max(x), n = length(x))
  })
  stats <- rbindlist(stats_list)

  cat("\n--- ", pname, " (", yr_range[1], "-", yr_range[2], ") ---\n")
  print(stats, digits = 3)

  fwrite(stats, file.path(tab_dir, paste0("table1_", pname, ".csv")))

  # LaTeX output
  var_labels <- c(
    mean_speed = "Speed (km/h)",
    mean_dist = "Distance (km)",
    mean_duration = "Duration (sec)",
    n_trips = "Number of trips",
    O3 = "Ozone (O\\textsubscript{3}, ppb)",
    NOx = "Nitrogen oxides (NO\\textsubscript{x}, ppb)",
    PM25 = "Particulate matter (PM\\textsubscript{2.5}, $\\mu$g/m\\textsuperscript{3})",
    SO2 = "Sulphur dioxide (SO\\textsubscript{2}, ppb)"
  )
  tex_lines <- c(
    "\\begin{tabular}{lrrrr}",
    "\\toprule",
    "Statistic & Mean & St Dev. & Min & Max \\\\",
    "\\midrule"
  )
  for (i in seq_len(nrow(stats))) {
    row <- stats[i]
    lab <- var_labels[row$variable]
    if (is.na(lab)) lab <- row$variable
    tex_lines <- c(tex_lines, sprintf(
      "%s & %.2f & %.2f & %.2f & %.2f \\\\",
      lab, row$mean, row$sd, row$min, row$max
    ))
  }
  tex_lines <- c(tex_lines, "\\bottomrule", "\\end{tabular}")
  writeLines(tex_lines, file.path(tab_dir, paste0("table1_", pname, ".tex")))
}

# Figure 2: Trip distributions ----

cat("\n=== Figure 2: Trip distributions ===\n")

for (pname in names(periods)) {
  yr_range <- periods[[pname]]

  # Read trip-level data for this period
  yr_files <- trip_files[as.integer(sub(".*trips_(\\d{4})\\.parquet", "\\1", basename(trip_files))) %in%
                          seq(yr_range[1], yr_range[2])]
  trips <- rbindlist(lapply(yr_files, function(f) {
    dt <- as.data.table(read_parquet(f, col_select = c("dist_km", "speed_kmh", "hour", "dow")))
    dt
  }))

  # Panel a: Distance distribution
  p1 <- ggplot(trips, aes(x = dist_km)) +
    geom_histogram(binwidth = 0.5, fill = "steelblue", color = "white") +
    labs(x = "Distance (km)", y = "Count", title = "Trip distance distribution") +
    coord_cartesian(xlim = c(0, 15)) +
    theme_minimal()

  # Panel b: Speed distribution
  p2 <- ggplot(trips, aes(x = speed_kmh)) +
    geom_histogram(binwidth = 0.5, fill = "steelblue", color = "white") +
    labs(x = "Speed (km/h)", y = "Count", title = "Trip speed distribution") +
    coord_cartesian(xlim = c(0, 30)) +
    theme_minimal()

  # Panel c: Trips by hour of day
  hourly_counts <- trips[, .N, by = hour]
  p3 <- ggplot(hourly_counts, aes(x = hour, y = N)) +
    geom_bar(stat = "identity", fill = "steelblue") +
    labs(x = "Hour of day", y = "Number of trips", title = "Trips by hour") +
    theme_minimal()

  # Panel d: Trips by day of week
  dow_counts <- trips[, .N, by = dow]
  dow_counts[, dow_label := factor(dow, levels = 1:7,
                                    labels = c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"))]
  p4 <- ggplot(dow_counts, aes(x = dow_label, y = N)) +
    geom_bar(stat = "identity", fill = "steelblue") +
    labs(x = "Day of week", y = "Number of trips", title = "Trips by day of week") +
    theme_minimal()

  pdf(file.path(fig_dir, paste0("figure2_", pname, ".pdf")), width = 10, height = 8)
  gridExtra::grid.arrange(p1, p2, p3, p4, ncol = 2)
  dev.off()

  rm(trips); gc(verbose = FALSE)
  cat("  Saved figure2_", pname, ".pdf\n")
}

# Figure 5: Ozone histograms ----

cat("\n=== Figure 5: Ozone histograms ===\n")

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt_h <- hourly[year >= yr_range[1] & year <= yr_range[2] & !is.na(O3)]
  dt_d <- daily[year >= yr_range[1] & year <= yr_range[2] & !is.na(O3)]

  p1 <- ggplot(dt_h, aes(x = O3)) +
    geom_histogram(binwidth = 2, fill = "steelblue", color = "white") +
    labs(x = "Ozone (ppb)", y = "Count", title = "Hourly ozone distribution") +
    theme_minimal()

  p2 <- ggplot(dt_d, aes(x = O3)) +
    geom_histogram(binwidth = 2, fill = "steelblue", color = "white") +
    labs(x = "Ozone (ppb)", y = "Count", title = "Daily mean ozone distribution") +
    theme_minimal()

  pdf(file.path(fig_dir, paste0("figure5_", pname, ".pdf")), width = 10, height = 4)
  gridExtra::grid.arrange(p1, p2, ncol = 2)
  dev.off()

  cat("  Saved figure5_", pname, ".pdf\n")
}

# Figure 6: Ozone and NO2 by hour of day ----

cat("\n=== Figure 6: Ozone and NO2 by hour of day ===\n")

for (pname in names(periods)) {
  yr_range <- periods[[pname]]
  dt <- hourly[year >= yr_range[1] & year <= yr_range[2]]

  by_hour <- dt[, .(O3 = mean(O3, na.rm = TRUE),
                     NO2 = mean(NO2, na.rm = TRUE)), by = hour]
  by_hour_long <- melt(by_hour, id.vars = "hour", variable.name = "pollutant", value.name = "ppb")

  p <- ggplot(by_hour_long, aes(x = hour, y = ppb, color = pollutant)) +
    geom_line(linewidth = 1) +
    geom_point(size = 2) +
    labs(x = "Hour of day", y = "Concentration (ppb)",
         title = "Mean pollutant concentrations by hour of day",
         color = "Pollutant") +
    scale_color_manual(values = c("O3" = "steelblue", "NO2" = "coral")) +
    theme_minimal()

  pdf(file.path(fig_dir, paste0("figure6_", pname, ".pdf")), width = 8, height = 5)
  print(p)
  dev.off()

  cat("  Saved figure6_", pname, ".pdf\n")
}

cat("\nDescriptives complete.\n")
