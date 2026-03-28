# Comparison figure: Original vs Replication vs Extension ----

source("R/00_packages.R")

fig_dir <- "output/figures"
tab_dir <- "output/tables"
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

# Coefficient data ----
# Original paper: Klingen & van Ommeren (2020), Table 6 equivalents
# Replication and extension: current results from scripts 08-11

coefs <- data.table(
  strategy = factor(rep(c("Within-day", "Spatio-temporal", "Cyclist panel"), each = 3),
                    levels = c("Within-day", "Spatio-temporal", "Cyclist panel")),
  sample = factor(rep(c("Original (2013-2017)", "Replication (2013-2017)", "Extension (2013-2025)"), 3),
                  levels = c("Original (2013-2017)", "Replication (2013-2017)", "Extension (2013-2025)")),
  coef = c(
    # Within-day
    -0.053, -0.072, -0.081,
    # Spatio-temporal (zone-week FE)
    -0.044, -0.031, -0.074,
    # Cyclist panel (d>=0, t>=4; extension uses d>=2 due to memory constraint)
    -0.026, -0.020, -0.022
  ),
  se = c(
    # Within-day
    0.016, 0.016, 0.010,
    # Spatio-temporal
    0.050, 0.051, 0.030,
    # Cyclist panel
    0.005, 0.005, 0.003
  )
)

coefs[, ci_lo := coef - 1.96 * se]
coefs[, ci_hi := coef + 1.96 * se]

# Color palette ----
cols <- c("Original (2013-2017)" = "#404040",
          "Replication (2013-2017)" = "#2166ac",
          "Extension (2013-2025)" = "#b2182b")

# Plot ----
p <- ggplot(coefs, aes(x = strategy, y = coef, color = sample, shape = sample)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray60", linewidth = 0.4) +
  geom_pointrange(aes(ymin = ci_lo, ymax = ci_hi),
                  position = position_dodge(width = 0.6),
                  size = 0.5, linewidth = 0.7) +
  scale_color_manual(values = cols) +
  scale_shape_manual(values = c(15, 16, 17)) +
  labs(x = NULL,
       y = "Effect on cycling speed (km/h per 10 ppb ozone)",
       color = NULL, shape = NULL) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "bottom",
    legend.margin = margin(t = -5),
    panel.grid.major.x = element_blank(),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(size = 10),
    plot.margin = margin(10, 15, 5, 10)
  )

# Save as PDF and PNG ----
pdf(file.path(fig_dir, "comparison_coefficients.pdf"), width = 8, height = 4.5)
print(p)
dev.off()

png(file.path(fig_dir, "comparison_coefficients.png"), width = 1600, height = 900, res = 200)
print(p)
dev.off()

cat("Saved comparison_coefficients.pdf and .png\n")

# Save comparison table as CSV ----
summary_tab <- coefs[, .(strategy, sample, coef, se, ci_lo, ci_hi)]
fwrite(summary_tab, file.path(tab_dir, "comparison_summary.csv"))
cat("Saved comparison_summary.csv\n")
