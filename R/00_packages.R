# Install and load required packages ----

pkgs <- c(
  "data.table",
  "arrow",
  "jsonlite",
  "curl",
  "fixest",
  "ggplot2",
  "gridExtra",
  "lubridate",
  "sf"
)

for (pkg in pkgs) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
  library(pkg, character.only = TRUE)
}

cat("All packages loaded.\n")
