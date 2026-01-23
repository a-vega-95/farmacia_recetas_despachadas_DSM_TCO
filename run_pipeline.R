# =============================================================================
# ETL RECETAS DESPACHADAS - EJECUTOR PRINCIPAL
# =============================================================================
# Uso: source("run_pipeline.R")
# =============================================================================

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║  ETL RECETAS DESPACHADAS - v1.2.1                            ║\n")
cat("║  Inicio:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "                         ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")

# Instalar/cargar here para rutas robustas
if (!require("here", quietly = TRUE)) {
  install.packages("here")
}
library(here)

# Ejecutar ETL optimizado con rutas robustas
source(here("src", "etl", "etl_bigdata.R"))

