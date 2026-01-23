# Simple logger utility
init_logger <- function(prefix = "etl") {
  # Usar here si está disponible, sino usar ruta relativa
  logs_dir <- if (requireNamespace("here", quietly = TRUE)) {
    here::here("logs")
  } else {
    "logs"
  }
  
  dir.create(logs_dir, showWarnings = FALSE, recursive = TRUE)
  log_file <- file.path(logs_dir, sprintf("%s_%s.log", prefix, format(Sys.time(), "%Y%m%d_%H%M%S")))
  list(
    file = log_file,
    write = function(level, message) {
      line <- sprintf("[%s] [%s] %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), toupper(level), message)
      cat(line, "\n")
      write(line, file = log_file, append = TRUE)
    }
  )
}

log_info <- function(logger, message) logger$write("INFO", message)
log_warn <- function(logger, message) logger$write("WARN", message)
log_error <- function(logger, message) logger$write("ERROR", message)
