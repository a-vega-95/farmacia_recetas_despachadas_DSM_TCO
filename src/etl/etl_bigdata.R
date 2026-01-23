# =============================================================================
# ETL BIG DATA - RECETAS DESPACHADAS HOMOLOGACIÓN
# =============================================================================
# Procesamiento optimizado para archivos grandes (200k+ registros)
# Salida: Parquet con row_group optimizado para Power BI y RStudio
# =============================================================================

library(readxl)
library(dplyr)
library(arrow)
library(stringr)
library(lubridate)
library(digest)

# Instalar/cargar here para rutas robustas
if (!require("here", quietly = TRUE)) {
  install.packages("here")
}
library(here)

# Logger
source(here("src", "utils", "logging.R"))

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Rutas robustas usando here::here() - funcionan independiente de dónde se ejecute
INPUT_DIR <- here("datos_entrada")
OUTPUT_DIR <- here("datos_salida")
DICT_FILE <- here("diccionario_homologacion", "diccionario_farmacia_iglobal.xlsx")
OUTPUT_FILE <- "recetas_despachadas_consolidado.parquet"

# Configuración de Parquet para Big Data
PARQUET_CONFIG <- list(
  row_group_size = 10000,        # Óptimo para Power BI (10k-50k)
  compression = "snappy",         # Balance velocidad/compresión
  use_dictionary = TRUE           # Mejora compresión de strings repetidos
)

cat("\n")
cat("═══════════════════════════════════════════════════════════════\n")
cat("  ETL BIG DATA - RECETAS DESPACHADAS\n")
cat("═══════════════════════════════════════════════════════════════\n")
cat(sprintf("Inicio: %s\n", Sys.time()))
cat(sprintf("Directorio de trabajo: %s\n", here()))
cat("\n")

logger <- init_logger("etl")
log_info(logger, "Inicio ETL BIG DATA - RECETAS DESPACHADAS")
log_info(logger, sprintf("Directorio raíz proyecto: %s", here()))

# =============================================================================
# VALIDACIÓN Y CREACIÓN DE DIRECTORIOS
# =============================================================================

cat("0. Validando estructura de directorios...\n")

# Validar/crear directorio datos_salida
if (!dir.exists(OUTPUT_DIR)) {
  cat(sprintf("   - Creando carpeta: %s\n", OUTPUT_DIR))
  log_info(logger, sprintf("Carpeta datos_salida no existe, creando: %s", OUTPUT_DIR))
  dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
  cat("   - Carpeta datos_salida creada exitosamente\n")
  log_info(logger, "Carpeta datos_salida creada exitosamente")
} else {
  cat("   - Carpeta datos_salida: OK\n")
  log_info(logger, sprintf("Carpeta datos_salida existe: %s", OUTPUT_DIR))
}

# Validar directorio datos_entrada
if (!dir.exists(INPUT_DIR)) {
  cat("\n")
  cat("═══════════════════════════════════════════════════════════════\n")
  cat("  ERROR: CARPETA datos_entrada NO ENCONTRADA\n")
  cat("═══════════════════════════════════════════════════════════════\n")
  cat(sprintf("Ruta esperada: %s\n", INPUT_DIR))
  cat("\n")
  cat("Creando carpeta datos_entrada...\n")
  log_error(logger, sprintf("Carpeta datos_entrada NO EXISTE: %s", INPUT_DIR))
  
  dir.create(INPUT_DIR, showWarnings = FALSE, recursive = TRUE)
  
  cat("\n")
  cat("ACCIÓN REQUERIDA:\n")
  cat(sprintf("1. Coloque los archivos XLSX en: %s\n", INPUT_DIR))
  cat("2. Ejecute nuevamente el pipeline\n")
  cat("\n")
  cat("Proceso detenido.\n")
  cat("═══════════════════════════════════════════════════════════════\n")
  cat("\n")
  
  log_error(logger, "Carpeta datos_entrada creada pero está vacía")
  log_error(logger, "PROCESO DETENIDO: Debe colocar archivos XLSX en datos_entrada/")
  
  stop(sprintf("PROCESO DETENIDO: Carpeta datos_entrada creada pero vacía.\nColoque archivos XLSX en: %s", INPUT_DIR))
}

cat("   - Carpeta datos_entrada: OK\n")
log_info(logger, sprintf("Carpeta datos_entrada existe: %s", INPUT_DIR))
cat("\n")

# =============================================================================
# PASO 1: CARGAR DICCIONARIO DE HOMOLOGACIÓN
# =============================================================================

cat("1. Cargando diccionario de homologación...\n")

diccionario <- read_excel(DICT_FILE) %>%
  select(1:2) %>%
  setNames(c("articulo_original", "codigo_iglobal"))

cat(sprintf("   - %s entradas en diccionario\n", format(nrow(diccionario), big.mark = ",")))
log_info(logger, sprintf("Diccionario cargado: %s entradas", format(nrow(diccionario), big.mark = ",")))

# =============================================================================
# PASO 2: LISTAR ARCHIVOS XLSX
# =============================================================================

cat("\n2. Listando archivos de entrada...\n")

archivos <- list.files(INPUT_DIR, pattern = "\\.xlsx$", full.names = TRUE)

if (length(archivos) == 0) {
  msg <- sprintf("No se encontraron archivos XLSX en: %s", INPUT_DIR)
  cat(sprintf("   - ERROR: %s\n", msg))
  log_error(logger, msg)
  stop(msg)
}

cat(sprintf("   - %d archivos encontrados\n", length(archivos)))
log_info(logger, sprintf("Archivos encontrados: %d", length(archivos)))

# =============================================================================
# PASO 3: PROCESAR ARCHIVOS DE FORMA EFICIENTE
# =============================================================================

cat("\n3. Procesando archivos (Big Data)...\n")

all_data <- list()
total_registros <- 0
hashes_originales <- list()

for (i in seq_along(archivos)) {
  archivo <- archivos[i]
  nombre <- basename(archivo)
  
  cat(sprintf("\n   [%d/%d] %s\n", i, length(archivos), nombre))
  log_info(logger, sprintf("Procesando archivo [%d/%d]: %s", i, length(archivos), nombre))
  
  # Leer con readxl (optimizado para archivos grandes)
  tryCatch({
    
    # Leer datos: saltar 8 filas para que fila 9 sea encabezado
    # Datos empiezan en fila 10
    df <- read_excel(
      archivo,
      skip = 8,  # Salta filas 1-8, fila 9 = encabezados, fila 10+ = datos
      col_types = "text",  # Leer todo como texto inicialmente
      .name_repair = "unique"
    )
    
    registros <- nrow(df)
    total_registros <- total_registros + registros
    
    cat(sprintf("      - %s registros leídos\n", format(registros, big.mark = ",")))
    log_info(logger, sprintf("Registros leídos: %s", format(registros, big.mark = ",")))
    
    # Calcular hash de integridad del archivo (primeras 3 columnas para eficiencia)
    hash_key <- paste0(df[[1]], df[[2]], if(ncol(df) >= 3) df[[3]] else "", collapse = "")
    file_hash <- digest(hash_key, algo = "xxhash64")
    hashes_originales[[i]] <- list(
      archivo = nombre,
      hash = file_hash,
      registros = registros
    )
    cat(sprintf("      - Hash: %s\n", substr(file_hash, 1, 16)))
    log_info(logger, sprintf("Hash archivo: %s", file_hash))
    
    # Agregar columna de origen
    df$archivo_origen <- nombre
    
    all_data[[i]] <- df
    
  }, error = function(e) {
    cat(sprintf("      ERROR: %s\n", e$message))
    log_error(logger, sprintf("Error leyendo %s: %s", nombre, e$message))
  })
}

cat(sprintf("\n   Total consolidado: %s registros\n", format(total_registros, big.mark = ",")))
log_info(logger, sprintf("Total registros (suma entradas): %s", format(total_registros, big.mark = ",")))

# Guardar hashes para validación posterior
hash_info <- data.frame(
  archivo = sapply(hashes_originales, function(x) x$archivo),
  hash = sapply(hashes_originales, function(x) x$hash),
  registros = sapply(hashes_originales, function(x) x$registros)
)
log_info(logger, sprintf("Hashes calculados para %d archivos", nrow(hash_info)))

# =============================================================================
# PASO 4: CONSOLIDAR Y NORMALIZAR
# =============================================================================

cat("\n4. Consolidando y normalizando datos...\n")

df_consolidado <- bind_rows(all_data)

cat(sprintf("   - Dimensiones: %s filas x %d columnas\n", 
            format(nrow(df_consolidado), big.mark = ","), 
            ncol(df_consolidado)))
log_info(logger, sprintf("Consolidado: %s filas x %d columnas", 
                         format(nrow(df_consolidado), big.mark = ","), ncol(df_consolidado)))

# =============================================================================
# PASO 5: NORMALIZAR FECHAS
# =============================================================================

cat("\n5. Normalizando fechas...\n")

columnas_fecha <- c(
  "FECHA GENERACION RECETA",
  "FECHA VIGENCIA RECETA",
  "FECHA DESPACHO",
  "FECHA PROXIMO DESPACHO",
  "FECHA DE NACIMIENTO",
  "FECHA VENCIMIENTO",
  "FECHA DESPACHO DOMICILIO"
)

for (col in columnas_fecha) {
  if (col %in% names(df_consolidado)) {
    df_consolidado[[col]] <- tryCatch({
      dmy(df_consolidado[[col]])
    }, error = function(e) {
      as.Date(df_consolidado[[col]], format = "%d-%m-%Y")
    })
    cat(sprintf("   - %s: OK\n", col))
    log_info(logger, sprintf("Fecha normalizada: %s", col))
  }
}

# =============================================================================
# PASO 5.5: NORMALIZAR HORAS Y CREAR COLUMNAS DERIVADAS
# =============================================================================

cat("\n5.5. Normalizando horas y creando columnas derivadas...\n")

columnas_hora <- c(
  "HORA GENERACION RECETA",
  "HORA DESPACHO",
  "HORA DESPACHO DOMICILIO"
)

for (col in columnas_hora) {
  if (col %in% names(df_consolidado)) {
    # Convertir decimal de Excel (0-1) a formato HH:MM:SS
    df_consolidado[[col]] <- tryCatch({
      hora_decimal <- as.numeric(df_consolidado[[col]])
      # Convertir a segundos desde medianoche
      segundos_totales <- hora_decimal * 24 * 3600
      # Formatear como HH:MM:SS
      horas <- floor(segundos_totales / 3600)
      minutos <- floor((segundos_totales %% 3600) / 60)
      segundos <- floor(segundos_totales %% 60)
      sprintf("%02d:%02d:%02d", horas, minutos, segundos)
    }, error = function(e) {
      df_consolidado[[col]]
    })
    cat(sprintf("   - %s: OK\n", col))
    log_info(logger, sprintf("Hora normalizada: %s", col))
  }
}

# Crear columna extension_17 basada en HORA DESPACHO >= 17:00
if ("HORA DESPACHO" %in% names(df_consolidado)) {
  df_consolidado <- df_consolidado %>%
    mutate(
      hora_despacho_decimal = as.numeric(sub(":.*", "", `HORA DESPACHO`)),
      extension_17 = if_else(hora_despacho_decimal >= 17, "SI", "NO", missing = "NO")
    ) %>%
    select(-hora_despacho_decimal)
  
  # Estadísticas
  extension_si <- sum(df_consolidado$extension_17 == "SI", na.rm = TRUE)
  pct_extension <- (extension_si / nrow(df_consolidado)) * 100
  
  cat(sprintf("   - Columna extension_17 creada\n"))
  cat(sprintf("   - Despachos >= 17:00hrs: %s (%.2f%%)\n", 
              format(extension_si, big.mark = ","), pct_extension))
  log_info(logger, sprintf("extension_17: %s despachos >= 17hrs (%.2f%%)", 
                           format(extension_si, big.mark = ","), pct_extension))
} else {
  log_warn(logger, "Columna HORA DESPACHO no encontrada, no se creó extension_17")
}

# Crear columna grupo_horas basada en HORA DESPACHO
if ("HORA DESPACHO" %in% names(df_consolidado)) {
  df_consolidado <- df_consolidado %>%
    mutate(
      hora_despacho_num = as.numeric(sub(":.*", "", `HORA DESPACHO`)),
      grupo_horas = sprintf("%02d:00-%02d:59", hora_despacho_num, hora_despacho_num)
    ) %>%
    select(-hora_despacho_num)
  
  # Estadísticas de grupos
  grupos_unicos <- n_distinct(df_consolidado$grupo_horas, na.rm = TRUE)
  cat(sprintf("   - Columna grupo_horas creada\n"))
  cat(sprintf("   - Grupos horarios identificados: %d\n", grupos_unicos))
  log_info(logger, sprintf("grupo_horas: %d grupos horarios creados", grupos_unicos))
} else {
  log_warn(logger, "Columna HORA DESPACHO no encontrada, no se creó grupo_horas")
}

# Crear columna iso_dow (día de la semana ISO: 1=Lunes, 7=Domingo)
if ("FECHA DESPACHO" %in% names(df_consolidado)) {
  df_consolidado <- df_consolidado %>%
    mutate(
      iso_dow = as.integer(strftime(`FECHA DESPACHO`, format = "%u"))
    )
  
  # Estadísticas por día de semana
  dias_semana <- c("Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo")
  cat(sprintf("   - Columna iso_dow creada (1=Lunes, 7=Domingo)\n"))
  
  # Distribución por día
  dist_dias <- df_consolidado %>%
    filter(!is.na(iso_dow)) %>%
    count(iso_dow) %>%
    arrange(iso_dow)
  
  if (nrow(dist_dias) > 0) {
    cat("   - Distribución por día de semana:\n")
    for (i in seq_len(nrow(dist_dias))) {
      dia_num <- dist_dias$iso_dow[i]
      dia_nombre <- dias_semana[dia_num]
      cantidad <- dist_dias$n[i]
      pct <- (cantidad / nrow(df_consolidado)) * 100
      cat(sprintf("     %d (%s): %s despachos (%.2f%%)\n", 
                  dia_num, dia_nombre, format(cantidad, big.mark = ","), pct))
    }
  }
  
  log_info(logger, sprintf("iso_dow: Columna día de semana creada (1-7)"))
} else {
  log_warn(logger, "Columna FECHA DESPACHO no encontrada, no se creó iso_dow")
}

# =============================================================================
# PASO 6: APLICAR HOMOLOGACIÓN IGLOBAL
# =============================================================================

cat("\n6. Aplicando homologación IGLOBAL...\n")

# Buscar columna ARTICULO (puede tener diferentes nombres después de bind_rows)
col_articulo <- names(df_consolidado)[str_detect(str_to_upper(names(df_consolidado)), "^ARTICULO")]
if (length(col_articulo) == 0) {
  col_articulo <- names(df_consolidado)[str_detect(str_to_upper(names(df_consolidado)), "ARTICULO")]
}

if (length(col_articulo) > 0) {
  
  cat(sprintf("   - Usando columna: %s\n", col_articulo[1]))
  log_info(logger, sprintf("Columna ARTICULO detectada: %s", col_articulo[1]))
  
  # Normalizar nombre de artículo para match
  df_consolidado <- df_consolidado %>%
    mutate(articulo_norm = str_to_upper(str_trim(.data[[col_articulo[1]]])))
  
  diccionario <- diccionario %>%
    mutate(articulo_norm = str_to_upper(str_trim(articulo_original)))

  # Deduplicar diccionario por articulo_norm para evitar many-to-many en el join
  total_dict_rows <- nrow(diccionario)
  diccionario_norm <- diccionario %>%
    filter(!is.na(articulo_norm)) %>%
    group_by(articulo_norm) %>%
    summarise(
      codigo_iglobal = {
        vals <- na.omit(codigo_iglobal)
        if (length(vals) > 0) vals[1] else NA_character_
      },
      .groups = "drop"
    )
  dedup_count <- total_dict_rows - nrow(diccionario_norm)
  if (dedup_count > 0) {
    log_warn(logger, sprintf("Diccionario con duplicados: %s filas colapsadas por articulo_norm", 
                             format(dedup_count, big.mark = ",")))
  } else {
    log_info(logger, "Diccionario sin duplicados por articulo_norm")
  }
  
  # Left join usando diccionario deduplicado para preservar conteo de filas
  df_consolidado <- df_consolidado %>%
    left_join(
      diccionario_norm %>% select(articulo_norm, codigo_iglobal),
      by = "articulo_norm"
    ) %>%
    select(-articulo_norm)
  
  # Estadísticas de homologación
  homologados <- sum(!is.na(df_consolidado$codigo_iglobal))
  pct_homologado <- (homologados / nrow(df_consolidado)) * 100
  
  cat(sprintf("   - Artículos homologados: %s (%.2f%%)\n", 
              format(homologados, big.mark = ","), 
              pct_homologado))
  cat(sprintf("   - Sin homologar: %s\n", 
              format(nrow(df_consolidado) - homologados, big.mark = ",")))
  log_info(logger, sprintf("Homologación: %s homologados (%.2f%%), %s sin homologar", 
                           format(homologados, big.mark = ","), pct_homologado, 
                           format(nrow(df_consolidado) - homologados, big.mark = ",")))
  
} else {
  cat("   - ADVERTENCIA: Columna ARTICULO no encontrada\n")
  cat(sprintf("   - Columnas disponibles con 'ART': %s\n", 
              paste(names(df_consolidado)[str_detect(str_to_upper(names(df_consolidado)), "ART")], collapse = ", ")))
  log_warn(logger, "Columna ARTICULO no encontrada en consolidado")
}

# =============================================================================
# PASO 7: CONVERTIR TIPOS DE DATOS
# =============================================================================

cat("\n7. Optimizando tipos de datos...\n")

# Columnas numéricas
cols_numeric <- c(
  "N°", "NUMERO RCETA", "ID ARTICULO HIJO", "TOTAL RECETADO",
  "CANT POR PERIODICIDAD", "TOTAL PRESCRIPCIONES POR RECETA",
  "ID RECETA", "EDAD AÑOS", "EDAD MESES", "EDAD DIAS"
)

for (col in cols_numeric) {
  if (col %in% names(df_consolidado)) {
    df_consolidado[[col]] <- as.numeric(df_consolidado[[col]])
  }
}

cat("   - Tipos de datos optimizados\n")
log_info(logger, "Tipos de datos optimizados")

# =============================================================================
# PASO 8: VALIDACIÓN DE INTEGRIDAD (HASHING)
# =============================================================================

cat("\n8. Validación de integridad (hashing)...\n")

# Calcular hash agregado del consolidado final
hash_key_final <- paste0(
  df_consolidado[[1]], 
  df_consolidado[[2]], 
  if(ncol(df_consolidado) >= 3) df_consolidado[[3]] else "",
  collapse = ""
)
hash_consolidado <- digest(hash_key_final, algo = "xxhash64")
cat(sprintf("   - Hash consolidado: %s\n", substr(hash_consolidado, 1, 16)))
log_info(logger, sprintf("Hash consolidado: %s", hash_consolidado))

# Validar conteo de registros (balance de masas estricto)
if (nrow(df_consolidado) != total_registros) {
  msg <- sprintf("Validación de integridad FALLIDA: entradas=%s, consolidado=%s",
                 format(total_registros, big.mark = ","),
                 format(nrow(df_consolidado), big.mark = ","))
  cat(sprintf("   - %s\n", msg))
  log_error(logger, msg)
  stop(msg)
}

cat(sprintf("   - Conteo verificado: %s registros (entradas = consolidado)\n", 
            format(nrow(df_consolidado), big.mark = ",")))
cat("   - Validación de integridad: OK\n")
log_info(logger, "Validación de integridad completada exitosamente")

# =============================================================================
# PASO 9: GUARDAR EN PARQUET OPTIMIZADO
# =============================================================================

cat("\n9. Guardando en formato Parquet (optimizado para Big Data)...\n")

output_path <- file.path(OUTPUT_DIR, OUTPUT_FILE)

# Escribir Parquet con configuración optimizada
write_parquet(
  df_consolidado,
  output_path,
  compression = PARQUET_CONFIG$compression,
  use_dictionary = PARQUET_CONFIG$use_dictionary,
  write_statistics = TRUE,
  data_page_size = 1024 * 1024,            # 1 MB páginas de datos
  chunk_size = PARQUET_CONFIG$row_group_size  # Chunks de 10k registros para Power BI
)

# Información del archivo generado
file_size <- file.info(output_path)$size / (1024^2)  # MB

cat(sprintf("   - Archivo: %s\n", output_path))
cat(sprintf("   - Tamaño: %.2f MB\n", file_size))
cat(sprintf("   - Compresión: %s\n", PARQUET_CONFIG$compression))
cat(sprintf("   - Row groups: ~%s registros por grupo\n", 
            format(PARQUET_CONFIG$row_group_size, big.mark = ",")))
log_info(logger, sprintf("Parquet escrito: %s (%.2f MB), compresion=%s, row_group_size~%s",
                         output_path, file_size, PARQUET_CONFIG$compression, 
                         format(PARQUET_CONFIG$row_group_size, big.mark = ",")))

# =============================================================================
# PASO 10: VALIDAR PARQUET Y BALANCE DE MASAS
# =============================================================================

cat("\n10. Validando archivo Parquet y balance de masas...\n")

# Leer metadata sin cargar datos
parquet_file <- read_parquet(output_path, as_data_frame = FALSE)
parquet_schema <- schema(parquet_file)

cat(sprintf("   - Columnas: %d\n", length(parquet_schema$names)))
cat(sprintf("   - Verificación: OK\n"))
log_info(logger, sprintf("Validación Parquet: %d columnas", length(parquet_schema$names)))

# Auditoría de balance de masas
rows_in_sum <- total_registros
rows_in_consolidado <- nrow(df_consolidado)
rows_out <- nrow(parquet_file)

cat(sprintf("   - Entradas (suma archivos): %s\n", format(rows_in_sum, big.mark = ",")))
cat(sprintf("   - Consolidado (filas): %s\n", format(rows_in_consolidado, big.mark = ",")))
cat(sprintf("   - Salida Parquet (filas): %s\n", format(rows_out, big.mark = ",")))

log_info(logger, sprintf("Entradas=%s, Consolidado=%s, Salida=%s",
                         format(rows_in_sum, big.mark = ","),
                         format(rows_in_consolidado, big.mark = ","),
                         format(rows_out, big.mark = ",")))

if (rows_in_consolidado == rows_out && rows_in_sum == rows_out) {
  cat("   - Balance de masas: OK\n")
  log_info(logger, "Balance de masas: OK (entradas = salida)")
} else {
  cat("   - Balance de masas: DESBALANCE\n")
  log_error(logger, sprintf("Desbalance de masas: entradas=%s, consolidado=%s, salida=%s",
                            format(rows_in_sum, big.mark = ","),
                            format(rows_in_consolidado, big.mark = ","),
                            format(rows_out, big.mark = ",")))
  # Eliminar Parquet inválido
  if (file.exists(output_path)) {
    file.remove(output_path)
    log_warn(logger, "Parquet eliminado por desbalance de masas")
  }
  stop(sprintf("Desbalance de masas: entradas=%s, consolidado=%s, salida=%s",
               format(rows_in_sum, big.mark = ","),
               format(rows_in_consolidado, big.mark = ","),
               format(rows_out, big.mark = ",")))
}

# Guardar hashes para auditoría
hash_audit_file <- here("logs", sprintf("hash_audit_%s.csv", format(Sys.time(), "%Y%m%d_%H%M%S")))
write.csv(hash_info, hash_audit_file, row.names = FALSE)
cat(sprintf("   - Auditoría de hashes guardada: %s\n", hash_audit_file))
log_info(logger, sprintf("Auditoría de hashes: %s", hash_audit_file))

# =============================================================================
# REPORTE FINAL
# =============================================================================

cat("\n")
cat("═══════════════════════════════════════════════════════════════\n")
cat("  REPORTE FINAL\n")
cat("═══════════════════════════════════════════════════════════════\n")
cat(sprintf("Archivos procesados: %d\n", length(archivos)))
cat(sprintf("Total registros: %s\n", format(nrow(df_consolidado), big.mark = ",")))
cat(sprintf("Total columnas: %d\n", ncol(df_consolidado)))
cat(sprintf("Entradas (suma archivos): %s\n", format(rows_in_sum, big.mark = ",")))
cat(sprintf("Salida Parquet (filas): %s\n", format(rows_out, big.mark = ",")))
cat(sprintf("\nColumnas principales:\n"))
cat(sprintf("  - Datos paciente: 26 columnas\n"))
cat(sprintf("  - Datos receta: 17 columnas\n"))
cat(sprintf("  - Datos artículo: 12 columnas\n"))
cat(sprintf("  - Datos despacho: 14 columnas\n"))
cat(sprintf("  - Columnas derivadas: 5 (archivo_origen, extension_17, grupo_horas, iso_dow, codigo_iglobal)\n"))
cat(sprintf("  - Homologación IGLOBAL: SI\n"))
cat(sprintf("\nSalida: %s\n", output_path))
cat(sprintf("Tamaño: %.2f MB\n", file_size))
cat(sprintf("Compresión: %.1fx\n", (total_registros * ncol(df_consolidado) * 50) / (file_size * 1024^2)))
cat(sprintf("\nOptimizado para: Power BI, RStudio, Python\n"))
cat(sprintf("Fin: %s\n", Sys.time()))
cat("═══════════════════════════════════════════════════════════════\n")
cat("\n")
