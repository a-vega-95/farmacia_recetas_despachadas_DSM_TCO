# ETL: Homologación de Recetas Despachadas
**Versión 1.2.1 - BIG DATA Optimizado**

---

## DESCRIPCIÓN

ETL para procesar recetas despachadas (archivos XLSX grandes 60+ MB) y generar Parquet consolidado con homologación iGLOBAL.

**Características:**
- Procesa archivos XLSX grandes sin colapsar RAM
- Normaliza fechas automáticamente
- Aplica diccionario de homologación iGLOBAL
- Genera Parquet optimizado (row groups 50,000 filas para Power BI/RStudio)
- Compresión Snappy

---

## ESTRUCTURA

```
proyecto/
├── PROYECTO.md                     # Esta documentación
├── mapeo_datos_crudos.txt          # 69 columnas de entrada
├── etl_bigdata.R                   # EJECUTABLE PRINCIPAL
├── run_pipeline.R                  # Wrapper de ejecución
├── datos_entrada/                  # *.xlsx aquí
├── datos_salida/                   # recetas_consolidadas.parquet aquí
└── diccionario_homologacion/       # diccionario_farmacia_iglobal.xlsx
```

---

## INSTALACIÓN

```r
install.packages(c("readxl", "dplyr", "tidyr", "stringr", "arrow", "readr"))
```

---

## EJECUCIÓN

**Desde RStudio:**
```r
source("run_pipeline.R")
```

**Desde terminal:**
```powershell
Rscript etl_bigdata.R
```

---

## COLUMNAS DE ENTRADA (69 TOTAL)

Ver [`mapeo_datos_crudos.txt`](mapeo_datos_crudos.txt) para detalle completo.

**Columnas clave procesadas:**
- N° → numero_fila
- NUMERO RCETA → numero_receta
- FECHA GENERACION RECETA → fecha_generacion_receta (normalizada)
- FECHA VIGENCIA RECETA → fecha_vigencia_receta (normalizada)
- FECHA DESPACHO → fecha_despacho (normalizada)
- ARTICULO → articulo_original
- CANTIDAD DESPACHADA → cantidad_despachada
- LOTE → lote
- FECHA VENCIMIENTO → fecha_vencimiento (normalizada)

---

## HOMOLOGACIÓN IGLOBAL

Se crea columna `codigo_iglobal` mediante lookup en diccionario:

```
diccionario_homologacion/diccionario_farmacia_iglobal.xlsx
Columnas: articulo_original | codigo_iglobal
```

Si no existe match → `codigo_iglobal = "SIN_HOMOLOGAR"`

---

## SALIDA

**Archivo:** `datos_salida/recetas_consolidadas.parquet`

**Características:**
- Formato: Apache Parquet v2.0
- Compresión: Snappy
- Row Groups: 50,000 filas (optimizado para lectura eficiente)
- Compatible con: Power BI, RStudio, Python pandas, Spark

**Lectura en RStudio:**
```r
library(arrow)
df <- read_parquet("datos_salida/recetas_consolidadas.parquet")
```

**Lectura en Power BI:**
```
Obtener datos → Parquet → Seleccionar archivo
```

---

## NOTAS TÉCNICAS

1. **Archivos XLSX grandes:** Se procesan por chunks implícitos (readxl gestiona memoria automáticamente)
2. **Fechas:** Se convierten a formato Date estándar R (YYYY-MM-DD)
3. **Limpieza:** Se eliminan espacios extras y se normalizan nombres de columnas
4. **Eficiencia:** Row groups de 50k permiten lectura selectiva sin cargar todo en RAM
5. **Compresión:** Snappy balance óptimo entre velocidad y tamaño

---

## TROUBLESHOOTING

**Error "readxl no puede leer archivo":**
- Verificar que archivos no estén abiertos en Excel
- Confirmar que encabezados están en fila 9

**Error "memoria insuficiente":**
- Procesar archivos de forma incremental (modificar `n_max` en `read_xlsx`)
- Aumentar chunk_size en write_parquet

**Parquet muy lento en Power BI:**
- Reducir row_group_size a 25,000
- Usar compresión 'gzip' en lugar de 'snappy'

---

## AUTOR

ETL Team - Versión 1.2.1
Fecha: 2026-01-15
