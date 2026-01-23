# ETL RECETAS DESPACHADAS - HOMOLOGACION IGLOBAL

Versión 1.2.1

---

## DESCRIPCION

Sistema ETL (Extract, Transform, Load) para el procesamiento automatizado de recetas farmacéuticas despachadas. El sistema consolida múltiples archivos Excel de gran tamaño, normaliza datos, aplica homologación con códigos iGLOBAL y genera un archivo Parquet optimizado para análisis en Power BI y RStudio.

Este proyecto está diseñado para manejar archivos de entrada superiores a 60 MB con más de 200,000 registros sin colapsar la memoria del sistema.

---

## AUTOR

**Anghello Vega**  
Gestor de Datos  
Departamento de Salud Municipal de Temuco

---

## CARACTERISTICAS PRINCIPALES

### Procesamiento Big Data
- Gestión eficiente de archivos XLSX grandes (60+ MB, 200k+ registros)
- Procesamiento por chunks implícitos mediante readxl
- Validación de integridad mediante hashing (xxHash64)
- Balance de masas estricto entre entradas y salidas

### Transformaciones de Datos
- Normalización automática de fechas (formatos múltiples)
- Conversión de horas desde formato decimal Excel a HH:MM:SS
- Creación de columnas derivadas (extension_17 para despachos >= 17:00hrs)
- Limpieza y estandarización de nombres de columnas
- Optimización de tipos de datos

### Homologación
- Aplicación de diccionario de homologación iGLOBAL
- Left join preservando todos los registros originales
- Detección automática de columnas ARTICULO
- Manejo de duplicados en diccionario
- Estadísticas de cobertura de homologación

### Salida Optimizada
- Formato Apache Parquet con compresión Snappy
- Row groups de 10,000 registros optimizados para Power BI
- Metadatos y estadísticas embebidas
- Compatible con múltiples plataformas de análisis

### Gestión de Rutas Robusta
- Uso de here::here() para rutas independientes del directorio de trabajo
- Validación automática de estructura de directorios
- Creación automática de carpeta datos_salida si no existe
- Detención controlada si falta carpeta datos_entrada con instrucciones claras

### Sistema de Logging
- Logs detallados con timestamp en formato [YYYY-MM-DD HH:MM:SS]
- Niveles: INFO, WARN, ERROR
- Auditoría de hashes por archivo procesado
- Registro de todas las operaciones críticas

---

## ESTRUCTURA DEL PROYECTO

```
ETL_farmacia_recetas_despachadas/
├── run_pipeline.R                          # Ejecutor principal del pipeline
├── 1.2.1v_recetas_despachadas_homologacion.Rproj  # Proyecto RStudio
├── README.md                               # Este archivo
├── datos_entrada/                          # Archivos XLSX de entrada
├── datos_salida/                           # Archivo Parquet consolidado
├── diccionario_homologacion/               # Diccionario de homologación
│   └── diccionario_farmacia_iglobal.xlsx
├── logs/                                   # Logs de ejecución y auditoría
│   ├── etl_YYYYMMDD_HHMMSS.log
│   └── hash_audit_YYYYMMDD_HHMMSS.csv
├── docs/                                   # Documentación adicional
│   ├── PROYECTO.md
│   └── LEEME.txt
└── src/                                    # Código fuente
    ├── etl/
    │   └── etl_bigdata.R                   # Motor ETL principal
    └── utils/
        └── logging.R                       # Sistema de logging
```

---

## REQUISITOS DEL SISTEMA

### Software
- R >= 4.0.0
- RStudio (recomendado)

### Paquetes R
```r
install.packages(c(
  "readxl",      # Lectura eficiente de archivos Excel
  "dplyr",       # Manipulación de datos
  "arrow",       # Lectura/escritura de Parquet
  "stringr",     # Manipulación de strings
  "lubridate",   # Manipulación de fechas
  "digest",      # Generación de hashes
  "here"         # Gestión de rutas robustas
))
```

### Requisitos de Hardware
- RAM: Mínimo 8 GB (recomendado 16 GB para archivos grandes)
- Disco: Espacio libre equivalente a 3x el tamaño total de archivos de entrada
- Procesador: Multi-core recomendado para mejor rendimiento

---

## FORMATO DE DATOS DE ENTRADA

### Estructura de Archivos Excel
- Formato: XLSX
- Ubicación: carpeta `datos_entrada/`
- Estructura: Encabezados en fila 9, datos desde fila 10
- Primeras 8 filas: Metadatos (se saltan automáticamente)

### Columnas Principales Procesadas
El sistema procesa 69 columnas en total, incluyendo:

**Datos de Paciente:**
- RUT, NOMBRE, APELLIDO PATERNO, APELLIDO MATERNO
- FECHA DE NACIMIENTO, EDAD AÑOS, EDAD MESES, EDAD DIAS
- SEXO, PUEBLO ORIGINARIO

**Datos de Receta:**
- NUMERO RCETA, ID RECETA
- FECHA GENERACION RECETA, HORA GENERACION RECETA
- FECHA VIGENCIA RECETA, FECHA PROXIMO DESPACHO
- DIAGNOSTICO, TIPO RECETA, ESTADO

**Datos de Artículo:**
- ARTICULO (nombre del medicamento)
- ID ARTICULO HIJO
- FORMA FARMACEUTICA, CONCENTRACION
- TOTAL RECETADO, CANT POR PERIODICIDAD

**Datos de Despacho:**
- FECHA DESPACHO, HORA DESPACHO
- CANTIDAD DESPACHADA, LOTE
- FECHA VENCIMIENTO
- SUCURSAL, TIPO DESPACHO

---

## DICCIONARIO DE HOMOLOGACION

### Ubicación
`diccionario_homologacion/diccionario_farmacia_iglobal.xlsx`

### Estructura
| Columna             | Tipo   | Descripción                          |
|---------------------|--------|--------------------------------------|
| articulo_original   | TEXT   | Nombre del artículo tal como aparece en recetas |
| codigo_iglobal      | TEXT   | Código homologado según sistema iGLOBAL |

### Funcionamiento
- El sistema realiza normalización de strings (trim, uppercase) antes del match
- Se utiliza left join para preservar todos los registros
- Artículos sin match mantienen codigo_iglobal = NA
- Se reporta porcentaje de cobertura de homologación

---

## EJECUCION

### Método 1: Desde RStudio (Recomendado)
1. Abrir el proyecto: `1.2.1v_recetas_despachadas_homologacion.Rproj`
2. Colocar archivos XLSX en carpeta `datos_entrada/`
3. Ejecutar en consola:
```r
source("run_pipeline.R")
```

### Método 2: Desde línea de comandos
```powershell
# Windows PowerShell
cd C:\ruta\al\proyecto
Rscript run_pipeline.R
```

```bash
# Linux/Mac Terminal
cd /ruta/al/proyecto
Rscript run_pipeline.R
```

### Método 3: Ejecución directa del motor ETL
```r
source("src/etl/etl_bigdata.R")
```

---

## PROCESO ETL DETALLADO

### PASO 0: Validación de Directorios
- Verifica existencia de carpeta `datos_salida/` (crea si no existe)
- Verifica existencia de carpeta `datos_entrada/`
- Si `datos_entrada/` no existe: crea carpeta, muestra instrucciones y detiene proceso
- Registra todas las validaciones en logs

### PASO 1: Carga de Diccionario de Homologación
- Lee archivo Excel del diccionario
- Selecciona las 2 primeras columnas
- Normaliza strings para matching
- Registra número de entradas en diccionario

### PASO 2: Listado de Archivos
- Busca todos los archivos .xlsx en `datos_entrada/`
- Valida que existan archivos para procesar
- Si no hay archivos: registra error en log y detiene proceso
- Reporta cantidad de archivos encontrados

### PASO 3: Procesamiento de Archivos
Para cada archivo:
- Lee datos saltando primeras 8 filas (encabezados en fila 9)
- Lee todas las columnas como texto inicialmente
- Calcula hash xxHash64 para validación de integridad
- Agrega columna `archivo_origen` con nombre del archivo
- Registra cantidad de registros y hash
- Maneja errores de lectura sin detener el proceso completo

### PASO 4: Consolidación
- Combina todos los DataFrames con bind_rows
- Preserva todas las columnas de todos los archivos
- Reporta dimensiones del consolidado
- Valida conteo total de registros

### PASO 5: Normalización de Fechas
Columnas procesadas:
- FECHA GENERACION RECETA
- FECHA VIGENCIA RECETA
- FECHA DESPACHO
- FECHA PROXIMO DESPACHO
- FECHA DE NACIMIENTO
- FECHA VENCIMIENTO
- FECHA DESPACHO DOMICILIO

Conversión: Formato DD-MM-YYYY a tipo Date de R (YYYY-MM-DD)

### PASO 5.5: Normalización de Horas y Columnas Derivadas
**Horas procesadas:**
- HORA GENERACION RECETA
- HORA DESPACHO
- HORA DESPACHO DOMICILIO

Conversión: Formato decimal Excel (0-1) a HH:MM:SS

**Columna derivada:**
- `extension_17`: Indica si el despacho fue >= 17:00 hrs (SI/NO)
- Calcula y reporta estadísticas de despachos en horario extendido

### PASO 6: Aplicación de Homologación iGLOBAL
- Detecta automáticamente columna ARTICULO
- Normaliza strings de artículos (trim, uppercase)
- Deduplicar diccionario por articulo_norm
- Realiza left join con diccionario
- Crea columna `codigo_iglobal`
- Reporta estadísticas: homologados vs sin homologar
- Registra advertencias si hay duplicados en diccionario

### PASO 7: Optimización de Tipos de Datos
Convierte a numérico:
- N°, NUMERO RCETA, ID ARTICULO HIJO
- TOTAL RECETADO, CANT POR PERIODICIDAD
- TOTAL PRESCRIPCIONES POR RECETA
- ID RECETA
- EDAD AÑOS, EDAD MESES, EDAD DIAS

### PASO 8: Validación de Integridad
- Calcula hash xxHash64 del consolidado final
- Valida balance de masas: total_entradas == total_consolidado
- Si hay discrepancia: registra error y detiene proceso
- Reporta validación exitosa

### PASO 9: Escritura de Parquet
Configuración de escritura:
- Compresión: Snappy
- Row group size: 10,000 registros
- Data page size: 1 MB
- Write statistics: TRUE
- Use dictionary: TRUE

Reporta:
- Ruta del archivo generado
- Tamaño en MB
- Configuración de compresión

### PASO 10: Validación Final y Balance de Masas
- Lee metadata del Parquet generado (sin cargar datos)
- Verifica número de columnas y filas
- Compara: entradas_suma == consolidado == salida_parquet
- Si hay desbalance: elimina Parquet y detiene proceso
- Guarda auditoría de hashes en CSV

### REPORTE FINAL
- Resumen ejecutivo con todas las métricas
- Total de archivos procesados
- Total de registros
- Dimensiones finales
- Balance de masas
- Información de compresión
- Timestamp de finalización

---

## ARCHIVO DE SALIDA

### Ubicación
`datos_salida/recetas_despachadas_consolidado.parquet`

### Características Técnicas
- **Formato:** Apache Parquet v2.0
- **Compresión:** Snappy (balance óptimo velocidad/tamaño)
- **Row Groups:** 10,000 registros por grupo
- **Encoding:** Dictionary encoding para columnas repetitivas
- **Estadísticas:** Min/Max/Null count por columna
- **Compatibilidad:** Power BI, RStudio, Python pandas, Apache Spark, DuckDB

### Ventajas del Formato Parquet
- Compresión eficiente (5-10x vs CSV)
- Lectura selectiva de columnas (no carga todo el archivo)
- Lectura selectiva de filas mediante predicates
- Preserva tipos de datos nativos (Date, Numeric, etc.)
- Metadatos embebidos para autodescripción

### Lectura en RStudio
```r
library(arrow)
df <- read_parquet("datos_salida/recetas_despachadas_consolidado.parquet")

# Lectura selectiva de columnas
df_subset <- read_parquet(
  "datos_salida/recetas_despachadas_consolidado.parquet",
  col_select = c("FECHA DESPACHO", "ARTICULO", "CANTIDAD DESPACHADA")
)

# Lectura con filtros (predicates)
library(dplyr)
df_filtrado <- read_parquet(
  "datos_salida/recetas_despachadas_consolidado.parquet",
  as_data_frame = TRUE
) %>% filter(`FECHA DESPACHO` >= as.Date("2025-01-01"))
```

### Lectura en Power BI
1. Obtener datos
2. Más... → Archivo → Parquet
3. Seleccionar archivo `recetas_despachadas_consolidado.parquet`
4. Cargar o Transformar datos

### Lectura en Python
```python
import pandas as pd
import pyarrow.parquet as pq

# Leer todo el archivo
df = pd.read_parquet("datos_salida/recetas_despachadas_consolidado.parquet")

# Leer columnas específicas
df_subset = pd.read_parquet(
    "datos_salida/recetas_despachadas_consolidado.parquet",
    columns=["FECHA DESPACHO", "ARTICULO", "CANTIDAD DESPACHADA"]
)

# Leer con filtros
table = pq.read_table(
    "datos_salida/recetas_despachadas_consolidado.parquet",
    filters=[("FECHA DESPACHO", ">=", "2025-01-01")]
)
df_filtrado = table.to_pandas()
```

---

## SISTEMA DE LOGS

### Ubicación
`logs/`

### Tipos de Archivos

#### 1. Logs de Ejecución
**Formato:** `etl_YYYYMMDD_HHMMSS.log`

**Contenido:**
- Timestamp de cada operación
- Nivel de log (INFO, WARN, ERROR)
- Mensajes descriptivos de cada paso
- Estadísticas de procesamiento
- Errores y advertencias

**Ejemplo:**
```
[2026-01-23 14:30:15] [INFO] Inicio ETL BIG DATA - RECETAS DESPACHADAS
[2026-01-23 14:30:15] [INFO] Directorio raíz proyecto: C:/MASSIVE/ETL/ETL_farmacia_recetas_despachadas
[2026-01-23 14:30:16] [INFO] Carpeta datos_salida existe: C:/MASSIVE/ETL/ETL_farmacia_recetas_despachadas/datos_salida
[2026-01-23 14:30:16] [INFO] Carpeta datos_entrada existe: C:/MASSIVE/ETL/ETL_farmacia_recetas_despachadas/datos_entrada
[2026-01-23 14:30:17] [INFO] Diccionario cargado: 1,245 entradas
[2026-01-23 14:30:18] [INFO] Archivos encontrados: 3
[2026-01-23 14:30:20] [INFO] Procesando archivo [1/3]: recetas_enero.xlsx
[2026-01-23 14:30:45] [INFO] Registros leídos: 87,543
```

#### 2. Auditoría de Hashes
**Formato:** `hash_audit_YYYYMMDD_HHMMSS.csv`

**Estructura:**
| archivo             | hash                             | registros |
|---------------------|----------------------------------|-----------|
| recetas_enero.xlsx  | f3c8d9a7b2e1f456...              | 87543     |
| recetas_febrero.xlsx| a1b2c3d4e5f6g789...              | 92187     |

**Propósito:**
- Validar integridad de archivos de entrada
- Detectar cambios en archivos entre ejecuciones
- Auditoría forense de datos procesados

---

## MANEJO DE ERRORES

### Errores Comunes y Soluciones

#### 1. Carpeta datos_entrada no encontrada
**Error:** "PROCESO DETENIDO: Carpeta datos_entrada creada pero vacía"

**Causa:** Primera ejecución o carpeta eliminada

**Solución:**
1. El sistema crea automáticamente la carpeta
2. Colocar archivos XLSX en `datos_entrada/`
3. Ejecutar nuevamente el pipeline

#### 2. No se encontraron archivos XLSX
**Error:** "No se encontraron archivos XLSX en: [ruta]"

**Causa:** Carpeta `datos_entrada/` existe pero está vacía

**Solución:**
- Verificar que los archivos tengan extensión .xlsx (no .xls)
- Confirmar que los archivos estén directamente en `datos_entrada/` (no en subcarpetas)

#### 3. Error de lectura de archivo Excel
**Comportamiento:** Registra error pero continúa con siguiente archivo

**Causas posibles:**
- Archivo abierto en Excel
- Archivo corrupto
- Formato no estándar

**Solución:**
- Cerrar archivo en Excel
- Verificar que encabezados estén en fila 9
- Revisar log para detalles del error

#### 4. Desbalance de masas
**Error:** "Desbalance de masas: entradas=X, consolidado=Y, salida=Z"

**Causa:** Pérdida o duplicación de registros durante procesamiento

**Comportamiento:** El sistema elimina el Parquet generado y detiene el proceso

**Solución:**
- Revisar logs para identificar el paso donde ocurrió el desbalance
- Verificar integridad de archivos de entrada
- Contactar al desarrollador si persiste

#### 5. Memoria insuficiente
**Error:** "Error: cannot allocate vector of size X MB"

**Causa:** Archivos demasiado grandes para RAM disponible

**Solución:**
- Procesar archivos en lotes más pequeños
- Aumentar RAM del sistema
- Dividir archivos grandes en varios archivos más pequeños

#### 6. Diccionario no encontrado
**Error:** Al intentar leer `diccionario_farmacia_iglobal.xlsx`

**Solución:**
- Verificar que el archivo exista en `diccionario_homologacion/`
- Confirmar que el nombre sea exacto (sensible a mayúsculas)

---

## VALIDACIONES DE INTEGRIDAD

### 1. Hashing de Archivos
- **Algoritmo:** xxHash64 (extremadamente rápido)
- **Alcance:** Primeras 3 columnas de cada archivo
- **Propósito:** Detectar cambios en archivos fuente

### 2. Balance de Masas
- **Validación 1:** Suma de registros de entrada == Registros consolidados
- **Validación 2:** Registros consolidados == Registros en Parquet
- **Acción si falla:** Elimina Parquet y detiene proceso

### 3. Validación de Conteo
- Suma acumulativa durante lectura de archivos
- Verificación después de bind_rows
- Verificación después de escribir Parquet
- Tres puntos de control independientes

### 4. Validación de Schema
- Lee metadata de Parquet generado
- Verifica número de columnas
- Confirma tipos de datos preservados

---

## OPTIMIZACIONES IMPLEMENTADAS

### Rendimiento
- Lectura eficiente con readxl (gestión automática de memoria)
- Row groups optimizados para lectura selectiva
- Compresión Snappy para balance velocidad/tamaño
- Dictionary encoding para columnas con valores repetidos

### Escalabilidad
- Diseño modular para fácil extensión
- Procesamiento independiente de cada archivo
- Manejo de errores no bloqueante por archivo
- Logs detallados para debugging

### Portabilidad
- Rutas robustas con here::here()
- Independiente del directorio de trabajo
- Compatible con Windows, Linux, Mac
- Validación automática de estructura

### Mantenibilidad
- Código documentado con comentarios claros
- Separación de concerns (ETL vs Logging)
- Configuración centralizada
- Logs estructurados para auditoría

---

## LIMITACIONES CONOCIDAS

1. **Formato de entrada:** Solo archivos XLSX (no XLS, no CSV)
2. **Estructura de encabezados:** Debe estar en fila 9 exactamente
3. **Memoria:** Limitado por RAM disponible para archivos muy grandes
4. **Paralelización:** Procesa archivos secuencialmente (no en paralelo)
5. **Validación de datos:** No valida calidad de datos (valores faltantes, outliers, etc.)

---

## RENDIMIENTO ESPERADO

### Tiempos de Procesamiento (Referencia)

| Tamaño Archivo | Registros | RAM Usada | Tiempo Lectura | Tiempo Total |
|----------------|-----------|-----------|----------------|--------------|
| 10 MB          | 20,000    | ~200 MB   | ~5 seg         | ~15 seg      |
| 50 MB          | 100,000   | ~800 MB   | ~20 seg        | ~45 seg      |
| 100 MB         | 200,000   | ~1.5 GB   | ~40 seg        | ~90 seg      |
| 200 MB         | 400,000   | ~3 GB     | ~80 seg        | ~3 min       |

*Medido en sistema: Intel i5, 16GB RAM, SSD*

### Compresión Esperada
- Ratio típico: 5-8x con Snappy
- Archivo 100 MB XLSX → 15-20 MB Parquet
- Mejora con mayor repetición de valores

---

## MANTENIMIENTO

### Actualización de Diccionario
1. Editar archivo: `diccionario_homologacion/diccionario_farmacia_iglobal.xlsx`
2. Mantener estructura de 2 columnas
3. Sin necesidad de modificar código
4. Próxima ejecución usará diccionario actualizado

### Agregar Nuevas Columnas Derivadas
Editar [etl_bigdata.R](src/etl/etl_bigdata.R) en sección "PASO 5.5" después de la creación de `extension_17`

### Modificar Configuración de Parquet
Editar [etl_bigdata.R](src/etl/etl_bigdata.R) en sección "CONFIGURACIÓN":
```r
PARQUET_CONFIG <- list(
  row_group_size = 10000,    # Modificar según necesidad
  compression = "snappy",     # Opciones: snappy, gzip, zstd
  use_dictionary = TRUE       # TRUE para mejor compresión
)
```

### Cambiar Ubicación de Carpetas
Modificar [etl_bigdata.R](src/etl/etl_bigdata.R) en sección "CONFIGURACIÓN":
```r
INPUT_DIR <- here("nueva_carpeta_entrada")
OUTPUT_DIR <- here("nueva_carpeta_salida")
```

---

## SEGURIDAD Y PRIVACIDAD

### Consideraciones
- Los archivos de entrada pueden contener datos sensibles (RUT, nombres, diagnósticos)
- Los logs no registran datos de pacientes, solo metadatos estadísticos
- El Parquet de salida contiene todos los datos originales sin anonimización

### Recomendaciones
- Restringir acceso a carpetas `datos_entrada/` y `datos_salida/`
- No compartir logs públicamente (pueden contener nombres de archivos)
- Implementar controles de acceso a nivel de sistema operativo
- Considerar encriptación de archivos Parquet si se almacenan en servidores compartidos

---

## SOPORTE TECNICO

### Información de Contacto
**Anghello Vega**  
Gestor de Datos  
Departamento de Salud Municipal de Temuco

### Reporte de Problemas
Al reportar un problema, incluir:
1. Versión del sistema (1.2.1)
2. Contenido del archivo log más reciente
3. Descripción del error observado
4. Tamaño y cantidad de archivos de entrada
5. Sistema operativo y versión de R

### Logs de Debugging
Los archivos en `logs/` contienen información detallada para diagnóstico:
- Timestamp de cada operación
- Estadísticas de procesamiento
- Mensajes de error completos
- Auditoría de hashes

---

## HISTORIAL DE VERSIONES

### v1.2.1 (2026-01-23)
- Implementación de rutas robustas con here::here()
- Validación automática de estructura de directorios
- Creación automática de datos_salida si no existe
- Validación y detención controlada si falta datos_entrada
- Mejoras en sistema de logging con registro de directorios
- Logging mejorado para todas las validaciones críticas

### v1.2.0 (2026-01-15)
- Optimización para Big Data (200k+ registros)
- Configuración de row groups para Power BI
- Normalización de horas desde formato Excel
- Columna derivada extension_17
- Sistema de hashing para integridad
- Balance de masas estricto

### v1.1.0 (2025-12)
- Implementación de homologación iGLOBAL
- Procesamiento de múltiples archivos
- Normalización de fechas
- Salida en formato Parquet

### v1.0.0 (2025-11)
- Versión inicial
- Procesamiento básico de Excel a CSV

---

## LICENCIA

Este software es de uso interno para el Departamento de Salud Municipal de Temuco.
Todos los derechos reservados.

---

## CONTACTO

Para consultas técnicas o solicitudes de mejoras, contactar a:

**Anghello Vega**  
Gestor de Datos  
Departamento de Salud Municipal de Temuco

---

Documento generado el 23 de enero de 2026
