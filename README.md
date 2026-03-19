# ETL Pipeline — Prueba Técnica Ingeniero de Datos

Pipeline ETL local que toma órdenes de una API mock en JSON, las combina con datos de usuarios y productos en CSV, aplica transformaciones y deja los resultados listos en Parquet particionado por fecha, listo para ingestión en Redshift.

---

## Qué hace este pipeline

El flujo es sencillo: lee las órdenes (del JSON o de la API si está disponible), valida y limpia los registros, elimina duplicados, y escribe dos capas de salida. La capa raw guarda el JSON tal como llegó, y la capa curated guarda el resultado transformado en Parquet particionado por fecha. Las dimensiones de usuarios y productos se leen desde CSV (o MSSQL si hay credenciales configuradas).

---

## Estructura del proyecto

```
etl-test/
├─ README.md
├─ requirements.txt
├─ sample_data/
│  ├─ api_orders.json      ← 7 órdenes con casos borde incluidos
│  ├─ users.csv
│  └─ products.csv
├─ src/
│  ├─ etl_job.py           ← punto de entrada, orquesta todo
│  ├─ transforms.py        ← toda la lógica de transformación
│  ├─ api_client.py        ← llamada a la API con reintentos
│  └─ db.py                ← carga usuarios y productos (CSV o MSSQL)
├─ sql/
│  └─ redshift-ddl.sql     ← DDL de las tres tablas + queries de ejemplo
├─ tests/
│  └─ test_transforms.py   ← 23 tests unitarios
├─ output/
│  ├─ etl.log
│  ├─ raw/                 ← JSON original recibido
│  └─ curated/             ← Parquet particionado por fecha
└─ docs/
   └─ design_notes.md      ← decisiones tomadas y por qué
```

---

## Pasos para ejecutar

### 1. Descomprimir y entrar al directorio

```bash
unzip etl-test.zip
cd etl-test
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Correr el job completo

```bash
python -m src.etl_job
```

Esto genera todo en `output/`. Se puede ejecutar dos veces seguidas y el resultado es exactamente el mismo (idempotencia por overwrite de partición).

### 4. Carga incremental — solo órdenes nuevas

```bash
python -m src.etl_job --since 2025-08-21T00:00:00Z

# También funciona así:
python -m src.etl_job --last-processed 2025-08-21T00:00:00Z
```

Solo procesa órdenes con `created_at` posterior al timestamp indicado.

### 5. Correr los tests

```bash
python -m pytest tests/ -v
```

Deben pasar los 23 tests. Cubren validación, deduplicación, transformaciones y mock de la API.

---

## Lo que genera el job

```
output/
├─ etl.log                                        ← log completo de la ejecución
├─ raw/
│  ├─ orders_full.json                            ← todos los registros tal como llegaron
│  ├─ orders_rejected.json                        ← los que fallaron validación, con motivo
│  └─ orders_2025-08-21T00-00-00.json             ← ejemplo de ejecución incremental
└─ curated/
   ├─ fact_order/
   │  ├─ order_date=2025-08-20/data.parquet
   │  ├─ order_date=2025-08-21/data.parquet
   │  └─ order_date=2025-08-22/data.parquet
   ├─ dim_user/data.parquet
   └─ dim_product/data.parquet
```

---

## Variables de entorno opcionales

Si quieres apuntar a una API real o a MSSQL, crea un `.env` (no se incluye en el repo):

```bash
# API
export API_BASE_URL=http://localhost:8000
export API_TIMEOUT=10

# MSSQL — si no están definidas, el job usa los CSV sin problema
export MSSQL_SERVER=localhost
export MSSQL_DB=mydb
export MSSQL_USER=sa
export MSSQL_PASS=your_password
```

---

## Decisiones principales

- **pandas en lugar de PySpark**: el volumen de datos no justifica levantar Spark. Con pandas y pyarrow el job corre en segundos localmente y el código es mucho más fácil de leer y testear.
- **Idempotencia**: cada vez que el job escribe una partición, primero borra el directorio existente y lo vuelve a crear. Correrlo dos veces da el mismo resultado.
- **Incrementalidad**: el flag `--since` filtra por `created_at`. En producción ese valor vendría de una tabla de watermark o un archivo de estado en S3.
- **Registros malformados**: `created_at` nulo o `items` vacío van a `orders_rejected.json`. Precio nulo dentro de un ítem se reemplaza por 0.0 y se deja pasar con un warning en el log.
- **Parquet particionado por fecha**: permite a Redshift hacer COPY directo desde S3 y soporta partition pruning en Athena/Spectrum.

---

## Tiempo empleado y supuestos

Tomó alrededor de 5 horas. La API mock no está corriendo, así que el job lee directamente de `sample_data/api_orders.json` como fallback. MSSQL tampoco está configurado, se usan los CSV. Ambas cosas están documentadas en `docs/design_notes.md`.
