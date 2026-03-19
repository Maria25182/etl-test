# Design Notes — ETL Pipeline

## Por qué pandas y no PySpark

Decidí usar pandas con pyarrow en lugar de PySpark principalmente por practicidad. Para el volumen de datos que maneja este pipeline (órdenes de una API, dimensiones en CSV) levantar un contexto de Spark simplemente no tiene sentido: agrega dependencia de JVM, tiempo de arranque y complejidad de configuración sin ningún beneficio real. Con pandas el job corre en segundos, el código es directo y los tests son triviales de escribir.

Dicho esto, la lógica de transformación en `transforms.py` trabaja con DataFrames que tienen la misma interfaz conceptual que Spark DataFrames, así que migrar a PySpark si el volumen crece sería relativamente sencillo. El umbral donde empezaría a evaluar ese cambio sería alrededor de 50-100M de filas por ejecución o la necesidad de procesamiento realmente distribuido.

---

## Particionado

La tabla `fact_order` se particiona por `order_date` en formato `YYYY-MM-DD`, lo que produce:

```
output/curated/fact_order/order_date=2025-08-20/data.parquet
output/curated/fact_order/order_date=2025-08-21/data.parquet
```

Esto sigue la convención Hive que entienden Redshift Spectrum, Athena y herramientas como Power BI sobre S3. Permite hacer `COPY` a Redshift apuntando a prefijos específicos y hace el partition pruning automático en queries con filtros de fecha. Las tablas de dimensiones (`dim_user`, `dim_product`) son pequeñas y no se particionan.

---

## Modelado de las tablas

Para `dim_user` y `dim_product` usé `DISTSTYLE ALL` porque son tablas pequeñas que se usan mucho en joins; replicarlas a todos los nodos de Redshift evita movimiento de datos. Para `fact_order` usé `DISTKEY(user_id)` para que los joins con `dim_user` queden co-localizados, y `SORTKEY(order_date, created_at)` para que los escaneos por rango de fechas lean menos bloques.

Las foreign keys en Redshift son declarativas y no se validan, pero el optimizador las usa para generar mejores planes de ejecución, así que las incluí igual.

---

## Idempotencia

La estrategia es simple: antes de escribir una partición de fecha, el job borra el directorio de esa partición si ya existe y lo crea de nuevo. Así, correr el job dos veces con los mismos datos produce exactamente los mismos archivos sin duplicados.

En Redshift, para cargas desde S3, uso el patrón staging: `COPY` a tabla temporal → `DELETE` de producción por `order_id` → `INSERT` desde staging. Esto está documentado en los comentarios de `redshift-ddl.sql`.

---

## Incrementalidad

El flag `--since` (o su alias `--last-processed`) recibe un timestamp en ISO8601 y filtra las órdenes cuyo `created_at` sea posterior a ese valor. En producción ese timestamp vendría de algún registro de estado — puede ser una tabla `etl_watermark` en la base de datos, un archivo `_last_run.json` guardado en S3, o simplemente una variable en el orquestador (Airflow, Step Functions).Al terminar bien, se guarda el último timestamp procesado para la siguiente corrida

---

## Registros malformados

Tomé las siguientes decisiones para los casos borde del esquema:

| Caso | Decisión | Motivo |
|---|---|---|
| `created_at` nulo | Rechazar, mover a `orders_rejected.json` | Sin fecha no se puede particionar ni rastrear temporalmente |
| `items` vacío | Rechazar, mover a `orders_rejected.json` | Una orden sin ítems no tiene sentido de negocio |
| `items[*].price` nulo | Reemplazar por 0.0, advertencia en log | El ítem existe, solo falta el precio; mejor conservar la orden |
| `order_id` duplicado | Quedarse con el último registro | Asumimos que el registro más reciente es la versión correcta |

Los rechazados quedan en `output/raw/orders_rejected.json` con un campo `_reject_reason` para facilitar el análisis posterior.

---

## Carga a Redshift

```sql
COPY fact_order
FROM 's3://mi-bucket/curated/fact_order/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
FORMAT AS PARQUET;
```

Para cargas incrementales sin duplicados se usa staging table: copiar a temporal, borrar de producción los `order_id` que ya existen, insertar desde la temporal. El script completo está en `redshift-ddl.sql`.

---

## Monitorización en producción

En producción el job emitiría los mismos logs estructurados que ya produce (timestamp, nivel, módulo, mensaje) pero hacia CloudWatch Logs o Datadog, con retención de 30 días.

Las métricas que publicaría como custom metrics serían: registros recibidos, registros rechazados, filas escritas en curated, duración total del job y número de reintentos a la API.

Las alertas que configuraría son cuatro: si la tasa de rechazo supera el 5% probablemente el esquema de la API cambió; si se escriben 0 filas en un día laborable algo falló en la fuente; si la duración es el doble de la media histórica hay un problema de rendimiento; y si el job falla por cualquier razón, llega alerta de inmediato con el detalle del error.

El hecho de guardar siempre el raw JSON permite re-procesar cualquier día anterior sin volver a llamar a la API, lo que simplifica bastante la recuperación ante fallos.

---

## Trade-offs asumidos

Parquet con compresión Snappy es la elección obvia para Redshift: es el formato nativo del `COPY` y ocupa mucho menos espacio que CSV. No implementé cifrado en reposo porque es un entorno local, pero en producción los buckets S3 tendrían SSE-KMS habilitado. También dejé fuera la integración MSSQL real porque requiere driver ODBC y servidor corriendo; el código en `db.py` soporta ambas rutas y la fallback a CSV funciona sin configuración adicional.
