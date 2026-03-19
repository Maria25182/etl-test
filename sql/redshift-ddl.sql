-- =============================================================================
-- redshift-ddl.sql
-- Star schema for the orders pipeline.
-- Compatible with Amazon Redshift and DuckDB.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_user
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_user (
    user_id     VARCHAR(64)  NOT NULL,
    email       VARCHAR(255),
    created_at  DATE,
    country     VARCHAR(8),

    -- Redshift best-practices
    PRIMARY KEY (user_id)
)
DISTSTYLE ALL          -- small table → replicate to all nodes
SORTKEY (user_id);

-- -----------------------------------------------------------------------------
-- dim_product
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    sku         VARCHAR(64)    NOT NULL,
    name        VARCHAR(255),
    category    VARCHAR(100),
    price       DECIMAL(12,2),

    PRIMARY KEY (sku)
)
DISTSTYLE ALL
SORTKEY (sku);

-- -----------------------------------------------------------------------------
-- fact_order
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_order (
    order_id            VARCHAR(64)     NOT NULL,
    user_id             VARCHAR(64)     NOT NULL,
    amount              DECIMAL(12,2),
    currency            VARCHAR(8),
    created_at          TIMESTAMP,
    order_date          DATE            NOT NULL,    -- partition / sort key
    total_items_qty     INTEGER,
    total_items_value   DECIMAL(12,2),
    metadata_source     VARCHAR(64),
    metadata_promo      VARCHAR(128),
    _ingested_at        TIMESTAMP       DEFAULT GETDATE(),

    PRIMARY KEY (order_id)
)
DISTKEY (user_id)           -- co-locate with dim_user joins
SORTKEY (order_date, created_at);

-- Foreign key declarations (informational in Redshift — not enforced)
ALTER TABLE fact_order ADD FOREIGN KEY (user_id)    REFERENCES dim_user(user_id);

-- =============================================================================
-- Example queries
-- =============================================================================

-- 1. Deduplicate orders (in case of re-load): keep last ingested record
-- Used during COPY + staging table pattern.
/*
BEGIN;
  -- Stage into a temp table
  CREATE TEMP TABLE staging_fact_order (LIKE fact_order);
  COPY staging_fact_order FROM 's3://bucket/curated/fact_order/'
      IAM_ROLE 'arn:aws:iam::ACCOUNT:role/RedshiftS3Role'
      FORMAT AS PARQUET;

  -- Delete existing rows that appear in staging
  DELETE FROM fact_order
  USING staging_fact_order s
  WHERE fact_order.order_id = s.order_id;

  -- Insert fresh rows
  INSERT INTO fact_order SELECT * FROM staging_fact_order;

  DROP TABLE staging_fact_order;
COMMIT;
*/

-- 2. Daily revenue aggregate
/*
SELECT
    order_date,
    currency,
    COUNT(*)            AS num_orders,
    SUM(amount)         AS total_revenue,
    AVG(amount)         AS avg_order_value,
    SUM(total_items_qty) AS units_sold
FROM fact_order
GROUP BY order_date, currency
ORDER BY order_date DESC;
*/

-- 3. Top users by revenue (join to dim_user)
/*
SELECT
    u.user_id,
    u.email,
    u.country,
    COUNT(f.order_id)   AS num_orders,
    SUM(f.amount)       AS lifetime_value
FROM fact_order f
JOIN dim_user u ON f.user_id = u.user_id
GROUP BY u.user_id, u.email, u.country
ORDER BY lifetime_value DESC
LIMIT 20;
*/

-- 4. Best-selling products (via items JSON — requires unnesting in Redshift Spectrum
--    or pre-flattened staging table)
/*
SELECT
    p.sku,
    p.name,
    p.category,
    SUM(f.total_items_qty)   AS units_sold,
    SUM(f.total_items_value) AS gross_revenue
FROM fact_order f
JOIN dim_product p ON f.metadata_source IS NOT NULL  -- placeholder join
GROUP BY p.sku, p.name, p.category
ORDER BY units_sold DESC;
*/

-- 5. Detect in-flight duplicates (before dedup step)
/*
SELECT order_id, COUNT(*) AS cnt
FROM staging_fact_order
GROUP BY order_id
HAVING COUNT(*) > 1;
*/
