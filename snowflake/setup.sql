-- ============================================================
-- Olist Analytics Pipeline — Snowflake Bootstrap
-- Run once as ACCOUNTADMIN to set up all infrastructure.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── Warehouse ─────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS OLIST_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT          = 'Compute warehouse for Olist Analytics Pipeline';

-- ── Database ──────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS OLIST_DB
    COMMENT = 'Olist E-Commerce Analytics Pipeline';

-- ── Layer schemas (Bronze → Silver → Gold) ───────────────────────────────
CREATE SCHEMA IF NOT EXISTS OLIST_DB.RAW
    COMMENT = 'Bronze: raw CSV data loaded via Snowflake internal stage';

CREATE SCHEMA IF NOT EXISTS OLIST_DB.STAGING
    COMMENT = 'Silver: cleaned and typed dbt staging models (stg_*)';

CREATE SCHEMA IF NOT EXISTS OLIST_DB.INTERMEDIATE
    COMMENT = 'Silver+: joined / enriched dbt intermediate models (int_*)';

CREATE SCHEMA IF NOT EXISTS OLIST_DB.MARTS
    COMMENT = 'Gold: business-facing analytical mart tables (mart_*)';

CREATE SCHEMA IF NOT EXISTS OLIST_DB.DEV_RAW
    COMMENT = 'Dev target for CI runs — mirrors RAW schema';

CREATE SCHEMA IF NOT EXISTS OLIST_DB.DEV_STAGING     COMMENT = 'Dev staging';
CREATE SCHEMA IF NOT EXISTS OLIST_DB.DEV_INTERMEDIATE COMMENT = 'Dev intermediate';
CREATE SCHEMA IF NOT EXISTS OLIST_DB.DEV_MARTS        COMMENT = 'Dev marts';

-- ── Role ──────────────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS OLIST_ROLE;

GRANT USAGE  ON WAREHOUSE OLIST_WH                 TO ROLE OLIST_ROLE;
GRANT USAGE  ON DATABASE  OLIST_DB                 TO ROLE OLIST_ROLE;

-- Grant full access on all schemas
GRANT ALL ON SCHEMA OLIST_DB.RAW                   TO ROLE OLIST_ROLE;
GRANT ALL ON SCHEMA OLIST_DB.STAGING               TO ROLE OLIST_ROLE;
GRANT ALL ON SCHEMA OLIST_DB.INTERMEDIATE          TO ROLE OLIST_ROLE;
GRANT ALL ON SCHEMA OLIST_DB.MARTS                 TO ROLE OLIST_ROLE;
GRANT ALL ON SCHEMA OLIST_DB.DEV_RAW               TO ROLE OLIST_ROLE;
GRANT ALL ON SCHEMA OLIST_DB.DEV_STAGING           TO ROLE OLIST_ROLE;
GRANT ALL ON SCHEMA OLIST_DB.DEV_INTERMEDIATE      TO ROLE OLIST_ROLE;
GRANT ALL ON SCHEMA OLIST_DB.DEV_MARTS             TO ROLE OLIST_ROLE;

-- Allow future tables to be accessible automatically
GRANT ALL ON FUTURE TABLES IN SCHEMA OLIST_DB.RAW              TO ROLE OLIST_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA OLIST_DB.STAGING          TO ROLE OLIST_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA OLIST_DB.INTERMEDIATE      TO ROLE OLIST_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA OLIST_DB.MARTS             TO ROLE OLIST_ROLE;
GRANT ALL ON FUTURE VIEWS  IN SCHEMA OLIST_DB.STAGING          TO ROLE OLIST_ROLE;
GRANT ALL ON FUTURE VIEWS  IN SCHEMA OLIST_DB.INTERMEDIATE      TO ROLE OLIST_ROLE;
GRANT ALL ON FUTURE VIEWS  IN SCHEMA OLIST_DB.MARTS             TO ROLE OLIST_ROLE;

-- ── Service user for Airflow + dbt ────────────────────────────────────────
CREATE USER IF NOT EXISTS OLIST_SVC_USER
    PASSWORD            = 'ChangeMe123!'     -- update before running
    DEFAULT_ROLE        = OLIST_ROLE
    DEFAULT_WAREHOUSE   = OLIST_WH
    DEFAULT_NAMESPACE   = OLIST_DB.RAW
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT             = 'Service account used by Airflow and dbt';

GRANT ROLE OLIST_ROLE TO USER OLIST_SVC_USER;

-- ── File format ───────────────────────────────────────────────────────────
USE SCHEMA OLIST_DB.RAW;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE                     = CSV
    SKIP_HEADER              = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                  = ('NULL', 'null', '', 'N/A')
    EMPTY_FIELD_AS_NULL      = TRUE
    DATE_FORMAT              = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT         = 'YYYY-MM-DD HH24:MI:SS'
    COMMENT                  = 'Standard CSV format for Olist dataset files';

-- ── Internal stage ────────────────────────────────────────────────────────
CREATE STAGE IF NOT EXISTS OLIST_DB.RAW.OLIST_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT     = 'Internal stage: upload Olist CSV files here before COPY INTO';

-- ── Raw tables (Bronze layer) ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_ORDERS (
    ORDER_ID                        VARCHAR(32),
    CUSTOMER_ID                     VARCHAR(32),
    ORDER_STATUS                    VARCHAR(20),
    ORDER_PURCHASE_TIMESTAMP        TIMESTAMP_NTZ,
    ORDER_APPROVED_AT               TIMESTAMP_NTZ,
    ORDER_DELIVERED_CARRIER_DATE    TIMESTAMP_NTZ,
    ORDER_DELIVERED_CUSTOMER_DATE   TIMESTAMP_NTZ,
    ORDER_ESTIMATED_DELIVERY_DATE   TIMESTAMP_NTZ,
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_CUSTOMERS (
    CUSTOMER_ID                     VARCHAR(32),
    CUSTOMER_UNIQUE_ID              VARCHAR(32),
    CUSTOMER_ZIP_CODE_PREFIX        VARCHAR(10),
    CUSTOMER_CITY                   VARCHAR(100),
    CUSTOMER_STATE                  VARCHAR(5),
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_ORDER_ITEMS (
    ORDER_ID                        VARCHAR(32),
    ORDER_ITEM_ID                   INTEGER,
    PRODUCT_ID                      VARCHAR(32),
    SELLER_ID                       VARCHAR(32),
    SHIPPING_LIMIT_DATE             TIMESTAMP_NTZ,
    PRICE                           FLOAT,
    FREIGHT_VALUE                   FLOAT,
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_PRODUCTS (
    PRODUCT_ID                      VARCHAR(32),
    PRODUCT_CATEGORY_NAME           VARCHAR(100),
    PRODUCT_NAME_LENGTH             INTEGER,
    PRODUCT_DESCRIPTION_LENGTH      INTEGER,
    PRODUCT_PHOTOS_QTY              INTEGER,
    PRODUCT_WEIGHT_G                FLOAT,
    PRODUCT_LENGTH_CM               FLOAT,
    PRODUCT_HEIGHT_CM               FLOAT,
    PRODUCT_WIDTH_CM                FLOAT,
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_SELLERS (
    SELLER_ID                       VARCHAR(32),
    SELLER_ZIP_CODE_PREFIX          VARCHAR(10),
    SELLER_CITY                     VARCHAR(100),
    SELLER_STATE                    VARCHAR(5),
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_ORDER_REVIEWS (
    REVIEW_ID                       VARCHAR(32),
    ORDER_ID                        VARCHAR(32),
    REVIEW_SCORE                    INTEGER,
    REVIEW_COMMENT_TITLE            VARCHAR(200),
    REVIEW_COMMENT_MESSAGE          VARCHAR(2000),
    REVIEW_CREATION_DATE            TIMESTAMP_NTZ,
    REVIEW_ANSWER_TIMESTAMP         TIMESTAMP_NTZ,
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_ORDER_PAYMENTS (
    ORDER_ID                        VARCHAR(32),
    PAYMENT_SEQUENTIAL              INTEGER,
    PAYMENT_TYPE                    VARCHAR(20),
    PAYMENT_INSTALLMENTS            INTEGER,
    PAYMENT_VALUE                   FLOAT,
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS OLIST_DB.RAW.RAW_PRODUCT_CATEGORY_TRANSLATION (
    PRODUCT_CATEGORY_NAME           VARCHAR(100),
    PRODUCT_CATEGORY_NAME_ENGLISH   VARCHAR(100),
    _LOADED_AT                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Verification query ───────────────────────────────────────────────────
-- Run after setup to confirm all objects were created:
-- SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
-- FROM OLIST_DB.INFORMATION_SCHEMA.TABLES
-- ORDER BY TABLE_SCHEMA, TABLE_NAME;
