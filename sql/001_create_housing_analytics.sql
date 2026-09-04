-- Phase 9 reviewed DDL. This file has not been executed against Snowflake.
-- The connection must select the intended database and warehouse. Deliberately
-- omitted: CREATE DATABASE/WAREHOUSE, credentials, roles, users, and grants.

CREATE SCHEMA IF NOT EXISTS HOUSING_ANALYTICS
    COMMENT = 'Validated monthly Canadian CMA housing analytics';

USE SCHEMA HOUSING_ANALYTICS;

-- Reproducible, high-churn staging persists across sessions for troubleshooting
-- but does not incur Fail-safe storage. Batch IDs isolate concurrent/retried loads.
CREATE TRANSIENT TABLE IF NOT EXISTS STG_HOUSING_MONTHLY (
    LOAD_BATCH_ID                         VARCHAR(36)     NOT NULL,
    VALIDATION_PROFILE                    VARCHAR(32)     NOT NULL,
    LOADED_AT_UTC                         TIMESTAMP_TZ(6) NOT NULL,
    REFERENCE_MONTH                       DATE            NOT NULL,
    CMA_CODE                              VARCHAR(3)      NOT NULL,
    DWELLING_TYPE                         VARCHAR(64)     NOT NULL,
    HOUSING_STARTS                        NUMBER(38,0),
    HOUSING_COMPLETIONS                   NUMBER(38,0),
    HOUSING_UNDER_CONSTRUCTION            NUMBER(38,0),
    ACTIVITY_MEASURE_COUNT                NUMBER(5,0),
    HAS_ACTIVITY_DATA                     BOOLEAN         NOT NULL,
    ACTIVITY_RELEASE_TIMESTAMP            TIMESTAMP_NTZ(6),
    ACTIVITY_ARCHIVE_SHA256               VARCHAR(64),
    STARTS_HOMEOWNER                      NUMBER(38,0),
    STARTS_RENTAL                         NUMBER(38,0),
    STARTS_CONDOMINIUM                    NUMBER(38,0),
    STARTS_COOPERATIVE                    NUMBER(38,0),
    STARTS_OTHER_MARKET                   NUMBER(38,0),
    MARKET_MEMBER_COUNT                   NUMBER(5,0),
    HAS_MARKET_DATA                       BOOLEAN         NOT NULL,
    MARKET_RELEASE_TIMESTAMP              TIMESTAMP_NTZ(6),
    MARKET_ARCHIVE_SHA256                 VARCHAR(64),
    GEOGRAPHY                             VARCHAR(200)    NOT NULL,
    HAS_COMPLETE_ACTIVITY                 BOOLEAN         NOT NULL,
    HAS_COMPLETE_MARKET_BREAKDOWN         BOOLEAN         NOT NULL,
    MARKET_STARTS_TOTAL                   NUMBER(38,0),
    NEW_HOUSING_PRICE_INDEX               NUMBER(18,4),
    NEW_HOUSE_PRICE_INDEX                 NUMBER(18,4),
    NEW_LAND_PRICE_INDEX                  NUMBER(18,4),
    PRICE_INDEX_COMPONENT_COUNT           NUMBER(5,0),
    PRICE_RELEASE_TIMESTAMP               TIMESTAMP_NTZ(6),
    PRICE_ARCHIVE_SHA256                  VARCHAR(64),
    HAS_PRICE_INDEX_DATA                  BOOLEAN         NOT NULL,
    HAS_COMPLETE_PRICE_INDEX              BOOLEAN         NOT NULL,
    RESIDENTIAL_PERMIT_VALUE_DOLLARS      NUMBER(24,4),
    PERMIT_RELEASE_TIMESTAMP              TIMESTAMP_NTZ(6),
    PERMIT_ARCHIVE_SHA256                 VARCHAR(64),
    HAS_PERMIT_DATA                       BOOLEAN         NOT NULL,
    COMPLETION_TO_START_RATIO             FLOAT,
    STARTS_3_MONTH_AVERAGE                FLOAT,
    STARTS_YEAR_OVER_YEAR_PCT             FLOAT,
    UNDER_CONSTRUCTION_MONTH_CHANGE       NUMBER(38,0),
    HAS_12_MONTH_ANOMALY_BASELINE         BOOLEAN         NOT NULL,
    STARTS_PRIOR_12_MONTH_AVERAGE         FLOAT,
    STARTS_PRIOR_12_MONTH_STDDEV          FLOAT,
    STARTS_ANOMALY_ZSCORE                 FLOAT,
    STARTS_ANOMALY_FLAG                   BOOLEAN,
    REFERENCE_YEAR                        NUMBER(4,0)     NOT NULL
)
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Batch-addressed validated rows awaiting atomic publication';

-- Permanent serving table. The natural key is
-- (REFERENCE_MONTH, CMA_CODE, DWELLING_TYPE); uniqueness is enforced by the
-- pipeline validation and scoped replacement, not an unenforced PK declaration.
CREATE TABLE IF NOT EXISTS FCT_HOUSING_MONTHLY (
    REFERENCE_MONTH                       DATE            NOT NULL,
    CMA_CODE                              VARCHAR(3)      NOT NULL,
    DWELLING_TYPE                         VARCHAR(64)     NOT NULL,
    HOUSING_STARTS                        NUMBER(38,0),
    HOUSING_COMPLETIONS                   NUMBER(38,0),
    HOUSING_UNDER_CONSTRUCTION            NUMBER(38,0),
    ACTIVITY_MEASURE_COUNT                NUMBER(5,0),
    HAS_ACTIVITY_DATA                     BOOLEAN         NOT NULL,
    ACTIVITY_RELEASE_TIMESTAMP            TIMESTAMP_NTZ(6),
    ACTIVITY_ARCHIVE_SHA256               VARCHAR(64),
    STARTS_HOMEOWNER                      NUMBER(38,0),
    STARTS_RENTAL                         NUMBER(38,0),
    STARTS_CONDOMINIUM                    NUMBER(38,0),
    STARTS_COOPERATIVE                    NUMBER(38,0),
    STARTS_OTHER_MARKET                   NUMBER(38,0),
    MARKET_MEMBER_COUNT                   NUMBER(5,0),
    HAS_MARKET_DATA                       BOOLEAN         NOT NULL,
    MARKET_RELEASE_TIMESTAMP              TIMESTAMP_NTZ(6),
    MARKET_ARCHIVE_SHA256                 VARCHAR(64),
    GEOGRAPHY                             VARCHAR(200)    NOT NULL,
    HAS_COMPLETE_ACTIVITY                 BOOLEAN         NOT NULL,
    HAS_COMPLETE_MARKET_BREAKDOWN         BOOLEAN         NOT NULL,
    MARKET_STARTS_TOTAL                   NUMBER(38,0),
    NEW_HOUSING_PRICE_INDEX               NUMBER(18,4),
    NEW_HOUSE_PRICE_INDEX                 NUMBER(18,4),
    NEW_LAND_PRICE_INDEX                  NUMBER(18,4),
    PRICE_INDEX_COMPONENT_COUNT           NUMBER(5,0),
    PRICE_RELEASE_TIMESTAMP               TIMESTAMP_NTZ(6),
    PRICE_ARCHIVE_SHA256                  VARCHAR(64),
    HAS_PRICE_INDEX_DATA                  BOOLEAN         NOT NULL,
    HAS_COMPLETE_PRICE_INDEX              BOOLEAN         NOT NULL,
    RESIDENTIAL_PERMIT_VALUE_DOLLARS      NUMBER(24,4),
    PERMIT_RELEASE_TIMESTAMP              TIMESTAMP_NTZ(6),
    PERMIT_ARCHIVE_SHA256                 VARCHAR(64),
    HAS_PERMIT_DATA                       BOOLEAN         NOT NULL,
    COMPLETION_TO_START_RATIO             FLOAT,
    STARTS_3_MONTH_AVERAGE                FLOAT,
    STARTS_YEAR_OVER_YEAR_PCT             FLOAT,
    UNDER_CONSTRUCTION_MONTH_CHANGE       NUMBER(38,0),
    HAS_12_MONTH_ANOMALY_BASELINE         BOOLEAN         NOT NULL,
    STARTS_PRIOR_12_MONTH_AVERAGE         FLOAT,
    STARTS_PRIOR_12_MONTH_STDDEV          FLOAT,
    STARTS_ANOMALY_ZSCORE                 FLOAT,
    STARTS_ANOMALY_FLAG                   BOOLEAN,
    REFERENCE_YEAR                        NUMBER(4,0)     NOT NULL,
    SOURCE_LOAD_BATCH_ID                  VARCHAR(36)     NOT NULL,
    SOURCE_VALIDATION_PROFILE             VARCHAR(32)     NOT NULL,
    CREATED_AT_UTC                        TIMESTAMP_TZ(6) NOT NULL,
    UPDATED_AT_UTC                        TIMESTAMP_TZ(6) NOT NULL
)
    COMMENT = 'Monthly CMA and canonical dwelling-type housing analytics fact';

CREATE TABLE IF NOT EXISTS ELT_LOAD_AUDIT (
    LOAD_BATCH_ID                         VARCHAR(36)     NOT NULL,
    VALIDATION_PROFILE                    VARCHAR(32)     NOT NULL,
    REFERENCE_START                       DATE,
    REFERENCE_END                         DATE,
    STATUS                                VARCHAR(16)     NOT NULL,
    VALIDATED_ROW_COUNT                   NUMBER(38,0),
    STAGED_ROW_COUNT                      NUMBER(38,0),
    PUBLISHED_ROW_COUNT                   NUMBER(38,0),
    VALIDATION_METRICS                    VARIANT,
    STARTED_AT_UTC                        TIMESTAMP_TZ(6) NOT NULL,
    COMPLETED_AT_UTC                      TIMESTAMP_TZ(6),
    ERROR_MESSAGE                         VARCHAR(4000)
)
    COMMENT = 'One row per attempted validated Snowflake publication batch';
