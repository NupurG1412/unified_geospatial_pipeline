
CREATE EXTENSION IF NOT EXISTS postgis;


-- DATASET REGISTRY TABLE
CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_id       SERIAL PRIMARY KEY,
    source           VARCHAR(200)  NOT NULL,
    schema_version   VARCHAR(20)   NOT NULL DEFAULT '1.0',
    update_frequency VARCHAR(50),
    trust_level      VARCHAR(10)   NOT NULL CHECK (trust_level IN ('high', 'medium', 'low')),
    ingested_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    notes            TEXT
);


--  UNIFIED MARINE SIGNALS TABLE
CREATE TABLE IF NOT EXISTS marine_signals (
    signal_id         BIGSERIAL PRIMARY KEY,                 -- updated
    source_id         VARCHAR(200),                          -- original row identifier from source
    timestamp         TIMESTAMP NOT NULL,                             -- observation time
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    geom              GEOGRAPHY(Point, 4326),                -- PostGIS spatial field
    feature_type      VARCHAR(100) NOT NULL,                 -- type of signal/observation
    normalized_value  DOUBLE PRECISION,                                 -- primary measurement value (NULL if missing)
    source_reference  VARCHAR(200),                          -- source name
    dataset_id        INT REFERENCES dataset_registry(dataset_id) ON DELETE SET NULL,
    confidence_score  FLOAT,                                 -- updated intelligence layer
    created_at        TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- DEDUPLICATION CONSTRAINT
ALTER TABLE marine_signals
ADD CONSTRAINT unique_signal
UNIQUE (source_id, timestamp, latitude, longitude, feature_type);

-- SPATIAL INDEX on geom
CREATE INDEX IF NOT EXISTS idx_marine_signals_geom
    ON marine_signals USING GIST (geom);

-- Additional indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_marine_signals_feature_type
    ON marine_signals (feature_type);

CREATE INDEX IF NOT EXISTS idx_marine_signals_timestamp
    ON marine_signals (timestamp);

CREATE INDEX IF NOT EXISTS idx_marine_signals_dataset
    ON marine_signals (dataset_id);

CREATE INDEX IF NOT EXISTS idx_feature_time
    ON marine_signals (feature_type, timestamp);