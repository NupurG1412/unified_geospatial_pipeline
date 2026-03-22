-- ============================================================
-- QUERY 1: Proximity Search
-- Find all signals within 1000 km of Mumbai, India
-- ============================================================

SELECT
    signal_id,
    source_id,
    feature_type,
    normalized_value,
    timestamp,
    latitude,
    longitude,
    ROUND(
        ST_Distance(
            geom,
            ST_SetSRID(ST_MakePoint(72.8777, 19.0760), 4326)::geography
        ) / 1000.0
    ) AS distance_km
FROM
    marine_signals
WHERE
    ST_DWithin(
        geom,
        ST_SetSRID(ST_MakePoint(72.8777, 19.0760), 4326)::geography,
        1000000   -- 1000 km in metres
    )
    AND normalized_value IS NOT NULL
ORDER BY
    distance_km ASC;


-- ============================================================
-- QUERY 2: Signal Density Grid
-- Aggregate signals into a 1-degree geographic grid
-- ============================================================

SELECT
    FLOOR(latitude)  AS lat_grid,
    FLOOR(longitude) AS lon_grid,
    feature_type,
    COUNT(*)                           AS signal_count,
    ROUND(AVG(normalized_value)::numeric, 3) AS avg_value,
    ROUND(MIN(normalized_value)::numeric, 3) AS min_value,
    ROUND(MAX(normalized_value)::numeric, 3) AS max_value
FROM
    marine_signals
WHERE
    normalized_value IS NOT NULL
GROUP BY
    FLOOR(latitude),
    FLOOR(longitude),
    feature_type
ORDER BY
    signal_count DESC;


-- ============================================================
-- QUERY 3: Temporal Trend per Feature Type
-- Average normalized value per month per signal type
-- ============================================================

SELECT
    feature_type,
    DATE_TRUNC('month', timestamp)         AS month,
    COUNT(*)                               AS reading_count,
    ROUND(AVG(normalized_value)::numeric, 3) AS avg_value,
    ROUND(STDDEV(normalized_value)::numeric, 3) AS stddev_value
FROM
    marine_signals
WHERE
    normalized_value IS NOT NULL
    AND timestamp IS NOT NULL
GROUP BY
    feature_type,
    DATE_TRUNC('month', timestamp)
ORDER BY
    feature_type,
    month;


-- ============================================================
-- EXTRA QUERY: Dataset Registry Overview
-- Shows provenance and trust level of all ingested datasets
-- ============================================================

SELECT
    dr.dataset_id,
    dr.source,
    dr.trust_level,
    dr.update_frequency,
    dr.ingested_at,
    COUNT(ms.signal_id)   AS total_signals,
    MIN(ms.timestamp)     AS earliest_signal,
    MAX(ms.timestamp)     AS latest_signal
FROM
    dataset_registry dr
LEFT JOIN
    marine_signals ms ON ms.dataset_id = dr.dataset_id
GROUP BY
    dr.dataset_id,
    dr.source,
    dr.trust_level,
    dr.update_frequency,
    dr.ingested_at
ORDER BY
    dr.dataset_id;

-- Signals by Region + Time
SELECT *
FROM marine_signals
WHERE timestamp BETWEEN '2020-01-01' AND '2022-12-31'
AND ST_DWithin(
    geom,
    ST_MakePoint(72.87,19.07)::geography,
    50000
);

-- Feature Type Distribution
SELECT feature_type, COUNT(*) AS count
FROM marine_signals
GROUP BY feature_type
ORDER BY count DESC;

-- Time-based Trend
SELECT DATE(timestamp) AS date, COUNT(*) AS signals
FROM marine_signals
GROUP BY DATE(timestamp)
ORDER BY date;

-- Basic Anomaly Detection
SELECT *
FROM marine_signals
WHERE normalized_value > (
    SELECT AVG(normalized_value) + 2 * STDDEV(normalized_value)
    FROM marine_signals
);

-- High Confidence Signals
SELECT *
FROM marine_signals
WHERE confidence_score > 0.85;