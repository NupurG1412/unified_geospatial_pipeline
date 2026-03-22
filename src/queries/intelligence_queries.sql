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