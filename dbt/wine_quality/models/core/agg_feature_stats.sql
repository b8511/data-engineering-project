WITH features AS (
    SELECT
        wine_type,
        'fixed_acidity' AS feature, fixed_acidity AS value
    FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'volatile_acidity', volatile_acidity FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'citric_acid', citric_acid FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'residual_sugar', residual_sugar FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'chlorides', chlorides FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'free_sulfur_dioxide', free_sulfur_dioxide FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'total_sulfur_dioxide', total_sulfur_dioxide FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'density', density FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'ph', ph FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'sulphates', sulphates FROM {{ ref('fact_wine_quality') }}
    UNION ALL
    SELECT wine_type, 'alcohol', alcohol FROM {{ ref('fact_wine_quality') }}
)

SELECT
    wine_type,
    feature,
    COUNT(*) AS sample_count,
    ROUND(MIN(value), 4) AS min_value,
    ROUND(MAX(value), 4) AS max_value,
    ROUND(AVG(value), 4) AS avg_value,
    ROUND(STDDEV(value), 4) AS stddev_value
FROM features
GROUP BY wine_type, feature
ORDER BY wine_type, feature
