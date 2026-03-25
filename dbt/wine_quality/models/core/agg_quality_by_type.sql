SELECT
    wine_type,
    quality,
    quality_category,
    COUNT(*) AS wine_count,
    ROUND(AVG(alcohol), 2) AS avg_alcohol,
    ROUND(AVG(fixed_acidity), 2) AS avg_fixed_acidity,
    ROUND(AVG(volatile_acidity), 2) AS avg_volatile_acidity,
    ROUND(AVG(residual_sugar), 2) AS avg_residual_sugar,
    ROUND(AVG(ph), 2) AS avg_ph
FROM {{ ref('fact_wine_quality') }}
GROUP BY wine_type, quality, quality_category
ORDER BY wine_type, quality
