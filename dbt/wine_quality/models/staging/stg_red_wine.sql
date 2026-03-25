SELECT
    CAST(fixed_acidity AS FLOAT64) AS fixed_acidity,
    CAST(volatile_acidity AS FLOAT64) AS volatile_acidity,
    CAST(citric_acid AS FLOAT64) AS citric_acid,
    CAST(residual_sugar AS FLOAT64) AS residual_sugar,
    CAST(chlorides AS FLOAT64) AS chlorides,
    CAST(free_sulfur_dioxide AS FLOAT64) AS free_sulfur_dioxide,
    CAST(total_sulfur_dioxide AS FLOAT64) AS total_sulfur_dioxide,
    CAST(density AS FLOAT64) AS density,
    CAST(pH AS FLOAT64) AS ph,
    CAST(sulphates AS FLOAT64) AS sulphates,
    CAST(alcohol AS FLOAT64) AS alcohol,
    CAST(quality AS INT64) AS quality,
    'red' AS wine_type
FROM {{ source('raw', 'external_red_wine') }}
