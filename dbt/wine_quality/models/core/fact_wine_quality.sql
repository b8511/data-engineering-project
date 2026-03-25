{{
    config(
        materialized='table',
        cluster_by=['wine_type', 'quality_category']
    )
}}

WITH unioned AS (
    SELECT * FROM {{ ref('stg_red_wine') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_white_wine') }}
),

enriched AS (
    SELECT
        ROW_NUMBER() OVER () AS wine_id,
        *,
        CASE
            WHEN quality <= 4 THEN 'low'
            WHEN quality <= 6 THEN 'medium'
            ELSE 'high'
        END AS quality_category
    FROM unioned
)

SELECT * FROM enriched
