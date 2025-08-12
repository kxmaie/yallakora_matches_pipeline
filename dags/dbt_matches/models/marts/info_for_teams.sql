SELECT 
team,
count (*) AS total_matches,
SUM(CASE WHEN team=winner then 1 ELSE 0 END) AS wins,
SUM(CASE WHEN winner='Draw' THEN 1 ELSE 0 END) AS draws,
SUM(CASE WHEN team != winner AND winner IS NOT NULL THEN 1 ELSE 0 END) AS losses
FROM(
   SELECT first_team AS team , winner FROM {{ ref('stg_played_matches') }}
   UNION ALL
   SELECT second_team , winner from {{ ref('stg_played_matches') }}
)
GROUP BY team