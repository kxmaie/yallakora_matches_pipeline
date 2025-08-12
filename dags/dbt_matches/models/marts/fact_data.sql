SELECT 
  tournament,
  match_info,
  status,
  first_team,
  second_team,
  NULL AS first_team_score,
  NULL AS second_team_score,
  NULL AS winner,
  time,
  FALSE AS is_played
FROM {{ ref('stg_matches') }}

UNION ALL

SELECT  
  tournament,
  match_info,
  status,
  first_team,
  second_team,
  first_team_score,
  second_team_score,
  winner,
  time,
  TRUE AS is_played
FROM {{ ref('stg_played_matches') }}
