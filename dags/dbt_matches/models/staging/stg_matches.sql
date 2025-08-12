SELECT 
  tournament,
  match_info,
  status,
  team_a as first_team,
  team_b as second_team,
  nullif(score,'"- - -"') as score,
  time
FROM {{ source('raw_data','MATCHES') }}
