WITH parsed_scores AS (
  SELECT 
    tournament,
    match_info,
    status,
    team_a,
    team_b,
    score,
    TRY_TO_NUMBER(SPLIT(REPLACE(score, '"', ''), ' - ')[0]::STRING) AS first_team_score,
    TRY_TO_NUMBER(SPLIT(REPLACE(score, '"', ''), ' - ')[1]::STRING) AS second_team_score,
    time
  FROM {{ source('raw_data','PLAYED_MATCHES') }}
)

SELECT 
  tournament,
  match_info,
  status,
  team_a AS first_team,
  team_b AS second_team,
  score,
  first_team_score,
  second_team_score,
  time,
  CASE
    WHEN first_team_score > second_team_score THEN team_a
    WHEN first_team_score < second_team_score THEN team_b
    WHEN first_team_score = second_team_score THEN 'Draw'
    ELSE NULL
  END AS winner
FROM parsed_scores
