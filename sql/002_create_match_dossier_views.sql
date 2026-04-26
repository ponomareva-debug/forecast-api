create or replace view public.v_match_dossier_debug as
select
  f.id as fixture_id,
  f.league_code,
  f.home_team,
  f.away_team,
  f.kickoff_at,

  fc.id as candidate_id,
  fc.selection_code,
  fc.bookmaker_code,
  fc.market_code,
  round((1.0 / nullif(fc.implied_probability, 0))::numeric, 2) as odds_value,
  fc.model_probability,
  fc.implied_probability,
  fc.fair_probability,
  fc.edge,
  fc.ev,
  fc.confidence,
  fc.candidate_status,

  pb.feature_value->>'home_probability' as pb_home_probability,
  pb.feature_value->>'draw_probability' as pb_draw_probability,
  pb.feature_value->>'away_probability' as pb_away_probability,
  pb.feature_value->>'home_goal_expectation' as pb_home_goal_expectation,
  pb.feature_value->>'away_goal_expectation' as pb_away_goal_expectation,

  xg.feature_value->'deltas'->>'elo_delta_home_minus_away' as xg_elo_delta,
  xg.feature_value->'deltas'->>'form3_delta_home_minus_away' as xg_form3_delta,
  xg.feature_value->'deltas'->>'form5_delta_home_minus_away' as xg_form5_delta,
  xg.feature_value->>'has_complete_snapshot' as xg_has_complete_snapshot,
  xg.feature_value->'home'->>'elo' as xg_home_elo,
  xg.feature_value->'away'->>'elo' as xg_away_elo,
  xg.feature_value->'home'->>'form3' as xg_home_form3,
  xg.feature_value->'away'->>'form3' as xg_away_form3,
  xg.feature_value->'home'->>'form5' as xg_home_form5,
  xg.feature_value->'away'->>'form5' as xg_away_form5,
  xg.feature_value->'home'->>'latest_match_date' as xg_home_latest_match_date,
  xg.feature_value->'away'->>'latest_match_date' as xg_away_latest_match_date,

  ce.feature_value->'deltas'->>'elo_delta_home_minus_away' as clubelo_elo_delta,
  ce.feature_value->>'has_complete_snapshot' as clubelo_has_complete_snapshot,
  ce.feature_value->'home'->>'team' as clubelo_home_team,
  ce.feature_value->'away'->>'team' as clubelo_away_team,
  ce.feature_value->'home'->>'elo' as clubelo_home_elo,
  ce.feature_value->'away'->>'elo' as clubelo_away_elo,
  ce.feature_value->'home'->>'rank' as clubelo_home_rank,
  ce.feature_value->'away'->>'rank' as clubelo_away_rank,
  ce.feature_value->'home'->>'country' as clubelo_home_country,
  ce.feature_value->'away'->>'country' as clubelo_away_country,

  us.feature_value->'deltas'->>'avg_xg_diff_delta_home_minus_away' as understat_xg_diff_delta,
  us.feature_value->'deltas'->>'avg_xg_for_delta_home_minus_away' as understat_xg_for_delta,
  us.feature_value->'deltas'->>'avg_xg_against_delta_home_minus_away' as understat_xg_against_delta,
  us.feature_value->>'has_complete_snapshot' as understat_has_complete_snapshot,
  us.feature_value->'home'->>'avg_xg_for' as understat_home_avg_xg_for,
  us.feature_value->'away'->>'avg_xg_for' as understat_away_avg_xg_for,
  us.feature_value->'home'->>'avg_xg_against' as understat_home_avg_xg_against,
  us.feature_value->'away'->>'avg_xg_against' as understat_away_avg_xg_against,
  us.feature_value->'home'->>'avg_xg_diff' as understat_home_avg_xg_diff,
  us.feature_value->'away'->>'avg_xg_diff' as understat_away_avg_xg_diff,
  us.feature_value->'home'->>'matches_count' as understat_home_matches_count,
  us.feature_value->'away'->>'matches_count' as understat_away_matches_count,

  espn.feature_value->>'has_complete_snapshot' as espn_has_complete_snapshot,
  espn.feature_value->'schedule_match'->>'game_id' as espn_game_id,
  espn.feature_value->'schedule_match'->>'match_quality' as espn_match_quality,
  espn.feature_value->'schedule_match'->>'date' as espn_game_date,
  espn.feature_value->'deltas'->>'avg_total_shots_delta_home_minus_away' as espn_shots_delta,
  espn.feature_value->'deltas'->>'avg_shots_on_target_delta_home_minus_away' as espn_sot_delta,
  espn.feature_value->'deltas'->>'avg_total_goals_delta_home_minus_away' as espn_goals_delta,
  espn.feature_value->'deltas'->>'avg_goals_conceded_delta_home_minus_away' as espn_goals_conceded_delta,
  espn.feature_value->'home_lineup_recent'->>'avg_total_shots' as espn_home_avg_total_shots,
  espn.feature_value->'away_lineup_recent'->>'avg_total_shots' as espn_away_avg_total_shots,
  espn.feature_value->'home_lineup_recent'->>'avg_shots_on_target' as espn_home_avg_sot,
  espn.feature_value->'away_lineup_recent'->>'avg_shots_on_target' as espn_away_avg_sot,
  espn.feature_value->'home_lineup_recent'->>'avg_total_goals' as espn_home_avg_total_goals,
  espn.feature_value->'away_lineup_recent'->>'avg_total_goals' as espn_away_avg_total_goals,
  espn.feature_value->'home_lineup_recent'->>'avg_goals_conceded' as espn_home_avg_goals_conceded,
  espn.feature_value->'away_lineup_recent'->>'avg_goals_conceded' as espn_away_avg_goals_conceded,
  espn.feature_value->'home_lineup_recent'->>'avg_yellow_cards' as espn_home_avg_yellow_cards,
  espn.feature_value->'away_lineup_recent'->>'avg_yellow_cards' as espn_away_avg_yellow_cards,
  espn.feature_value->'home_lineup_recent'->>'avg_red_cards' as espn_home_avg_red_cards,
  espn.feature_value->'away_lineup_recent'->>'avg_red_cards' as espn_away_avg_red_cards,
  espn.feature_value->'home_lineup_recent'->>'matches_count' as espn_home_matches_count,
  espn.feature_value->'away_lineup_recent'->>'matches_count' as espn_away_matches_count

from public.fixtures f
left join public.forecast_candidates fc
  on fc.fixture_id = f.id
left join public.match_features pb
  on pb.fixture_id = f.id
  and pb.source = 'penaltyblog'
  and pb.feature_key = 'dixon_coles_1x2'
left join public.match_features xg
  on xg.fixture_id = f.id
  and xg.source = 'xgabora'
  and xg.feature_key = 'elo_form_snapshot'
left join public.match_features ce
  on ce.fixture_id = f.id
  and ce.source = 'soccerdata_clubelo'
  and ce.feature_key = 'current_elo_snapshot'
left join public.match_features us
  on us.fixture_id = f.id
  and us.source = 'soccerdata_understat'
  and us.feature_key = 'recent_xg_snapshot'
left join public.match_features espn
  on espn.fixture_id = f.id
  and espn.source = 'soccerdata_espn'
  and espn.feature_key = 'schedule_lineup_snapshot';

create or replace view public.v_match_dossier_active as
select *
from public.v_match_dossier_debug
where kickoff_at > now()
  and candidate_id is not null
  and pb_home_probability is not null
  and xg_has_complete_snapshot = 'true'
  and clubelo_has_complete_snapshot = 'true'
  and understat_has_complete_snapshot = 'true'
  and espn_has_complete_snapshot = 'true';
