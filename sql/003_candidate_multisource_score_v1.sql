create or replace view public.v_candidate_multisource_score_v1 as
with typed as (
  select
    *,
    nullif(pb_home_probability, '')::numeric as n_pb_home_probability,
    nullif(pb_draw_probability, '')::numeric as n_pb_draw_probability,
    nullif(pb_away_probability, '')::numeric as n_pb_away_probability,
    nullif(xg_elo_delta, '')::numeric as n_xg_elo_delta,
    nullif(xg_form5_delta, '')::numeric as n_xg_form5_delta,
    nullif(clubelo_elo_delta, '')::numeric as n_clubelo_elo_delta,
    nullif(understat_xg_diff_delta, '')::numeric as n_understat_xg_diff_delta,
    nullif(understat_xg_for_delta, '')::numeric as n_understat_xg_for_delta,
    nullif(espn_shots_delta, '')::numeric as n_espn_shots_delta,
    nullif(espn_sot_delta, '')::numeric as n_espn_sot_delta,
    nullif(espn_goals_delta, '')::numeric as n_espn_goals_delta,
    nullif(espn_goals_conceded_delta, '')::numeric as n_espn_goals_conceded_delta
  from public.v_match_dossier_active
), signals as (
  select
    *,
    case selection_code
      when 'home' then n_pb_home_probability
      when 'draw' then n_pb_draw_probability
      when 'away' then n_pb_away_probability
    end as selected_pb_probability,

    case
      when selection_code = 'home' and n_xg_elo_delta > 20 then 1
      when selection_code = 'home' and n_xg_elo_delta < -20 then -1
      when selection_code = 'away' and n_xg_elo_delta < -20 then 1
      when selection_code = 'away' and n_xg_elo_delta > 20 then -1
      else 0
    end as xgabora_elo_signal,

    case
      when selection_code = 'home' and n_xg_form5_delta > 0.05 then 1
      when selection_code = 'home' and n_xg_form5_delta < -0.05 then -1
      when selection_code = 'away' and n_xg_form5_delta < -0.05 then 1
      when selection_code = 'away' and n_xg_form5_delta > 0.05 then -1
      else 0
    end as xgabora_form5_signal,

    case
      when selection_code = 'home' and n_clubelo_elo_delta > 20 then 1
      when selection_code = 'home' and n_clubelo_elo_delta < -20 then -1
      when selection_code = 'away' and n_clubelo_elo_delta < -20 then 1
      when selection_code = 'away' and n_clubelo_elo_delta > 20 then -1
      else 0
    end as clubelo_signal,

    case
      when selection_code = 'home' and n_understat_xg_diff_delta > 0.15 then 1
      when selection_code = 'home' and n_understat_xg_diff_delta < -0.15 then -1
      when selection_code = 'away' and n_understat_xg_diff_delta < -0.15 then 1
      when selection_code = 'away' and n_understat_xg_diff_delta > 0.15 then -1
      else 0
    end as understat_xg_signal,

    case
      when selection_code = 'home' and n_espn_sot_delta > 0.5 then 1
      when selection_code = 'home' and n_espn_sot_delta < -0.5 then -1
      when selection_code = 'away' and n_espn_sot_delta < -0.5 then 1
      when selection_code = 'away' and n_espn_sot_delta > 0.5 then -1
      else 0
    end as espn_sot_signal,

    case
      when selection_code = 'home' and n_espn_shots_delta > 1.0 then 1
      when selection_code = 'home' and n_espn_shots_delta < -1.0 then -1
      when selection_code = 'away' and n_espn_shots_delta < -1.0 then 1
      when selection_code = 'away' and n_espn_shots_delta > 1.0 then -1
      else 0
    end as espn_shots_signal,

    case
      when selection_code = 'home' and n_espn_goals_delta > 0.3 then 1
      when selection_code = 'home' and n_espn_goals_delta < -0.3 then -1
      when selection_code = 'away' and n_espn_goals_delta < -0.3 then 1
      when selection_code = 'away' and n_espn_goals_delta > 0.3 then -1
      else 0
    end as espn_goals_signal,

    case
      when selection_code = 'home' and n_espn_goals_conceded_delta < -0.3 then 1
      when selection_code = 'home' and n_espn_goals_conceded_delta > 0.3 then -1
      when selection_code = 'away' and n_espn_goals_conceded_delta > 0.3 then 1
      when selection_code = 'away' and n_espn_goals_conceded_delta < -0.3 then -1
      else 0
    end as espn_defense_signal
  from typed
), counted as (
  select
    *,
    ((xgabora_elo_signal > 0)::int
      + (xgabora_form5_signal > 0)::int
      + (clubelo_signal > 0)::int
      + (understat_xg_signal > 0)::int
      + (espn_sot_signal > 0)::int
      + (espn_shots_signal > 0)::int
      + (espn_goals_signal > 0)::int
      + (espn_defense_signal > 0)::int) as source_support_count,

    ((xgabora_elo_signal < 0)::int
      + (xgabora_form5_signal < 0)::int
      + (clubelo_signal < 0)::int
      + (understat_xg_signal < 0)::int
      + (espn_sot_signal < 0)::int
      + (espn_shots_signal < 0)::int
      + (espn_goals_signal < 0)::int
      + (espn_defense_signal < 0)::int) as source_contradiction_count
  from signals
)
select
  *,
  round((
    confidence
    + (xgabora_elo_signal * 0.035)
    + (xgabora_form5_signal * 0.025)
    + (clubelo_signal * 0.045)
    + (understat_xg_signal * 0.060)
    + (espn_sot_signal * 0.035)
    + (espn_shots_signal * 0.025)
    + (espn_goals_signal * 0.025)
    + (espn_defense_signal * 0.025)
    + case
        when ev > 0 then least(ev, 1.0) * 0.050
        else greatest(ev, -1.0) * 0.050
      end
    - case when selection_code = 'draw' then 0.080 else 0 end
    - case
        when odds_value > 6 then 0.060
        when odds_value < 1.5 then 0.040
        else 0
      end
  )::numeric, 6) as multisource_score_v1,

  case
    when source_support_count >= 5 and source_contradiction_count <= 1 then 'strong_supported'
    when source_support_count >= source_contradiction_count + 2 then 'supported'
    when source_contradiction_count >= source_support_count + 2 then 'contradicted'
    else 'mixed'
  end as multisource_alignment_label
from counted;
