create or replace view public.v_candidate_publishable_v1 as
select
  *,
  case
    when candidate_status <> 'generated' then 'not_generated'
    when ev <= 0 then 'negative_or_zero_ev'
    when edge <= 0 then 'negative_or_zero_edge'
    when selection_code = 'draw' then 'draw_excluded_v1'
    when odds_value < 1.50 then 'odds_too_low'
    when odds_value > 6.00 then 'odds_too_high'
    when multisource_alignment_label = 'contradicted' then 'contradicted_by_sources'
    when source_contradiction_count >= 4 then 'too_many_contradictions'
    when multisource_score_v1 < 0.75 then 'score_too_low'
    else 'publishable'
  end as publish_filter_reason,

  case
    when candidate_status = 'generated'
      and ev > 0
      and edge > 0
      and selection_code <> 'draw'
      and odds_value between 1.50 and 6.00
      and multisource_alignment_label in ('strong_supported', 'supported')
      and source_support_count >= 4
      and source_contradiction_count <= 2
      and multisource_score_v1 >= 0.90
    then true
    else false
  end as premium_eligible_v1,

  case
    when candidate_status = 'generated'
      and ev > 0
      and edge > 0
      and selection_code <> 'draw'
      and odds_value between 1.50 and 4.80
      and multisource_alignment_label in ('strong_supported', 'supported', 'mixed')
      and source_support_count >= 3
      and source_contradiction_count <= 3
      and multisource_score_v1 >= 0.80
    then true
    else false
  end as free_eligible_v1

from public.v_candidate_multisource_score_v1;

create or replace view public.v_candidate_publishable_ranked_v1 as
select *
from public.v_candidate_publishable_v1
where free_eligible_v1 = true
   or premium_eligible_v1 = true
order by
  premium_eligible_v1 desc,
  multisource_score_v1 desc,
  ev desc,
  confidence desc;
