alter table public.forecast_candidates
  drop constraint if exists forecast_candidates_candidate_status_check;

alter table public.forecast_candidates
  add constraint forecast_candidates_candidate_status_check
  check (
    candidate_status in (
      'generated',
      'selected_free',
      'published_free',
      'selected_premium',
      'published_premium'
    )
  );
