create table if not exists public.ai_forecast_reports (
  id bigserial primary key,
  published_forecast_id bigint references public.published_forecasts(id) on delete set null,
  candidate_id bigint,
  fixture_id bigint,
  publication_type text,
  status text not null default 'created',
  ai_model text,
  dossier jsonb,
  research_context jsonb,
  agent_outputs jsonb,
  final_verdict jsonb,
  telegram_text text,
  created_at timestamptz not null default now()
);

create index if not exists ai_forecast_reports_published_forecast_id_idx
on public.ai_forecast_reports(published_forecast_id);

create index if not exists ai_forecast_reports_fixture_id_idx
on public.ai_forecast_reports(fixture_id);

create index if not exists ai_forecast_reports_created_at_idx
on public.ai_forecast_reports(created_at desc);
