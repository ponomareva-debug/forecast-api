create table if not exists public.forecast_model_runs (
  id bigserial primary key,
  model_name text not null,
  model_version text not null,
  league_code text not null,
  market_code text not null,
  training_rows int,
  training_start_date date,
  training_end_date date,
  parameters jsonb not null default '{}'::jsonb,
  status text not null default 'completed',
  created_at timestamptz default now()
);

create table if not exists public.match_features (
  id bigserial primary key,
  fixture_id bigint not null references public.fixtures(id) on delete cascade,
  model_run_id bigint references public.forecast_model_runs(id) on delete set null,
  source text not null,
  feature_key text not null,
  feature_value jsonb not null,
  collected_at timestamptz default now()
);

create unique index if not exists match_features_unique_feature
on public.match_features(fixture_id, source, feature_key);

create index if not exists match_features_fixture_id_idx
on public.match_features(fixture_id);

create index if not exists match_features_source_key_idx
on public.match_features(source, feature_key);
