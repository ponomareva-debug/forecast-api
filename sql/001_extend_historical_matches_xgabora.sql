alter table public.historical_matches
add column if not exists home_elo numeric,
add column if not exists away_elo numeric,
add column if not exists form3_home numeric,
add column if not exists form3_away numeric,
add column if not exists form5_home numeric,
add column if not exists form5_away numeric,
add column if not exists odd_home numeric,
add column if not exists odd_draw numeric,
add column if not exists odd_away numeric,
add column if not exists max_home numeric,
add column if not exists max_draw numeric,
add column if not exists max_away numeric;

create index if not exists historical_matches_league_date_idx
on public.historical_matches(league_code, match_date);
