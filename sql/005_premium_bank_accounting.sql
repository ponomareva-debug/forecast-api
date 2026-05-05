-- Premium virtual bank accounting
-- Purpose: support multiple simultaneously open premium bets without overusing bankroll.
-- Model:
-- 1) When a premium bet is placed, stake is deducted from current_cash immediately.
-- 2) Pending exposure is tracked separately as open_stake.
-- 3) On settlement:
--    won  -> current_cash += stake_amount * odds_value; profit = stake * (odds - 1)
--    lost -> current_cash unchanged; profit = -stake
--    void -> current_cash += stake_amount; profit = 0

create table if not exists public.premium_bank_accounts (
  id bigserial primary key,
  account_code text not null unique default 'MODEL_BANK',
  currency text not null default 'RUB',
  starting_balance numeric not null default 10000,
  current_cash numeric not null default 10000,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

insert into public.premium_bank_accounts (account_code, currency, starting_balance, current_cash)
values ('MODEL_BANK', 'RUB', 10000, 10000)
on conflict (account_code) do nothing;

create table if not exists public.premium_bank_ledger (
  id bigserial primary key,
  account_id bigint references public.premium_bank_accounts(id),

  published_forecast_id bigint not null,
  fixture_id bigint,
  candidate_id bigint,
  publication_type text not null default 'premium',

  home_team text,
  away_team text,
  kickoff_at timestamptz,

  selection_code text,
  selection_label text,
  market_code text,
  odds_value numeric,

  estimated_probability_percent numeric,
  confidence_score numeric,
  verdict text,
  risk_level text,

  bank_before numeric not null,
  stake_percent numeric not null,
  stake_amount numeric not null,
  potential_profit numeric,
  potential_return numeric,
  expected_value_percent numeric,
  expected_profit numeric,

  cash_before numeric,
  cash_after_place numeric,
  settlement_return numeric,
  profit_amount numeric,
  cash_after_settlement numeric,

  result_status text not null default 'pending',
  result_source text,
  home_score integer,
  away_score integer,

  analyst_json jsonb,
  stake_json jsonb,
  placement_json jsonb,
  settlement_json jsonb,

  created_at timestamptz not null default now(),
  placed_at timestamptz not null default now(),
  settled_at timestamptz
);

alter table public.premium_bank_ledger
  add column if not exists account_id bigint references public.premium_bank_accounts(id),
  add column if not exists cash_before numeric,
  add column if not exists cash_after_place numeric,
  add column if not exists settlement_return numeric,
  add column if not exists cash_after_settlement numeric,
  add column if not exists result_source text,
  add column if not exists home_score integer,
  add column if not exists away_score integer,
  add column if not exists placement_json jsonb,
  add column if not exists settlement_json jsonb,
  add column if not exists placed_at timestamptz default now();

create unique index if not exists premium_bank_ledger_published_forecast_id_uidx
  on public.premium_bank_ledger (published_forecast_id);

create index if not exists premium_bank_ledger_account_status_idx
  on public.premium_bank_ledger (account_id, result_status, created_at desc);

create index if not exists premium_bank_ledger_fixture_idx
  on public.premium_bank_ledger (fixture_id);

create index if not exists premium_bank_ledger_settled_at_idx
  on public.premium_bank_ledger (settled_at desc);

create table if not exists public.premium_bank_transactions (
  id bigserial primary key,
  account_id bigint not null references public.premium_bank_accounts(id),
  ledger_id bigint references public.premium_bank_ledger(id),
  published_forecast_id bigint,
  transaction_type text not null,
  amount numeric not null,
  balance_before numeric not null,
  balance_after numeric not null,
  metadata jsonb,
  created_at timestamptz not null default now()
);

create index if not exists premium_bank_transactions_account_created_idx
  on public.premium_bank_transactions (account_id, created_at desc);

create index if not exists premium_bank_transactions_ledger_idx
  on public.premium_bank_transactions (ledger_id);

create or replace function public.get_premium_bank_state(p_account_code text default 'MODEL_BANK')
returns table (
  account_id bigint,
  account_code text,
  currency text,
  starting_balance numeric,
  current_cash numeric,
  open_stake numeric,
  pending_bets_count bigint,
  settled_profit numeric,
  settled_bets_count bigint,
  total_bets_count bigint,
  current_equity numeric,
  roi_percent numeric
)
language sql
security definer
set search_path = public
as $$
  with account as (
    select *
    from public.premium_bank_accounts
    where account_code = p_account_code
    limit 1
  ), stats as (
    select
      a.id as account_id,
      coalesce(sum(l.stake_amount) filter (where l.result_status = 'pending'), 0) as open_stake,
      count(*) filter (where l.result_status = 'pending') as pending_bets_count,
      coalesce(sum(l.profit_amount) filter (where l.result_status in ('won', 'lost', 'void')), 0) as settled_profit,
      count(*) filter (where l.result_status in ('won', 'lost', 'void')) as settled_bets_count,
      count(l.id) as total_bets_count,
      coalesce(sum(l.stake_amount) filter (where l.result_status in ('won', 'lost', 'void')), 0) as settled_stake
    from account a
    left join public.premium_bank_ledger l on l.account_id = a.id
    group by a.id
  )
  select
    a.id,
    a.account_code,
    a.currency,
    a.starting_balance,
    a.current_cash,
    s.open_stake,
    s.pending_bets_count,
    s.settled_profit,
    s.settled_bets_count,
    s.total_bets_count,
    a.current_cash + s.open_stake as current_equity,
    case
      when s.settled_stake > 0 then round((s.settled_profit / s.settled_stake) * 100, 2)
      else null
    end as roi_percent
  from account a
  join stats s on s.account_id = a.id;
$$;

create or replace function public.place_premium_bet(payload jsonb, p_account_code text default 'MODEL_BANK')
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  account_row public.premium_bank_accounts%rowtype;
  existing_row public.premium_bank_ledger%rowtype;
  ledger_row public.premium_bank_ledger%rowtype;
  v_published_forecast_id bigint;
  v_stake_amount numeric;
  v_cash_before numeric;
  v_cash_after numeric;
begin
  v_published_forecast_id := nullif(payload->>'published_forecast_id', '')::bigint;
  v_stake_amount := coalesce(nullif(payload->>'stake_amount', '')::numeric, 0);

  if v_published_forecast_id is null then
    raise exception 'published_forecast_id is required';
  end if;

  select * into existing_row
  from public.premium_bank_ledger
  where published_forecast_id = v_published_forecast_id
  limit 1;

  if found then
    return jsonb_build_object(
      'status', 'ok',
      'idempotent', true,
      'message', 'Premium bet already placed',
      'ledger_id', existing_row.id,
      'result_status', existing_row.result_status,
      'bank_state', (select to_jsonb(s) from public.get_premium_bank_state(p_account_code) s)
    );
  end if;

  select * into account_row
  from public.premium_bank_accounts
  where account_code = p_account_code
  for update;

  if not found then
    insert into public.premium_bank_accounts (account_code, currency, starting_balance, current_cash)
    values (p_account_code, 'RUB', 10000, 10000)
    returning * into account_row;
  end if;

  if v_stake_amount <= 0 then
    raise exception 'stake_amount must be positive';
  end if;

  if v_stake_amount > account_row.current_cash then
    raise exception 'insufficient current_cash: stake %, current_cash %', v_stake_amount, account_row.current_cash;
  end if;

  v_cash_before := account_row.current_cash;
  v_cash_after := account_row.current_cash - v_stake_amount;

  insert into public.premium_bank_ledger (
    account_id,
    published_forecast_id,
    fixture_id,
    candidate_id,
    publication_type,
    home_team,
    away_team,
    kickoff_at,
    selection_code,
    selection_label,
    market_code,
    odds_value,
    estimated_probability_percent,
    confidence_score,
    verdict,
    risk_level,
    bank_before,
    stake_percent,
    stake_amount,
    potential_profit,
    potential_return,
    expected_value_percent,
    expected_profit,
    cash_before,
    cash_after_place,
    result_status,
    analyst_json,
    stake_json,
    placement_json,
    placed_at
  ) values (
    account_row.id,
    v_published_forecast_id,
    nullif(payload->>'fixture_id', '')::bigint,
    nullif(payload->>'candidate_id', '')::bigint,
    coalesce(payload->>'publication_type', 'premium'),
    payload->>'home_team',
    payload->>'away_team',
    nullif(payload->>'kickoff_at', '')::timestamptz,
    payload->>'selection_code',
    payload->>'selection_label',
    payload->>'market_code',
    nullif(payload->>'odds_value', '')::numeric,
    nullif(payload->>'estimated_probability_percent', '')::numeric,
    nullif(payload->>'confidence_score', '')::numeric,
    payload->>'verdict',
    payload->>'risk_level',
    v_cash_before,
    nullif(payload->>'stake_percent', '')::numeric,
    v_stake_amount,
    nullif(payload->>'potential_profit', '')::numeric,
    nullif(payload->>'potential_return', '')::numeric,
    nullif(payload->>'expected_value_percent', '')::numeric,
    nullif(payload->>'expected_profit', '')::numeric,
    v_cash_before,
    v_cash_after,
    'pending',
    payload->'analyst_json',
    payload->'stake_json',
    payload,
    now()
  ) returning * into ledger_row;

  update public.premium_bank_accounts
  set current_cash = v_cash_after,
      updated_at = now()
  where id = account_row.id;

  insert into public.premium_bank_transactions (
    account_id,
    ledger_id,
    published_forecast_id,
    transaction_type,
    amount,
    balance_before,
    balance_after,
    metadata
  ) values (
    account_row.id,
    ledger_row.id,
    v_published_forecast_id,
    'stake_placed',
    -v_stake_amount,
    v_cash_before,
    v_cash_after,
    payload
  );

  return jsonb_build_object(
    'status', 'ok',
    'idempotent', false,
    'message', 'Premium bet placed and stake reserved from current_cash',
    'ledger_id', ledger_row.id,
    'published_forecast_id', ledger_row.published_forecast_id,
    'cash_before', v_cash_before,
    'cash_after', v_cash_after,
    'bank_state', (select to_jsonb(s) from public.get_premium_bank_state(p_account_code) s)
  );
end;
$$;

create or replace function public.settle_premium_bet(
  p_published_forecast_id bigint,
  p_result_status text,
  p_home_score integer default null,
  p_away_score integer default null,
  p_result_source text default 'settlement_flow',
  p_settlement_json jsonb default '{}'::jsonb,
  p_account_code text default 'MODEL_BANK'
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  account_row public.premium_bank_accounts%rowtype;
  ledger_row public.premium_bank_ledger%rowtype;
  v_return numeric := 0;
  v_profit numeric := 0;
  v_cash_before numeric;
  v_cash_after numeric;
  v_transaction_type text;
begin
  if p_result_status not in ('won', 'lost', 'void') then
    raise exception 'p_result_status must be won, lost, or void';
  end if;

  select l.* into ledger_row
  from public.premium_bank_ledger l
  join public.premium_bank_accounts a on a.id = l.account_id
  where l.published_forecast_id = p_published_forecast_id
    and a.account_code = p_account_code
  for update of l;

  if not found then
    raise exception 'premium_bank_ledger row not found for published_forecast_id %', p_published_forecast_id;
  end if;

  if ledger_row.result_status <> 'pending' then
    return jsonb_build_object(
      'status', 'ok',
      'idempotent', true,
      'message', 'Premium bet already settled',
      'ledger_id', ledger_row.id,
      'result_status', ledger_row.result_status,
      'profit_amount', ledger_row.profit_amount,
      'bank_state', (select to_jsonb(s) from public.get_premium_bank_state(p_account_code) s)
    );
  end if;

  select * into account_row
  from public.premium_bank_accounts
  where id = ledger_row.account_id
  for update;

  v_cash_before := account_row.current_cash;

  if p_result_status = 'won' then
    v_return := round(ledger_row.stake_amount * ledger_row.odds_value, 2);
    v_profit := round(ledger_row.stake_amount * (ledger_row.odds_value - 1), 2);
    v_transaction_type := 'settlement_won_return';
  elsif p_result_status = 'lost' then
    v_return := 0;
    v_profit := -ledger_row.stake_amount;
    v_transaction_type := 'settlement_lost';
  else
    v_return := ledger_row.stake_amount;
    v_profit := 0;
    v_transaction_type := 'settlement_void_refund';
  end if;

  v_cash_after := v_cash_before + v_return;

  update public.premium_bank_accounts
  set current_cash = v_cash_after,
      updated_at = now()
  where id = account_row.id;

  update public.premium_bank_ledger
  set result_status = p_result_status,
      result_source = p_result_source,
      home_score = p_home_score,
      away_score = p_away_score,
      settlement_return = v_return,
      profit_amount = v_profit,
      cash_after_settlement = v_cash_after,
      bank_after = v_cash_after,
      settlement_json = p_settlement_json,
      settled_at = now()
  where id = ledger_row.id
  returning * into ledger_row;

  insert into public.premium_bank_transactions (
    account_id,
    ledger_id,
    published_forecast_id,
    transaction_type,
    amount,
    balance_before,
    balance_after,
    metadata
  ) values (
    account_row.id,
    ledger_row.id,
    p_published_forecast_id,
    v_transaction_type,
    v_return,
    v_cash_before,
    v_cash_after,
    jsonb_build_object(
      'result_status', p_result_status,
      'home_score', p_home_score,
      'away_score', p_away_score,
      'profit_amount', v_profit,
      'settlement_json', p_settlement_json
    )
  );

  return jsonb_build_object(
    'status', 'ok',
    'idempotent', false,
    'message', 'Premium bet settled',
    'ledger_id', ledger_row.id,
    'published_forecast_id', ledger_row.published_forecast_id,
    'result_status', ledger_row.result_status,
    'settlement_return', v_return,
    'profit_amount', v_profit,
    'cash_before', v_cash_before,
    'cash_after', v_cash_after,
    'bank_state', (select to_jsonb(s) from public.get_premium_bank_state(p_account_code) s)
  );
end;
$$;

create or replace view public.v_premium_bank_report_daily as
select
  date_trunc('day', settled_at)::date as report_date,
  count(*) as bets_count,
  count(*) filter (where result_status = 'won') as won_count,
  count(*) filter (where result_status = 'lost') as lost_count,
  count(*) filter (where result_status = 'void') as void_count,
  coalesce(sum(stake_amount), 0) as total_staked,
  coalesce(sum(profit_amount), 0) as profit,
  round(coalesce(sum(profit_amount), 0) / nullif(sum(stake_amount), 0) * 100, 2) as roi_percent
from public.premium_bank_ledger
where result_status in ('won', 'lost', 'void')
  and settled_at is not null
group by 1
order by 1 desc;

create or replace view public.v_premium_bank_open_exposure as
select
  a.account_code,
  a.currency,
  a.current_cash,
  coalesce(sum(l.stake_amount), 0) as open_stake,
  count(l.id) as pending_bets_count,
  a.current_cash + coalesce(sum(l.stake_amount), 0) as current_equity
from public.premium_bank_accounts a
left join public.premium_bank_ledger l
  on l.account_id = a.id
  and l.result_status = 'pending'
group by a.id, a.account_code, a.currency, a.current_cash;
