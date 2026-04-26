import io
import math
from datetime import datetime, timezone
from typing import Optional

from fastapi import Body
from pydantic import BaseModel

from fixed_main import app, get_supabase, utc_now_iso, _latest_odds_by_key, _existing_candidate_statuses
from runtime_extensions import _safe_float, _safe_int, _parse_date, _canonical_name
from soccerdata_extensions import (
    _load_aliases,
    _team_candidates,
    _read_clubelo_dataframe,
    _parse_clubelo_dataframe,
    _find_team_elo,
)
from understat_extensions import (
    _parse_understat_schedule,
    _team_recent_xg_snapshot,
)
from espn_extensions import (
    _try_reader_method,
    _parse_espn_schedule,
    _parse_espn_lineups,
    _aggregate_team_games,
    _find_schedule_match,
    _team_recent_lineup_snapshot,
)


LEAGUES = {
    "EPL": {
        "enabled": True,
        "xgabora_division": "E0",
        "odds_sport_key": "soccer_epl",
        "soccerdata_names": ["ENG-Premier League", "Premier League"],
    },
    "LA_LIGA": {
        "enabled": True,
        "xgabora_division": "SP1",
        "odds_sport_key": "soccer_spain_la_liga",
        "soccerdata_names": ["ESP-La Liga", "La Liga"],
    },
    "SERIE_A": {
        "enabled": True,
        "xgabora_division": "I1",
        "odds_sport_key": "soccer_italy_serie_a",
        "soccerdata_names": ["ITA-Serie A", "Serie A"],
    },
    "BUNDESLIGA": {
        "enabled": True,
        "xgabora_division": "D1",
        "odds_sport_key": "soccer_germany_bundesliga",
        "soccerdata_names": ["GER-Bundesliga", "Bundesliga"],
    },
    "LIGUE_1": {
        "enabled": True,
        "xgabora_division": "F1",
        "odds_sport_key": "soccer_france_ligue_one",
        "soccerdata_names": ["FRA-Ligue 1", "Ligue 1"],
    },
}

DEFAULT_LEAGUES = [league_code for league_code, cfg in LEAGUES.items() if cfg.get("enabled")]
DEFAULT_SEASON_ATTEMPTS = [2025, "2025", "2025-2026", [2025], ["2025"], 2024, "2024", [2024]]


class MultiLeagueRequest(BaseModel):
    leagues: Optional[list[str]] = None


class ImportXgaboraLeaguesRequest(BaseModel):
    leagues: Optional[list[str]] = None
    start_date: str = "2020-07-01"


def _selected_leagues(requested: Optional[list[str]] = None):
    if not requested:
        return DEFAULT_LEAGUES
    selected = []
    for league_code in requested:
        clean = str(league_code).strip().upper()
        if clean in LEAGUES and clean not in selected:
            selected.append(clean)
    return selected


def _json_safe_value(value):
    if value is None:
        return None
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return float(value)
    return value if isinstance(value, str) else str(value)


def _remove_route(path, method):
    app.router.routes = [
        route
        for route in app.router.routes
        if not (
            getattr(route, "path", None) == path
            and method.upper() in getattr(route, "methods", set())
        )
    ]
    app.openapi_schema = None


def _make_soccerdata_reader(source_name, league_code):
    import soccerdata as sd

    cfg = LEAGUES[league_code]
    league_attempts = cfg["soccerdata_names"]
    attempts = []
    reader_cls = getattr(sd, source_name)

    for league in league_attempts:
        for season in DEFAULT_SEASON_ATTEMPTS:
            kwargs = {"leagues": league, "seasons": season}
            try:
                reader = reader_cls(**kwargs)
                attempts.append({"kwargs": kwargs, "status": "created"})
                return reader, attempts
            except Exception as e:
                attempts.append({"kwargs": kwargs, "status": "error", "error": str(e)})

    try:
        reader = reader_cls()
        attempts.append({"kwargs": {}, "status": "created"})
        return reader, attempts
    except Exception as e:
        attempts.append({"kwargs": {}, "status": "error", "error": str(e)})

    raise RuntimeError({"message": f"Could not create {source_name} reader", "league_code": league_code, "attempts": attempts})


def _download_xgabora_matches():
    import requests
    import pandas as pd

    csv_url = "https://raw.githubusercontent.com/xgabora/Club-Football-Match-Data-2000-2025/main/data/Matches.csv"
    response = requests.get(csv_url, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download xgabora CSV: {response.status_code} {response.text[:300]}")
    return pd.read_csv(io.StringIO(response.text))


@app.get("/debug/league-config")
def debug_league_config():
    return {
        "status": "ok",
        "service": "forecast-api",
        "enabled_leagues": DEFAULT_LEAGUES,
        "leagues": LEAGUES,
        "note": "ESPN remains mandatory in SQL views. Leagues without complete ESPN snapshots will not become publishable.",
    }


@app.post("/admin/import-xgabora-leagues")
def import_xgabora_leagues(payload: ImportXgaboraLeaguesRequest = Body(default=ImportXgaboraLeaguesRequest())):
    try:
        import pandas as pd

        supabase = get_supabase()
        league_codes = _selected_leagues(payload.leagues)
        if not league_codes:
            return {"status": "error", "service": "forecast-api", "message": "No valid leagues selected"}

        df = _download_xgabora_matches()
        required_columns = ["Division", "MatchDate", "HomeTeam", "AwayTeam", "FTHome", "FTAway"]
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Missing required xgabora columns",
                "missing_columns": missing_required,
                "available_columns": [str(col) for col in df.columns],
            }

        optional_mapping = {
            "HomeElo": "home_elo",
            "AwayElo": "away_elo",
            "Form3Home": "form3_home",
            "Form3Away": "form3_away",
            "Form5Home": "form5_home",
            "Form5Away": "form5_away",
            "OddHome": "odd_home",
            "OddDraw": "odd_draw",
            "OddAway": "odd_away",
            "MaxHome": "max_home",
            "MaxDraw": "max_draw",
            "MaxAway": "max_away",
        }
        start_date = pd.to_datetime(payload.start_date).date()
        division_counts = df["Division"].astype(str).value_counts().to_dict()
        results = {}

        for league_code in league_codes:
            division = LEAGUES[league_code]["xgabora_division"]
            league_df = df[df["Division"].astype(str) == division].copy()
            raw_rows = int(len(league_df))

            if league_df.empty:
                results[league_code] = {
                    "status": "error",
                    "division": division,
                    "message": "No rows found for division",
                    "raw_rows": raw_rows,
                }
                continue

            league_df["match_date"] = pd.to_datetime(league_df["MatchDate"], errors="coerce", dayfirst=False).dt.date
            league_df["home_goals"] = pd.to_numeric(league_df["FTHome"], errors="coerce")
            league_df["away_goals"] = pd.to_numeric(league_df["FTAway"], errors="coerce")
            league_df = league_df.dropna(subset=["match_date", "HomeTeam", "AwayTeam", "home_goals", "away_goals"])

            min_available_date = str(league_df["match_date"].min()) if not league_df.empty else None
            max_available_date = str(league_df["match_date"].max()) if not league_df.empty else None
            league_df = league_df[league_df["match_date"] >= start_date].copy()

            rows = []
            for _, row in league_df.iterrows():
                home_goals = _safe_float(row["home_goals"])
                away_goals = _safe_float(row["away_goals"])
                if home_goals is None or away_goals is None:
                    continue

                prepared = {
                    "league_code": league_code,
                    "season": None,
                    "match_date": row["match_date"].isoformat(),
                    "home_team": str(row["HomeTeam"]).strip(),
                    "away_team": str(row["AwayTeam"]).strip(),
                    "home_goals": int(home_goals),
                    "away_goals": int(away_goals),
                    "source": "xgabora_matches_csv",
                }
                for source_col, target_col in optional_mapping.items():
                    if source_col in row.index:
                        prepared[target_col] = _safe_float(row[source_col])
                rows.append(prepared)

            if not rows:
                results[league_code] = {
                    "status": "error",
                    "division": division,
                    "message": "No rows prepared after cleaning/date filter",
                    "raw_rows": raw_rows,
                    "min_available_date": min_available_date,
                    "max_available_date": max_available_date,
                }
                continue

            supabase.table("historical_matches").delete().eq("league_code", league_code).execute()
            batch_size = 500
            upserted_total = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                (
                    supabase.table("historical_matches")
                    .upsert(batch, on_conflict="league_code,match_date,home_team,away_team")
                    .execute()
                )
                upserted_total += len(batch)

            teams = sorted(set([r["home_team"] for r in rows] + [r["away_team"] for r in rows]))
            results[league_code] = {
                "status": "ok",
                "division": division,
                "raw_rows": raw_rows,
                "min_available_date": min_available_date,
                "max_available_date": max_available_date,
                "import_start_date": start_date.isoformat(),
                "prepared_rows": int(len(rows)),
                "upserted_rows": int(upserted_total),
                "teams_count": int(len(teams)),
                "teams_sample": teams[:30],
            }

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "xgabora/Club-Football-Match-Data-2000-2025",
            "selected_leagues": league_codes,
            "division_counts_sample": {k: division_counts.get(v["xgabora_division"], 0) for k, v in LEAGUES.items()},
            "results": results,
        }

    except Exception as e:
        return {"status": "error", "service": "forecast-api", "error": str(e), "error_type": type(e).__name__}


def _forecast_one_league(supabase, league_code, now_iso):
    import numpy as np
    import pandas as pd
    import penaltyblog as pb

    market_code = "h2h"
    model_run_id = None
    model_run_insert_error = None
    features_upserted_count = 0
    features_upsert_error = None

    history_resp = (
        supabase.table("historical_matches")
        .select("match_date, home_team, away_team, home_goals, away_goals")
        .eq("league_code", league_code)
        .order("match_date", desc=True)
        .limit(2000)
        .execute()
    )
    history_rows = history_resp.data or []
    if len(history_rows) < 100:
        return {"status": "error", "league_code": league_code, "message": "Not enough historical matches", "historical_rows": len(history_rows)}

    aliases = _load_aliases(supabase, league_code)

    def canonical_team_name(name: str) -> str:
        clean_name = str(name).strip()
        return aliases.get(clean_name, clean_name)

    df = pd.DataFrame(history_rows).copy()
    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
    df["home_goals"] = pd.to_numeric(df["home_goals"], errors="coerce")
    df["away_goals"] = pd.to_numeric(df["away_goals"], errors="coerce")
    df["home_team"] = df["home_team"].astype(str).str.strip()
    df["away_team"] = df["away_team"].astype(str).str.strip()
    df = df.dropna(subset=["match_date", "home_team", "away_team", "home_goals", "away_goals"])
    if len(df) < 100:
        return {"status": "error", "league_code": league_code, "message": "Not enough clean historical matches", "historical_rows_after_cleaning": len(df)}

    df = df.sort_values("match_date", ascending=True).copy()
    training_start_date = df["match_date"].min().date().isoformat()
    training_end_date = df["match_date"].max().date().isoformat()

    try:
        model_run_resp = (
            supabase.table("forecast_model_runs")
            .insert({
                "model_name": "DixonColesGoalModel",
                "model_version": "quant_v1_penaltyblog_multi_league",
                "league_code": league_code,
                "market_code": market_code,
                "training_rows": int(len(df)),
                "training_start_date": training_start_date,
                "training_end_date": training_end_date,
                "parameters": {
                    "xi": 0.001,
                    "history_limit": 2000,
                    "bookmaker_mode": "single_bookmaker",
                    "bookmakers_enabled": ["unibet_uk"],
                    "markets_enabled": ["h2h"],
                    "multi_bookmaker_consensus": False,
                },
                "status": "completed",
            })
            .execute()
        )
        model_run = (model_run_resp.data or [None])[0]
        model_run_id = model_run["id"] if model_run else None
    except Exception as insert_error:
        model_run_insert_error = str(insert_error)

    history_teams = set(df["home_team"].tolist() + df["away_team"].tolist())
    goals_home = np.array(df["home_goals"].to_numpy(), dtype=np.int64, copy=True)
    goals_away = np.array(df["away_goals"].to_numpy(), dtype=np.int64, copy=True)
    team_home = np.array(df["home_team"].astype(str).to_numpy(), dtype=object, copy=True)
    team_away = np.array(df["away_team"].astype(str).to_numpy(), dtype=object, copy=True)
    goals_home.setflags(write=True)
    goals_away.setflags(write=True)
    team_home.setflags(write=True)
    team_away.setflags(write=True)

    weights = pb.models.dixon_coles_weights(df["match_date"], xi=0.001)
    weights = np.array(weights, dtype=np.float64, copy=True)
    weights.setflags(write=True)

    model = pb.models.DixonColesGoalModel(goals_home, goals_away, team_home, team_away, weights)
    model.fit(use_gradient=True, minimizer_options={"maxiter": 3000})

    fixtures_resp = (
        supabase.table("fixtures")
        .select("id, external_event_id, home_team, away_team, kickoff_at")
        .eq("league_code", league_code)
        .eq("event_status", "scheduled")
        .gt("kickoff_at", now_iso)
        .order("kickoff_at", desc=False)
        .execute()
    )
    fixtures = fixtures_resp.data or []
    if not fixtures:
        return {"status": "ok", "league_code": league_code, "generated_candidates": 0, "message": "No scheduled future fixtures found"}

    fixture_ids = [f["id"] for f in fixtures]
    odds_resp = (
        supabase.table("odds_snapshots")
        .select("fixture_id, bookmaker_code, market_code, selection_code, odds_value, snapshot_time")
        .in_("fixture_id", fixture_ids)
        .eq("market_code", market_code)
        .execute()
    )
    odds_rows = odds_resp.data or []
    if not odds_rows:
        return {"status": "ok", "league_code": league_code, "generated_candidates": 0, "message": "No h2h odds found"}

    latest_by_key = _latest_odds_by_key(odds_rows)
    odds_by_fixture_bookmaker = {}
    for row in latest_by_key.values():
        key = (row["fixture_id"], row["bookmaker_code"])
        odds_by_fixture_bookmaker.setdefault(key, []).append(row)

    fixtures_map = {f["id"]: f for f in fixtures}
    existing_statuses = _existing_candidate_statuses(supabase, fixture_ids, market_code)
    selection_to_index = {"home": 0, "draw": 1, "away": 2}

    candidates = []
    skipped_fixtures = []
    predicted_fixtures_count = 0

    for (fixture_id, bookmaker_code), rows in odds_by_fixture_bookmaker.items():
        fixture = fixtures_map.get(fixture_id)
        if not fixture:
            continue

        raw_home_team = str(fixture["home_team"]).strip()
        raw_away_team = str(fixture["away_team"]).strip()
        mapped_home_team = canonical_team_name(raw_home_team)
        mapped_away_team = canonical_team_name(raw_away_team)

        if mapped_home_team not in history_teams or mapped_away_team not in history_teams:
            skipped_fixtures.append({
                "fixture_id": fixture_id,
                "home_team": raw_home_team,
                "away_team": raw_away_team,
                "mapped_home_team": mapped_home_team,
                "mapped_away_team": mapped_away_team,
                "reason": "team_not_in_training_data",
            })
            continue

        by_selection = {
            str(row["selection_code"]).strip(): row
            for row in rows
            if str(row["selection_code"]).strip() in selection_to_index
        }
        if set(by_selection.keys()) != {"home", "draw", "away"}:
            skipped_fixtures.append({"fixture_id": fixture_id, "reason": "missing_h2h_selection", "available_selections": sorted(by_selection.keys())})
            continue

        odds_values = {}
        valid_odds = True
        for selection_code, row in by_selection.items():
            try:
                odds_value = float(row["odds_value"])
            except Exception:
                valid_odds = False
                break
            if not np.isfinite(odds_value) or odds_value <= 1.01 or odds_value > 50:
                valid_odds = False
                break
            odds_values[selection_code] = odds_value
        if not valid_odds:
            skipped_fixtures.append({"fixture_id": fixture_id, "reason": "invalid_odds"})
            continue

        prediction = model.predict(mapped_home_team, mapped_away_team)
        probs = prediction.home_draw_away
        model_probabilities = {"home": float(probs[0]), "draw": float(probs[1]), "away": float(probs[2])}

        try:
            supabase.table("match_features").upsert({
                "fixture_id": fixture_id,
                "model_run_id": model_run_id,
                "source": "penaltyblog",
                "feature_key": "dixon_coles_1x2",
                "feature_value": {
                    "league_code": league_code,
                    "raw_home_team": raw_home_team,
                    "raw_away_team": raw_away_team,
                    "mapped_home_team": mapped_home_team,
                    "mapped_away_team": mapped_away_team,
                    "home_probability": round(float(probs[0]), 6),
                    "draw_probability": round(float(probs[1]), 6),
                    "away_probability": round(float(probs[2]), 6),
                    "home_goal_expectation": round(float(prediction.home_goal_expectation), 6),
                    "away_goal_expectation": round(float(prediction.away_goal_expectation), 6),
                    "model_name": "DixonColesGoalModel",
                    "model_version": "quant_v1_penaltyblog_multi_league",
                    "training_rows": int(len(df)),
                    "training_start_date": training_start_date,
                    "training_end_date": training_end_date,
                },
            }, on_conflict="fixture_id,source,feature_key").execute()
            features_upserted_count += 1
        except Exception as feature_error:
            features_upsert_error = str(feature_error)

        implied_probabilities = {s: 1.0 / odds_values[s] for s in ["home", "draw", "away"]}
        overround = sum(implied_probabilities.values())
        if not np.isfinite(overround) or overround <= 1.0 or overround > 1.35:
            skipped_fixtures.append({"fixture_id": fixture_id, "reason": "bad_overround", "overround": round(float(overround), 6) if np.isfinite(overround) else None})
            continue

        fair_probabilities = {s: implied_probabilities[s] / overround for s in ["home", "draw", "away"]}
        predicted_fixtures_count += 1

        for selection_code in ["home", "draw", "away"]:
            odds_value = odds_values[selection_code]
            model_probability = model_probabilities[selection_code]
            implied_probability = implied_probabilities[selection_code]
            fair_probability = fair_probabilities[selection_code]
            edge = model_probability - fair_probability
            ev = (model_probability * odds_value) - 1.0

            confidence = 0.5
            if ev > 0:
                confidence += min(ev * 2.0, 0.25)
            if edge > 0:
                confidence += min(edge * 1.5, 0.20)
            if 1.45 <= odds_value <= 4.50:
                confidence += 0.05
            if overround <= 1.08:
                confidence += 0.05
            if selection_code == "draw":
                confidence -= 0.05
            if odds_value > 6.0:
                confidence -= 0.10
            confidence = max(0.05, min(confidence, 0.95))

            status_key = (fixture_id, bookmaker_code, market_code, selection_code)
            candidates.append({
                "fixture_id": fixture_id,
                "bookmaker_code": bookmaker_code,
                "market_code": market_code,
                "selection_code": selection_code,
                "model_probability": round(float(model_probability), 6),
                "implied_probability": round(float(implied_probability), 6),
                "fair_probability": round(float(fair_probability), 6),
                "edge": round(float(edge), 6),
                "ev": round(float(ev), 6),
                "confidence": round(float(confidence), 6),
                "candidate_status": existing_statuses.get(status_key, "generated"),
            })

    if candidates:
        upsert_resp = supabase.table("forecast_candidates").upsert(
            candidates,
            on_conflict="fixture_id,bookmaker_code,market_code,selection_code",
        ).execute()
        upserted_count = len(upsert_resp.data or candidates)
    else:
        upserted_count = 0

    return {
        "status": "ok",
        "league_code": league_code,
        "model": "DixonColesGoalModel",
        "generated_candidates": upserted_count,
        "scheduled_fixtures_count": len(fixtures),
        "predicted_fixtures_count": predicted_fixtures_count,
        "latest_h2h_rows_count": len(latest_by_key),
        "skipped_fixtures_count": len(skipped_fixtures),
        "skipped_fixtures_sample": skipped_fixtures[:10],
        "debug_model_run_id": model_run_id,
        "debug_training_start_date": training_start_date,
        "debug_training_end_date": training_end_date,
        "debug_model_run_insert_error": model_run_insert_error,
        "debug_features_upserted_count": features_upserted_count,
        "debug_features_upsert_error": features_upsert_error,
    }


@app.post("/forecast/run-v3")
def forecast_run_v3(payload: MultiLeagueRequest = Body(default=MultiLeagueRequest())):
    try:
        supabase = get_supabase()
        league_codes = _selected_leagues(payload.leagues)
        now_iso = utc_now_iso()
        results = {}
        total_generated = 0

        for league_code in league_codes:
            result = _forecast_one_league(supabase, league_code, now_iso)
            results[league_code] = result
            total_generated += int(result.get("generated_candidates") or 0)

        return {
            "status": "ok",
            "service": "forecast-api",
            "model": "DixonColesGoalModel",
            "version": "run-v3-multi-league",
            "selected_leagues": league_codes,
            "generated_candidates_total": total_generated,
            "results": results,
        }
    except Exception as e:
        return {"status": "error", "service": "forecast-api", "error": str(e), "error_type": type(e).__name__}


@app.post("/features/enrich-xgabora-v2")
def enrich_xgabora_features_v2(payload: MultiLeagueRequest = Body(default=MultiLeagueRequest())):
    try:
        supabase = get_supabase()
        league_codes = _selected_leagues(payload.leagues)
        results = {}
        total_upserted = 0

        for league_code in league_codes:
            aliases = _load_aliases(supabase, league_code)
            fixtures_resp = (
                supabase.table("fixtures")
                .select("id, home_team, away_team, kickoff_at")
                .eq("league_code", league_code)
                .eq("event_status", "scheduled")
                .order("kickoff_at", desc=False)
                .execute()
            )
            fixtures = fixtures_resp.data or []
            if not fixtures:
                results[league_code] = {"status": "ok", "enriched_count": 0, "message": "No scheduled fixtures found"}
                continue

            history_resp = (
                supabase.table("historical_matches")
                .select("match_date, home_team, away_team, home_goals, away_goals, home_elo, away_elo, form3_home, form3_away, form5_home, form5_away, odd_home, odd_draw, odd_away, max_home, max_draw, max_away")
                .eq("league_code", league_code)
                .order("match_date", desc=True)
                .execute()
            )
            history_rows = history_resp.data or []
            if not history_rows:
                results[league_code] = {"status": "error", "enriched_count": 0, "message": "No historical_matches rows found"}
                continue

            prepared_history = []
            for row in history_rows:
                match_date = _parse_date(row.get("match_date"))
                if not match_date:
                    continue
                prepared_history.append({
                    **row,
                    "match_date_parsed": match_date,
                    "mapped_home_team": _canonical_name(row.get("home_team"), aliases),
                    "mapped_away_team": _canonical_name(row.get("away_team"), aliases),
                })

            def result_for_side(row, side):
                home_goals = _safe_int(row.get("home_goals"))
                away_goals = _safe_int(row.get("away_goals"))
                if home_goals is None or away_goals is None:
                    return None
                if side == "home":
                    if home_goals > away_goals:
                        return "win"
                    if home_goals < away_goals:
                        return "loss"
                    return "draw"
                if away_goals > home_goals:
                    return "win"
                if away_goals < home_goals:
                    return "loss"
                return "draw"

            def team_snapshot(team, fixture_date):
                for row in prepared_history:
                    match_date = row["match_date_parsed"]
                    if fixture_date and match_date >= fixture_date:
                        continue
                    if row["mapped_home_team"] == team:
                        side = "home"
                        opponent = row["mapped_away_team"]
                        elo = row.get("home_elo")
                        form3 = row.get("form3_home")
                        form5 = row.get("form5_home")
                        team_odds = row.get("odd_home")
                        opponent_odds = row.get("odd_away")
                        max_team_odds = row.get("max_home")
                        max_opponent_odds = row.get("max_away")
                        goals_for = row.get("home_goals")
                        goals_against = row.get("away_goals")
                    elif row["mapped_away_team"] == team:
                        side = "away"
                        opponent = row["mapped_home_team"]
                        elo = row.get("away_elo")
                        form3 = row.get("form3_away")
                        form5 = row.get("form5_away")
                        team_odds = row.get("odd_away")
                        opponent_odds = row.get("odd_home")
                        max_team_odds = row.get("max_away")
                        max_opponent_odds = row.get("max_home")
                        goals_for = row.get("away_goals")
                        goals_against = row.get("home_goals")
                    else:
                        continue
                    days_since = (fixture_date - match_date).days if fixture_date else None
                    return {
                        "team": team,
                        "latest_match_date": match_date.isoformat(),
                        "days_since_latest_match": days_since,
                        "side": side,
                        "opponent": opponent,
                        "elo": _safe_float(elo),
                        "form3": _safe_float(form3),
                        "form5": _safe_float(form5),
                        "goals_for": _safe_int(goals_for),
                        "goals_against": _safe_int(goals_against),
                        "result": result_for_side(row, side),
                        "closing_odds_team": _safe_float(team_odds),
                        "closing_odds_draw": _safe_float(row.get("odd_draw")),
                        "closing_odds_opponent": _safe_float(opponent_odds),
                        "max_odds_team": _safe_float(max_team_odds),
                        "max_odds_draw": _safe_float(row.get("max_draw")),
                        "max_odds_opponent": _safe_float(max_opponent_odds),
                    }
                return None

            feature_rows = []
            missing_snapshots = []
            complete_snapshots_count = 0
            for fixture in fixtures:
                fixture_date = _parse_date(fixture.get("kickoff_at"))
                raw_home_team = str(fixture.get("home_team", "")).strip()
                raw_away_team = str(fixture.get("away_team", "")).strip()
                mapped_home_team = _canonical_name(raw_home_team, aliases)
                mapped_away_team = _canonical_name(raw_away_team, aliases)
                home_snapshot = team_snapshot(mapped_home_team, fixture_date)
                away_snapshot = team_snapshot(mapped_away_team, fixture_date)
                if home_snapshot and away_snapshot:
                    complete_snapshots_count += 1
                else:
                    missing_snapshots.append({
                        "fixture_id": fixture.get("id"),
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "mapped_home_team": mapped_home_team,
                        "mapped_away_team": mapped_away_team,
                        "home_snapshot_found": home_snapshot is not None,
                        "away_snapshot_found": away_snapshot is not None,
                    })

                elo_delta = None
                form3_delta = None
                form5_delta = None
                if home_snapshot and away_snapshot:
                    if home_snapshot.get("elo") is not None and away_snapshot.get("elo") is not None:
                        elo_delta = round(home_snapshot["elo"] - away_snapshot["elo"], 6)
                    if home_snapshot.get("form3") is not None and away_snapshot.get("form3") is not None:
                        form3_delta = round(home_snapshot["form3"] - away_snapshot["form3"], 6)
                    if home_snapshot.get("form5") is not None and away_snapshot.get("form5") is not None:
                        form5_delta = round(home_snapshot["form5"] - away_snapshot["form5"], 6)

                feature_rows.append({
                    "fixture_id": fixture["id"],
                    "source": "xgabora",
                    "feature_key": "elo_form_snapshot",
                    "feature_value": {
                        "league_code": league_code,
                        "raw_home_team": raw_home_team,
                        "raw_away_team": raw_away_team,
                        "mapped_home_team": mapped_home_team,
                        "mapped_away_team": mapped_away_team,
                        "fixture_date": fixture_date.isoformat() if fixture_date else None,
                        "home": home_snapshot,
                        "away": away_snapshot,
                        "deltas": {
                            "elo_delta_home_minus_away": elo_delta,
                            "form3_delta_home_minus_away": form3_delta,
                            "form5_delta_home_minus_away": form5_delta,
                        },
                        "has_complete_snapshot": home_snapshot is not None and away_snapshot is not None,
                        "source_history_rows": len(prepared_history),
                    },
                })

            upsert_resp = supabase.table("match_features").upsert(feature_rows, on_conflict="fixture_id,source,feature_key").execute()
            upserted = len(upsert_resp.data or feature_rows)
            total_upserted += upserted
            results[league_code] = {
                "status": "ok",
                "scheduled_fixtures_count": len(fixtures),
                "history_rows_count": len(history_rows),
                "enriched_count": len(feature_rows),
                "upserted_count": upserted,
                "complete_snapshots_count": complete_snapshots_count,
                "missing_snapshots_count": len(missing_snapshots),
                "missing_snapshots_sample": missing_snapshots[:10],
            }

        return {"status": "ok", "service": "forecast-api", "source": "xgabora", "selected_leagues": league_codes, "upserted_total": total_upserted, "results": results}
    except Exception as e:
        return {"status": "error", "service": "forecast-api", "source": "xgabora", "error": str(e), "error_type": type(e).__name__}


@app.post("/features/enrich-soccerdata-clubelo-v2")
def enrich_soccerdata_clubelo_v2(payload: MultiLeagueRequest = Body(default=MultiLeagueRequest())):
    try:
        supabase = get_supabase()
        league_codes = _selected_leagues(payload.leagues)
        df, attempts = _read_clubelo_dataframe()
        records, columns, parse_errors = _parse_clubelo_dataframe(df)
        if not records:
            return {"status": "error", "service": "forecast-api", "source": "soccerdata_clubelo", "message": "No ClubElo records parsed", "parse_errors": parse_errors}

        elo_by_key = {record["team_key"]: record for record in records}
        results = {}
        total_upserted = 0

        for league_code in league_codes:
            aliases = _load_aliases(supabase, league_code)
            fixtures_resp = supabase.table("fixtures").select("id, home_team, away_team, kickoff_at").eq("league_code", league_code).eq("event_status", "scheduled").order("kickoff_at", desc=False).execute()
            fixtures = fixtures_resp.data or []
            feature_rows = []
            missing_snapshots = []
            complete_snapshots_count = 0
            for fixture in fixtures:
                raw_home_team = str(fixture.get("home_team", "")).strip()
                raw_away_team = str(fixture.get("away_team", "")).strip()
                home_snapshot = _find_team_elo(raw_home_team, aliases, elo_by_key)
                away_snapshot = _find_team_elo(raw_away_team, aliases, elo_by_key)
                if home_snapshot and away_snapshot:
                    complete_snapshots_count += 1
                else:
                    missing_snapshots.append({
                        "fixture_id": fixture.get("id"),
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "home_snapshot_found": home_snapshot is not None,
                        "away_snapshot_found": away_snapshot is not None,
                        "home_candidates_checked": _team_candidates(raw_home_team, aliases),
                        "away_candidates_checked": _team_candidates(raw_away_team, aliases),
                    })
                elo_delta = round(home_snapshot["elo"] - away_snapshot["elo"], 6) if home_snapshot and away_snapshot else None
                feature_rows.append({
                    "fixture_id": fixture["id"],
                    "source": "soccerdata_clubelo",
                    "feature_key": "current_elo_snapshot",
                    "feature_value": {
                        "league_code": league_code,
                        "raw_home_team": raw_home_team,
                        "raw_away_team": raw_away_team,
                        "kickoff_at": fixture.get("kickoff_at"),
                        "home": home_snapshot,
                        "away": away_snapshot,
                        "deltas": {"elo_delta_home_minus_away": elo_delta},
                        "has_complete_snapshot": home_snapshot is not None and away_snapshot is not None,
                        "source_records_count": len(records),
                        "read_attempts": attempts,
                    },
                })

            if feature_rows:
                upsert_resp = supabase.table("match_features").upsert(feature_rows, on_conflict="fixture_id,source,feature_key").execute()
                upserted = len(upsert_resp.data or feature_rows)
            else:
                upserted = 0
            total_upserted += upserted
            results[league_code] = {
                "status": "ok",
                "scheduled_fixtures_count": len(fixtures),
                "enriched_count": len(feature_rows),
                "upserted_count": upserted,
                "complete_snapshots_count": complete_snapshots_count,
                "missing_snapshots_count": len(missing_snapshots),
                "missing_snapshots_sample": missing_snapshots[:10],
            }

        return {"status": "ok", "service": "forecast-api", "source": "soccerdata_clubelo", "selected_leagues": league_codes, "upserted_total": total_upserted, "results": results}
    except Exception as e:
        return {"status": "error", "service": "forecast-api", "source": "soccerdata_clubelo", "error": str(e), "error_type": type(e).__name__}


@app.post("/features/enrich-soccerdata-understat-v2")
def enrich_soccerdata_understat_v2(payload: MultiLeagueRequest = Body(default=MultiLeagueRequest())):
    try:
        supabase = get_supabase()
        league_codes = _selected_leagues(payload.leagues)
        results = {}
        total_upserted = 0

        for league_code in league_codes:
            aliases = _load_aliases(supabase, league_code)
            fixtures_resp = supabase.table("fixtures").select("id, home_team, away_team, kickoff_at").eq("league_code", league_code).eq("event_status", "scheduled").order("kickoff_at", desc=False).execute()
            fixtures = fixtures_resp.data or []
            if not fixtures:
                results[league_code] = {"status": "ok", "enriched_count": 0, "message": "No scheduled fixtures found"}
                continue

            try:
                reader, reader_attempts = _make_soccerdata_reader("Understat", league_code)
                schedule_df, schedule_attempts = _try_reader_method(reader, "read_schedule")
                records, columns, parse_errors = _parse_understat_schedule(schedule_df)
            except Exception as read_error:
                results[league_code] = {"status": "error", "message": "Could not read Understat schedule", "error": str(read_error)}
                continue

            if not records:
                results[league_code] = {"status": "error", "message": "No Understat records parsed", "columns": columns, "parse_errors": parse_errors}
                continue

            feature_rows = []
            missing_snapshots = []
            complete_snapshots_count = 0
            for fixture in fixtures:
                fixture_date = _parse_date(fixture.get("kickoff_at"))
                raw_home_team = str(fixture.get("home_team", "")).strip()
                raw_away_team = str(fixture.get("away_team", "")).strip()
                home_snapshot = _team_recent_xg_snapshot(raw_home_team, aliases, records, fixture_date, last_n=5)
                away_snapshot = _team_recent_xg_snapshot(raw_away_team, aliases, records, fixture_date, last_n=5)
                if home_snapshot and away_snapshot:
                    complete_snapshots_count += 1
                else:
                    missing_snapshots.append({
                        "fixture_id": fixture.get("id"),
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "home_snapshot_found": home_snapshot is not None,
                        "away_snapshot_found": away_snapshot is not None,
                        "home_candidates_checked": _team_candidates(raw_home_team, aliases),
                        "away_candidates_checked": _team_candidates(raw_away_team, aliases),
                    })

                xg_diff_delta = None
                xg_for_delta = None
                xg_against_delta = None
                if home_snapshot and away_snapshot:
                    if home_snapshot.get("avg_xg_diff") is not None and away_snapshot.get("avg_xg_diff") is not None:
                        xg_diff_delta = round(home_snapshot["avg_xg_diff"] - away_snapshot["avg_xg_diff"], 6)
                    if home_snapshot.get("avg_xg_for") is not None and away_snapshot.get("avg_xg_for") is not None:
                        xg_for_delta = round(home_snapshot["avg_xg_for"] - away_snapshot["avg_xg_for"], 6)
                    if home_snapshot.get("avg_xg_against") is not None and away_snapshot.get("avg_xg_against") is not None:
                        xg_against_delta = round(home_snapshot["avg_xg_against"] - away_snapshot["avg_xg_against"], 6)

                feature_rows.append({
                    "fixture_id": fixture["id"],
                    "source": "soccerdata_understat",
                    "feature_key": "recent_xg_snapshot",
                    "feature_value": {
                        "league_code": league_code,
                        "raw_home_team": raw_home_team,
                        "raw_away_team": raw_away_team,
                        "kickoff_at": fixture.get("kickoff_at"),
                        "fixture_date": fixture_date.isoformat() if fixture_date else None,
                        "home": home_snapshot,
                        "away": away_snapshot,
                        "deltas": {
                            "avg_xg_diff_delta_home_minus_away": xg_diff_delta,
                            "avg_xg_for_delta_home_minus_away": xg_for_delta,
                            "avg_xg_against_delta_home_minus_away": xg_against_delta,
                        },
                        "has_complete_snapshot": home_snapshot is not None and away_snapshot is not None,
                        "source_records_count": len(records),
                        "reader_attempts": reader_attempts,
                        "schedule_attempts": schedule_attempts,
                    },
                })

            upsert_resp = supabase.table("match_features").upsert(feature_rows, on_conflict="fixture_id,source,feature_key").execute()
            upserted = len(upsert_resp.data or feature_rows)
            total_upserted += upserted
            results[league_code] = {
                "status": "ok",
                "scheduled_fixtures_count": len(fixtures),
                "understat_records_count": len(records),
                "enriched_count": len(feature_rows),
                "upserted_count": upserted,
                "complete_snapshots_count": complete_snapshots_count,
                "missing_snapshots_count": len(missing_snapshots),
                "missing_snapshots_sample": missing_snapshots[:10],
            }

        return {"status": "ok", "service": "forecast-api", "source": "soccerdata_understat", "selected_leagues": league_codes, "upserted_total": total_upserted, "results": results}
    except Exception as e:
        return {"status": "error", "service": "forecast-api", "source": "soccerdata_understat", "error": str(e), "error_type": type(e).__name__}


@app.post("/features/enrich-soccerdata-espn-v2")
def enrich_soccerdata_espn_v2(payload: MultiLeagueRequest = Body(default=MultiLeagueRequest())):
    try:
        supabase = get_supabase()
        league_codes = _selected_leagues(payload.leagues)
        results = {}
        total_upserted = 0

        for league_code in league_codes:
            aliases = _load_aliases(supabase, league_code)
            fixtures_resp = supabase.table("fixtures").select("id, home_team, away_team, kickoff_at").eq("league_code", league_code).eq("event_status", "scheduled").order("kickoff_at", desc=False).execute()
            fixtures = fixtures_resp.data or []
            if not fixtures:
                results[league_code] = {"status": "ok", "enriched_count": 0, "message": "No scheduled fixtures found"}
                continue

            try:
                reader, reader_attempts = _make_soccerdata_reader("ESPN", league_code)
                schedule_df, schedule_attempts = _try_reader_method(reader, "read_schedule")
                lineup_df, lineup_attempts = _try_reader_method(reader, "read_lineup")
                schedule_records, schedule_columns = _parse_espn_schedule(schedule_df)
                lineup_records, lineup_columns = _parse_espn_lineups(lineup_df)
                team_games = _aggregate_team_games(lineup_records)
            except Exception as read_error:
                results[league_code] = {"status": "error", "message": "Could not read ESPN schedule/lineup", "error": str(read_error)}
                continue

            if not schedule_records:
                results[league_code] = {"status": "error", "message": "No ESPN schedule records parsed", "schedule_columns": schedule_columns}
                continue

            feature_rows = []
            missing_snapshots = []
            complete_snapshots_count = 0
            for fixture in fixtures:
                fixture_date = _parse_date(fixture.get("kickoff_at"))
                raw_home_team = str(fixture.get("home_team", "")).strip()
                raw_away_team = str(fixture.get("away_team", "")).strip()
                schedule_match = _find_schedule_match(fixture, aliases, schedule_records)
                home_lineup = _team_recent_lineup_snapshot(raw_home_team, aliases, team_games, fixture_date, last_n=5)
                away_lineup = _team_recent_lineup_snapshot(raw_away_team, aliases, team_games, fixture_date, last_n=5)
                has_complete = schedule_match is not None and home_lineup is not None and away_lineup is not None
                if has_complete:
                    complete_snapshots_count += 1
                else:
                    missing_snapshots.append({
                        "fixture_id": fixture.get("id"),
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "schedule_match_found": schedule_match is not None,
                        "home_lineup_found": home_lineup is not None,
                        "away_lineup_found": away_lineup is not None,
                        "home_candidates_checked": _team_candidates(raw_home_team, aliases),
                        "away_candidates_checked": _team_candidates(raw_away_team, aliases),
                    })

                shots_delta = None
                sot_delta = None
                goals_delta = None
                conceded_delta = None
                if home_lineup and away_lineup:
                    if home_lineup.get("avg_total_shots") is not None and away_lineup.get("avg_total_shots") is not None:
                        shots_delta = round(home_lineup["avg_total_shots"] - away_lineup["avg_total_shots"], 6)
                    if home_lineup.get("avg_shots_on_target") is not None and away_lineup.get("avg_shots_on_target") is not None:
                        sot_delta = round(home_lineup["avg_shots_on_target"] - away_lineup["avg_shots_on_target"], 6)
                    if home_lineup.get("avg_total_goals") is not None and away_lineup.get("avg_total_goals") is not None:
                        goals_delta = round(home_lineup["avg_total_goals"] - away_lineup["avg_total_goals"], 6)
                    if home_lineup.get("avg_goals_conceded") is not None and away_lineup.get("avg_goals_conceded") is not None:
                        conceded_delta = round(home_lineup["avg_goals_conceded"] - away_lineup["avg_goals_conceded"], 6)

                feature_rows.append({
                    "fixture_id": fixture["id"],
                    "source": "soccerdata_espn",
                    "feature_key": "schedule_lineup_snapshot",
                    "feature_value": {
                        "league_code": league_code,
                        "raw_home_team": raw_home_team,
                        "raw_away_team": raw_away_team,
                        "kickoff_at": fixture.get("kickoff_at"),
                        "fixture_date": fixture_date.isoformat() if fixture_date else None,
                        "schedule_match": schedule_match,
                        "home_lineup_recent": home_lineup,
                        "away_lineup_recent": away_lineup,
                        "deltas": {
                            "avg_total_shots_delta_home_minus_away": shots_delta,
                            "avg_shots_on_target_delta_home_minus_away": sot_delta,
                            "avg_total_goals_delta_home_minus_away": goals_delta,
                            "avg_goals_conceded_delta_home_minus_away": conceded_delta,
                        },
                        "has_complete_snapshot": has_complete,
                        "source_schedule_records_count": len(schedule_records),
                        "source_lineup_records_count": len(lineup_records),
                        "source_team_games_count": len(team_games),
                        "reader_attempts": reader_attempts,
                        "schedule_attempts": schedule_attempts,
                        "lineup_attempts": lineup_attempts,
                    },
                })

            upsert_resp = supabase.table("match_features").upsert(feature_rows, on_conflict="fixture_id,source,feature_key").execute()
            upserted = len(upsert_resp.data or feature_rows)
            total_upserted += upserted
            results[league_code] = {
                "status": "ok",
                "scheduled_fixtures_count": len(fixtures),
                "espn_schedule_records_count": len(schedule_records),
                "espn_lineup_records_count": len(lineup_records),
                "espn_team_games_count": len(team_games),
                "enriched_count": len(feature_rows),
                "upserted_count": upserted,
                "complete_snapshots_count": complete_snapshots_count,
                "missing_snapshots_count": len(missing_snapshots),
                "missing_snapshots_sample": missing_snapshots[:10],
            }

        return {"status": "ok", "service": "forecast-api", "source": "soccerdata_espn", "selected_leagues": league_codes, "upserted_total": total_upserted, "results": results, "note": "ESPN remains mandatory in SQL views. Incomplete ESPN snapshots block publishable candidates."}
    except Exception as e:
        return {"status": "error", "service": "forecast-api", "source": "soccerdata_espn", "error": str(e), "error_type": type(e).__name__}
