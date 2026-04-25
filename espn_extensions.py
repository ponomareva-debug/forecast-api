import math
import re
from datetime import datetime

from fixed_main import app, get_supabase
from soccerdata_extensions import _load_aliases, _team_candidates


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


def _safe_float(value):
    try:
        import pandas as pd
        if value is None or pd.isna(value):
            return None
        num = float(value)
        if not math.isfinite(num):
            return None
        return round(num, 6)
    except Exception:
        return None


def _safe_int(value):
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


def _name_key(name):
    if name is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(name).lower())


def _team_keys(raw_team_name, aliases):
    return {_name_key(name) for name in _team_candidates(raw_team_name, aliases) if name}


def _parse_date(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    if " " in text:
        text = text.split(" ", 1)[0]
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def _parse_game_date(game_value):
    if game_value is None:
        return None
    text = str(game_value).strip()
    if len(text) < 10:
        return None
    return _parse_date(text[:10])


def _flatten_columns(df):
    if df is None:
        return []

    columns = []
    for col in list(df.columns):
        if isinstance(col, tuple):
            parts = [str(part) for part in col if str(part) != ""]
            columns.append("__".join(parts))
        else:
            columns.append(str(col))
    return columns


def _reset_flat_df(df):
    clean_df = df.reset_index().copy()
    clean_df.columns = _flatten_columns(clean_df)
    return clean_df


def _sample_rows(df, limit=5):
    if df is None or len(df) == 0:
        return []

    clean_df = _reset_flat_df(df)
    raw_rows = clean_df.head(limit).to_dict(orient="records")

    return [
        {str(key): _json_safe_value(value) for key, value in row.items()}
        for row in raw_rows
    ]


def _make_espn_reader():
    import soccerdata as sd

    attempts = []
    league_attempts = ["ENG-Premier League", "Premier League"]
    season_attempts = [2025, "2025", "2025-2026", [2025], ["2025"], 2024, "2024", [2024]]

    for league in league_attempts:
        for season in season_attempts:
            kwargs = {"leagues": league, "seasons": season}
            try:
                reader = sd.ESPN(**kwargs)
                attempts.append({"kwargs": kwargs, "status": "created"})
                return reader, attempts
            except Exception as e:
                attempts.append({"kwargs": kwargs, "status": "error", "error": str(e)})

    try:
        reader = sd.ESPN()
        attempts.append({"kwargs": {}, "status": "created"})
        return reader, attempts
    except Exception as e:
        attempts.append({"kwargs": {}, "status": "error", "error": str(e)})

    raise RuntimeError({"message": "Could not create ESPN reader", "attempts": attempts})


def _try_reader_method(reader, method_name):
    attempts = []

    if not hasattr(reader, method_name):
        return None, [{"method": method_name, "status": "missing"}]

    method = getattr(reader, method_name)
    call_variants = [{"args": [], "kwargs": {}}]

    for variant in call_variants:
        try:
            df = method(*variant["args"], **variant["kwargs"])
            attempts.append({
                "method": method_name,
                "args": variant["args"],
                "kwargs": variant["kwargs"],
                "status": "ok",
            })
            return df, attempts
        except Exception as e:
            attempts.append({
                "method": method_name,
                "args": variant["args"],
                "kwargs": variant["kwargs"],
                "status": "error",
                "error": str(e),
            })

    return None, attempts


def _parse_espn_schedule(df):
    if df is None or len(df) == 0:
        return [], []

    clean_df = _reset_flat_df(df)
    records = []

    for _, row in clean_df.iterrows():
        home_team = row.get("home_team")
        away_team = row.get("away_team")
        match_date = _parse_date(row.get("date"))

        if home_team is None or away_team is None or match_date is None:
            continue

        records.append({
            "league": _json_safe_value(row.get("league")),
            "season": _json_safe_value(row.get("season")),
            "game": _json_safe_value(row.get("game")),
            "date": _json_safe_value(row.get("date")),
            "match_date": match_date.isoformat(),
            "match_date_parsed": match_date,
            "home_team": str(home_team).strip(),
            "away_team": str(away_team).strip(),
            "home_key": _name_key(home_team),
            "away_key": _name_key(away_team),
            "game_id": _json_safe_value(row.get("game_id")),
            "league_id": _json_safe_value(row.get("league_id")),
        })

    return records, list(clean_df.columns)


def _find_schedule_match(fixture, aliases, schedule_records):
    fixture_date = _parse_date(fixture.get("kickoff_at"))
    home_keys = _team_keys(fixture.get("home_team"), aliases)
    away_keys = _team_keys(fixture.get("away_team"), aliases)

    exact = []
    fallback = []

    for row in schedule_records:
        teams_match = row.get("home_key") in home_keys and row.get("away_key") in away_keys
        if not teams_match:
            continue

        if fixture_date and row.get("match_date_parsed") == fixture_date:
            exact.append(row)
        else:
            fallback.append(row)

    if exact:
        return {**exact[0], "match_quality": "teams_and_date"}

    if fallback:
        # choose nearest date by absolute distance from fixture date if possible
        if fixture_date:
            fallback = sorted(
                fallback,
                key=lambda item: abs((item["match_date_parsed"] - fixture_date).days),
            )
        return {**fallback[0], "match_quality": "teams_only_nearest_date"}

    return None


def _parse_espn_lineups(df):
    if df is None or len(df) == 0:
        return [], []

    clean_df = _reset_flat_df(df)
    records = []

    for _, row in clean_df.iterrows():
        game = row.get("game")
        team = row.get("team")
        player = row.get("player")
        match_date = _parse_game_date(game)

        if game is None or team is None or player is None or match_date is None:
            continue

        records.append({
            "league": _json_safe_value(row.get("league")),
            "season": _json_safe_value(row.get("season")),
            "game": str(game),
            "match_date": match_date.isoformat(),
            "match_date_parsed": match_date,
            "team": str(team).strip(),
            "team_key": _name_key(team),
            "player": str(player).strip(),
            "is_home": _json_safe_value(row.get("is_home")),
            "position": _json_safe_value(row.get("position")),
            "sub_in": _json_safe_value(row.get("sub_in")),
            "sub_out": _json_safe_value(row.get("sub_out")),
            "appearances": _safe_float(row.get("appearances")),
            "fouls_committed": _safe_float(row.get("fouls_committed")),
            "fouls_suffered": _safe_float(row.get("fouls_suffered")),
            "own_goals": _safe_float(row.get("own_goals")),
            "red_cards": _safe_float(row.get("red_cards")),
            "yellow_cards": _safe_float(row.get("yellow_cards")),
            "goals_conceded": _safe_float(row.get("goals_conceded")),
            "saves": _safe_float(row.get("saves")),
            "shots_faced": _safe_float(row.get("shots_faced")),
            "goal_assists": _safe_float(row.get("goal_assists")),
            "shots_on_target": _safe_float(row.get("shots_on_target")),
            "total_goals": _safe_float(row.get("total_goals")),
            "total_shots": _safe_float(row.get("total_shots")),
            "offsides": _safe_float(row.get("offsides")),
        })

    return records, list(clean_df.columns)


def _aggregate_team_games(lineup_records):
    grouped = {}

    numeric_fields = [
        "appearances", "fouls_committed", "fouls_suffered", "own_goals", "red_cards",
        "yellow_cards", "goals_conceded", "saves", "shots_faced", "goal_assists",
        "shots_on_target", "total_goals", "total_shots", "offsides",
    ]

    for row in lineup_records:
        key = (row["team_key"], row["game"])
        item = grouped.setdefault(key, {
            "team": row["team"],
            "team_key": row["team_key"],
            "game": row["game"],
            "match_date": row["match_date"],
            "match_date_parsed": row["match_date_parsed"],
            "players_count": 0,
            "starters_count": 0,
            **{field: 0.0 for field in numeric_fields},
        })

        item["players_count"] += 1
        if str(row.get("sub_in")).lower() == "start":
            item["starters_count"] += 1

        for field in numeric_fields:
            value = row.get(field)
            if value is not None:
                item[field] += float(value)

    return sorted(
        grouped.values(),
        key=lambda item: item.get("match_date") or "",
        reverse=True,
    )


def _team_recent_lineup_snapshot(raw_team_name, aliases, team_games, fixture_date, last_n=5):
    keys = _team_keys(raw_team_name, aliases)
    matches = []

    for game in team_games:
        if game.get("team_key") not in keys:
            continue
        match_date = game.get("match_date_parsed")
        if fixture_date and match_date and match_date >= fixture_date:
            continue

        matches.append({
            "game": game.get("game"),
            "match_date": game.get("match_date"),
            "players_count": _safe_int(game.get("players_count")),
            "starters_count": _safe_int(game.get("starters_count")),
            "total_goals": _safe_float(game.get("total_goals")),
            "total_shots": _safe_float(game.get("total_shots")),
            "shots_on_target": _safe_float(game.get("shots_on_target")),
            "goal_assists": _safe_float(game.get("goal_assists")),
            "yellow_cards": _safe_float(game.get("yellow_cards")),
            "red_cards": _safe_float(game.get("red_cards")),
            "fouls_committed": _safe_float(game.get("fouls_committed")),
            "fouls_suffered": _safe_float(game.get("fouls_suffered")),
            "goals_conceded": _safe_float(game.get("goals_conceded")),
            "shots_faced": _safe_float(game.get("shots_faced")),
            "saves": _safe_float(game.get("saves")),
        })

        if len(matches) >= last_n:
            break

    if not matches:
        return None

    def avg(field):
        values = [m[field] for m in matches if m.get(field) is not None]
        return round(sum(values) / len(values), 6) if values else None

    return {
        "team": str(raw_team_name).strip(),
        "candidate_names_checked": _team_candidates(raw_team_name, aliases),
        "matches_count": len(matches),
        "last_n": last_n,
        "avg_total_goals": avg("total_goals"),
        "avg_total_shots": avg("total_shots"),
        "avg_shots_on_target": avg("shots_on_target"),
        "avg_goal_assists": avg("goal_assists"),
        "avg_yellow_cards": avg("yellow_cards"),
        "avg_red_cards": avg("red_cards"),
        "avg_fouls_committed": avg("fouls_committed"),
        "avg_fouls_suffered": avg("fouls_suffered"),
        "avg_goals_conceded": avg("goals_conceded"),
        "avg_shots_faced": avg("shots_faced"),
        "avg_saves": avg("saves"),
        "recent_matches": matches,
    }


@app.get("/debug/soccerdata-espn")
def debug_soccerdata_espn():
    try:
        reader, reader_attempts = _make_espn_reader()
        reader_methods = [name for name in dir(reader) if name.startswith("read_")]

        preferred_methods = [
            "read_schedule",
            "read_matchsheet",
            "read_lineup",
            "read_team_match_stats",
            "read_player_match_stats",
        ]

        method_results = []
        selected = None

        for method_name in preferred_methods:
            df, attempts = _try_reader_method(reader, method_name)
            result = {
                "method": method_name,
                "attempts": attempts,
                "ok": df is not None,
                "rows": int(len(df)) if df is not None else 0,
                "columns": _flatten_columns(df) if df is not None else [],
                "sample_rows": _sample_rows(df, limit=3) if df is not None else [],
            }
            method_results.append(result)

            if selected is None and df is not None and len(df) > 0:
                selected = result

        return {
            "status": "ok" if selected else "error",
            "service": "forecast-api",
            "source": "soccerdata_espn",
            "reader_attempts": reader_attempts,
            "reader_methods": reader_methods,
            "method_results": method_results,
            "selected_method": selected["method"] if selected else None,
            "selected_rows": selected["rows"] if selected else 0,
            "selected_columns": selected["columns"] if selected else [],
            "selected_sample_rows": selected["sample_rows"] if selected else [],
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "source": "soccerdata_espn",
            "error": str(e),
            "error_type": type(e).__name__,
        }


@app.post("/features/enrich-soccerdata-espn")
def enrich_soccerdata_espn():
    try:
        supabase = get_supabase()
        league_code = "EPL"
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
            return {
                "status": "ok",
                "service": "forecast-api",
                "source": "soccerdata_espn",
                "enriched_count": 0,
                "message": "No scheduled fixtures found",
            }

        reader, reader_attempts = _make_espn_reader()
        schedule_df, schedule_attempts = _try_reader_method(reader, "read_schedule")
        lineup_df, lineup_attempts = _try_reader_method(reader, "read_lineup")

        schedule_records, schedule_columns = _parse_espn_schedule(schedule_df)
        lineup_records, lineup_columns = _parse_espn_lineups(lineup_df)
        team_games = _aggregate_team_games(lineup_records)

        if not schedule_records:
            return {
                "status": "error",
                "service": "forecast-api",
                "source": "soccerdata_espn",
                "message": "No ESPN schedule records parsed",
                "reader_attempts": reader_attempts,
                "schedule_attempts": schedule_attempts,
                "schedule_columns": schedule_columns,
            }

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
                    "note": "ESPN schedule match plus last 5 lineup-derived team game aggregates before fixture kickoff",
                },
            })

        upsert_resp = (
            supabase.table("match_features")
            .upsert(feature_rows, on_conflict="fixture_id,source,feature_key")
            .execute()
        )

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "soccerdata_espn",
            "feature_key": "schedule_lineup_snapshot",
            "scheduled_fixtures_count": len(fixtures),
            "espn_schedule_records_count": len(schedule_records),
            "espn_lineup_records_count": len(lineup_records),
            "espn_team_games_count": len(team_games),
            "schedule_columns": schedule_columns,
            "lineup_columns": lineup_columns,
            "enriched_count": len(feature_rows),
            "upserted_count": len(upsert_resp.data or feature_rows),
            "complete_snapshots_count": complete_snapshots_count,
            "missing_snapshots_count": len(missing_snapshots),
            "missing_snapshots_sample": missing_snapshots[:10],
            "message": "soccerdata ESPN schedule/lineup features saved to match_features",
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "source": "soccerdata_espn",
            "error": str(e),
            "error_type": type(e).__name__,
        }
