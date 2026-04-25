import math
import re
from datetime import datetime, timezone

from fixed_main import app, get_supabase


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


def _name_key(name):
    if name is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(name).lower())


def _canonical_name(name, aliases):
    clean = str(name).strip()
    return aliases.get(clean, clean)


def _team_candidates(raw_name, aliases):
    raw = str(raw_name).strip()
    mapped = _canonical_name(raw, aliases)

    extras = {
        "Manchester United": ["Man United", "Man Utd", "Manchester Utd"],
        "Man United": ["Manchester United", "Man Utd", "Manchester Utd"],
        "Manchester City": ["Man City", "Manchester City"],
        "Man City": ["Manchester City", "Man City"],
        "Tottenham Hotspur": ["Tottenham", "Spurs"],
        "Tottenham": ["Tottenham Hotspur", "Spurs"],
        "Newcastle United": ["Newcastle"],
        "Newcastle": ["Newcastle United"],
        "West Ham United": ["West Ham"],
        "West Ham": ["West Ham United"],
        "Wolverhampton Wanderers": ["Wolves", "Wolverhampton"],
        "Wolves": ["Wolverhampton Wanderers", "Wolverhampton"],
        "Brighton and Hove Albion": ["Brighton", "Brighton & Hove Albion"],
        "Brighton": ["Brighton and Hove Albion", "Brighton & Hove Albion"],
        "Nottingham Forest": ["Nottm Forest", "Nott'm Forest"],
        "Nottm Forest": ["Nottingham Forest", "Nott'm Forest"],
        "Leeds United": ["Leeds"],
        "Leeds": ["Leeds United"],
        "AFC Bournemouth": ["Bournemouth"],
        "Bournemouth": ["AFC Bournemouth"],
    }

    candidates = []
    for item in [raw, mapped]:
        if item and item not in candidates:
            candidates.append(item)
        for extra in extras.get(item, []):
            if extra and extra not in candidates:
                candidates.append(extra)

    return candidates


def _load_aliases(supabase, league_code):
    aliases_resp = (
        supabase.table("team_aliases")
        .select("source_name, canonical_name")
        .eq("league_code", league_code)
        .execute()
    )
    return {
        str(row["source_name"]).strip(): str(row["canonical_name"]).strip()
        for row in (aliases_resp.data or [])
    }


def _read_clubelo_dataframe():
    import soccerdata as sd

    reader = sd.ClubElo()
    attempts = []

    if hasattr(reader, "read_by_date"):
        try:
            df = reader.read_by_date()
            return df, attempts + [{"method": "read_by_date", "status": "ok"}]
        except TypeError as e:
            attempts.append({"method": "read_by_date", "status": "type_error", "error": str(e)})
            try:
                today = datetime.now(timezone.utc).date().isoformat()
                df = reader.read_by_date(today)
                return df, attempts + [{"method": "read_by_date(today)", "status": "ok"}]
            except Exception as ee:
                attempts.append({"method": "read_by_date(today)", "status": "error", "error": str(ee)})
        except Exception as e:
            attempts.append({"method": "read_by_date", "status": "error", "error": str(e)})

    if hasattr(reader, "read_team_history"):
        attempts.append({"method": "read_team_history", "status": "available_but_not_used_for_all_teams"})

    raise RuntimeError({"message": "Could not read ClubElo dataframe", "attempts": attempts})


def _parse_clubelo_dataframe(df):
    import pandas as pd

    if df is None or len(df) == 0:
        return [], [], []

    clean_df = df.reset_index().copy()
    columns = [str(col) for col in clean_df.columns]
    lower_map = {str(col).lower(): str(col) for col in clean_df.columns}

    team_column_candidates = [
        "team", "club", "name", "team_name", "club_name", "index",
    ]
    elo_column_candidates = [
        "elo", "rating", "clubelo", "rank_elo",
    ]
    rank_column_candidates = ["rank", "ranking"]
    country_column_candidates = ["country", "nation"]
    level_column_candidates = ["level"]
    from_column_candidates = ["from", "from_date"]
    to_column_candidates = ["to", "to_date"]

    def pick_column(candidates):
        for candidate in candidates:
            if candidate in lower_map:
                return lower_map[candidate]
        return None

    team_col = pick_column(team_column_candidates)
    elo_col = pick_column(elo_column_candidates)
    rank_col = pick_column(rank_column_candidates)
    country_col = pick_column(country_column_candidates)
    level_col = pick_column(level_column_candidates)
    from_col = pick_column(from_column_candidates)
    to_col = pick_column(to_column_candidates)

    if team_col is None or elo_col is None:
        return [], columns, [
            {
                "error": "Could not identify team or elo column",
                "columns": columns,
                "team_col": team_col,
                "elo_col": elo_col,
            }
        ]

    records = []
    errors = []

    for _, row in clean_df.iterrows():
        team = row.get(team_col)
        elo = _safe_float(row.get(elo_col))
        if team is None or str(team).strip() == "" or elo is None:
            continue

        records.append({
            "team": str(team).strip(),
            "team_key": _name_key(team),
            "elo": elo,
            "rank": _json_safe_value(row.get(rank_col)) if rank_col else None,
            "country": _json_safe_value(row.get(country_col)) if country_col else None,
            "level": _json_safe_value(row.get(level_col)) if level_col else None,
            "from": _json_safe_value(row.get(from_col)) if from_col else None,
            "to": _json_safe_value(row.get(to_col)) if to_col else None,
        })

    return records, columns, errors


def _find_team_elo(raw_team_name, aliases, elo_by_key):
    candidates = _team_candidates(raw_team_name, aliases)
    for candidate in candidates:
        record = elo_by_key.get(_name_key(candidate))
        if record:
            return {
                **record,
                "matched_by": candidate,
                "candidate_names_checked": candidates,
            }
    return None


@app.get("/debug/soccerdata-clubelo")
def debug_soccerdata_clubelo():
    try:
        df, attempts = _read_clubelo_dataframe()
        records, columns, parse_errors = _parse_clubelo_dataframe(df)

        return {
            "status": "ok" if records else "error",
            "service": "forecast-api",
            "source": "soccerdata_clubelo",
            "read_attempts": attempts,
            "raw_rows": int(len(df)) if df is not None else 0,
            "raw_columns": columns,
            "parsed_records_count": len(records),
            "parse_errors": parse_errors,
            "sample_records": records[:20],
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "source": "soccerdata_clubelo",
            "error": str(e),
            "error_type": type(e).__name__,
        }


@app.post("/features/enrich-soccerdata-clubelo")
def enrich_soccerdata_clubelo():
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
                "source": "soccerdata_clubelo",
                "enriched_count": 0,
                "message": "No scheduled fixtures found",
            }

        df, attempts = _read_clubelo_dataframe()
        records, columns, parse_errors = _parse_clubelo_dataframe(df)
        if not records:
            return {
                "status": "error",
                "service": "forecast-api",
                "source": "soccerdata_clubelo",
                "message": "No ClubElo records parsed",
                "read_attempts": attempts,
                "raw_columns": columns,
                "parse_errors": parse_errors,
            }

        elo_by_key = {record["team_key"]: record for record in records}

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

            elo_delta = None
            if home_snapshot and away_snapshot:
                elo_delta = round(home_snapshot["elo"] - away_snapshot["elo"], 6)

            feature_rows.append({
                "fixture_id": fixture["id"],
                "source": "soccerdata_clubelo",
                "feature_key": "current_elo_snapshot",
                "feature_value": {
                    "raw_home_team": raw_home_team,
                    "raw_away_team": raw_away_team,
                    "kickoff_at": fixture.get("kickoff_at"),
                    "home": home_snapshot,
                    "away": away_snapshot,
                    "deltas": {
                        "elo_delta_home_minus_away": elo_delta,
                    },
                    "has_complete_snapshot": home_snapshot is not None and away_snapshot is not None,
                    "source_records_count": len(records),
                    "read_attempts": attempts,
                    "note": "current ClubElo snapshot from soccerdata",
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
            "source": "soccerdata_clubelo",
            "feature_key": "current_elo_snapshot",
            "scheduled_fixtures_count": len(fixtures),
            "clubelo_records_count": len(records),
            "clubelo_columns": columns,
            "enriched_count": len(feature_rows),
            "upserted_count": len(upsert_resp.data or feature_rows),
            "complete_snapshots_count": complete_snapshots_count,
            "missing_snapshots_count": len(missing_snapshots),
            "missing_snapshots_sample": missing_snapshots[:10],
            "message": "soccerdata ClubElo features saved to match_features",
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "source": "soccerdata_clubelo",
            "error": str(e),
            "error_type": type(e).__name__,
        }
