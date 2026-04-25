import math
import re
from datetime import datetime, timezone

from fixed_main import app, get_supabase
from soccerdata_extensions import _load_aliases, _team_candidates


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


def _parse_date(value):
    if not value:
        return None
    text = str(value)
    if "T" in text:
        text = text.split("T", 1)[0]
    if " " in text:
        text = text.split(" ", 1)[0]
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def _column_key(col):
    return re.sub(r"[^a-z0-9]+", "", str(col).lower())


def _pick_column(columns, candidates):
    keyed = {_column_key(col): col for col in columns}
    for candidate in candidates:
        key = _column_key(candidate)
        if key in keyed:
            return keyed[key]
    return None


def _make_understat_reader():
    import soccerdata as sd

    attempts = []
    league_attempts = ["ENG-Premier League", "Premier League"]
    season_attempts = [2025, "2025", "2025-2026", [2025], ["2025"], 2024, "2024", [2024]]

    for league in league_attempts:
        for season in season_attempts:
            kwargs = {"leagues": league, "seasons": season}
            try:
                reader = sd.Understat(**kwargs)
                attempts.append({"kwargs": kwargs, "status": "created"})
                return reader, attempts
            except Exception as e:
                attempts.append({"kwargs": kwargs, "status": "error", "error": str(e)})

    try:
        reader = sd.Understat()
        attempts.append({"kwargs": {}, "status": "created"})
        return reader, attempts
    except Exception as e:
        attempts.append({"kwargs": {}, "status": "error", "error": str(e)})

    raise RuntimeError({"message": "Could not create Understat reader", "attempts": attempts})


def _read_understat_schedule():
    reader, attempts = _make_understat_reader()
    public_methods = [name for name in dir(reader) if name.startswith("read_")]

    if not hasattr(reader, "read_schedule"):
        raise RuntimeError({
            "message": "Understat reader has no read_schedule method",
            "reader_methods": public_methods,
            "attempts": attempts,
        })

    try:
        df = reader.read_schedule()
        attempts.append({"method": "read_schedule", "status": "ok"})
        return df, attempts, public_methods
    except Exception as e:
        attempts.append({"method": "read_schedule", "status": "error", "error": str(e)})
        raise RuntimeError({
            "message": "Could not read Understat schedule",
            "reader_methods": public_methods,
            "attempts": attempts,
        })


def _parse_understat_schedule(df):
    if df is None or len(df) == 0:
        return [], [], [{"error": "empty_understat_schedule"}]

    clean_df = df.reset_index().copy()
    columns = [str(col) for col in clean_df.columns]

    date_col = _pick_column(columns, ["date", "datetime", "match_date", "kickoff", "kickoff_time"])
    home_col = _pick_column(columns, ["home_team", "home", "team_home", "home_name", "home_team_name"])
    away_col = _pick_column(columns, ["away_team", "away", "team_away", "away_name", "away_team_name"])
    home_xg_col = _pick_column(columns, ["home_xg", "xg_home", "hxg", "home_xg_value", "home_xg_total"])
    away_xg_col = _pick_column(columns, ["away_xg", "xg_away", "axg", "away_xg_value", "away_xg_total"])
    home_goals_col = _pick_column(columns, ["home_goals", "home_score", "fthome", "home_goals_ft"])
    away_goals_col = _pick_column(columns, ["away_goals", "away_score", "ftaway", "away_goals_ft"])

    errors = []
    if not home_col or not away_col:
        errors.append({"error": "missing_team_columns", "home_col": home_col, "away_col": away_col, "columns": columns})
    if not home_xg_col or not away_xg_col:
        errors.append({"error": "missing_xg_columns", "home_xg_col": home_xg_col, "away_xg_col": away_xg_col, "columns": columns})
    if errors:
        return [], columns, errors

    records = []
    for _, row in clean_df.iterrows():
        home_team = row.get(home_col)
        away_team = row.get(away_col)
        home_xg = _safe_float(row.get(home_xg_col))
        away_xg = _safe_float(row.get(away_xg_col))

        if home_team is None or away_team is None or home_xg is None or away_xg is None:
            continue

        match_date = _parse_date(row.get(date_col)) if date_col else None

        records.append({
            "match_date": match_date.isoformat() if match_date else None,
            "match_date_parsed": match_date,
            "home_team": str(home_team).strip(),
            "away_team": str(away_team).strip(),
            "home_key": _name_key(home_team),
            "away_key": _name_key(away_team),
            "home_xg": home_xg,
            "away_xg": away_xg,
            "home_goals": _safe_int(row.get(home_goals_col)) if home_goals_col else None,
            "away_goals": _safe_int(row.get(away_goals_col)) if away_goals_col else None,
        })

    records = sorted(
        records,
        key=lambda item: item.get("match_date") or "",
        reverse=True,
    )

    return records, columns, []


def _team_keys(raw_team_name, aliases):
    names = _team_candidates(raw_team_name, aliases)
    return {_name_key(name) for name in names if name}


def _team_recent_xg_snapshot(raw_team_name, aliases, understat_records, fixture_date, last_n=5):
    keys = _team_keys(raw_team_name, aliases)
    matches = []

    for row in understat_records:
        match_date = row.get("match_date_parsed")
        if fixture_date and match_date and match_date >= fixture_date:
            continue

        if row.get("home_key") in keys:
            matches.append({
                "match_date": row.get("match_date"),
                "side": "home",
                "opponent": row.get("away_team"),
                "xg_for": row.get("home_xg"),
                "xg_against": row.get("away_xg"),
                "goals_for": row.get("home_goals"),
                "goals_against": row.get("away_goals"),
            })
        elif row.get("away_key") in keys:
            matches.append({
                "match_date": row.get("match_date"),
                "side": "away",
                "opponent": row.get("home_team"),
                "xg_for": row.get("away_xg"),
                "xg_against": row.get("home_xg"),
                "goals_for": row.get("away_goals"),
                "goals_against": row.get("home_goals"),
            })

        if len(matches) >= last_n:
            break

    if not matches:
        return None

    xg_for_values = [m["xg_for"] for m in matches if m.get("xg_for") is not None]
    xg_against_values = [m["xg_against"] for m in matches if m.get("xg_against") is not None]

    avg_xg_for = round(sum(xg_for_values) / len(xg_for_values), 6) if xg_for_values else None
    avg_xg_against = round(sum(xg_against_values) / len(xg_against_values), 6) if xg_against_values else None
    avg_xg_diff = None
    if avg_xg_for is not None and avg_xg_against is not None:
        avg_xg_diff = round(avg_xg_for - avg_xg_against, 6)

    return {
        "team": str(raw_team_name).strip(),
        "candidate_names_checked": _team_candidates(raw_team_name, aliases),
        "matches_count": len(matches),
        "last_n": last_n,
        "avg_xg_for": avg_xg_for,
        "avg_xg_against": avg_xg_against,
        "avg_xg_diff": avg_xg_diff,
        "recent_matches": matches,
    }


@app.get("/debug/soccerdata-understat")
def debug_soccerdata_understat():
    try:
        df, attempts, methods = _read_understat_schedule()
        records, columns, parse_errors = _parse_understat_schedule(df)

        return {
            "status": "ok" if records else "error",
            "service": "forecast-api",
            "source": "soccerdata_understat",
            "read_attempts": attempts,
            "reader_methods": methods,
            "raw_rows": int(len(df)) if df is not None else 0,
            "raw_columns": columns,
            "parsed_records_count": len(records),
            "parse_errors": parse_errors,
            "sample_records": [
                {k: v for k, v in record.items() if k != "match_date_parsed"}
                for record in records[:10]
            ],
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "source": "soccerdata_understat",
            "error": str(e),
            "error_type": type(e).__name__,
        }


@app.post("/features/enrich-soccerdata-understat")
def enrich_soccerdata_understat():
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
                "source": "soccerdata_understat",
                "enriched_count": 0,
                "message": "No scheduled fixtures found",
            }

        df, attempts, methods = _read_understat_schedule()
        records, columns, parse_errors = _parse_understat_schedule(df)
        if not records:
            return {
                "status": "error",
                "service": "forecast-api",
                "source": "soccerdata_understat",
                "message": "No Understat xG records parsed",
                "read_attempts": attempts,
                "reader_methods": methods,
                "raw_columns": columns,
                "parse_errors": parse_errors,
            }

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
                    "read_attempts": attempts,
                    "note": "last 5 Understat matches before fixture kickoff for each team",
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
            "source": "soccerdata_understat",
            "feature_key": "recent_xg_snapshot",
            "scheduled_fixtures_count": len(fixtures),
            "understat_records_count": len(records),
            "understat_columns": columns,
            "enriched_count": len(feature_rows),
            "upserted_count": len(upsert_resp.data or feature_rows),
            "complete_snapshots_count": complete_snapshots_count,
            "missing_snapshots_count": len(missing_snapshots),
            "missing_snapshots_sample": missing_snapshots[:10],
            "message": "soccerdata Understat xG features saved to match_features",
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "source": "soccerdata_understat",
            "error": str(e),
            "error_type": type(e).__name__,
        }
