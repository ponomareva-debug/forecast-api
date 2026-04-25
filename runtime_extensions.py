import io
import math
from datetime import datetime, timezone

from fixed_main import app, get_supabase


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
        if pd.isna(value):
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
        num = int(float(value))
        return num
    except Exception:
        return None


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


def _canonical_name(name, aliases):
    clean = str(name).strip()
    return aliases.get(clean, clean)


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


@app.get("/debug/soccerdata-import")
def debug_soccerdata_import():
    try:
        import soccerdata as sd

        version = getattr(sd, "__version__", None)

        public_attrs = [
            name
            for name in dir(sd)
            if not name.startswith("_")
        ]

        expected_sources = [
            "ClubElo",
            "FBref",
            "Understat",
            "Sofascore",
            "WhoScored",
            "ESPN",
            "SoFIFA",
        ]

        return {
            "status": "ok",
            "service": "forecast-api",
            "soccerdata_import": True,
            "soccerdata_version": version,
            "expected_sources_available": [
                source for source in expected_sources if hasattr(sd, source)
            ],
            "expected_sources_missing": [
                source for source in expected_sources if not hasattr(sd, source)
            ],
            "public_attrs_sample": public_attrs[:80],
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "soccerdata_import": False,
            "error": str(e),
            "error_type": type(e).__name__,
        }


@app.get("/debug/xgabora-columns")
def debug_xgabora_columns():
    try:
        import requests
        import pandas as pd

        csv_url = "https://raw.githubusercontent.com/xgabora/Club-Football-Match-Data-2000-2025/main/data/Matches.csv"

        response = requests.get(csv_url, timeout=120)
        if response.status_code != 200:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Failed to download xgabora CSV",
                "http_status": response.status_code,
                "response_sample": response.text[:300],
            }

        df = pd.read_csv(io.StringIO(response.text))
        columns = [str(col) for col in list(df.columns)]

        epl_df = df[df["Division"].astype(str) == "E0"].copy() if "Division" in df.columns else df.head(0).copy()

        candidate_sample_columns = [
            "Division", "MatchDate", "HomeTeam", "AwayTeam", "FTHome", "FTAway",
            "HomeElo", "AwayElo", "Form3Home", "Form3Away", "Form5Home", "Form5Away",
            "OddHome", "OddDraw", "OddAway", "MaxHome", "MaxDraw", "MaxAway",
        ]
        sample_columns = [col for col in candidate_sample_columns if col in df.columns]

        sample_rows = []
        if not epl_df.empty and sample_columns:
            raw_rows = epl_df[sample_columns].head(3).to_dict(orient="records")
            sample_rows = [
                {str(key): _json_safe_value(value) for key, value in row.items()}
                for row in raw_rows
            ]

        target_columns = [
            "HomeElo", "AwayElo", "Form3Home", "Form3Away", "Form5Home", "Form5Away",
            "OddHome", "OddDraw", "OddAway", "MaxHome", "MaxDraw", "MaxAway",
        ]

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "xgabora/Club-Football-Match-Data-2000-2025",
            "total_rows": int(len(df)),
            "total_columns": int(len(columns)),
            "columns": columns,
            "epl_rows": int(len(epl_df)),
            "target_columns_present": [col for col in target_columns if col in df.columns],
            "target_columns_missing": [col for col in target_columns if col not in df.columns],
            "sample_columns_used": sample_columns,
            "epl_sample_rows": sample_rows,
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "error": str(e),
            "error_type": type(e).__name__,
        }


_remove_route("/admin/import-xgabora-epl", "POST")


@app.post("/admin/import-xgabora-epl")
def import_xgabora_epl_extended():
    try:
        import requests
        import pandas as pd

        supabase = get_supabase()

        csv_url = "https://raw.githubusercontent.com/xgabora/Club-Football-Match-Data-2000-2025/main/data/Matches.csv"
        response = requests.get(csv_url, timeout=120)

        if response.status_code != 200:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Failed to download xgabora CSV",
                "http_status": response.status_code,
                "response_sample": response.text[:300],
            }

        df = pd.read_csv(io.StringIO(response.text))

        required_columns = ["Division", "MatchDate", "HomeTeam", "AwayTeam", "FTHome", "FTAway"]
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Missing required columns",
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

        optional_present = [source_col for source_col in optional_mapping if source_col in df.columns]
        optional_missing = [source_col for source_col in optional_mapping if source_col not in df.columns]

        division_counts = df["Division"].astype(str).value_counts().head(20).to_dict()

        epl_df = df[df["Division"].astype(str) == "E0"].copy()
        raw_epl_rows = int(len(epl_df))

        if epl_df.empty:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "No EPL rows found in xgabora CSV",
                "raw_downloaded_rows": int(len(df)),
                "division_counts_sample": division_counts,
            }

        epl_df["match_date"] = pd.to_datetime(epl_df["MatchDate"], errors="coerce", dayfirst=False).dt.date
        epl_df["home_goals"] = pd.to_numeric(epl_df["FTHome"], errors="coerce")
        epl_df["away_goals"] = pd.to_numeric(epl_df["FTAway"], errors="coerce")
        epl_df = epl_df.dropna(subset=["match_date", "HomeTeam", "AwayTeam", "home_goals", "away_goals"])

        min_available_date = str(epl_df["match_date"].min())
        max_available_date = str(epl_df["match_date"].max())

        start_date = pd.to_datetime("2020-07-01").date()
        epl_df = epl_df[epl_df["match_date"] >= start_date].copy()

        rows = []
        for _, row in epl_df.iterrows():
            home_goals = _safe_float(row["home_goals"])
            away_goals = _safe_float(row["away_goals"])
            if home_goals is None or away_goals is None:
                continue

            prepared = {
                "league_code": "EPL",
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
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "No rows prepared for EPL import after cleaning/date filter",
                "raw_epl_rows": raw_epl_rows,
                "min_available_date": min_available_date,
                "max_available_date": max_available_date,
                "import_start_date": start_date.isoformat(),
            }

        supabase.table("historical_matches").delete().eq("league_code", "EPL").execute()

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
        feature_non_null_counts = {
            target_col: sum(1 for item in rows if item.get(target_col) is not None)
            for target_col in optional_mapping.values()
        }

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "xgabora/Club-Football-Match-Data-2000-2025",
            "raw_downloaded_rows": int(len(df)),
            "raw_epl_rows": raw_epl_rows,
            "min_available_date": min_available_date,
            "max_available_date": max_available_date,
            "import_start_date": start_date.isoformat(),
            "prepared_rows": int(len(rows)),
            "upserted_rows": int(upserted_total),
            "teams_count": int(len(teams)),
            "optional_columns_present": optional_present,
            "optional_columns_missing": optional_missing,
            "feature_non_null_counts": feature_non_null_counts,
            "teams": teams,
            "division_counts_sample": division_counts,
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "error": str(e),
            "error_type": type(e).__name__,
        }


@app.post("/features/enrich-xgabora")
def enrich_xgabora_features():
    try:
        supabase = get_supabase()
        league_code = "EPL"

        aliases_resp = (
            supabase.table("team_aliases")
            .select("source_name, canonical_name")
            .eq("league_code", league_code)
            .execute()
        )
        aliases = {
            str(row["source_name"]).strip(): str(row["canonical_name"]).strip()
            for row in (aliases_resp.data or [])
        }

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
                "enriched_count": 0,
                "message": "No scheduled fixtures found",
            }

        history_resp = (
            supabase.table("historical_matches")
            .select(
                "match_date, home_team, away_team, home_goals, away_goals, "
                "home_elo, away_elo, form3_home, form3_away, form5_home, form5_away, "
                "odd_home, odd_draw, odd_away, max_home, max_draw, max_away"
            )
            .eq("league_code", league_code)
            .order("match_date", desc=True)
            .execute()
        )
        history_rows = history_resp.data or []
        if not history_rows:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "No historical_matches rows found for EPL",
            }

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

                days_since = None
                if fixture_date:
                    days_since = (fixture_date - match_date).days

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
                    "note": "latest historical xgabora row before fixture kickoff for each team",
                },
            })

        if not feature_rows:
            return {
                "status": "ok",
                "service": "forecast-api",
                "enriched_count": 0,
                "message": "No xgabora feature rows prepared",
            }

        upsert_resp = (
            supabase.table("match_features")
            .upsert(feature_rows, on_conflict="fixture_id,source,feature_key")
            .execute()
        )

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "xgabora",
            "feature_key": "elo_form_snapshot",
            "scheduled_fixtures_count": len(fixtures),
            "history_rows_count": len(history_rows),
            "prepared_history_rows_count": len(prepared_history),
            "enriched_count": len(feature_rows),
            "upserted_count": len(upsert_resp.data or feature_rows),
            "complete_snapshots_count": complete_snapshots_count,
            "missing_snapshots_count": len(missing_snapshots),
            "missing_snapshots_sample": missing_snapshots[:10],
            "message": "xgabora Elo/Form/Odds features saved to match_features",
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "error": str(e),
            "error_type": type(e).__name__,
        }
