import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from supabase import Client, create_client

app = FastAPI(title="forecast-api", version="0.1.0")


class MarkPublishedRequest(BaseModel):
    published_forecast_id: int
    telegram_message_id: str | None = None
    message_text: str | None = None


class MarkFailedRequest(BaseModel):
    published_forecast_id: int
    message_text: str | None = None


def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing")

    return create_client(url, key)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "forecast-api",
    }


@app.post("/forecast/run")
def forecast_run():
    try:
        supabase = get_supabase()
        now_iso = utc_now_iso()

        fixtures_resp = (
            supabase.table("fixtures")
            .select("id, external_event_id, home_team, away_team, kickoff_at")
            .eq("event_status", "scheduled")
            .gt("kickoff_at", now_iso)
            .execute()
        )

        fixtures = fixtures_resp.data or []
        if not fixtures:
            return {
                "status": "ok",
                "service": "forecast-api",
                "generated_candidates": 0,
                "message": "No scheduled future fixtures found",
            }

        fixture_ids = [f["id"] for f in fixtures]

        odds_resp = (
            supabase.table("odds_snapshots")
            .select(
                "fixture_id, bookmaker_code, market_code, selection_code, odds_value, snapshot_time"
            )
            .in_("fixture_id", fixture_ids)
            .eq("market_code", "h2h")
            .execute()
        )

        odds_rows = odds_resp.data or []
        if not odds_rows:
            return {
                "status": "ok",
                "service": "forecast-api",
                "generated_candidates": 0,
                "message": "No h2h odds found",
            }

        existing_candidates_resp = (
            supabase.table("forecast_candidates")
            .select(
                "id, fixture_id, bookmaker_code, market_code, selection_code, candidate_status"
            )
            .in_("fixture_id", fixture_ids)
            .eq("market_code", "h2h")
            .execute()
        )

        existing_candidates = existing_candidates_resp.data or []
        existing_map = {
            (
                row["fixture_id"],
                row["bookmaker_code"],
                row["market_code"],
                row["selection_code"],
            ): row
            for row in existing_candidates
        }

        latest_by_key = {}

        for row in odds_rows:
            key = (
                row["fixture_id"],
                row["bookmaker_code"],
                row["market_code"],
                row["selection_code"],
            )

            current = latest_by_key.get(key)
            if current is None or row["snapshot_time"] > current["snapshot_time"]:
                latest_by_key[key] = row

        candidates = []

        for row in latest_by_key.values():
            odds_value = float(row["odds_value"])
            if odds_value <= 1:
                continue

            implied_probability = 1.0 / odds_value

            key = (
                row["fixture_id"],
                row["bookmaker_code"],
                row["market_code"],
                row["selection_code"],
            )
            existing = existing_map.get(key)

            candidate_status = (
                existing["candidate_status"] if existing else "generated"
            )

            candidates.append(
                {
                    "fixture_id": row["fixture_id"],
                    "bookmaker_code": row["bookmaker_code"],
                    "market_code": row["market_code"],
                    "selection_code": row["selection_code"],
                    "model_probability": round(implied_probability, 6),
                    "implied_probability": round(implied_probability, 6),
                    "fair_probability": round(implied_probability, 6),
                    "edge": 0,
                    "ev": 0,
                    "confidence": 0.5,
                    "candidate_status": candidate_status,
                }
            )

        if not candidates:
            return {
                "status": "ok",
                "service": "forecast-api",
                "generated_candidates": 0,
                "message": "No valid candidates after filtering",
            }

        upsert_resp = (
            supabase.table("forecast_candidates")
            .upsert(
                candidates,
                on_conflict="fixture_id,bookmaker_code,market_code,selection_code",
            )
            .execute()
        )

        upserted_count = len(upsert_resp.data or [])

        return {
            "status": "ok",
            "service": "forecast-api",
            "generated_candidates": upserted_count,
            "scheduled_fixtures_count": len(fixtures),
            "latest_h2h_rows_count": len(latest_by_key),
            "message": "Candidates generated",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/forecast/select-free")
def forecast_select_free():
    try:
        supabase = get_supabase()

        published_free_resp = (
            supabase.table("published_forecasts")
            .select("fixture_id")
            .eq("publication_type", "free")
            .execute()
        )
        published_free_rows = published_free_resp.data or []
        blocked_fixture_ids = {row["fixture_id"] for row in published_free_rows}

        reserved_resp = (
            supabase.table("forecast_candidates")
            .select("id, fixture_id")
            .eq("candidate_status", "selected_free")
            .execute()
        )
        reserved_rows = reserved_resp.data or []
        blocked_fixture_ids.update(row["fixture_id"] for row in reserved_rows)

        candidates_resp = (
            supabase.table("forecast_candidates")
            .select(
                "id, fixture_id, bookmaker_code, market_code, selection_code, "
                "model_probability, implied_probability, fair_probability, edge, ev, confidence, generated_at"
            )
            .eq("candidate_status", "generated")
            .order("confidence", desc=True)
            .order("id", desc=False)
            .execute()
        )

        candidates = candidates_resp.data or []
        if not candidates:
            return {
                "status": "ok",
                "service": "forecast-api",
                "selected": False,
                "message": "No generated candidates found",
            }

        selected_candidate = None

        for candidate in candidates:
            if candidate["fixture_id"] in blocked_fixture_ids:
                continue
            selected_candidate = candidate
            break

        if selected_candidate is None:
            return {
                "status": "ok",
                "service": "forecast-api",
                "selected": False,
                "message": "No eligible free candidate found",
            }

        update_resp = (
            supabase.table("forecast_candidates")
            .update({"candidate_status": "selected_free"})
            .eq("id", selected_candidate["id"])
            .execute()
        )

        updated = (update_resp.data or [None])[0]

        return {
            "status": "ok",
            "service": "forecast-api",
            "selected": True,
            "candidate": updated,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/forecast/create-published-free")
def forecast_create_published_free():
    try:
        supabase = get_supabase()

        selected_resp = (
            supabase.table("forecast_candidates")
            .select(
                "id, fixture_id, bookmaker_code, market_code, selection_code, implied_probability"
            )
            .eq("candidate_status", "selected_free")
            .order("id", desc=False)
            .execute()
        )

        selected_candidates = selected_resp.data or []
        if not selected_candidates:
            return {
                "status": "ok",
                "service": "forecast-api",
                "created": False,
                "message": "No selected_free candidate found",
            }

        candidate = None

        for row in selected_candidates:
            existing_pub = (
                supabase.table("published_forecasts")
                .select("id, candidate_id, publication_type")
                .eq("candidate_id", row["id"])
                .eq("publication_type", "free")
                .execute()
            )

            if existing_pub.data or []:
                (
                    supabase.table("forecast_candidates")
                    .update({"candidate_status": "published_free"})
                    .eq("id", row["id"])
                    .execute()
                )
                continue

            candidate = row
            break

        if candidate is None:
            return {
                "status": "ok",
                "service": "forecast-api",
                "created": False,
                "message": "All selected_free candidates already have published_forecasts rows",
            }

        implied_probability = float(candidate["implied_probability"])
        odds_value = round(1.0 / implied_probability, 2) if implied_probability > 0 else None

        insert_resp = (
            supabase.table("published_forecasts")
            .insert(
                {
                    "candidate_id": candidate["id"],
                    "fixture_id": candidate["fixture_id"],
                    "publication_type": "free",
                    "publication_channel": "telegram_channel",
                    "publication_status": "pending",
                    "published_odds_value": odds_value,
                }
            )
            .execute()
        )

        created_row = (insert_resp.data or [None])[0]

        (
            supabase.table("forecast_candidates")
            .update({"candidate_status": "published_free"})
            .eq("id", candidate["id"])
            .execute()
        )

        fixture_resp = (
            supabase.table("fixtures")
            .select("id, home_team, away_team, kickoff_at")
            .eq("id", candidate["fixture_id"])
            .single()
            .execute()
        )

        fixture = fixture_resp.data or {}

        publication_payload = {
            "published_forecast_id": created_row["id"],
            "candidate_id": candidate["id"],
            "fixture_id": candidate["fixture_id"],
            "home_team": fixture.get("home_team"),
            "away_team": fixture.get("away_team"),
            "kickoff_at": fixture.get("kickoff_at"),
            "bookmaker_code": candidate["bookmaker_code"],
            "market_code": candidate["market_code"],
            "selection_code": candidate["selection_code"],
            "odds_value": odds_value,
        }

        return {
            "status": "ok",
            "service": "forecast-api",
            "created": True,
            "published_forecast": created_row,
            "publication_payload": publication_payload,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/forecast/mark-published")
def forecast_mark_published(payload: MarkPublishedRequest):
    try:
        supabase = get_supabase()

        update_resp = (
            supabase.table("published_forecasts")
            .update(
                {
                    "publication_status": "sent",
                    "telegram_message_id": payload.telegram_message_id,
                    "message_text": payload.message_text,
                }
            )
            .eq("id", payload.published_forecast_id)
            .execute()
        )

        updated_row = (update_resp.data or [None])[0]

        return {
            "status": "ok",
            "service": "forecast-api",
            "updated": True,
            "published_forecast": updated_row,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/forecast/mark-failed")
def forecast_mark_failed(payload: MarkFailedRequest):
    try:
        supabase = get_supabase()

        update_resp = (
            supabase.table("published_forecasts")
            .update(
                {
                    "publication_status": "failed",
                    "telegram_message_id": None,
                    "message_text": payload.message_text,
                }
            )
            .eq("id", payload.published_forecast_id)
            .execute()
        )

        updated_row = (update_resp.data or [None])[0]

        return {
            "status": "ok",
            "service": "forecast-api",
            "updated": True,
            "published_forecast": updated_row,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/forecast/settle")
def forecast_settle():
    try:
        supabase = get_supabase()

        published_resp = (
            supabase.table("published_forecasts")
            .select(
                "id, candidate_id, fixture_id, publication_status, published_odds_value"
            )
            .eq("publication_status", "sent")
            .execute()
        )

        published_rows = published_resp.data or []
        if not published_rows:
            return {
                "status": "ok",
                "service": "forecast-api",
                "settled_count": 0,
                "message": "No sent published forecasts found",
            }

        published_ids = [row["id"] for row in published_rows]
        fixture_ids = list({row["fixture_id"] for row in published_rows})
        candidate_ids = list({row["candidate_id"] for row in published_rows})

        existing_results_resp = (
            supabase.table("forecast_results")
            .select("published_forecast_id")
            .in_("published_forecast_id", published_ids)
            .execute()
        )
        existing_results = existing_results_resp.data or []
        settled_published_ids = {row["published_forecast_id"] for row in existing_results}

        fixtures_resp = (
            supabase.table("fixtures")
            .select("id, event_status, home_score, away_score")
            .in_("id", fixture_ids)
            .eq("event_status", "finished")
            .execute()
        )
        fixtures_rows = fixtures_resp.data or []
        fixtures_map = {row["id"]: row for row in fixtures_rows}

        candidates_resp = (
            supabase.table("forecast_candidates")
            .select("id, fixture_id, market_code, selection_code")
            .in_("id", candidate_ids)
            .execute()
        )
        candidates_rows = candidates_resp.data or []
        candidates_map = {row["id"]: row for row in candidates_rows}

        rows_to_insert = []

        for pub in published_rows:
            published_forecast_id = pub["id"]

            if published_forecast_id in settled_published_ids:
                continue

            fixture = fixtures_map.get(pub["fixture_id"])
            if not fixture:
                continue

            candidate = candidates_map.get(pub["candidate_id"])
            if not candidate:
                continue

            if candidate["market_code"] != "h2h":
                continue

            home_score = fixture.get("home_score")
            away_score = fixture.get("away_score")

            if home_score is None or away_score is None:
                continue

            if home_score > away_score:
                actual_selection = "home"
            elif away_score > home_score:
                actual_selection = "away"
            else:
                actual_selection = "draw"

            selection_code = candidate["selection_code"]

            if selection_code == actual_selection:
                outcome = "won"
            else:
                outcome = "lost"

            odds_value = (
                float(pub["published_odds_value"])
                if pub.get("published_odds_value") is not None
                else None
            )

            if odds_value is None or odds_value <= 1:
                continue

            if outcome == "won":
                profit_units = round(odds_value - 1, 2)
            elif outcome == "lost":
                profit_units = -1.0
            else:
                profit_units = 0.0

            rows_to_insert.append(
                {
                    "published_forecast_id": published_forecast_id,
                    "fixture_id": pub["fixture_id"],
                    "settlement_status": "settled",
                    "outcome": outcome,
                    "profit_units": profit_units,
                    "settled_at": utc_now_iso(),
                }
            )

        if not rows_to_insert:
            return {
                "status": "ok",
                "service": "forecast-api",
                "settled_count": 0,
                "message": "No eligible forecasts to settle",
            }

        insert_resp = (
            supabase.table("forecast_results")
            .insert(rows_to_insert)
            .execute()
        )

        inserted_count = len(insert_resp.data or [])

        return {
            "status": "ok",
            "service": "forecast-api",
            "settled_count": inserted_count,
            "message": "Forecasts settled",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/penaltyblog-model-test")
def debug_penaltyblog_model_test():
    try:
        import numpy as np
        import pandas as pd
        import penaltyblog as pb

        data = [
            {"team_home": "Arsenal", "team_away": "Chelsea", "goals_home": 2, "goals_away": 1},
            {"team_home": "Chelsea", "team_away": "Arsenal", "goals_home": 1, "goals_away": 1},
            {"team_home": "Arsenal", "team_away": "Tottenham", "goals_home": 3, "goals_away": 1},
            {"team_home": "Tottenham", "team_away": "Arsenal", "goals_home": 0, "goals_away": 2},
            {"team_home": "Chelsea", "team_away": "Tottenham", "goals_home": 2, "goals_away": 2},
            {"team_home": "Tottenham", "team_away": "Chelsea", "goals_home": 1, "goals_away": 2},
            {"team_home": "Arsenal", "team_away": "Chelsea", "goals_home": 1, "goals_away": 0},
            {"team_home": "Chelsea", "team_away": "Tottenham", "goals_home": 2, "goals_away": 0},
            {"team_home": "Tottenham", "team_away": "Arsenal", "goals_home": 1, "goals_away": 3},
            {"team_home": "Arsenal", "team_away": "Tottenham", "goals_home": 2, "goals_away": 2},
        ]

        df = pd.DataFrame(data).copy()

        goals_home = np.array(df["goals_home"].to_numpy(), dtype=np.int64, copy=True)
        goals_away = np.array(df["goals_away"].to_numpy(), dtype=np.int64, copy=True)
        team_home = np.array(df["team_home"].astype(str).to_numpy(), dtype=object, copy=True)
        team_away = np.array(df["team_away"].astype(str).to_numpy(), dtype=object, copy=True)

        goals_home.setflags(write=True)
        goals_away.setflags(write=True)
        team_home.setflags(write=True)
        team_away.setflags(write=True)

        model = pb.models.DixonColesGoalModel(
            goals_home,
            goals_away,
            team_home,
            team_away,
        )

        model.fit(
            use_gradient=True,
            minimizer_options={"maxiter": 3000}
        )

        prediction = model.predict("Arsenal", "Chelsea")
        probs = prediction.home_draw_away

        return {
            "status": "ok",
            "model": "DixonColesGoalModel",
            "home_team": "Arsenal",
            "away_team": "Chelsea",
            "home_probability": round(float(probs[0]), 6),
            "draw_probability": round(float(probs[1]), 6),
            "away_probability": round(float(probs[2]), 6),
            "sum_probability": round(float(sum(probs)), 6),
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
        }

@app.post("/admin/import-xgabora-epl")
def import_xgabora_epl():
    try:
        import io
        import math
        import requests
        import pandas as pd

        supabase = get_supabase()

        csv_url = (
            "https://raw.githubusercontent.com/"
            "xgabora/Club-Football-Match-Data-2000-2025/main/data/Matches.csv"
        )

        resp = requests.get(csv_url, timeout=120)

        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to download CSV: {resp.status_code} {resp.text[:300]}"
            )

        df = pd.read_csv(io.StringIO(resp.text))

        required_columns = [
            "Division",
            "MatchDate",
            "HomeTeam",
            "AwayTeam",
            "FTHome",
            "FTAway",
        ]

        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            return {
                "status": "error",
                "message": "Missing required columns",
                "missing_columns": missing,
                "available_columns": list(df.columns),
            }

        division_counts = (
            df["Division"]
            .astype(str)
            .value_counts()
            .head(20)
            .to_dict()
        )

        epl_df = df[df["Division"].astype(str) == "E0"].copy()

        raw_epl_rows = len(epl_df)

        epl_df = epl_df[required_columns].copy()

        epl_df["match_date"] = pd.to_datetime(
            epl_df["MatchDate"],
            errors="coerce",
            dayfirst=True,
        ).dt.date

        epl_df["home_goals"] = pd.to_numeric(epl_df["FTHome"], errors="coerce")
        epl_df["away_goals"] = pd.to_numeric(epl_df["FTAway"], errors="coerce")

        epl_df = epl_df.dropna(
            subset=[
                "match_date",
                "HomeTeam",
                "AwayTeam",
                "home_goals",
                "away_goals",
            ]
        )

        min_available_date = str(epl_df["match_date"].min())
        max_available_date = str(epl_df["match_date"].max())

        start_date = pd.to_datetime("2020-07-01").date()

        epl_df = epl_df[epl_df["match_date"] >= start_date].copy()

        if epl_df.empty:
            return {
                "status": "error",
                "message": "No EPL rows after date filter",
                "raw_epl_rows": raw_epl_rows,
                "min_available_date": min_available_date,
                "max_available_date": max_available_date,
                "start_date": start_date.isoformat(),
                "division_counts_sample": division_counts,
            }

        rows = []

        for _, row in epl_df.iterrows():
            home_goals = int(row["home_goals"])
            away_goals = int(row["away_goals"])

            if not math.isfinite(home_goals) or not math.isfinite(away_goals):
                continue

            rows.append(
                {
                    "league_code": "EPL",
                    "season": None,
                    "match_date": row["match_date"].isoformat(),
                    "home_team": str(row["HomeTeam"]).strip(),
                    "away_team": str(row["AwayTeam"]).strip(),
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                    "source": "xgabora_matches_csv",
                }
            )

        if not rows:
            return {
                "status": "error",
                "message": "No rows prepared for import",
                "raw_epl_rows": raw_epl_rows,
                "min_available_date": min_available_date,
                "max_available_date": max_available_date,
                "start_date": start_date.isoformat(),
            }

        # Чистим старый импорт EPL, чтобы не смешивать 2000-е с новым рабочим окном.
        (
            supabase.table("historical_matches")
            .delete()
            .eq("league_code", "EPL")
            .execute()
        )

        batch_size = 500
        upserted_total = 0

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]

            result = (
                supabase.table("historical_matches")
                .upsert(
                    batch,
                    on_conflict="league_code,match_date,home_team,away_team",
                )
                .execute()
            )

            # Supabase может вернуть data, но нам важнее факт успешного запроса.
            upserted_total += len(batch)

        teams = sorted(
            set([r["home_team"] for r in rows] + [r["away_team"] for r in rows])
        )

        return {
            "status": "ok",
            "source": "xgabora/Club-Football-Match-Data-2000-2025",
            "raw_downloaded_rows": len(df),
            "raw_epl_rows": raw_epl_rows,
            "min_available_date": min_available_date,
            "max_available_date": max_available_date,
            "import_start_date": start_date.isoformat(),
            "prepared_rows": len(rows),
            "upserted_rows": upserted_total,
            "teams_count": len(teams),
            "teams": teams,
            "division_counts_sample": division_counts,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
        }

@app.get("/debug/penaltyblog-real-data-test")
def debug_penaltyblog_real_data_test():
    try:
        import numpy as np
        import pandas as pd
        import penaltyblog as pb

        supabase = get_supabase()

        league_code = "EPL"

        # 1. История матчей
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
            return {
                "status": "error",
                "message": "Not enough historical matches",
                "historical_rows": len(history_rows),
            }

        # 2. Aliases
        aliases_resp = (
            supabase.table("team_aliases")
            .select("source_name, canonical_name")
            .eq("league_code", league_code)
            .execute()
        )

        alias_rows = aliases_resp.data or []

        aliases = {
            str(row["source_name"]).strip(): str(row["canonical_name"]).strip()
            for row in alias_rows
        }

        def canonical_team_name(name: str) -> str:
            clean_name = str(name).strip()
            return aliases.get(clean_name, clean_name)

        # 3. Подготовка history dataframe
        df = pd.DataFrame(history_rows).copy()

        df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
        df["home_goals"] = pd.to_numeric(df["home_goals"], errors="coerce")
        df["away_goals"] = pd.to_numeric(df["away_goals"], errors="coerce")
        df["home_team"] = df["home_team"].astype(str).str.strip()
        df["away_team"] = df["away_team"].astype(str).str.strip()

        df = df.dropna(
            subset=["match_date", "home_team", "away_team", "home_goals", "away_goals"]
        )

        if len(df) < 100:
            return {
                "status": "error",
                "message": "Not enough clean historical matches after parsing",
                "historical_rows_after_cleaning": len(df),
            }

        df = df.sort_values("match_date", ascending=True).copy()

        history_teams = set(df["home_team"].tolist() + df["away_team"].tolist())

        # 4. Берём ближайшие scheduled fixtures
        fixtures_resp = (
            supabase.table("fixtures")
            .select("id, home_team, away_team, kickoff_at")
            .eq("league_code", league_code)
            .eq("event_status", "scheduled")
            .order("kickoff_at", desc=False)
            .limit(20)
            .execute()
        )

        fixture_rows = fixtures_resp.data or []

        if not fixture_rows:
            return {
                "status": "error",
                "message": "No scheduled EPL fixtures found",
            }

        selected_fixture = None
        skipped_fixtures = []

        for fixture in fixture_rows:
            raw_home = str(fixture["home_team"]).strip()
            raw_away = str(fixture["away_team"]).strip()

            mapped_home = canonical_team_name(raw_home)
            mapped_away = canonical_team_name(raw_away)

            home_in_history = mapped_home in history_teams
            away_in_history = mapped_away in history_teams

            if home_in_history and away_in_history:
                selected_fixture = {
                    **fixture,
                    "raw_home_team": raw_home,
                    "raw_away_team": raw_away,
                    "mapped_home_team": mapped_home,
                    "mapped_away_team": mapped_away,
                }
                break

            skipped_fixtures.append(
                {
                    "fixture_id": fixture["id"],
                    "kickoff_at": fixture["kickoff_at"],
                    "raw_home_team": raw_home,
                    "raw_away_team": raw_away,
                    "mapped_home_team": mapped_home,
                    "mapped_away_team": mapped_away,
                    "home_in_history": home_in_history,
                    "away_in_history": away_in_history,
                }
            )

        if selected_fixture is None:
            return {
                "status": "error",
                "message": "No scheduled fixture found where both teams exist in historical training data",
                "history_teams_count": len(history_teams),
                "history_teams": sorted(list(history_teams)),
                "aliases": aliases,
                "skipped_fixtures": skipped_fixtures,
            }

        # 5. Подготовка numpy arrays, writable copy
        goals_home = np.array(df["home_goals"].to_numpy(), dtype=np.int64, copy=True)
        goals_away = np.array(df["away_goals"].to_numpy(), dtype=np.int64, copy=True)
        team_home = np.array(df["home_team"].astype(str).to_numpy(), dtype=object, copy=True)
        team_away = np.array(df["away_team"].astype(str).to_numpy(), dtype=object, copy=True)

        goals_home.setflags(write=True)
        goals_away.setflags(write=True)
        team_home.setflags(write=True)
        team_away.setflags(write=True)

        # 6. Time weights для Dixon-Coles
        weights = pb.models.dixon_coles_weights(df["match_date"], xi=0.001)
        weights = np.array(weights, dtype=np.float64, copy=True)
        weights.setflags(write=True)

        # 7. Обучение модели
        model = pb.models.DixonColesGoalModel(
            goals_home,
            goals_away,
            team_home,
            team_away,
            weights,
        )

        model.fit(
            use_gradient=True,
            minimizer_options={"maxiter": 3000}
        )

        home_team = selected_fixture["mapped_home_team"]
        away_team = selected_fixture["mapped_away_team"]

        prediction = model.predict(home_team, away_team)
        probs = prediction.home_draw_away

        return {
            "status": "ok",
            "model": "DixonColesGoalModel",
            "historical_rows_used": len(df),
            "history_teams_count": len(history_teams),
            "fixture": {
                "id": selected_fixture["id"],
                "kickoff_at": selected_fixture["kickoff_at"],
                "raw_home_team": selected_fixture["raw_home_team"],
                "raw_away_team": selected_fixture["raw_away_team"],
                "mapped_home_team": selected_fixture["mapped_home_team"],
                "mapped_away_team": selected_fixture["mapped_away_team"],
            },
            "probabilities": {
                "home": round(float(probs[0]), 6),
                "draw": round(float(probs[1]), 6),
                "away": round(float(probs[2]), 6),
                "sum": round(float(sum(probs)), 6),
            },
            "expectations": {
                "home_goals": round(float(prediction.home_goal_expectation), 4),
                "away_goals": round(float(prediction.away_goal_expectation), 4),
            },
            "skipped_fixtures_before_selected": skipped_fixtures,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
        }

@app.get("/debug/team-name-check")
def debug_team_name_check():
    try:
        supabase = get_supabase()

        history_resp = (
            supabase.table("historical_matches")
            .select("home_team, away_team")
            .eq("league_code", "EPL")
            .execute()
        )

        fixture_resp = (
            supabase.table("fixtures")
            .select("id, home_team, away_team, kickoff_at")
            .eq("league_code", "EPL")
            .eq("event_status", "scheduled")
            .order("kickoff_at", desc=False)
            .limit(10)
            .execute()
        )

        history_rows = history_resp.data or []
        fixture_rows = fixture_resp.data or []

        history_teams = set()

        for row in history_rows:
            history_teams.add(str(row["home_team"]).strip())
            history_teams.add(str(row["away_team"]).strip())

        checks = []

        for fixture in fixture_rows:
            home_team = str(fixture["home_team"]).strip()
            away_team = str(fixture["away_team"]).strip()

            checks.append(
                {
                    "fixture_id": fixture["id"],
                    "kickoff_at": fixture["kickoff_at"],
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_in_history": home_team in history_teams,
                    "away_in_history": away_team in history_teams,
                }
            )

        return {
            "status": "ok",
            "history_teams_count": len(history_teams),
            "history_teams_sample": sorted(list(history_teams))[:80],
            "fixtures_checked": checks,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
        }

@app.get("/debug/xgabora-epl-date-check")
def debug_xgabora_epl_date_check():
    try:
        import io
        import requests
        import pandas as pd

        csv_url = (
            "https://raw.githubusercontent.com/"
            "xgabora/Club-Football-Match-Data-2000-2025/main/data/Matches.csv"
        )

        resp = requests.get(csv_url, timeout=120)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to download CSV: {resp.status_code} {resp.text[:300]}")

        df = pd.read_csv(io.StringIO(resp.text))

        epl = df[df["Division"].astype(str) == "E0"].copy()

        # Важно: Football-Data часто хранит даты в dayfirst формате.
        epl["parsed_date"] = pd.to_datetime(
            epl["MatchDate"],
            errors="coerce",
            dayfirst=True
        )

        epl = epl.dropna(subset=["parsed_date"])

        latest = (
            epl.sort_values("parsed_date", ascending=False)
            [["Division", "MatchDate", "parsed_date", "HomeTeam", "AwayTeam", "FTHome", "FTAway"]]
            .head(30)
            .to_dict(orient="records")
        )

        earliest = (
            epl.sort_values("parsed_date", ascending=True)
            [["Division", "MatchDate", "parsed_date", "HomeTeam", "AwayTeam", "FTHome", "FTAway"]]
            .head(10)
            .to_dict(orient="records")
        )

        teams_latest_3_seasons = sorted(
            set(
                epl[epl["parsed_date"] >= "2022-07-01"]["HomeTeam"].astype(str).tolist()
                + epl[epl["parsed_date"] >= "2022-07-01"]["AwayTeam"].astype(str).tolist()
            )
        )

        return {
            "status": "ok",
            "epl_rows": len(epl),
            "min_date": str(epl["parsed_date"].min().date()),
            "max_date": str(epl["parsed_date"].max().date()),
            "latest_30": latest,
            "earliest_10": earliest,
            "teams_latest_3_seasons": teams_latest_3_seasons,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
        }
@app.post("/forecast/run-v2")
def forecast_run_v2():
    try:
        import numpy as np
        import pandas as pd
        import penaltyblog as pb

        supabase = get_supabase()

        league_code = "EPL"
        market_code = "h2h"

        # 1. История матчей
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
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Not enough historical matches",
                "historical_rows": len(history_rows),
            }

        # 2. Aliases
        aliases_resp = (
            supabase.table("team_aliases")
            .select("source_name, canonical_name")
            .eq("league_code", league_code)
            .execute()
        )

        alias_rows = aliases_resp.data or []

        aliases = {
            str(row["source_name"]).strip(): str(row["canonical_name"]).strip()
            for row in alias_rows
        }

        def canonical_team_name(name: str) -> str:
            clean_name = str(name).strip()
            return aliases.get(clean_name, clean_name)

        # 3. Подготовка history dataframe
        df = pd.DataFrame(history_rows).copy()

        df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
        df["home_goals"] = pd.to_numeric(df["home_goals"], errors="coerce")
        df["away_goals"] = pd.to_numeric(df["away_goals"], errors="coerce")
        df["home_team"] = df["home_team"].astype(str).str.strip()
        df["away_team"] = df["away_team"].astype(str).str.strip()

        df = df.dropna(
            subset=["match_date", "home_team", "away_team", "home_goals", "away_goals"]
        )

        if len(df) < 100:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Not enough clean historical matches after parsing",
                "historical_rows_after_cleaning": len(df),
            }

        df = df.sort_values("match_date", ascending=True).copy()

        training_start_date = df["match_date"].min().date().isoformat()
        training_end_date = df["match_date"].max().date().isoformat()

        try:
            model_run_resp = (
                supabase.table("forecast_model_runs")
                .insert(
                    {
                        "model_name": "DixonColesGoalModel",
                        "model_version": "quant_v1_penaltyblog",
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
                    }
                )
                .execute()
            )

            model_run = (model_run_resp.data or [None])[0]
            model_run_id = model_run["id"] if model_run else None

        except Exception as insert_error:
            model_run_insert_error = str(insert_error)

        history_teams = set(df["home_team"].tolist() + df["away_team"].tolist())

        # 4. Обучаем Dixon-Coles
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

        model = pb.models.DixonColesGoalModel(
            goals_home,
            goals_away,
            team_home,
            team_away,
            weights,
        )

        model.fit(
            use_gradient=True,
            minimizer_options={"maxiter": 3000}
        )

        # 5. Берём scheduled fixtures
        fixtures_resp = (
            supabase.table("fixtures")
            .select("id, external_event_id, home_team, away_team, kickoff_at")
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
                "generated_candidates": 0,
                "message": "No scheduled fixtures found",
            }

        fixture_ids = [f["id"] for f in fixtures]

        # 6. Берём h2h odds
        odds_resp = (
            supabase.table("odds_snapshots")
            .select(
                "fixture_id, bookmaker_code, market_code, selection_code, odds_value, snapshot_time"
            )
            .in_("fixture_id", fixture_ids)
            .eq("market_code", market_code)
            .execute()
        )

        odds_rows = odds_resp.data or []

        if not odds_rows:
            return {
                "status": "ok",
                "service": "forecast-api",
                "generated_candidates": 0,
                "message": "No h2h odds found",
            }

        # 7. Оставляем только latest odds по fixture/bookmaker/market/selection
        latest_by_key = {}

        for row in odds_rows:
            key = (
                row["fixture_id"],
                row["bookmaker_code"],
                row["market_code"],
                row["selection_code"],
            )

            current = latest_by_key.get(key)

            if current is None or row["snapshot_time"] > current["snapshot_time"]:
                latest_by_key[key] = row

        # 8. Группируем odds по fixture + bookmaker
        odds_by_fixture_bookmaker = {}

        for row in latest_by_key.values():
            key = (row["fixture_id"], row["bookmaker_code"])
            odds_by_fixture_bookmaker.setdefault(key, []).append(row)

        fixtures_map = {f["id"]: f for f in fixtures}

        selection_to_index = {
            "home": 0,
            "draw": 1,
            "away": 2,
        }

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
                skipped_fixtures.append(
                    {
                        "fixture_id": fixture_id,
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "mapped_home_team": mapped_home_team,
                        "mapped_away_team": mapped_away_team,
                        "reason": "team_not_in_training_data",
                    }
                )
                continue

            # Нужно иметь все 3 исхода h2h
            by_selection = {
                str(row["selection_code"]).strip(): row
                for row in rows
                if str(row["selection_code"]).strip() in selection_to_index
            }

            if set(by_selection.keys()) != {"home", "draw", "away"}:
                skipped_fixtures.append(
                    {
                        "fixture_id": fixture_id,
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "reason": "missing_h2h_selection",
                        "available_selections": sorted(list(by_selection.keys())),
                    }
                )
                continue

            # Валидация odds
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
                skipped_fixtures.append(
                    {
                        "fixture_id": fixture_id,
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "reason": "invalid_odds",
                    }
                )
                continue

            # 9. Prediction
            prediction = model.predict(mapped_home_team, mapped_away_team)
            probs = prediction.home_draw_away

            model_probabilities = {
                "home": float(probs[0]),
                "draw": float(probs[1]),
                "away": float(probs[2]),
            }

            # 10. Снятие маржи букмекера
            implied_probabilities = {
                selection_code: 1.0 / odds_values[selection_code]
                for selection_code in ["home", "draw", "away"]
            }

            overround = sum(implied_probabilities.values())

            if not np.isfinite(overround) or overround <= 1.0 or overround > 1.35:
                skipped_fixtures.append(
                    {
                        "fixture_id": fixture_id,
                        "home_team": raw_home_team,
                        "away_team": raw_away_team,
                        "reason": "bad_overround",
                        "overround": round(float(overround), 6) if np.isfinite(overround) else None,
                    }
                )
                continue

            fair_probabilities = {
                selection_code: implied_probabilities[selection_code] / overround
                for selection_code in ["home", "draw", "away"]
            }

            predicted_fixtures_count += 1

            # 11. Candidates
            for selection_code in ["home", "draw", "away"]:
                odds_value = odds_values[selection_code]
                model_probability = model_probabilities[selection_code]
                implied_probability = implied_probabilities[selection_code]
                fair_probability = fair_probabilities[selection_code]

                edge = model_probability - fair_probability
                ev = (model_probability * odds_value) - 1.0

                # Confidence v1:
                # не "вероятность победы", а качество value-сигнала
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

                candidates.append(
                    {
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
                        "candidate_status": "generated",
                    }
                )

        if not candidates:
            return {
                "status": "ok",
                "service": "forecast-api",
                "generated_candidates": 0,
                "scheduled_fixtures_count": len(fixtures),
                "latest_h2h_rows_count": len(latest_by_key),
                "predicted_fixtures_count": predicted_fixtures_count,
                "skipped_fixtures": skipped_fixtures[:20],
                "message": "No valid candidates generated",
            }

        # 12. Upsert candidates
        upsert_resp = (
            supabase.table("forecast_candidates")
            .upsert(
                candidates,
                on_conflict="fixture_id,bookmaker_code,market_code,selection_code",
            )
            .execute()
        )

        upserted_count = len(upsert_resp.data or candidates)

        return {
            "status": "ok",
            "service": "forecast-api",
            "model": "DixonColesGoalModel",
            "generated_candidates": upserted_count,
            "scheduled_fixtures_count": len(fixtures),
            "predicted_fixtures_count": predicted_fixtures_count,
            "latest_h2h_rows_count": len(latest_by_key),
            "skipped_fixtures_count": len(skipped_fixtures),
            "skipped_fixtures_sample": skipped_fixtures[:10],
            "message": "Candidates generated with forecast_run_v2",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
