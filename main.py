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
