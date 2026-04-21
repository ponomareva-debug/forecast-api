import os

from fastapi import FastAPI, HTTPException
from supabase import Client, create_client

app = FastAPI(title="forecast-api", version="0.1.0")


def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing")

    return create_client(url, key)


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

        fixtures_resp = (
            supabase.table("fixtures")
            .select("id, external_event_id, home_team, away_team, kickoff_at")
            .eq("event_status", "scheduled")
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
                    "candidate_status": "generated",
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
