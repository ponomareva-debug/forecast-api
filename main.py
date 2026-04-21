import os
from fastapi import FastAPI, HTTPException
from supabase import create_client, Client

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
        "service": "forecast-api"
    }


@app.post("/forecast/run")
def forecast_run():
    try:
        supabase = get_supabase()

        fixtures_resp = (
            supabase.table("fixtures")
            .select("id", count="exact")
            .eq("event_status", "scheduled")
            .execute()
        )

        odds_resp = (
            supabase.table("odds_snapshots")
            .select("id", count="exact")
            .execute()
        )

        return {
            "status": "ok",
            "service": "forecast-api",
            "scheduled_fixtures_count": fixtures_resp.count or 0,
            "odds_snapshots_count": odds_resp.count or 0,
            "message": "Supabase connection works"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
