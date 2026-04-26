from fixed_main import app, get_supabase


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


def _create_published_forecast(publication_type):
    try:
        supabase = get_supabase()

        if publication_type not in {"free", "premium"}:
            return {
                "status": "error",
                "service": "forecast-api",
                "created": False,
                "message": "publication_type must be free or premium",
            }

        selected_status = "selected_free" if publication_type == "free" else "selected_premium"
        published_status = "published_free" if publication_type == "free" else "published_premium"

        selected_resp = (
            supabase.table("forecast_candidates")
            .select("id, fixture_id, bookmaker_code, market_code, selection_code, implied_probability")
            .eq("candidate_status", selected_status)
            .order("id", desc=False)
            .execute()
        )
        selected_candidates = selected_resp.data or []

        if not selected_candidates:
            return {
                "status": "ok",
                "service": "forecast-api",
                "publication_type": publication_type,
                "created": False,
                "message": f"No {selected_status} candidate found",
            }

        candidate = None
        skipped_candidates = []
        for row in selected_candidates:
            existing_pub = (
                supabase.table("published_forecasts")
                .select("id, candidate_id, fixture_id, publication_type, publication_status")
                .eq("candidate_id", row["id"])
                .eq("publication_type", publication_type)
                .execute()
            )
            existing_rows = existing_pub.data or []
            if existing_rows:
                supabase.table("forecast_candidates").update({"candidate_status": published_status}).eq("id", row["id"]).execute()
                skipped_candidates.append({
                    "candidate_id": row["id"],
                    "fixture_id": row["fixture_id"],
                    "reason": "candidate_already_published_for_type",
                    "existing": existing_rows[:3],
                })
                continue

            existing_fixture_pub = (
                supabase.table("published_forecasts")
                .select("id, candidate_id, fixture_id, publication_type, publication_status")
                .eq("fixture_id", row["fixture_id"])
                .eq("publication_type", publication_type)
                .execute()
            )
            existing_fixture_rows = existing_fixture_pub.data or []
            if existing_fixture_rows:
                supabase.table("forecast_candidates").update({"candidate_status": published_status}).eq("id", row["id"]).execute()
                skipped_candidates.append({
                    "candidate_id": row["id"],
                    "fixture_id": row["fixture_id"],
                    "reason": "fixture_already_published_for_type",
                    "existing": existing_fixture_rows[:3],
                })
                continue

            candidate = row
            break

        if candidate is None:
            return {
                "status": "ok",
                "service": "forecast-api",
                "publication_type": publication_type,
                "created": False,
                "message": f"All {selected_status} candidates already have published_forecasts rows or fixture is already published",
                "skipped_candidates_sample": skipped_candidates[:10],
            }

        implied_probability = float(candidate["implied_probability"])
        odds_value = round(1.0 / implied_probability, 2) if implied_probability > 0 else None

        insert_payload = {
            "candidate_id": candidate["id"],
            "fixture_id": candidate["fixture_id"],
            "publication_type": publication_type,
            "publication_channel": "telegram_channel",
            "publication_status": "pending",
            "published_odds_value": odds_value,
        }

        insert_resp = (
            supabase.table("published_forecasts")
            .insert(insert_payload)
            .execute()
        )
        created_row = (insert_resp.data or [None])[0]

        supabase.table("forecast_candidates").update({"candidate_status": published_status}).eq("id", candidate["id"]).execute()

        fixture_resp = (
            supabase.table("fixtures")
            .select("id, home_team, away_team, kickoff_at")
            .eq("id", candidate["fixture_id"])
            .single()
            .execute()
        )
        fixture = fixture_resp.data or {}

        dossier_resp = (
            supabase.table("v_candidate_multisource_score_v1")
            .select(
                "candidate_id, source_support_count, source_contradiction_count, "
                "multisource_alignment_label, multisource_score_v1, "
                "understat_xg_diff_delta, espn_shots_delta, espn_sot_delta, clubelo_elo_delta"
            )
            .eq("candidate_id", candidate["id"])
            .execute()
        )
        dossier = (dossier_resp.data or [None])[0]

        publication_payload = {
            "published_forecast_id": created_row["id"] if created_row else None,
            "candidate_id": candidate["id"],
            "fixture_id": candidate["fixture_id"],
            "publication_type": publication_type,
            "home_team": fixture.get("home_team"),
            "away_team": fixture.get("away_team"),
            "kickoff_at": fixture.get("kickoff_at"),
            "bookmaker_code": candidate["bookmaker_code"],
            "market_code": candidate["market_code"],
            "selection_code": candidate["selection_code"],
            "odds_value": odds_value,
            "multisource_dossier": dossier,
        }

        return {
            "status": "ok",
            "service": "forecast-api",
            "publication_type": publication_type,
            "created": True,
            "published_forecast": created_row,
            "publication_payload": publication_payload,
            "skipped_candidates_sample": skipped_candidates[:10],
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "publication_type": publication_type,
            "created": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "message": "create published forecast failed; returned structured error instead of HTTP 500",
        }


_remove_route("/forecast/create-published-free", "POST")
_remove_route("/forecast/create-published-premium", "POST")


@app.post("/forecast/create-published-free")
def forecast_create_published_free_multisource():
    return _create_published_forecast("free")


@app.post("/forecast/create-published-premium")
def forecast_create_published_premium_multisource():
    return _create_published_forecast("premium")
