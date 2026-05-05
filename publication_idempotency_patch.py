import publication_extensions
from fixed_main import get_supabase


CANDIDATE_SELECT = (
    "id, fixture_id, bookmaker_code, market_code, selection_code, "
    "model_probability, implied_probability, fair_probability, edge, ev, confidence"
)

DOSSIER_SELECT = (
    "candidate_id, model_probability, implied_probability, fair_probability, edge, ev, confidence, "
    "pb_home_probability, pb_draw_probability, pb_away_probability, "
    "pb_home_goal_expectation, pb_away_goal_expectation, "
    "source_support_count, source_contradiction_count, "
    "multisource_alignment_label, multisource_score_v1, "
    "xg_elo_delta, xg_form5_delta, clubelo_elo_delta, "
    "understat_xg_diff_delta, understat_xg_for_delta, understat_xg_against_delta, "
    "espn_shots_delta, espn_sot_delta, espn_goals_delta, espn_goals_conceded_delta"
)


def _candidate_by_id(supabase, candidate_id):
    resp = (
        supabase.table("forecast_candidates")
        .select(CANDIDATE_SELECT)
        .eq("id", candidate_id)
        .single()
        .execute()
    )
    return resp.data or None


def _build_publication_response(supabase, publication_type, candidate, published_forecast, created, message=None):
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
        .select(DOSSIER_SELECT)
        .eq("candidate_id", candidate["id"])
        .execute()
    )
    dossier = (dossier_resp.data or [None])[0] or {}

    odds_value = None
    if published_forecast and published_forecast.get("published_odds_value") is not None:
        odds_value = published_forecast.get("published_odds_value")
    elif candidate.get("implied_probability") is not None:
        implied_probability = float(candidate["implied_probability"])
        odds_value = round(1.0 / implied_probability, 2) if implied_probability > 0 else None

    response = {
        "status": "ok",
        "service": "forecast-api",
        "publication_type": publication_type,
        "created": created,
        "published_forecast": published_forecast,
        "publication_payload": {
            "published_forecast_id": published_forecast.get("id") if published_forecast else None,
            "candidate_id": candidate["id"],
            "fixture_id": candidate["fixture_id"],
            "publication_type": publication_type,
            "home_team": fixture.get("home_team"),
            "away_team": fixture.get("away_team"),
            "kickoff_at": fixture.get("kickoff_at"),
            "bookmaker_code": candidate.get("bookmaker_code"),
            "market_code": candidate.get("market_code"),
            "selection_code": candidate.get("selection_code"),
            "odds_value": odds_value,
            "model_probability": candidate.get("model_probability") or dossier.get("model_probability"),
            "implied_probability": candidate.get("implied_probability") or dossier.get("implied_probability"),
            "fair_probability": candidate.get("fair_probability") or dossier.get("fair_probability"),
            "edge": candidate.get("edge") or dossier.get("edge"),
            "ev": candidate.get("ev") or dossier.get("ev"),
            "confidence": candidate.get("confidence") or dossier.get("confidence"),
            "multisource_dossier": dossier,
        },
    }

    if message:
        response["message"] = message

    return response


def _existing_pending_publication(supabase, publication_type):
    resp = (
        supabase.table("published_forecasts")
        .select("id, candidate_id, fixture_id, publication_type, publication_channel, publication_status, published_at, telegram_message_id, message_text, published_odds_value")
        .eq("publication_type", publication_type)
        .eq("publication_status", "pending")
        .order("id", desc=False)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def _create_published_forecast_idempotent(publication_type):
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

        # Global idempotency guard: if a pending publication of this type already exists,
        # return it instead of creating another one from a later selected candidate.
        existing_pending = _existing_pending_publication(supabase, publication_type)
        if existing_pending:
            existing_candidate = _candidate_by_id(supabase, existing_pending["candidate_id"])
            if existing_candidate:
                supabase.table("forecast_candidates").update({"candidate_status": published_status}).eq("id", existing_candidate["id"]).execute()
                return _build_publication_response(
                    supabase,
                    publication_type,
                    existing_candidate,
                    existing_pending,
                    created=False,
                    message="Existing pending published_forecast returned; no duplicate row created",
                )

        selected_resp = (
            supabase.table("forecast_candidates")
            .select(CANDIDATE_SELECT)
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

        candidate = selected_candidates[0]

        existing_pub = (
            supabase.table("published_forecasts")
            .select("id, candidate_id, fixture_id, publication_type, publication_channel, publication_status, published_at, telegram_message_id, message_text, published_odds_value")
            .eq("candidate_id", candidate["id"])
            .eq("publication_type", publication_type)
            .order("id", desc=False)
            .execute()
        )
        existing_rows = existing_pub.data or []
        if existing_rows:
            supabase.table("forecast_candidates").update({"candidate_status": published_status}).eq("id", candidate["id"]).execute()
            return _build_publication_response(
                supabase,
                publication_type,
                candidate,
                existing_rows[0],
                created=False,
                message="Existing published_forecast returned; no duplicate row created",
            )

        existing_fixture_pub = (
            supabase.table("published_forecasts")
            .select("id, candidate_id, fixture_id, publication_type, publication_channel, publication_status, published_at, telegram_message_id, message_text, published_odds_value")
            .eq("fixture_id", candidate["fixture_id"])
            .eq("publication_type", publication_type)
            .order("id", desc=False)
            .execute()
        )
        existing_fixture_rows = existing_fixture_pub.data or []
        if existing_fixture_rows:
            supabase.table("forecast_candidates").update({"candidate_status": published_status}).eq("id", candidate["id"]).execute()
            return _build_publication_response(
                supabase,
                publication_type,
                candidate,
                existing_fixture_rows[0],
                created=False,
                message="Existing fixture published_forecast returned; no duplicate row created",
            )

        implied_probability = float(candidate["implied_probability"])
        odds_value = round(1.0 / implied_probability, 2) if implied_probability > 0 else None

        insert_resp = (
            supabase.table("published_forecasts")
            .insert({
                "candidate_id": candidate["id"],
                "fixture_id": candidate["fixture_id"],
                "publication_type": publication_type,
                "publication_channel": "telegram_channel",
                "publication_status": "pending",
                "published_odds_value": odds_value,
            })
            .execute()
        )
        created_row = (insert_resp.data or [None])[0]

        supabase.table("forecast_candidates").update({"candidate_status": published_status}).eq("id", candidate["id"]).execute()

        return _build_publication_response(
            supabase,
            publication_type,
            candidate,
            created_row,
            created=True,
        )

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "publication_type": publication_type,
            "created": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "message": "idempotent create published forecast failed",
        }


publication_extensions._create_published_forecast = _create_published_forecast_idempotent
