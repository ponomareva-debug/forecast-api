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


def _collect_blocked_fixture_ids(supabase):
    blocked = set()

    published_resp = (
        supabase.table("published_forecasts")
        .select("fixture_id")
        .execute()
    )
    for row in published_resp.data or []:
        if row.get("fixture_id") is not None:
            blocked.add(row["fixture_id"])

    reserved_resp = (
        supabase.table("forecast_candidates")
        .select("fixture_id, candidate_status")
        .in_("candidate_status", [
            "selected_free",
            "selected_premium",
            "published_free",
            "published_premium",
        ])
        .execute()
    )
    for row in reserved_resp.data or []:
        if row.get("fixture_id") is not None:
            blocked.add(row["fixture_id"])

    return blocked


def _select_multisource_candidate(selection_type):
    supabase = get_supabase()

    if selection_type not in {"free", "premium"}:
        return {
            "status": "error",
            "service": "forecast-api",
            "selected": False,
            "message": "selection_type must be free or premium",
        }

    eligible_column = "free_eligible_v1" if selection_type == "free" else "premium_eligible_v1"
    target_status = "selected_free" if selection_type == "free" else "selected_premium"

    blocked_fixture_ids = _collect_blocked_fixture_ids(supabase)

    candidates_resp = (
        supabase.table("v_candidate_publishable_v1")
        .select(
            "candidate_id, fixture_id, home_team, away_team, kickoff_at, "
            "bookmaker_code, market_code, selection_code, odds_value, "
            "model_probability, implied_probability, fair_probability, edge, ev, confidence, "
            "source_support_count, source_contradiction_count, multisource_alignment_label, "
            "multisource_score_v1, free_eligible_v1, premium_eligible_v1, publish_filter_reason, candidate_status"
        )
        .eq(eligible_column, True)
        .eq("candidate_status", "generated")
        .order("multisource_score_v1", desc=True)
        .order("ev", desc=True)
        .order("confidence", desc=True)
        .execute()
    )

    candidates = candidates_resp.data or []
    if not candidates:
        return {
            "status": "ok",
            "service": "forecast-api",
            "selection_type": selection_type,
            "selected": False,
            "message": f"No {selection_type} eligible generated candidates found in v_candidate_publishable_v1",
            "blocked_fixture_ids_count": len(blocked_fixture_ids),
        }

    selected_candidate = None
    skipped_blocked = []

    for candidate in candidates:
        if candidate.get("fixture_id") in blocked_fixture_ids:
            skipped_blocked.append({
                "candidate_id": candidate.get("candidate_id"),
                "fixture_id": candidate.get("fixture_id"),
                "home_team": candidate.get("home_team"),
                "away_team": candidate.get("away_team"),
                "selection_code": candidate.get("selection_code"),
                "multisource_score_v1": candidate.get("multisource_score_v1"),
            })
            continue
        selected_candidate = candidate
        break

    if selected_candidate is None:
        return {
            "status": "ok",
            "service": "forecast-api",
            "selection_type": selection_type,
            "selected": False,
            "message": f"No unblocked {selection_type} eligible candidate found",
            "eligible_candidates_count": len(candidates),
            "blocked_fixture_ids_count": len(blocked_fixture_ids),
            "skipped_blocked_sample": skipped_blocked[:10],
        }

    update_resp = (
        supabase.table("forecast_candidates")
        .update({"candidate_status": target_status})
        .eq("id", selected_candidate["candidate_id"])
        .eq("candidate_status", "generated")
        .execute()
    )
    updated = (update_resp.data or [None])[0]

    if updated is None:
        return {
            "status": "error",
            "service": "forecast-api",
            "selection_type": selection_type,
            "selected": False,
            "message": "Candidate was not updated. It may have been selected by another run.",
            "candidate": selected_candidate,
        }

    return {
        "status": "ok",
        "service": "forecast-api",
        "selection_engine": "multisource_score_v1",
        "selection_type": selection_type,
        "selected": True,
        "candidate": updated,
        "selection_metadata": selected_candidate,
        "eligible_candidates_count": len(candidates),
        "blocked_fixture_ids_count": len(blocked_fixture_ids),
        "skipped_blocked_sample": skipped_blocked[:10],
    }


_remove_route("/forecast/select-free", "POST")
_remove_route("/forecast/select-premium", "POST")


@app.post("/forecast/select-free")
def forecast_select_free_multisource():
    return _select_multisource_candidate("free")


@app.post("/forecast/select-premium")
def forecast_select_premium_multisource():
    return _select_multisource_candidate("premium")
