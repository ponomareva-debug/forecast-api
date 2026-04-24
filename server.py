import inspect
from typing import Any

import main
from fastapi.routing import APIRoute
from starlette.routing import request_response

app = main.app


def _get_latest_model_run_debug() -> dict[str, Any]:
    try:
        supabase = main.get_supabase()
        resp = (
            supabase.table("forecast_model_runs")
            .select("id, training_start_date, training_end_date")
            .eq("model_name", "DixonColesGoalModel")
            .order("id", desc=True)
            .limit(1)
            .execute()
        )

        row = (resp.data or [None])[0]
        if not row:
            return {
                "debug_model_run_id": None,
                "debug_training_start_date": None,
                "debug_training_end_date": None,
            }

        return {
            "debug_model_run_id": row.get("id"),
            "debug_training_start_date": row.get("training_start_date"),
            "debug_training_end_date": row.get("training_end_date"),
        }
    except Exception as exc:
        return {
            "debug_model_run_id": None,
            "debug_training_start_date": None,
            "debug_training_end_date": None,
            "debug_model_run_error": str(exc),
        }


def _patch_forecast_run_v2_response() -> None:
    for route in app.router.routes:
        if not isinstance(route, APIRoute):
            continue

        if route.path != "/forecast/run-v2" or "POST" not in route.methods:
            continue

        original_call = route.dependant.call

        async def patched_forecast_run_v2(*args: Any, **kwargs: Any) -> Any:
            result = original_call(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result

            if isinstance(result, dict) and result.get("status") == "ok":
                result.update(_get_latest_model_run_debug())

            return result

        route.endpoint = patched_forecast_run_v2
        route.dependant.call = patched_forecast_run_v2
        route.app = request_response(route.get_route_handler())
        return


_patch_forecast_run_v2_response()
