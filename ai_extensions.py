import json
import os
from typing import Any, Dict, Optional

from fastapi import Body
from pydantic import BaseModel

from fixed_main import app, get_supabase


class AIForecastReportRequest(BaseModel):
    published_forecast_id: Optional[int] = None
    publication_type: str = "premium"
    model: str = "gpt-4.1-mini"
    save: bool = True


def _safe_json_loads(value, default=None):
    if default is None:
        default = {}
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _get_openai_client():
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENROUTER_BASE_URL")

    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    if base_url:
        return OpenAI(api_key=api_key, base_url=base_url)
    return OpenAI(api_key=api_key)


def _json_chat(model: str, system: str, user_payload: Dict[str, Any], temperature: float = 0.2) -> Dict[str, Any]:
    client = _get_openai_client()
    response = client.chat.completions.create(
        model=model,
        temperature=temperature,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
    )
    text = response.choices[0].message.content or "{}"
    return _safe_json_loads(text, default={"raw": text})


def _text_chat(model: str, system: str, user_payload: Dict[str, Any], temperature: float = 0.5) -> str:
    client = _get_openai_client()
    response = client.chat.completions.create(
        model=model,
        temperature=temperature,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
    )
    return response.choices[0].message.content or ""


def _latest_pending_publication(supabase, publication_type: str):
    resp = (
        supabase.table("published_forecasts")
        .select("id, candidate_id, fixture_id, publication_type, publication_status, published_odds_value, published_at")
        .eq("publication_type", publication_type)
        .eq("publication_status", "pending")
        .order("id", desc=False)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def _publication_by_id(supabase, published_forecast_id: int):
    resp = (
        supabase.table("published_forecasts")
        .select("id, candidate_id, fixture_id, publication_type, publication_status, published_odds_value, published_at")
        .eq("id", published_forecast_id)
        .single()
        .execute()
    )
    return resp.data or None


def _load_candidate_dossier(supabase, candidate_id: int):
    resp = (
        supabase.table("v_candidate_multisource_score_v1")
        .select("*")
        .eq("candidate_id", candidate_id)
        .single()
        .execute()
    )
    return resp.data or None


def _load_match_features(supabase, fixture_id: int):
    resp = (
        supabase.table("match_features")
        .select("source, feature_key, feature_value")
        .eq("fixture_id", fixture_id)
        .execute()
    )
    rows = resp.data or []
    return rows


def _compact_research_context(dossier: Dict[str, Any], features: list[Dict[str, Any]]) -> Dict[str, Any]:
    feature_map = {f"{row.get('source')}:{row.get('feature_key')}": row.get("feature_value") for row in features}

    return {
        "match": {
            "fixture_id": dossier.get("fixture_id"),
            "home_team": dossier.get("home_team"),
            "away_team": dossier.get("away_team"),
            "kickoff_at": dossier.get("kickoff_at"),
            "selection_code": dossier.get("selection_code"),
            "odds_value": dossier.get("odds_value"),
        },
        "market_model": {
            "model_probability": dossier.get("model_probability"),
            "implied_probability": dossier.get("implied_probability"),
            "fair_probability": dossier.get("fair_probability"),
            "edge": dossier.get("edge"),
            "ev": dossier.get("ev"),
            "confidence": dossier.get("confidence"),
            "multisource_score_v1": dossier.get("multisource_score_v1"),
            "alignment": dossier.get("multisource_alignment_label"),
            "support_count": dossier.get("source_support_count"),
            "contradiction_count": dossier.get("source_contradiction_count"),
        },
        "signals": {
            "penaltyblog": {
                "home_probability": dossier.get("pb_home_probability"),
                "draw_probability": dossier.get("pb_draw_probability"),
                "away_probability": dossier.get("pb_away_probability"),
                "home_goal_expectation": dossier.get("pb_home_goal_expectation"),
                "away_goal_expectation": dossier.get("pb_away_goal_expectation"),
            },
            "xgabora": {
                "elo_delta": dossier.get("xg_elo_delta"),
                "form3_delta": dossier.get("xg_form3_delta"),
                "form5_delta": dossier.get("xg_form5_delta"),
            },
            "clubelo": {
                "elo_delta": dossier.get("clubelo_elo_delta"),
                "home_elo": dossier.get("clubelo_home_elo"),
                "away_elo": dossier.get("clubelo_away_elo"),
            },
            "understat": {
                "xg_diff_delta": dossier.get("understat_xg_diff_delta"),
                "xg_for_delta": dossier.get("understat_xg_for_delta"),
                "xg_against_delta": dossier.get("understat_xg_against_delta"),
                "home_avg_xg_for": dossier.get("understat_home_avg_xg_for"),
                "away_avg_xg_for": dossier.get("understat_away_avg_xg_for"),
            },
            "espn": {
                "shots_delta": dossier.get("espn_shots_delta"),
                "sot_delta": dossier.get("espn_sot_delta"),
                "goals_delta": dossier.get("espn_goals_delta"),
                "goals_conceded_delta": dossier.get("espn_goals_conceded_delta"),
            },
        },
        "raw_feature_payloads": feature_map,
        "external_research_note": "No live web-search API is connected yet. Injuries, weather and expert opinions are not fetched automatically in this version.",
    }


def _run_agents(model: str, research_context: Dict[str, Any]) -> Dict[str, Any]:
    base_rule = (
        "You are part of a sports betting analysis pipeline. "
        "Use only the provided structured data. Do not invent injuries, weather, news, lineups, odds movement, or expert opinions. "
        "If a data category is missing, state that it is missing. Return valid JSON only."
    )

    stats_agent = _json_chat(
        model,
        base_rule + " Focus on model probabilities, EV, edge, xG, Elo, form and ESPN lineup-derived signals.",
        research_context,
        temperature=0.15,
    )

    risk_agent = _json_chat(
        model,
        base_rule + " Focus on risks, contradictions, weak spots, and whether the pick should be downgraded.",
        research_context,
        temperature=0.15,
    )

    market_agent = _json_chat(
        model,
        base_rule + " Focus on betting-market interpretation: odds value, implied probability, fair probability, EV and whether the price is acceptable.",
        research_context,
        temperature=0.15,
    )

    return {
        "stats_agent": stats_agent,
        "risk_agent": risk_agent,
        "market_agent": market_agent,
    }


def _final_verdict(model: str, research_context: Dict[str, Any], agent_outputs: Dict[str, Any]) -> Dict[str, Any]:
    system = (
        "You are the final betting analyst. Combine structured match data and sub-agent outputs into one verdict. "
        "Use only provided data. Do not invent facts. Return valid JSON only with keys: verdict, confidence_label, confidence_score_0_100, recommended_pick, fair_odds_comment, key_reasons, risk_factors, missing_information, final_note."
    )
    return _json_chat(
        model,
        system,
        {"research_context": research_context, "agent_outputs": agent_outputs},
        temperature=0.2,
    )


def _telegram_text(model: str, research_context: Dict[str, Any], final_verdict: Dict[str, Any], publication_type: str) -> str:
    system = (
        "You write concise Telegram betting posts in Russian. "
        "Style: confident, analytical, readable, no fake guarantees, no invented news. "
        "Include match, pick, odds, short reasoning, risk note, and responsible wording. "
        "For premium, make it more analytical and valuable. For free, make it shorter."
    )
    return _text_chat(
        model,
        system,
        {
            "publication_type": publication_type,
            "research_context": research_context,
            "final_verdict": final_verdict,
        },
        temperature=0.45,
    )


@app.post("/ai/generate-forecast-report")
def generate_ai_forecast_report(payload: AIForecastReportRequest = Body(default=AIForecastReportRequest())):
    try:
        supabase = get_supabase()

        publication = None
        if payload.published_forecast_id is not None:
            publication = _publication_by_id(supabase, payload.published_forecast_id)
        else:
            publication = _latest_pending_publication(supabase, payload.publication_type)

        if not publication:
            return {
                "status": "ok",
                "service": "forecast-api",
                "created": False,
                "message": "No publication found for AI report generation",
                "publication_type": payload.publication_type,
            }

        dossier = _load_candidate_dossier(supabase, publication["candidate_id"])
        if not dossier:
            return {
                "status": "error",
                "service": "forecast-api",
                "created": False,
                "message": "Candidate dossier not found in v_candidate_multisource_score_v1",
                "published_forecast": publication,
            }

        features = _load_match_features(supabase, publication["fixture_id"])
        research_context = _compact_research_context(dossier, features)
        agent_outputs = _run_agents(payload.model, research_context)
        final = _final_verdict(payload.model, research_context, agent_outputs)
        tg_text = _telegram_text(payload.model, research_context, final, publication["publication_type"])

        report_row = None
        if payload.save:
            insert_resp = (
                supabase.table("ai_forecast_reports")
                .insert({
                    "published_forecast_id": publication["id"],
                    "candidate_id": publication["candidate_id"],
                    "fixture_id": publication["fixture_id"],
                    "publication_type": publication["publication_type"],
                    "status": "created",
                    "ai_model": payload.model,
                    "dossier": dossier,
                    "research_context": research_context,
                    "agent_outputs": agent_outputs,
                    "final_verdict": final,
                    "telegram_text": tg_text,
                })
                .execute()
            )
            report_row = (insert_resp.data or [None])[0]

        return {
            "status": "ok",
            "service": "forecast-api",
            "created": True,
            "published_forecast": publication,
            "ai_report": report_row,
            "final_verdict": final,
            "telegram_text": tg_text,
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "created": False,
            "error": str(e),
            "error_type": type(e).__name__,
        }
