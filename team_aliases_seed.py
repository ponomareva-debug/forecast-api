from collections import defaultdict
from typing import Optional

from fastapi import Body
from pydantic import BaseModel

from fixed_main import app, get_supabase


DEFAULT_TEAM_ALIASES = [
    # EPL / common English names
    {"league_code": "EPL", "source_name": "Wolverhampton Wanderers", "canonical_name": "Wolves"},
    {"league_code": "EPL", "source_name": "Nottingham Forest", "canonical_name": "Nottm Forest"},
    {"league_code": "EPL", "source_name": "Manchester United", "canonical_name": "Man United"},
    {"league_code": "EPL", "source_name": "Manchester City", "canonical_name": "Man City"},
    {"league_code": "EPL", "source_name": "Newcastle United", "canonical_name": "Newcastle"},
    {"league_code": "EPL", "source_name": "West Ham United", "canonical_name": "West Ham"},
    {"league_code": "EPL", "source_name": "Tottenham Hotspur", "canonical_name": "Tottenham"},
    {"league_code": "EPL", "source_name": "AFC Bournemouth", "canonical_name": "Bournemouth"},
    {"league_code": "EPL", "source_name": "Leeds United", "canonical_name": "Leeds"},
    {"league_code": "EPL", "source_name": "Brighton & Hove Albion", "canonical_name": "Brighton"},
    {"league_code": "EPL", "source_name": "Brighton and Hove Albion", "canonical_name": "Brighton"},

    # La Liga
    {"league_code": "LA_LIGA", "source_name": "Alavés", "canonical_name": "Alaves"},
    {"league_code": "LA_LIGA", "source_name": "Athletic Bilbao", "canonical_name": "Ath Bilbao"},
    {"league_code": "LA_LIGA", "source_name": "Atlético Madrid", "canonical_name": "Ath Madrid"},
    {"league_code": "LA_LIGA", "source_name": "Atletico Madrid", "canonical_name": "Ath Madrid"},
    {"league_code": "LA_LIGA", "source_name": "CA Osasuna", "canonical_name": "Osasuna"},
    {"league_code": "LA_LIGA", "source_name": "Real Betis", "canonical_name": "Betis"},
    {"league_code": "LA_LIGA", "source_name": "Espanyol", "canonical_name": "Espanol"},
    {"league_code": "LA_LIGA", "source_name": "Rayo Vallecano", "canonical_name": "Vallecano"},
    {"league_code": "LA_LIGA", "source_name": "Real Sociedad", "canonical_name": "Sociedad"},
    {"league_code": "LA_LIGA", "source_name": "Elche CF", "canonical_name": "Elche"},
    {"league_code": "LA_LIGA", "source_name": "Celta Vigo", "canonical_name": "Celta"},
    {"league_code": "LA_LIGA", "source_name": "Las Palmas", "canonical_name": "Las Palmas"},

    # Serie A
    {"league_code": "SERIE_A", "source_name": "Atalanta BC", "canonical_name": "Atalanta"},
    {"league_code": "SERIE_A", "source_name": "AC Milan", "canonical_name": "Milan"},
    {"league_code": "SERIE_A", "source_name": "Inter Milan", "canonical_name": "Inter"},
    {"league_code": "SERIE_A", "source_name": "AS Roma", "canonical_name": "Roma"},
    {"league_code": "SERIE_A", "source_name": "Hellas Verona", "canonical_name": "Verona"},
    {"league_code": "SERIE_A", "source_name": "S.S. Lazio", "canonical_name": "Lazio"},
    {"league_code": "SERIE_A", "source_name": "US Cremonese", "canonical_name": "Cremonese"},
    {"league_code": "SERIE_A", "source_name": "SSC Napoli", "canonical_name": "Napoli"},

    # Bundesliga
    {"league_code": "BUNDESLIGA", "source_name": "1. FC Heidenheim", "canonical_name": "Heidenheim"},
    {"league_code": "BUNDESLIGA", "source_name": "1. FC Köln", "canonical_name": "FC Koln"},
    {"league_code": "BUNDESLIGA", "source_name": "FC Koln", "canonical_name": "FC Koln"},
    {"league_code": "BUNDESLIGA", "source_name": "Eintracht Frankfurt", "canonical_name": "Ein Frankfurt"},
    {"league_code": "BUNDESLIGA", "source_name": "TSG Hoffenheim", "canonical_name": "Hoffenheim"},
    {"league_code": "BUNDESLIGA", "source_name": "VfB Stuttgart", "canonical_name": "Stuttgart"},
    {"league_code": "BUNDESLIGA", "source_name": "Bayer Leverkusen", "canonical_name": "Leverkusen"},
    {"league_code": "BUNDESLIGA", "source_name": "FC St. Pauli", "canonical_name": "St Pauli"},
    {"league_code": "BUNDESLIGA", "source_name": "FSV Mainz 05", "canonical_name": "Mainz"},
    {"league_code": "BUNDESLIGA", "source_name": "Borussia Monchengladbach", "canonical_name": "M'gladbach"},
    {"league_code": "BUNDESLIGA", "source_name": "Borussia Mönchengladbach", "canonical_name": "M'gladbach"},
    {"league_code": "BUNDESLIGA", "source_name": "Borussia Dortmund", "canonical_name": "Dortmund"},
    {"league_code": "BUNDESLIGA", "source_name": "SC Freiburg", "canonical_name": "Freiburg"},
    {"league_code": "BUNDESLIGA", "source_name": "VfL Wolfsburg", "canonical_name": "Wolfsburg"},
    {"league_code": "BUNDESLIGA", "source_name": "Werder Bremen", "canonical_name": "Werder Bremen"},
    {"league_code": "BUNDESLIGA", "source_name": "VfL Bochum", "canonical_name": "Bochum"},

    # Ligue 1
    {"league_code": "LIGUE_1", "source_name": "Paris Saint Germain", "canonical_name": "Paris SG"},
    {"league_code": "LIGUE_1", "source_name": "Paris Saint-Germain", "canonical_name": "Paris SG"},
    {"league_code": "LIGUE_1", "source_name": "PSG", "canonical_name": "Paris SG"},
    {"league_code": "LIGUE_1", "source_name": "AS Monaco", "canonical_name": "Monaco"},
    {"league_code": "LIGUE_1", "source_name": "RC Lens", "canonical_name": "Lens"},
    {"league_code": "LIGUE_1", "source_name": "Olympique Lyonnais", "canonical_name": "Lyon"},
    {"league_code": "LIGUE_1", "source_name": "Olympique Marseille", "canonical_name": "Marseille"},
    {"league_code": "LIGUE_1", "source_name": "Le Havre AC", "canonical_name": "Le Havre"},
    {"league_code": "LIGUE_1", "source_name": "Saint Etienne", "canonical_name": "St Etienne"},
    {"league_code": "LIGUE_1", "source_name": "Stade Rennes", "canonical_name": "Rennes"},

    # Eredivisie
    {"league_code": "EREDIVISIE", "source_name": "FC Utrecht", "canonical_name": "Utrecht"},
    {"league_code": "EREDIVISIE", "source_name": "NEC Nijmegen", "canonical_name": "Nijmegen"},
    {"league_code": "EREDIVISIE", "source_name": "FC Volendam", "canonical_name": "Volendam"},
    {"league_code": "EREDIVISIE", "source_name": "FC Zwolle", "canonical_name": "Zwolle"},
    {"league_code": "EREDIVISIE", "source_name": "Heracles Almelo", "canonical_name": "Heracles"},
    {"league_code": "EREDIVISIE", "source_name": "Fortuna Sittard", "canonical_name": "For Sittard"},
    {"league_code": "EREDIVISIE", "source_name": "FC Twente Enschede", "canonical_name": "Twente"},
    {"league_code": "EREDIVISIE", "source_name": "PSV", "canonical_name": "PSV Eindhoven"},
    {"league_code": "EREDIVISIE", "source_name": "Go Ahead Eagles", "canonical_name": "Go Ahead Eagles"},
    {"league_code": "EREDIVISIE", "source_name": "RKC Waalwijk", "canonical_name": "Waalwijk"},

    # Portugal Primeira
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Sporting Lisbon", "canonical_name": "Sp Lisbon"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Sporting CP", "canonical_name": "Sp Lisbon"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Moreirense FC", "canonical_name": "Moreirense"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "CF Estrela", "canonical_name": "Estrela"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Estrela Amadora", "canonical_name": "Estrela"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Famalicão", "canonical_name": "Famalicao"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "FC Famalicao", "canonical_name": "Famalicao"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "FC Porto", "canonical_name": "Porto"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Braga", "canonical_name": "Sp Braga"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "SC Braga", "canonical_name": "Sp Braga"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Rio Ave FC", "canonical_name": "Rio Ave"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Vitória SC", "canonical_name": "Guimaraes"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Vitoria SC", "canonical_name": "Guimaraes"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Vitória Guimarães", "canonical_name": "Guimaraes"},
    {"league_code": "PORTUGAL_PRIMEIRA", "source_name": "Vitoria Guimaraes", "canonical_name": "Guimaraes"},

    # Championship
    {"league_code": "CHAMPIONSHIP", "source_name": "Birmingham City", "canonical_name": "Birmingham"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Blackburn Rovers", "canonical_name": "Blackburn"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Leicester City", "canonical_name": "Leicester"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Stoke City", "canonical_name": "Stoke"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Swansea City", "canonical_name": "Swansea"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Charlton Athletic", "canonical_name": "Charlton"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Coventry City", "canonical_name": "Coventry"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Derby County", "canonical_name": "Derby"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Hull City", "canonical_name": "Hull"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Norwich City", "canonical_name": "Norwich"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Ipswich Town", "canonical_name": "Ipswich"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Queens Park Rangers", "canonical_name": "QPR"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Oxford United", "canonical_name": "Oxford"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Preston North End", "canonical_name": "Preston"},
    {"league_code": "CHAMPIONSHIP", "source_name": "West Bromwich Albion", "canonical_name": "West Brom"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Sheffield Wednesday", "canonical_name": "Sheffield Weds"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Cardiff City", "canonical_name": "Cardiff"},
    {"league_code": "CHAMPIONSHIP", "source_name": "Bristol City", "canonical_name": "Bristol City"},
]


class UpsertDefaultAliasesRequest(BaseModel):
    leagues: Optional[list[str]] = None


def _selected_aliases(leagues: Optional[list[str]] = None):
    if not leagues:
        return DEFAULT_TEAM_ALIASES
    selected = {str(league).strip().upper() for league in leagues}
    return [row for row in DEFAULT_TEAM_ALIASES if row["league_code"] in selected]


@app.post("/admin/upsert-default-team-aliases")
def upsert_default_team_aliases(payload: UpsertDefaultAliasesRequest = Body(default=UpsertDefaultAliasesRequest())):
    try:
        supabase = get_supabase()
        rows = _selected_aliases(payload.leagues)
        if not rows:
            return {"status": "ok", "service": "forecast-api", "upserted_count": 0, "message": "No aliases selected"}

        grouped = defaultdict(list)
        for row in rows:
            grouped[row["league_code"]].append(row)

        results = {}
        total_inserted = 0
        for league_code, league_rows in grouped.items():
            source_names = [row["source_name"] for row in league_rows]
            supabase.table("team_aliases").delete().eq("league_code", league_code).in_("source_name", source_names).execute()
            insert_resp = supabase.table("team_aliases").insert(league_rows).execute()
            inserted = len(insert_resp.data or league_rows)
            total_inserted += inserted
            results[league_code] = {
                "aliases_count": len(league_rows),
                "inserted_count": inserted,
                "sample": league_rows[:10],
            }

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "default_team_aliases_seed",
            "selected_leagues": sorted(grouped.keys()),
            "upserted_count": total_inserted,
            "results": results,
        }
    except Exception as e:
        return {"status": "error", "service": "forecast-api", "error": str(e), "error_type": type(e).__name__}
