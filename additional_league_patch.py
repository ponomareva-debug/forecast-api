"""Patch multi_league_extensions with additional enabled leagues.

This keeps the main multi-league implementation stable while expanding the active
league config for odds ingestion, historical import, run-v3, and enrichment v2
endpoints.
"""

import multi_league_extensions as mle


ADDITIONAL_LEAGUES = {
    "EREDIVISIE": {
        "enabled": True,
        "xgabora_division": "N1",
        "odds_sport_key": "soccer_netherlands_eredivisie",
        "soccerdata_names": ["NED-Eredivisie", "Eredivisie"],
    },
    "PORTUGAL_PRIMEIRA": {
        "enabled": True,
        "xgabora_division": "P1",
        "odds_sport_key": "soccer_portugal_primeira_liga",
        "soccerdata_names": ["POR-Primeira Liga", "Primeira Liga"],
    },
    "CHAMPIONSHIP": {
        "enabled": True,
        "xgabora_division": "E1",
        "odds_sport_key": "soccer_efl_champ",
        "soccerdata_names": ["ENG-Championship", "Championship"],
    },
}


for league_code, cfg in ADDITIONAL_LEAGUES.items():
    mle.LEAGUES[league_code] = cfg

mle.DEFAULT_LEAGUES = [
    league_code
    for league_code, cfg in mle.LEAGUES.items()
    if cfg.get("enabled")
]
