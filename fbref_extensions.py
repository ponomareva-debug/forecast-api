import math
import re

from fixed_main import app


def _json_safe_value(value):
    if value is None:
        return None
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return float(value)
    return value if isinstance(value, str) else str(value)


def _flatten_columns(df):
    if df is None:
        return []

    columns = []
    for col in list(df.columns):
        if isinstance(col, tuple):
            parts = [str(part) for part in col if str(part) != ""]
            columns.append("__".join(parts))
        else:
            columns.append(str(col))
    return columns


def _sample_rows(df, limit=3):
    if df is None or len(df) == 0:
        return []

    clean_df = df.reset_index().copy()
    clean_df.columns = _flatten_columns(clean_df)
    raw_rows = clean_df.head(limit).to_dict(orient="records")

    return [
        {str(key): _json_safe_value(value) for key, value in row.items()}
        for row in raw_rows
    ]


def _make_fbref_reader():
    import soccerdata as sd

    attempts = []
    league_attempts = ["ENG-Premier League", "Premier League"]
    season_attempts = [2025, "2025", "2025-2026", [2025], ["2025"], 2024, "2024", [2024]]

    for league in league_attempts:
        for season in season_attempts:
            kwargs = {"leagues": league, "seasons": season}
            try:
                reader = sd.FBref(**kwargs)
                attempts.append({"kwargs": kwargs, "status": "created"})
                return reader, attempts
            except Exception as e:
                attempts.append({"kwargs": kwargs, "status": "error", "error": str(e)})

    try:
        reader = sd.FBref()
        attempts.append({"kwargs": {}, "status": "created"})
        return reader, attempts
    except Exception as e:
        attempts.append({"kwargs": {}, "status": "error", "error": str(e)})

    raise RuntimeError({"message": "Could not create FBref reader", "attempts": attempts})


def _try_fbref_team_stats(reader):
    attempts = []

    if not hasattr(reader, "read_team_season_stats"):
        return None, attempts + [{"method": "read_team_season_stats", "status": "missing"}]

    stat_type_attempts = [
        "standard",
        "shooting",
        "passing",
        "passing_types",
        "goal_shot_creation",
        "defense",
        "possession",
        "playing_time",
        "misc",
    ]

    for stat_type in stat_type_attempts:
        try:
            df = reader.read_team_season_stats(stat_type=stat_type)
            attempts.append({"method": "read_team_season_stats", "stat_type": stat_type, "status": "ok"})
            return {"stat_type": stat_type, "df": df}, attempts
        except TypeError as e:
            attempts.append({"method": "read_team_season_stats", "stat_type": stat_type, "status": "type_error", "error": str(e)})
            try:
                df = reader.read_team_season_stats(stat_type)
                attempts.append({"method": "read_team_season_stats_positional", "stat_type": stat_type, "status": "ok"})
                return {"stat_type": stat_type, "df": df}, attempts
            except Exception as ee:
                attempts.append({"method": "read_team_season_stats_positional", "stat_type": stat_type, "status": "error", "error": str(ee)})
        except Exception as e:
            attempts.append({"method": "read_team_season_stats", "stat_type": stat_type, "status": "error", "error": str(e)})

    return None, attempts


@app.get("/debug/soccerdata-fbref")
def debug_soccerdata_fbref():
    try:
        reader, reader_attempts = _make_fbref_reader()
        reader_methods = [name for name in dir(reader) if name.startswith("read_")]

        team_stats_result, stats_attempts = _try_fbref_team_stats(reader)

        if team_stats_result is None:
            return {
                "status": "error",
                "service": "forecast-api",
                "source": "soccerdata_fbref",
                "message": "Could not read FBref team season stats",
                "reader_attempts": reader_attempts,
                "reader_methods": reader_methods,
                "stats_attempts": stats_attempts,
            }

        df = team_stats_result["df"]
        columns = _flatten_columns(df)

        useful_column_patterns = [
            "team",
            "squad",
            "players",
            "age",
            "possession",
            "goals",
            "assists",
            "xg",
            "npxg",
            "shots",
            "passes",
            "tackles",
            "pressures",
        ]

        useful_columns = [
            col for col in columns
            if any(pattern in str(col).lower() for pattern in useful_column_patterns)
        ]

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "soccerdata_fbref",
            "reader_attempts": reader_attempts,
            "reader_methods": reader_methods,
            "selected_stat_type": team_stats_result["stat_type"],
            "stats_attempts": stats_attempts,
            "raw_rows": int(len(df)) if df is not None else 0,
            "raw_columns_count": len(columns),
            "raw_columns": columns,
            "useful_columns_sample": useful_columns[:80],
            "sample_rows": _sample_rows(df, limit=3),
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "source": "soccerdata_fbref",
            "error": str(e),
            "error_type": type(e).__name__,
        }
