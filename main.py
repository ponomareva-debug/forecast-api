from fixed_main import app, get_supabase


@app.get("/debug/xgabora-columns")
def debug_xgabora_columns():
    try:
        import io
        import math
        import requests
        import pandas as pd

        def clean_value(value):
            if value is None:
                return None
            try:
                if pd.isna(value):
                    return None
            except Exception:
                pass
            if isinstance(value, float):
                if not math.isfinite(value):
                    return None
                return float(value)
            if isinstance(value, int):
                return int(value)
            return str(value) if not isinstance(value, (str, bool)) else value

        csv_url = "https://raw.githubusercontent.com/xgabora/Club-Football-Match-Data-2000-2025/main/data/Matches.csv"

        response = requests.get(csv_url, timeout=120)
        if response.status_code != 200:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Failed to download xgabora CSV",
                "http_status": response.status_code,
                "response_sample": response.text[:300],
            }

        df = pd.read_csv(io.StringIO(response.text))

        columns = [str(col) for col in list(df.columns)]

        if "Division" in df.columns:
            epl_df = df[df["Division"].astype(str) == "E0"].copy()
        else:
            epl_df = df.head(0).copy()

        candidate_sample_columns = [
            "Division",
            "MatchDate",
            "HomeTeam",
            "AwayTeam",
            "FTHome",
            "FTAway",
            "HomeElo",
            "AwayElo",
            "Form3Home",
            "Form3Away",
            "Form5Home",
            "Form5Away",
            "OddHome",
            "OddDraw",
            "OddAway",
            "MaxHome",
            "MaxDraw",
            "MaxAway",
        ]

        sample_columns = [col for col in candidate_sample_columns if col in df.columns]

        sample_rows = []
        if not epl_df.empty and sample_columns:
            raw_rows = epl_df[sample_columns].head(3).to_dict(orient="records")
            sample_rows = [
                {str(key): clean_value(value) for key, value in row.items()}
                for row in raw_rows
            ]

        target_columns = [
            "HomeElo",
            "AwayElo",
            "Form3Home",
            "Form3Away",
            "Form5Home",
            "Form5Away",
            "OddHome",
            "OddDraw",
            "OddAway",
            "MaxHome",
            "MaxDraw",
            "MaxAway",
        ]

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "xgabora/Club-Football-Match-Data-2000-2025",
            "total_rows": int(len(df)),
            "total_columns": int(len(columns)),
            "columns": columns,
            "epl_rows": int(len(epl_df)),
            "target_columns_present": [col for col in target_columns if col in df.columns],
            "target_columns_missing": [col for col in target_columns if col not in df.columns],
            "sample_columns_used": sample_columns,
            "epl_sample_rows": sample_rows,
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "error": str(e),
            "error_type": type(e).__name__,
        }


# Replace old /admin/import-xgabora-epl from fixed_main with extended importer.
app.router.routes = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) == "/admin/import-xgabora-epl"
        and "POST" in getattr(route, "methods", set())
    )
]
app.openapi_schema = None


@app.post("/admin/import-xgabora-epl")
def import_xgabora_epl_extended():
    try:
        import io
        import math
        import requests
        import pandas as pd

        def safe_float(value):
            try:
                if pd.isna(value):
                    return None
                num = float(value)
                if not math.isfinite(num):
                    return None
                return round(num, 6)
            except Exception:
                return None

        supabase = get_supabase()

        csv_url = "https://raw.githubusercontent.com/xgabora/Club-Football-Match-Data-2000-2025/main/data/Matches.csv"
        response = requests.get(csv_url, timeout=120)

        if response.status_code != 200:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Failed to download xgabora CSV",
                "http_status": response.status_code,
                "response_sample": response.text[:300],
            }

        df = pd.read_csv(io.StringIO(response.text))

        required_columns = [
            "Division",
            "MatchDate",
            "HomeTeam",
            "AwayTeam",
            "FTHome",
            "FTAway",
        ]

        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "Missing required columns",
                "missing_columns": missing_required,
                "available_columns": [str(col) for col in df.columns],
            }

        optional_mapping = {
            "HomeElo": "home_elo",
            "AwayElo": "away_elo",
            "Form3Home": "form3_home",
            "Form3Away": "form3_away",
            "Form5Home": "form5_home",
            "Form5Away": "form5_away",
            "OddHome": "odd_home",
            "OddDraw": "odd_draw",
            "OddAway": "odd_away",
            "MaxHome": "max_home",
            "MaxDraw": "max_draw",
            "MaxAway": "max_away",
        }

        optional_present = [source_col for source_col in optional_mapping if source_col in df.columns]
        optional_missing = [source_col for source_col in optional_mapping if source_col not in df.columns]

        division_counts = df["Division"].astype(str).value_counts().head(20).to_dict()

        epl_df = df[df["Division"].astype(str) == "E0"].copy()
        raw_epl_rows = int(len(epl_df))

        if epl_df.empty:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "No EPL rows found in xgabora CSV",
                "raw_downloaded_rows": int(len(df)),
                "division_counts_sample": division_counts,
            }

        epl_df["match_date"] = pd.to_datetime(
            epl_df["MatchDate"],
            errors="coerce",
            dayfirst=False,
        ).dt.date
        epl_df["home_goals"] = pd.to_numeric(epl_df["FTHome"], errors="coerce")
        epl_df["away_goals"] = pd.to_numeric(epl_df["FTAway"], errors="coerce")

        epl_df = epl_df.dropna(
            subset=["match_date", "HomeTeam", "AwayTeam", "home_goals", "away_goals"]
        )

        min_available_date = str(epl_df["match_date"].min())
        max_available_date = str(epl_df["match_date"].max())

        start_date = pd.to_datetime("2020-07-01").date()
        epl_df = epl_df[epl_df["match_date"] >= start_date].copy()

        rows = []

        for _, row in epl_df.iterrows():
            home_goals = safe_float(row["home_goals"])
            away_goals = safe_float(row["away_goals"])

            if home_goals is None or away_goals is None:
                continue

            prepared = {
                "league_code": "EPL",
                "season": None,
                "match_date": row["match_date"].isoformat(),
                "home_team": str(row["HomeTeam"]).strip(),
                "away_team": str(row["AwayTeam"]).strip(),
                "home_goals": int(home_goals),
                "away_goals": int(away_goals),
                "source": "xgabora_matches_csv",
            }

            for source_col, target_col in optional_mapping.items():
                if source_col in row.index:
                    prepared[target_col] = safe_float(row[source_col])

            rows.append(prepared)

        if not rows:
            return {
                "status": "error",
                "service": "forecast-api",
                "message": "No rows prepared for EPL import after cleaning/date filter",
                "raw_epl_rows": raw_epl_rows,
                "min_available_date": min_available_date,
                "max_available_date": max_available_date,
                "import_start_date": start_date.isoformat(),
            }

        (
            supabase.table("historical_matches")
            .delete()
            .eq("league_code", "EPL")
            .execute()
        )

        batch_size = 500
        upserted_total = 0

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            (
                supabase.table("historical_matches")
                .upsert(
                    batch,
                    on_conflict="league_code,match_date,home_team,away_team",
                )
                .execute()
            )
            upserted_total += len(batch)

        teams = sorted(set([r["home_team"] for r in rows] + [r["away_team"] for r in rows]))

        feature_non_null_counts = {
            target_col: sum(1 for item in rows if item.get(target_col) is not None)
            for target_col in optional_mapping.values()
        }

        return {
            "status": "ok",
            "service": "forecast-api",
            "source": "xgabora/Club-Football-Match-Data-2000-2025",
            "raw_downloaded_rows": int(len(df)),
            "raw_epl_rows": raw_epl_rows,
            "min_available_date": min_available_date,
            "max_available_date": max_available_date,
            "import_start_date": start_date.isoformat(),
            "prepared_rows": int(len(rows)),
            "upserted_rows": int(upserted_total),
            "teams_count": int(len(teams)),
            "optional_columns_present": optional_present,
            "optional_columns_missing": optional_missing,
            "feature_non_null_counts": feature_non_null_counts,
            "teams": teams,
            "division_counts_sample": division_counts,
        }

    except Exception as e:
        return {
            "status": "error",
            "service": "forecast-api",
            "error": str(e),
            "error_type": type(e).__name__,
        }
