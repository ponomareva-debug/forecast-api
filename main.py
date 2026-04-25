from fixed_main import app


@app.get("/debug/xgabora-columns")
def debug_xgabora_columns():
    try:
        import io
        import requests
        import pandas as pd

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

        columns = list(df.columns)
        epl_df = df[df["Division"].astype(str) == "E0"].copy() if "Division" in df.columns else df.head(0).copy()

        sample_columns = [
            col for col in [
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
            if col in columns
        ]

        sample_rows = []
        if not epl_df.empty:
            sample_rows = (
                epl_df[sample_columns]
                .head(3)
                .where(pd.notnull(epl_df[sample_columns].head(3)), None)
                .to_dict(orient="records")
            )

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
            "target_columns_present": [col for col in target_columns if col in columns],
            "target_columns_missing": [col for col in target_columns if col not in columns],
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
