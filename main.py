from fastapi import FastAPI

app = FastAPI(title="forecast-api", version="0.1.0")

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "forecast-api"
    }
