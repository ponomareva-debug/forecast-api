import soccerdata_extensions


_original_team_candidates = soccerdata_extensions._team_candidates


def _patched_team_candidates(raw_name, aliases):
    candidates = _original_team_candidates(raw_name, aliases)
    normalized = str(raw_name or "").strip().lower()

    if normalized in {
        "nottingham forest",
        "nottm forest",
        "nott'm forest",
    }:
        for extra in ["Forest", "Nott Forest", "Nottingham"]:
            if extra not in candidates:
                candidates.append(extra)

    return candidates


soccerdata_extensions._team_candidates = _patched_team_candidates
