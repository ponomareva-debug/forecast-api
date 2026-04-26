from fixed_main import app
import runtime_extensions  # noqa: F401 - registers extra routes on app
import soccerdata_extensions  # noqa: F401 - registers soccerdata routes on app
import soccerdata_alias_patch  # noqa: F401 - patches soccerdata team aliases
import understat_extensions  # noqa: F401 - registers soccerdata Understat routes on app
import fbref_extensions  # noqa: F401 - registers soccerdata FBref routes on app
import espn_extensions  # noqa: F401 - registers soccerdata ESPN routes on app
import espn_json_patch  # noqa: F401 - patches ESPN feature JSON serialization
import selection_extensions  # noqa: F401 - overrides selection routes with multisource scoring
import publication_extensions  # noqa: F401 - overrides publication routes for free/premium
import publication_idempotency_patch  # noqa: F401 - makes publication creation idempotent
import multi_league_extensions  # noqa: F401 - registers multi-league import/run/enrichment routes
