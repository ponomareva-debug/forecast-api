from fixed_main import app
import runtime_extensions  # noqa: F401 - registers extra routes on app
import soccerdata_extensions  # noqa: F401 - registers soccerdata routes on app
import soccerdata_alias_patch  # noqa: F401 - patches soccerdata team aliases
