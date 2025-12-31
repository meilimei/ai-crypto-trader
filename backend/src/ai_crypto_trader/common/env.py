from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def init_env() -> None:
    """
    Load environment variables from the backend project .env file.

    - Find project root by walking up from this file until pyproject.toml is found.
    - Load {root}/.env via python-dotenv.
    - Idempotent and safe to call multiple times.
    """
    here = Path(__file__).resolve()
    root = here
    while root != root.parent and not (root / "pyproject.toml").exists():
        root = root.parent

    env_path = root / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
        logger.info("Loaded environment from %s", env_path)
    else:
        logger.info(".env file not found at %s; using existing process env only", env_path)
