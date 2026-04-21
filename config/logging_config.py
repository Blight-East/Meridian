import logging
import os

_DEFAULT_LOG_DIR = "/tmp/agent-flux/logs"


def _resolve_log_dir() -> str:
    preferred = os.environ.get("AGENT_FLUX_LOG_DIR", _DEFAULT_LOG_DIR)
    for candidate in (preferred, _DEFAULT_LOG_DIR):
        try:
            os.makedirs(candidate, exist_ok=True)
            return candidate
        except OSError:
            continue
    return _DEFAULT_LOG_DIR


LOG_DIR = _resolve_log_dir()

def get_logger(name):
    logger = logging.getLogger(f"agent_flux.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(name)s] [%(levelname)s] %(message)s")
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
        fh = logging.FileHandler(os.path.join(LOG_DIR, f"{name}.log"))
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger
