"""
runtime/safety/fingerprint_check.py

Boot-time release-fingerprint integrity check. Imported by API
(`runtime/main.py`) and worker (`runtime/worker.py`) at startup.

Calls `deploy/fingerprint/runtime_verify.sh` as a subprocess (so the
canonical hashing and signature-verification logic lives in exactly one
place — the shell script — and Python never duplicates it).

Behavior:
  - On clean exit (rc=0): logs INFO and returns.
  - On signature/mismatch (rc in {10, 11, 12, 13}):
      * if FINGERPRINT_KILL_ON_MISMATCH is "1" (the default for prod
        boot): logs CRITICAL and raises SystemExit(rc) so the process
        dies immediately. pm2 records the crash; the inflight alert
        will fire if the worker stops draining.
      * otherwise: logs CRITICAL and returns (verifier metric still
        gets written, so monitoring catches it).
  - On config error (rc=14) or missing tooling: logs CRITICAL but does
    NOT crash boot — refusing to start because the verifier is
    misconfigured would be a self-inflicted outage.

This module deliberately has zero non-stdlib imports so it can be loaded
before any heavy framework code (FastAPI, SQLAlchemy, Redis client) and
crash the process before those libraries get a chance to open sockets,
connections, or transactions.

Public API:
    verify_or_die(role: str) -> None
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger("payflux.fingerprint")

# Resolve repo root: this file is runtime/safety/fingerprint_check.py
# so repo root is two parents up.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_VERIFIER = _REPO_ROOT / "deploy" / "fingerprint" / "runtime_verify.sh"
_DEFAULT_FP_PATH = _REPO_ROOT / "deploy" / "artifacts" / "release_fingerprint.json"
_DEFAULT_CONFIG_VERIFIER = _REPO_ROOT / "deploy" / "fingerprint" / "verify_config_fingerprint.sh"
_DEFAULT_CONFIG_EXPECTED = _REPO_ROOT / "deploy" / "artifacts" / "expected_config_fingerprint.json"
_DEFAULT_RUNTIME_VERIFIER = _REPO_ROOT / "deploy" / "fingerprint" / "verify_runtime_fingerprint.sh"
_DEFAULT_RUNTIME_EXPECTED = _REPO_ROOT / "deploy" / "artifacts" / "expected_runtime_fingerprint.json"
# No public key path — Sigstore keyless verification uses GitHub OIDC +
# Fulcio + Rekor as the trust root. See deploy/fingerprint/TRUST_MODEL.md.

# Verifier exit codes (mirror runtime_verify.sh).
_RC_OK = 0
_RC_SIG_INVALID = 10
_RC_FP_MISMATCH = 11
_RC_FP_MISSING = 12
_RC_RECOMPUTE_FAILED = 13
_RC_CONFIG_ERROR = 14

# Fatal codes — process must die if FINGERPRINT_KILL_ON_MISMATCH=1.
_FATAL_CODES = {_RC_SIG_INVALID, _RC_FP_MISMATCH, _RC_FP_MISSING}

# Config-verifier exit codes (mirror verify_config_fingerprint.sh).
_CFG_RC_OK = 0
_CFG_RC_DRIFT = 20
_CFG_RC_UNAVAILABLE = 21
_CFG_RC_EXPECTED_MISSING = 22
_CFG_RC_RECOMPUTE_FAILED = 23
_CFG_RC_CONFIG_ERROR = 24
_CFG_RC_BASELINE_INCOMPLETE = 25
# Drift, unavailable required source, missing baseline, and incomplete
# baseline are ALL fail-closed at boot. Each one means we cannot vouch
# that the runtime config matches what was approved at deploy time.
_CFG_FATAL_CODES = {
    _CFG_RC_DRIFT,
    _CFG_RC_UNAVAILABLE,
    _CFG_RC_EXPECTED_MISSING,
    _CFG_RC_BASELINE_INCOMPLETE,
}

# Runtime-verifier exit codes (mirror verify_runtime_fingerprint.sh).
_RT_RC_OK = 0
_RT_RC_COMPOSITE_DRIFT = 30
_RT_RC_INTERPRETER_DRIFT = 31
_RT_RC_SITE_PACKAGES_DRIFT = 32
_RT_RC_UNAVAILABLE = 33
_RT_RC_RECOMPUTE_FAILED = 34
_RT_RC_CONFIG_ERROR = 35
_RT_RC_EXPECTED_MISSING = 36
_RT_RC_BASELINE_INCOMPLETE = 37
# Drift in any component, missing baseline, baseline incomplete, and a
# required component being unavailable on the host are all fail-closed
# at boot. Each one means the live Python runtime cannot be vouched for.
_RT_FATAL_CODES = {
    _RT_RC_COMPOSITE_DRIFT,
    _RT_RC_INTERPRETER_DRIFT,
    _RT_RC_SITE_PACKAGES_DRIFT,
    _RT_RC_UNAVAILABLE,
    _RT_RC_EXPECTED_MISSING,
    _RT_RC_BASELINE_INCOMPLETE,
}


def _bool_env(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


# Enforcement modes — single canonical knob for the entire fingerprint
# subsystem. Default is "report" so this module can land in production
# without any host needing pre-existing baselines / env vars; flipping
# to "enforce" is a deliberate operator action AFTER baselines are
# captured and metrics have been clean for the bake period documented
# in deploy/fingerprint/TRUST_MODEL.md.
#
#   enforce  — verifiers run; fatal codes raise SystemExit and crash boot
#   report   — verifiers run; fatal codes are logged + metered but NEVER
#              kill the process. Safe for initial rollout and for hosts
#              that legitimately can't run a verifier (e.g. a dev box).
#   off      — verifiers are skipped entirely. Use for unit tests; do
#              NOT use in any deployed environment.
_MODE_ENFORCE = "enforce"
_MODE_REPORT = "report"
_MODE_OFF = "off"
_VALID_MODES = {_MODE_ENFORCE, _MODE_REPORT, _MODE_OFF}


def _enforcement_mode() -> str:
    """
    Resolve the current enforcement mode, honoring legacy env knobs so
    operators who already set FINGERPRINT_KILL_ON_MISMATCH=1 or
    FINGERPRINT_BOOT_CHECK_DISABLED=1 don't have to re-learn anything.

    Precedence:
      1. FINGERPRINT_ENFORCEMENT_MODE (canonical)
      2. FINGERPRINT_BOOT_CHECK_DISABLED=1  → off
      3. FINGERPRINT_KILL_ON_MISMATCH=1     → enforce
      4. default                            → report (safe rollout)
    """
    raw = os.environ.get("FINGERPRINT_ENFORCEMENT_MODE")
    if raw is not None:
        mode = raw.strip().lower()
        if mode in _VALID_MODES:
            return mode
        logger.critical(
            "FINGERPRINT_ENFORCEMENT_MODE=%r is not one of %s; falling back to 'report'",
            raw, sorted(_VALID_MODES),
        )
        return _MODE_REPORT
    if _bool_env("FINGERPRINT_BOOT_CHECK_DISABLED", default=False):
        return _MODE_OFF
    if _bool_env("FINGERPRINT_KILL_ON_MISMATCH", default=False):
        return _MODE_ENFORCE
    return _MODE_REPORT


def verify_or_die(role: str) -> None:
    """
    Run the source-tree fingerprint verifier once for the given role.

    Behavior is gated by FINGERPRINT_ENFORCEMENT_MODE (resolved by
    `_enforcement_mode()`): in `enforce` mode a fatal verdict raises
    SystemExit; in `report` mode (the default) it logs CRITICAL and
    metrics still update; in `off` mode the check is skipped entirely.

    Args:
        role: short label ("api", "worker", "scheduler"). Becomes the
              `component` label on the emitted Prometheus metric.

    Raises:
        SystemExit(rc): if the verifier reports a fatal integrity
            failure AND enforcement mode is `enforce`.
    """
    mode = _enforcement_mode()
    if mode == _MODE_OFF:
        logger.warning(
            "fingerprint boot check OFF (role=%s) — should only be set in dev/tests", role,
        )
        return

    verifier = Path(os.environ.get("FINGERPRINT_VERIFIER_PATH", _DEFAULT_VERIFIER))
    if not verifier.is_file():
        logger.critical(
            "fingerprint verifier missing at %s; refusing to vouch for integrity (role=%s)",
            verifier, role,
        )
        # Configuration problem on the host, not a tampering signal.
        # Do not crash boot — would create a chicken-and-egg deploy bug.
        return

    # RELEASE_OIDC_REPO is mandatory for the Sigstore identity binding.
    # In `report` mode we log loudly and continue so the host can boot
    # while operators finish provisioning. In `enforce` mode we exit 14.
    if not os.environ.get("RELEASE_OIDC_REPO"):
        logger.critical(
            "RELEASE_OIDC_REPO env var is unset; Sigstore identity binding cannot be wildcard "
            "(role=%s mode=%s) — see deploy/fingerprint/TRUST_MODEL.md", role, mode,
        )
        if mode == _MODE_ENFORCE:
            sys.exit(14)
        return

    env = os.environ.copy()
    env["ROLE"] = role
    env.setdefault("FINGERPRINT_DEPLOYED_PATH", str(_DEFAULT_FP_PATH))
    # The shell verifier still respects its own KILL flag for the metrics
    # branch, but we never let it kill — Python owns the SystemExit so we
    # can apply enforcement mode uniformly across all three checks.
    env["FINGERPRINT_KILL_ON_MISMATCH"] = "0"

    try:
        result = subprocess.run(
            ["bash", str(verifier)],
            cwd=str(_REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.critical(
            "fingerprint verifier failed to execute (role=%s mode=%s): %s", role, mode, exc,
        )
        return

    rc = result.returncode
    stderr_tail = (result.stderr or "").strip().splitlines()[-5:]
    stderr_blob = " | ".join(stderr_tail)

    if rc == _RC_OK:
        logger.info("fingerprint OK (role=%s mode=%s)", role, mode)
        return

    if rc in _FATAL_CODES:
        logger.critical(
            "FINGERPRINT INTEGRITY FAILURE role=%s mode=%s rc=%d details=%s",
            role, mode, rc, stderr_blob,
        )
        if mode == _MODE_ENFORCE:
            # Hard exit. Do NOT raise — let the process terminate cleanly
            # so pm2 records it as a crash and the supervisor restarts
            # (or, if the deployed artifact is genuinely tampered, keeps
            # crashing until an operator intervenes).
            sys.exit(rc)
        return

    if rc in (_RC_RECOMPUTE_FAILED, _RC_CONFIG_ERROR):
        logger.critical(
            "fingerprint verifier configuration/tooling error role=%s mode=%s rc=%d details=%s "
            "(boot continues; investigate verifier setup)",
            role, mode, rc, stderr_blob,
        )
        return

    logger.critical(
        "fingerprint verifier returned unexpected rc=%d role=%s mode=%s details=%s",
        rc, role, mode, stderr_blob,
    )


def verify_config_or_die(role: str) -> None:
    """
    Run the CONFIG-fingerprint verifier once for the given role. Companion
    to `verify_or_die`; covers the runtime-config layer (env, db flags,
    redis flags, nginx) the source-tree fingerprint deliberately omits.

    Fail-closed conditions (raise SystemExit when enforcement mode is
    `enforce`; logged + metered but non-fatal in `report` mode):
      * composite drift in any source
      * a required source is unavailable on the host
      * the expected baseline file is missing
      * the expected baseline file does not declare a required source
        (i.e. baseline was captured without STRICT=1 and is incomplete)

    A misconfigured verifier (jq/bash/builder missing, ROLE unset) logs
    CRITICAL but does NOT crash boot — refusing to start because the
    verifier is broken would be a self-inflicted outage.

    Args:
        role: short label ("api", "worker", "scheduler"). Becomes the
              `component` label on the emitted Prometheus metric.

    Raises:
        SystemExit(rc): if the verifier reports a fatal config-integrity
            failure AND enforcement mode is `enforce`.
    """
    mode = _enforcement_mode()
    if mode == _MODE_OFF:
        logger.warning(
            "config fingerprint boot check OFF (role=%s) — should only be set in dev/tests",
            role,
        )
        return

    verifier = Path(os.environ.get("FINGERPRINT_CONFIG_VERIFIER_PATH", _DEFAULT_CONFIG_VERIFIER))
    if not verifier.is_file():
        logger.critical(
            "config verifier missing at %s (role=%s mode=%s); cannot vouch for runtime config integrity",
            verifier, role, mode,
        )
        return

    env = os.environ.copy()
    env["ROLE"] = role
    env.setdefault("FINGERPRINT_CONFIG_EXPECTED_PATH", str(_DEFAULT_CONFIG_EXPECTED))
    # Python owns the SystemExit; the shell verifier should not also kill.
    env["FINGERPRINT_KILL_ON_MISMATCH"] = "0"

    try:
        result = subprocess.run(
            ["bash", str(verifier)],
            cwd=str(_REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.critical(
            "config verifier failed to execute (role=%s mode=%s): %s", role, mode, exc,
        )
        return

    rc = result.returncode
    stderr_tail = (result.stderr or "").strip().splitlines()[-5:]
    stderr_blob = " | ".join(stderr_tail)

    if rc == _CFG_RC_OK:
        logger.info("config fingerprint OK (role=%s mode=%s)", role, mode)
        return

    if rc in _CFG_FATAL_CODES:
        logger.critical(
            "CONFIG INTEGRITY FAILURE role=%s mode=%s rc=%d details=%s",
            role, mode, rc, stderr_blob,
        )
        if mode == _MODE_ENFORCE:
            sys.exit(rc)
        return

    if rc in (_CFG_RC_RECOMPUTE_FAILED, _CFG_RC_CONFIG_ERROR):
        logger.critical(
            "config verifier configuration/tooling error role=%s mode=%s rc=%d details=%s "
            "(boot continues; investigate verifier setup)",
            role, mode, rc, stderr_blob,
        )
        return

    logger.critical(
        "config verifier returned unexpected rc=%d role=%s mode=%s details=%s",
        rc, role, mode, stderr_blob,
    )


def verify_runtime_or_die(role: str) -> None:
    """
    Run the RUNTIME-fingerprint verifier once for the given role. Third
    companion to `verify_or_die` and `verify_config_or_die`; covers the
    installed Python runtime — the interpreter binary on disk plus
    every file under each site-packages directory. Closes the "edit
    installed package" attack vector that the source-tree fingerprint
    deliberately ignores.

    Fail-closed conditions (raise SystemExit when enforcement mode is
    `enforce`; logged + metered but non-fatal in `report` mode):
      * composite drift (any component changed)
      * interpreter drift (python binary on disk changed)
      * site_packages drift (any installed package file changed)
      * required component unavailable on host
      * expected baseline file missing
      * expected baseline file does not declare a required component
        (i.e. baseline was captured without STRICT=1 and is incomplete)

    A misconfigured verifier (jq/bash/builder missing, ROLE unset) logs
    CRITICAL but does NOT crash boot — refusing to start because the
    verifier is broken would be a self-inflicted outage.

    Args:
        role: short label ("api", "worker", "scheduler"). Becomes the
              `component` label on the emitted Prometheus metric.

    Raises:
        SystemExit(rc): if the verifier reports a fatal runtime-integrity
            failure AND enforcement mode is `enforce`.
    """
    mode = _enforcement_mode()
    if mode == _MODE_OFF:
        logger.warning(
            "runtime fingerprint boot check OFF (role=%s) — should only be set in dev/tests",
            role,
        )
        return

    verifier = Path(os.environ.get("FINGERPRINT_RUNTIME_VERIFIER_PATH", _DEFAULT_RUNTIME_VERIFIER))
    if not verifier.is_file():
        logger.critical(
            "runtime verifier missing at %s (role=%s mode=%s); cannot vouch for installed-runtime integrity",
            verifier, role, mode,
        )
        return

    env = os.environ.copy()
    env["ROLE"] = role
    env.setdefault("FINGERPRINT_RUNTIME_EXPECTED_PATH", str(_DEFAULT_RUNTIME_EXPECTED))
    # Python owns the SystemExit; the shell verifier should not also kill.
    env["FINGERPRINT_KILL_ON_MISMATCH"] = "0"

    try:
        result = subprocess.run(
            ["bash", str(verifier)],
            cwd=str(_REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            # Site-packages walks are I/O heavy; allow more wall time
            # than the source/config verifiers. Still bounded so a
            # hung filesystem can't wedge boot indefinitely.
            timeout=300,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.critical(
            "runtime verifier failed to execute (role=%s mode=%s): %s", role, mode, exc,
        )
        return

    rc = result.returncode
    stderr_tail = (result.stderr or "").strip().splitlines()[-5:]
    stderr_blob = " | ".join(stderr_tail)

    if rc == _RT_RC_OK:
        logger.info("runtime fingerprint OK (role=%s mode=%s)", role, mode)
        return

    if rc in _RT_FATAL_CODES:
        logger.critical(
            "RUNTIME INTEGRITY FAILURE role=%s mode=%s rc=%d details=%s",
            role, mode, rc, stderr_blob,
        )
        if mode == _MODE_ENFORCE:
            sys.exit(rc)
        return

    if rc in (_RT_RC_RECOMPUTE_FAILED, _RT_RC_CONFIG_ERROR):
        logger.critical(
            "runtime verifier configuration/tooling error role=%s mode=%s rc=%d details=%s "
            "(boot continues; investigate verifier setup)",
            role, mode, rc, stderr_blob,
        )
        return

    logger.critical(
        "runtime verifier returned unexpected rc=%d role=%s mode=%s details=%s",
        rc, role, mode, stderr_blob,
    )


__all__ = [
    "verify_or_die",
    "verify_config_or_die",
    "verify_runtime_or_die",
    # Exposed for tests + operator tooling that wants to introspect the
    # current mode without re-implementing the env-precedence rules.
    "_enforcement_mode",
]
