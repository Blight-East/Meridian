"""
runtime/safety/test_fingerprint_check.py

Lockdown tests for the enforcement-mode resolution and the boot-check
contract. These tests do NOT exercise the actual shell verifiers — they
guard the Python policy layer that decides:

  * what to do when env vars are unset / typoed,
  * what legacy knobs still work,
  * that `enforce` mode never silently downgrades.

The default mode is the most important invariant: shipping the boot
hooks with `enforce` as the default would crash any host that hasn't
yet captured baselines or set RELEASE_OIDC_REPO. The test for
`_enforcement_mode_default_is_report` is the trip-wire for that
regression.

Run:
    python3 -m pytest runtime/safety/test_fingerprint_check.py -q
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

# Repo root on sys.path so `runtime.safety.fingerprint_check` imports
# without needing the test runner to set PYTHONPATH.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from runtime.safety import fingerprint_check  # noqa: E402


_ENV_KNOBS = (
    "FINGERPRINT_ENFORCEMENT_MODE",
    "FINGERPRINT_KILL_ON_MISMATCH",
    "FINGERPRINT_BOOT_CHECK_DISABLED",
)


def _clean_env() -> dict:
    """Build a baseline env dict with every fingerprint knob unset."""
    env = dict(os.environ)
    for key in _ENV_KNOBS:
        env.pop(key, None)
    return env


class EnforcementModeResolution(unittest.TestCase):
    """Lock down the precedence rules in `_enforcement_mode()`."""

    def test_default_is_report(self):
        """
        Shipping default MUST be `report`. Flipping this to `enforce`
        without a deliberate operator-side env change would crash any
        host that lacks pre-existing baselines / OIDC env. If you are
        editing this assertion, you are also signing up to write the
        deploy-runbook entry that captures baselines first.
        """
        with mock.patch.dict(os.environ, _clean_env(), clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "report")

    def test_explicit_enforce_wins(self):
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "enforce"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "enforce")

    def test_explicit_report_wins(self):
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "report"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "report")

    def test_explicit_off_wins(self):
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "off"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "off")

    def test_canonical_mode_is_case_insensitive(self):
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "ENFORCE"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "enforce")

    def test_typo_falls_back_to_report_not_off(self):
        """
        A typoed mode value (e.g. `enfoce`) MUST NOT silently disable
        the verifier. Falling back to `report` keeps metrics flowing
        and surfaces the typo in the CRITICAL log line.
        """
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "enfoce"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "report")

    def test_legacy_kill_on_mismatch_maps_to_enforce(self):
        env = _clean_env()
        env["FINGERPRINT_KILL_ON_MISMATCH"] = "1"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "enforce")

    def test_legacy_boot_check_disabled_maps_to_off(self):
        env = _clean_env()
        env["FINGERPRINT_BOOT_CHECK_DISABLED"] = "1"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "off")

    def test_canonical_mode_overrides_legacy_kill(self):
        """
        If both the canonical and legacy knobs are set with conflicting
        values, the canonical knob MUST win. Otherwise an operator
        flipping enforcement back to `report` for a hotfix would be
        silently overridden by a stale legacy env var.
        """
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "report"
        env["FINGERPRINT_KILL_ON_MISMATCH"] = "1"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(fingerprint_check._enforcement_mode(), "report")

    def test_off_mode_short_circuits_check(self):
        """In `off` mode the verifier subprocess MUST NOT be invoked."""
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "off"
        with mock.patch.dict(os.environ, env, clear=True), \
             mock.patch.object(fingerprint_check.subprocess, "run") as mock_run:
            fingerprint_check.verify_or_die("api")
            fingerprint_check.verify_config_or_die("api")
            fingerprint_check.verify_runtime_or_die("api")
            mock_run.assert_not_called()


class ReportModeDoesNotKill(unittest.TestCase):
    """
    Strongest invariant in this file: in `report` mode, NO code path may
    raise SystemExit. If you ever add a new error branch and forget to
    gate it on `mode == _MODE_ENFORCE`, this test fails.
    """

    def _run_all_checks_with_fake_verifier(self, returncode: int) -> None:
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "report"
        # Provide RELEASE_OIDC_REPO so verify_or_die doesn't bail before
        # invoking the (mocked) subprocess.
        env["RELEASE_OIDC_REPO"] = "test/repo"
        fake_completed = mock.MagicMock(returncode=returncode, stderr="", stdout="")
        with mock.patch.dict(os.environ, env, clear=True), \
             mock.patch.object(fingerprint_check.Path, "is_file", return_value=True), \
             mock.patch.object(fingerprint_check.subprocess, "run", return_value=fake_completed):
            # None of these should raise SystemExit in report mode.
            fingerprint_check.verify_or_die("api")
            fingerprint_check.verify_config_or_die("api")
            fingerprint_check.verify_runtime_or_die("api")

    def test_report_mode_swallows_source_fatal(self):
        # _RC_FP_MISMATCH = 11 — the canonical "tampered artifact" code.
        self._run_all_checks_with_fake_verifier(11)

    def test_report_mode_swallows_config_fatal(self):
        # _CFG_RC_DRIFT = 20.
        self._run_all_checks_with_fake_verifier(20)

    def test_report_mode_swallows_runtime_fatal(self):
        # _RT_RC_INTERPRETER_DRIFT = 31.
        self._run_all_checks_with_fake_verifier(31)

    def test_report_mode_swallows_missing_oidc_repo(self):
        """RELEASE_OIDC_REPO unset should NOT exit in report mode."""
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "report"
        env.pop("RELEASE_OIDC_REPO", None)
        with mock.patch.dict(os.environ, env, clear=True), \
             mock.patch.object(fingerprint_check.Path, "is_file", return_value=True):
            # Must return cleanly.
            fingerprint_check.verify_or_die("api")


class EnforceModeKills(unittest.TestCase):
    """Mirror invariant: in `enforce` mode, fatal codes MUST exit."""

    def _expect_systemexit(self, fn, returncode: int) -> int:
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "enforce"
        env["RELEASE_OIDC_REPO"] = "test/repo"
        fake_completed = mock.MagicMock(returncode=returncode, stderr="", stdout="")
        with mock.patch.dict(os.environ, env, clear=True), \
             mock.patch.object(fingerprint_check.Path, "is_file", return_value=True), \
             mock.patch.object(fingerprint_check.subprocess, "run", return_value=fake_completed):
            with self.assertRaises(SystemExit) as ctx:
                fn("api")
            return int(ctx.exception.code)

    def test_enforce_kills_on_source_fatal(self):
        self.assertEqual(self._expect_systemexit(fingerprint_check.verify_or_die, 11), 11)

    def test_enforce_kills_on_config_fatal(self):
        self.assertEqual(self._expect_systemexit(fingerprint_check.verify_config_or_die, 20), 20)

    def test_enforce_kills_on_runtime_fatal(self):
        self.assertEqual(self._expect_systemexit(fingerprint_check.verify_runtime_or_die, 31), 31)

    def test_enforce_does_not_kill_on_recompute_failure(self):
        """
        Tooling errors (jq missing, builder broken) MUST NOT crash boot
        even in enforce mode — that's a self-inflicted outage.
        Codes: source 13, config 23, runtime 34.
        """
        env = _clean_env()
        env["FINGERPRINT_ENFORCEMENT_MODE"] = "enforce"
        env["RELEASE_OIDC_REPO"] = "test/repo"
        for fn, rc in (
            (fingerprint_check.verify_or_die, 13),
            (fingerprint_check.verify_config_or_die, 23),
            (fingerprint_check.verify_runtime_or_die, 34),
        ):
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(fingerprint_check.Path, "is_file", return_value=True), \
                 mock.patch.object(
                     fingerprint_check.subprocess, "run",
                     return_value=mock.MagicMock(returncode=rc, stderr="", stdout=""),
                 ):
                # Should return cleanly, not raise.
                fn("api")


if __name__ == "__main__":
    unittest.main()
