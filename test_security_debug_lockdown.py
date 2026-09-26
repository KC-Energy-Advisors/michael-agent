"""
[SEC-1] Debug-surface lockdown - security and regression tests.

Drives the REAL FastAPI app (middleware + routing) through an in-process ASGI
transport, so the gate itself is exercised, not just the handlers. GHL and
Claude are faked; nothing leaves this process: no SMS, no GHL, no Anthropic.

Run:  python -m unittest test_security_debug_lockdown -v
"""

import asyncio
import contextlib
import io
import os
import unittest

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")

import httpx
import michael_agent as ma
from test_ai_pause import FakeGHL, StubClaude, PauseCase, payload

RealAsyncClient = httpx.AsyncClient          # captured before any FakeGHL.install()
GOOD_SECRET = "correct-horse-battery-staple-7f3a9c"

REMOVED = [("GET", "/debug/state/CONTACT123"),
           ("POST", "/debug/send-test-sms"),
           ("POST", "/debug/set-state/CONTACT123"),
           ("GET", "/debug/claude-test")]
RETAINED = [("POST", "/debug/reset/CONTACT123"),
            ("POST", "/debug/reset-contact"),
            ("GET", "/debug/form-schema")]
EVASIONS = [("GET", "/DEBUG/state/CONTACT123"), ("POST", "/Debug/Send-Test-SMS"),
            ("GET", "/debug"), ("GET", "/debug/"), ("GET", "/debug/state/CONTACT123/"),
            ("GET", "/%64ebug/state/CONTACT123"), ("POST", "/debug/%73end-test-sms"),
            ("OPTIONS", "/debug/send-test-sms"), ("PUT", "/debug/set-state/CONTACT123"),
            ("GET", "/debug/does-not-exist")]
SMS_BODY = {"contact_id": "CONTACT123", "to_number": "+13145550123", "message": "pwned"}


def call(method, path, headers=None, json=None):
    async def _go():
        transport = httpx.ASGITransport(app=ma.app)
        async with RealAsyncClient(transport=transport, base_url="http://michael.test") as c:
            return await c.request(method, path, headers=headers or {}, json=json)
    return asyncio.run(_go())


class SecCase(unittest.TestCase):
    def setUp(self):
        self._saved = {n: getattr(ma, n) for n in
                       ("DEBUG_ROUTES_ENABLED", "DEBUG_RESET_SECRET", "GHL_API_KEY",
                        "send_sms_via_ghl", "claude", "fetch_ghl_custom_field_schema")}
        self.sms_calls = []
        async def _no_sms(*a, **k):
            self.sms_calls.append((a, k))
            return {"status": "accepted"}
        ma.send_sms_via_ghl = _no_sms
        self.claude = StubClaude()
        ma.claude = self.claude
        ma._state_store.clear()
        self.addCleanup(self._restore)

    def _restore(self):
        for n, v in self._saved.items():
            setattr(ma, n, v)
        ma._state_store.clear()

    def assertNoSideEffects(self):
        self.assertEqual(self.sms_calls, [], "an SMS path was reached")
        self.assertEqual(self.claude.calls, [], "Claude was called")
        self.assertEqual(ma._state_store, {}, "in-memory state was touched")


# ═════════════════════════════════════════════════════════════════════════
class S01_DisabledByDefault(SecCase):
    def test_default_env_is_off(self):
        self.assertFalse(self._saved["DEBUG_ROUTES_ENABLED"],
                         "DEBUG_ROUTES_ENABLED must default to off")

    def test_every_debug_route_is_404_even_with_a_valid_secret(self):
        ma.DEBUG_ROUTES_ENABLED = False
        ma.DEBUG_RESET_SECRET = GOOD_SECRET
        for method, path in REMOVED + RETAINED + EVASIONS:
            for hdrs in ({}, {"X-Debug-Secret": GOOD_SECRET}):
                r = call(method, path, headers=hdrs, json=SMS_BODY)
                self.assertEqual(r.status_code, 404, f"{method} {path} {hdrs} -> {r.status_code}")
                self.assertEqual(r.json(), {"detail": "Not Found"})
        self.assertNoSideEffects()

    def test_cors_preflight_does_not_open_the_surface(self):
        ma.DEBUG_ROUTES_ENABLED = False
        r = call("OPTIONS", "/debug/send-test-sms",
                 headers={"Origin": "https://stlenergyadvisors.com",
                          "Access-Control-Request-Method": "POST"})
        self.assertEqual(r.status_code, 404)


class S02_EnabledRequiresSecret(SecCase):
    def setUp(self):
        super().setUp()
        ma.DEBUG_ROUTES_ENABLED = True

    def test_secret_unset_fails_closed_503(self):
        ma.DEBUG_RESET_SECRET = ""
        for method, path in REMOVED + RETAINED:
            r = call(method, path, headers={"X-Debug-Secret": "anything"}, json=SMS_BODY)
            self.assertEqual(r.status_code, 503, f"{method} {path}")
        self.assertNoSideEffects()

    def test_missing_or_wrong_secret_401(self):
        ma.DEBUG_RESET_SECRET = GOOD_SECRET
        for hdrs in ({}, {"X-Debug-Secret": "wrong"}, {"X-Debug-Secret": GOOD_SECRET[:-1]},
                     {"x-debug-secret": GOOD_SECRET.upper()}):
            for method, path in REMOVED + RETAINED:
                r = call(method, path, headers=hdrs, json=SMS_BODY)
                self.assertEqual(r.status_code, 401, f"{method} {path} {hdrs}")
        self.assertNoSideEffects()

    def test_secret_equal_to_ghl_key_is_refused_503(self):
        ma.GHL_API_KEY = "pit-shared-value-0000000000000000000000"
        ma.DEBUG_RESET_SECRET = ma.GHL_API_KEY
        r = call("GET", "/debug/form-schema", headers={"X-Debug-Secret": ma.GHL_API_KEY})
        self.assertEqual(r.status_code, 503)
        self.assertNotIn(ma.GHL_API_KEY, r.text)

    def test_secret_equal_to_anthropic_key_is_refused_503(self):
        ma.DEBUG_RESET_SECRET = os.environ["ANTHROPIC_API_KEY"]
        r = call("GET", "/debug/form-schema",
                 headers={"X-Debug-Secret": os.environ["ANTHROPIC_API_KEY"]})
        self.assertEqual(r.status_code, 503)


class S03_RemovedRoutesAreGone(SecCase):
    def test_removed_routes_404_even_when_fully_authorised(self):
        ma.DEBUG_ROUTES_ENABLED = True
        ma.DEBUG_RESET_SECRET = GOOD_SECRET
        for method, path in REMOVED:
            r = call(method, path, headers={"X-Debug-Secret": GOOD_SECRET}, json=SMS_BODY)
            self.assertEqual(r.status_code, 404, f"{method} {path}")
        self.assertNoSideEffects()

    def test_handlers_no_longer_exist(self):
        for name in ("debug_get_state", "debug_send_test_sms", "debug_set_stage", "debug_claude_test"):
            self.assertFalse(hasattr(ma, name), name)
        paths = {getattr(r, "path", "") for r in ma.app.routes}
        for p in ("/debug/state/{contact_id}", "/debug/send-test-sms",
                  "/debug/set-state/{contact_id}", "/debug/claude-test"):
            self.assertNotIn(p, paths)

    def test_only_the_three_retained_debug_routes_remain(self):
        debug = sorted(getattr(r, "path", "") for r in ma.app.routes
                       if getattr(r, "path", "").startswith("/debug"))
        self.assertEqual(debug, ["/debug/form-schema", "/debug/reset-contact",
                                 "/debug/reset/{contact_id}"])


class S04_RetainedRoutesWorkWhenAuthorised(SecCase):
    def setUp(self):
        super().setUp()
        ma.DEBUG_ROUTES_ENABLED = True
        ma.DEBUG_RESET_SECRET = GOOD_SECRET
        self.h = {"X-Debug-Secret": GOOD_SECRET}

    def test_form_schema_reachable(self):
        async def _schema(force=False):
            return {"id1": "contact.homeowner_status"}
        ma.fetch_ghl_custom_field_schema = _schema
        r = call("GET", "/debug/form-schema", headers=self.h)
        self.assertEqual(r.status_code, 200)
        self.assertIn("schema_resolved", r.json())

    def test_reset_contact_reachable_and_validates_input(self):
        r = call("POST", "/debug/reset-contact", headers=self.h, json={})
        self.assertEqual(r.status_code, 400)

    def test_reset_reachable(self):
        r = call("POST", "/debug/reset/NOBODY", headers=self.h)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["existed"], False)


class S05_NoLeaksInResponsesOrLogs(SecCase):
    def test_blocked_requests_log_no_ids_secrets_or_bodies(self):
        ma.DEBUG_RESET_SECRET = GOOD_SECRET
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            for enabled in (False, True):
                ma.DEBUG_ROUTES_ENABLED = enabled
                call("GET", "/debug/state/CONTACT123", headers={"X-Debug-Secret": "wrong"})
                call("POST", "/debug/send-test-sms", headers={"X-Debug-Secret": "wrong"},
                     json=SMS_BODY)
        out = buf.getvalue()
        for leak in ("CONTACT123", GOOD_SECRET, "wrong", "+13145550123", "pwned"):
            self.assertNotIn(leak, out, f"log leaked {leak!r}")
        self.assertIn("[SEC] debug route blocked", out)

    def test_error_bodies_never_echo_secrets(self):
        ma.DEBUG_ROUTES_ENABLED = True
        ma.DEBUG_RESET_SECRET = GOOD_SECRET
        r = call("GET", "/debug/form-schema", headers={"X-Debug-Secret": "wrong"})
        self.assertNotIn(GOOD_SECRET, r.text)
        self.assertNotIn("wrong", r.text)

    def test_health_masks_contact_id(self):
        saved = ma._qh_stats["last_hold_failure_contact"]
        self.addCleanup(lambda: ma._qh_stats.__setitem__("last_hold_failure_contact", saved))
        ma._qh_stats["last_hold_failure_contact"] = "AbCdEfGh1234"
        r = call("GET", "/health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["after_hours"]["last_hold_failure_contact"], "***1234")
        self.assertNotIn("AbCdEfGh1234", r.text)

    def test_no_key_prefix_logging_in_source(self):
        src = open(ma.__file__, encoding="utf-8").read()
        self.assertNotIn("api_key[:", src)
        self.assertNotIn("API KEY PREFIX", src)


class S06_ProductionRoutesPassThrough(SecCase):
    def test_gate_only_matches_debug_paths(self):
        for r in ma.app.routes:
            p = getattr(r, "path", "")
            self.assertEqual(ma._is_debug_path(p), p.startswith("/debug"), p)
        for p in ("/webhook/inbound", "/webhook/booking-followup", "/webhook/booking-nudge",
                  "/webhook/message-status", "/webhook/after-hours-resume",
                  "/webhook/website-chat", "/health", "/", "/debugger", "/webhook/debug"):
            self.assertFalse(ma._is_debug_path(p), p)

    def test_root_and_health_unchanged(self):
        self.assertEqual(call("GET", "/").json(), {"status": "ok"})
        r = call("GET", "/health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "ok")

    def test_website_chat_still_reachable(self):
        r = call("POST", "/webhook/website-chat", json={"message": ""})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["mode"], "ai")


class S07_WebhooksEndToEndThroughTheGate(PauseCase):
    """The real inbound path through the FULL ASGI stack, debug gate included."""

    def post_inbound(self, cid, text, tags):
        ma._contact_last_processed_ts.pop(cid, None)
        return call("POST", "/webhook/inbound", json=payload(cid, text, tags=tags))

    def test_normal_inbound_replies(self):
        g = FakeGHL("w1", tags=["ai-engaged", "ai-outreach-sent"]); g.install()
        self.seed_conversation("w1")
        r = self.post_inbound("w1", "yes I own it", g.contact["tags"])
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(g.sms), 1, r.text)

    def test_ai_paused_still_suppressed(self):
        g = FakeGHL("w2", tags=["ai-engaged", "ai-paused"]); g.install()
        self.seed_conversation("w2")
        r = self.post_inbound("w2", "hello?", g.contact["tags"])
        self.assertEqual(r.json().get("reason"), "ai_paused")
        self.assertEqual(g.sms, [])

    def test_stop_still_opts_out(self):
        g = FakeGHL("w3", tags=["ai-engaged"]); g.install()
        self.seed_conversation("w3")
        self.post_inbound("w3", "STOP", g.contact["tags"])
        self.assertEqual(ma.get_state("w3")["stage"], ma.Stage.DNC)
        self.assertIn("solar-dnc", g.tags_now())

    def test_dnc_contact_still_silent(self):
        g = FakeGHL("w4", tags=["solar-dnc", "dnc"]); g.install()
        self.seed_conversation("w4")
        self.post_inbound("w4", "hi again", g.contact["tags"])
        self.assertEqual(g.sms, [])


if __name__ == "__main__":
    unittest.main()
