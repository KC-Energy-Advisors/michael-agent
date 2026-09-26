"""
[AI-PAUSE] Human-takeover guard - offline end-to-end tests.

Drives the REAL inbound_webhook / after_hours_resume / send_sms_via_ghl code
against an in-memory fake of the GHL API (every HTTP call is intercepted) and
a stubbed Claude. Nothing leaves this process: no SMS, no GHL, no Anthropic.

Run:  python -m unittest test_ai_pause -v
"""

import asyncio
import json
import os
import re
import unittest

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")

import michael_agent as ma

run = asyncio.run
CLAUDE_TEXT = "Makes sense. Roughly what does the Ameren bill run each month?"


# ── Claude stub ──────────────────────────────────────────────────────────
class _Blk:
    def __init__(self, t): self.text = t
class _Resp:
    def __init__(self, t): self.content = [_Blk(t)]
class _Msgs:
    def __init__(self, owner): self.o = owner
    def create(self, **kw):
        self.o.calls.append(kw)
        return _Resp(self.o.text)
class StubClaude:
    def __init__(self, text=CLAUDE_TEXT):
        self.text, self.calls = text, []
        self.messages = _Msgs(self)


# ── Fake GHL ─────────────────────────────────────────────────────────────
class HResp:
    def __init__(self, code, payload=None):
        self.status_code, self._p = code, payload
        self.text = json.dumps(payload) if payload is not None else ""
        self.headers = {}
    @property
    def is_success(self): return 200 <= self.status_code < 300
    def json(self):
        if self._p is None: raise ValueError("not json")
        return self._p
    def raise_for_status(self):
        if not self.is_success:
            raise RuntimeError(f"HTTP {self.status_code} (mock)")
        return self


class FakeGHL:
    """One contact, one conversation. Records every SMS POST and tag write."""

    def __init__(self, cid, tags=(), dnd_status="inactive", appointments=(),
                 fail_contact_get=False):
        self.cid = cid
        self.contact = {"id": cid, "phone": "+13145550123", "email": "",
                        "firstName": "Pat", "lastName": "Lee", "dnd": False,
                        "dndSettings": {"SMS": {"status": dnd_status}},
                        "tags": list(tags), "dateAdded": "", "customFields": []}
        self.appointments = list(appointments)
        self.fail_contact_get = fail_contact_get
        self.sms = []          # bodies of every POST /conversations/messages
        self.tag_puts = []     # every tag array PUT to the contact
        self.gets = []
        self.thread = []       # GHL message thread, NEWEST FIRST (as GHL returns it)

    def _now(self):
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    def lead_texts(self, body):
        """GHL stores the inbound before firing the webhook."""
        self.thread.insert(0, {"id": f"in{len(self.thread)}", "direction": "inbound",
                               "body": body, "dateAdded": self._now(),
                               "messageType": "TYPE_SMS"})

    def owner_texts_manually(self, body):
        self.thread.insert(0, {"id": f"man{len(self.thread)}", "direction": "outbound",
                               "body": body, "dateAdded": self._now(), "userId": "OWNER",
                               "messageType": "TYPE_SMS", "source": "app"})

    def install(self):
        g = self
        class Client:
            def __init__(s, *a, **k): pass
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return False
            async def get(s, url, **k):
                g.gets.append(url)
                if "/appointments" in url:
                    return HResp(200, {"events": g.appointments})
                if "/opportunities" in url:
                    return HResp(200, {"opportunities": []})
                if "/customFields" in url:
                    return HResp(200, {"customFields": []})
                if "/conversations/search" in url:
                    return HResp(200, {"conversations": [{"id": "CV1"}]})
                if "/messages" in url:
                    return HResp(200, {"messages": {"messages": list(g.thread)}})
                if re.search(r"/contacts/[^/?]+", url):
                    if g.fail_contact_get:
                        return HResp(500, {})
                    return HResp(200, {"contact": dict(g.contact)})
                return HResp(200, {})
            async def post(s, url, **k):
                if url.endswith("/conversations/messages"):
                    _txt = (k.get("json") or {}).get("message", "")
                    g.sms.append(_txt)
                    g.thread.insert(0, {"id": f"out{len(g.thread)}", "direction": "outbound",
                                        "body": _txt, "dateAdded": g._now(),
                                        "messageType": "TYPE_SMS", "source": "app"})
                    return HResp(201, {"messageId": f"M{len(g.sms)}",
                                       "conversationId": "CV1"})
                if "/conversations/search" in url:
                    return HResp(200, {"conversations": [{"id": "CV1"}]})
                return HResp(200, {})
            async def put(s, url, **k):
                body = k.get("json") or {}
                if "tags" in body:
                    g.tag_puts.append(list(body["tags"]))
                    g.contact["tags"] = list(body["tags"])
                return HResp(200, {"contact": dict(g.contact)})
            async def delete(s, url, **k):
                return HResp(200, {})
        ma.httpx.AsyncClient = Client

    # Live GHL workflow "AI Pause - After Hours Hold Clear" (published 2026-09-26):
    #   Trigger: Contact Tag Added = ai-paused  ->  Remove Contact Tag = after-hours-hold
    pause_workflow_live = True

    def set_tags(self, tags):
        before = {t.lower() for t in self.contact["tags"]}
        tags = list(tags)
        if (self.pause_workflow_live and "ai-paused" in {t.lower() for t in tags}
                and "ai-paused" not in before):
            tags = [t for t in tags if t.lower() != "after-hours-hold"]
        self.contact["tags"] = tags

    def tags_now(self):
        return [t.lower() for t in self.contact["tags"]]


class Req:
    def __init__(self, payload):
        self._b = json.dumps(payload).encode()
        self.headers = {"content-type": "application/json", "user-agent": "test"}
        self.method = "POST"
        class _U:
            path = "/webhook/inbound"
            def __str__(s): return "http://test/webhook/inbound"
        self.url = _U()
        self.query_params = {}
        class _C: host = "127.0.0.1"
        self.client = _C()
    async def body(self): return self._b
    async def json(self): return json.loads(self._b)


_EVT = [0]
def payload(cid, text, tags=None, evt=None, include_tags=True):
    _EVT[0] += 1
    p = {"contactId": cid, "message": text, "phone": "+13145550123",
         "firstName": "Pat", "type": "SMS", "direction": "inbound",
         "messageId": evt or f"evt-{cid}-{_EVT[0]}"}
    if include_tags:
        p["tags"] = list(tags or [])
    return p


def resp_json(r):
    return json.loads(bytes(r.body).decode())


# ── Base case ────────────────────────────────────────────────────────────
class PauseCase(unittest.TestCase):
    CID = "c"

    def setUp(self):
        self._saved = {n: getattr(ma, n) for n in
                       ("claude", "within_send_window", "FORM_AWARE_ENABLED",
                        "AI_PAUSE_ENABLED")}
        self._client = ma.httpx.AsyncClient
        ma._state_store.clear()
        ma._no_form_fields.clear()
        ma._processed_event_ids.clear()
        self.claude = StubClaude()
        ma.claude = self.claude
        ma.within_send_window = lambda now=None: True
        ma.FORM_AWARE_ENABLED = True          # production value
        ma.AI_PAUSE_ENABLED = True
        self.addCleanup(self._restore)

    def _restore(self):
        for n, v in self._saved.items():
            setattr(ma, n, v)
        ma.httpx.AsyncClient = self._client
        ma._state_store.clear()

    def seed_conversation(self, cid, **extra):
        """A mid-qualification lead: Michael asked ownership, lead is engaged."""
        st = ma.get_state(cid)
        st.update({"stage": ma.Stage.ASK_OWNERSHIP, "phone": "+13145550123",
                   "location_confirmed": True, "q_utility": ma.UTIL_AMEREN,
                   "messages": [
                       {"role": "user", "content": "[Meta lead]"},
                       {"role": "assistant", "content": "Are you with Ameren Missouri?"},
                       {"role": "user", "content": "Yes"},
                       {"role": "assistant", "content": "Got it. Do you own the home?"},
                   ]})
        st.update(extra)
        ma.save_state(cid, st)
        return st

    def inbound(self, cid, text, **kw):
        # Real texts arrive seconds apart; clear the existing 2s post-processing
        # debounce so back-to-back test messages are not dropped as a burst.
        ma._contact_last_processed_ts.pop(cid, None)
        return resp_json(run(ma.inbound_webhook(Req(payload(cid, text, **kw)))))


# ═════════════════════════════════════════════════════════════════════════
class T01_NormalUnpaused(PauseCase):
    def test_unpaused_inbound_is_processed_normally(self):
        g = FakeGHL("n1", tags=["ai-engaged", "ai-outreach-sent"]); g.install()
        self.seed_conversation("n1")
        out = self.inbound("n1", "yes I own it", tags=g.contact["tags"])
        self.assertGreaterEqual(len(self.claude.calls), 1, out)
        self.assertEqual(len(g.sms), 1, out)
        self.assertNotEqual(out.get("reason"), "ai_paused")


class T02_PausedOrdinaryInbound(PauseCase):
    def test_no_claude_no_sms_no_history(self):
        g = FakeGHL("p2", tags=["ai-engaged", "ai-paused"]); g.install()
        st = self.seed_conversation("p2")
        hist_before = list(st["messages"]); stage_before = st["stage"]
        out = self.inbound("p2", "yes I own it, what's next?", tags=g.contact["tags"])
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(out["sms_sent"], False)
        self.assertEqual(self.claude.calls, [])
        self.assertEqual(g.sms, [])
        st = ma.get_state("p2")
        self.assertEqual(st["messages"], hist_before)   # never added to history
        self.assertEqual(st["stage"], stage_before)     # no restart / no mutation
        self.assertTrue(st.get("ai_paused_last_suppressed_at"))

    def test_fresh_tag_wins_over_stale_payload(self):
        # Tag added in GHL AFTER the webhook fired: payload lacks it, live read has it.
        g = FakeGHL("p2b", tags=["ai-engaged", "ai-paused"]); g.install()
        self.seed_conversation("p2b")
        out = self.inbound("p2b", "hello?", tags=["ai-engaged"])
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(self.claude.calls, []); self.assertEqual(g.sms, [])

    def test_paused_form_resubmission_gets_no_opener(self):
        g = FakeGHL("p2c", tags=["ai-paused", "meta-lead"]); g.install()
        out = self.inbound("p2c", "form submission", tags=g.contact["tags"])
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])


class T03_PausedStopStillOptsOut(PauseCase):
    def _check(self, cid, text):
        g = FakeGHL(cid, tags=["ai-engaged", "ai-paused"]); g.install()
        self.seed_conversation(cid)
        out = self.inbound(cid, text, tags=g.contact["tags"])
        st = ma.get_state(cid)
        self.assertEqual(st["stage"], ma.Stage.DNC, out)
        self.assertEqual(self.claude.calls, [])             # STOP never reaches Claude
        self.assertIn("solar-dnc", g.tags_now())            # durable DNC tags written
        self.assertIn("dnc", g.tags_now())
        self.assertIn("ai-paused", g.tags_now())            # pause untouched
        self.assertEqual(len(g.sms), 1)                     # compliance confirmation
        self.assertIn("unsubscribed", g.sms[0].lower())

    def test_stop_keyword(self):          self._check("s3a", "STOP")
    def test_stop_texting_me(self):       self._check("s3b", "stop texting me")
    def test_unsubscribe(self):           self._check("s3c", "unsubscribe")

    def test_carrier_dnd_still_blocks_even_the_confirmation(self):
        # GHL/carrier already set STOP DND -> existing behaviour: we send nothing.
        g = FakeGHL("s3d", tags=["ai-paused"], dnd_status="permanent"); g.install()
        self.seed_conversation("s3d")
        self.inbound("s3d", "STOP", tags=g.contact["tags"])
        self.assertEqual(g.sms, [])
        self.assertEqual(ma.get_state("s3d")["stage"], ma.Stage.DNC)


class T04_SolarDncUnchanged(PauseCase):
    def test_dnc_contact_still_silent(self):
        g = FakeGHL("d4", tags=["solar-dnc", "dnc"]); g.install()
        self.seed_conversation("d4")
        self.inbound("d4", "hi again", tags=g.contact["tags"])
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])
        self.assertEqual(ma.get_state("d4")["stage"], ma.Stage.DNC)

    def test_dnc_and_paused_still_silent(self):
        g = FakeGHL("d4b", tags=["solar-dnc", "ai-paused"]); g.install()
        self.seed_conversation("d4b")
        self.inbound("d4b", "hi again", tags=g.contact["tags"])
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])


class T05_DedupWhilePaused(PauseCase):
    def test_retry_of_a_paused_event_never_becomes_a_conversation(self):
        g = FakeGHL("u5", tags=["ai-engaged", "ai-paused"]); g.install()
        self.seed_conversation("u5")
        p = payload("u5", "are you there?", tags=g.contact["tags"], evt="EVT-U5")
        first = resp_json(run(ma.inbound_webhook(Req(p))))
        self.assertEqual(first["reason"], "ai_paused")
        g.set_tags(["ai-engaged"])                       # owner un-pauses
        p["tags"] = ["ai-engaged"]
        retry = resp_json(run(ma.inbound_webhook(Req(p))))  # same event id retried
        self.assertEqual(retry.get("reason"), "duplicate_event_id")
        self.assertEqual(self.claude.calls, []); self.assertEqual(g.sms, [])


class T06_T07_Booked(PauseCase):
    def _booked(self, cid, tags):
        g = FakeGHL(cid, tags=tags, appointments=[{"id": "A1",
                    "appointmentStatus": "confirmed", "startTime": "2099-01-01T15:00:00Z"}])
        g.install()
        self.seed_conversation(cid, stage=ma.Stage.BOOKED, appointment_booked=True)
        return g

    def test_booked_unpaused_still_gets_booked_followup(self):
        g = self._booked("b6", ["appointment booked", "ai-engaged"])
        out = self.inbound("b6", "what should I have ready for the visit?",
                           tags=g.contact["tags"])
        # Existing booked flow runs exactly as before (here: its canned
        # booked-contact message; no Claude needed for that branch).
        self.assertTrue(out.get("entered_booked_path"), out)
        self.assertTrue(out.get("sms_sent"), out)
        self.assertEqual(len(g.sms), 1, out)

    def test_booked_and_paused_gets_nothing(self):
        g = self._booked("b7", ["appointment booked", "ai-engaged", "ai-paused"])
        out = self.inbound("b7", "what should I have ready for the visit?",
                           tags=g.contact["tags"])
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(self.claude.calls, []); self.assertEqual(g.sms, [])
        self.assertIn("appointment booked", g.tags_now())   # booking untouched


class T08_T09_AfterHours(PauseCase):
    def test_after_hours_unpaused_unchanged_hold_placed(self):
        ma.within_send_window = lambda now=None: False
        g = FakeGHL("h8", tags=["ai-engaged"]); g.install()
        self.seed_conversation("h8")
        self.inbound("h8", "yes I own it", tags=g.contact["tags"])
        self.assertEqual(g.sms, [])
        self.assertIn("after-hours-hold", g.tags_now())

    def test_after_hours_paused_no_claude_no_hold(self):
        ma.within_send_window = lambda now=None: False
        g = FakeGHL("h9", tags=["ai-engaged", "ai-paused"]); g.install()
        self.seed_conversation("h9")
        out = self.inbound("h9", "yes I own it", tags=g.contact["tags"])
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(self.claude.calls, []); self.assertEqual(g.sms, [])
        self.assertNotIn("after-hours-hold", g.tags_now())

    def test_existing_hold_is_cleared_when_paused_inbound_arrives(self):
        g = FakeGHL("h9b", tags=["ai-engaged", "ai-paused", "after-hours-hold"]); g.install()
        self.seed_conversation("h9b")
        out = self.inbound("h9b", "still interested", tags=g.contact["tags"])
        self.assertTrue(out["hold_cleared"])
        self.assertNotIn("after-hours-hold", g.tags_now())
        self.assertIn("ai-paused", g.tags_now())

    def test_morning_resume_for_a_paused_contact_never_speaks(self):
        g = FakeGHL("h9c", tags=["ai-paused", "after-hours-hold"]); g.install()
        self.seed_conversation("h9c")
        class R:
            async def body(s): return json.dumps({"contact_id": "h9c"}).encode()
        out = resp_json(run(ma.after_hours_resume(R())))
        self.assertEqual(out["reason"], "ai_paused")
        self.assertTrue(out["hold_cleared"])
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])
        self.assertNotIn("after-hours-hold", g.tags_now())


class T10_T11_T12_T13_Resume(PauseCase):
    def test_full_takeover_lifecycle(self):
        g = FakeGHL("r1", tags=["ai-engaged"]); g.install()
        self.seed_conversation("r1", q_homeowner=ma.QUAL_YES)
        pre = ma.get_state("r1")
        pre_hist, pre_stage = list(pre["messages"]), pre["stage"]

        # Owner pauses; lead texts twice during the pause.
        g.set_tags(["ai-engaged", "ai-paused"])
        self.inbound("r1", "PAUSED-MSG-ONE about panels", tags=g.contact["tags"])
        self.inbound("r1", "PAUSED-MSG-TWO about financing", tags=g.contact["tags"])
        self.assertEqual(self.claude.calls, []); self.assertEqual(g.sms, [])

        # 10: owner removes the tag -> there is no code path that sends.
        g.set_tags(["ai-engaged"])
        self.assertEqual(g.sms, [])
        self.assertNotIn("after-hours-hold", g.tags_now())   # nothing queued

        # 13: pre-pause state intact.
        mid = ma.get_state("r1")
        self.assertEqual(mid["messages"], pre_hist)
        self.assertEqual(mid["stage"], pre_stage)
        self.assertEqual(mid["q_homeowner"], ma.QUAL_YES)

        # 11: next NEW inbound resumes normally.
        self.inbound("r1", "NEW-MSG-AFTER-RESUME my bill is about $180",
                     tags=g.contact["tags"])
        self.assertEqual(len(self.claude.calls), 1)
        self.assertEqual(len(g.sms), 1)

        # 12: paused-interval messages are NOT replayed to Claude.
        sent = json.dumps(self.claude.calls[-1].get("messages", []))
        self.assertIn("NEW-MSG-AFTER-RESUME", sent)
        self.assertNotIn("PAUSED-MSG-ONE", sent)
        self.assertNotIn("PAUSED-MSG-TWO", sent)
        self.assertIn("Do you own the home?", sent)            # pre-pause context kept

        # 13: no restart of qualification.
        post = ma.get_state("r1")
        self.assertNotEqual(post["stage"], ma.Stage.INITIAL)
        self.assertEqual(post["q_homeowner"], ma.QUAL_YES)


class T14_LookupFailure(PauseCase):
    def test_unknown_state_fails_closed(self):
        g = FakeGHL("f14", tags=["ai-engaged"], fail_contact_get=True); g.install()
        self.seed_conversation("f14")
        out = self.inbound("f14", "hello", include_tags=False)
        self.assertEqual(out["reason"], "ai_pause_state_unknown")
        self.assertEqual(self.claude.calls, []); self.assertEqual(g.sms, [])

    def test_lookup_failure_with_payload_pause_tag_is_paused(self):
        g = FakeGHL("f14b", tags=["ai-paused"], fail_contact_get=True); g.install()
        self.seed_conversation("f14b")
        out = self.inbound("f14b", "hello", tags=["ai-paused"])
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(g.sms, [])

    def test_lookup_failure_unpaused_payload_keeps_existing_fail_closed_send(self):
        # Behaviour unchanged from production: payload says not paused, the
        # live read is down, so the compliance gate refuses the send.
        g = FakeGHL("f14c", tags=["ai-engaged"], fail_contact_get=True); g.install()
        self.seed_conversation("f14c")
        out = self.inbound("f14c", "hello", tags=["ai-engaged"])
        self.assertNotEqual(out.get("reason"), "ai_paused")
        self.assertEqual(g.sms, [])

    def test_stop_during_unknown_state_is_not_swallowed(self):
        g = FakeGHL("f14d", tags=[], fail_contact_get=True); g.install()
        self.seed_conversation("f14d")
        self.inbound("f14d", "STOP", include_tags=False)
        self.assertEqual(ma.get_state("f14d")["stage"], ma.Stage.DNC)


class T16_SendGate(PauseCase):
    """Defense in depth inside send_sms_via_ghl."""
    def test_guarded_kinds_suppressed_exempt_kinds_allowed(self):
        g = FakeGHL("g16", tags=["ai-paused"]); g.install()
        for kind in (ma.SendKind.OUTREACH, ma.SendKind.QUALIFICATION,
                     ma.SendKind.BOOKING_PITCH, ma.SendKind.NURTURE,
                     ma.SendKind.BILL_ACK, ma.SendKind.SYSTEM):
            with self.subTest(kind=kind):
                r = run(ma.send_sms_via_ghl("g16", f"x {kind.value}",
                                            "+13145550123", kind=kind))
                self.assertFalse(r["sent"])
                self.assertIn(r["reason"], ("ai_paused", "booked_guard"))
        self.assertEqual(g.sms, [])
        for kind in (ma.SendKind.OPT_OUT, ma.SendKind.BOOKED_REPLY):
            with self.subTest(kind=kind):
                r = run(ma.send_sms_via_ghl("g16", f"ok {kind.value}",
                                            "+13145550123", kind=kind))
                self.assertNotEqual(r.get("reason"), "ai_paused")
        self.assertEqual(len(g.sms), 2)

    def test_dnd_still_wins_over_pause(self):
        g = FakeGHL("g16b", tags=["ai-paused"], dnd_status="permanent"); g.install()
        r = run(ma.send_sms_via_ghl("g16b", "hi", "+13145550123",
                                    kind=ma.SendKind.BOOKED_REPLY))
        self.assertEqual(r["reason"], ma.SUPPRESS_REASON_DND)

    def test_paused_send_never_places_after_hours_hold(self):
        ma.within_send_window = lambda now=None: False
        g = FakeGHL("g16c", tags=["ai-paused"]); g.install()
        r = run(ma.send_sms_via_ghl("g16c", "hi", "+13145550123",
                                    kind=ma.SendKind.NURTURE))
        self.assertIn(r["reason"], ("ai_paused", "booked_guard"))
        self.assertNotIn("after-hours-hold", g.tags_now())


class T17_KillSwitch(PauseCase):
    def test_flag_off_restores_previous_behaviour(self):
        ma.AI_PAUSE_ENABLED = False
        g = FakeGHL("k17", tags=["ai-engaged", "ai-paused"]); g.install()
        self.seed_conversation("k17")
        out = self.inbound("k17", "yes I own it", tags=g.contact["tags"])
        self.assertNotEqual(out.get("reason"), "ai_paused")
        self.assertEqual(len(g.sms), 1)


class _ResumeReq:
    def __init__(self, cid): self._b = json.dumps({"contact_id": cid}).encode()
    async def body(self): return self._b


class _BFReq(Req):
    pass


class T18_AfterHoursPauseLifecycle(PauseCase):
    """
    Invariant: once ai-paused is applied, Michael never answers a message that
    arrived before or during the pause just because the tag was removed.
    """

    def night_message(self, g, cid, text):
        """Lead texts after hours while NOT paused -> existing hold is placed."""
        ma.within_send_window = lambda now=None: False
        g.lead_texts(text)
        self.inbound(cid, text, tags=g.contact["tags"])
        self.assertIn("after-hours-hold", g.tags_now())      # existing behaviour
        self.assertEqual(g.sms, [])

    def morning_resume(self, cid):
        ma.within_send_window = lambda now=None: True
        return resp_json(run(ma.after_hours_resume(_ResumeReq(cid))))

    def pause(self, g, companion_workflow=False):
        g.set_tags(g.contact["tags"] + ["ai-paused"])
        if companion_workflow:
            # Proposed GHL workflow "AI Pause - Clear After-Hours Hold":
            # Tag added ai-paused -> Remove tag after-hours-hold.
            g.set_tags([t for t in g.contact["tags"] if t.lower() != "after-hours-hold"])

    def unpause(self, g):
        g.set_tags([t for t in g.contact["tags"] if t.lower() != "ai-paused"])

    # 8. ordinary after-hours lead who was never paused (existing behaviour)
    def test_never_paused_after_hours_lead_is_answered_at_resume(self):
        g = FakeGHL("L8", tags=["ai-engaged"]); g.install(); self.seed_conversation("L8")
        self.night_message(g, "L8", "do you serve Arnold?")
        out = self.morning_resume("L8")
        self.assertTrue(out.get("sms_sent"), out)
        self.assertEqual(len(g.sms), 1)

    # 1. after-hours -> pause -> still paused at resume
    def test_after_hours_then_pause_through_resume(self):
        g = FakeGHL("L1", tags=["ai-engaged"]); g.install(); self.seed_conversation("L1")
        g.pause_workflow_live = False   # Python-layer defense on its own (original model)
        self.night_message(g, "L1", "do you serve Arnold?")
        self.pause(g)
        out = self.morning_resume("L1")
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(g.sms, [])
        self.assertNotIn("after-hours-hold", g.tags_now())

    # 2a. after-hours -> pause -> unpause BEFORE resume, nothing else happens.
    #     Python alone cannot observe a pause that produced no event. This test
    #     models the GHL companion workflow as DISABLED, documenting exactly
    #     what it protects against if it is ever unpublished.
    @unittest.expectedFailure
    def test_silent_pause_unpause_before_resume_python_only_KNOWN_GAP(self):
        g = FakeGHL("L2a", tags=["ai-engaged"]); g.install(); self.seed_conversation("L2a")
        g.pause_workflow_live = False
        self.night_message(g, "L2a", "do you serve Arnold?")
        self.pause(g); self.unpause(g)
        self.morning_resume("L2a")
        self.assertEqual(g.sms, [])          # fails today: resume answers the held message

    # 2b. same sequence WITH the proposed companion GHL workflow -> invariant holds
    def test_silent_pause_unpause_before_resume_with_companion_workflow(self):
        g = FakeGHL("L2b", tags=["ai-engaged"]); g.install(); self.seed_conversation("L2b")
        self.night_message(g, "L2b", "do you serve Arnold?")
        self.pause(g, companion_workflow=True); self.unpause(g)
        out = self.morning_resume("L2b")
        self.assertEqual(out["reason"], "hold_already_cleared")
        self.assertEqual(g.sms, [])

    # 2c. SAME silent sequence with the LIVE production workflow (plain pause/unpause)
    def test_silent_pause_unpause_before_resume_with_live_ghl_workflow(self):
        g = FakeGHL("L2c", tags=["ai-engaged"]); g.install(); self.seed_conversation("L2c")
        self.night_message(g, "L2c", "do you serve Arnold?")
        self.pause(g)
        self.assertNotIn("after-hours-hold", g.tags_now())   # workflow fired on tag add
        self.unpause(g)
        out = self.morning_resume("L2c")
        self.assertEqual(out["reason"], "hold_already_cleared")
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls[1:], [])
        # dormant until a NEW inbound after unpause, then normal
        ma.within_send_window = lambda now=None: True
        g.lead_texts("NEW message after unpause")
        self.inbound("L2c", "NEW message after unpause", tags=g.contact["tags"])
        self.assertEqual(len(g.sms), 1)

    # 3. after-hours -> pause -> manual texts -> unpause -> resume (Python alone suffices)
    def test_after_hours_pause_manual_texts_unpause_resume(self):
        g = FakeGHL("L3", tags=["ai-engaged"]); g.install(); self.seed_conversation("L3")
        g.pause_workflow_live = False   # Python-layer defense on its own (original model)
        self.night_message(g, "L3", "do you serve Arnold?")
        self.pause(g)
        g.owner_texts_manually("Yes we do - I'll call you at 10 to go over it.")
        self.unpause(g)
        out = self.morning_resume("L3")
        self.assertEqual(out["reason"], "already_replied")
        self.assertEqual(g.sms, [])
        self.assertNotIn("after-hours-hold", g.tags_now())

    # 3b. lead replies DURING the pause -> hold cleared at that moment
    def test_after_hours_pause_lead_replies_while_paused_then_unpause(self):
        g = FakeGHL("L3b", tags=["ai-engaged"]); g.install(); self.seed_conversation("L3b")
        g.pause_workflow_live = False   # Python-layer defense on its own (original model)
        self.night_message(g, "L3b", "do you serve Arnold?")
        self.pause(g)
        g.lead_texts("also what about Fenton?")
        out = self.inbound("L3b", "also what about Fenton?", tags=g.contact["tags"])
        self.assertTrue(out["hold_cleared"])
        self.unpause(g)
        res = self.morning_resume("L3b")
        self.assertEqual(res["reason"], "hold_already_cleared")
        self.assertEqual(g.sms, [])

    # Same three sequences with the LIVE GHL workflow: the hold is removed the
    # moment ai-paused is added, so the resume stops even earlier. Invariant:
    # zero outbound SMS and no hold left behind.
    def test_live_workflow_all_takeover_sequences_send_nothing(self):
        for name, extra in (("through", None), ("manual", "manual"), ("lead", "lead")):
            with self.subTest(sequence=name):
                cid = f"LW-{name}"
                g = FakeGHL(cid, tags=["ai-engaged"]); g.install(); self.seed_conversation(cid)
                self.night_message(g, cid, "do you serve Arnold?")
                self.pause(g)
                self.assertNotIn("after-hours-hold", g.tags_now())
                if extra == "manual":
                    g.owner_texts_manually("I'll call you at 10.")
                if extra == "lead":
                    g.lead_texts("also Fenton?")
                    self.inbound(cid, "also Fenton?", tags=g.contact["tags"])
                if name != "through":
                    self.unpause(g)
                out = self.morning_resume(cid)
                self.assertIn(out["reason"], ("hold_already_cleared", "ai_paused"))
                self.assertEqual(g.sms, [])
                self.assertNotIn("after-hours-hold", g.tags_now())

    # 4. pause -> unpause, no new inbound -> nothing is ever sent or queued
    def test_pause_unpause_no_new_inbound_sends_nothing(self):
        g = FakeGHL("L4", tags=["ai-engaged"]); g.install(); self.seed_conversation("L4")
        self.pause(g); self.unpause(g)
        self.assertEqual(g.sms, [])
        self.assertNotIn("after-hours-hold", g.tags_now())
        self.assertEqual(self.claude.calls, [])

    # 5. pause -> manual texts -> unpause -> NEW inbound -> clean resume
    def test_pause_unpause_new_inbound_resumes_without_manual_or_paused_content(self):
        g = FakeGHL("L5", tags=["ai-engaged"]); g.install()
        self.seed_conversation("L5", q_homeowner=ma.QUAL_YES)
        self.pause(g)
        g.lead_texts("PAUSED-INBOUND question")
        self.inbound("L5", "PAUSED-INBOUND question", tags=g.contact["tags"])
        g.owner_texts_manually("MANUAL-OWNER-TEXT I'll handle this")
        self.unpause(g)
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])
        g.lead_texts("NEW-AFTER-UNPAUSE bill is about $180")
        self.inbound("L5", "NEW-AFTER-UNPAUSE bill is about $180", tags=g.contact["tags"])
        self.assertEqual(len(self.claude.calls), 1); self.assertEqual(len(g.sms), 1)
        sent = json.dumps(self.claude.calls[-1].get("messages", []))
        self.assertIn("NEW-AFTER-UNPAUSE", sent)
        self.assertNotIn("PAUSED-INBOUND", sent)
        self.assertNotIn("MANUAL-OWNER-TEXT", sent)
        self.assertNotEqual(ma.get_state("L5")["stage"], ma.Stage.INITIAL)

    # 6. STOP while paused, after hours -> DNC recorded; resume never speaks
    def test_stop_while_paused_after_hours(self):
        g = FakeGHL("L6", tags=["ai-engaged", "ai-paused"]); g.install(); self.seed_conversation("L6")
        ma.within_send_window = lambda now=None: False
        g.lead_texts("STOP")
        self.inbound("L6", "STOP", tags=g.contact["tags"])
        self.assertEqual(ma.get_state("L6")["stage"], ma.Stage.DNC)
        self.assertIn("solar-dnc", g.tags_now()); self.assertIn("dnc", g.tags_now())
        self.assertEqual(self.claude.calls, [])
        out = self.morning_resume("L6")
        self.assertEqual(g.sms, [])
        self.assertIn(out["reason"], ("consumer_opt_out", "ai_paused", "hold_already_cleared"))

    # 7. booking confirmation while paused is still sent (owner-approved exemption)
    def test_booking_confirmation_while_paused_is_sent(self):
        g = FakeGHL("L7", tags=["ai-engaged", "ai-paused"]); g.install(); self.seed_conversation("L7")
        out = resp_json(run(ma.booking_followup(_BFReq(
            {"contactId": "L7", "phone": "+13145550123", "firstName": "Pat"}))))
        self.assertEqual(len(g.sms), 1, out)
        self.assertEqual(self.claude.calls, [])


class T19_TagInterplay(PauseCase):
    """ai-paused must not disturb solar-dnc / solar-disqualified / booked / engagement."""

    def test_paused_disqualified_contact_silent_tags_untouched(self):
        g = FakeGHL("i1", tags=["ai-engaged", "solar-disqualified", "not_qualified", "ai-paused"])
        g.install(); self.seed_conversation("i1", stage=ma.Stage.DISQUALIFIED)
        self.inbound("i1", "actually I bought a house", tags=g.contact["tags"])
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])
        for t in ("solar-disqualified", "not_qualified", "ai-paused", "ai-engaged"):
            self.assertIn(t, g.tags_now())
        self.assertEqual(ma.get_state("i1")["stage"], ma.Stage.DISQUALIFIED)

    def test_paused_reply_still_writes_engagement_marker_and_keeps_all_tags(self):
        g = FakeGHL("i2", tags=["ai-outreach-sent", "appointment booked", "ai-paused"])
        g.install(); self.seed_conversation("i2")
        self.inbound("i2", "quick question", tags=g.contact["tags"])
        self.assertIn("ai-engaged", g.tags_now())            # nurture-cancel marker, as before
        for t in ("ai-outreach-sent", "appointment booked", "ai-paused"):
            self.assertIn(t, g.tags_now())
        self.assertNotIn("solar-dnc", g.tags_now())          # pause is never DNC
        self.assertEqual(g.contact["dndSettings"]["SMS"]["status"], "inactive")

    def test_unpaused_solar_dnc_behaviour_identical_with_flag_on_and_off(self):
        for flag in (True, False):
            with self.subTest(flag=flag):
                ma.AI_PAUSE_ENABLED = flag
                cid = f"i3{int(flag)}"
                g = FakeGHL(cid, tags=["solar-dnc"]); g.install(); self.seed_conversation(cid)
                self.inbound(cid, "hello", tags=g.contact["tags"])
                self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
