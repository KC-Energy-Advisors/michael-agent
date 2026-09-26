"""
[REOFFER-1 / PB-2] Booking-link re-offer, scheduling-promise backstop and the
install-time answer - offline tests.

Reuses the AI-pause harness: the REAL michael_agent / inbound_webhook /
send_sms_via_ghl code against an in-memory fake of GHL and a stubbed Claude.
Nothing leaves this process: no SMS, no GHL, no Anthropic.

Run:  python -m unittest test_booking_link_reoffer -v
"""

import os
import unittest

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")

import michael_agent as ma
from test_ai_pause import FakeGHL, StubClaude, PauseCase, payload, Req, resp_json, run

LINK = ma.BOOKING_LINK
QUALIFIED_TAGS = ["ai-engaged", "ai-outreach-sent", "booking_link_sent", "qualified"]
NO_LINK_PREV = "Totally fair question. Panels usually come with a 25-year warranty."
LINK_PREV = f"Here's the calendar - pick whatever time works best.\n{LINK}"


def link_count(text):
    return (text or "").count(LINK)


class ReofferCase(PauseCase):
    """PauseCase plus an event recorder and the re-offer kill switch."""

    def setUp(self):
        super().setUp()
        self._saved_reoffer = ma.BOOKING_REOFFER_ENABLED
        self._saved_ev = ma.ev
        self._saved_key = (ma.GHL_API_KEY, ma.GHL_LOCATION_ID)
        ma.BOOKING_REOFFER_ENABLED = True
        # booked_verdict() fails closed without a key, which would suppress
        # every BOOKING_PITCH send. A dummy key lets it ask the FAKE GHL (every
        # HTTP call is intercepted) and get a determinate answer.
        ma.GHL_API_KEY, ma.GHL_LOCATION_ID = "test-key-fake-ghl", "LOC-TEST"
        self.events = []
        def _rec(event, contact_id="", **fields):
            self.events.append((event, fields))
            return self._saved_ev(event, contact_id, **fields)
        ma.ev = _rec
        self.addCleanup(self._restore_reoffer)

    def _restore_reoffer(self):
        ma.BOOKING_REOFFER_ENABLED = self._saved_reoffer
        ma.ev = self._saved_ev
        ma.GHL_API_KEY, ma.GHL_LOCATION_ID = self._saved_key

    def said(self, text):
        self.claude.text = text

    def event_names(self):
        return [e for e, _ in self.events]

    def seed_qualified(self, cid, prev=NO_LINK_PREV, stage=None, **extra):
        """Qualified lead, link sent earlier in the thread, NOT booked."""
        st = ma.get_state(cid)
        st.update({
            "stage": stage or ma.Stage.SEND_BOOKING, "phone": "+13145550123",
            "qualified": True, "location_confirmed": True, "homeowner": "yes",
            "q_homeowner": ma.QUAL_YES, "q_utility": ma.UTIL_AMEREN,
            "q_bill_100_plus": ma.QUAL_YES,
            # Michael's most recent AUTOMATED outbound SMS (what send_sms_via_ghl records).
            "last_michael_outbound": prev,
            "messages": [
                {"role": "user", "content": "[Meta lead]"},
                {"role": "assistant", "content": LINK_PREV},
                {"role": "user", "content": "ok are the panels any good?"},
                {"role": "assistant", "content": prev},
            ]})
        st.update(extra)
        ma.save_state(cid, st)
        return st

    def seed_unqualified(self, cid, **extra):
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

    def agent(self, cid, text):
        st = ma.get_state(cid)
        return ma.michael_agent(cid, text, prior_outbound=ma.last_outbound_message(st))


# ═════════════════════════════════════════════════════════════════════════
#  The 12 planned tests
# ═════════════════════════════════════════════════════════════════════════
class P01_SendBookingStageTag(ReofferCase):
    def test_transition_with_tag_gets_link_once(self):
        self.seed_qualified("r1")
        self.said("Yes, we handle all the permits. Let me get you on the calendar. [SEND_BOOKING]")
        out = self.agent("r1", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)
        self.assertNotIn("[SEND_BOOKING]", out)
        self.assertTrue(out.startswith("Yes, we handle all the permits."), out)
        self.assertTrue(out.rstrip().endswith(LINK), out)          # link on its own last line
        self.assertIn("BOOKING_LINK_REOFFERED", self.event_names())
        self.assertEqual(ma.get_state("r1")["stage"], ma.Stage.SEND_BOOKING)

    def test_bare_tag_gets_fallback_line_plus_link(self):
        self.seed_qualified("r1b")
        self.said("[SEND_BOOKING]")
        out = self.agent("r1b", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)
        self.assertTrue(out.strip().split("\n")[0], out)            # never an empty lead-in


class P02_BookedNeverGetsLink(ReofferCase):
    def test_appointment_booked(self):
        self.seed_qualified("r2", stage=ma.Stage.BOOKED, appointment_booked=True)
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        out = self.agent("r2", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 0, out)
        self.assertNotIn("BOOKING_LINK_REOFFERED", self.event_names())

    def test_booked_flag_with_stale_send_booking_stage(self):
        self.seed_qualified("r2b", appointment_booked=True)        # stage still SEND_BOOKING
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        out = self.agent("r2b", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 0, out)

    def test_eligibility_helper_refuses_booked(self):
        self.assertEqual(ma.booking_link_reoffer_eligible(
            {"stage": ma.Stage.SEND_BOOKING, "appointment_booked": True, "qualified": True}),
            (False, "booked"))


class P03_BackstopQualified(ReofferCase):
    def test_promise_without_tag_at_send_booking(self):
        self.seed_qualified("r3")
        self.said("Good question. Let's grab a time to come take a look.")
        out = self.agent("r3", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)
        self.assertIn("BOOKING_LINK_REOFFERED", self.event_names())

    def test_qualified_record_before_send_booking_stage_moves_stage(self):
        self.seed_qualified("r3b", stage=ma.Stage.ASK_BILL)
        self.said("Perfect, let me get you on the calendar.")
        out = self.agent("r3b", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)
        self.assertEqual(ma.get_state("r3b")["stage"], ma.Stage.SEND_BOOKING)


class P04_UnqualifiedPromiseLoggedOnly(ReofferCase):
    def test_no_link_for_unqualified(self):
        self.seed_unqualified("r4")
        self.said("Sure thing. Let me get you on the calendar.")
        out = self.agent("r4", "is this legit?")
        self.assertEqual(link_count(out), 0, out)
        self.assertIn("SCHEDULING_PROMISE_NO_LINK", self.event_names())
        self.assertNotEqual(ma.get_state("r4")["stage"], ma.Stage.SEND_BOOKING)


class P05_HardStopsWin(ReofferCase):
    def test_criterion_failed_gets_no_link(self):
        self.seed_qualified("r5", stage=ma.Stage.ASK_BILL, qualified=False,
                            q_homeowner=ma.QUAL_NO, homeowner="no")
        self.said("Let's find a time to come take a look.")
        out = self.agent("r5", "is this legit?")
        self.assertEqual(link_count(out or ""), 0, out)

    def test_disqualify_tag_in_same_reply(self):
        self.seed_unqualified("r5b")
        self.said("Renters can't go solar, but let's find a time if that changes. [DISQUALIFY:NOT_OWNER]")
        out = self.agent("r5b", "I rent")
        self.assertEqual(link_count(out or ""), 0, out)

    def test_dnc_stage_is_silent(self):
        self.seed_qualified("r5c", stage=ma.Stage.DNC)
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        self.assertIsNone(self.agent("r5c", "hello?"))

    def test_disqualified_stage_is_silent(self):
        self.seed_qualified("r5d", stage=ma.Stage.DISQUALIFIED)
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        self.assertIsNone(self.agent("r5d", "hello?"))

    def test_ai_paused_end_to_end(self):
        g = FakeGHL("r5e", tags=QUALIFIED_TAGS + ["ai-paused"]); g.install()
        self.seed_qualified("r5e")
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        out = self.inbound("r5e", "do you guys handle the permits?", tags=g.contact["tags"])
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(self.claude.calls, []); self.assertEqual(g.sms, [])


class P06_NeverTwoLinks(ReofferCase):
    def test_reply_already_has_link(self):
        self.seed_qualified("r6")
        self.said(f"Let me get you on the calendar.\n{LINK}")
        out = self.agent("r6", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)

    def test_tag_plus_typed_link(self):
        self.seed_qualified("r6b")
        self.said(f"Let me get you on the calendar. {LINK} [SEND_BOOKING]")
        out = self.agent("r6b", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)

    def test_raw_leadconnector_url_counts_as_a_link(self):
        self.assertTrue(ma.contains_booking_link(
            "book here https://api.leadconnectorhq.com/widget/booking/abc"))


class P07_PreviousOutboundThrottle(ReofferCase):
    def test_previous_outbound_had_link_suppresses(self):
        self.seed_qualified("r7", prev=LINK_PREV)
        self.said("Yes, we handle permits. Let me get you on the calendar. [SEND_BOOKING]")
        out = self.agent("r7", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 0, out)
        self.assertTrue(out.strip(), out)
        self.assertIn("BOOKING_LINK_REOFFER_SUPPRESSED", self.event_names())

    def test_bare_tag_with_previous_link_points_back(self):
        self.seed_qualified("r7b", prev=LINK_PREV)
        self.said("[SEND_BOOKING]")
        out = self.agent("r7b", "do you guys handle the permits?")
        self.assertEqual(out, ma._REOFFER_POINT_BACK_LINE)

    def test_manual_ghl_message_with_link_does_not_suppress(self):
        # Owner pasted the link manually in GHL after Michael's no-link reply.
        # Manual sends are not Michael's previous message -> link still goes out.
        st = self.seed_qualified("r7d")
        st["messages"].append({"role": "assistant", "content": LINK_PREV})  # e.g. rebuilt from GHL
        self.said("Let's get you a time set.")
        out = ma.michael_agent("r7d", "do you guys handle the permits?", prior_outbound=LINK_PREV)
        self.assertEqual(link_count(out), 1, out)

    def test_manual_message_without_link_does_not_unsuppress(self):
        # Michael's last automated SMS carried the link; a later manual text did not.
        self.seed_qualified("r7e", prev=LINK_PREV)
        self.said("Let's get you a time set.")
        out = ma.michael_agent("r7e", "do you guys handle the permits?",
                               prior_outbound="Hey, this is the owner - happy to help.")
        self.assertEqual(link_count(out), 0, out)

    def test_unknown_after_restart_sends_link(self):
        st = self.seed_qualified("r7f", prev=LINK_PREV)
        st.pop("last_michael_outbound")          # process restarted: not known
        self.said("Let's get you a time set.")
        out = self.agent("r7f", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)

    def test_link_earlier_in_thread_is_not_a_reason_to_withhold(self):
        # LINK_PREV sits two messages back; the IMMEDIATELY previous one has no link.
        st = self.seed_qualified("r7c")
        self.assertIn(LINK, st["messages"][1]["content"])
        self.said("Let's get you a time set.")
        out = self.agent("r7c", "do you guys handle the permits?")
        self.assertEqual(link_count(out), 1, out)


class P08_InstallTimeEligible(ReofferCase):
    def test_owner_answer_plus_link_without_claude(self):
        self.seed_qualified("r8")
        out = self.agent("r8", "How long does the installation take?")
        self.assertEqual(self.claude.calls, [])
        self.assertEqual(out, f"{ma.INSTALL_TIME_ANSWER}\n{LINK}")
        self.assertTrue(out.startswith("Most installations get knocked out in one day. "
                                       "Depending on the size of the system, sometimes two."))
        self.assertIn("INSTALL_TIME_ANSWERED", self.event_names())

    def test_variants_detected(self):
        for q in ("how long does install take", "How long will it take to install?",
                  "how many days to put the panels up?", "how long is the install"):
            self.assertTrue(ma.is_install_time_question(q), q)

    def test_whole_project_questions_go_to_claude(self):
        for q in ("how long does the whole process take to install and turn on?",
                  "how long until it's installed and approved?",
                  "how long does it take?"):
            self.assertFalse(ma.is_install_time_question(q), q)

    def test_previous_link_drops_link_but_keeps_answer(self):
        self.seed_qualified("r8b", prev=LINK_PREV)
        out = self.agent("r8b", "how long does install take?")
        self.assertEqual(out, ma.INSTALL_TIME_ANSWER)


class P09_InstallTimeNotEligible(ReofferCase):
    def test_unqualified_goes_to_claude_no_link(self):
        self.seed_unqualified("r9")
        self.said("Most installs are done in a day. Do you own the home?")
        out = self.agent("r9", "how long does install take?")
        self.assertEqual(len(self.claude.calls), 1)
        self.assertEqual(link_count(out), 0, out)

    def test_prompt_separates_install_from_project_timeline(self):
        st = self.seed_unqualified("r9b")
        prompt = ma.build_system_prompt(st)
        self.assertIn("INSTALL DAY VS. PROJECT TIMELINE", prompt)
        self.assertIn("most are done in one day; a bigger system sometimes takes two", prompt)
        self.assertIn("Permits + Ameren interconnection run about 3–4", prompt)


class P10_NegationsAndFalsePositives(ReofferCase):
    def test_link_refusals_unchanged(self):
        for t in ("don't send me the link", "stop sending links", "no more links please"):
            self.assertFalse(ma.is_booking_link_request(t), t)

    def test_non_scheduling_replies_not_matched(self):
        for t in ("Got it. Do you own the home?",
                  "At that rate, the math is harder to make work. I'll keep your info on file.",
                  "Panels are warrantied for 25 years.",
                  "You've been unsubscribed. You won't hear from us again."):
            self.assertFalse(ma.is_scheduling_promise(t), t)

    def test_scheduling_replies_matched(self):
        for t in ("Let me get you on the calendar.", "Let's grab a time.",
                  "Let's get you a time set to show you exactly what it would look like.",
                  "Got it - let's get you set up for an in-home review.",
                  "Sounds good - let's find a time to come take a look."):
            self.assertTrue(ma.is_scheduling_promise(t), t)


class P11_Gsm7AndLength(ReofferCase):
    def _assert_sms_ok(self, text):
        self.assertFalse({c for c in text if c not in ma._GSM7_CHARSET}, text)
        self.assertLessEqual(len(text), 3 * 153, len(text))

    def test_install_answer(self):
        self.seed_qualified("r11")
        self._assert_sms_ok(self.agent("r11", "how long does install take?"))

    def test_reoffer_with_unicode_dash(self):
        self.seed_qualified("r11b")
        self.said("Yes — we pull every permit. Let’s get you a time set. [SEND_BOOKING]")
        out = self.agent("r11b", "do you guys handle the permits?")
        self._assert_sms_ok(out)
        self.assertEqual(link_count(out), 1)


class P12_KillSwitch(ReofferCase):
    def setUp(self):
        super().setUp()
        ma.BOOKING_REOFFER_ENABLED = False

    def test_send_booking_stage_tag_is_suppressed_as_before(self):
        self.seed_qualified("r12")
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        out = self.agent("r12", "do you guys handle the permits?")
        self.assertEqual(out, "Let me get you on the calendar.")

    def test_backstop_off(self):
        self.seed_qualified("r12b")
        self.said("Let's grab a time.")
        self.assertEqual(self.agent("r12b", "do you guys handle the permits?"), "Let's grab a time.")

    def test_install_question_goes_to_claude(self):
        self.seed_qualified("r12c")
        self.said("It's usually a day.")
        self.agent("r12c", "how long does install take?")
        self.assertEqual(len(self.claude.calls), 1)

    def test_prompt_reverts_to_previous_goal(self):
        st = self.seed_qualified("r12d")
        prompt = ma.build_system_prompt(st)
        self.assertIn("the booking link is sent automatically", prompt)
        self.assertNotIn("SCHEDULING LANGUAGE AND THE LINK TRAVEL TOGETHER", prompt)


# ═════════════════════════════════════════════════════════════════════════
#  Prompt / code alignment
# ═════════════════════════════════════════════════════════════════════════
class A01_PromptAlignment(ReofferCase):
    def test_send_booking_goal_matches_code(self):
        st = self.seed_qualified("a1")
        prompt = ma.build_system_prompt(st)
        self.assertIn("code appends the booking link to this same message", prompt)
        self.assertIn("SCHEDULING LANGUAGE AND THE LINK TRAVEL TOGETHER", prompt)
        # The contradictory stage goal is gone at SEND_BOOKING.
        self.assertNotIn("BOOKING — lead is FULLY QUALIFIED.", prompt)


# ═════════════════════════════════════════════════════════════════════════
#  End-to-end regressions (real inbound_webhook, fake GHL)
# ═════════════════════════════════════════════════════════════════════════
class E01_QualifiedUnbookedReofferEndToEnd(ReofferCase):
    def test_sms_carries_link(self):
        g = FakeGHL("e1", tags=QUALIFIED_TAGS); g.install()
        self.seed_qualified("e1")
        self.said("Yes, we handle permits. Let me get you on the calendar. [SEND_BOOKING]")
        out = self.inbound("e1", "do you guys handle the permits?", tags=g.contact["tags"])
        self.assertEqual(len(g.sms), 1, out)
        self.assertEqual(link_count(g.sms[0]), 1, g.sms[0])


class E02_ImmediatelyPreviousSuppressionEndToEnd(ReofferCase):
    def test_back_to_back_then_reoffer(self):
        g = FakeGHL("e2", tags=QUALIFIED_TAGS); g.install()
        self.seed_qualified("e2")
        self.said("Yes, we pull the permits. Let me get you on the calendar. [SEND_BOOKING]")
        self.inbound("e2", "do you guys handle the permits?", tags=g.contact["tags"])
        self.said("Yes, all of it. Let's grab a time. [SEND_BOOKING]")
        self.inbound("e2", "what about the inspection?", tags=g.contact["tags"])
        self.said("Good question - it's covered too. Let's get you a time set. [SEND_BOOKING]")
        self.inbound("e2", "and is the warranty transferable?", tags=g.contact["tags"])
        # 2nd follows a link -> withheld; 3rd follows a no-link message -> link returns.
        self.assertEqual([link_count(s) for s in g.sms], [1, 0, 1], g.sms)


class E02b_ManualSendNeverCountsEndToEnd(ReofferCase):
    def test_only_accepted_michael_sends_are_recorded(self):
        g = FakeGHL("e2b", tags=QUALIFIED_TAGS); g.install()
        self.seed_qualified("e2b")
        self.said("Yes, we pull the permits. Let me get you on the calendar. [SEND_BOOKING]")
        self.inbound("e2b", "do you guys handle the permits?", tags=g.contact["tags"])
        self.assertEqual(link_count(ma.get_state("e2b")["last_michael_outbound"]), 1)
        g.owner_texts_manually("Hi, this is the owner - happy to answer anything.")
        # Manual message did not replace Michael's record -> next promise is suppressed.
        self.said("Sure - let's grab a time. [SEND_BOOKING]")
        self.inbound("e2b", "what about the inspection?", tags=g.contact["tags"])
        self.assertEqual([link_count(s) for s in g.sms], [1, 0], g.sms)


class E03_BookedLockoutEndToEnd(ReofferCase):
    def test_booked_contact_gets_no_link(self):
        g = FakeGHL("e3", tags=["appointment booked", "ai-engaged"],
                    appointments=[{"id": "A1", "appointmentStatus": "confirmed",
                                   "startTime": "2099-01-01T15:00:00Z"}])
        g.install()
        self.seed_qualified("e3", stage=ma.Stage.BOOKED, appointment_booked=True)
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        self.inbound("e3", "do you guys handle the permits?", tags=g.contact["tags"])
        self.assertTrue(all(link_count(s) == 0 for s in g.sms), g.sms)


class E04_DncAndStopEndToEnd(ReofferCase):
    def test_stop_from_qualified_lead(self):
        g = FakeGHL("e4", tags=QUALIFIED_TAGS); g.install()
        self.seed_qualified("e4")
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        self.inbound("e4", "STOP", tags=g.contact["tags"])
        self.assertEqual(self.claude.calls, [])
        self.assertEqual(len(g.sms), 1)
        self.assertIn("unsubscribed", g.sms[0].lower())
        self.assertEqual(link_count(g.sms[0]), 0)
        self.assertEqual(ma.get_state("e4")["stage"], ma.Stage.DNC)

    def test_dnc_tagged_contact_silent(self):
        g = FakeGHL("e4b", tags=QUALIFIED_TAGS + ["solar-dnc", "dnc"]); g.install()
        self.seed_qualified("e4b")
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        self.inbound("e4b", "how long does install take?", tags=g.contact["tags"])
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])


class E05_AiPausedInstallQuestionEndToEnd(ReofferCase):
    def test_paused_install_question_is_silent(self):
        g = FakeGHL("e5", tags=QUALIFIED_TAGS + ["ai-paused"]); g.install()
        self.seed_qualified("e5")
        out = self.inbound("e5", "how long does install take?", tags=g.contact["tags"])
        self.assertEqual(out["reason"], "ai_paused")
        self.assertEqual(g.sms, []); self.assertEqual(self.claude.calls, [])


class E06_DuplicateEventIdEndToEnd(ReofferCase):
    def test_retried_event_sends_one_link(self):
        g = FakeGHL("e6", tags=QUALIFIED_TAGS); g.install()
        self.seed_qualified("e6")
        self.said("Let me get you on the calendar. [SEND_BOOKING]")
        p = payload("e6", "do you guys handle the permits?", tags=g.contact["tags"], evt="EVT-E6")
        ma._contact_last_processed_ts.pop("e6", None)
        run(ma.inbound_webhook(Req(p)))
        ma._contact_last_processed_ts.pop("e6", None)
        retry = resp_json(run(ma.inbound_webhook(Req(p))))
        self.assertEqual(retry.get("reason"), "duplicate_event_id", retry)
        self.assertEqual(len(g.sms), 1, g.sms)
        self.assertEqual(link_count(g.sms[0]), 1)


class E07_InstallAnswerEndToEnd(ReofferCase):
    def test_qualified_install_question_sms(self):
        g = FakeGHL("e7", tags=QUALIFIED_TAGS); g.install()
        self.seed_qualified("e7")
        self.inbound("e7", "How long does the installation take?", tags=g.contact["tags"])
        self.assertEqual(len(g.sms), 1)
        self.assertTrue(g.sms[0].startswith("Most installations get knocked out in one day."))
        self.assertEqual(link_count(g.sms[0]), 1)
        self.assertEqual(self.claude.calls, [])


if __name__ == "__main__":
    unittest.main()
