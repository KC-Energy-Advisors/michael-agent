"""
Regression tests for the conversion intent engine  [CONVERT-1]

    python -m unittest test_conversion_intent -v
    python test_conversion_intent.py

stdlib only — no pytest, no network, no Anthropic key. Every Claude call is
replaced by a stub, so a failure here is always a logic failure.

THE BUG THESE EXIST FOR
    Outbound (GHL nurture workflow):
      "Hey Susan, I'll leave you alone after this lol. Did you still want me
       to put together the numbers for your home, or should I close this out?"
    Inbound:
      "Yes please"
    Reply we sent:
      "Are you the homeowner?"

    A "yes" means whatever the previous outbound message asked. These tests
    pin down both halves of that: the conversion cases must convert, and the
    qualification cases must NOT be swept into the booking path with them.
"""

import asyncio
import os
import unittest

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")

import michael_agent as ma


# ── Claude stub ──────────────────────────────────────────────────────────
# Records every call so a test can assert which system prompt was used —
# that is how "did this go through the qualification prompt?" is checked.

class _StubBlock:
    def __init__(self, text):
        self.text = text


class _StubResponse:
    def __init__(self, text):
        self.content = [_StubBlock(text)]


class _StubMessages:
    def __init__(self, owner):
        self._owner = owner

    def create(self, **kwargs):
        self._owner.calls.append(kwargs)
        if self._owner.raises:
            raise RuntimeError("stub: Claude unavailable")
        return _StubResponse(self._owner.reply_text)


class StubClaude:
    def __init__(self, reply_text="Got it. Are you the homeowner?", raises=False):
        self.reply_text = reply_text
        self.raises     = raises
        self.calls      = []
        self.messages   = _StubMessages(self)

    @property
    def systems(self):
        return [c.get("system", "") for c in self.calls]


NURTURE_BREAKUP = (
    "Hey Susan, I'll leave you alone after this lol. Did you still want me to "
    "put together the numbers for your home, or should I close this out?"
)
UTILITY_Q   = "Are you currently with Ameren Missouri?"
OWNERSHIP_Q = "Are you the homeowner?"
SHOW_Q      = "Want me to show you what your home could qualify for?"
TIME_Q      = "Want me to send over a time to go through it?"


class ConversionTestCase(unittest.TestCase):
    """Shared fixture: clean state store and a stubbed Claude per test."""

    def setUp(self):
        ma._state_store.clear()
        self._real_claude = ma.claude
        self.claude = StubClaude()
        ma.claude = self.claude
        self.addCleanup(self._restore)

    def _restore(self):
        ma.claude = self._real_claude
        ma._state_store.clear()

    def make_lead(self, contact_id, *, stage=None, prior_outbound="", **fields):
        """A lead mid-conversation, shaped the way a restored contact looks."""
        state = ma.get_state(contact_id)
        state["stage"]              = stage or ma.Stage.ASK_OWNERSHIP
        state["contact_name"]       = fields.pop("contact_name", "Susan Miller")
        state["phone"]              = "+13145550123"
        state["location_confirmed"] = fields.pop("location_confirmed", True)
        state.update(fields)
        if prior_outbound:
            state["messages"] = [
                {"role": "user",      "content": "Hi"},
                {"role": "assistant", "content": prior_outbound},
            ]
        ma.save_state(contact_id, state)
        return state


# ═════════════════════════════════════════════════════════════════════════
#  Classifier units — the two halves the decision rests on
# ═════════════════════════════════════════════════════════════════════════

class TestOutboundClassifier(unittest.TestCase):

    def test_nurture_breakup_is_a_conversion_invitation(self):
        self.assertEqual(ma.classify_outbound_intent(NURTURE_BREAKUP),
                         ma.OUT_CONVERSION_INV)

    def test_conversion_invitation_phrasings(self):
        for text in (
            "Want me to put together the numbers?",
            "Want to see what your home could qualify for?",
            "Want me to show you what this could look like?",
            "Want to go over the numbers?",
            "Would you like to take a look?",
            "Did you still want me to put this together?",
            "Should we take a look at it?",
            "Should I close this out?",
            TIME_Q,
            SHOW_Q,
        ):
            with self.subTest(text=text):
                self.assertEqual(ma.classify_outbound_intent(text),
                                 ma.OUT_CONVERSION_INV)

    def test_qualification_questions_are_not_invitations(self):
        cases = {
            OWNERSHIP_Q: ma.OUT_OWNERSHIP_Q,
            "Are you the homeowner?": ma.OUT_OWNERSHIP_Q,
            "Do you own the home?": ma.OUT_OWNERSHIP_Q,
            UTILITY_Q: ma.OUT_UTILITY_Q,
            "Quick question: are you on Ameren Missouri for electric?": ma.OUT_UTILITY_Q,
            "About what does the Ameren bill usually run you?": ma.OUT_UTILITY_Q,
            "About what does the monthly bill usually run you?": ma.OUT_BILL_Q,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(ma.classify_outbound_intent(text), expected)

    def test_booking_pitch_recognised_by_its_link(self):
        self.assertEqual(
            ma.classify_outbound_intent(ma.build_booking_message()),
            ma.OUT_BOOKING_PITCH,
        )

    def test_no_text_is_unknown_not_guessed(self):
        self.assertEqual(ma.classify_outbound_intent(""), ma.OUT_UNKNOWN)
        self.assertEqual(ma.classify_outbound_intent(None), ma.OUT_UNKNOWN)


class TestAffirmativeDetection(unittest.TestCase):

    def test_natural_affirmatives(self):
        for text in (
            "yes", "Yes please", "yes please!", "yeah", "yep", "Yup", "sure",
            "Sure thing", "absolutely", "Absolutely.", "I do", "sounds good",
            "okay", "ok", "OK!", "let's do it", "lets do it", "interested",
            "send it", "send it over", "works for me", "that works",
            "definitely", "please do", "go ahead", "for sure", "why not",
            "of course", "alright", "perfect", "cool", "yes sir", "i'm in",
        ):
            with self.subTest(text=text):
                self.assertTrue(ma.is_affirmative_reply(text), text)

    def test_negatives_and_qualified_yeses_rejected(self):
        for text in (
            "no", "nope", "no thanks", "not interested", "not right now",
            "maybe later", "yes but I'm renting", "yes, though I don't own it",
            "I already have solar", "stop", "I'm too busy right now",
            "yes if the price is right and my roof can handle another 20 years",
        ):
            with self.subTest(text=text):
                self.assertFalse(ma.is_affirmative_reply(text), text)

    def test_substantive_messages_are_not_bare_affirmatives(self):
        for text in (
            "How much does it cost?",
            "My bill is about $210 a month",
            "What happens at the appointment?",
        ):
            with self.subTest(text=text):
                self.assertFalse(ma.is_affirmative_reply(text), text)


class TestSchedulingRequests(unittest.TestCase):

    def test_explicit_call_and_schedule_requests(self):
        for text in (
            "call me", "Can you call", "can you just call me?",
            "give me a call", "I'm free now", "when can we talk",
            "let's talk", "send me your calendar", "how do I schedule",
            "what times do you have", "when are you available",
            "book a time", "schedule me",
        ):
            with self.subTest(text=text):
                self.assertTrue(ma.is_explicit_scheduling_request(text), text)

    def test_ordinary_messages_are_not_scheduling_requests(self):
        for text in ("yes", "how much is it?", "I'm on Ameren", "not interested"):
            with self.subTest(text=text):
                self.assertFalse(ma.is_explicit_scheduling_request(text), text)


# ═════════════════════════════════════════════════════════════════════════
#  TEST 1 — the Susan case
# ═════════════════════════════════════════════════════════════════════════

class Test1_HighIntentAfterBreakupText(ConversionTestCase):
    """
    Outbound: "Did you still want me to put together the numbers for your
              home, or should I close this out?"
    Inbound:  "Yes please"
    Expected: booking-oriented reply. Must NOT ask "Are you the homeowner?".
    """

    def _run_susan(self, *, prior_in_memory):
        cid = "susan-1"
        # The nurture text came from a GHL workflow, so in production it is
        # NOT in local memory. Both variants must behave identically.
        self.make_lead(
            cid,
            stage=ma.Stage.ASK_OWNERSHIP,
            homeowner=None,
            prior_outbound=NURTURE_BREAKUP if prior_in_memory else "",
        )
        self.claude.reply_text = "Yep absolutely. Grab whatever time works best for you:"
        return cid, ma.michael_agent(
            cid, "Yes please",
            prior_outbound="" if prior_in_memory else NURTURE_BREAKUP,
        )

    def test_reply_is_booking_oriented(self):
        _, reply = self._run_susan(prior_in_memory=True)
        self.assertIsNotNone(reply)
        self.assertIn(ma.BOOKING_LINK, reply)

    def test_reply_does_not_ask_about_ownership(self):
        _, reply = self._run_susan(prior_in_memory=True)
        self.assertNotIn("homeowner", reply.lower())
        self.assertNotIn("do you own", reply.lower())

    def test_qualification_prompt_is_never_built(self):
        self._run_susan(prior_in_memory=True)
        for system in self.claude.systems:
            self.assertNotIn("QUALIFICATION CRITERIA", system)
            self.assertNotIn("CURRENT GOAL: ASK OWNERSHIP", system)

    def test_works_when_the_nurture_text_is_only_known_from_ghl(self):
        _, reply = self._run_susan(prior_in_memory=False)
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertNotIn("homeowner", reply.lower())

    def test_stage_advances_to_send_booking(self):
        cid, _ = self._run_susan(prior_in_memory=True)
        self.assertEqual(ma.get_state(cid)["stage"], ma.Stage.SEND_BOOKING)

    def test_she_is_not_marked_qualified_by_a_link_going_out(self):
        # [BUNDLE-2] She said yes to an invitation. That is high intent, not a
        # statement that she owns the home — the criteria travelled with the
        # link for her to self-qualify against.
        cid, _ = self._run_susan(prior_in_memory=True)
        self.assertFalse(ma.get_state(cid)["qualified"])

    def test_decision_is_logged_as_high_intent(self):
        state = ma.get_state("susan-decision")
        decision = ma.decide_conversion_action(state, "Yes please",
                                               prior_outbound=NURTURE_BREAKUP)
        self.assertEqual(decision.intent, ma.IN_HIGH_INTENT)
        self.assertEqual(decision.previous_outbound_type, ma.OUT_CONVERSION_INV)
        self.assertEqual(decision.action, ma.ACT_SEND_BOOKING)

    def test_a_claude_outage_still_produces_a_booking_reply(self):
        cid = "susan-outage"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP, homeowner=None)
        self.claude.raises = True
        reply = ma.michael_agent(cid, "Yes please")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertNotIn("homeowner", reply.lower())


# ═════════════════════════════════════════════════════════════════════════
#  TEST 2 — a "yes" to the utility question is NOT a conversion
# ═════════════════════════════════════════════════════════════════════════

class Test2_UtilityAffirmativeContinuesQualification(ConversionTestCase):
    """
    Outbound: "Are you currently with Ameren Missouri?"
    Inbound:  "Yes"
    Expected: qualification continues. No booking link just because the word
              "yes" was used.
    """

    def setUp(self):
        super().setUp()
        self.cid = "ameren-yes"
        self.make_lead(
            self.cid,
            stage=ma.Stage.ASK_LOCATION,
            location_confirmed=False,
            homeowner=None,
            prior_outbound=UTILITY_Q,
        )
        self.claude.reply_text = "Got it. Are you the homeowner?"
        self.reply = ma.michael_agent(self.cid, "Yes")

    def test_no_booking_link_is_sent(self):
        self.assertNotIn(ma.BOOKING_LINK, self.reply)

    def test_stage_does_not_jump_to_booking(self):
        self.assertNotEqual(ma.get_state(self.cid)["stage"], ma.Stage.SEND_BOOKING)

    def test_it_went_through_the_qualification_prompt(self):
        self.assertTrue(any("QUALIFICATION CRITERIA" in s for s in self.claude.systems))

    def test_decision_is_qualification_not_conversion(self):
        decision = ma.decide_conversion_action(
            ma.get_state("fresh"), "Yes", prior_outbound=UTILITY_Q)
        self.assertEqual(decision.intent, ma.IN_QUAL_AFFIRM)
        self.assertEqual(decision.previous_outbound_type, ma.OUT_UTILITY_Q)
        self.assertEqual(decision.action, ma.ACT_CONTINUE_QUAL)

    def test_one_yes_does_not_confirm_both_utility_and_ownership(self):
        # The pre-existing invariant: utility and ownership are separate
        # questions needing separate answers. The conversion step runs AFTER
        # update_state_from_inbound() precisely so it cannot disturb this —
        # setting location_confirmed first would change the _loc_conf_before
        # snapshot that _detect_homeowner() depends on.
        state = ma.get_state("invariant-check")
        state["stage"]              = ma.Stage.ASK_OWNERSHIP
        state["location_confirmed"] = False
        state["homeowner"]          = None
        ma.update_state_from_inbound(state, "Yes")
        decision = ma.decide_conversion_action(state, "Yes", prior_outbound=UTILITY_Q)

        self.assertEqual(decision.qualification_answer, "utility_yes")
        self.assertTrue(state["location_confirmed"])
        self.assertIsNone(
            state["homeowner"],
            "one 'yes' must not confirm the utility AND home ownership",
        )


# ═════════════════════════════════════════════════════════════════════════
#  TEST 3 — "Sure" to a conversion invitation
# ═════════════════════════════════════════════════════════════════════════

class Test3_SureToQualifyInvitation(ConversionTestCase):
    """
    Outbound: "Want me to show you what your home could qualify for?"
    Inbound:  "Sure"
    Expected: booking-oriented response.
    """

    def test_booking_link_is_sent(self):
        cid = "sure-lead"
        self.make_lead(cid, prior_outbound=SHOW_Q, homeowner=None)
        self.claude.reply_text = "Sounds good — pick a time that works for you:"
        reply = ma.michael_agent(cid, "Sure")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertNotIn("homeowner", reply.lower())

    def test_send_over_a_time_invitation_also_converts(self):
        cid = "time-lead"
        self.make_lead(cid, prior_outbound=TIME_Q, homeowner=None)
        self.claude.reply_text = "Will do — grab whatever time suits you:"
        reply = ma.michael_agent(cid, "Sure")
        self.assertIn(ma.BOOKING_LINK, reply)


# ═════════════════════════════════════════════════════════════════════════
#  TEST 4 — "Yes" to the ownership question continues, never restarts
# ═════════════════════════════════════════════════════════════════════════

class Test4_OwnershipAffirmativeContinuesFlow(ConversionTestCase):
    """
    Outbound: "Are you the homeowner?"
    Inbound:  "Yes"
    Expected: the flow continues naturally. Ownership is recorded, so the
              next turn moves on instead of asking again.
    """

    def _run(self, inbound):
        cid = f"owner-{inbound.replace(' ', '-')}"
        self.make_lead(cid, stage=ma.Stage.ASK_OWNERSHIP,
                       location_confirmed=True, homeowner=None,
                       prior_outbound=OWNERSHIP_Q)
        self.claude.reply_text = "Got it. About what does the Ameren bill usually run?"
        reply = ma.michael_agent(cid, inbound)
        return cid, reply

    def test_ownership_is_recorded(self):
        cid, _ = self._run("Yes")
        self.assertEqual(ma.get_state(cid)["homeowner"], "yes")

    def test_yes_please_is_also_understood(self):
        # The original bug in miniature: _BRIEF_YES is anchored and rejects
        # the trailing "please", so this used to record nothing at all.
        cid, _ = self._run("Yes please")
        self.assertEqual(ma.get_state(cid)["homeowner"], "yes")

    def test_qualification_is_not_restarted(self):
        cid, _ = self._run("Yes")
        goal = ma._goal_from_prompt(ma.build_system_prompt(ma.get_state(cid)))
        self.assertNotIn("ASK OWNERSHIP", goal)
        self.assertNotIn("ASK UTILITY", goal)

    def test_no_booking_link_yet(self):
        _, reply = self._run("Yes")
        self.assertNotIn(ma.BOOKING_LINK, reply)


# ═════════════════════════════════════════════════════════════════════════
#  TEST 5 — explicit call request
# ═════════════════════════════════════════════════════════════════════════

class Test5_ExplicitCallRequest(ConversionTestCase):
    """
    Inbound:  "Can you just call me?"
    Expected: move directly to the call/scheduling action — not a
              qualification question.
    """

    def test_call_request_goes_straight_to_scheduling(self):
        cid = "call-me"
        self.make_lead(cid, prior_outbound=OWNERSHIP_Q, homeowner=None)
        self.claude.reply_text = "Easiest way is to grab a time that works for you:"
        reply = ma.michael_agent(cid, "Can you just call me?")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertNotIn("homeowner", reply.lower())

    def test_intent_is_logged_as_an_explicit_scheduling_request(self):
        decision = ma.decide_conversion_action(
            ma.get_state("call-decide"), "Can you just call me?",
            prior_outbound=OWNERSHIP_Q)
        self.assertEqual(decision.intent, ma.IN_SCHEDULING)
        self.assertEqual(decision.action, ma.ACT_SEND_BOOKING)

    def test_a_call_request_outranks_the_pending_qualification_question(self):
        # Even mid-qualification, "call me" is the answer to what happens next.
        decision = ma.decide_conversion_action(
            ma.get_state("call-mid"), "when can we talk", prior_outbound=UTILITY_Q)
        self.assertEqual(decision.action, ma.ACT_SEND_BOOKING)


# ═════════════════════════════════════════════════════════════════════════
#  TEST 6 — booked lockout is untouched
# ═════════════════════════════════════════════════════════════════════════

class Test6_BookedLockoutStillWins(ConversionTestCase):
    """
    A booked lead sends an affirmative.
    Expected: the booked lockout still prevents booking-link messages.
    """

    def _booked(self, cid, **extra):
        state = self.make_lead(cid, stage=ma.Stage.BOOKED, **extra)
        state["appointment_booked"] = True
        ma.save_state(cid, state)
        return state

    def test_no_booking_link_for_a_booked_lead(self):
        cid = "booked-yes"
        self._booked(cid)
        self.claude.reply_text = "Sounds good, see you then."
        reply = ma.michael_agent(cid, "Yes please", prior_outbound=NURTURE_BREAKUP)
        self.assertIsNotNone(reply)
        self.assertNotIn(ma.BOOKING_LINK, reply)

    def test_no_booking_link_for_an_explicit_call_request_either(self):
        cid = "booked-call"
        self._booked(cid)
        self.claude.reply_text = "I'll see you at the appointment."
        reply = ma.michael_agent(cid, "Can you call me?", prior_outbound=NURTURE_BREAKUP)
        self.assertNotIn(ma.BOOKING_LINK, reply)

    def test_the_conversion_path_is_unreachable_when_booked(self):
        # The booked lockout returns before the conversion step, so the only
        # system prompt built is the booked follow-up one.
        cid = "booked-prompt"
        self._booked(cid)
        self.claude.reply_text = "See you then."
        ma.michael_agent(cid, "Yes please", prior_outbound=NURTURE_BREAKUP)
        for system in self.claude.systems:
            self.assertNotIn("WRITE ONE SHORT LEAD-IN LINE", system)
            self.assertNotIn("QUALIFICATION CRITERIA", system)

    def test_stale_stage_with_appointment_booked_still_locks_out(self):
        cid = "booked-stale"
        state = self.make_lead(cid, stage=ma.Stage.SEND_BOOKING)
        state["appointment_booked"] = True
        ma.save_state(cid, state)
        self.claude.reply_text = "Already got you down."
        reply = ma.michael_agent(cid, "Yes please", prior_outbound=NURTURE_BREAKUP)
        self.assertNotIn(ma.BOOKING_LINK, reply)

    def test_send_guard_suppresses_a_booking_pitch_when_ghl_says_booked(self):
        # The authoritative check, one layer below michael_agent(): even if a
        # booking pitch were somehow produced, it cannot leave the service.
        async def _booked_verdict(_cid):
            return True, "test:booked"

        real = ma.booked_verdict
        ma.booked_verdict = _booked_verdict
        try:
            result = asyncio.run(ma.send_sms_via_ghl(
                "booked-guard", "here is the link " + ma.BOOKING_LINK,
                to_number="+13145550123", kind=ma.SendKind.BOOKING_PITCH,
            ))
        finally:
            ma.booked_verdict = real

        self.assertEqual(result["status"], ma.SendStatus.SUPPRESSED.value)
        self.assertEqual(result["reason"], "booked_guard")

    def test_a_booking_stage_reply_is_classified_as_a_guarded_booking_pitch(self):
        state = ma.get_state("kind-check")
        state["stage"] = ma.Stage.SEND_BOOKING
        kind = ma.classify_reply_kind(state)
        self.assertEqual(kind, ma.SendKind.BOOKING_PITCH)
        self.assertIn(kind, ma._BOOKED_GUARDED)


# ═════════════════════════════════════════════════════════════════════════
#  TEST 7 — compliance is never overridden
# ═════════════════════════════════════════════════════════════════════════

class Test7_ComplianceOutranksEverything(ConversionTestCase):
    """
    Lead says STOP.
    Expected: no conversion logic runs at all.
    """

    def test_stop_produces_an_opt_out_and_no_link(self):
        cid = "stop-lead"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP)
        reply = ma.michael_agent(cid, "STOP", prior_outbound=NURTURE_BREAKUP)
        self.assertIn("unsubscribed", reply.lower())
        self.assertNotIn(ma.BOOKING_LINK, reply)
        self.assertEqual(ma.get_state(cid)["stage"], ma.Stage.DNC)

    def test_stop_never_calls_claude(self):
        cid = "stop-no-claude"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP)
        ma.michael_agent(cid, "STOP", prior_outbound=NURTURE_BREAKUP)
        self.assertEqual(self.claude.calls, [])

    def test_an_already_dnc_lead_stays_silent_after_an_affirmative(self):
        cid = "dnc-lead"
        self.make_lead(cid, stage=ma.Stage.DNC, prior_outbound=NURTURE_BREAKUP)
        self.assertIsNone(
            ma.michael_agent(cid, "Yes please", prior_outbound=NURTURE_BREAKUP))

    def test_stop_wins_even_when_wrapped_in_an_affirmative(self):
        cid = "stop-mixed"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP)
        reply = ma.michael_agent(cid, "yes please stop texting me",
                                 prior_outbound=NURTURE_BREAKUP)
        self.assertNotIn(ma.BOOKING_LINK, reply)
        self.assertEqual(ma.get_state(cid)["stage"], ma.Stage.DNC)


# ═════════════════════════════════════════════════════════════════════════
#  Surrounding protections that must not regress
# ═════════════════════════════════════════════════════════════════════════

class TestExistingProtections(ConversionTestCase):

    def test_disqualified_leads_stay_silent(self):
        cid = "dq-lead"
        self.make_lead(cid, stage=ma.Stage.DISQUALIFIED, prior_outbound=NURTURE_BREAKUP)
        self.assertIsNone(
            ma.michael_agent(cid, "Yes please", prior_outbound=NURTURE_BREAKUP))

    def test_daily_limit_still_blocks_the_conversion_path(self):
        cid = "limit-lead"
        state = self.make_lead(cid, prior_outbound=NURTURE_BREAKUP)
        state["msgs_today"]    = ma.MAX_DAILY_MSGS
        state["last_msg_date"] = ma.datetime.now(tz=ma.CENTRAL_TZ).strftime("%Y-%m-%d")
        ma.save_state(cid, state)
        self.assertIsNone(
            ma.michael_agent(cid, "Yes please", prior_outbound=NURTURE_BREAKUP))

    def test_the_model_can_never_inject_its_own_url(self):
        cid = "url-lead"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP, homeowner=None)
        self.claude.reply_text = (
            "Here you go: https://api.leadconnectorhq.com/widget/booking/evil"
        )
        reply = ma.michael_agent(cid, "Yes please")
        self.assertNotIn("leadconnectorhq.com", reply)
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertEqual(reply.count("http"), 1)

    def test_control_tags_never_leak_into_a_conversion_reply(self):
        cid = "tag-lead"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP, homeowner=None)
        self.claude.reply_text = "Sounds good: [SEND_BOOKING]"
        reply = ma.michael_agent(cid, "Yes please")
        self.assertNotIn("[SEND_BOOKING]", reply)
        self.assertNotIn("[", reply)

    def test_a_cost_question_still_gets_a_real_answer(self):
        # Objections must not be swallowed by the conversion path.
        cid = "cost-lead"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP, homeowner=None)
        reply = ma.michael_agent(cid, "How much does it cost?")
        self.assertIn("depends", reply.lower())

    def test_conversation_memory_records_both_sides(self):
        cid = "memory-lead"
        self.make_lead(cid, prior_outbound=NURTURE_BREAKUP, homeowner=None)
        self.claude.reply_text = "Grab a time that works:"
        reply = ma.michael_agent(cid, "Yes please")
        msgs = ma.get_state(cid)["messages"]
        self.assertEqual(msgs[-2], {"role": "user", "content": "Yes please"})
        self.assertEqual(msgs[-1], {"role": "assistant", "content": reply})

    def test_an_ambiguous_affirmative_with_no_history_does_not_convert(self):
        # After a restart with nothing recoverable from GHL, guessing is how a
        # utility "yes" would turn into a booking link. Fall back to the
        # normal flow instead.
        decision = ma.decide_conversion_action(ma.get_state("blank"), "yes")
        self.assertEqual(decision.intent, ma.IN_AMBIG_AFFIRM)
        self.assertEqual(decision.previous_outbound_type, ma.OUT_UNKNOWN)
        self.assertEqual(decision.action, ma.ACT_NO_OVERRIDE)

    def test_an_affirmative_from_an_already_qualified_lead_converts(self):
        state = ma.get_state("qualified-lead")
        state["stage"]     = ma.Stage.SEND_BOOKING
        state["qualified"] = True
        decision = ma.decide_conversion_action(state, "sounds good")
        self.assertEqual(decision.intent, ma.IN_HIGH_INTENT)
        self.assertEqual(decision.action, ma.ACT_SEND_BOOKING)


class TestPriorOutboundRecovery(unittest.TestCase):
    """fetch_last_outbound_message() must degrade to '' and never raise."""

    def test_missing_credentials_return_empty(self):
        real_key, real_loc = ma.GHL_API_KEY, ma.GHL_LOCATION_ID
        ma.GHL_API_KEY = ""
        try:
            self.assertEqual(asyncio.run(ma.fetch_last_outbound_message("c1")), "")
        finally:
            ma.GHL_API_KEY, ma.GHL_LOCATION_ID = real_key, real_loc

    def test_empty_contact_id_returns_empty(self):
        self.assertEqual(asyncio.run(ma.fetch_last_outbound_message("")), "")

    def test_newest_outbound_body_is_selected(self):
        messages = [
            {"direction": "inbound",  "body": "Yes please"},
            {"direction": "outbound", "body": NURTURE_BREAKUP},
            {"direction": "outbound", "body": "an older one"},
        ]
        self.assertEqual(ma._extract_outbound_body(messages), NURTURE_BREAKUP)

    def test_no_outbound_messages_returns_empty(self):
        self.assertEqual(ma._extract_outbound_body(
            [{"direction": "inbound", "body": "hi"}]), "")


# ═════════════════════════════════════════════════════════════════════════
#  BUNDLED / STATE-BASED QUALIFICATION  [BUNDLE-1]
#
#  Qualification is three independent tri-state facts, not a script:
#      q_homeowner      yes | no | unknown
#      q_utility        ameren | other | unknown
#      q_bill_100_plus  yes | no | unknown
#
#  The rule everything below turns on: a bare "yes" confirms all three ONLY
#  when the message it answers bundled all three. Otherwise it confirms
#  exactly what was asked, and nothing more.
# ═════════════════════════════════════════════════════════════════════════

BUNDLED_OFFER = (
    "If you own the home, have Ameren, and spend over $100/month on electric, "
    "I can show you what the $0-down option could look like."
)
BUNDLED_OFFER_2 = (
    "If you're the homeowner, have Ameren, and the bill is usually north of "
    "$100, I can put together what the $0-down solar numbers could look like."
)


class BundledTestCase(ConversionTestCase):
    """A lead whose three criteria all start unknown."""

    def make_unqualified_lead(self, contact_id, *, prior_outbound=BUNDLED_OFFER, **fields):
        state = self.make_lead(contact_id, prior_outbound=prior_outbound,
                               homeowner=None, **fields)
        state.setdefault("q_homeowner", ma.QUAL_UNKNOWN)
        for field, value in (("q_homeowner", ma.QUAL_UNKNOWN),
                             ("q_utility", ma.QUAL_UNKNOWN),
                             ("q_bill_100_plus", ma.QUAL_UNKNOWN)):
            state[field] = fields.get(field, value)
        ma.save_state(contact_id, state)
        return state

    def record(self, contact_id):
        s = ma.get_state(contact_id)
        return (s.get("q_homeowner"), s.get("q_utility"), s.get("q_bill_100_plus"))


class TestBundledOutboundClassification(unittest.TestCase):

    def test_a_bundled_offer_is_its_own_class(self):
        for text in (BUNDLED_OFFER, BUNDLED_OFFER_2):
            with self.subTest(text=text[:40]):
                self.assertEqual(ma.classify_outbound_intent(text), ma.OUT_BUNDLED_QUAL)

    def test_bundling_requires_conditional_framing_not_just_two_nouns(self):
        # Two criteria named, but it ASKS for one of them. A "yes" here can
        # only mean Ameren — misreading it as a bundle would invent two facts.
        text = "About what does the Ameren bill usually run you?"
        self.assertEqual(ma.bundled_criteria_mentioned(text), 2)
        self.assertFalse(ma.is_bundled_qualification_message(text))
        self.assertNotEqual(ma.classify_outbound_intent(text), ma.OUT_BUNDLED_QUAL)

    def test_a_single_condition_is_not_a_bundle(self):
        self.assertFalse(ma.is_bundled_qualification_message(
            "If you own the home, I can take a look."))

    def test_a_bundle_ending_in_an_invitation_still_reads_as_a_bundle(self):
        # Both readings are true; the bundled one is strictly more informative,
        # because a yes carries the facts as well as the interest.
        text = BUNDLED_OFFER + " Want me to put it together?"
        self.assertEqual(ma.classify_outbound_intent(text), ma.OUT_BUNDLED_QUAL)

    def test_the_booking_link_still_outranks_everything(self):
        text = BUNDLED_OFFER + "\n" + ma.BOOKING_LINK
        self.assertEqual(ma.classify_outbound_intent(text), ma.OUT_BOOKING_PITCH)


class TestFactExtraction(unittest.TestCase):
    """The natural-language half, in isolation."""

    def test_a_full_sentence_sets_every_field_it_states(self):
        facts = ma.extract_qualification_facts("I own the place and my Ameren bill is $190")
        self.assertEqual(facts[ma.Q_HOMEOWNER], ma.QUAL_YES)
        self.assertEqual(facts[ma.Q_UTILITY], ma.UTIL_AMEREN)
        self.assertEqual(facts[ma.Q_BILL], ma.QUAL_YES)

    def test_a_bill_figure_alone_says_nothing_about_the_other_two(self):
        facts = ma.extract_qualification_facts("My bill is around $160")
        self.assertEqual(facts[ma.Q_BILL], ma.QUAL_YES)
        self.assertNotIn(ma.Q_HOMEOWNER, facts)
        self.assertNotIn(ma.Q_UTILITY, facts)

    def test_a_low_bill_is_a_no_not_an_absence(self):
        facts = ma.extract_qualification_facts("it's like $70 a month")
        self.assertEqual(facts[ma.Q_BILL], ma.QUAL_NO)

    def test_renting_and_other_utilities(self):
        self.assertEqual(
            ma.extract_qualification_facts("No I rent")[ma.Q_HOMEOWNER], ma.QUAL_NO)
        self.assertEqual(
            ma.extract_qualification_facts("I use Cuivre River")[ma.Q_UTILITY], ma.UTIL_OTHER)

    def test_ameren_illinois_does_not_qualify(self):
        self.assertEqual(
            ma.extract_qualification_facts("I'm on Ameren Illinois")[ma.Q_UTILITY],
            ma.UTIL_OTHER)

    def test_a_bare_yes_to_a_bundle_confirms_all_three(self):
        facts = ma.extract_qualification_facts("Yes please", ma.OUT_BUNDLED_QUAL)
        self.assertEqual(facts[ma.Q_HOMEOWNER], ma.QUAL_YES)
        self.assertEqual(facts[ma.Q_UTILITY], ma.UTIL_AMEREN)
        self.assertEqual(facts[ma.Q_BILL], ma.QUAL_YES)

    def test_a_bare_yes_to_one_question_confirms_only_that_one(self):
        facts = ma.extract_qualification_facts("Yes", ma.OUT_UTILITY_Q)
        self.assertEqual(facts, {ma.Q_UTILITY: ma.UTIL_AMEREN, "_source": "utility_affirmative"})

    def test_a_bare_yes_to_an_invitation_confirms_no_facts_at_all(self):
        # Interest is not qualification. Booking them is fine; claiming to know
        # they own their home is not.
        facts = ma.extract_qualification_facts("Yes please", ma.OUT_CONVERSION_INV)
        self.assertNotIn(ma.Q_HOMEOWNER, facts)
        self.assertNotIn(ma.Q_UTILITY, facts)
        self.assertNotIn(ma.Q_BILL, facts)

    def test_a_bare_yes_with_no_known_question_confirms_nothing(self):
        self.assertEqual(ma.extract_qualification_facts("yes", ma.OUT_UNKNOWN), {})

    def test_blanket_confirmations_are_understood(self):
        for text in ("That's me", "All of those apply", "all of that applies",
                     "yes to all", "that's me!"):
            with self.subTest(text=text):
                self.assertTrue(ma.is_blanket_confirmation(text), text)
                facts = ma.extract_qualification_facts(text, ma.OUT_BUNDLED_QUAL)
                self.assertEqual(facts[ma.Q_UTILITY], ma.UTIL_AMEREN)

    def test_an_explicit_fact_overrides_the_blanket_yes(self):
        facts = ma.extract_qualification_facts(
            "Yes, but I have Cuivre River", ma.OUT_BUNDLED_QUAL)
        self.assertEqual(facts[ma.Q_UTILITY], ma.UTIL_OTHER)


class TestVerdict(unittest.TestCase):

    def _state(self, home, util, bill):
        s = ma.get_state(f"v-{home}-{util}-{bill}")
        s["q_homeowner"], s["q_utility"], s["q_bill_100_plus"] = home, util, bill
        return s

    def test_all_three_confirmed_is_qualified(self):
        v, missing = ma.qualification_verdict(
            self._state(ma.QUAL_YES, ma.UTIL_AMEREN, ma.QUAL_YES))
        self.assertEqual(v, ma.VERDICT_QUALIFIED)
        self.assertEqual(missing, [])

    def test_any_single_failure_disqualifies(self):
        for state in (
            self._state(ma.QUAL_NO, ma.UTIL_AMEREN, ma.QUAL_YES),
            self._state(ma.QUAL_YES, ma.UTIL_OTHER, ma.QUAL_YES),
            self._state(ma.QUAL_YES, ma.UTIL_AMEREN, ma.QUAL_NO),
        ):
            with self.subTest(state=ma.qualification_summary(state)):
                v, _ = ma.qualification_verdict(state)
                self.assertEqual(v, ma.VERDICT_DISQUALIFIED)

    def test_unknown_is_incomplete_not_failed(self):
        v, missing = ma.qualification_verdict(
            self._state(ma.QUAL_YES, ma.QUAL_UNKNOWN, ma.QUAL_YES))
        self.assertEqual(v, ma.VERDICT_INCOMPLETE)
        self.assertEqual(missing, [ma.Q_UTILITY])

    def test_a_fresh_contact_is_incomplete_on_all_three(self):
        v, missing = ma.qualification_verdict(ma.get_state("brand-new"))
        self.assertEqual(v, ma.VERDICT_INCOMPLETE)
        self.assertEqual(len(missing), 3)


# ═════════════════════════════════════════════════════════════════════════
#  TEST A — bundled offer, blanket yes
# ═════════════════════════════════════════════════════════════════════════

class TestA_BundledOfferBlanketYes(BundledTestCase):
    """
    AI:   "If you own the home, have Ameren, and spend over $100/month on
           electric, I can show you what the $0-down option could look like."
    Lead: "Yes please"
    Expected: all three confirmed, high intent, booking-oriented reply.
    """

    def setUp(self):
        super().setUp()
        self.cid = "bundle-yes"
        self.make_unqualified_lead(self.cid)
        self.claude.reply_text = "Perfect — grab whatever time works for you:"
        self.reply = ma.michael_agent(self.cid, "Yes please")

    def test_all_three_criteria_are_recorded(self):
        self.assertEqual(self.record(self.cid),
                         (ma.QUAL_YES, ma.UTIL_AMEREN, ma.QUAL_YES))

    def test_verdict_is_qualified(self):
        verdict, _ = ma.qualification_verdict(ma.get_state(self.cid))
        self.assertEqual(verdict, ma.VERDICT_QUALIFIED)

    def test_the_reply_carries_the_booking_link(self):
        self.assertIn(ma.BOOKING_LINK, self.reply)

    def test_it_does_not_re_ask_any_criterion(self):
        lowered = self.reply.lower()
        for phrase in ("homeowner", "do you own", "ameren", "bill"):
            self.assertNotIn(phrase, lowered)

    def test_the_decision_is_logged_as_a_bundled_affirmative(self):
        state = self.make_unqualified_lead("bundle-decide")
        ma.apply_qualification_facts(
            state, ma.extract_qualification_facts("Yes please", ma.OUT_BUNDLED_QUAL))
        decision = ma.decide_conversion_action(state, "Yes please",
                                               prior_outbound=BUNDLED_OFFER)
        self.assertEqual(decision.intent, ma.IN_BUNDLED_AFFIRM)
        self.assertEqual(decision.previous_outbound_type, ma.OUT_BUNDLED_QUAL)
        self.assertEqual(decision.action, ma.ACT_SEND_BOOKING)

    def test_blanket_phrasings_behave_identically(self):
        for i, text in enumerate(("That's me", "All of those apply", "Yeah definitely")):
            with self.subTest(text=text):
                cid = f"bundle-blanket-{i}"
                self.make_unqualified_lead(cid)
                self.claude.reply_text = "Sounds good — pick a time:"
                reply = ma.michael_agent(cid, text)
                self.assertEqual(self.record(cid),
                                 (ma.QUAL_YES, ma.UTIL_AMEREN, ma.QUAL_YES))
                self.assertIn(ma.BOOKING_LINK, reply)


# ═════════════════════════════════════════════════════════════════════════
#  TEST B — single question, single fact
# ═════════════════════════════════════════════════════════════════════════

class TestB_SingleQuestionConfirmsOneFact(BundledTestCase):
    """
    AI:   "Are you with Ameren?"
    Lead: "Yes"
    Expected: utility=ameren; homeowner and bill stay unknown.
    """

    def setUp(self):
        super().setUp()
        self.cid = "single-q"
        self.make_unqualified_lead(self.cid, prior_outbound=UTILITY_Q,
                                   stage=ma.Stage.ASK_LOCATION,
                                   location_confirmed=False)
        self.claude.reply_text = "Got it. Are you the homeowner?"
        self.reply = ma.michael_agent(self.cid, "Yes")

    def test_utility_is_confirmed(self):
        self.assertEqual(ma.get_state(self.cid)["q_utility"], ma.UTIL_AMEREN)

    def test_ownership_stays_unknown(self):
        self.assertEqual(ma.get_state(self.cid)["q_homeowner"], ma.QUAL_UNKNOWN)

    def test_bill_stays_unknown(self):
        self.assertEqual(ma.get_state(self.cid)["q_bill_100_plus"], ma.QUAL_UNKNOWN)

    def test_no_booking_link(self):
        self.assertNotIn(ma.BOOKING_LINK, self.reply)

    def test_verdict_is_incomplete_with_two_still_missing(self):
        verdict, missing = ma.qualification_verdict(ma.get_state(self.cid))
        self.assertEqual(verdict, ma.VERDICT_INCOMPLETE)
        self.assertEqual(sorted(missing), sorted([ma.Q_HOMEOWNER, ma.Q_BILL]))


# ═════════════════════════════════════════════════════════════════════════
#  TEST C — bundled offer, partial answer completing a known record
# ═════════════════════════════════════════════════════════════════════════

class TestC_PartialAnswerCompletesTheRecord(BundledTestCase):
    """
    AI:   bundled criteria
    Lead: "I own it and my bill is around $180."
    State already has utility=ameren.
    Expected: homeowner=yes, bill=yes, utility unchanged, move toward booking.
    """

    def setUp(self):
        super().setUp()
        self.cid = "bundle-partial"
        self.make_unqualified_lead(self.cid, q_utility=ma.UTIL_AMEREN)
        self.claude.reply_text = "Great — grab a time that works:"
        self.reply = ma.michael_agent(self.cid, "I own it and my bill is around $180.")

    def test_the_stated_facts_are_recorded(self):
        home, util, bill = self.record(self.cid)
        self.assertEqual(home, ma.QUAL_YES)
        self.assertEqual(bill, ma.QUAL_YES)

    def test_the_already_known_utility_is_untouched(self):
        self.assertEqual(ma.get_state(self.cid)["q_utility"], ma.UTIL_AMEREN)

    def test_it_moves_toward_booking(self):
        self.assertIn(ma.BOOKING_LINK, self.reply)
        self.assertEqual(ma.get_state(self.cid)["stage"], ma.Stage.SEND_BOOKING)

    def test_the_bill_amount_is_kept(self):
        self.assertEqual(ma.get_state(self.cid)["monthly_bill"], "$180/month")

    def test_it_never_re_asks_the_utility(self):
        self.assertNotIn("ameren", self.reply.lower())


class TestC2_OneCriterionLeftIsAskedAlone(BundledTestCase):
    """
    The case from the brief: two facts given, utility genuinely unknown.
    Expected: ask ONLY about Ameren, never restart qualification.
    """

    def setUp(self):
        super().setUp()
        self.cid = "bundle-one-left"
        self.make_unqualified_lead(self.cid)
        self.claude.reply_text = "Perfect. And you're with Ameren for electric, right?"
        self.reply = ma.michael_agent(self.cid, "My bill is $220 and yeah I own it")

    def test_the_two_stated_facts_are_recorded(self):
        home, util, bill = self.record(self.cid)
        self.assertEqual(home, ma.QUAL_YES)
        self.assertEqual(bill, ma.QUAL_YES)
        self.assertEqual(util, ma.QUAL_UNKNOWN)

    def test_the_action_is_ask_missing_naming_the_utility(self):
        state = ma.get_state(self.cid)
        decision = ma.decide_conversion_action(state, "My bill is $220 and yeah I own it",
                                               prior_outbound=BUNDLED_OFFER)
        self.assertEqual(decision.action, ma.ACT_ASK_MISSING)
        self.assertEqual(decision.missing_field, ma.Q_UTILITY)

    def test_no_booking_link_until_the_last_fact_lands(self):
        self.assertNotIn(ma.BOOKING_LINK, self.reply)

    def test_the_prompt_asks_for_the_utility_and_nothing_else(self):
        goal = ma._goal_from_prompt(
            ma.build_system_prompt(ma.get_state(self.cid), ask_only=ma.Q_UTILITY))
        self.assertIn("ASK UTILITY", goal)
        self.assertNotIn("ASK OWNERSHIP", goal)
        self.assertNotIn("ASK BILL", goal)

    def test_confirming_ameren_next_turn_goes_straight_to_booking(self):
        self.claude.reply_text = "Perfect — grab a time that works:"
        reply = ma.michael_agent(self.cid, "Yes", prior_outbound=
                                 "And you're with Ameren for electric, right?")
        self.assertEqual(ma.get_state(self.cid)["q_utility"], ma.UTIL_AMEREN)
        self.assertIn(ma.BOOKING_LINK, reply)


# ═════════════════════════════════════════════════════════════════════════
#  TEST D — bundled offer, one criterion fails
# ═════════════════════════════════════════════════════════════════════════

class TestD_QualifiedBillButRenting(BundledTestCase):
    """
    AI:   bundled criteria
    Lead: "My bill is $190 but I rent."
    Expected: bill=yes, homeowner=no, and NO booking.
    """

    def setUp(self):
        super().setUp()
        self.cid = "bundle-renter"
        self.make_unqualified_lead(self.cid)
        self.claude.reply_text = (
            "Got it — solar really only works for homeowners. "
            "If that ever changes, reach out. [DISQUALIFY:NOT_OWNER]"
        )
        self.reply = ma.michael_agent(self.cid, "My bill is $190 but I rent.")

    def test_the_bill_is_still_recorded(self):
        self.assertEqual(ma.get_state(self.cid)["q_bill_100_plus"], ma.QUAL_YES)

    def test_renting_is_recorded_as_a_no_not_an_unknown(self):
        self.assertEqual(ma.get_state(self.cid)["q_homeowner"], ma.QUAL_NO)

    def test_no_booking_link_is_sent(self):
        self.assertNotIn(ma.BOOKING_LINK, self.reply)

    def test_the_verdict_is_disqualified(self):
        verdict, _ = ma.qualification_verdict(ma.get_state(self.cid))
        self.assertEqual(verdict, ma.VERDICT_DISQUALIFIED)

    def test_the_conversion_path_refuses_to_fire(self):
        decision = ma.decide_conversion_action(
            ma.get_state(self.cid), "My bill is $190 but I rent.",
            prior_outbound=BUNDLED_OFFER)
        self.assertEqual(decision.action, ma.ACT_NO_OVERRIDE)

    def test_even_an_explicit_call_request_cannot_book_a_renter(self):
        state = ma.get_state(self.cid)
        decision = ma.decide_conversion_action(state, "just call me",
                                               prior_outbound=BUNDLED_OFFER)
        self.assertEqual(decision.action, ma.ACT_NO_OVERRIDE)


# ═════════════════════════════════════════════════════════════════════════
#  TEST E — bundled offer, wrong utility
# ═════════════════════════════════════════════════════════════════════════

class TestE_YesButDifferentUtility(BundledTestCase):
    """
    AI:   bundled criteria
    Lead: "Yes, but I have Cuivre River."
    Expected: utility=other, never treated as Ameren-qualified.
    """

    def setUp(self):
        super().setUp()
        self.cid = "bundle-cuivre"
        self.make_unqualified_lead(self.cid)
        self.claude.reply_text = (
            "Got it — we focus on Ameren Missouri homeowners. "
            "I'll keep your info on file. [DISQUALIFY:OUT_OF_AREA]"
        )
        self.reply = ma.michael_agent(self.cid, "Yes, but I have Cuivre River.")

    def test_utility_is_recorded_as_other(self):
        self.assertEqual(ma.get_state(self.cid)["q_utility"], ma.UTIL_OTHER)

    def test_the_leading_yes_does_not_manufacture_an_ameren_confirmation(self):
        self.assertNotEqual(ma.get_state(self.cid)["q_utility"], ma.UTIL_AMEREN)

    def test_no_booking_link_is_sent(self):
        self.assertNotIn(ma.BOOKING_LINK, self.reply)

    def test_the_verdict_is_disqualified(self):
        verdict, _ = ma.qualification_verdict(ma.get_state(self.cid))
        self.assertEqual(verdict, ma.VERDICT_DISQUALIFIED)

    def test_a_later_blanket_yes_cannot_overturn_the_explicit_answer(self):
        # A bundled blanket yes is an inference; an explicit statement is not.
        # The inference must never win.
        state = ma.get_state(self.cid)
        ma.apply_qualification_facts(
            state, ma.extract_qualification_facts("yes please", ma.OUT_BUNDLED_QUAL))
        self.assertEqual(state["q_utility"], ma.UTIL_OTHER)


# ═════════════════════════════════════════════════════════════════════════
#  TEST F — Susan's scenario survives the rewrite
# ═════════════════════════════════════════════════════════════════════════

class TestF_SusanStillConverts(BundledTestCase):
    """
    The original bug. A conversion invitation is not a bundled offer, so it
    confirms no facts — but interest alone is still enough to book.
    """

    def test_susan_still_gets_the_booking_link(self):
        cid = "susan-after-bundle"
        self.make_unqualified_lead(cid, prior_outbound=NURTURE_BREAKUP)
        self.claude.reply_text = "Yep absolutely. Grab whatever time works best for you:"
        reply = ma.michael_agent(cid, "Yes please")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertNotIn("homeowner", reply.lower())

    def test_susan_is_booked_without_inventing_qualification_facts(self):
        cid = "susan-no-invent"
        self.make_unqualified_lead(cid, prior_outbound=NURTURE_BREAKUP)
        self.claude.reply_text = "Grab a time that works:"
        ma.michael_agent(cid, "Yes please")
        # Booking her is right; claiming she told us she owns the home is not.
        self.assertEqual(ma.get_state(cid)["q_homeowner"], ma.QUAL_UNKNOWN)

    def test_the_ghl_recovered_nurture_text_still_works(self):
        cid = "susan-ghl-bundle"
        self.make_unqualified_lead(cid, prior_outbound="")
        self.claude.reply_text = "Absolutely — pick a time:"
        reply = ma.michael_agent(cid, "Yes please", prior_outbound=NURTURE_BREAKUP)
        self.assertIn(ma.BOOKING_LINK, reply)


# ═════════════════════════════════════════════════════════════════════════
#  TEST G — booked lockout, TEST H — STOP
# ═════════════════════════════════════════════════════════════════════════

class TestG_BookedContactAfterBundledOffer(BundledTestCase):

    def test_a_booked_lead_gets_no_new_link(self):
        cid = "bundle-booked"
        state = self.make_unqualified_lead(cid)
        state["stage"] = ma.Stage.BOOKED
        state["appointment_booked"] = True
        ma.save_state(cid, state)
        self.claude.reply_text = "See you then."
        reply = ma.michael_agent(cid, "Yes please")
        self.assertNotIn(ma.BOOKING_LINK, reply)

    def test_the_qualification_record_is_not_touched_for_a_booked_lead(self):
        cid = "bundle-booked-2"
        state = self.make_unqualified_lead(cid)
        state["stage"] = ma.Stage.BOOKED
        state["appointment_booked"] = True
        ma.save_state(cid, state)
        self.claude.reply_text = "See you then."
        ma.michael_agent(cid, "Yes please")
        self.assertEqual(self.record(cid),
                         (ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN))


class TestH_StopAfterBundledOffer(BundledTestCase):

    def test_stop_wins_over_a_bundled_offer(self):
        cid = "bundle-stop"
        self.make_unqualified_lead(cid)
        reply = ma.michael_agent(cid, "STOP")
        self.assertIn("unsubscribed", reply.lower())
        self.assertNotIn(ma.BOOKING_LINK, reply)
        self.assertEqual(ma.get_state(cid)["stage"], ma.Stage.DNC)

    def test_stop_records_no_qualification_facts(self):
        cid = "bundle-stop-2"
        self.make_unqualified_lead(cid)
        ma.michael_agent(cid, "STOP")
        self.assertEqual(self.record(cid),
                         (ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN))


# ═════════════════════════════════════════════════════════════════════════
#  The prompt asks the right thing
# ═════════════════════════════════════════════════════════════════════════

class TestPromptGoals(BundledTestCase):

    def test_two_unknowns_produce_a_bundled_invitation_goal(self):
        state = self.make_unqualified_lead("goal-bundle", q_utility=ma.UTIL_AMEREN)
        goal = ma._goal_from_prompt(ma.build_system_prompt(state))
        self.assertIn("BUNDLED QUALIFICATION", goal)

    def test_three_unknowns_produce_a_bundled_invitation_goal(self):
        state = self.make_unqualified_lead("goal-bundle-3")
        goal = ma._goal_from_prompt(ma.build_system_prompt(state))
        self.assertIn("BUNDLED QUALIFICATION", goal)

    def test_one_unknown_asks_only_that_one(self):
        state = self.make_unqualified_lead(
            "goal-one", q_homeowner=ma.QUAL_YES, q_bill_100_plus=ma.QUAL_YES)
        goal = ma._goal_from_prompt(ma.build_system_prompt(state))
        self.assertIn("ASK UTILITY", goal)
        self.assertNotIn("ASK OWNERSHIP", goal)

    def test_a_complete_record_produces_a_booking_goal(self):
        state = self.make_unqualified_lead(
            "goal-qualified", q_homeowner=ma.QUAL_YES,
            q_utility=ma.UTIL_AMEREN, q_bill_100_plus=ma.QUAL_YES)
        goal = ma._goal_from_prompt(ma.build_system_prompt(state))
        self.assertIn("BOOKING", goal)

    def test_a_failed_criterion_produces_a_disqualify_goal(self):
        state = self.make_unqualified_lead("goal-dq", q_homeowner=ma.QUAL_NO)
        goal = ma._goal_from_prompt(ma.build_system_prompt(state))
        self.assertIn("DISQUALIFY", goal)
        self.assertIn("NOT_OWNER", goal)

    def test_the_prompt_forbids_asking_one_at_a_time(self):
        state = self.make_unqualified_lead("goal-prompt")
        system = ma.build_system_prompt(state)
        self.assertIn("QUALIFICATION CRITERIA", system)
        self.assertIn("DO NOT INTERROGATE", system)
        self.assertNotIn("QUALIFICATION ORDER", system)

    def test_confirmed_criteria_are_listed_as_settled(self):
        state = self.make_unqualified_lead(
            "goal-known", q_homeowner=ma.QUAL_YES, q_utility=ma.UTIL_AMEREN)
        system = ma.build_system_prompt(state)
        self.assertIn("HOMEOWNER: confirmed", system)
        self.assertIn("UTILITY: Ameren Missouri confirmed", system)

    def test_service_area_is_never_rendered_as_a_utility_confirmation(self):
        # An address in the metro says nothing about who bills them. Conflating
        # the two is how a lead gets booked on an unverified utility.
        state = self.make_unqualified_lead("goal-area", location_confirmed=True)
        system = ma.build_system_prompt(state)
        self.assertIn("SERVICE AREA", system)
        self.assertNotIn("UTILITY: Ameren Missouri confirmed", system)


class TestLegacyFieldSync(BundledTestCase):
    """The original fields must keep telling the same story."""

    def test_the_record_writes_through_to_the_legacy_fields(self):
        state = ma.get_state("sync-1")
        state["q_homeowner"] = ma.QUAL_YES
        state["q_utility"]   = ma.UTIL_AMEREN
        ma.sync_qualification_fields(state)
        self.assertEqual(state["homeowner"], "yes")
        self.assertTrue(state["location_confirmed"])

    def test_the_legacy_fields_seed_the_record(self):
        state = ma.get_state("sync-2")
        state["homeowner"]    = "yes"
        state["monthly_bill"] = "$150/month"
        ma.sync_qualification_fields(state)
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)
        self.assertEqual(state["q_bill_100_plus"], ma.QUAL_YES)

    def test_service_area_never_seeds_the_utility_criterion(self):
        state = ma.get_state("sync-3")
        state["location_confirmed"] = True
        ma.sync_qualification_fields(state)
        self.assertEqual(state["q_utility"], ma.QUAL_UNKNOWN)

    def test_a_restored_qualified_contact_is_not_re_qualified(self):
        # A contact restored from GHL tags demonstrably answered all three
        # once; re-opening them would walk them backwards after a restart.
        state = ma.get_state("sync-4")
        state["stage"]     = ma.Stage.SEND_BOOKING
        state["qualified"] = True
        ma.sync_qualification_fields(state)
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_QUALIFIED)

    def test_a_low_bill_on_file_seeds_a_no(self):
        state = ma.get_state("sync-5")
        state["monthly_bill"] = "$60/month"
        ma.sync_qualification_fields(state)
        self.assertEqual(state["q_bill_100_plus"], ma.QUAL_NO)


class TestStaleStageCannotInventFacts(BundledTestCase):
    """
    The legacy detectors trust `stage`; the record trusts the message actually
    sent. When those disagree — routine after a restart restores a stage from
    tags — the message wins, because a stale label must never manufacture a
    confirmation nobody gave.
    """

    def test_a_yes_to_the_utility_question_cannot_confirm_ownership(self):
        cid = "stale-stage"
        # Stage says ASK_OWNERSHIP, but what we actually sent was the utility
        # question. Only the utility may be confirmed.
        self.make_unqualified_lead(cid, prior_outbound=UTILITY_Q,
                                   stage=ma.Stage.ASK_OWNERSHIP,
                                   location_confirmed=True)
        self.claude.reply_text = "Got it. Are you the homeowner?"
        ma.michael_agent(cid, "Yes")
        home, util, bill = self.record(cid)
        self.assertEqual(util, ma.UTIL_AMEREN)
        self.assertEqual(home, ma.QUAL_UNKNOWN,
                         "a stale stage must not turn a utility yes into ownership")
        self.assertEqual(bill, ma.QUAL_UNKNOWN)

    def test_a_yes_to_the_ownership_question_cannot_confirm_the_utility(self):
        cid = "stale-stage-2"
        self.make_unqualified_lead(cid, prior_outbound=OWNERSHIP_Q,
                                   stage=ma.Stage.ASK_OWNERSHIP,
                                   location_confirmed=True)
        self.claude.reply_text = "Got it. What does the Ameren bill usually run?"
        ma.michael_agent(cid, "Yes")
        home, util, bill = self.record(cid)
        self.assertEqual(home, ma.QUAL_YES)
        self.assertEqual(util, ma.QUAL_UNKNOWN)

    def test_behaviour_is_unchanged_when_the_previous_outbound_is_unknown(self):
        # No history and nothing recoverable: the original stage-based reading
        # still applies, so this is not a regression for old contacts.
        self.assertEqual(
            ma._detect_homeowner("Yes", ma.Stage.ASK_OWNERSHIP, location_confirmed=True),
            "yes",
        )


# ═════════════════════════════════════════════════════════════════════════
#  HIGH INTENT + INCOMPLETE RECORD  [BUNDLE-2]
#
#  A lead who says yes to an invitation gets the conditions AND the link in
#  ONE message. Not three questions and then a link — that burns the momentum
#  that produced the reply. Not a bare link either — that books someone who
#  may not qualify. Conditions let them self-qualify without being asked.
#
#  And crucially: sending that message records NOTHING. Susan never said she
#  owns her home, so nothing may claim she did.
# ═════════════════════════════════════════════════════════════════════════

class TestCriteriaClause(unittest.TestCase):

    def test_one_two_and_three_criteria_read_naturally(self):
        self.assertEqual(ma.criteria_clause([ma.Q_BILL]),
                         "the electric bill usually runs over $100 a month")
        self.assertEqual(ma.criteria_clause([ma.Q_HOMEOWNER, ma.Q_UTILITY]),
                         "you own the home and you're on Ameren")
        self.assertEqual(
            ma.criteria_clause([ma.Q_HOMEOWNER, ma.Q_UTILITY, ma.Q_BILL]),
            "you own the home, you're on Ameren, and the electric bill usually "
            "runs over $100 a month")

    def test_order_is_fixed_regardless_of_input_order(self):
        self.assertEqual(ma.criteria_clause([ma.Q_BILL, ma.Q_HOMEOWNER]),
                         ma.criteria_clause([ma.Q_HOMEOWNER, ma.Q_BILL]))

    def test_nothing_missing_yields_no_clause(self):
        self.assertEqual(ma.criteria_clause([]), "")


class TestHighIntentWithUnknownRecord(BundledTestCase):
    """
    Susan: "Yes please" to the breakup text, all three criteria unknown.
    Expected: conditions + link in one message; record untouched.
    """

    def setUp(self):
        super().setUp()
        self.cid = "susan-bundle2"
        self.make_unqualified_lead(self.cid, prior_outbound=NURTURE_BREAKUP)

    def test_the_prompt_tells_claude_to_fold_the_conditions_in(self):
        self.claude.reply_text = (
            "Absolutely. If you own the home, you're on Ameren, and you're usually "
            "over $100 a month, I'd be glad to show you what a $0-down option "
            "could look like. Grab a time that works for you:"
        )
        ma.michael_agent(self.cid, "Yes please")
        system = self.claude.systems[-1]
        self.assertIn("FOLD IT INTO THIS SAME MESSAGE", system)
        self.assertIn("you own the home", system)
        self.assertIn("you're on Ameren", system)
        self.assertIn("over $100 a month", system)
        self.assertIn("Conditions, never questions", system)

    def test_the_link_still_goes_out_in_the_same_message(self):
        self.claude.reply_text = (
            "Absolutely. If you own the home, have Ameren, and are usually over "
            "$100 a month on electric, I can show you what this looks like:"
        )
        reply = ma.michael_agent(self.cid, "Yes please")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertIn("if you own the home", reply.lower())

    def test_the_fallback_also_carries_conditions_and_the_link(self):
        # Claude outage: the deterministic line must still do both jobs.
        self.claude.raises = True
        reply = ma.michael_agent(self.cid, "Yes please")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertIn("you own the home", reply)
        self.assertIn("Ameren", reply)
        self.assertIn("$100", reply)

    def test_the_fallback_states_conditions_it_never_asks(self):
        self.claude.raises = True
        reply = ma.michael_agent(self.cid, "Yes please")
        lowered = reply.lower()
        self.assertNotIn("are you the homeowner", lowered)
        self.assertNotIn("are you with ameren", lowered)
        self.assertNotIn("are you on ameren", lowered)

    def test_the_record_stays_unknown(self):
        self.claude.raises = True
        ma.michael_agent(self.cid, "Yes please")
        self.assertEqual(self.record(self.cid),
                         (ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN))

    def test_she_is_not_flagged_qualified(self):
        self.claude.raises = True
        ma.michael_agent(self.cid, "Yes please")
        self.assertFalse(ma.get_state(self.cid).get("qualified"))

    def test_a_question_shaped_reply_from_claude_is_rejected(self):
        # If the model asks instead of stating, the homeowner would have to
        # answer before the link meant anything. Fall back rather than ship it.
        self.claude.reply_text = "Sure. Are you the homeowner?"
        reply = ma.michael_agent(self.cid, "Yes please")
        self.assertNotIn("are you the homeowner", reply.lower())
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertIn("you own the home", reply)

    def test_an_explicit_call_request_with_an_unknown_record_behaves_the_same(self):
        cid = "call-unknown"
        self.make_unqualified_lead(cid, prior_outbound=OWNERSHIP_Q)
        self.claude.raises = True
        reply = ma.michael_agent(cid, "Can you just call me?")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertIn("you own the home", reply)


class TestHighIntentWithPartialRecord(BundledTestCase):
    """Known facts are never restated; only the gaps become conditions."""

    def test_only_the_missing_bill_is_named(self):
        cid = "partial-bill"
        self.make_unqualified_lead(cid, prior_outbound=NURTURE_BREAKUP,
                                   q_homeowner=ma.QUAL_YES,
                                   q_utility=ma.UTIL_AMEREN)
        self.claude.raises = True
        reply = ma.michael_agent(cid, "Yes please")
        self.assertIn("$100", reply)
        self.assertNotIn("you own the home", reply)
        self.assertNotIn("you're on Ameren", reply)
        self.assertIn(ma.BOOKING_LINK, reply)

    def test_the_prompt_lists_what_is_already_confirmed(self):
        cid = "partial-prompt"
        self.make_unqualified_lead(cid, prior_outbound=NURTURE_BREAKUP,
                                   q_homeowner=ma.QUAL_YES,
                                   q_utility=ma.UTIL_AMEREN)
        self.claude.reply_text = "Absolutely — as long as the bill is usually over $100:"
        ma.michael_agent(cid, "Yes please")
        system = self.claude.systems[-1]
        self.assertIn("Already confirmed (do NOT restate or re-ask)", system)
        self.assertIn("they own the home", system)
        self.assertIn("they're on Ameren", system)

    def test_two_missing_criteria_are_both_named(self):
        cid = "partial-two"
        self.make_unqualified_lead(cid, prior_outbound=NURTURE_BREAKUP,
                                   q_homeowner=ma.QUAL_YES)
        self.claude.raises = True
        reply = ma.michael_agent(cid, "Yes please")
        self.assertIn("Ameren", reply)
        self.assertIn("$100", reply)
        self.assertNotIn("you own the home", reply)


class TestHighIntentWithCompleteRecord(BundledTestCase):
    """Nothing missing: a plain booking message, no conditions bolted on."""

    def setUp(self):
        super().setUp()
        self.cid = "complete-record"
        self.make_unqualified_lead(self.cid, prior_outbound=NURTURE_BREAKUP,
                                   q_homeowner=ma.QUAL_YES,
                                   q_utility=ma.UTIL_AMEREN,
                                   q_bill_100_plus=ma.QUAL_YES)

    def test_the_prompt_tells_claude_not_to_restate_anything(self):
        self.claude.reply_text = "Perfect — grab a time that works for you:"
        ma.michael_agent(self.cid, "Yes please")
        system = self.claude.systems[-1]
        self.assertIn("All three criteria are already confirmed", system)
        self.assertNotIn("FOLD IT INTO THIS SAME MESSAGE", system)

    def test_the_fallback_carries_no_conditions(self):
        self.claude.raises = True
        reply = ma.michael_agent(self.cid, "Yes please")
        self.assertIn(ma.BOOKING_LINK, reply)
        self.assertNotIn("if you own", reply.lower())
        self.assertNotIn("$100", reply)

    def test_the_qualified_flag_is_set_for_a_real_verdict(self):
        self.claude.raises = True
        ma.michael_agent(self.cid, "Yes please")
        self.assertTrue(ma.get_state(self.cid)["qualified"])


class TestNoLinkForExplicitDisqualification(BundledTestCase):
    """An explicit failing fact beats any amount of enthusiasm."""

    def _run(self, field, value, inbound):
        cid = f"dq-{field}-{value}"
        self.make_unqualified_lead(cid, prior_outbound=NURTURE_BREAKUP,
                                   **{field: value})
        self.claude.reply_text = "Got it — I'll keep your info on file."
        return ma.michael_agent(cid, inbound)

    def test_a_known_renter_never_gets_the_link(self):
        reply = self._run("q_homeowner", ma.QUAL_NO, "Yes please")
        self.assertNotIn(ma.BOOKING_LINK, reply)

    def test_a_known_other_utility_never_gets_the_link(self):
        reply = self._run("q_utility", ma.UTIL_OTHER, "Yes please")
        self.assertNotIn(ma.BOOKING_LINK, reply)

    def test_a_known_low_bill_never_gets_the_link(self):
        reply = self._run("q_bill_100_plus", ma.QUAL_NO, "Yes please")
        self.assertNotIn(ma.BOOKING_LINK, reply)

    def test_not_even_an_explicit_call_request(self):
        reply = self._run("q_homeowner", ma.QUAL_NO, "Can you just call me?")
        self.assertNotIn(ma.BOOKING_LINK, reply)


class TestQualifiedTagHonesty(BundledTestCase):
    """
    GHL must not record a qualification nobody gave — while restart recovery
    keeps working exactly as before.
    """

    def test_a_link_sent_on_high_intent_is_not_tagged_qualified(self):
        state = ma.get_state("tag-unqual")
        state["stage"] = ma.Stage.SEND_BOOKING
        tags = ma.resolve_ghl_tags(state["stage"], qualified=False)
        self.assertIn("BOOKING_LINK_SENT", tags)
        self.assertNotIn("QUALIFIED", tags)

    def test_a_genuinely_qualified_lead_is_tagged_qualified(self):
        tags = ma.resolve_ghl_tags(ma.Stage.SEND_BOOKING, qualified=True)
        self.assertIn("QUALIFIED", tags)
        self.assertIn("BOOKING_LINK_SENT", tags)

    def test_restart_recovery_still_resumes_at_send_booking(self):
        # The whole point of keeping BOOKING_LINK_SENT: restore is unchanged.
        stage, why = ma.restore_stage_from_ghl(["BOOKING_LINK_SENT"])
        self.assertEqual(stage, ma.Stage.SEND_BOOKING)

    def test_restore_from_booking_link_sent_does_not_claim_qualification(self):
        state = ma.get_state("restore-unqual")
        ma.apply_restored_stage("restore-unqual", state, ["BOOKING_LINK_SENT"])
        self.assertEqual(state["stage"], ma.Stage.SEND_BOOKING)
        self.assertFalse(state.get("qualified"))
        verdict, missing = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_INCOMPLETE)
        self.assertEqual(len(missing), 3)

    def test_restore_from_the_qualified_tag_does_claim_it(self):
        state = ma.get_state("restore-qual")
        ma.apply_restored_stage("restore-qual", state, ["QUALIFIED", "BOOKING_LINK_SENT"])
        self.assertTrue(state["qualified"])
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_QUALIFIED)

    def test_the_send_booking_stage_alone_never_backfills_the_record(self):
        state = ma.get_state("no-backfill")
        state["stage"] = ma.Stage.SEND_BOOKING
        ma.sync_qualification_fields(state)
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)
        self.assertEqual(state["q_utility"], ma.QUAL_UNKNOWN)

    def test_a_booked_contact_still_backfills(self):
        state = ma.get_state("booked-backfill")
        state["stage"] = ma.Stage.BOOKED
        ma.sync_qualification_fields(state)
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_QUALIFIED)


class TestBundledYesStillConfirmsFacts(BundledTestCase):
    """
    The distinction that makes all of this safe, restated as a pair.
    Same words, two different previous messages, two different outcomes.
    """

    def test_yes_to_a_bundled_offer_confirms_the_criteria(self):
        cid = "pair-bundled"
        self.make_unqualified_lead(cid, prior_outbound=BUNDLED_OFFER)
        self.claude.raises = True
        reply = ma.michael_agent(cid, "Yes please")
        self.assertEqual(self.record(cid), (ma.QUAL_YES, ma.UTIL_AMEREN, ma.QUAL_YES))
        self.assertIn(ma.BOOKING_LINK, reply)
        # Already confirmed, so no conditions are restated.
        self.assertNotIn("$100", reply)

    def test_yes_to_an_invitation_confirms_nothing_but_still_books(self):
        cid = "pair-invitation"
        self.make_unqualified_lead(cid, prior_outbound=NURTURE_BREAKUP)
        self.claude.raises = True
        reply = ma.michael_agent(cid, "Yes please")
        self.assertEqual(self.record(cid),
                         (ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN, ma.QUAL_UNKNOWN))
        self.assertIn(ma.BOOKING_LINK, reply)
        # Unknown, so the conditions ride along.
        self.assertIn("$100", reply)


# ═════════════════════════════════════════════════════════════════════════
#  STALE-STAGE OWNERSHIP INFERENCE  [BUNDLE-3]
#
#  Stage.ASK_LOCATION was in the "we advanced past ownership" list, left over
#  from the flow where ownership was asked FIRST. After the order was reversed
#  — utility/service area is now question one — that stage came to mean the
#  opposite: the utility question is still pending, so ownership has not been
#  asked at all.
#
#  The effect: anyone sitting on the first question was silently recorded as a
#  homeowner, the prompt then printed "HOMEOWNER: confirmed", the question was
#  never asked, and a renter could reach the booking link unqualified.
#
#  These tests pin the boundary from both sides: the stages that genuinely
#  come after ownership must still infer it, and ASK_LOCATION must never.
# ═════════════════════════════════════════════════════════════════════════

class TestStaleAskLocationCannotInferOwnership(BundledTestCase):

    def _turn(self, cid, inbound, prior, stage, *, reply="Got it.", **fields):
        """One full agent turn from an explicit starting stage."""
        self.make_unqualified_lead(cid, prior_outbound=prior, stage=stage,
                                   location_confirmed=fields.pop("location_confirmed", False),
                                   **fields)
        self.claude.reply_text = reply
        return ma.michael_agent(cid, inbound)

    # ── the bug itself ───────────────────────────────────────────────

    def test_a_utility_answer_at_ask_location_never_becomes_ownership(self):
        cid = "stale-loc-utility"
        self._turn(cid, "Yes", UTILITY_Q, ma.Stage.ASK_LOCATION,
                   reply="Got it. Are you the homeowner?")
        state = ma.get_state(cid)
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)
        self.assertIsNone(state["homeowner"],
                          "ASK_LOCATION must not imply ownership — it means the "
                          "utility question is still pending")
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)

    def test_a_non_answer_at_ask_location_never_becomes_ownership(self):
        # The purest form: they asked who we are. Nothing about ownership was
        # said, asked, or implied — and yet this used to record homeowner=yes.
        cid = "stale-loc-whois"
        self._turn(cid, "who is this?", UTILITY_Q, ma.Stage.ASK_LOCATION,
                   reply="Michael with STL Energy Advisors.")
        state = ma.get_state(cid)
        self.assertIsNone(state["homeowner"])
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)

    def test_a_fresh_contact_falling_back_to_ask_location_infers_nothing(self):
        # INITIAL with no service area resolves to ASK_LOCATION at the end of
        # the turn, which is exactly where the stale inference used to fire.
        cid = "stale-loc-initial"
        self._turn(cid, "who is this?", UTILITY_Q, ma.Stage.INITIAL,
                   reply="Michael with STL Energy Advisors.")
        state = ma.get_state(cid)
        self.assertEqual(state["stage"], ma.Stage.ASK_LOCATION)
        self.assertIsNone(state["homeowner"])
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)

    def test_a_restored_stage_cannot_manufacture_ownership_either(self):
        # Restart recovery lands a contact on a stage from GHL tags while the
        # real conversation is somewhere else entirely. The message sent is
        # the authority, not the label.
        cid = "stale-loc-restored"
        state = ma.get_state(cid)
        ma.apply_restored_stage(cid, state, [ma.TAG_ENGAGED])
        state["q_homeowner"] = ma.QUAL_UNKNOWN
        state["homeowner"]   = None
        state["messages"]    = [{"role": "assistant", "content": UTILITY_Q}]
        ma.save_state(cid, state)

        self.claude.reply_text = "Got it. Are you the homeowner?"
        ma.michael_agent(cid, "Yes")

        state = ma.get_state(cid)
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)
        self.assertIsNone(state["homeowner"])

    def test_the_prompt_still_asks_about_ownership_afterwards(self):
        # The real-world consequence: if ownership were silently inferred, the
        # prompt would list it as confirmed and never ask.
        cid = "stale-loc-prompt"
        self._turn(cid, "Yes", UTILITY_Q, ma.Stage.ASK_LOCATION,
                   reply="Got it. Are you the homeowner?")
        system = ma.build_system_prompt(ma.get_state(cid))
        self.assertNotIn("HOMEOWNER: confirmed", system)

    def test_a_renter_at_ask_location_is_never_flipped_to_owner(self):
        cid = "stale-loc-renter"
        self._turn(cid, "I rent", UTILITY_Q, ma.Stage.ASK_LOCATION,
                   reply="Got it — solar really only works for homeowners.")
        state = ma.get_state(cid)
        self.assertEqual(state["q_homeowner"], ma.QUAL_NO)
        self.assertEqual(state["homeowner"], "no")

    def test_a_renter_cannot_reach_the_booking_link_through_this_path(self):
        # The end-to-end harm the stale entry allowed.
        cid = "stale-loc-renter-book"
        self._turn(cid, "I rent", UTILITY_Q, ma.Stage.ASK_LOCATION,
                   reply="Got it — solar really only works for homeowners.")
        verdict, _ = ma.qualification_verdict(ma.get_state(cid))
        self.assertEqual(verdict, ma.VERDICT_DISQUALIFIED)
        decision = ma.decide_conversion_action(ma.get_state(cid), "yes please",
                                               prior_outbound=NURTURE_BREAKUP)
        self.assertEqual(decision.action, ma.ACT_NO_OVERRIDE)

    # ── the other side of the boundary: real progress still infers ───

    def test_ask_bill_implies_ownership_end_to_end(self):
        # Reaching the bill question means ownership was asked and answered.
        cid = "ask-bill-infers"
        self.make_unqualified_lead(cid, prior_outbound=OWNERSHIP_Q,
                                   stage=ma.Stage.ASK_OWNERSHIP,
                                   location_confirmed=True)
        state = ma.get_state(cid)
        state["homeowner"] = None
        state["q_homeowner"] = ma.QUAL_UNKNOWN
        state["stage"] = ma.Stage.ASK_BILL
        ma.save_state(cid, state)
        self.claude.reply_text = "About what does the Ameren bill usually run?"
        ma.michael_agent(cid, "sounds good", prior_outbound="Anything else?")
        self.assertEqual(ma.get_state(cid)["homeowner"], "yes")

    def test_ask_ownership_itself_never_infers_the_answer(self):
        # Sitting ON the question is not the same as having passed it.
        cid = "ask-ownership-no-infer"
        self._turn(cid, "who is this?", OWNERSHIP_Q, ma.Stage.ASK_OWNERSHIP,
                   reply="Michael with STL Energy Advisors.",
                   location_confirmed=True)
        self.assertIsNone(ma.get_state(cid)["homeowner"])

    def test_the_record_and_the_legacy_field_never_disagree_after_a_turn(self):
        # The inference writes the legacy field; the sync at the end of the
        # turn is what keeps the record from lagging a message behind.
        cid = "sync-after-inference"
        self.make_unqualified_lead(cid, prior_outbound=OWNERSHIP_Q,
                                   stage=ma.Stage.ASK_OWNERSHIP,
                                   location_confirmed=True)
        state = ma.get_state(cid)
        state["homeowner"] = None
        state["stage"] = ma.Stage.ASK_BILL
        ma.save_state(cid, state)
        self.claude.reply_text = "About what does the bill usually run?"
        ma.michael_agent(cid, "sounds good", prior_outbound="Anything else?")
        state = ma.get_state(cid)
        self.assertEqual(state["homeowner"], "yes")
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
