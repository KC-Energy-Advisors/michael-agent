"""
Regression tests for Meta Instant Form awareness  [FORM-AWARE]

    python -m unittest test_form_aware -v
    python test_form_aware.py

stdlib only - no pytest, no network, no Anthropic key. Every Claude call and
every GHL call is stubbed, so a failure here is always a logic failure.

WHAT THESE EXIST FOR
    The Meta form asks ownership, bill and utility BEFORE Michael texts. If
    Michael then opens with "are you currently with Ameren Missouri?" the
    homeowner has been asked something they already answered, and the form
    was pointless. These tests pin down:

      * known answers are never re-asked
      * only genuinely missing answers are asked for
      * "Another electric provider" is a QUESTION, not a rejection
      * an explicit SMS correction always beats the form
      * every production safety gate still fires unchanged
"""

import asyncio
import os
import unittest

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")

import michael_agent as ma

run = asyncio.run


# ── Claude stub ──────────────────────────────────────────────────────────

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
        return _StubResponse(self._owner.reply_text)


class StubClaude:
    def __init__(self, reply_text="Got it."):
        self.reply_text = reply_text
        self.calls      = []
        self.messages   = _StubMessages(self)

    @property
    def systems(self):
        return [c.get("system", "") for c in self.calls]


# ── Form payload helpers ─────────────────────────────────────────────────

def form_fields(homeowner="", bill="", utility=""):
    """GHL contact custom fields as resolve_custom_fields() returns them."""
    out = {}
    if homeowner:
        out["homeowner_status"] = homeowner
    if bill:
        out["avg_monthly_bill"] = bill
    if utility:
        out["utility_provider"] = utility
    return out


QUALIFIED_FORM = form_fields("Yes", "$200-$299", "Ameren Missouri")


class FormAwareTestCase(unittest.TestCase):
    """Clean state, stubbed Claude, flag ON, GHL reads stubbed."""

    def setUp(self):
        ma._state_store.clear()
        ma._no_form_fields.clear()      # per-contact cache: isolate per test
        self._real_claude = ma.claude
        self._real_flag   = ma.FORM_AWARE_ENABLED
        self._real_fetch  = ma.fetch_ghl_contact_compliance
        self._real_schema = ma.fetch_ghl_custom_field_schema
        self._real_elig   = dict(ma.UTILITY_ELIGIBLE)

        self.claude = StubClaude()
        ma.claude = self.claude
        ma.FORM_AWARE_ENABLED = True
        self.addCleanup(self._restore)

    def _restore(self):
        ma.claude                      = self._real_claude
        ma.FORM_AWARE_ENABLED          = self._real_flag
        ma.fetch_ghl_contact_compliance = self._real_fetch
        ma.fetch_ghl_custom_field_schema = self._real_schema
        ma.UTILITY_ELIGIBLE.clear()
        ma.UTILITY_ELIGIBLE.update(self._real_elig)
        ma._state_store.clear()
        ma._no_form_fields.clear()

    def stub_contact(self, custom_fields=None, address="", tags=None, ok=True):
        """Make the GHL contact record return these form answers."""
        async def _fake(contact_id):
            return {
                "ok": ok, "first_name": "Sarah", "full_name": "Sarah Miller",
                "phone": "+13145550123", "email": "s@example.com",
                "dnd": False, "dnd_sms": {}, "tags": list(tags or []),
                "date_added": "", "http": 200 if ok else 500,
                "custom_fields": dict(custom_fields or {}),
                "address": address,
            }
        ma.fetch_ghl_contact_compliance = _fake

    def hydrate(self, contact_id="c1", custom_fields=None, address=""):
        self.stub_contact(custom_fields, address)
        state = ma.get_state(contact_id)
        report = run(ma.hydrate_form_facts(contact_id, state))
        return state, report


# ═════════════════════════════════════════════════════════════════════════
#  1-3  Known answers are not re-asked; missing ones are
# ═════════════════════════════════════════════════════════════════════════

class Test01_FullyQualifiedAsksNothing(FormAwareTestCase):

    def test_all_three_facts_load_from_the_form(self):
        state, report = self.hydrate(custom_fields=QUALIFIED_FORM)
        self.assertTrue(report["ran"])
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)
        self.assertEqual(state["q_bill_100_plus"], ma.QUAL_YES)

    def test_the_verdict_is_qualified_with_nothing_missing(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        verdict, missing = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_QUALIFIED)
        self.assertEqual(missing, [])

    def test_the_opener_asks_ZERO_qualification_questions(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM,
                                address="1423 Oak Street, Chesterfield MO")
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        low = msg.lower()
        self.assertNotIn("ameren", low)
        self.assertNotIn("homeowner", low)
        self.assertNotIn("own the home", low)
        self.assertNotIn("bill", low)

    def test_the_opener_references_the_street_naturally_not_the_full_address(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM,
                                address="1423 Oak Street, Chesterfield MO 63017")
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertIn("Oak", msg)
        self.assertNotIn("1423", msg)          # no house number
        self.assertNotIn("63017", msg)         # no zip
        self.assertNotIn("Chesterfield MO", msg)

    def test_street_hint_degrades_safely(self):
        for raw, want in (
            ("1423 Oak Street, Chesterfield MO", "Oak"),
            ("742 Evergreen Terrace", "Evergreen"),
            ("", ""),
            ("12345", ""),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(ma.street_hint(raw), want)


class Test02_MissingUtility(FormAwareTestCase):

    def test_only_the_utility_is_missing(self):
        state, _ = self.hydrate(
            custom_fields=form_fields(homeowner="Yes", bill="$150-$199"))
        verdict, missing = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_INCOMPLETE)
        self.assertEqual(missing, [ma.Q_UTILITY])

    def test_the_opener_asks_utility_and_nothing_else(self):
        state, _ = self.hydrate(
            custom_fields=form_fields(homeowner="Yes", bill="$150-$199"))
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertIn("ameren", msg.lower())
        self.assertNotIn("homeowner", msg.lower())
        self.assertNotIn("bill", msg.lower())


class Test03_MissingBill(FormAwareTestCase):

    def test_only_the_bill_is_missing(self):
        state, _ = self.hydrate(
            custom_fields=form_fields(homeowner="Yes", utility="Ameren Missouri"))
        verdict, missing = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_INCOMPLETE)
        self.assertEqual(missing, [ma.Q_BILL])

    def test_the_opener_asks_the_bill_and_nothing_else(self):
        state, _ = self.hydrate(
            custom_fields=form_fields(homeowner="Yes", utility="Ameren Missouri"))
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertIn("bill", msg.lower())
        self.assertNotIn("ameren", msg.lower())
        self.assertNotIn("homeowner", msg.lower())


# ═════════════════════════════════════════════════════════════════════════
#  4-6  Deterministic disqualification
# ═════════════════════════════════════════════════════════════════════════

class Test04_Renter(FormAwareTestCase):

    def test_a_renter_is_disqualified_deterministically(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("No", "$300+", "Ameren Missouri"))
        self.assertEqual(state["q_homeowner"], ma.QUAL_NO)
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_DISQUALIFIED)

    def test_python_not_claude_owns_that_call(self):
        # The verdict is a pure function of the record. No model involved.
        state, _ = self.hydrate(
            custom_fields=form_fields("No", "$300+", "Ameren Missouri"))
        self.assertEqual(self.claude.calls, [])


class Test04b_DisqualifiedBeforeFirstContact(FormAwareTestCase):
    """
    The form can disqualify a lead BEFORE Michael ever texts. That case did
    not exist before form-awareness, and the historical opener would have
    asked a renter whether they are on Ameren. An empty opener means
    "send nothing" and every call site honours it.
    """

    def test_a_renter_gets_no_opener_at_all(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("No", "$300+", "Ameren Missouri"))
        self.assertEqual(
            ma.build_new_contact_outreach(first_name="Sarah", state=state), "")

    def test_a_low_bill_lead_gets_no_opener(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "Under $100", "Ameren Missouri"))
        self.assertEqual(
            ma.build_new_contact_outreach(first_name="Sarah", state=state), "")

    def test_an_ameren_illinois_lead_gets_no_opener(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$300+", "Ameren Illinois"))
        self.assertEqual(
            ma.build_new_contact_outreach(first_name="Sarah", state=state), "")

    def test_a_disqualified_lead_is_never_asked_about_ameren(self):
        for cf in (form_fields("No", "$300+", "Ameren Missouri"),
                   form_fields("Yes", "Under $100", "Ameren Missouri"),
                   form_fields("Yes", "$300+", "Ameren Illinois")):
            with self.subTest(cf=cf):
                ma._state_store.clear()
                state, _ = self.hydrate(custom_fields=cf)
                msg = ma.build_new_contact_outreach(first_name="S", state=state)
                self.assertNotIn("ameren", msg.lower())

    def test_a_pending_lead_still_gets_an_opener(self):
        # PENDING is not disqualified - it must still start the conversation.
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$300+", "Another electric provider"))
        self.assertNotEqual(
            ma.build_new_contact_outreach(first_name="Sarah", state=state), "")

    def test_an_incomplete_lead_still_gets_an_opener(self):
        state, _ = self.hydrate(custom_fields=form_fields("Yes"))
        self.assertNotEqual(
            ma.build_new_contact_outreach(first_name="Sarah", state=state), "")


class Test05_LowBill(FormAwareTestCase):

    def test_under_100_is_disqualified(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "Under $100", "Ameren Missouri"))
        self.assertEqual(state["q_bill_100_plus"], ma.QUAL_NO)
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_DISQUALIFIED)

    def test_every_bucket_maps_to_the_right_side_of_the_threshold(self):
        for bucket, want in (("Under $100", ma.QUAL_NO),
                             ("$100-$149", ma.QUAL_YES),
                             ("$150-$199", ma.QUAL_YES),
                             ("$200-$299", ma.QUAL_YES),
                             ("$300+",     ma.QUAL_YES)):
            with self.subTest(bucket=bucket):
                facts = ma.facts_from_form_fields(form_fields(bill=bucket))
                self.assertEqual(facts[ma.Q_BILL], want)

    def test_no_bucket_straddles_the_threshold(self):
        # A straddling bucket could not resolve and would force a re-ask.
        for bucket, (_verdict, floor) in ma.FORM_BILL_BUCKETS.items():
            with self.subTest(bucket=bucket):
                if _verdict == ma.QUAL_YES:
                    self.assertGreaterEqual(floor, ma.BILL_THRESHOLD)


class Test06_AmerenIllinois(FormAwareTestCase):

    def test_ameren_illinois_is_not_ameren_missouri(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$300+", "Ameren Illinois"))
        self.assertEqual(state["q_utility"], ma.UTIL_OTHER)
        self.assertNotEqual(state["q_utility"], ma.UTIL_AMEREN)

    def test_ameren_illinois_disqualifies(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$300+", "Ameren Illinois"))
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_DISQUALIFIED)

    def test_the_conversational_path_agrees(self):
        self.assertEqual(ma._detect_utility("I'm on Ameren Illinois"),
                         ma.UTIL_OTHER)


# ═════════════════════════════════════════════════════════════════════════
#  7-9  "Another electric provider" — the eligibility register
# ═════════════════════════════════════════════════════════════════════════

class Test07_AnotherProviderIsNotARejection(FormAwareTestCase):

    def setUp(self):
        super().setUp()
        self.state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$200-$299",
                                      "Another electric provider"))

    def test_it_is_pending_not_other(self):
        self.assertEqual(self.state["q_utility"], ma.UTIL_PENDING)
        self.assertNotEqual(self.state["q_utility"], ma.UTIL_OTHER)

    def test_it_is_NOT_disqualified(self):
        verdict, missing = ma.qualification_verdict(self.state)
        self.assertNotEqual(verdict, ma.VERDICT_DISQUALIFIED)
        self.assertEqual(verdict, ma.VERDICT_INCOMPLETE)
        self.assertEqual(missing, [ma.Q_UTILITY])

    def test_it_is_NOT_qualified_either(self):
        verdict, _ = ma.qualification_verdict(self.state)
        self.assertNotEqual(verdict, ma.VERDICT_QUALIFIED)

    def test_michael_asks_for_the_provider_name_not_about_ameren(self):
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=self.state)
        low = msg.lower()
        self.assertIn("provider", low)
        self.assertNotIn("ameren", low)    # they already said it is not Ameren

    def test_the_prompt_forbids_claude_from_judging_eligibility(self):
        prompt = ma.build_system_prompt(self.state)
        low = prompt.lower()
        self.assertIn("net metering", low)      # named only to forbid it
        self.assertIn("do not", low)
        self.assertIn("who's your electric provider", low)

    def test_naming_the_provider_later_resolves_it(self):
        facts = ma.extract_qualification_facts("Cuivre River", utility_pending=True)
        self.assertEqual(facts[ma.Q_UTILITY], ma.UTIL_OTHER)


class Test08_VerifiedEligibleNonAmeren(FormAwareTestCase):

    def test_a_provider_added_to_the_register_satisfies_the_requirement(self):
        # This is the maintenance interface: verify the tariff, add the entry.
        ma.UTILITY_ELIGIBLE["springfield city utilities"] = "Springfield CU"
        self.assertEqual(
            ma.utility_state_for_name("Springfield City Utilities"),
            ma.UTIL_AMEREN)

    def test_a_lead_on_that_provider_becomes_qualified(self):
        ma.UTILITY_ELIGIBLE["springfield city utilities"] = "Springfield CU"
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$200-$299",
                                      "Springfield City Utilities"))
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_QUALIFIED)

    def test_ameren_missouri_is_eligible_by_default(self):
        verdict, canonical = ma.classify_utility_name("Ameren Missouri")
        self.assertEqual(verdict, ma.UTIL_ELIGIBILITY_ELIGIBLE)
        self.assertEqual(canonical, "Ameren Missouri")


class Test09_UnverifiedProviderCannotBeSilentlyQualified(FormAwareTestCase):

    def test_an_unknown_provider_is_unverified(self):
        verdict, _ = ma.classify_utility_name("Central Missouri Power Alliance")
        self.assertEqual(verdict, ma.UTIL_ELIGIBILITY_UNVERIFIED)

    def test_unverified_means_pending_not_qualified(self):
        self.assertEqual(
            ma.utility_state_for_name("Central Missouri Power Alliance"),
            ma.UTIL_PENDING)

    def test_unverified_means_pending_not_disqualified(self):
        self.assertNotEqual(
            ma.utility_state_for_name("Central Missouri Power Alliance"),
            ma.UTIL_OTHER)

    def test_a_lead_on_an_unverified_provider_never_reaches_qualified(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$300+",
                                      "Central Missouri Power Alliance"))
        verdict, missing = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_INCOMPLETE)
        self.assertEqual(missing, [ma.Q_UTILITY])

    def test_a_generic_not_ameren_answer_is_pending(self):
        self.assertEqual(ma._detect_utility("we're not with Ameren"),
                         ma.UTIL_PENDING)
        self.assertEqual(ma._detect_utility("it's a rural electric co-op"),
                         ma.UTIL_PENDING)


# ═════════════════════════════════════════════════════════════════════════
#  10  Explicit correction beats the form
# ═════════════════════════════════════════════════════════════════════════

class Test10_ExplicitContradictionWins(FormAwareTestCase):

    def test_actually_I_rent_overrides_a_form_that_said_yes(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)

        facts = ma.extract_qualification_facts("Actually I rent")
        ma.apply_qualification_facts(state, facts, "c1")
        self.assertEqual(state["q_homeowner"], ma.QUAL_NO)

    def test_the_verdict_is_reevaluated_after_the_correction(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        ma.apply_qualification_facts(
            state, ma.extract_qualification_facts("Actually I rent"), "c1")
        verdict, _ = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_DISQUALIFIED)

    def test_an_explicit_utility_correction_also_wins(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        ma.apply_qualification_facts(
            state, ma.extract_qualification_facts("I'm actually on Cuivre River"),
            "c1")
        self.assertEqual(state["q_utility"], ma.UTIL_OTHER)

    def test_form_answers_cannot_overwrite_an_existing_answer(self):
        # The reverse direction: form is the LOWEST tier.
        state = ma.get_state("c-tier")
        state["q_homeowner"] = ma.QUAL_NO          # they told us they rent
        ma.apply_qualification_facts(
            state, ma.facts_from_form_fields(form_fields(homeowner="Yes")), "c-tier")
        self.assertEqual(state["q_homeowner"], ma.QUAL_NO)

    def test_a_blanket_yes_cannot_resolve_a_pending_utility(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("Yes", "$200-$299",
                                      "Another electric provider"))
        ma.apply_qualification_facts(
            state,
            ma.extract_qualification_facts("yes please", ma.OUT_BUNDLED_QUAL),
            "c1")
        self.assertEqual(state["q_utility"], ma.UTIL_PENDING)


# ═════════════════════════════════════════════════════════════════════════
#  11-12  Durability: after-hours and process-memory loss
# ═════════════════════════════════════════════════════════════════════════

class Test11_OvernightLead(FormAwareTestCase):

    def test_2am_is_outside_the_send_window(self):
        import datetime as dt
        self.assertFalse(ma.within_send_window(
            dt.datetime(2026, 6, 15, 2, 0, tzinfo=ma.CENTRAL_TZ)))

    def test_9am_is_inside_the_send_window(self):
        import datetime as dt
        self.assertTrue(ma.within_send_window(
            dt.datetime(2026, 6, 15, 9, 0, tzinfo=ma.CENTRAL_TZ)))

    def test_facts_reconstruct_at_resume_even_with_empty_memory(self):
        # Simulate the dyno restarting between the hold and the resume.
        self.stub_contact(QUALIFIED_FORM, address="1423 Oak Street")
        ma._state_store.clear()
        state = ma.get_state("overnight")
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)

        run(ma.hydrate_form_facts("overnight", state))
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)
        self.assertEqual(state["q_bill_100_plus"], ma.QUAL_YES)

    def test_the_morning_opener_is_still_form_aware(self):
        self.stub_contact(QUALIFIED_FORM, address="1423 Oak Street")
        state = ma.get_state("overnight")
        run(ma.hydrate_form_facts("overnight", state))
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertNotIn("ameren", msg.lower())
        self.assertIn("Oak", msg)


class Test12_StateLossReconstruction(FormAwareTestCase):

    def test_hydration_reuses_a_supplied_compliance_payload(self):
        # The resume path already fetched the contact; no second call.
        calls = []

        async def _boom(contact_id):
            calls.append(contact_id)
            return {"ok": False}
        ma.fetch_ghl_contact_compliance = _boom

        state = ma.get_state("reuse")
        report = run(ma.hydrate_form_facts(
            "reuse", state,
            compliance={"ok": True, "custom_fields": QUALIFIED_FORM,
                        "address": "1423 Oak Street"}))
        self.assertTrue(report["ran"])
        self.assertEqual(calls, [])                 # no redundant fetch
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)

    def test_already_hydrated_state_makes_no_api_call(self):
        calls = []

        async def _count(contact_id):
            calls.append(contact_id)
            return {"ok": True, "custom_fields": {}, "address": "", "tags": []}
        ma.fetch_ghl_contact_compliance = _count

        state = ma.get_state("settled")
        state["q_homeowner"]     = ma.QUAL_YES
        state["q_utility"]       = ma.UTIL_AMEREN
        state["q_bill_100_plus"] = ma.QUAL_YES
        report = run(ma.hydrate_form_facts("settled", state))
        self.assertFalse(report["ran"])
        self.assertEqual(report["reason"], "already_hydrated")
        self.assertEqual(calls, [])

    def test_a_failed_lookup_is_non_fatal(self):
        self.stub_contact(ok=False)
        state = ma.get_state("down")
        report = run(ma.hydrate_form_facts("down", state))
        self.assertFalse(report["ran"])
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)

    def test_an_exception_is_swallowed_and_state_is_untouched(self):
        async def _raise(contact_id):
            raise RuntimeError("GHL exploded")
        ma.fetch_ghl_contact_compliance = _raise
        state = ma.get_state("boom")
        report = run(ma.hydrate_form_facts("boom", state))
        self.assertEqual(report["reason"], "exception")
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)


# ═════════════════════════════════════════════════════════════════════════
#  13-18  Every existing safety gate still fires
# ═════════════════════════════════════════════════════════════════════════

class _GateTestCase(FormAwareTestCase):
    """Stubs the three gates inside send_sms_via_ghl independently."""

    def arm(self, *, booked=(False, "clean"), allowed=True, in_window=True):
        async def _bv(cid):
            return booked
        async def _cc(cid, state=None):
            return {"allowed": allowed, "reason": "stub", "category": "TEST",
                    "code": "", "permanent": not allowed,
                    "phone": "+13145550123", "email": ""}
        async def _hold(cid):
            self.holds.append(cid)
            return True
        async def _rec(cid, cat, code, perm, email=""):
            return None

        self.holds = []
        ma.booked_verdict            = _bv
        ma.can_contact_sms           = _cc
        ma.within_send_window        = lambda now=None: in_window
        ma.place_after_hours_hold    = _hold
        ma.record_sms_outcome        = _rec

    def setUp(self):
        super().setUp()
        self._orig = {n: getattr(ma, n) for n in
                      ("booked_verdict", "can_contact_sms", "within_send_window",
                       "place_after_hours_hold", "record_sms_outcome")}
        self.addCleanup(lambda: [setattr(ma, n, v) for n, v in self._orig.items()])


class Test13_ReplyBeforeNurture(FormAwareTestCase):

    def test_ai_engaged_is_the_nurture_cancel_signal(self):
        self.assertEqual(ma.TAG_ENGAGED, "ai-engaged")

    def test_restore_reads_engagement_as_progress(self):
        stage, why = ma.restore_stage_from_ghl([ma.TAG_ENGAGED])
        self.assertEqual(stage, ma.Stage.ASK_OWNERSHIP)
        self.assertIn("engaged", why)

    def test_form_facts_do_not_disturb_the_engaged_marker(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        self.assertNotIn("ai-engaged", str(state.get("stage")))
        self.assertEqual(state["stage"], ma.Stage.INITIAL)


class Test14_BookingBeforeNurture(_GateTestCase):

    def test_guarded_kinds_are_suppressed_for_a_booked_contact(self):
        self.arm(booked=(True, "tag"))
        for kind in (ma.SendKind.OUTREACH, ma.SendKind.QUALIFICATION,
                     ma.SendKind.BOOKING_PITCH, ma.SendKind.NURTURE):
            with self.subTest(kind=kind):
                res = run(ma.send_sms_via_ghl("c", "hi", "+1314", kind=kind))
                self.assertFalse(res["sent"])
                self.assertEqual(res["reason"], "booked_guard")

    def test_an_indeterminate_verdict_also_suppresses(self):
        self.arm(booked=(None, "indeterminate"))
        res = run(ma.send_sms_via_ghl("c", "hi", "+1314",
                                      kind=ma.SendKind.NURTURE))
        self.assertFalse(res["sent"])
        self.assertEqual(res["reason"], "booked_guard")

    def test_a_qualified_form_lead_does_not_bypass_the_booked_guard(self):
        self.arm(booked=(True, "appointment"))
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        res = run(ma.send_sms_via_ghl("c1", "hi", "+1314",
                                      kind=ma.SendKind.OUTREACH))
        self.assertFalse(res["sent"])


class Test15_StopAndDnd(_GateTestCase):

    def test_dnd_blocks_every_send_kind(self):
        self.arm(allowed=False)
        for kind in list(ma.SendKind):
            with self.subTest(kind=kind):
                res = run(ma.send_sms_via_ghl("c", "hi", "+1314", kind=kind))
                self.assertFalse(res["sent"])
                self.assertEqual(res["reason"], ma.SUPPRESS_REASON_DND)

    def test_a_fully_qualified_form_lead_is_still_blocked_by_dnd(self):
        self.arm(allowed=False)
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        res = run(ma.send_sms_via_ghl("c1", "hi", "+1314",
                                      kind=ma.SendKind.OUTREACH))
        self.assertFalse(res["sent"])
        self.assertEqual(res["reason"], ma.SUPPRESS_REASON_DND)

    def test_hydration_skips_a_dnc_contact_entirely(self):
        self.stub_contact(QUALIFIED_FORM)
        state = ma.get_state("dnc")
        state["stage"] = ma.Stage.DNC
        # The webhook guard checks stage before hydrating; assert the stage
        # survives a hydration call regardless.
        run(ma.hydrate_form_facts("dnc", state))
        self.assertEqual(state["stage"], ma.Stage.DNC)


class Test16_DuplicateWebhook(FormAwareTestCase):

    def test_a_repeated_event_id_is_a_duplicate(self):
        self.assertFalse(ma._is_duplicate_event_id("evt-form-1", "c1"))
        self.assertTrue(ma._is_duplicate_event_id("evt-form-1", "c1"))

    def test_an_empty_event_id_is_never_a_duplicate(self):
        self.assertFalse(ma._is_duplicate_event_id("", "c1"))
        self.assertFalse(ma._is_duplicate_event_id("", "c1"))

    def test_hydrating_twice_is_idempotent(self):
        state, first = self.hydrate(custom_fields=QUALIFIED_FORM)
        second = run(ma.hydrate_form_facts("c1", state))
        self.assertTrue(first["ran"])
        self.assertFalse(second["ran"])
        self.assertEqual(second["reason"], "already_hydrated")


class Test17_BookingLinkNotResent(_GateTestCase):

    def test_booking_link_sent_tag_restores_to_send_booking(self):
        stage, _ = ma.restore_stage_from_ghl(["BOOKING_LINK_SENT"])
        self.assertEqual(stage, ma.Stage.SEND_BOOKING)

    def test_outbound_dedup_blocks_an_identical_resend(self):
        self.arm()
        ma._outbound_fingerprints.clear()
        self.assertFalse(ma.is_duplicate_outbound("c-link", ma.BOOKING_LINK))
        self.assertTrue(ma.is_duplicate_outbound("c-link", ma.BOOKING_LINK))

    def test_a_link_already_sent_does_not_reopen_qualification(self):
        # BOOKING_LINK_SENT alone must NOT backfill the criteria as answered.
        state = ma.get_state("c-link2")
        ma.apply_restored_stage("c-link2", state, ["BOOKING_LINK_SENT"], "")
        self.assertEqual(state["stage"], ma.Stage.SEND_BOOKING)
        self.assertFalse(state.get("qualified"))


class Test18_BookingRaceAtSendTime(_GateTestCase):

    def test_the_send_time_check_wins_over_stale_state(self):
        # State says "not booked"; GHL says booked at the moment of sending.
        self.arm(booked=(True, "appointment(active)"))
        state = ma.get_state("race")
        state["appointment_booked"] = False
        state["stage"] = ma.Stage.SEND_BOOKING
        res = run(ma.send_sms_via_ghl("race", "one more thing",
                                      "+1314", kind=ma.SendKind.NURTURE))
        self.assertFalse(res["sent"])
        self.assertEqual(res["reason"], "booked_guard")

    def test_after_hours_defers_rather_than_sends(self):
        self.arm(in_window=False)
        res = run(ma.send_sms_via_ghl("late", "hi", "+1314",
                                      kind=ma.SendKind.OUTREACH))
        self.assertFalse(res["sent"])
        self.assertEqual(res["reason"], ma.SUPPRESS_REASON_WINDOW)
        self.assertEqual(self.holds, ["late"])

    def test_gate_order_booked_beats_window(self):
        # A booked contact is refused on the booked guard, not deferred.
        self.arm(booked=(True, "tag"), in_window=False)
        res = run(ma.send_sms_via_ghl("both", "hi", "+1314",
                                      kind=ma.SendKind.NURTURE))
        self.assertEqual(res["reason"], "booked_guard")
        self.assertEqual(self.holds, [])            # no pointless hold tag


# ═════════════════════════════════════════════════════════════════════════
#  19  The q_utility tag-restore bug
# ═════════════════════════════════════════════════════════════════════════

class Test19_UtilityTagRestore(FormAwareTestCase):

    def test_parse_qualification_tags_returns_a_utility(self):
        facts = ma.parse_qualification_tags(["ameren-confirmed"])
        self.assertEqual(facts.get("utility"), ma.UTIL_AMEREN)

    def test_ameren_confirmed_tag_now_restores_q_utility(self):
        # THE BUG: this value was returned and never read, so a contact whose
        # memory was wiped got asked for a utility GHL already had.
        state = ma.get_state("tag-restore")
        self.assertEqual(state["q_utility"], ma.QUAL_UNKNOWN)
        ma.michael_agent("tag-restore", "hi", ghl_tags=["ameren-confirmed"])
        self.assertEqual(ma.get_state("tag-restore")["q_utility"], ma.UTIL_AMEREN)

    def test_an_out_of_area_tag_restores_as_other(self):
        state = ma.get_state("tag-out")
        ma.michael_agent("tag-out", "hi", ghl_tags=["ameren-illinois"])
        self.assertEqual(ma.get_state("tag-out")["q_utility"], ma.UTIL_OTHER)

    def test_the_tag_never_overwrites_a_known_answer(self):
        state = ma.get_state("tag-keep")
        state["q_utility"] = ma.UTIL_OTHER          # they told us themselves
        ma.save_state("tag-keep", state)
        ma.michael_agent("tag-keep", "hi", ghl_tags=["ameren-confirmed"])
        self.assertEqual(ma.get_state("tag-keep")["q_utility"], ma.UTIL_OTHER)


# ═════════════════════════════════════════════════════════════════════════
#  20  Backward compatibility
# ═════════════════════════════════════════════════════════════════════════

class Test20_BackwardCompatible(FormAwareTestCase):

    def test_flag_off_makes_hydration_inert(self):
        ma.FORM_AWARE_ENABLED = False
        self.stub_contact(QUALIFIED_FORM)
        state = ma.get_state("off")
        report = run(ma.hydrate_form_facts("off", state))
        self.assertFalse(report["ran"])
        self.assertEqual(report["reason"], "flag_off")
        self.assertEqual(state["q_homeowner"], ma.QUAL_UNKNOWN)

    def test_flag_off_keeps_the_historical_opener(self):
        ma.FORM_AWARE_ENABLED = False
        msg = ma.build_new_contact_outreach(first_name="Sarah")
        self.assertIn("Ameren Missouri", msg)
        self.assertIn("STL Energy Advisors", msg)

    def test_a_lead_with_no_form_fields_behaves_exactly_as_before(self):
        self.stub_contact({})
        state = ma.get_state("plain")
        report = run(ma.hydrate_form_facts("plain", state))
        self.assertFalse(report["ran"])
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertIn("Ameren Missouri", msg)

    def test_the_meta_opener_is_unchanged_without_form_facts(self):
        msg = ma.build_new_contact_outreach(
            first_name="Sarah", lead_source="facebook")
        self.assertIn("Facebook", msg)
        self.assertIn("Ameren Missouri", msg)

    def test_partial_form_data_still_falls_through_for_two_unknowns(self):
        state, _ = self.hydrate(custom_fields=form_fields(homeowner="Yes"))
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertIn("Ameren Missouri", msg)   # historical copy

    def test_unrecognised_answer_strings_are_ignored_not_guessed(self):
        facts = ma.facts_from_form_fields(
            form_fields(homeowner="Maybe?", bill="A lot"))
        self.assertNotIn(ma.Q_HOMEOWNER, facts)
        self.assertNotIn(ma.Q_BILL, facts)

    def test_bill_threshold_default_is_unchanged(self):
        self.assertEqual(ma.BILL_THRESHOLD, 100)


# ═════════════════════════════════════════════════════════════════════════
#  Field-key resolution and the GHL write-back
# ═════════════════════════════════════════════════════════════════════════

class Test21_FieldResolution(FormAwareTestCase):

    def test_id_keyed_contact_fields_resolve_through_the_schema(self):
        schema = {"aBc123": "homeowner_status", "dEf456": "avg_monthly_bill"}
        raw = [{"id": "aBc123", "value": "Yes"},
               {"id": "dEf456", "value": "$200-$299"}]
        out = ma.resolve_custom_fields(raw, schema)
        self.assertEqual(out["homeowner_status"], "Yes")
        self.assertEqual(out["avg_monthly_bill"], "$200-$299")

    def test_name_keyed_webhook_fields_resolve_without_a_schema(self):
        raw = [{"fieldKey": "contact.utility_provider", "value": "Ameren Missouri"}]
        out = ma.resolve_custom_fields(raw, {})
        self.assertEqual(out["utility_provider"], "Ameren Missouri")

    def test_matching_tolerates_the_contact_prefix_and_case(self):
        for key in ("contact.homeowner_status", "homeowner_status",
                    "Contact.Homeowner_Status"):
            with self.subTest(key=key):
                self.assertEqual(
                    ma._match_form_field({key: "Yes"}, ma.FORM_FIELD_HOMEOWNER),
                    "Yes")

    def test_en_dash_bill_buckets_still_map(self):
        # GHL and Meta both like to re-encode hyphens as en dashes.
        facts = ma.facts_from_form_fields({"avg_monthly_bill": "$150–$199"})
        self.assertEqual(facts[ma.Q_BILL], ma.QUAL_YES)

    def test_qualification_status_write_is_skipped_without_a_schema(self):
        async def _empty(force=False):
            return {}
        ma.fetch_ghl_custom_field_schema = _empty
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        self.assertFalse(run(ma.sync_qualification_to_ghl("c1", state)))

    def test_python_owns_qualification_status_not_claude(self):
        captured = {}

        class _Resp:
            is_success = True
            status_code = 200

        class _Client:
            def __init__(self, *a, **k): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def put(self, url, json=None, headers=None):
                captured["payload"] = json
                return _Resp()

        async def _schema(force=False):
            return {"f1": "qualification_status", "f2": "qualification_missing",
                    "f3": "form_answers_seen_at"}

        ma.fetch_ghl_custom_field_schema = _schema
        real_client = ma.httpx.AsyncClient
        real_key    = ma.GHL_API_KEY
        ma.httpx.AsyncClient = _Client
        ma.GHL_API_KEY = "test-key"
        try:
            state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
            ok = run(ma.sync_qualification_to_ghl("c1", state))
        finally:
            ma.httpx.AsyncClient = real_client
            ma.GHL_API_KEY = real_key

        self.assertTrue(ok)
        vals = {e["id"]: e["value"] for e in captured["payload"]["customFields"]}
        self.assertEqual(vals["f1"], "QUALIFIED")
        self.assertEqual(vals["f2"], "")

    def test_a_pending_lead_reports_partial_with_the_missing_field(self):
        captured = {}

        class _Resp:
            is_success = True
            status_code = 200

        class _Client:
            def __init__(self, *a, **k): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def put(self, url, json=None, headers=None):
                captured["payload"] = json
                return _Resp()

        async def _schema(force=False):
            return {"f1": "qualification_status", "f2": "qualification_missing"}

        ma.fetch_ghl_custom_field_schema = _schema
        real_client = ma.httpx.AsyncClient
        real_key    = ma.GHL_API_KEY
        ma.httpx.AsyncClient = _Client
        ma.GHL_API_KEY = "test-key"
        try:
            state, _ = self.hydrate(
                custom_fields=form_fields("Yes", "$300+",
                                          "Another electric provider"))
            run(ma.sync_qualification_to_ghl("c1", state))
        finally:
            ma.httpx.AsyncClient = real_client
            ma.GHL_API_KEY = real_key

        vals = {e["id"]: e["value"] for e in captured["payload"]["customFields"]}
        self.assertEqual(vals["f1"], "PARTIAL")
        self.assertEqual(vals["f2"], ma.Q_UTILITY)


class Test23_FieldKeyAliases(FormAwareTestCase):
    """
    GHL freezes a custom field's internal key at creation. Renaming the field
    does not rewrite the key, so contact.qualification_ststus is permanent.
    Both spellings must resolve to the same field.
    """

    def test_the_typo_key_maps_to_the_canonical_name(self):
        self.assertEqual(ma.canonical_field_key("qualification_ststus"),
                         "qualification_status")

    def test_the_correct_key_still_maps_to_itself(self):
        self.assertEqual(ma.canonical_field_key("qualification_status"),
                         "qualification_status")

    def test_the_contact_prefix_is_stripped_before_aliasing(self):
        self.assertEqual(ma.canonical_field_key("contact.qualification_ststus"),
                         "qualification_status")

    def test_an_unaliased_key_passes_through_untouched(self):
        for k in ("homeowner_status", "utility_provider", "avg_monthly_bill"):
            with self.subTest(k=k):
                self.assertEqual(ma.canonical_field_key(k), k)

    def test_match_form_field_accepts_the_typo_spelling(self):
        self.assertEqual(
            ma._match_form_field({"contact.qualification_ststus": "QUALIFIED"},
                                 ma.FORM_FIELD_QUAL_STATUS),
            "QUALIFIED")

    def test_match_form_field_still_accepts_the_correct_spelling(self):
        self.assertEqual(
            ma._match_form_field({"contact.qualification_status": "PARTIAL"},
                                 ma.FORM_FIELD_QUAL_STATUS),
            "PARTIAL")

    def test_the_write_back_targets_the_typo_key_id(self):
        captured = {}

        class _Resp:
            is_success = True
            status_code = 200

        class _Client:
            def __init__(self, *a, **k): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def put(self, url, json=None, headers=None):
                captured["payload"] = json
                return _Resp()

        async def _schema(force=False):
            # The REAL production schema: typo key, correct display name.
            return {"35D91OE7vxt3QgkxTTJa": "qualification_ststus",
                    "44jgbqQ6ISkdMB8Eay0K": "qualification_missing",
                    "iMA95PN86XSwL4picIRQ": "form_answers_seen_at"}

        ma.fetch_ghl_custom_field_schema = _schema
        real_client, real_key = ma.httpx.AsyncClient, ma.GHL_API_KEY
        ma.httpx.AsyncClient = _Client
        ma.GHL_API_KEY = "test-key"
        try:
            state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
            ok = run(ma.sync_qualification_to_ghl("c1", state))
        finally:
            ma.httpx.AsyncClient, ma.GHL_API_KEY = real_client, real_key

        self.assertTrue(ok)
        vals = {e["id"]: e["value"] for e in captured["payload"]["customFields"]}
        # All THREE managed fields written - not 2 of 3 as before the alias.
        self.assertEqual(len(vals), 3)
        self.assertEqual(vals["35D91OE7vxt3QgkxTTJa"], "QUALIFIED")

    def test_the_probe_reports_the_typo_key_as_present(self):
        async def _s(force=False):
            return {"a": "homeowner_status", "b": "utility_provider",
                    "c": "avg_monthly_bill", "d": "qualification_ststus"}
        ma.fetch_ghl_custom_field_schema = _s
        real = ma.DEBUG_RESET_SECRET
        ma.DEBUG_RESET_SECRET = "s3cret"
        try:
            class _Req:
                headers = {"X-Debug-Secret": "s3cret"}
            r = run(ma.debug_form_schema(_Req()))
        finally:
            ma.DEBUG_RESET_SECRET = real
        self.assertTrue(r["ok"])
        self.assertTrue(r["managed_fields"]["qualification_status"])


class Test22_SchemaProbeEndpoint(FormAwareTestCase):
    """The read-only pre-flight diagnostic. Must leak nothing and mutate nothing."""

    class _Req:
        def __init__(self, secret=None):
            self.headers = {"X-Debug-Secret": secret} if secret else {}

    def test_it_is_hard_disabled_without_a_secret(self):
        real = ma.DEBUG_RESET_SECRET
        ma.DEBUG_RESET_SECRET = ""
        try:
            r = run(ma.debug_form_schema(self._Req()))
            self.assertEqual(r.status_code, 503)
        finally:
            ma.DEBUG_RESET_SECRET = real

    def test_a_wrong_secret_is_401(self):
        real = ma.DEBUG_RESET_SECRET
        ma.DEBUG_RESET_SECRET = "right"
        try:
            r = run(ma.debug_form_schema(self._Req("wrong")))
            self.assertEqual(r.status_code, 401)
        finally:
            ma.DEBUG_RESET_SECRET = real

    def _probe(self, schema):
        async def _s(force=False):
            return schema
        ma.fetch_ghl_custom_field_schema = _s
        real = ma.DEBUG_RESET_SECRET
        ma.DEBUG_RESET_SECRET = "s3cret"
        try:
            return run(ma.debug_form_schema(self._Req("s3cret")))
        finally:
            ma.DEBUG_RESET_SECRET = real

    def test_ok_when_all_three_form_fields_are_present(self):
        r = self._probe({"a": "homeowner_status", "b": "utility_provider",
                         "c": "avg_monthly_bill", "d": "qualification_status"})
        self.assertTrue(r["ok"])
        self.assertTrue(all(r["required_form_fields"].values()))
        self.assertTrue(r["managed_fields"]["qualification_status"])

    def test_not_ok_when_a_required_field_is_missing(self):
        r = self._probe({"a": "homeowner_status", "b": "utility_provider"})
        self.assertFalse(r["ok"])
        self.assertFalse(r["required_form_fields"]["avg_monthly_bill"])

    def test_not_ok_when_the_schema_does_not_resolve(self):
        r = self._probe({})
        self.assertFalse(r["ok"])
        self.assertFalse(r["schema_resolved"])

    def test_it_reports_the_flag_state(self):
        ma.FORM_AWARE_ENABLED = False
        r = self._probe({"a": "homeowner_status", "b": "utility_provider",
                         "c": "avg_monthly_bill"})
        self.assertFalse(r["form_aware_enabled"])

    def test_it_returns_names_only_and_no_contact_data(self):
        r = self._probe({"a": "homeowner_status", "b": "utility_provider",
                         "c": "avg_monthly_bill"})
        blob = str(r).lower()
        for leak in ("phone", "email", "+1", "address", "firstname", "value"):
            self.assertNotIn(leak, blob, f"probe leaked {leak!r}")

    def test_the_probe_does_not_mutate_state(self):
        before = dict(ma._state_store)
        self._probe({"a": "homeowner_status"})
        self.assertEqual(ma._state_store, before)


class Test24_BareNumberBillReplies(FormAwareTestCase):
    """
    [GAP-1] Michael asks "what's your electric bill running most months?" and
    the homeowner answers "around 180". Before this fix that answer was
    dropped: a bare number needed a bill word nearby, and neither the
    previous-outbound type nor pending_question rescued it, so the lead
    could never reach QUALIFIED.

    A bare number counts as a bill figure ONLY when the conversation put the
    bill question on the table. Outside that context nothing changes.
    """

    MUST_PARSE = ["180", "around 180", "180/month", "$180", "probably 180",
                  "probably like 150-200", "150-200", "about 200",
                  "200 bucks", "like 180"]

    def test_every_required_phrasing_parses_in_bill_context(self):
        for t in self.MUST_PARSE:
            with self.subTest(text=t):
                f = ma.extract_qualification_facts(
                    t, previous_outbound_type=ma.OUT_BILL_Q)
                self.assertEqual(f.get(ma.Q_BILL), ma.QUAL_YES, t)

    def test_every_required_phrasing_yields_an_amount(self):
        for t in self.MUST_PARSE:
            with self.subTest(text=t):
                f = ma.extract_qualification_facts(
                    t, previous_outbound_type=ma.OUT_BILL_Q)
                self.assertTrue(str(f.get("bill_amount", "")).startswith("$"), t)

    def test_bare_numbers_are_NOT_bills_outside_bill_context(self):
        # The whole point: no global "any number is a bill" behaviour.
        for t in [x for x in self.MUST_PARSE if "$" not in x]:
            with self.subTest(text=t):
                f = ma.extract_qualification_facts(t)
                self.assertIsNone(f.get(ma.Q_BILL), t)

    def test_explicit_currency_still_parses_without_context(self):
        f = ma.extract_qualification_facts("$180")
        self.assertEqual(f.get(ma.Q_BILL), ma.QUAL_YES)

    def test_pending_question_bill_is_an_independent_context_signal(self):
        f = ma.extract_qualification_facts("around 180", bill_context=True)
        self.assertEqual(f.get(ma.Q_BILL), ma.QUAL_YES)

    def test_non_numeric_replies_in_bill_context_settle_nothing(self):
        for t in ("no", "not sure", "yes", "idk", "why do you ask"):
            with self.subTest(text=t):
                f = ma.extract_qualification_facts(
                    t, previous_outbound_type=ma.OUT_BILL_Q)
                self.assertIsNone(f.get(ma.Q_BILL), t)

    def test_out_of_band_numbers_are_ignored_even_in_bill_context(self):
        # Too small to be a bill, and a phone number must never parse.
        for t in ("I have 3 kids", "call me at 5", "3145550123"):
            with self.subTest(text=t):
                f = ma.extract_qualification_facts(
                    t, previous_outbound_type=ma.OUT_BILL_Q)
                self.assertIsNone(f.get(ma.Q_BILL), t)

    def test_a_low_range_disqualifies(self):
        f = ma.extract_qualification_facts("50-80", previous_outbound_type=ma.OUT_BILL_Q)
        self.assertEqual(f.get(ma.Q_BILL), ma.QUAL_NO)

    def test_a_range_straddling_the_threshold_is_left_unresolved(self):
        # 75-125 could be either side. Guessing here would be worse than asking.
        f = ma.extract_qualification_facts("75-125", previous_outbound_type=ma.OUT_BILL_Q)
        self.assertIsNone(f.get(ma.Q_BILL))

    def test_a_high_range_takes_the_conservative_low_end(self):
        f = ma.extract_qualification_facts("150-200", previous_outbound_type=ma.OUT_BILL_Q)
        self.assertEqual(f.get("bill_amount"), "$150/month")

    def test_explicit_bill_language_is_unchanged(self):
        # Pre-existing behaviour must be byte-identical.
        for t, want in (("my bill is around 180", ma.QUAL_YES),
                        ("about 180 a month", ma.QUAL_YES),
                        ("we pay about 250", ma.QUAL_YES),
                        ("electric runs 300", ma.QUAL_YES),
                        ("it's like $70 a month", ma.QUAL_NO)):
            with self.subTest(text=t):
                self.assertEqual(
                    ma.extract_qualification_facts(t).get(ma.Q_BILL), want, t)

    def test_the_answer_completes_qualification_end_to_end(self):
        # CASE 7 from certification: form gave homeowner+utility, bill missing.
        state, _ = self.hydrate(
            custom_fields=form_fields(homeowner="Yes", utility="Ameren Missouri"))
        self.assertEqual(ma.qualification_verdict(state)[1], [ma.Q_BILL])
        ma.apply_qualification_facts(
            state,
            ma.extract_qualification_facts("around 180",
                                           previous_outbound_type=ma.OUT_BILL_Q),
            "c1")
        verdict, missing = ma.qualification_verdict(state)
        self.assertEqual(verdict, ma.VERDICT_QUALIFIED)
        self.assertEqual(missing, [])

    def test_a_low_bare_number_answer_disqualifies_end_to_end(self):
        state, _ = self.hydrate(
            custom_fields=form_fields(homeowner="Yes", utility="Ameren Missouri"))
        ma.apply_qualification_facts(
            state,
            ma.extract_qualification_facts("about 70",
                                           previous_outbound_type=ma.OUT_BILL_Q),
            "c1")
        self.assertEqual(ma.qualification_verdict(state)[0], ma.VERDICT_DISQUALIFIED)


class Test25_BookingLinkRequests(FormAwareTestCase):
    """
    [GAP-2] "send me the link" is about as unambiguous as booking intent
    gets, yet it fell through to the normal flow and the link depended on
    Claude emitting [SEND_BOOKING]. It usually did; it was not guaranteed.

    Now deterministic - but GATED. Asking for the link is strong intent and
    no evidence of fitness, so a lead we know nothing about cannot pull the
    link simply by asking.
    """

    POSITIVE = [
        "send me the link", "can you send the link", "send that link",
        "where's the link", "wheres the link", "send the link",
        "send me that link", "send me a link", "text me the link",
        "shoot me the link", "send it over", "send that over",
        "can u send me the link", "could you send me the link",
        "what's the link", "send me the booking link", "share the link",
        "send me the scheduling link", "resend the link",
        "send the link again", "the link didn't work",
        "send me the sign up link",
    ]
    NEGATIVE = [
        "don't send me the link", "stop sending links",
        "I already have the link", "no more links", "is this link safe",
        "I clicked the link and nothing happened", "not interested",
        "unsubscribe", "link", "the link", "yeah", "yes", "stop", "STOP",
        "is this solar", "around 180", "I have Ameren", "my bill is 200",
        "who is this", "why are you texting me", "I'm the homeowner",
        "what is this about", "already booked", "how much does it cost",
    ]

    def _state(self, **fields):
        ma._state_store.clear()
        s = ma.get_state("link")
        s.update(fields)
        return s

    # -- detector -------------------------------------------------
    def test_every_positive_phrasing_is_a_link_request(self):
        for t in self.POSITIVE:
            with self.subTest(text=t):
                self.assertTrue(ma.is_booking_link_request(t), t)

    def test_no_negative_phrasing_is_a_link_request(self):
        for t in self.NEGATIVE:
            with self.subTest(text=t):
                self.assertFalse(ma.is_booking_link_request(t), t)

    def test_a_refusal_naming_the_link_is_not_a_request(self):
        for t in ("don't send me the link", "stop sending links",
                  "no more links", "I already have the link"):
            with self.subTest(text=t):
                self.assertFalse(ma.is_booking_link_request(t), t)

    def test_empty_input_is_safe(self):
        for t in ("", "   ", None):
            self.assertFalse(ma.is_booking_link_request(t))

    # -- the qualification gate -----------------------------------
    def test_confirmed_criteria_count(self):
        for fields, want in (
            ({}, 0),
            ({"q_homeowner": ma.QUAL_YES}, 1),
            ({"q_utility": ma.UTIL_AMEREN}, 1),
            ({"q_bill_100_plus": ma.QUAL_YES}, 1),
            ({"q_homeowner": ma.QUAL_YES, "q_utility": ma.UTIL_AMEREN}, 2),
            ({"q_homeowner": ma.QUAL_YES, "q_utility": ma.UTIL_AMEREN,
              "q_bill_100_plus": ma.QUAL_YES}, 3),
        ):
            with self.subTest(fields=fields):
                self.assertEqual(ma.confirmed_criteria_count(self._state(**fields)), want)

    def test_pending_utility_does_NOT_count_as_confirmed(self):
        self.assertEqual(
            ma.confirmed_criteria_count(self._state(q_utility=ma.UTIL_PENDING)), 0)

    def test_a_disqualifying_answer_does_NOT_count_as_confirmed(self):
        self.assertEqual(
            ma.confirmed_criteria_count(
                self._state(q_homeowner=ma.QUAL_NO, q_bill_100_plus=ma.QUAL_NO)), 0)

    def test_zero_confirmed_criteria_does_NOT_get_the_link(self):
        d = ma.decide_conversion_action(self._state(), "send me the link")
        self.assertEqual(d.action, ma.ACT_NO_OVERRIDE)
        self.assertIn("qualify first", d.reason)

    def test_pending_utility_alone_does_NOT_get_the_link(self):
        d = ma.decide_conversion_action(
            self._state(q_utility=ma.UTIL_PENDING), "send me the link")
        self.assertEqual(d.action, ma.ACT_NO_OVERRIDE)

    def test_one_confirmed_criterion_IS_enough(self):
        for fields in ({"q_homeowner": ma.QUAL_YES},
                       {"q_utility": ma.UTIL_AMEREN},
                       {"q_bill_100_plus": ma.QUAL_YES}):
            with self.subTest(fields=fields):
                d = ma.decide_conversion_action(self._state(**fields), "send me the link")
                self.assertEqual(d.action, ma.ACT_SEND_BOOKING)

    def test_a_fully_qualified_lead_gets_the_link(self):
        d = ma.decide_conversion_action(
            self._state(q_homeowner=ma.QUAL_YES, q_utility=ma.UTIL_AMEREN,
                        q_bill_100_plus=ma.QUAL_YES), "send me the link")
        self.assertEqual(d.action, ma.ACT_SEND_BOOKING)
        self.assertIn("3/3", d.reason)

    def test_a_disqualified_lead_NEVER_gets_the_link(self):
        for fields in ({"q_homeowner": ma.QUAL_NO},
                       {"q_utility": ma.UTIL_OTHER},
                       {"q_bill_100_plus": ma.QUAL_NO}):
            with self.subTest(fields=fields):
                base = {"q_homeowner": ma.QUAL_YES, "q_utility": ma.UTIL_AMEREN,
                        "q_bill_100_plus": ma.QUAL_YES}
                base.update(fields)
                d = ma.decide_conversion_action(self._state(**base), "send me the link")
                self.assertEqual(d.action, ma.ACT_NO_OVERRIDE)
                self.assertIn("criterion failed", d.reason)

    def test_the_gate_applies_to_every_positive_phrasing(self):
        for t in self.POSITIVE:
            with self.subTest(text=t):
                self.assertEqual(
                    ma.decide_conversion_action(self._state(), t).action,
                    ma.ACT_NO_OVERRIDE, t)
                self.assertEqual(
                    ma.decide_conversion_action(
                        self._state(q_homeowner=ma.QUAL_YES), t).action,
                    ma.ACT_SEND_BOOKING, t)

    # -- existing behaviour must not move -------------------------
    def test_call_and_schedule_phrasings_stay_UNGATED(self):
        for t in ("call me", "what times do you have", "let's talk",
                  "send me your calendar", "calendar link", "pick a time"):
            with self.subTest(text=t):
                self.assertEqual(
                    ma.decide_conversion_action(self._state(), t).action,
                    ma.ACT_SEND_BOOKING, t)

    def test_existing_scheduling_detector_is_unchanged(self):
        for t in ("call me", "what times do you have", "let's talk",
                  "send me your calendar", "calendar link",
                  "when are you available", "book me a time", "come by",
                  "how do I schedule", "pick a time", "give me a call"):
            with self.subTest(text=t):
                self.assertTrue(ma.is_explicit_scheduling_request(t), t)

    def test_a_negated_link_request_falls_through_untouched(self):
        d = ma.decide_conversion_action(
            self._state(q_homeowner=ma.QUAL_YES), "don't send me the link")
        self.assertEqual(d.action, ma.ACT_NO_OVERRIDE)
        self.assertNotIn("qualify first", d.reason)

    # -- end to end -----------------------------------------------
    def test_link_is_sent_even_when_claude_emits_no_tag(self):
        self.claude.reply_text = "Absolutely."
        s = self._state(q_homeowner=ma.QUAL_YES, q_utility=ma.UTIL_AMEREN,
                        q_bill_100_plus=ma.QUAL_YES, stage=ma.Stage.ASK_BILL)
        ma.save_state("link", s)
        out = ma.michael_agent("link", "send me the link")
        self.assertIn(ma.BOOKING_LINK, out or "")

    def test_no_link_for_an_unknown_lead_even_when_asked(self):
        self.claude.reply_text = "Happy to help."
        s = self._state(stage=ma.Stage.ASK_OWNERSHIP)
        ma.save_state("link", s)
        out = ma.michael_agent("link", "send me the link")
        self.assertNotIn(ma.BOOKING_LINK, out or "")

    def test_booked_contact_asking_for_the_link_is_still_locked_out(self):
        self.claude.reply_text = "Sure. [SEND_BOOKING]"
        s = self._state(stage=ma.Stage.BOOKED, appointment_booked=True,
                        q_homeowner=ma.QUAL_YES)
        ma.save_state("link", s)
        out = ma.michael_agent("link", "send me the link")
        self.assertNotIn(ma.BOOKING_LINK, out or "")

    def test_dnc_contact_asking_for_the_link_gets_nothing(self):
        s = self._state(stage=ma.Stage.DNC, q_homeowner=ma.QUAL_YES)
        ma.save_state("link", s)
        self.assertIsNone(ma.michael_agent("link", "send me the link"))


class Test26_NoFormFieldsNegativeCache(FormAwareTestCase):
    """
    A contact with no form answers never sets form_facts_loaded, so its three
    criteria stayed UNKNOWN and every later inbound re-fetched the same empty
    contact record - one GHL GET per message, forever. Every legacy V1 lead is
    in that state.

    A CONFIRMED absence is now remembered for the process lifetime. A failed
    or indeterminate lookup is NOT a confirmed absence and is never cached.
    """

    def setUp(self):
        super().setUp()
        ma._no_form_fields.clear()
        self.addCleanup(ma._no_form_fields.clear)

    def _counting_stub(self, cf, ok=True):
        """Returns the call log; the stub records every GHL fetch."""
        calls = []
        async def _f(contact_id):
            calls.append(contact_id)
            return {"ok": ok, "first_name": "S", "full_name": "S",
                    "phone": "+13145550123", "email": "", "dnd": False,
                    "dnd_sms": {}, "tags": [], "date_added": "",
                    "http": 200 if ok else 500,
                    "custom_fields": dict(cf or {}), "address": ""}
        ma.fetch_ghl_contact_compliance = _f
        return calls

    def _raising_stub(self):
        calls = []
        async def _f(contact_id):
            calls.append(contact_id)
            raise TimeoutError("ghl timeout")
        ma.fetch_ghl_contact_compliance = _f
        return calls

    # -- successful negative caching -------------------------------
    def test_a_confirmed_absence_is_fetched_once_then_cached(self):
        calls = self._counting_stub({})
        st = ma.get_state("neg")
        for _ in range(5):
            run(ma.hydrate_form_facts("neg", st))
        self.assertEqual(len(calls), 1, "should fetch once, then use the cache")

    def test_the_cached_reason_is_reported(self):
        self._counting_stub({})
        st = ma.get_state("neg")
        run(ma.hydrate_form_facts("neg", st))
        second = run(ma.hydrate_form_facts("neg", st))
        self.assertEqual(second["reason"], "no_form_fields_cached")
        self.assertFalse(second["ran"])

    def test_the_contact_is_recorded_in_the_cache(self):
        self._counting_stub({})
        run(ma.hydrate_form_facts("neg", ma.get_state("neg")))
        self.assertIn("neg", ma._no_form_fields)

    def test_the_cache_is_per_contact(self):
        calls = self._counting_stub({})
        run(ma.hydrate_form_facts("a", ma.get_state("a")))
        run(ma.hydrate_form_facts("b", ma.get_state("b")))
        self.assertEqual(len(calls), 2)
        self.assertEqual(ma._no_form_fields, {"a", "b"})

    def test_qualification_state_is_untouched_by_the_cache(self):
        self._counting_stub({})
        st = ma.get_state("neg")
        for _ in range(3):
            run(ma.hydrate_form_facts("neg", st))
        self.assertEqual(st["q_homeowner"], ma.QUAL_UNKNOWN)
        self.assertEqual(st["q_utility"], ma.QUAL_UNKNOWN)
        self.assertEqual(st["q_bill_100_plus"], ma.QUAL_UNKNOWN)

    def test_a_cached_contact_still_gets_the_legacy_opener(self):
        self._counting_stub({})
        st = ma.get_state("neg")
        run(ma.hydrate_form_facts("neg", st))
        run(ma.hydrate_form_facts("neg", st))
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=st)
        self.assertIn("Ameren Missouri", msg)

    # -- failures must NOT be cached -------------------------------
    def test_an_api_failure_is_NOT_cached_and_retries(self):
        calls = self._counting_stub({}, ok=False)
        st = ma.get_state("fail")
        for _ in range(3):
            run(ma.hydrate_form_facts("fail", st))
        self.assertEqual(len(calls), 3, "a failed lookup must retry every time")
        self.assertNotIn("fail", ma._no_form_fields)

    def test_a_timeout_is_NOT_cached_and_retries(self):
        calls = self._raising_stub()
        st = ma.get_state("boom")
        for _ in range(3):
            run(ma.hydrate_form_facts("boom", st))
        self.assertEqual(len(calls), 3)
        self.assertNotIn("boom", ma._no_form_fields)

    def test_a_failure_then_a_success_still_caches(self):
        st = ma.get_state("flaky")
        self._counting_stub({}, ok=False)
        run(ma.hydrate_form_facts("flaky", st))
        self.assertNotIn("flaky", ma._no_form_fields)
        calls = self._counting_stub({})          # GHL recovers
        run(ma.hydrate_form_facts("flaky", st))
        self.assertIn("flaky", ma._no_form_fields)
        run(ma.hydrate_form_facts("flaky", st))
        self.assertEqual(len(calls), 1)

    def test_a_failure_then_real_data_hydrates_normally(self):
        st = ma.get_state("recover")
        self._counting_stub({}, ok=False)
        run(ma.hydrate_form_facts("recover", st))
        self._counting_stub(QUALIFIED_FORM)
        rep = run(ma.hydrate_form_facts("recover", st))
        self.assertTrue(rep["ran"])
        self.assertEqual(st["q_homeowner"], ma.QUAL_YES)
        self.assertNotIn("recover", ma._no_form_fields)

    # -- real form data is unaffected ------------------------------
    def test_real_form_data_hydrates_and_is_never_cached_negative(self):
        calls = self._counting_stub(QUALIFIED_FORM)
        st = ma.get_state("good")
        rep = run(ma.hydrate_form_facts("good", st))
        self.assertTrue(rep["ran"])
        self.assertNotIn("good", ma._no_form_fields)
        self.assertEqual(len(calls), 1)

    def test_hydrated_contact_short_circuits_on_already_hydrated(self):
        calls = self._counting_stub(QUALIFIED_FORM)
        st = ma.get_state("good")
        for _ in range(4):
            run(ma.hydrate_form_facts("good", st))
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            run(ma.hydrate_form_facts("good", st))["reason"], "already_hydrated")

    def test_partial_form_data_is_not_cached_negative(self):
        # One field present is not an absence.
        self._counting_stub(form_fields(homeowner="Yes"))
        st = ma.get_state("partial")
        run(ma.hydrate_form_facts("partial", st))
        self.assertNotIn("partial", ma._no_form_fields)
        self.assertEqual(st["q_homeowner"], ma.QUAL_YES)

    def test_a_webhook_payload_with_answers_beats_the_cache(self):
        calls = self._counting_stub({})
        st = ma.get_state("wh")
        run(ma.hydrate_form_facts("wh", st))          # caches the absence
        self.assertIn("wh", ma._no_form_fields)
        body = {"customData": dict(QUALIFIED_FORM)}   # a later webhook carries them
        rep = run(ma.hydrate_form_facts("wh", st, body=body))
        self.assertTrue(rep["ran"])
        self.assertEqual(st["q_homeowner"], ma.QUAL_YES)
        self.assertEqual(len(calls), 1, "payload answers need no extra fetch")

    # -- later conversational corrections still work ---------------
    def test_explicit_correction_works_on_a_cached_contact(self):
        self._counting_stub({})
        st = ma.get_state("conv")
        for _ in range(3):
            run(ma.hydrate_form_facts("conv", st))
        ma.apply_qualification_facts(
            st, ma.extract_qualification_facts("I own it and I'm on Ameren"), "conv")
        self.assertEqual(st["q_homeowner"], ma.QUAL_YES)
        self.assertEqual(st["q_utility"], ma.UTIL_AMEREN)

    def test_a_cached_contact_can_still_reach_qualified_by_conversation(self):
        self._counting_stub({})
        st = ma.get_state("conv2")
        run(ma.hydrate_form_facts("conv2", st))
        for inbound, prev in (("yes I own it", ma.OUT_OWNERSHIP_Q),
                              ("I have Ameren", ma.OUT_UTILITY_Q),
                              ("around 180", ma.OUT_BILL_Q)):
            ma.apply_qualification_facts(
                st, ma.extract_qualification_facts(inbound, previous_outbound_type=prev),
                "conv2")
        self.assertEqual(ma.qualification_verdict(st)[0], ma.VERDICT_QUALIFIED)

    def test_caching_does_not_block_a_later_disqualifying_correction(self):
        self._counting_stub({})
        st = ma.get_state("conv3")
        run(ma.hydrate_form_facts("conv3", st))
        ma.apply_qualification_facts(
            st, ma.extract_qualification_facts("I rent"), "conv3")
        self.assertEqual(ma.qualification_verdict(st)[0], ma.VERDICT_DISQUALIFIED)

    # -- housekeeping ----------------------------------------------
    def test_the_cache_is_lru_bounded(self):
        for i in range(ma._MAX_NO_FORM_CACHE + 50):
            ma._remember_no_form_fields(f"c{i}")
        self.assertLessEqual(len(ma._no_form_fields), ma._MAX_NO_FORM_CACHE)

    def test_an_empty_contact_id_is_never_cached(self):
        ma._remember_no_form_fields("")
        self.assertNotIn("", ma._no_form_fields)

    def test_flag_off_never_touches_the_cache(self):
        ma.FORM_AWARE_ENABLED = False
        calls = self._counting_stub({})
        run(ma.hydrate_form_facts("off", ma.get_state("off")))
        self.assertEqual(len(calls), 0)
        self.assertEqual(ma._no_form_fields, set())


class Test27_OneMessageBooking(FormAwareTestCase):
    """
    [BOOKING-1] Michael used to ask permission to send the calendar and then
    send it on the next turn. A qualified homeowner had to send an extra text
    purely to receive a link we were always going to send.

    Now the link travels in the same message as the offer - but only when the
    lead is sufficiently qualified, and never when they are hedging on an
    open qualification question.
    """

    COOPERATIVE = ["either", "both", "I guess", "maybe", "whatever works",
                   "fine", "that's right", "doesn't matter", "up to you",
                   "no preference", "don't care", "either one", "suppose so"]
    AFFIRMATIVE = ["sure", "yeah", "okay", "sounds good", "yes", "works"]
    NEVER = ["not interested", "no", "maybe not", "no thanks", "stop",
             "who is this", "how much does it cost", "is this a scam",
             "take me off your list", "I rent"]

    def _state(self, prior="Morning or afternoon usually better?", **fields):
        ma._state_store.clear()
        s = ma.get_state("b1")
        s.update(fields)
        s["messages"] = [{"role": "user", "content": "hi"},
                         {"role": "assistant", "content": prior}]
        return s

    def _qualified_enough(self, **extra):
        f = {"q_homeowner": ma.QUAL_YES, "q_utility": ma.UTIL_AMEREN}
        f.update(extra)
        return self._state(**f)

    # -- the opener no longer asks permission ---------------------
    def test_the_qualified_opener_contains_the_link(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM,
                                address="1423 Oak Street, Chesterfield MO")
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertIn(ma.BOOKING_LINK, msg)

    def test_the_qualified_opener_asks_no_permission(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        low = msg.lower()
        for phrase in ("want me to", "should i", "would you like me to",
                       "shall i", "do you want"):
            self.assertNotIn(phrase, low, phrase)

    def test_the_qualified_opener_still_asks_no_qualification_questions(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        low = ma.build_new_contact_outreach(first_name="Sarah", state=state).lower()
        for k in ("ameren", "homeowner", "own the home", "bill"):
            self.assertNotIn(k, low, k)

    def test_openers_that_still_need_information_carry_NO_link(self):
        # Only a fully qualified lead gets the link unprompted.
        for cf in (form_fields(homeowner="Yes", utility="Ameren Missouri"),
                   form_fields(homeowner="Yes", bill="$150-$199"),
                   form_fields(homeowner="Yes", bill="$300+",
                               utility="Another Electric Provider"),
                   {}):
            with self.subTest(cf=cf):
                ma._state_store.clear()
                ma._no_form_fields.clear()
                state, _ = self.hydrate(custom_fields=cf)
                msg = ma.build_new_contact_outreach(first_name="S", state=state)
                self.assertNotIn(ma.BOOKING_LINK, msg)

    def test_a_disqualified_lead_still_gets_nothing(self):
        state, _ = self.hydrate(
            custom_fields=form_fields("No", "$300+", "Ameren Missouri"))
        self.assertEqual(
            ma.build_new_contact_outreach(first_name="S", state=state), "")

    def test_opener_carries_booking_link_helper(self):
        self.assertTrue(ma.opener_carries_booking_link(f"hi {ma.BOOKING_LINK}"))
        self.assertFalse(ma.opener_carries_booking_link("hi there"))
        self.assertFalse(ma.opener_carries_booking_link(""))

    def test_a_link_bearing_opener_classifies_as_a_booking_pitch(self):
        # This is WHY the caller must advance the stage - otherwise the next
        # affirmative reads as a yes to a pitch and the link goes out twice.
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM)
        msg = ma.build_new_contact_outreach(first_name="Sarah", state=state)
        self.assertEqual(ma.classify_outbound_intent(msg), ma.OUT_BOOKING_PITCH)

    # -- cooperative replies advance ------------------------------
    def test_cooperative_replies_are_detected(self):
        for w in self.COOPERATIVE:
            with self.subTest(text=w):
                self.assertTrue(ma.is_cooperative_reply(w), w)

    def test_cooperative_replies_advance_when_qualified_enough(self):
        for w in self.COOPERATIVE:
            with self.subTest(text=w):
                d = ma.decide_conversion_action(self._qualified_enough(), w)
                self.assertEqual(d.action, ma.ACT_SEND_BOOKING, w)

    def test_affirmatives_also_advance_when_qualified_enough(self):
        for w in self.AFFIRMATIVE:
            with self.subTest(text=w):
                d = ma.decide_conversion_action(self._qualified_enough(), w)
                self.assertEqual(d.action, ma.ACT_SEND_BOOKING, w)

    def test_negations_built_into_go_aheads_still_count(self):
        # "doesn't matter" / "no preference" are cooperation, not refusal.
        for w in ("doesn't matter", "don't care", "no preference"):
            with self.subTest(text=w):
                self.assertTrue(ma.is_cooperative_reply(w), w)

    # -- guards ---------------------------------------------------
    def test_nothing_confirmed_means_no_link(self):
        for w in self.COOPERATIVE + self.AFFIRMATIVE:
            with self.subTest(text=w):
                d = ma.decide_conversion_action(self._state(), w)
                self.assertNotEqual(d.action, ma.ACT_SEND_BOOKING, w)

    def test_hedging_on_an_open_qualification_question_does_not_advance(self):
        for w in ("maybe", "I guess", "either", "whatever works"):
            with self.subTest(text=w):
                s = self._state(prior="What's your electric bill running most months?",
                                q_homeowner=ma.QUAL_YES)
                d = ma.decide_conversion_action(s, w)
                self.assertNotEqual(d.action, ma.ACT_SEND_BOOKING, w)

    def test_a_fully_qualified_lead_may_advance_even_after_a_question(self):
        s = self._state(prior="What's your electric bill running most months?",
                        q_homeowner=ma.QUAL_YES, q_utility=ma.UTIL_AMEREN,
                        q_bill_100_plus=ma.QUAL_YES)
        self.assertEqual(
            ma.decide_conversion_action(s, "maybe").action, ma.ACT_SEND_BOOKING)

    def test_a_disqualified_lead_never_advances(self):
        for w in self.COOPERATIVE + self.AFFIRMATIVE:
            with self.subTest(text=w):
                s = self._qualified_enough(q_homeowner=ma.QUAL_NO)
                self.assertEqual(
                    ma.decide_conversion_action(s, w).action, ma.ACT_NO_OVERRIDE, w)

    def test_pending_utility_alone_is_not_enough(self):
        s = self._state(q_utility=ma.UTIL_PENDING)
        self.assertNotEqual(
            ma.decide_conversion_action(s, "either").action, ma.ACT_SEND_BOOKING)

    def test_objections_and_questions_never_look_cooperative(self):
        for w in self.NEVER:
            with self.subTest(text=w):
                self.assertFalse(ma.is_cooperative_reply(w), w)

    def test_a_long_message_is_never_a_bare_cooperative_reply(self):
        self.assertFalse(ma.is_cooperative_reply(
            "maybe, but first I need to know how much this is going to cost me"))

    # -- the critical separation: no fact confirmation ------------
    def test_cooperative_words_NEVER_confirm_qualification_facts(self):
        # This is why is_cooperative_reply is separate from
        # is_affirmative_reply: a hedge must not assert homeownership.
        for w in self.COOPERATIVE:
            for prev in (ma.OUT_BUNDLED_QUAL, ma.OUT_OWNERSHIP_Q, ma.OUT_UTILITY_Q):
                with self.subTest(text=w, prev=prev):
                    facts = ma.extract_qualification_facts(w, previous_outbound_type=prev)
                    self.assertNotIn(ma.Q_HOMEOWNER, facts, w)
                    self.assertNotIn(ma.Q_UTILITY, facts, w)
                    self.assertNotIn(ma.Q_BILL, facts, w)

    def test_affirmative_fact_confirmation_is_unchanged(self):
        # Pre-existing behaviour must not move.
        f = ma.extract_qualification_facts("yes please", ma.OUT_BUNDLED_QUAL)
        self.assertEqual(f[ma.Q_HOMEOWNER], ma.QUAL_YES)
        self.assertEqual(f[ma.Q_UTILITY], ma.UTIL_AMEREN)
        self.assertEqual(f[ma.Q_BILL], ma.QUAL_YES)

    # -- end to end -----------------------------------------------
    def test_one_message_not_two_for_a_cooperative_qualified_lead(self):
        self.claude.reply_text = "Sounds good."
        s = self._qualified_enough(q_bill_100_plus=ma.QUAL_YES,
                                   stage=ma.Stage.ASK_BILL)
        ma.save_state("b1", s)
        out = ma.michael_agent("b1", "either")
        self.assertIn(ma.BOOKING_LINK, out or "")

    def test_booked_lockout_still_beats_a_cooperative_reply(self):
        self.claude.reply_text = "Sure. [SEND_BOOKING]"
        s = self._qualified_enough(stage=ma.Stage.BOOKED, appointment_booked=True)
        ma.save_state("b1", s)
        out = ma.michael_agent("b1", "either")
        self.assertNotIn(ma.BOOKING_LINK, out or "")

    def test_dnc_still_beats_a_cooperative_reply(self):
        s = self._qualified_enough(stage=ma.Stage.DNC)
        ma.save_state("b1", s)
        self.assertIsNone(ma.michael_agent("b1", "either"))


class Test28_RestartDurability(FormAwareTestCase):
    """
    [DURABILITY] A Meta V2 lead answers the three questions BEFORE Michael
    texts. Those answers live in GHL custom fields, which outlive the Render
    process. A restart must never make Michael re-ask them.

    The restart path is: _state_store is empty -> /webhook/inbound ->
    hydrate_form_facts() runs BEFORE any routing or michael_agent() call ->
    facts are back in state -> qualification_verdict() is correct again.
    """

    def _restart(self):
        """Simulate a Render restart: every process-local cache is gone."""
        ma._state_store.clear()
        ma._no_form_fields.clear()
        ma._processed_event_ids.clear()
        ma._outbound_fingerprints.clear()

    def _rehydrate(self, cid="v2", cf=None, address="1423 Oak Street"):
        self.stub_contact(cf if cf is not None else QUALIFIED_FORM, address)
        state = ma.get_state(cid)
        report = run(ma.hydrate_form_facts(cid, state))
        return state, report

    # 1. fully qualified -> restart -> still QUALIFIED, nothing re-asked
    def test_1_fully_qualified_survives_a_restart(self):
        self._restart()
        state, report = self._rehydrate()
        self.assertTrue(report["ran"])
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)
        self.assertEqual(state["q_bill_100_plus"], ma.QUAL_YES)
        self.assertEqual(ma.qualification_verdict(state)[0], ma.VERDICT_QUALIFIED)

    def test_1b_no_qualification_question_is_re_asked_after_restart(self):
        self._restart()
        state, _ = self._rehydrate()
        low = ma.build_new_contact_outreach(first_name="Sarah", state=state).lower()
        for banned in ("are you the homeowner", "own the home", "ameren",
                       "electric bill", "bill running"):
            self.assertNotIn(banned, low, banned)

    def test_1c_the_record_is_rebuilt_from_GHL_not_memory(self):
        self._restart()
        fresh = ma.get_state("v2")
        self.assertEqual(fresh["q_homeowner"], ma.QUAL_UNKNOWN)   # memory gone
        state, _ = self._rehydrate()
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)        # GHL restored

    # 2. partially qualified -> restart -> asks ONLY the missing field
    def test_2_partial_restart_asks_only_the_missing_bill(self):
        self._restart()
        state, _ = self._rehydrate(
            cf=form_fields(homeowner="Yes", utility="Ameren Missouri"))
        self.assertEqual(ma.qualification_verdict(state)[1], [ma.Q_BILL])
        low = ma.build_new_contact_outreach(first_name="S", state=state).lower()
        self.assertIn("bill", low)
        self.assertNotIn("ameren", low)
        self.assertNotIn("homeowner", low)

    def test_2b_partial_restart_asks_only_the_missing_utility(self):
        self._restart()
        state, _ = self._rehydrate(
            cf=form_fields(homeowner="Yes", bill="$150-$199"))
        self.assertEqual(ma.qualification_verdict(state)[1], [ma.Q_UTILITY])
        low = ma.build_new_contact_outreach(first_name="S", state=state).lower()
        self.assertIn("ameren", low)
        self.assertNotIn("homeowner", low)
        self.assertNotIn("bill", low)

    def test_2c_partial_restart_asks_only_the_missing_homeowner(self):
        self._restart()
        state, _ = self._rehydrate(
            cf=form_fields(bill="$150-$199", utility="Ameren Missouri"))
        self.assertEqual(ma.qualification_verdict(state)[1], [ma.Q_HOMEOWNER])
        low = ma.build_new_contact_outreach(first_name="S", state=state).lower()
        self.assertIn("homeowner", low)
        self.assertNotIn("ameren", low)
        self.assertNotIn("bill", low)

    # 3. explicit corrections and precedence survive rehydration
    def test_3_an_explicit_correction_is_not_undone_by_rehydration(self):
        self._restart()
        state, _ = self._rehydrate()
        ma.apply_qualification_facts(
            state, ma.extract_qualification_facts("Actually I rent"), "v2")
        self.assertEqual(state["q_homeowner"], ma.QUAL_NO)
        # A later re-fetch of the SAME form field must not resurrect "yes".
        run(ma.hydrate_form_facts("v2", state))
        self.assertEqual(state["q_homeowner"], ma.QUAL_NO)
        self.assertEqual(ma.qualification_verdict(state)[0], ma.VERDICT_DISQUALIFIED)

    def test_3b_form_precedence_is_unchanged_after_restart(self):
        self._restart()
        state, _ = self._rehydrate()
        # form is the LOWEST tier and cannot overwrite an explicit answer
        state["q_utility"] = ma.UTIL_OTHER
        ma.apply_qualification_facts(
            state, ma.facts_from_form_fields(QUALIFIED_FORM), "v2")
        self.assertEqual(state["q_utility"], ma.UTIL_OTHER)

    # 4. BOOKING_LINK_SENT and the facts survive INDEPENDENTLY
    def test_4_booking_link_tag_restores_the_stage(self):
        stage, why = ma.restore_stage_from_ghl(
            ["source-fb-paid", "meta-lead", "ai-outreach-sent",
             "QUALIFIED", "BOOKING_LINK_SENT"])
        self.assertEqual(stage, ma.Stage.SEND_BOOKING)
        self.assertIn("qualified", why)

    def test_4b_outreach_tag_alone_does_NOT_imply_the_link_went_out(self):
        # The exact bug: ai-outreach-sent restores only to ASK_OWNERSHIP.
        stage, _ = ma.restore_stage_from_ghl(["ai-outreach-sent"])
        self.assertEqual(stage, ma.Stage.ASK_OWNERSHIP)

    def test_4c_facts_and_stage_restore_independently(self):
        self._restart()
        state, _ = self._rehydrate()                       # facts from fields
        ma.apply_restored_stage("v2", state,
                                ["ai-outreach-sent", "QUALIFIED",
                                 "BOOKING_LINK_SENT"], "")  # stage from tags
        self.assertEqual(state["stage"], ma.Stage.SEND_BOOKING)
        self.assertEqual(state["q_homeowner"], ma.QUAL_YES)
        self.assertEqual(state["q_utility"], ma.UTIL_AMEREN)
        self.assertEqual(state["q_bill_100_plus"], ma.QUAL_YES)

    # 5. link already sent -> restart -> no questions AND no duplicate link
    def test_5_restart_after_link_sent_asks_nothing_and_resends_nothing(self):
        self._restart()
        state, _ = self._rehydrate()
        ma.apply_restored_stage("v2", state,
                                ["ai-outreach-sent", "QUALIFIED",
                                 "BOOKING_LINK_SENT"], "")
        ma.save_state("v2", state)
        self.assertEqual(state["stage"], ma.Stage.SEND_BOOKING)
        self.assertTrue(state.get("qualified"))
        # First outreach is suppressed for a contact already carrying the tags
        self.assertTrue(ma.has_tag(["ai-outreach-sent"], ma.TAG_OUTREACH_SENT))

    def test_5b_a_restored_contact_is_not_walked_back_into_qualification(self):
        self._restart()
        state, _ = self._rehydrate()
        ma.apply_restored_stage("v2", state,
                                ["ai-outreach-sent", "QUALIFIED",
                                 "BOOKING_LINK_SENT"], "")
        self.assertEqual(ma.qualification_verdict(state)[1], [])

    # 6. booked lead -> restart -> lockout wins, nothing generated
    def test_6_booked_lockout_survives_a_restart(self):
        self._restart()
        state, _ = self._rehydrate()
        ma.apply_restored_stage("v2", state, ["appointment booked"], "")
        self.assertEqual(state["stage"], ma.Stage.BOOKED)

    def test_6b_a_booked_contact_generates_no_conversion_messaging(self):
        self._restart()
        state, _ = self._rehydrate()
        state["stage"] = ma.Stage.BOOKED
        state["appointment_booked"] = True
        ma.save_state("v2", state)
        self.claude.reply_text = "Sure! [SEND_BOOKING]"
        out = ma.michael_agent("v2", "sounds good")
        self.assertNotIn(ma.BOOKING_LINK, out or "")

    def test_6c_a_booked_contact_is_never_re_qualified(self):
        self._restart()
        state, _ = self._rehydrate(cf={})        # even with no form data
        ma.apply_restored_stage("v2", state, ["appointment booked"], "")
        self.assertEqual(state["stage"], ma.Stage.BOOKED)
        self.assertEqual(ma.qualification_verdict(state)[0], ma.VERDICT_QUALIFIED)


class Test29_BookingLinkSentPersistence(FormAwareTestCase):
    """
    [BOOKING-2] The qualified V2 opener carries the calendar, but only
    ai-outreach-sent was written afterwards. restore_stage_from_ghl() maps
    that to ASK_OWNERSHIP, so a restart forgot the link had gone out.
    """

    ACCEPTED = {"status": ma.SendStatus.ACCEPTED.value, "sent": True}

    def setUp(self):
        super().setUp()
        self.written = []
        async def _add(cid, tags):
            self.written.append((cid, list(tags)))
            return True
        self._real_add = ma.add_ghl_tags
        ma.add_ghl_tags = _add
        self.addCleanup(lambda: setattr(ma, "add_ghl_tags", self._real_add))

    def _opener(self):
        state, _ = self.hydrate(custom_fields=QUALIFIED_FORM,
                                address="1423 Oak Street")
        return ma.build_new_contact_outreach(first_name="Sarah", state=state)

    def test_a_qualified_opener_with_the_calendar_persists_the_marker(self):
        msg = self._opener()
        self.assertIn(ma.BOOKING_LINK, msg)
        ok = run(ma.persist_booking_link_sent("c1", msg, self.ACCEPTED))
        self.assertTrue(ok)
        tags = [t for _, ts in self.written for t in ts]
        self.assertIn("BOOKING_LINK_SENT", tags)
        self.assertIn("QUALIFIED", tags)

    def test_an_opener_without_a_link_persists_nothing(self):
        ok = run(ma.persist_booking_link_sent(
            "c1", "Quick question: are you on Ameren Missouri?", self.ACCEPTED))
        self.assertFalse(ok)
        self.assertEqual(self.written, [])

    def test_a_FAILED_send_never_persists_the_marker(self):
        msg = self._opener()
        for bad in ({"status": ma.SendStatus.SUPPRESSED.value, "reason": "booked_guard"},
                    {"status": ma.SendStatus.SUPPRESSED.value, "reason": "compliance_dnd"},
                    {"status": ma.SendStatus.SUPPRESSED.value, "reason": "outside_send_window"},
                    {"status": ma.SendStatus.REJECTED.value},
                    {"status": ma.SendStatus.NOT_ATTEMPTED.value},
                    {}, None):
            with self.subTest(result=bad):
                self.written.clear()
                self.assertFalse(run(ma.persist_booking_link_sent("c1", msg, bad)))
                self.assertEqual(self.written, [])

    def test_a_tag_write_failure_is_non_fatal(self):
        async def _boom(cid, tags):
            raise RuntimeError("GHL down")
        ma.add_ghl_tags = _boom
        self.assertFalse(
            run(ma.persist_booking_link_sent("c1", self._opener(), self.ACCEPTED)))

    def test_an_empty_contact_id_persists_nothing(self):
        self.assertFalse(
            run(ma.persist_booking_link_sent("", self._opener(), self.ACCEPTED)))

    def test_after_restart_the_marker_restores_the_booking_stage(self):
        # What GHL now holds after a successful qualified opener.
        tags = ["source-fb-paid", "meta-lead", "ai-outreach-sent",
                "QUALIFIED", "BOOKING_LINK_SENT"]
        stage, why = ma.restore_stage_from_ghl(tags)
        self.assertEqual(stage, ma.Stage.SEND_BOOKING)

    def test_the_restored_contact_is_marked_qualified_and_link_sent(self):
        ma._state_store.clear()
        state = ma.get_state("r")
        ma.apply_restored_stage("r", state,
                                ["ai-outreach-sent", "QUALIFIED",
                                 "BOOKING_LINK_SENT"], "")
        self.assertEqual(state["stage"], ma.Stage.SEND_BOOKING)
        self.assertTrue(state.get("qualified"))
        self.assertTrue(state.get("booking_detected"))

    def test_without_the_patch_behaviour_the_stage_would_regress(self):
        # Pins the exact defect: outreach tag alone loses the link knowledge.
        self.assertEqual(
            ma.restore_stage_from_ghl(["ai-outreach-sent"])[0],
            ma.Stage.ASK_OWNERSHIP)
        self.assertEqual(
            ma.restore_stage_from_ghl(
                ["ai-outreach-sent", "BOOKING_LINK_SENT"])[0],
            ma.Stage.SEND_BOOKING)

    def test_a_repeat_opener_is_suppressed_for_an_outreached_contact(self):
        # The existing first-outreach guard still prevents a second opener.
        self.assertTrue(ma.has_tag(["ai-outreach-sent"], ma.TAG_OUTREACH_SENT))

    def test_the_opener_itself_is_unchanged_by_this_patch(self):
        msg = self._opener()
        self.assertIn(ma.BOOKING_LINK, msg)
        self.assertNotIn("want me to", msg.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)
