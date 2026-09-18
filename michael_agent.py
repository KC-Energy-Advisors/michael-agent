"""
michael_agent.py  ·  v3.8  (dedup rewrite: 8-second window + GHL message-ID tier)
────────────────────────────────────────────────────────────────────────────────
STL Energy Advisors — AI Appointment-Setting Agent
  (customer-facing brand: STL Energy Advisors)
Agent Name : Michael
Model      : Claude (claude-opus-4-6)
Platform   : GoHighLevel (GHL) via inbound webhook
────────────────────────────────────────────────────────────────────────────────

STL market migration: customer-facing copy updated from KC/Evergy to STL/Ameren.
Backend routes/env/GHL identifiers intentionally unchanged.
────────────────────────────────────────────────────────────────────────────────

HOW IT WORKS
────────────
1.  GHL fires a webhook to /webhook/inbound when a contact sends an SMS
    or when a chat widget / calendar form is submitted.
2.  For form submissions (new unbooked leads), Michael fires a predefined
    first-outreach SMS (introduces Michael/STL Energy Advisors, asks UTILITY
    first) and sets stage to ASK_OWNERSHIP.
3.  For inbound SMS (lead replies), michael_agent() loads state, builds a
    dynamic context-aware system prompt, calls Claude, parses the reply,
    updates state, and sends the response via GHL's Conversations API.
4.  State is persisted in memory so Michael always knows what's been
    confirmed and never repeats a question.

QUALIFICATION STAGES
────────────────────
  INITIAL        → brand-new contact, never messaged
  ASK_OWNERSHIP  → first outreach sent; qualification in progress
                   (area asked first, then ownership, then bill)
  ASK_LOCATION   → legacy label; area-first flow uses ASK_OWNERSHIP throughout
  ASK_BILL       → area confirmed; asking about monthly electric bill
  SEND_BOOKING   → qualified; booking link sent
  BOOKED         → appointment confirmed — hold until consult
  DISQUALIFIED   → failed a qualifier — stop messaging
  DNC            → STOP/opt-out received — never contact again

ENTRY PATHS  (v2.9)
───────────────────
  PATH 1: CHAT WIDGET LEAD
    Lead submits chat widget → qualification SMS flow → booking link sent
    → they book → bill reminder (warm continuation tone) → bill received → "See you then."
    State tag: entry_path = "chat_widget"

  PATH 2: DIRECT CALENDAR BOOKING
    Lead books directly via calendar page (no prior SMS conversation)
    → bill reminder (fresh intro tone) → bill received → "See you then."
    State tag: entry_path = "direct_booking"

  The system NEVER resets a chat-widget lead after booking.
  entry_path is set at first contact and never overwritten.
  Phone-number lookup prevents state loss when appointment webhook
  arrives with a different GHL contact_id than the chat session.

CHANGES IN v3.7  (middleware + uvicorn access log + flush audit)
──────────────────────────────────────────────────────────────────────────
  ROOT CAUSE ADDRESSED: ngrok shows 200 OK but Python terminal shows nothing.
  Even with v3.6's stdout fixes, uvicorn can suppress its own access log when
  log_level is not set explicitly, and the app has no middleware to log requests
  before the route handler fires.  Two additions:

  [FIX-7.A]  FastAPI @app.middleware("http") — fires for EVERY request
             Prints: [MW] ▶ TIMESTAMP | METHOD PATH
             Prints: [MW] ◀ METHOD PATH → STATUS_CODE (Xms)
             This guarantees terminal output even if the route handler itself
             raises before printing anything.  Uses flush=True on every line.

  [FIX-7.B]  uvicorn.run(..., access_log=True, log_level="info")
             Forces uvicorn to emit its built-in access log lines (the ones
             that look like: INFO:     127.0.0.1:NNNNN - "POST /webhook/inbound
             HTTP/1.1" 200 OK).  These appear in stdout via uvicorn's own
             logging machinery regardless of anything the app does.

CHANGES IN v3.6  (raw-first parse + stdout fix + unknown-event fallback SMS)
──────────────────────────────────────────────────────────────────────────
  ROOT CAUSE FIXED: Chat widget webhooks hit ngrok (200 OK) but Python
  terminal showed NOTHING and no SMS was sent.  Two bugs in combination:

  BUG 1 — Silent parse failure / invisible early return
  ──────────────────────────────────────────────────────
    OLD CODE:
      try:
          body = await request.json()
      except Exception as parse_err:
          log.warning(f"Failed to parse webhook JSON: {parse_err}")  ← stderr only
          return JSONResponse({"status": "success", "reason": "invalid_json"})

    GHL chat widget webhooks can arrive as URL-encoded form data
    (application/x-www-form-urlencoded) or with an empty body depending
    on your GHL workflow configuration.  request.json() raises, the catch
    calls log.warning() (which goes to stderr, invisible in most terminals),
    and the function returns 200 immediately.  Zero print() statements ran.
    Zero SMS was sent.  The terminal was silent.  Booking webhooks worked
    because GHL sends them as proper JSON — they never hit the parse error.

  BUG 2 — Python stdout buffering swallows print() output
  ─────────────────────────────────────────────────────────
    When uvicorn is started with --reload or run as a subprocess (common
    in development), Python defaults to full-buffering on stdout.  print()
    calls accumulate in a 4–8 KB buffer before flushing.  If the process
    handles a short request and no other output occurs, you never see the
    print() lines — they sit in the buffer until the next flush event.

  [FIX-6.A]  sys.stdout.reconfigure(line_buffering=True) at startup
             Forces stdout to flush after every newline, identical to
             interactive terminal behavior.  A single line at module init.

  [FIX-6.B]  logging.basicConfig → StreamHandler(sys.stdout)
             Python's default logging writes to stderr.  Redirected to
             stdout so log.info() / log.warning() / log.error() appear
             in the same stream as print() — no more invisible warnings.

  [FIX-6.C]  Raw-first parse — NEVER silently drop a webhook
             New parse section in inbound_webhook():
               1. await request.body() — capture raw bytes IMMEDIATELY,
                  print them with flush=True as the very first terminal output
               2. json.loads(raw_bytes) — fast JSON attempt
               3. urllib.parse.parse_qs() — URL-form fallback
               4. raw-string wrapper — last resort, never drops payload
             Also absorbs query-string params (GHL sometimes passes
             contact_id as ?contactId=xxx with empty body).
             The raw body banner (### WEBHOOK HIT ###) now appears even
             if every subsequent parse and routing step fails.

  [FIX-6.D]  Unknown-event fallback SMS
             If msg_type is still "unknown" after all routing branches,
             and the contact is at INITIAL stage with a phone number,
             the first-outreach SMS is sent anyway with a clear
             "[FALLBACK]" log label.  Previously: silent no-op.

SCENARIO VERIFICATION:
  A  GHL sends JSON chat widget webhook → json.loads() succeeds →
     banner printed → routing → outreach SMS sent ✓
  B  GHL sends URL-form chat widget webhook → json.loads() fails →
     parse_qs() fallback → banner printed → routing → outreach SMS ✓
  C  GHL sends empty body with query params → body={} from qs →
     banner printed → fallback SMS attempt for INITIAL contacts ✓
  D  uvicorn --reload subprocess → stdout line-buffered → all
     print() lines visible immediately in terminal ✓
  E  Booking webhook (always JSON) → unaffected ✓

CHANGES IN v3.5  (ownership-first outreach + INITIAL-stage Claude bypass)
──────────────────────────────────────────────────────────────────────
  ROOT CAUSE FIXED: A fresh chat widget lead (name + phone + address, no
  booking event) was receiving a generic closing confirmation like "You're
  all set. Based on what you shared, you may qualify for solar savings in
  your area. One of our specialists will reach out shortly..." instead of
  an actual qualification question.
  Root cause: when a widget lead's opening message was a real sentence
  (not a GHL placeholder like "start"), detect_payload_type() returned
  "chat_widget" → michael_agent() (Claude) was called → Claude generated
  a soft close with no continuation path.

  [FIX-5.A]  INITIAL-stage Claude bypass (chat_widget real-message path)
             For any chat widget lead at Stage.INITIAL — regardless of
             whether their payload type is "form_submission" or "chat_widget"
             — Claude is NEVER called for the first response.  Michael
             always drives the opening message via build_new_contact_outreach().
             Logged as NEW_LEAD_PATH.  Claude only enters on reply 2+.

  [FIX-5.B]  _cw_state_booked guard bypassed for fresh form submissions
             The SAFETY GUARD that blocks booked contacts from re-entering
             qualification now respects _is_fresh_form_sub.  When a fresh
             widget lead's phone resolves (via _resolve_contact_id_by_phone)
             to a previously booked contact, the stale booked state is reset
             to INITIAL before the outreach is sent.  The new lead gets a
             fresh qualification flow, not a silent skip.

  [FIX-5.C]  build_new_contact_outreach() — STL/Ameren utility-first format
             New message format:
               "Hey {first}, this is Michael with STL Energy Advisors.
                Got your info — your home may be worth taking a look at.
                Quick question: are you on Ameren Missouri for electric?"
             (legacy ownership-first format prior to this rev:
                Do you own the home at {street}?"
             The area/service-area question is eliminated from the outreach.
             location_confirmed is set to True by both the form_submission
             and the chat_widget INITIAL paths so the system prompt never
             instructs Claude to ask about area again.

  [FIX-5.D]  System prompt updated — ownership-first qualification order
             QUALIFICATION ORDER now starts with OWN THE HOME (step 1)
             instead of SERVICE AREA.  Area is treated as confirmed-at-
             form-time.  The fallback current_goal for unknown/edge-case
             state now defaults to "ASK OWNERSHIP" not "ASK AREA".

SCENARIO VERIFICATION:
  A  message="start" (placeholder), stage=INITIAL → form_submission path →
     build_new_contact_outreach() → ownership question with address ✓
  B  message="I want to qualify" (real), stage=INITIAL → chat_widget path →
     [FIX-5.A] intercept → build_new_contact_outreach() → ownership question ✓
     Claude is NOT called → no generic closing message ✓
  C  Lead replies "Yes I own it" → stage=ASK_OWNERSHIP, loc_conf=True →
     system prompt goal: ASK BILL → Claude asks bill amount ✓
  D  Widget lead whose phone maps to stale BOOKED contact →
     [FIX-5.B] stale state reset → INITIAL → ownership outreach sent ✓
  E  Real appointment webhook → _is_fresh_form_sub=False →
     booked guard NOT bypassed → BOOKED_PATH unchanged ✓

CHANGES IN v3.4  (fresh-lead booked-path guard + GHL 400 fix)
────────────────────────────────────────────────────────────
  ROOT CAUSE FIXED (1): A chat widget lead with a real message like "I want to
  see if I qualify for solar savings" was entering entered_booked_path=True and
  receiving skip_reason=booked_followup_sms_exception.
  Root cause: stale in-memory state (appointment_booked=True or stage=BOOKED)
  from a prior contact with the same phone number (or from _resolve_contact_id_
  by_phone redirecting to a previously booked contact) caused _state_says_booked
  to fire, which set _is_booked_contact=True even for a fresh widget inquiry.

  ROOT CAUSE FIXED (2): GHL's Conversations API was returning 400 Bad Request
  because the toNumber field was absent from the payload whenever the caller
  passed an empty phone string.  GHL requires toNumber for every SMS send.

  [FIX-4.A]  Fresh widget/form submissions MUST NOT enter booked path
             _is_lead_payload is now computed BEFORE _is_booked_contact.
             When _is_lead_payload=True AND _payload_says_booked=False (no
             explicit booking fields like appointmentId/calendarId/startTime+
             endTime in the current payload), _is_booked_contact is forced
             to False regardless of tags or in-memory state.
             _booked_override_reason is printed in [ROUTING] log when active.
             The ONLY way a widget-identified lead can enter the booked path is
             if the same webhook also contains explicit booking proof fields.

  [FIX-4.B]  send_sms_via_ghl() — always-present toNumber + debug logging
             • New fetch_ghl_contact_phone(contact_id) helper: calls
               GET /contacts/{contactId} to resolve phone when not in payload.
             • toNumber is now ALWAYS included in the outbound SMS payload.
               If to_number arg is empty, fetch_ghl_contact_phone is called
               first; the resolved value is used (or "" logged clearly).
             • Pre-flight log now shows: full endpoint URL, redacted API key
               tail (last 8 chars), exact JSON payload via json.dumps(), and
               fromNumber / toNumber with ✓ or ⚠ indicators.
             • Response log now shows: status code, full parsed response body.
             • On non-2xx: prints "!! EXACT GHL ERROR" banner with the full
               GHL error body and a checklist of the most common 400 causes.

  [FIX-4.C]  Path classification logging
             [ROUTING] block now ends with a "→ PATH CLASS" line for every
             route decision:
               NEW_LEAD_PATH | contact=... | phone=... | name=...
               BOOKED_PATH   | contact=... | phone=... | name=...
               NO_OP         | contact=... | phone=... | name=...

SCENARIO VERIFICATION:
  A  Fresh widget lead, message="I want to qualify" → _is_lead_payload=True,
     _payload_says_booked=False → _is_fresh_form_sub=True → _is_booked_contact
     forced False → NEW_LEAD_PATH even if prior state had appointment_booked ✓
  B  Widget lead with appointmentId in payload → _payload_says_booked=True →
     _is_fresh_form_sub=False → normal booked detection applies → BOOKED_PATH ✓
  C  Real appointment webhook, no widget signals → _is_lead_payload=False →
     _is_fresh_form_sub=False → normal booked detection → BOOKED_PATH ✓
  D  send_sms_via_ghl with empty to_number → fetch_ghl_contact_phone() called →
     phone resolved from GHL API → toNumber always in payload → no more 400 ✓
  E  send_sms_via_ghl with 400 response → full GHL error body printed ✓

CHANGES IN v3.3  (placeholder-aware lead routing)
────────────────────────────────────────────────
  ROOT CAUSE FIXED: Chat widget leads with message="start" were silently dropped
  with reason="no_routing_signal_matched" and payload_cw_reason="has_real_message=True
  (live SMS reply)".  GHL injects "start" as a synthetic message value for form
  submissions — it is NOT a human reply.  The detector treated it as one.

  [FIX-3.3-A]  is_placeholder_widget_message(text) — new helper
               Returns True for synthetic GHL message values that are never
               real human SMS replies: "start", "begin", "new lead",
               "form submission", "widget", "submitted", "n/a", etc.
               Also matches "start." / "start!" via regex.

  [FIX-3.3-B]  normalize_payload() — has_real_message fix
               Changed from: has_real_message = bool(raw_message)
               Changed to  : has_real_message = bool(raw_message) and
                               not is_placeholder_widget_message(raw_message)
               Placeholder messages no longer count as real messages anywhere
               downstream (detect_payload_type, booked PATH B check, etc.).

  [FIX-3.3-C]  is_chat_widget_lead_payload() — HARD EXCLUSION 2 hardened
               Defense-in-depth: even if has_real_message=True arrives in a
               parsed dict built outside normalize_payload(), the exclusion now
               re-checks is_placeholder_widget_message() before rejecting.

  [FIX-3.3-D]  Routing diagnostic expanded
               [ROUTING] block now shows parsed[message], has_real_message,
               and msg_is_placeholder so it is immediately obvious in terminal
               whether the message field triggered or bypassed the exclusion.

SCENARIO VERIFICATION:
  A  message="start", medium="chat_widget", empty tags → placeholder=True →
     has_real_message=False → payload Signal 1 → LEAD PATH ✓
  A2 message="start", no medium, has phone+name → placeholder=True →
     has_real_message=False → form_submission shape (Signal 5) → LEAD PATH ✓
  B  message="yeah I own it" → placeholder=False → has_real_message=True →
     HARD EXCLUSION 2 → not a fresh lead form sub (correct) ✓
  C  tags=["source_chat_widget"] → _tag_says_cw=True → LEAD PATH ✓
  D  Booked contact → _is_booked_contact=True → BOOKED PATH unchanged ✓

CHANGES IN v3.2  (tag-independent lead routing)
────────────────────────────────────────────────
  ROOT CAUSE FIXED: Chat widget leads with empty tags were silently dropped
  with reason="no_routing_tag_matched".  GHL does not always attach the
  "source_chat_widget" tag to the webhook payload at fire time.

  [FIX-R1]  is_chat_widget_lead_payload(body, parsed) — new helper
            Returns (True, reason_str) when a payload is a legitimate chat
            widget/form submission regardless of tags.  Checks (in order):
              • HARD EXCLUSION: appointment/booking webhooks → False
              • HARD EXCLUSION: real SMS body (live reply) → False
              • Signal 1: medium field in chat widget set
              • Signal 2: source field in chat widget set
              • Signal 3: type field in form/widget set
              • Signal 4: nested customData.medium / customData.source
              • Signal 5: form_submission shape (no message body, has contact info)
              • Signal 6: state.entry_path == "chat_widget" (returning widget contact)

  [FIX-R2]  Tag-independent lead routing gate
            The old gate `if "source_chat_widget" not in tags: → skip` is
            replaced with:
              _is_lead_payload = _tag_says_cw OR _cw_payload_ok
              if not _is_lead_payload: → safe no-op
            Legitimate chat widget leads with empty tags now route correctly
            into the form submission / qualification path.

  [FIX-R3]  Expanded routing diagnostic
            The [ROUTING] block now shows:
              tag_says_cw, payload_says_cw, payload_cw_reason, _is_lead_payload
            Making it immediately obvious in terminal why a payload was or
            was not classified as a chat widget lead.

  [FIX-R4]  Safe no-op response updated
            Returns reason="no_routing_signal_matched" (not "no_routing_tag_matched")
            and includes tag_says_cw, payload_says_cw, payload_cw_reason fields
            for diagnostic visibility in GHL execution logs.

SCENARIO VERIFICATION:
  A  tags=[], medium="chat_widget", message="start", inbound → payload Signal 1 → LEAD PATH ✓
  B  tags=[], form_submission shape (cid+phone+no message) → payload Signal 5 → LEAD PATH ✓
  C  tags=[], appointment fields present → HARD EXCLUSION → booked routing wins ✓
  D  tags=[], junk/unknown payload with no contact info → no_routing_signal_matched ✓
  E  Booked contact with any tags → _is_booked_contact=True → BOOKED PATH (unchanged) ✓

CHANGES IN v3.1  (booked bill intelligence)
────────────────────────────────────────────
  ROOT CAUSE FIXED: A booked lead sent "Here ya go" with a bill photo and
  received "Not sure I caught what you're sending over — what's up?"
  Root cause: is_bill_response() was too conservative and missed casual
  bill-submission phrases, causing the message to fall through to the generic
  booked Claude follow-up path which had no context about the bill photo.

  [FIX-NEW-1]  is_booked_bill_submission() — new comprehensive detector
               Replaces is_bill_response() as the gate for PATH A (bill ack).
               Covers: has_attachment, msg_type=MMS/IMAGE, 40+ explicit phrases
               ("here ya go", "sent it", "just sent", "see attached", "above",
               "electric bill", "can you see it", "does this work", etc.),
               AND a short-ambiguous-reply fallback for booked leads waiting
               on a bill ("here", "sent", "done", "k", "gotchu", etc.).
               Returns (bool, reason_str) for full diagnostic traceability.

  [FIX-NEW-2]  Booked bill routing priority reaffirmed
               PATH A (BILL_ACK) is now the FIRST check inside the
               bill_reminder_sent block, evaluated via is_booked_bill_submission().
               Only if that returns False does execution fall through to PATH B
               (silent skip for workflow webhooks) or PATH C (Claude follow-up).
               Claude is NEVER called for bill submissions regardless of what
               text accompanies the photo.

  [FIX-NEW-3]  BOOKED BILL DIAGNOSTIC block added
               Before any routing decision in the post-reminder reply section,
               a "BOOKED BILL DIAG" banner is printed showing: contact_id,
               full_name, stage, all booked/bill flags, has_attachment,
               has_real_message, msg_type, raw message, is_booked_bill_sub
               result + reason, and ROUTE CHOSEN (BILL_ACK /
               SECOND_APPT_WEBHOOK_SKIP / BOOKED_FOLLOWUP_AGENT).

  [FIX-NEW-4]  Comprehensive phrase library
               _BILL_SUBMISSION_STRONG regex covers every phrase listed in
               the spec: "here ya go", "sent it", "just sent", "there it is",
               "see attached", "look above", "check above", "electric bill",
               "utility bill", "power bill", "does this work", "can you see it",
               "is this good", "did you get it", "gotchu", "this one", etc.
               _BILL_AMBIGUOUS_SHORT covers one-word / two-word replies that
               are almost certainly bill-related in context.
               _BILL_HARD_NEGATIVE guards against logistics questions being
               mis-classified as bill submissions.

SCENARIO VERIFICATION (mentally confirmed):
  A  Booked + "Here ya go" + photo → is_booked_bill_submission() = True (attachment)  → BILL_ACK ✓
  B  Booked + photo only, no caption → has_attachment=True                             → BILL_ACK ✓
  C  Booked + "sent it" (GHL split webhook, no attachment) → strong_phrase match       → BILL_ACK ✓
  D  Booked + "What time are you coming by?" → hard_negative_logistics                 → BOOKED_FOLLOWUP_AGENT ✓
  E  Duplicate appt webhook, no real message → not _is_real_inbound                    → SECOND_APPT_WEBHOOK_SKIP ✓

CHANGES IN v3.0  (production hardening — booked lockout + event-id dedup)
──────────────────────────────────────────────────────────────────────────
  ROOT CAUSE FIXED: After a chat-widget lead booked and sent their bill photo,
  the system re-sent the old pre-booking qualification / booking-link text.
  Five independent failure modes were identified and patched:

  [FIX-8]   BOOKED LOCKOUT in michael_agent():
            When appointment_booked=True or stage=BOOKED, michael_agent() now
            uses _build_booked_followup_prompt() — a stripped-down prompt that
            EXPLICITLY forbids qualification questions, booking links, and
            control tags.  Claude can no longer generate booking language for
            a booked contact regardless of how it was invoked.
            final_confirmation_sent=True → immediate None (no Claude call at all).

  [FIX-9]   GUARD 0 — final_confirmation_sent:
            Added as the first priority guard in inbound_webhook(), before
            bill_received (GUARD 1) and appointment_booked (GUARD 2).
            Once the bill ack has been sent, every subsequent inbound message
            is silently no-op'd with a clear log label.

  [FIX-10]  UNIVERSAL phone-based contact resolution:
            Previously _resolve_contact_id_by_phone only ran for payloads
            that already looked like appointment webhooks.  Bug: if an MMS
            bill photo arrived with a different contactId than the chat session,
            fresh INITIAL state was used and all guards missed.
            Fixed: phone resolution now runs for EVERY webhook where a phone
            is present and the webhook contact_id has no/INITIAL history.

  [FIX-11]  Event-ID idempotency dedup:
            Added _processed_event_ids cache + _is_duplicate_event_id().
            GHL injects a unique id/messageId/eventId per webhook.  When GHL
            fires multiple webhooks for the same event (e.g. both the MMS
            inbound webhook AND a "Customer replied" workflow webhook), the
            second one is dropped at the top of inbound_webhook() before any
            state access.  Also added to normalize_payload() return dict.

  [FIX-12]  _build_booked_followup_prompt():
            New minimal Claude system prompt used EXCLUSIVELY for booked
            follow-ups.  Contains an explicit "HARD STOP" block that forbids
            [SEND_BOOKING], [QUALIFIED], URLs, and qualification questions.
            Michael can still reply naturally to "Ok thanks", "What time?", etc.

  [FIX-13]  [SEND_BOOKING] absolute strip for booked contacts:
            In the booked-prompt path of michael_agent(), ANY [SEND_BOOKING]
            or [QUALIFIED] tag in Claude's output is stripped and the booking
            URL is NEVER injected, regardless of stage or flags.

CHANGES IN v2.9  (two-path routing upgrade)
────────────────────────────────────────────
  [PATH-1]  entry_path="chat_widget" stamped at form-submission (INITIAL stage).
  [PATH-2]  entry_path="direct_booking" stamped when a booking webhook arrives
            for a contact with no prior SMS history.
  [PHONE]   _phone_to_contact lookup — resolves appointment webhooks that arrive
            with a different contact_id to the existing chat-session contact_id.
  [MSG-1]   Warm bill reminder for chat widget leads — continuation tone.
  [MSG-2]   Clear bill reminder for direct bookings — brief intro tone.
  [MSG-3]   Simplified bill ack for both paths: "Perfect, got it. See you then."
  [GUARD]   entry_path never overwritten once set — chat widget leads cannot
            accidentally be re-classified as direct bookings.

CHANGES IN v2.8  (adaptive settler upgrade)
────────────────────────────────────────────
  [ADAPT-1]  build_system_prompt(state) — replaces static SYSTEM_PROMPT.
             Injects a "WHAT YOU ALREADY KNOW" block and "CURRENT GOAL"
             into every Claude call so Michael never re-asks confirmed
             questions regardless of conversation length or order.
  [ADAPT-2]  update_state_from_inbound() — conservative regex parser
             extracts homeowner status and bill amount from inbound text
             BEFORE Claude is called, so the system prompt is current.
  [ADAPT-3]  Stage→state inference — when Claude's control tags advance
             the stage (e.g., INITIAL→ASK_LOCATION), homeowner / location
             flags are inferred as confirmed. Belt-and-suspenders with the
             regex parser.
  [ADAPT-4]  New state fields: homeowner, location_confirmed, monthly_bill,
             bill_photo_received, address.
  [ADAPT-5]  Updated first-outreach text — "Quick question so I can point
             you in the right direction: do you own the home at {street}?"
  [ADAPT-6]  Outbound dedup — fingerprints outbound messages to prevent
             concurrent webhooks from sending the same SMS twice.
  [ADAPT-7]  Structured logging — [INBOUND] [STATE] [ROUTING] [AGENT]
             [SMS] [DUPLICATE] [ERROR] labels throughout.
  [ADAPT-8]  Fixed duplicate /debug/state/{contact_id} route (FastAPI
             would silently ignore the second definition; removed it).
  [ADAPT-9]  System prompt now explicitly instructs Michael to drive
             qualified leads toward booking — not wait, not over-qualify.

SETUP
─────
  pip install fastapi uvicorn anthropic httpx python-dotenv

  .env file:
    ANTHROPIC_API_KEY=sk-ant-...
    GHL_API_KEY=your_ghl_api_key
    GHL_LOCATION_ID=your_location_id
    GHL_FROM_NUMBER=+18163190932
    BOOKING_LINK=https://stlenergyadvisors.com/get-solar-info?source=sms
    BOOKED_TAG=appointment booked
    PORT=8000
"""

import os
import re
import sys
import json
import hashlib
import hmac
import logging
import traceback
from enum import Enum
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import NamedTuple, Optional
import asyncio
from urllib.parse import parse_qs

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from anthropic import Anthropic
from dotenv import load_dotenv

MODEL = "claude-opus-4-6"
CHAT_MODEL = MODEL

# ─────────────────────────────────────────────
#  SETUP
# ─────────────────────────────────────────────

# ── [FIX-6.A] Force stdout to line-buffered mode ────────────────────────
# When Python is run under uvicorn --reload (or any subprocess/pipe), stdout
# defaults to full-buffering — print() output is swallowed until the buffer
# fills or the process exits.  This single line makes every print() appear
# immediately in the terminal, exactly like it does in interactive mode.
# Without this fix, the chat widget webhook can be fully processed and SMS
# sent yet the terminal shows NOTHING.
try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass   # Python < 3.7 fallback — output may still buffer in rare cases

# ── Module-level startup proof-of-life (fires on every load/reload) ──────────
# This prints the moment Python imports michael_agent.py.
# If you see this in the terminal but NOT "=== WEBHOOK HIT ===" when ngrok
# shows 200 OK, then a different uvicorn worker process is handling requests.
# Run: ps aux | grep uvicorn   — and check which PID is active.
try:
    import os as _os_mod
    _os_mod.write(1, b"\n[MICHAEL] === MODULE LOADED === michael_agent.py is live\n")
    _os_mod.write(2, b"[MICHAEL] === MODULE LOADED === michael_agent.py is live\n")
except Exception:
    pass
print("[MICHAEL] === MODULE LOADED === michael_agent.py starting up ...", flush=True)

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# ── [FIX-6.B] Logging writes to stdout, not stderr ──────────────────────
# Python's default logging.basicConfig() sends output to stderr.
# If your terminal only shows stdout (e.g., redirected with 2>/dev/null, or
# a process manager that separates the streams), log.warning() / log.info()
# lines are invisible even though the code ran.  Route logging to stdout so
# it appears alongside the print() statements.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("michael")

_required_env = ["ANTHROPIC_API_KEY", "GHL_API_KEY", "GHL_LOCATION_ID"]
for _var in _required_env:
    if not os.getenv(_var):
        log.warning(f"⚠️  ENV WARNING: {_var} is not set — check your .env file")

app    = FastAPI(title="Michael — STL Energy Advisors AI Agent")

# ── CORS — allows browser requests from the Vercel frontend ──────────────
# allow_origin_regex covers all preview/branch deploys automatically.
# allow_credentials is False because the frontend sends no cookies or auth headers.
# FastAPI/Starlette CORSMiddleware handles OPTIONS preflight automatically.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kc-energy-advisors-v2.vercel.app",
        "https://kcenergyadvisors.com",
        "https://stlenergyadvisors.com",
        "https://www.stlenergyadvisors.com",
        "http://localhost:3000",
    ],
    allow_origin_regex=r"https://(kc-energy-advisors-v2[\w-]*\.vercel\.app|stlenergyadvisors\.com)",
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

claude = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── [FIX-7.A] FastAPI request/response middleware ────────────────────────────
# Logs EVERY incoming HTTP request with method, path, status code, and elapsed
# time.  Fires even when the route handler raises an exception.  Output goes to
# stdout (line-buffered per sys.stdout.reconfigure above) so it always appears
# in the terminal alongside the route-level prints.
import time as _time_module  # avoid shadowing any local var named "time"

@app.middleware("http")
async def _log_every_request(request: Request, call_next):
    # Triple-layer output: raw fd write + stdout + stderr
    # Guarantees something appears even if Python's print() is buffered or redirected
    _ts  = _time_module.strftime("%Y-%m-%d %H:%M:%S")
    _mw_line_in = f"\n[MW] >>> {_ts} | {request.method} {request.url.path}\n"
    try:
        os.write(1, _mw_line_in.encode())   # raw stdout fd — bypasses ALL Python buffering
        os.write(2, _mw_line_in.encode())   # raw stderr fd  — visible even when stdout piped
    except Exception:
        pass
    print(_mw_line_in, end="", flush=True)
    _t0 = _time_module.monotonic()
    try:
        response = await call_next(request)
    except Exception as _mw_err:
        _ms = (_time_module.monotonic() - _t0) * 1000
        _mw_line_ex = f"[MW] <<< {request.method} {request.url.path} → 500 EXCEPTION ({_ms:.0f}ms): {_mw_err}\n"
        try:
            os.write(1, _mw_line_ex.encode())
            os.write(2, _mw_line_ex.encode())
        except Exception:
            pass
        print(_mw_line_ex, end="", flush=True)
        raise
    _ms = (_time_module.monotonic() - _t0) * 1000
    _mw_line_out = f"[MW] <<< {request.method} {request.url.path} → {response.status_code} ({_ms:.0f}ms)\n"
    try:
        os.write(1, _mw_line_out.encode())
        os.write(2, _mw_line_out.encode())
    except Exception:
        pass
    print(_mw_line_out, end="", flush=True)
    return response


# ── [FIX-7.C] Startup diagnostic — prints immediately when uvicorn is ready ──
# Confirms: (a) this Python file was loaded, (b) FastAPI registered the routes,
# (c) stdout is working.  Look for this block after "Uvicorn running on http://..."
@app.on_event("startup")
async def _startup_diagnostic():
    _startup_lines = [
        "",
        "=" * 64,
        "  MICHAEL AGENT — STARTUP COMPLETE",
        f"  PID        : {os.getpid()}",
        f"  Registered routes:",
    ]
    for _r in app.routes:
        _methods = getattr(_r, "methods", None)
        _path    = getattr(_r, "path", "?")
        if _methods:
            _startup_lines.append(f"    {sorted(_methods)} {_path}")
    _startup_lines += [
        "  Webhook endpoint: POST /webhook/inbound",
        "  If you see this but NOT '=== WEBHOOK HIT ===' on requests,",
        "  ngrok is pointing at the wrong port or process.",
        "=" * 64,
        "",
    ]
    for _sl in _startup_lines:
        try:
            os.write(1, (_sl + "\n").encode())
        except Exception:
            pass
        print(_sl, flush=True)


# ── Booking URL ───────────────────────────────────────────────────────────────
# [FIX-1] The single source of truth for the booking page URL.
# This hardcoded constant is what EVERY outbound booking SMS must use.
# It is also what sanitize_outbound_message() replaces bad URLs WITH — so it
# must be correct regardless of what .env contains.
_CORRECT_BOOKING_URL = "https://stlenergyadvisors.com/get-solar-info?source=sms"

# BOOKING_LINK is the runtime variable used throughout the module.
# We read from .env for flexibility, but immediately override any bad value.
BOOKING_LINK = os.getenv("BOOKING_LINK", _CORRECT_BOOKING_URL)

# [FIX-5 + FIX-6A] Startup guard — detect AND self-heal a stale .env value.
# Previously this block only LOGGED the bad value; it never fixed BOOKING_LINK,
# so the bad URL flowed into every booking SMS for the entire process lifetime.
# Now we override BOOKING_LINK to _CORRECT_BOOKING_URL immediately.
#
# Any .env that still contains a raw LeadConnector widget URL or the legacy
# kcenergyadvisors.com calendar URL is self-healed to the clean public URL.
_OLD_BOOKING_DOMAINS = (
    "kcenergyadvisors.com/get-solar-info",         # legacy KC-era calendar URL
    "kcenergyadvisors.net/get-solar-info",         # legacy alt-domain variant
    "api.leadconnectorhq.com/widget/booking",      # raw GHL widget — never customer-facing
    "leadconnectorhq.com/widget/booking",          # non-subdomain variant
)
if any(old in BOOKING_LINK for old in _OLD_BOOKING_DOMAINS):
    log.warning("=" * 70)
    log.warning("⚠️  BOOKING LINK WARNING — legacy KC booking URL detected in .env!")
    log.warning(f"⚠️  Bad value     : {BOOKING_LINK}")
    log.warning(f"⚠️  Self-healing  : BOOKING_LINK overridden to {_CORRECT_BOOKING_URL}")
    log.warning(f"⚠️  Fix your .env: BOOKING_LINK={_CORRECT_BOOKING_URL}")
    log.warning("=" * 70)
    print("=" * 70)
    print("⚠️  STARTUP: stale BOOKING_LINK in .env — self-healing to STL URL")
    print(f"⚠️  Was  : {BOOKING_LINK}")
    print(f"⚠️  Now  : {_CORRECT_BOOKING_URL}")
    print("⚠️  Update your .env to silence this warning.")
    print("=" * 70)
    BOOKING_LINK = _CORRECT_BOOKING_URL   # ← THE FIX: override the stale env value
GHL_API_KEY     = os.getenv("GHL_API_KEY",     "")   # must be set in .env — no hardcoded fallback
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID", "")   # must be set in .env — no hardcoded fallback
GHL_FROM_NUMBER = os.getenv("GHL_FROM_NUMBER", "+18163190932")
GHL_API_BASE    = "https://services.leadconnectorhq.com"

# [FIX-7] Warn loudly at startup if the required GHL credentials are missing.
# These can never have useful hardcoded fallbacks — they are account-specific secrets.
for _cred_name, _cred_val in (("GHL_API_KEY", GHL_API_KEY), ("GHL_LOCATION_ID", GHL_LOCATION_ID)):
    if not _cred_val:
        log.warning(f"⚠️  STARTUP WARNING: {_cred_name} is not set — all GHL API calls will fail until this is configured in .env")
        model = MODEL
MAX_DAILY_MSGS  = 6

# ── [META-B1] Durable GHL tag markers ────────────────────────────────────
# These are the ONLY tags Michael relies on for cross-restart durability.
# Process memory (_state_store) is wiped on every Render restart/spin-down,
# so anything that must survive a restart lives in GHL as a contact tag.
#   TAG_ENGAGED  — lead has replied at least once.  Gates the GHL nurture
#                  workflow (no canned follow-up once a human is talking)
#                  AND suppresses a repeat first-outreach after a restart.
#   TAG_DNC      — durable opt-out marker.  Matches the existing GHL
#                  "solar-dnc" tag used by the Customer Replied trigger
#                  filter, so the two systems agree on one marker.
TAG_ENGAGED = "ai-engaged"
TAG_DNC     = "solar-dnc"
TAG_OUTREACH_SENT = "ai-outreach-sent"
#   TAG_DISQUALIFIED — durable "stop nurturing this lead" marker, lowercase
#                  to match the solar-dnc convention the GHL workflows gate on.
TAG_DISQUALIFIED  = "solar-disqualified"
#   TAG_UNDELIVERABLE — a message to this contact was filtered or rejected by
#                  the carrier. Flags the contact for a human to look at.
#                  It does NOT suppress sending: the invariant is about
#                  booked contacts, not undeliverable ones.
TAG_UNDELIVERABLE = "sms-undeliverable"
#   TAG_NUDGE_SENT — the one booking-link follow-up has been sent. Durable in
#                  GHL, so a Render restart or a duplicate workflow firing
#                  can never produce a second nudge.
TAG_NUDGE_SENT    = "booking-nudge-sent"
#   TAG_NUDGE_CANCELLED — a human has taken this conversation over. Set when
#                  a manual follow-up goes out after the booking link, so the
#                  homeowner never gets two follow-ups from us on the same day.
#                  Python cannot see manual sends: they never reach
#                  /webhook/inbound, and GHL's documented /conversations/search
#                  returns lastMessageType but no date and no direction, so
#                  there is nothing to compare against. This tag is the signal.
TAG_NUDGE_CANCELLED = "ai-nudge-cancelled"

# ── [DELIVERY-2] Carrier error taxonomy ──────────────────────────────────
# Codes GHL surfaces through its "Messaging Error Code - SMS" workflow
# trigger. PERMANENT means resending the same message to the same number
# will fail again — retrying is pointless and, for filtered content,
# actively harmful to sender reputation. Nothing in this file ever retries.
_CARRIER_PERMANENT: frozenset = frozenset({
    "30007",   # Message filtered (content / spam signals / carrier rules)
    "30003",   # Unreachable destination handset
    "30004",   # Message blocked
    "30005",   # Unknown destination handset
    "30006",   # Landline or unreachable carrier
    "21211",   # Invalid "To" number
    "21610",   # Recipient has opted out (STOP)
    "21614",   # Not a mobile number
    "30034",   # Unregistered / mis-registered A2P 10DLC sender
})
_CARRIER_TRANSIENT: frozenset = frozenset({
    "30001",   # Queue overflow
    "30002",   # Account suspended
    "30008",   # Unknown error — may succeed on a later, different message
    "21408",   # Permission to send to this region not enabled
})


def classify_carrier_error(code: str) -> str:
    """"permanent" | "transient" | "unknown" for a carrier error code."""
    c = str(code or "").strip()
    if c in _CARRIER_PERMANENT:
        return "permanent"
    if c in _CARRIER_TRANSIENT:
        return "transient"
    return "unknown"

CENTRAL_TZ      = ZoneInfo("America/Chicago")
BOOKED_TAG      = os.getenv("BOOKED_TAG", "appointment booked")

# [DEBUG-RESET] Shared secret guarding POST /debug/reset-contact.
# No default and no fallback: when unset the endpoint is hard-disabled (503),
# so an un-configured deployment cannot expose it.  Never logged.
DEBUG_RESET_SECRET = os.getenv("DEBUG_RESET_SECRET", "")


# ─────────────────────────────────────────────
#  QUALIFICATION STAGES
# ─────────────────────────────────────────────

class SendStatus(str, Enum):
    """
    [DELIVERY] Lifecycle of one outbound SMS.

        not_attempted -> suppressed                    (guard/dedup: never sent)
        not_attempted -> rejected                      (GHL refused at API time)
        not_attempted -> accepted -> delivered|failed  (GHL queued it)

    ACCEPTED is the crucial one. A 2xx from GHL means GHL took the message
    for sending — NOT that a handset received it. Carrier failures such as
    30007 arrive later, asynchronously, long after the 2xx.

    DELIVERED and FAILED are only reachable from a delivery-status feed,
    which does not exist yet. Nothing may claim DELIVERED until it does.
    """
    NOT_ATTEMPTED = "not_attempted"
    SUPPRESSED    = "suppressed"
    REJECTED      = "rejected"
    ACCEPTED      = "accepted"
    DELIVERED     = "delivered"
    FAILED        = "failed"


# Statuses that justify advancing conversation state. ACCEPTED qualifies:
# the message is genuinely in flight and will usually arrive, and rolling it
# back would risk asking the same question twice if it does.
_STATUS_ADVANCES_STATE: frozenset = frozenset({
    SendStatus.ACCEPTED, SendStatus.DELIVERED,
})


class SendKind(str, Enum):
    """
    [BOOKED-GUARD] Classification of every outbound SMS.

    The booked guard suppresses only the classes that constitute unsolicited
    progression of a lead. Replies to a booked contact, booking confirmations,
    bill acknowledgments and opt-out confirmations are NEVER suppressed —
    a booked homeowner must still be able to hold a conversation, and a STOP
    confirmation is a compliance obligation.
    """
    OUTREACH      = "outreach"        # first contact                 GUARDED
    QUALIFICATION = "qualification"   # AI qualifying question        GUARDED
    BOOKING_PITCH = "booking_pitch"   # booking link / invite         GUARDED
    NURTURE       = "nurture"         # no-response follow-up         GUARDED
    BOOKED_REPLY  = "booked_reply"    # answering a booked contact
    BILL_ACK      = "bill_ack"        # bill-photo acknowledgment
    OPT_OUT       = "opt_out"         # STOP confirmation (compliance)
    SYSTEM        = "system"          # failsafe / admin debug


# Classes subject to the authoritative booked check.
_BOOKED_GUARDED: frozenset = frozenset({
    SendKind.OUTREACH,
    SendKind.QUALIFICATION,
    SendKind.BOOKING_PITCH,
    SendKind.NURTURE,
})


class Stage(str, Enum):
    INITIAL       = "INITIAL"
    ASK_OWNERSHIP = "ASK_OWNERSHIP"
    ASK_LOCATION  = "ASK_LOCATION"
    ASK_BILL      = "ASK_BILL"
    SEND_BOOKING  = "SEND_BOOKING"
    BOOKED        = "BOOKED"
    DISQUALIFIED  = "DISQUALIFIED"
    DNC           = "DNC"


# ─────────────────────────────────────────────
#  GHL PIPELINE → STAGE ROUTING
#
#  GHL pipeline stage name is the PRIMARY routing driver.
#  When present it overrides in-memory state for routing decisions.
#  In-memory state is retained as cache/fallback if GHL API fails.
# ─────────────────────────────────────────────

GHL_STAGE_MAP: dict[str, Stage] = {
    "New Lead":                        Stage.INITIAL,
    "Contacted":                       Stage.ASK_OWNERSHIP,
    "Qualified":                       Stage.SEND_BOOKING,
    "Solar Savings Report Scheduled":  Stage.BOOKED,
    "Consultation Completed":          Stage.BOOKED,
    "Proposal Sent":                   Stage.BOOKED,
    "Closed WON":                      Stage.BOOKED,
    "Closed LOST":                     Stage.DISQUALIFIED,
}

# Stages that PERMANENTLY suppress opener/qualification logic
GHL_BOOKED_STAGES: frozenset = frozenset({
    "Solar Savings Report Scheduled",
    "Consultation Completed",
    "Proposal Sent",
    "Closed WON",
    "Closed LOST",
})

def ghl_stage_to_internal(ghl_stage: str) -> "Optional[Stage]":
    """Map GHL pipeline stage name to internal Stage. Returns None if unmapped."""
    return GHL_STAGE_MAP.get((ghl_stage or "").strip())


# ─────────────────────────────────────────────
#  IN-MEMORY STATE STORE
# ─────────────────────────────────────────────

def get_state(contact_id: str) -> dict:
    if contact_id not in _state_store:
        _state_store[contact_id] = {
            # ── SMS compliance ledger ───────────────────────────
            "sms_opt_out"           : False,    # consumer opt-out - NEVER text
            "sms_ineligible"        : False,    # number cannot receive SMS
            "sms_failure_category"  : "",
            "sms_failure_code"      : "",
            "sms_failure_at"        : "",
            "fallback_pending"      : False,    # preserved for Phase 2. NOT contacted.
            "fallback_email"        : "",
            # Core qualification stage
            "stage"                 : Stage.INITIAL,
            # Claude conversation history
            "messages"              : [],
            # Daily send-rate limiting
            "msgs_today"            : 0,
            "last_msg_date"         : "",
            # Contact info
            "contact_name"          : "",
            "phone"                 : "",
            "address"               : "",       # [ADAPT-4] street address from form
            # Qualification facts (adaptive state)  [ADAPT-4]
            "homeowner"             : None,     # None | "yes" | "no"
            "location_confirmed"    : False,    # True once service area confirmed
            "monthly_bill"          : "",       # e.g. "$150/month" once parsed
            # ── [BUNDLE-1] Qualification record ──────────────────────────
            # The three criteria the offer actually depends on, each a real
            # tri-state so "not asked yet" is never mistaken for "no". These
            # are the source of truth; the three fields above are kept in
            # sync with them by sync_qualification_fields() because tags,
            # stage restore and the prompt still read the originals.
            "q_homeowner"           : "unknown",   # yes | no | unknown
            "q_utility"             : "unknown",   # ameren | other | unknown
            "q_bill_100_plus"       : "unknown",   # yes | no | unknown
            # True once the bundled criteria message has gone out, so the
            # agent does not send the same framing twice.
            "bundled_offer_sent"    : False,
            # Booking / appointment state
            "qualified"              : False,
            "booking_detected"       : False,
            "booking_follow_up_sent" : False,   # legacy alias — kept for backward compat
            "booking_followup_sent"  : False,   # authoritative dedup flag for /webhook/booking-followup
            # Booked-contact bill-photo flow
            "bill_reminder_sent"    : False,
            "bill_photo_received"   : False,    # [ADAPT-4]
            "bill_ack_sent"         : False,
            # [PATH-1/2] Entry path — set once at first contact, NEVER overwritten.
            # "chat_widget"    → came through website chat widget; may have prior SMS history
            # "direct_booking" → booked directly via calendar page; no prior SMS conversation
            # "unknown"        → not yet determined (server restart, test payloads, etc.)
            "entry_path"            : "unknown",
            # Raw source string from first webhook — for audit / debugging only
            "lead_source"           : "",
            # ── EXPLICIT DURABLE FLAGS (v2.9+) ────────────────────────────
            # These are the AUTHORITATIVE source of truth for routing priority.
            # They are set exactly once (never cleared by inbound messages) and
            # are checked BEFORE any other routing logic so post-booking states
            # ALWAYS take precedence over the qualification/chat-widget flow.
            #
            # appointment_booked  — True once the calendar booking is confirmed.
            #   Once True, no inbound message can restart qualification.
            # bill_requested      — True once the bill reminder SMS was sent.
            #   Alias for bill_reminder_sent; synced whenever that flag is set.
            # bill_received       — True once the lead's bill was acked.
            #   Set together with bill_ack_sent and bill_photo_received.
            #   Once True, all inbound messages are silently no-op'd (flow complete).
            # final_confirmation_sent — True once "Perfect, got it. See you then." sent.
            # source_chat_widget  — True when entry_path = "chat_widget".
            # source_direct_calendar — True when entry_path = "direct_booking".
            "appointment_booked"      : False,
            "bill_requested"          : False,
            "bill_received"           : False,
            "final_confirmation_sent" : False,
            "source_chat_widget"      : False,
            "source_direct_calendar"  : False,
            # ── Pending question tracking (v3.6+) ─────────────────────────
            # Tracks the qualification field the agent most recently asked about.
            # Used to disambiguate brief yes/no replies and detect duplicate sends.
            # Values: "utility" | "ownership" | "bill" | "send_booking" | "none"
            "pending_question"        : "none",
            # [CONVERT-1] What we said last, and how it was classified.
            # last_outbound_text may be recovered from GHL when the message
            # was sent by a nurture workflow rather than by this service.
            "last_outbound_intent"    : "none",
            "last_outbound_text"      : "",
            # ── [DELIVERY] Outbound delivery bookkeeping ──────────────────
            # last_send_status  — SendStatus of the most recent attempt.
            # last_message_id   — GHL message id, for correlating a future
            #                     delivery-status event back to this step.
            # undelivered_count — permanent carrier failures seen so far.
            "last_send_status"        : SendStatus.NOT_ATTEMPTED.value,
            "last_message_id"         : "",
            "undelivered_count"       : 0,
        }
    return _state_store[contact_id]

_state_store: dict[str, dict] = {}

def save_state(contact_id: str, state: dict):
    _state_store[contact_id] = state


def parse_qualification_tags(tags: list) -> dict:
    """
    Extract qualification facts from GHL contact tags to prevent re-asking.

    Tag conventions (lowercase):
      homeowner, renter
      ameren-confirmed, ameren-illinois, out-of-area
      bill-75-99, bill-100-150, bill-150-200, bill-200-plus
      roof-asphalt, roof-tile, roof-metal, roof-flat
      timeline-ready, timeline-6months, timeline-1year
    """
    result: dict = {}
    for tag in (tags or []):
        t = str(tag).lower().strip()
        if t == "homeowner":
            result["homeowner"] = "yes"
        elif t == "renter":
            result["homeowner"] = "no"
        elif t == "ameren-confirmed":
            result["location_confirmed"] = True
            result["utility"] = UTIL_AMEREN
        elif t in ("ameren-illinois", "out-of-area"):
            result["out_of_area"] = True
            result["utility"] = UTIL_OTHER
        elif t.startswith("bill-"):
            result["bill_range"] = t[5:].replace("-", "–")
        elif t.startswith("roof-"):
            result["roof_type"] = t[5:]
        elif t.startswith("timeline-"):
            result["timeline"] = t[9:]
    return result


# ─────────────────────────────────────────────
#  PAYLOAD NORMALIZER
#
#  Accepts ANY GHL webhook payload and returns a
#  clean, consistent dict — no KeyErrors ever.
# ─────────────────────────────────────────────

_BOOKING_TAG_KEYWORDS = frozenset({
    "appointment", "booked", "booking", "confirmed", "appt",
})


def _safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


# ─────────────────────────────────────────────
#  PLACEHOLDER WIDGET MESSAGE DETECTOR  [FIX-3.3]
#
#  GHL injects synthetic, non-conversational values into the `message` field
#  of chat widget and form-submission webhook payloads.  The most common is
#  the string "start" — it is a system initializer, never typed by a human.
#
#  These placeholder values must NOT be treated as real human SMS replies
#  for the purpose of lead routing.  Treating them as real messages causes
#  is_chat_widget_lead_payload() to reject the payload as a "live SMS reply"
#  and then drop the lead with reason="no_routing_signal_matched".
#
#  This function is called in two places:
#    1. normalize_payload() — sets has_real_message=False for placeholders
#    2. is_chat_widget_lead_payload() — defense-in-depth HARD EXCLUSION 2
# ─────────────────────────────────────────────

# Exact normalized lowercase values that are always synthetic / non-human.
_PLACEHOLDER_WIDGET_VALUES: frozenset[str] = frozenset({
    # GHL default starters
    "start", "begin",
    # Workflow / automation injected strings
    "new lead", "new submission",
    "form submission", "form submitted",
    "chat widget", "widget submission", "widget start", "widget",
    "lead submitted", "lead",
    "submitted",
    # Null-ish placeholders
    "n/a", "na", "none", "-", "--",
})


def is_placeholder_widget_message(text: str) -> bool:
    """
    [FIX-3.3] Returns True if `text` is a synthetic, non-conversational value
    injected by GHL for form submissions or chat widget initiations.

    True examples (not real human replies):
      "start", "Start", "START", "start.", "new lead", "form submission"

    False examples (real human replies — must stay as-is):
      "yeah I own it", "what's the cost?", "yes", "here ya go", "sent it", "hi"
    """
    if not text:
        return True   # empty / missing = no real message content
    normalized = text.strip().lower()
    if normalized in _PLACEHOLDER_WIDGET_VALUES:
        return True
    # "start" with optional trailing punctuation  ("start.", "start!", "start?")
    if re.match(r'^start[.\s!?]*$', normalized):
        return True
    return False


def normalize_payload(body: dict) -> dict:
    if not isinstance(body, dict):
        body = {}

    nested = body.get("contact") or {}
    if not isinstance(nested, dict):
        nested = {}

    contact_id = _safe_str(
        body.get("contactId") or body.get("contact_id") or
        nested.get("id") or body.get("id") or ""
    )

    first_name = _safe_str(
        body.get("firstName") or body.get("first_name") or
        nested.get("firstName") or nested.get("first_name") or ""
    )
    last_name = _safe_str(
        body.get("lastName") or body.get("last_name") or
        nested.get("lastName") or nested.get("last_name") or ""
    )
    full_name = _safe_str(
        body.get("full_name") or body.get("fullName") or
        body.get("contactName") or body.get("contact_name") or
        nested.get("name") or ""
    )
    if not full_name:
        full_name = f"{first_name} {last_name}".strip() or "Unknown"

    email = _safe_str(
        body.get("email") or body.get("emailAddress") or body.get("email_address") or
        nested.get("email") or nested.get("emailAddress") or ""
    )

    phone = _safe_str(
        body.get("phone") or body.get("phoneNumber") or body.get("phone_number") or
        body.get("from") or nested.get("phone") or nested.get("phoneNumber") or ""
    )

    address = _safe_str(
        body.get("address") or body.get("address1") or body.get("street") or
        nested.get("address") or nested.get("address1") or ""
    )
    city  = _safe_str(body.get("city")  or nested.get("city")  or "")
    state = _safe_str(
        body.get("state") or body.get("province") or
        nested.get("state") or nested.get("province") or ""
    )

    # ── Message text extraction — handles both flat and nested GHL formats ──
    # GHL "Customer Replied" workflow webhooks often send the SMS body in a nested
    # message object: {"message": {"body": "Yes", "type": "SMS", "direction": "inbound"}}
    # rather than at the top level.  Without this, body.get("message") returns the
    # dict, _safe_str() stringifies it, and Claude receives "{'body': 'Yes', ...}" as
    # the lead's reply — or the message is missed entirely, causing silent drops.
    _msg_field = body.get("message")
    if isinstance(_msg_field, dict):
        # Nested GHL Customer Replied format — extract real text from inside the object
        _msg_str = _safe_str(
            _msg_field.get("body") or _msg_field.get("text") or
            _msg_field.get("content") or _msg_field.get("messageText") or ""
        )
        # Also capture nested direction and ID for use below
        _nested_msg_direction = _safe_str(_msg_field.get("direction") or "").lower()
        _nested_msg_id        = _safe_str(_msg_field.get("id") or _msg_field.get("messageId") or "")
    elif isinstance(_msg_field, str):
        _msg_str              = _msg_field
        _nested_msg_direction = ""
        _nested_msg_id        = ""
    else:
        _msg_str              = ""
        _nested_msg_direction = ""
        _nested_msg_id        = ""

    raw_message = _safe_str(
        _msg_str or
        body.get("body") or body.get("text") or
        body.get("messageBody") or body.get("message_body") or
        body.get("smsBody") or body.get("sms_body") or
        body.get("content") or body.get("messageText") or body.get("message_text") or
        ""
    )

    # [FIX-3.3] has_real_message is True ONLY for genuine human-typed messages.
    # Placeholder values like "start" are injected by GHL for form submissions
    # and chat widget initiations — they are NOT real SMS replies.
    has_real_message = bool(raw_message) and not is_placeholder_widget_message(raw_message)
    message = raw_message if raw_message else "start"

    # ── Message type — MUST be assigned before has_attachment ─────
    # UnboundLocalError root-cause fix: msg_type was previously assigned
    # AFTER has_attachment, which references it via short-circuit `or`.
    # Python marks msg_type as a local the instant it sees any assignment
    # in the function, so reading it before the assignment always crashed.
    # Appointment booking webhooks (no attachments) hit this every time.
    msg_type = _safe_str(
        body.get("type") or body.get("messageType") or
        body.get("message_type") or "SMS"
    ).upper()

    # ── MMS / image attachment detection ─────────────────────────
    # GHL sends image webhooks with an `attachments` list (or mediaUrls / media)
    # and/or messageType == "MMS" / "IMAGE".  We flag these so the router can
    # respond instead of silently dropping the webhook.
    _raw_attachments = body.get("attachments") or body.get("mediaUrls") or body.get("media") or []
    has_attachment = bool(_raw_attachments) or msg_type in ("MMS", "IMAGE")

    raw_tags = body.get("tags") or nested.get("tags") or []
    if isinstance(raw_tags, str):
        tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    elif isinstance(raw_tags, list):
        tags = [_safe_str(t) for t in raw_tags if t]
    else:
        tags = []

    direction = _safe_str(
        body.get("direction") or body.get("messageDirection") or
        body.get("message_direction") or _nested_msg_direction or "inbound"
    ).lower()

    booking_detected = any(
        any(kw in tag.lower() for kw in _BOOKING_TAG_KEYWORDS)
        for tag in tags
    )

    # ── Lead source — preserved for entry-path classification ──────
    # Used to set state["lead_source"] at first contact.  Do NOT use
    # this for appointment detection — that's is_booked_appointment_lead().
    lead_source = _safe_str(
        body.get("source") or body.get("lead_source") or body.get("sourceId") or
        nested.get("source") or nested.get("lead_source") or ""
    ).lower()

    # ── Event / Message ID — used for idempotency dedup  [FIX-11] ──────
    # GHL sends a unique identifier per webhook event.  We cache these so
    # that duplicate webhook fires (e.g. both the raw MMS inbound webhook
    # AND a GHL "Customer replied" workflow webhook for the same message)
    # are detected and dropped before any state is touched.
    #
    # ⚠️  IMPORTANT — DO NOT use body["id"] or nested["id"] here.
    # Those top-level "id" fields typically contain the GHL contact ID,
    # NOT a webhook/message event ID.  If a contact ID gets stored in
    # _processed_event_ids, every future webhook from that same contact
    # will be incorrectly dropped as a duplicate — permanently silencing
    # the contact until the server restarts.
    # Only read fields that are exclusively used for event/message IDs.
    event_id = _safe_str(
        body.get("messageId")  or body.get("message_id") or
        body.get("eventId")    or body.get("event_id")   or
        body.get("webhookId")  or body.get("webhook_id") or
        _nested_msg_id or  # extracted from body["message"]["id"] for nested GHL format
        ""
        # body.get("id")    ← FORBIDDEN: may be contact ID
        # nested.get("id")  ← FORBIDDEN: may be contact ID
    )

    return {
        "contact_id"      : contact_id,
        "first_name"      : first_name,
        "last_name"       : last_name,
        "full_name"       : full_name,
        "email"           : email,
        "phone"           : phone,
        "address"         : address,
        "city"            : city,
        "state"           : state,
        "message"         : message,
        "has_real_message": has_real_message,
        "has_attachment"  : has_attachment,
        "tags"            : tags,
        "direction"       : direction,
        "msg_type"        : msg_type,
        "booking_detected": booking_detected,
        "lead_source"     : lead_source,
        "event_id"        : event_id,     # [FIX-11] idempotency key
    }


# ─────────────────────────────────────────────
#  CUSTOM FIELD EXTRACTOR
# ─────────────────────────────────────────────

def extract_custom_fields(body: dict) -> dict:
    if not isinstance(body, dict):
        return {}

    result: dict[str, str] = {}
    nested = body.get("contact") or {}
    if not isinstance(nested, dict):
        nested = {}

    for key in ("customField", "customFields", "custom_fields", "custom_field"):
        raw = body.get(key) or nested.get(key)
        if not isinstance(raw, list):
            continue
        for item in raw:
            if not isinstance(item, dict):
                continue
            field_name = _safe_str(
                item.get("name") or item.get("fieldKey") or
                item.get("key") or item.get("id") or ""
            ).lower()
            value = _safe_str(item.get("value") or item.get("fieldValue") or "")
            if field_name and value:
                result[field_name] = value

    for key in ("customData", "custom_data", "formData", "form_data",
                "formFields", "form_fields"):
        raw = body.get(key) or nested.get(key)
        if not isinstance(raw, dict):
            continue
        for k, v in raw.items():
            v_str = _safe_str(v)
            if k and v_str:
                result[k.lower()] = v_str

    return result


def _find_custom_field(custom_fields: dict, keywords: list[str]) -> str:
    for key, value in custom_fields.items():
        if any(kw in key for kw in keywords):
            return value
    return ""


# ─────────────────────────────────────────────
#  PAYLOAD TYPE DETECTOR
# ─────────────────────────────────────────────

def detect_payload_type(parsed: dict) -> str:
    # MMS-only webhooks (has_attachment=True, has_real_message=False) are treated
    # as chat events — they come from a live lead, not a form submission.
    return "chat_widget" if (parsed.get("has_real_message") or parsed.get("has_attachment")) else "form_submission"


# ─────────────────────────────────────────────
#  CHAT WIDGET / FORM LEAD PAYLOAD DETECTOR  [FIX-NEW: tag-independent routing]
#
#  GHL does not always attach the "source_chat_widget" tag to the webhook
#  payload at the time it fires.  Routing must not depend solely on tags.
#
#  This helper classifies a payload as a legitimate new-lead form submission
#  using PAYLOAD SHAPE signals instead of (or in addition to) tags.
#
#  Callers use the reason string to log exactly WHY it was classified — making
#  "source_chat_widget tag missing" failures diagnosable in the terminal.
# ─────────────────────────────────────────────

# Medium / source / type values that unambiguously indicate a chat widget or
# web form submission.  Checked against normalized lowercase field values.
_CHAT_WIDGET_MEDIUMS: frozenset[str] = frozenset({
    "chat_widget", "chat", "widget", "chatwidget",
    "web_chat", "webchat",
})
_CHAT_WIDGET_SOURCES: frozenset[str] = frozenset({
    "chat_widget", "chat", "widget", "web_chat", "webchat",
    "web_form", "webform", "lead_form", "leadform",
    "website", "web",
})
_CHAT_WIDGET_TYPES: frozenset[str] = frozenset({
    "form", "widget", "chat_widget", "lead_form", "web_form", "chat",
})


def is_chat_widget_lead_payload(body: dict, parsed: dict) -> tuple[bool, str]:
    """
    [FIX-NEW] Returns (True, reason) when the webhook payload represents a
    legitimate chat widget or form-style lead submission, REGARDLESS of whether
    the "source_chat_widget" tag is present in the payload's tags list.

    Priority checks (first match wins):
      HARD EXCLUSION 1: appointment/booking webhooks → always False
      HARD EXCLUSION 2: real inbound SMS body → always False (live reply, not a form sub)
      Signal 1: `medium` field in _CHAT_WIDGET_MEDIUMS
      Signal 2: `source` field in _CHAT_WIDGET_SOURCES
      Signal 3: `type` field in _CHAT_WIDGET_TYPES
      Signal 4: `customData.medium` or `customData.source`
      Signal 5: form_submission payload shape (no message body, has contact info)
      Signal 6: existing state shows "chat_widget" entry_path (returning contact)

    Returns (False, reason) for:
      • Appointment / calendar booking webhooks
      • Real inbound SMS messages from leads (live chat replies)
      • Junk payloads with no contact info
      • Anything that already routed as booked
    """
    if not isinstance(body, dict):
        return False, "body_not_dict"

    # ── HARD EXCLUSION 1: appointment webhooks are not lead form subs ──
    _is_appt, _appt_why = is_booked_appointment_lead(body)
    if _is_appt:
        return False, f"appointment_webhook ({_appt_why})"

    # ── HARD EXCLUSION 2: real human inbound SMS (live lead reply, not a form sub) ──
    # normalize_payload() already sets has_real_message=False for placeholder
    # values like "start" — so this guard normally only fires for genuine human
    # replies.  We add the is_placeholder_widget_message() check here too as
    # defense-in-depth for parsed dicts built outside normalize_payload().
    if parsed.get("has_real_message"):
        _raw_for_check = _safe_str(
            body.get("message") or body.get("body") or body.get("text") or
            body.get("messageBody") or ""
        )
        if not is_placeholder_widget_message(_raw_for_check):
            return False, f"has_real_message=True (live SMS reply): {_raw_for_check[:50]!r}"
        # has_real_message=True but message is a placeholder — allow detection to continue

    # ── Signal 1: explicit `medium` field ────────────────────────────
    _medium = str(
        body.get("medium") or body.get("leadMedium") or body.get("lead_medium") or
        body.get("contactMedium") or ""
    ).lower().strip()
    if _medium in _CHAT_WIDGET_MEDIUMS:
        return True, f"medium={_medium!r}"

    # ── Signal 2: `source` field indicates chat/web origin ────────────
    _source = str(
        body.get("source") or body.get("lead_source") or
        body.get("sourceId") or body.get("source_id") or ""
    ).lower().strip()
    if _source in _CHAT_WIDGET_SOURCES:
        return True, f"source={_source!r}"

    # ── Signal 3: `type` field indicates form/widget ──────────────────
    _type = str(body.get("type") or "").lower().strip()
    if _type in _CHAT_WIDGET_TYPES:
        return True, f"type={_type!r}"

    # ── Signal 4: nested customData.medium / customData.source ────────
    for _ck in ("customData", "custom_data", "customFields", "custom_fields"):
        _cd = body.get(_ck)
        if isinstance(_cd, dict):
            _cd_medium = str(_cd.get("medium") or _cd.get("lead_medium") or "").lower().strip()
            _cd_source = str(_cd.get("source") or _cd.get("lead_source") or "").lower().strip()
            if _cd_medium in _CHAT_WIDGET_MEDIUMS:
                return True, f"{_ck}.medium={_cd_medium!r}"
            if _cd_source in _CHAT_WIDGET_SOURCES:
                return True, f"{_ck}.source={_cd_source!r}"

    # ── Signal 5: form_submission payload shape ────────────────────────
    # Key catch-all: GHL chat widget payloads have no message body (message="start"),
    # carry contact info (id/phone/name), and are not appointment webhooks.
    # detect_payload_type() returns "form_submission" for exactly these.
    _pt = detect_payload_type(parsed)
    _cid = parsed.get("contact_id", "")
    _has_contact_info = bool(
        _cid and (
            parsed.get("phone") or
            parsed.get("email") or
            (parsed.get("full_name") and parsed["full_name"] not in ("", "Unknown"))
        )
    )
    if _pt == "form_submission" and _has_contact_info:
        return True, (
            f"form_submission_shape: contact_id={_cid!r}, "
            f"has_phone={bool(parsed.get('phone'))}, "
            f"has_email={bool(parsed.get('email'))}, "
            f"has_name={bool(parsed.get('full_name') and parsed['full_name'] != 'Unknown')}"
        )

    # ── Signal 6: existing state shows chat_widget entry_path ─────────
    # A returning contact whose entry_path was stamped as "chat_widget" in a
    # previous session is still a chat widget contact, even if the new payload
    # carries different/empty tags.
    _existing_state = _state_store.get(_cid)
    if _existing_state and _existing_state.get("entry_path") == "chat_widget":
        # Only apply if not already booked (booked is handled upstream)
        _ep_booked = (
            _existing_state.get("appointment_booked") or
            _existing_state.get("stage") in (Stage.BOOKED,)
        )
        if not _ep_booked:
            return True, f"state.entry_path=chat_widget (returning widget contact, non-booked)"

    return False, (
        f"no_widget_signal: payload_type={_pt!r}, "
        f"source={_source!r}, medium={_medium!r}, type={_type!r}, "
        f"has_contact_info={_has_contact_info}"
    )


# ─────────────────────────────────────────────
#  APPOINTMENT PAYLOAD DETECTOR
#
#  Public API:
#    is_booked_appointment_lead(body) → (bool, reason_str)
#      Single source of truth.  Returns True ONLY for payloads that carry
#      unambiguous evidence of a completed calendar booking.
#      Chat widget submissions, form leads, and basic contact records
#      can NEVER return True regardless of what fields they happen to carry.
#
#  Internal helpers (keep old names for backward-compat with callers):
#    _appointment_trigger(body)  → str | ""
#    is_appointment_payload(body) → bool
# ─────────────────────────────────────────────

# Sources that definitively mean "not a booking" — checked before anything else.
_NON_BOOKING_SOURCES: frozenset[str] = frozenset({
    "chat", "chat_widget", "chatwidget", "widget",
    "ai", "ai_chat", "aichat",
    "web", "website", "web_form", "webform",
    "form", "contact_form", "contactform",
    "landing_page", "landingpage",
    "facebook", "instagram", "tiktok", "twitter", "linkedin",
    "google", "google_ads", "googleads",
    "manual", "import", "csv", "api",
    "email", "email_campaign",
})

# eventType values that unambiguously mean a calendar appointment was booked.
# Does NOT include the generic "type" field (which is the SMS/message type).
_APPT_EVENT_TYPES: frozenset[str] = frozenset({
    "appointmentcreate", "appointmentbooked",  "appointmentcreated",
    "appointmentupdate", "appointmentupdated",
    "appointmentstatus", "appointmentconfirmed", "appointmentdeleted",
    "calendarappointmentcreated",
})

# appointmentStatus values that mean a booking exists.
# "new" and "accepted" are deliberately excluded — GHL sets those on
# every new contact/lead record and they do NOT mean an appointment exists.
_STRICT_APPT_STATUSES: frozenset[str] = frozenset({
    "booked", "confirmed", "scheduled",
})

# Source values that mean a calendar booking tool sent this webhook.
# Exact match only — no substring check.
_CALENDAR_SOURCES: frozenset[str] = frozenset({
    "calendar", "calendarbooking", "calendarform",
    "calendly", "scheduleonce", "acuity",
})


def is_booked_appointment_lead(body: dict) -> tuple[bool, str]:
    """
    Strict gate: returns (True, reason) ONLY when the payload contains
    unambiguous evidence of a completed calendar appointment booking.

    Returns (False, reason) for:
      • Chat widget / live SMS messages  (has a real message body)
      • Form submissions / new lead webhooks
      • Basic contact-info payloads (phone, address, name only)
      • Any payload whose source is a known non-booking channel
      • Payloads where the only "appointment" signal is a generic status
        like "new" or "accepted" (which GHL adds to every new lead)

    Edit this function to change what counts as a booked appointment.
    """
    if not isinstance(body, dict):
        return False, "body_not_dict"

    # ── HARD EXCLUSION A: real message body ────────────────────────────
    # Calendar booking webhooks NEVER carry a message body.
    # If there is one, this is a live chat/SMS exchange, not a booking event.
    _raw_msg = str(
        body.get("message") or body.get("body") or body.get("text") or
        body.get("messageBody") or body.get("message_body") or
        body.get("smsBody") or body.get("sms_body") or ""
    ).strip()
    if _raw_msg:
        return False, f"has_message_body={_raw_msg[:40]!r} — live chat/SMS, not a booking webhook"

    # ── HARD EXCLUSION B: known non-booking source ─────────────────────
    _source = str(body.get("source") or body.get("lead_source") or "").lower().strip()
    if _source:
        # Exact match first
        if _source in _NON_BOOKING_SOURCES:
            return False, f"source={_source!r} — known non-booking channel"
        # Prefix match for compound values like "chat_widget_v2"
        for _ns in _NON_BOOKING_SOURCES:
            if _source.startswith(_ns):
                return False, f"source={_source!r} starts with non-booking prefix {_ns!r}"

    # ── SIGNAL 0: Explicit GHL workflow event_type ─────────────────────
    _ev = str(body.get("event_type") or "").strip().lower()
    if _ev == "appointment_booked":
        return True, f"event_type={body['event_type']!r} (GHL workflow signal)"
    for _ck in ("customData", "custom_data", "customFields", "custom_fields"):
        _cd = body.get(_ck)
        if isinstance(_cd, dict):
            if str(_cd.get("event_type") or "").strip().lower() == "appointment_booked":
                return True, f"{_ck}.event_type='appointment_booked' (GHL workflow signal)"

    # ── SIGNAL 1: Explicit appointment ID ─────────────────────────────
    for k in ("appointmentId", "appointment_id", "appt_id", "appoinmentId"):
        if body.get(k):
            return True, f"{k}={body[k]!r}"

    # ── SIGNAL 2: Calendar ID ──────────────────────────────────────────
    for k in ("calendarId", "calendar_id", "calId"):
        if body.get(k):
            return True, f"{k}={body[k]!r}"

    # ── SIGNAL 3: Time slot PAIR — startTime AND endTime together ──────
    # startTime alone appears on contact records and is not sufficient.
    # Both fields together indicate a specific booked slot.
    _has_start = any(body.get(k) for k in ("startTime", "start_time", "StartTime"))
    _has_end   = any(body.get(k) for k in ("endTime",   "end_time",   "EndTime"))
    if _has_start and _has_end:
        _st = body.get("startTime") or body.get("start_time") or body.get("StartTime")
        _et = body.get("endTime")   or body.get("end_time")   or body.get("EndTime")
        return True, f"startTime={_st!r} + endTime={_et!r} (time slot pair)"

    # ── SIGNAL 4: Calendar-booking-specific fields ─────────────────────
    # These fields only appear in GHL calendar booking webhooks.
    for k in ("selectedTimezone", "selectedSlot", "calendarEventId",
              "bookingId", "booking_id", "slotStartTime", "slotEndTime"):
        if body.get(k):
            return True, f"{k}={body[k]!r} (calendar-only field)"

    # ── SIGNAL 5: eventType — appointment-specific values ONLY ────────
    # Uses eventType / event_type / event — NOT body["type"] which is the
    # GHL message type (SMS / MMS / EMAIL) and causes false positives.
    raw_et  = str(body.get("eventType") or body.get("event_type") or body.get("event") or "")
    et_norm = raw_et.lower().replace("_", "").replace(".", "").replace(" ", "")
    if et_norm in _APPT_EVENT_TYPES:
        return True, f"eventType={raw_et!r}"

    # ── SIGNAL 6: appointmentStatus — strict values only ──────────────
    # "new" and "accepted" are excluded — GHL adds them to every new lead.
    raw_status = str(
        body.get("appointmentStatus") or body.get("appointment_status") or ""
    ).lower().strip()
    if raw_status in _STRICT_APPT_STATUSES:
        return True, f"appointmentStatus={raw_status!r}"

    # ── SIGNAL 7: Source — exact calendar-tool match only ─────────────
    # Substring matching ("booking" in "facebook_booking") caused false positives.
    if _source in _CALENDAR_SOURCES:
        return True, f"source={_source!r} (calendar tool)"

    # ── SIGNAL 8: Nested appointment object with real fields inside ────
    # Only fires if the nested dict has at least one actual appointment field.
    # The old code returned truthy for ANY non-empty appointment dict, which
    # caused false positives when GHL included an empty appointment object.
    appt_obj = body.get("appointment")
    if isinstance(appt_obj, dict) and appt_obj:
        for k in ("id", "appointmentId", "calendarId", "startTime", "endTime"):
            if appt_obj.get(k):
                return True, f"appointment.{k}={appt_obj[k]!r}"

    # ── SIGNAL 9: One level into data/payload wrappers only ───────────
    # GHL v2 wraps calendar events in body["data"] or body["payload"].
    # Does NOT recurse into body["contact"] — contact profile fields
    # (e.g. a contact's stored appointmentStatus) are not booking evidence.
    for wk in ("data", "payload"):
        nested = body.get(wk)
        if isinstance(nested, dict) and nested:
            _inner_ok, _inner_reason = is_booked_appointment_lead(nested)
            if _inner_ok:
                return True, f"{wk}.{_inner_reason}"

    return False, "no_appointment_signals_found"


def _appointment_trigger(body: dict, _depth: int = 0) -> str:
    """
    Thin wrapper around is_booked_appointment_lead() that preserves the
    existing str | "" return contract used by callers throughout the file.
    Returns the reason string if this is a booking, "" if it is not.
    (The _depth parameter is kept for signature compatibility but unused.)
    """
    _is_booking, _reason = is_booked_appointment_lead(body)
    return _reason if _is_booking else ""


def is_appointment_payload(body: dict) -> bool:
    """Return True if body looks like a GHL appointment/calendar webhook."""
    _is_booking, _ = is_booked_appointment_lead(body)
    return _is_booking


# ─────────────────────────────────────────────
#  BOOKED-CONTACT TAG DETECTOR
# ─────────────────────────────────────────────

def is_already_booked(tags: list[str]) -> bool:
    configured_tag = BOOKED_TAG.lower().strip()
    for tag in tags:
        t = tag.lower().strip()
        if t == configured_tag:
            return True
        if t == "appointment_booked":
            return True
        if "appointment" in t and "booked" in t:
            return True
        if "appointment" in t and "confirmed" in t:
            return True
        # Post-flow tags — contact is past booking stage
        # GHL stamps these after bill is received and flow completes.
        # Present in Customer Replied webhooks even after a server restart wipes state.
        if t in ("bill_received", "flow_complete", "bill_received_confirmed"):
            return True
    return False


# ─────────────────────────────────────────────
#  DEDUPLICATION CACHE  (inbound + outbound)
#
#  INBOUND DEDUP — TWO-TIER DESIGN
#  ─────────────────────────────────
#  GHL commonly fires 2–4 webhooks for a single user SMS:
#    • The raw inbound SMS webhook  (has messageId)
#    • One or more workflow automation webhooks  (may have different/no messageId)
#
#  OLD BUG: _fingerprint() used a 1-MINUTE clock bucket.
#    sha1(contact_id + message + "YYYYMMDDHHMM")
#    • Two GHL webhook deliveries for the same "Yes" correctly deduped ✓
#    • But: user sends "Yes" to ownership question, then 30s later sends "Yes"
#      again (or to a different question) — SAME fingerprint → SECOND REPLY BLOCKED ✗
#    • Also: GHL workflow webhooks can arrive 1–5s after the raw webhook,
#      but a user replying again is typically 10s+ later — 1-minute window
#      can't distinguish them.
#
#  NEW DESIGN: TIER 1 → TIER 2 with hard short window
#  ──────────────────────────────────────────────────
#  TIER 1 (preferred): GHL message ID key
#    fp = sha1("GHL_MSG:" + contact_id + ":" + ghl_message_id)
#    • Exact: only deduplicates the exact same GHL message event.
#    • Window: permanent (same messageId = same event, always).
#    • Used when: parsed["event_id"] is non-empty AND appears to be a
#      GHL message ID (contains digits and letters, not a UUID contact ID).
#
#  TIER 2 (fallback): Short-window content fingerprint
#    fp = sha1("CONTENT:" + contact_id + ":" + message + ":" + ts_bucket)
#    where ts_bucket = floor(unix_timestamp / window_seconds)
#    • Window: 8 seconds for ALL messages.
#      — GHL's duplicate webhook delivery: typically 0–3 seconds apart → caught ✓
#      — User replying again: typically 10s+ later → allowed through ✓
#    • Does NOT vary by message length — short messages ("Yes") need the
#      same protection against multi-webhook delivery, not more restriction.
#
#  OUTBOUND DEDUP: unchanged (1-minute window for sends is correct — we never
#  want to send the same SMS text twice in one minute regardless of trigger).
# ─────────────────────────────────────────────

_processed_fingerprints: set[str] = set()
_outbound_fingerprints:  set[str] = set()
_MAX_DEDUP_CACHE = 2000

# ── [DEBUG-RESET] Dedup-cache ownership index ────────────────────────
# The dedup caches above store opaque SHA-1 hashes (and raw GHL event ids)
# with no recoverable link back to the contact they belong to.  That makes a
# *per-contact* cache purge impossible without a side index.
#
# This dict maps  cache_key -> contact_id  and is written next to every
# .add() below.  It is READ BY EXACTLY ONE CALLER: the /debug/reset-contact
# endpoint.  No dedup decision consults it, so inbound/outbound dedup
# behaviour for real leads is completely unchanged.
#
# Staleness is harmless by construction: entries may outlive the cache values
# they point at (the caches self-evict), and a reset then performs a
# set.discard() on an already-absent key — a no-op.
_dedup_owner: dict[str, str] = {}
_MAX_DEDUP_OWNER = 16000


def _remember_dedup_owner(key: str, contact_id: str) -> None:
    """[DEBUG-RESET] Record which contact planted a dedup cache key."""
    if not (key and contact_id):
        return
    if len(_dedup_owner) >= _MAX_DEDUP_OWNER:
        for _k in list(_dedup_owner)[: _MAX_DEDUP_OWNER // 2]:
            _dedup_owner.pop(_k, None)
    _dedup_owner[key] = contact_id

# Inbound dedup window: seconds.  Catches GHL's typical multi-webhook delivery
# gap (0–3s) while allowing a user's second reply (usually 10s+ later) through.
_INBOUND_DEDUP_WINDOW_SECS: int = 8


def _inbound_fingerprint(
    contact_id: str,
    message: str,
    ghl_message_id: str = "",
) -> tuple[str, str]:
    """
    Returns (fingerprint_hex, description_str) for inbound dedup logging.

    TIER 1 — GHL message ID (exact event identity):
      Used when ghl_message_id is non-empty.
      fp = sha1("GHL_MSG:{contact_id}:{ghl_message_id}")
      Description: "tier=1_ghl_id | ghl_message_id=..."

    TIER 2 — Short-window content hash (fallback):
      Used when ghl_message_id is empty.
      fp = sha1("CONTENT:{contact_id}:{message}:{8-second-bucket}")
      Description: "tier=2_content | window=8s | bucket=... | msg_len=..."
    """
    if ghl_message_id:
        raw     = f"GHL_MSG:{contact_id}:{ghl_message_id}"
        fp      = hashlib.sha1(raw.encode()).hexdigest()
        desc    = f"tier=1_ghl_id | ghl_message_id={ghl_message_id!r} | fp_prefix={fp[:12]}"
        return fp, desc

    # TIER 2: time-bucketed content hash
    _now_ts  = int(datetime.utcnow().timestamp())
    _bucket  = _now_ts // _INBOUND_DEDUP_WINDOW_SECS
    raw      = f"CONTENT:{contact_id}:{message}:{_bucket}"
    fp       = hashlib.sha1(raw.encode()).hexdigest()
    desc     = (
        f"tier=2_content | window={_INBOUND_DEDUP_WINDOW_SECS}s | "
        f"ts={_now_ts} | bucket={_bucket} | "
        f"msg_len={len(message)} | msg_preview={message[:30]!r} | "
        f"fp_prefix={fp[:12]}"
    )
    return fp, desc


def _evict_fingerprints_if_needed() -> None:
    """LRU eviction for the inbound fingerprint cache."""
    if len(_processed_fingerprints) >= _MAX_DEDUP_CACHE:
        _keep = list(_processed_fingerprints)[_MAX_DEDUP_CACHE // 2 :]
        _processed_fingerprints.clear()
        _processed_fingerprints.update(_keep)


def is_duplicate_inbound(
    contact_id: str,
    message: str,
    ghl_message_id: str = "",
) -> tuple[bool, str]:
    """
    Always checks and registers BOTH tiers simultaneously.

    TIER 1 (when ghl_message_id present): exact GHL event identity.
    TIER 2 (always):                       8-second content-hash window.

    Checking BOTH tiers fixes the dual-webhook problem:
      GHL fires two webhooks for one customer SMS with DIFFERENT message IDs.
      TIER 1 alone passes both (different IDs = different fingerprints).
      TIER 2 catches the second one (same content + same time bucket = same hash).

    Registering both also means that whichever path processes the first webhook
    (booked path or chat-widget path) always plants the content fingerprint, so the
    second webhook is blocked regardless of which code path it arrives on.
    """
    # Always compute TIER 2 content fingerprint
    t2_fp, t2_desc = _inbound_fingerprint(contact_id, message, ghl_message_id="")

    if ghl_message_id:
        # Compute TIER 1 exact-ID fingerprint
        t1_raw = f"GHL_MSG:{contact_id}:{ghl_message_id}"
        t1_fp  = hashlib.sha1(t1_raw.encode()).hexdigest()

        # Check TIER 1 first (same event_id = definite duplicate)
        if t1_fp in _processed_fingerprints:
            return True, f"tier=1_ghl_id | ghl_message_id={ghl_message_id!r} | fp={t1_fp[:12]}"

        # Check TIER 2 (same content within 8s = duplicate delivery, different ID)
        if t2_fp in _processed_fingerprints:
            return True, f"tier=2_content_window | ghl_message_id={ghl_message_id!r} (different from first) | {t2_desc}"

        # New event — register both so either tier catches subsequent duplicates
        _evict_fingerprints_if_needed()
        _processed_fingerprints.add(t1_fp)
        _processed_fingerprints.add(t2_fp)
        _remember_dedup_owner(t1_fp, contact_id)   # [DEBUG-RESET] index only
        _remember_dedup_owner(t2_fp, contact_id)   # [DEBUG-RESET] index only
        return False, f"new_event | tier=1_ghl_id={ghl_message_id!r} | also_registered_tier2 | {t2_desc}"
    else:
        # No GHL message ID — TIER 2 only
        if t2_fp in _processed_fingerprints:
            return True, f"fingerprint_match | {t2_desc}"
        _evict_fingerprints_if_needed()
        _processed_fingerprints.add(t2_fp)
        _remember_dedup_owner(t2_fp, contact_id)   # [DEBUG-RESET] index only
        return False, f"new_event | {t2_desc}"


# Keep old name as a shim so any remaining callers don't break.
# New callers should use is_duplicate_inbound() directly for the reason string.
def is_duplicate(contact_id: str, message: str) -> bool:
    """Backward-compat shim — wraps is_duplicate_inbound(), discards reason."""
    _dup, _ = is_duplicate_inbound(contact_id, message, ghl_message_id="")
    return _dup


def is_duplicate_outbound(contact_id: str, message: str) -> bool:
    """
    [ADAPT-6] Prevent sending the same outbound SMS twice within one minute.
    1-minute window is intentional for outbound — we never want to double-send
    the same reply text regardless of what triggered the second send.
    """
    fp = hashlib.sha1(
        f"OUT:{contact_id}:{message[:120]}:{datetime.utcnow().strftime('%Y%m%d%H%M')}".encode()
    ).hexdigest()
    if fp in _outbound_fingerprints:
        return True
    if len(_outbound_fingerprints) >= _MAX_DEDUP_CACHE:
        _keep = list(_outbound_fingerprints)[_MAX_DEDUP_CACHE // 2 :]
        _outbound_fingerprints.clear()
        _outbound_fingerprints.update(_keep)
    _outbound_fingerprints.add(fp)
    _remember_dedup_owner(fp, contact_id)          # [DEBUG-RESET] index only
    return False


# ─────────────────────────────────────────────
#  EVENT-ID IDEMPOTENCY CACHE  [FIX-11]
#
#  GHL can fire multiple webhooks for a single lead action.
#  Example: when a lead sends an MMS bill photo, GHL may fire:
#    1. The raw inbound message webhook (messageType=MMS, has attachment)
#    2. A second webhook from a "Customer replied" workflow automation
#       — same contactId, same timestamp, but different payload structure
#
#  Content-fingerprint dedup (above) only catches identical message bodies
#  in the same minute.  The GHL workflow webhook typically has DIFFERENT
#  content (e.g. a contact-update trigger body vs. the raw SMS body).
#
#  The fix: GHL attaches a unique id/messageId/eventId to most webhooks.
#  We cache these in a long-lived set.  Any webhook whose event_id was
#  already processed is immediately dropped — before any state is read.
#
#  This is the idempotency layer.  It does NOT replace content dedup;
#  both run in series for defense-in-depth.
# ─────────────────────────────────────────────

_processed_event_ids: set[str] = set()
_MAX_EVENT_ID_CACHE = 5000


def _is_duplicate_event_id(event_id: str, contact_id: str = "") -> bool:
    """
    [FIX-11] Returns True if this event_id was already processed.

    contact_id is OPTIONAL and purely informational: when supplied it is
    recorded in the [DEBUG-RESET] ownership index so /debug/reset-contact can
    purge this contact's event ids.  It has no effect on the return value.
    LRU-evicts the oldest half when the cache is full.
    Returns False (non-duplicate) when event_id is empty — we cannot
    deduplicate what we cannot identify, so we let it through.
    """
    if not event_id:
        return False   # no ID → cannot dedup → let it through

    if event_id in _processed_event_ids:
        return True

    if len(_processed_event_ids) >= _MAX_EVENT_ID_CACHE:
        pruned = list(_processed_event_ids)[_MAX_EVENT_ID_CACHE // 2:]
        _processed_event_ids.clear()
        _processed_event_ids.update(pruned)

    _processed_event_ids.add(event_id)
    _remember_dedup_owner(event_id, contact_id)    # [DEBUG-RESET] index only
    return False


# ─────────────────────────────────────────────
#  PER-CONTACT PROCESSING LOCK + INBOUND DEBOUNCE
#
#  Root cause of duplicate replies / race conditions:
#  FastAPI's asyncio event loop handles multiple concurrent webhook requests.
#  Between any `await` point in inbound_webhook, a SECOND coroutine for the
#  SAME contact can advance past the dedup checks and reach michael_agent()
#  before the first coroutine has saved state.  Result: two calls to
#  michael_agent() with identical pre-transition state → two outbound SMS.
#
#  Fix 1 — Per-contact asyncio.Lock:
#    One lock per contact_id, stored in _contact_processing_locks.
#    Before any state read or michael_agent() call, the handler checks:
#      • if locked → drop immediately (another coroutine is processing)
#      • if free   → acquire → process → release in finally
#    asyncio.Lock is single-threaded-safe: check + acquire is atomic because
#    there is no `await` between them (the event loop cannot interleave).
#    TTL: handled by the finally block — lock always released after processing.
#
#  Fix 2 — Post-completion debounce:
#    After the lock is released (processing complete + SMS sent), a 2-second
#    window suppresses any new trigger for the same contact.  Covers the edge
#    case where GHL fires a delayed second webhook AFTER processing finishes
#    (the lock is free but the state update is brand-new).  2 seconds is safe
#    because it takes a human 3-15+ seconds to read a received SMS and reply.
# ─────────────────────────────────────────────

# contact_id → asyncio.Lock  (one lock per contact, created on first access)
_contact_processing_locks: dict[str, asyncio.Lock] = {}

# contact_id → monotonic timestamp of when we last completed processing
_contact_last_processed_ts: dict[str, float] = {}
_INBOUND_DEBOUNCE_SECS = 2.0   # seconds to suppress after processing completes


def _get_or_create_contact_lock(contact_id: str) -> asyncio.Lock:
    if contact_id not in _contact_processing_locks:
        _contact_processing_locks[contact_id] = asyncio.Lock()
    return _contact_processing_locks[contact_id]


def _is_debounced(contact_id: str) -> tuple[bool, float]:
    """Return (is_debounced, elapsed_secs) for logging."""
    last    = _contact_last_processed_ts.get(contact_id, 0.0)
    elapsed = _time_module.monotonic() - last
    return elapsed < _INBOUND_DEBOUNCE_SECS, elapsed


def _mark_contact_processed(contact_id: str) -> None:
    """Called from the finally block each time inbound_webhook completes for a contact."""
    _contact_last_processed_ts[contact_id] = _time_module.monotonic()


# ─────────────────────────────────────────────
#  PHONE→CONTACT LOOKUP  [PATH-1/2]
#
#  Root-cause fix for the "contact reset" bug:
#
#  GHL sometimes fires the appointment booking webhook with a contact_id
#  that differs from the one used during the chat widget SMS flow.
#  When that happens the agent creates fresh state for the new contact_id
#  and treats the person like a brand-new cold lead — exactly the jarring
#  "re-intro" experience the user reports.
#
#  Solution: maintain a normalized phone → contact_id map.
#  Every time we see a phone+contact_id pair we register it.
#  When an appointment webhook arrives with a contact_id that has no/INITIAL
#  state, we look the phone up and switch to the known contact_id — preserving
#  the full conversation history and entry_path from the chat widget session.
# ─────────────────────────────────────────────

_phone_to_contact: dict[str, str] = {}


def _normalize_phone(phone: str) -> str:
    """Strip all non-digits and return last 10 digits (US numbers)."""
    digits = re.sub(r"\D", "", phone or "")
    return digits[-10:] if len(digits) >= 10 else digits


def _register_phone(contact_id: str, phone: str) -> None:
    """
    Map a phone number to a contact_id for future cross-webhook lookups.
    First registration wins — we never overwrite with a potentially stale
    contact_id from a downstream appointment webhook.
    """
    norm = _normalize_phone(phone)
    if not (norm and contact_id):
        return
    existing = _phone_to_contact.get(norm)
    if not existing:
        _phone_to_contact[norm] = contact_id
        print(f"[PHONE-MAP] 📱 Registered {norm} → {contact_id!r}")
    elif existing != contact_id:
        # Already mapped to a different ID — keep the original (more trusted)
        print(f"[PHONE-MAP] ⚠  Phone {norm} already mapped to {existing!r} — ignoring new {contact_id!r}")


def _resolve_contact_id_by_phone(webhook_contact_id: str, phone: str) -> str:
    """
    For appointment booking webhooks that may arrive with a different contact_id
    than the one used during the chat widget flow, look up the canonical
    contact_id via phone number.

    Only redirects when:
      • Phone matches a previously registered contact_id
      • That contact has non-trivial state (non-INITIAL stage or has messages)
      • The webhook contact_id itself has no meaningful state

    Returns the contact_id to use — either the original or the looked-up one.
    """
    if not phone:
        return webhook_contact_id

    norm       = _normalize_phone(phone)
    known_cid  = _phone_to_contact.get(norm)

    if not known_cid or known_cid == webhook_contact_id:
        return webhook_contact_id   # no hit or same contact — nothing to do

    known_state   = _state_store.get(known_cid)
    webhook_state = _state_store.get(webhook_contact_id)

    has_known_history = (
        known_state is not None and
        (known_state.get("stage") not in (Stage.INITIAL, None) or
         bool(known_state.get("messages")))
    )
    webhook_is_fresh = (
        webhook_state is None or
        (webhook_state.get("stage") in (Stage.INITIAL, None) and
         not webhook_state.get("messages"))
    )

    if has_known_history and webhook_is_fresh:
        print(f"[PHONE-MAP] 🔗 Appt webhook CID {webhook_contact_id!r} has no prior history")
        print(f"[PHONE-MAP] 🔗 Phone {norm!r} → known contact {known_cid!r} "
              f"(stage={known_state.get('stage')}, "
              f"msgs={len(known_state.get('messages', []))})")
        print(f"[PHONE-MAP] 🔗 Redirecting to {known_cid!r} — preserving conversation state")
        return known_cid

    return webhook_contact_id


# ─────────────────────────────────────────────
#  INBOUND STATE PARSERS  [ADAPT-2]
#
#  Extract qualification signals from the lead's
#  raw text BEFORE Claude is called, so the
#  dynamic system prompt reflects what we just
#  learned. Conservative: only updates fields
#  that are currently unknown.
# ─────────────────────────────────────────────

# Explicit homeowner confirmation (always trusted regardless of stage)
_HOMEOWNER_YES_EXPLICIT = re.compile(
    r'\b(i\s+own|i\s+am\s+the\s+owner|i\'m\s+the\s+owner|my\s+home|my\s+house|homeowner|yes\s+i\s+own|i\s+do\s+own)\b',
    re.IGNORECASE,
)
# Explicit renter (always trusted)
_HOMEOWNER_NO_EXPLICIT = re.compile(
    r'\b(i\s+rent\b|i\'m\s+renting|i\s+am\s+renting|renter\b|not\s+the\s+owner|i\s+don\'?t\s+own)\b',
    re.IGNORECASE,
)
# Brief yes/no — trusted when stage is ASK_OWNERSHIP AND location already confirmed
# (so the bare 'yes' isn't ambiguously answering the area question)
_BRIEF_YES = re.compile(r'^(yes|yep|yeah|yup|i\s+do)[.!\s]*$', re.IGNORECASE)
_BRIEF_NO  = re.compile(r'^(no|nope|nah|i\s+don\'?t)[.!\s]*$', re.IGNORECASE)

# Service-area confirmation — explicit STL Missouri-side / Ameren mentions
# (post-migration: KC / Kansas terms intentionally excluded — those leads are
# now out-of-area; Claude's [DISQUALIFY:OUT_OF_AREA] handles them politely.)
_LOC_YES_EXPLICIT = re.compile(
    r"(i'?m?\s*(in|near|close\s+to|by)\b"
    r"|within\s+\d+\s*(min|minutes?|hr|hours?)"
    r"|(about|around|less\s+than)\s+\d+\s*(min|minutes?|hr|hours?)"
    r"|\b(stl|st\.?\s*louis|saint\s+louis|missouri|ameren|st\.?\s*charles|saint\s+charles|jefferson\s+county|franklin\s+county|lincoln\s+county|warren\s+county|o'?fallon|st\.?\s*peters|saint\s+peters|chesterfield|ballwin|kirkwood|webster\s+groves|wentzville)\b)",
    re.IGNORECASE,
)

# Bill amount: "$150", "$200/month", "150 a month", "200 dollars a month"
_BILL_AMOUNT = re.compile(
    r'\$\s*(\d{2,4})(?:\s*(?:/\s*month|a\s+month|per\s+month|monthly))?'
    r'|(\d{2,4})\s*(?:dollars?|bucks?)\s*(?:a\s+month|per\s+month|monthly)'
    r'|(\d{2,4})\s*(?:a\s+month|per\s+month|monthly)\b',
    re.IGNORECASE,
)


def _detect_homeowner(text: str, stage: Stage, location_confirmed: bool = False,
                      previous_outbound_type: str = "unknown") -> Optional[str]:
    """
    Return 'yes', 'no', or None.
    Explicit ownership patterns always match regardless of stage.
    Brief yes/no ('yes', 'yep', etc.) are trusted when:
      - stage is ASK_OWNERSHIP (we KNOW the prior question was about ownership)
      - location_confirmed is preferred but NOT required at ASK_OWNERSHIP —
        the new flow always sets location_confirmed=True before advancing to
        ASK_OWNERSHIP, but belt-and-suspenders: if stage=ASK_OWNERSHIP, we
        trust the brief yes/no because that is EXACTLY the question we just asked.

    [FIX-8.C] Removed the `location_confirmed` hard requirement at ASK_OWNERSHIP.
    If stage=ASK_OWNERSHIP, we know the prior outreach asked "Do you own the home?"
    so "Yes", "Yep", "I do", "Yeah" MUST be homeowner confirmations.
    """
    if _HOMEOWNER_NO_EXPLICIT.search(text):
        return "no"
    if _HOMEOWNER_YES_EXPLICIT.search(text):
        return "yes"
    # At ASK_OWNERSHIP, brief yes/no is trusted ONLY when location is already confirmed.
    # If location is not yet confirmed, the pending question was utility (first outreach),
    # so brief "yes" is a utility confirmation — not homeowner — in that reply.
    # Requiring location_confirmed=True prevents a single "yes" from simultaneously
    # setting both utility and ownership when they arrive in separate messages.
    # [BUNDLE-1] Stage is a weaker signal than the message actually sent. When
    # the previous outbound is known and was NOT about ownership, a bare "yes"
    # cannot be an ownership answer however the stage happens to be labelled —
    # stale stages are common after a restart, and inventing a fact here is
    # exactly the accident the qualification record exists to prevent.
    _asked_ownership = previous_outbound_type in (
        OUT_UNKNOWN, OUT_OWNERSHIP_Q, OUT_BUNDLED_QUAL,
    )
    if stage == Stage.ASK_OWNERSHIP and location_confirmed and _asked_ownership:
        if _BRIEF_YES.match(text.strip()):
            return "yes"
        if _BRIEF_NO.match(text.strip()):
            return "no"
    return None


def _detect_location_confirmed(text: str, location_confirmed_already: bool, homeowner_set: bool,
                               previous_outbound_type: str = "unknown") -> bool:
    """
    Return True if this inbound text confirms service area (St. Louis Missouri-side, Ameren Missouri).

    Two cases:
      1. Explicit mention of a known city, state, or distance → always trusted.
      2. Brief 'yes' / 'yep' → only trusted when we're clearly still on the
         first question (neither location nor homeowner confirmed yet).

    Returns False (do not update) if unclear.
    Does NOT detect 'no' — out-of-area disqualification is handled by Claude.
    """
    if location_confirmed_already:
        return False   # already set — nothing to do
    if _LOC_YES_EXPLICIT.search(text):
        return True
    # Brief 'yes' when on the very first question (location not confirmed, no
    # owner answer yet) AND the last thing we sent was not some other question.
    _asked_area = previous_outbound_type in (
        OUT_UNKNOWN, OUT_UTILITY_Q, OUT_BUNDLED_QUAL,
    )
    if not homeowner_set and _asked_area and _BRIEF_YES.match(text.strip()):
        return True
    return False


def _detect_bill_amount(text: str) -> str:
    """Extract a monthly bill amount from text. Returns '$N/month' string or ''."""
    m = _BILL_AMOUNT.search(text)
    if m:
        raw = m.group(1) or m.group(2) or m.group(3)
        if raw:
            try:
                val = int(raw.replace(",", ""))
                if 10 <= val <= 2000:
                    return f"${val}/month"
            except ValueError:
                log.debug(f"_detect_bill_amount: regex matched {raw!r} but int() conversion failed — skipping")
    return ""


def update_state_from_inbound(state: dict, inbound_text: str,
                              previous_outbound_type: str = "unknown") -> None:
    """
    [ADAPT-2] Parse the lead's inbound message and update state fields in place.
    Called BEFORE Claude so build_system_prompt() sees the latest signals.
    Only updates fields that are currently unknown to avoid false overrides.
    """
    stage      = state["stage"]
    loc_conf   = bool(state.get("location_confirmed"))
    homeown    = state.get("homeowner")

    # Snapshot BEFORE any update this pass.
    # _detect_homeowner() receives the PRE-update value so a single "yes" cannot
    # simultaneously confirm location AND ownership in the same inbound message.
    _loc_conf_before = loc_conf

    # Service area — parse first so location_confirmed is current for logging.
    if _detect_location_confirmed(inbound_text, loc_conf, homeowner_set=homeown is not None,
                                  previous_outbound_type=previous_outbound_type):
        state["location_confirmed"] = True
        loc_conf = True
        print(f"[STATE] 📍 location_confirmed=True | FIELD_UPDATED=location_confirmed")

    # Homeowner — only parse when we haven't confirmed yet.
    # Pass PRE-update loc_conf: if "yes" just confirmed utility this pass, it cannot
    # also confirm homeownership — those are separate questions requiring separate replies.
    if state.get("homeowner") is None:
        detected = _detect_homeowner(inbound_text, stage, location_confirmed=_loc_conf_before,
                                     previous_outbound_type=previous_outbound_type)
        if detected:
            state["homeowner"] = detected
            print(f"[STATE] 🏠 homeowner={detected!r} | FIELD_UPDATED=homeowner")

    # Bill — parse any time it's unknown (leads often volunteer this early)
    if not state.get("monthly_bill"):
        detected_bill = _detect_bill_amount(inbound_text)
        if detected_bill:
            state["monthly_bill"] = detected_bill
            print(f"[STATE] 💡 monthly_bill={detected_bill!r} parsed from: {inbound_text[:60]!r}")


# ─────────────────────────────────────────────
#  INTENT DETECTION  [FIX-5]
#
#  Classifies the lead's inbound message BEFORE
#  calling Claude so we can answer questions
#  directly (fast path) or tell Claude to handle
#  them before advancing the qualification flow.
# ─────────────────────────────────────────────

_COST_QUESTION = re.compile(
    r'\b('
    r'how\s+(expensive|much|costly)|'
    r'what\s+(does\s+it|do\s+you|does\s+solar)\s+cost|'
    r'is\s+(it|this|solar)\s+free|'
    r'do\s+i\s+have\s+to\s+pay|'
    r'upfront\s+cost|'
    r'cost\s+me\b|'
    r'how\s+much\s+is\s+(?:it|this|solar)|'
    r'what\s+are\s+the\s+(payments?|fees?)|'
    r'do\s+you\s+(finance|offer\s+financing)|'
    r'financing\b|'
    r'monthly\s+payment|'
    r'out\s+of\s+pocket'
    r')\b',
    re.IGNORECASE,
)
_PROCESS_QUESTION = re.compile(
    r'\b('
    r'how\s+does\s+(this|it|solar)\s+work|'
    r'what\s+happens\s+at\s+(the\s+)?appointment|'
    r'what\s+(do|will)\s+you\s+do|'
    r'what\s+is\s+(this|the\s+appointment)|'
    r'tell\s+me\s+more|'
    r'more\s+info|'
    r'what\s+is\s+involved|'
    r'what\s+does\s+the\s+(appointment|consult|visit)\s+involve'
    r')\b',
    re.IGNORECASE,
)


def detect_intent(text: str) -> str:
    """
    [FIX-5] Classify the lead's inbound message intent.
    Returns one of: 'cost_question' | 'process_question' | 'question' | 'answer'

    Used in michael_agent() to:
      • Short-circuit Claude with a canned answer for common questions
      • Prevent SEND_BOOKING intercept from wiping out Claude's question answer
    """
    if _COST_QUESTION.search(text):
        return "cost_question"
    if _PROCESS_QUESTION.search(text):
        return "process_question"
    if "?" in text:
        return "question"
    return "answer"


def build_cost_answer(state: dict) -> str:
    """
    [FIX-5] Human, context-aware answer to cost / pricing questions.

    • References the lead's known bill amount when available
    • Emphasises no upfront cost and free consultation
    • If lead is already qualified, appends booking link
    """
    bill      = state.get("monthly_bill", "")
    qualified = state.get("qualified", False) or state.get("stage") == Stage.SEND_BOOKING

    if bill:
        bill_line = (
            f"With a {bill} bill, you're really not buying solar — you're replacing that Ameren charge "
            f"with one fixed payment that doesn't move when they file the next rate case."
        )
    else:
        bill_line = (
            "You're not really buying solar — you're replacing your Ameren bill with a fixed payment "
            "that depends on your home, usage, and how your system gets set up."
        )

    base = (
        f"I wish I could give you a simple number, but it genuinely depends on your home, "
        f"your usage, and how everything gets structured. {bill_line} "
        f"The only way to get a real number is to look at your actual setup — that's exactly "
        f"what the free consultation does."
    )

    if qualified:
        contact_name = state.get("contact_name", "")
        booking = build_booking_message(full_name=contact_name)
        return f"{base}\n\n{booking}"
    return base


def build_process_answer(state: dict) -> str:
    """
    [FIX-5] Human answer to 'how does this work / what happens at the appointment' questions.
    If lead is already qualified, appends booking link.
    """
    qualified = state.get("qualified", False) or state.get("stage") == Stage.SEND_BOOKING

    base = (
        "It's a free in-person visit at your home. "
        "Your advisor pulls up your actual Ameren usage, maps out what a system would look like "
        "for your specific setup, and walks you through the real numbers — no estimates. "
        "No pressure — if it doesn't make sense for your home, they'll tell you straight up."
    )

    if qualified:
        contact_name = state.get("contact_name", "")
        booking = build_booking_message(full_name=contact_name)
        return f"{base}\n\n{booking}"
    return base


# ─────────────────────────────────────────────
#  QUALIFICATION RECORD  [BUNDLE-1]
#
#  WHY THIS EXISTS
#    Qualification used to be a script: one question per SMS, in a fixed
#    order, driven by Stage. That reads like a form, not a person, and it
#    costs conversions — three round trips before anyone sees a calendar.
#
#    It also could not represent what it needed to. Of the three original
#    fields only `homeowner` was tri-state; `location_confirmed` was a bool
#    that conflated "no" with "not asked yet" AND conflated service area with
#    utility provider, and the $100 threshold existed only as prose inside
#    the prompt, never as a field.
#
#  THE MODEL
#    Three independent facts, each genuinely tri-state, each inferable from
#    natural language in any order, from any message:
#
#      q_homeowner      yes | no | unknown
#      q_utility        ameren | other | unknown
#      q_bill_100_plus  yes | no | unknown
#
#    The agent asks only for what is missing. When two or more are unknown it
#    states all the conditions in one conversational line and lets the
#    homeowner confirm them together.
#
#  THE AMBIGUITY RULE — the important one
#    A bare "yes" confirms ALL THREE facts only when the message it answers
#    explicitly bundled all three. A "yes" to "Are you with Ameren?" confirms
#    Ameren and nothing else, ever. That distinction is enforced in exactly
#    one place — extract_qualification_facts() — and is what stops a bundled
#    convenience from becoming a silent assumption.
#
#  LEGACY FIELDS
#    homeowner / location_confirmed / monthly_bill are still written and read
#    by tag resolution, stage restore, the booked guard and the prompt. They
#    are kept in sync with the record in both directions by
#    sync_qualification_fields(), so nothing downstream changed.
# ─────────────────────────────────────────────

QUAL_UNKNOWN = "unknown"
QUAL_YES     = "yes"
QUAL_NO      = "no"
UTIL_AMEREN  = "ameren"
UTIL_OTHER   = "other"

# The threshold the offer is built around. Below it, the math does not work.
BILL_THRESHOLD = 100

VERDICT_QUALIFIED    = "qualified"
VERDICT_DISQUALIFIED = "disqualified"
VERDICT_INCOMPLETE   = "incomplete"

# Field keys, used in logs and in the "what is still missing" list.
Q_HOMEOWNER = "homeowner"
Q_UTILITY   = "utility"
Q_BILL      = "bill_100_plus"


# ── Ownership, stated explicitly ─────────────────────────────────────────
_OWN_YES_RE = re.compile(
    r"\b(i\s+own|we\s+own|i'?m\s+the\s+owner|i\s+am\s+the\s+owner"
    r"|own\s+(?:it|the\s+(?:home|house|place|property))"
    r"|homeowner|home\s?owner|owner\s+here"
    r"|it'?s\s+my\s+(?:home|house|place)|my\s+own\s+home"
    r"|bought\s+(?:it|the\s+(?:home|house|place)))\b",
    re.IGNORECASE,
)
_OWN_NO_RE = re.compile(
    r"\b(i\s+rent\b|we\s+rent\b|i'?m\s+renting|i\s+am\s+renting|renting\b|renter\b"
    r"|not\s+the\s+owner|don'?t\s+own|do\s+not\s+own|landlord|tenant"
    r"|leasing|i\s+lease\b)\b",
    re.IGNORECASE,
)

# ── Utility, stated explicitly ───────────────────────────────────────────
# Ameren Missouri is the only qualifying provider. Ameren ILLINOIS is a
# different utility on the other side of the river and does NOT qualify,
# so it is matched before the bare "ameren" test.
_UTIL_AMEREN_IL_RE = re.compile(r"ameren\s+(?:il\b|illinois)", re.IGNORECASE)
_UTIL_AMEREN_RE    = re.compile(r"\bameren\b", re.IGNORECASE)

# Named non-Ameren providers seen around the St. Louis metro, plus the
# generic co-op phrasing. Any of these disqualifies on utility.
_UTIL_OTHER_RE = re.compile(
    r"\b(cuivre\s*river|evergy|spire|citizens\s+electric|rural\s+electric"
    r"|electric\s+co-?op|co-?operative|kcp&?l|empire\s+district"
    r"|missouri\s+rural|lacle?de)\b"
    r"|\bnot\s+(?:with\s+|on\s+)?ameren\b"
    r"|\bdon'?t\s+have\s+ameren\b",
    re.IGNORECASE,
)

# ── Bill amount and threshold ────────────────────────────────────────────
# _BILL_AMOUNT (above) needs a "$" or an explicit "a month". People also
# write "my bill is around 180", so a bare number is accepted when the
# message is clearly about the bill.
_BILL_CONTEXT_RE = re.compile(r"\b(bill|electric|power|ameren|pay|spend)\b", re.IGNORECASE)
_BILL_BARE_NUM_RE = re.compile(r"\b(\d{2,4})\b")

_BILL_OVER_RE = re.compile(
    r"\b(over|above|more\s+than|north\s+of|at\s+least|upwards\s+of|higher\s+than)\b"
    r"[^\d]{0,12}\$?\s*(\d{2,4})",
    re.IGNORECASE,
)
_BILL_UNDER_RE = re.compile(
    r"\b(under|below|less\s+than|lower\s+than|no\s+more\s+than)\b"
    r"[^\d]{0,12}\$?\s*(\d{2,4})",
    re.IGNORECASE,
)
# Vague amounts people actually use. Only mapped when clearly about the bill.
_BILL_VAGUE_HIGH_RE = re.compile(
    r"\b(couple\s+hundred|few\s+hundred|two\s+hundred|three\s+hundred"
    r"|hundreds|way\s+more\s+than\s+that)\b",
    re.IGNORECASE,
)


def _detect_ownership(text: str) -> Optional[str]:
    """QUAL_YES / QUAL_NO from an explicit statement, else None."""
    t = text or ""
    if _OWN_NO_RE.search(t):
        return QUAL_NO
    if _OWN_YES_RE.search(t):
        return QUAL_YES
    return None


def _detect_utility(text: str) -> Optional[str]:
    """UTIL_AMEREN / UTIL_OTHER from an explicit statement, else None."""
    t = text or ""
    if _UTIL_AMEREN_IL_RE.search(t) or _UTIL_OTHER_RE.search(t):
        return UTIL_OTHER
    if _UTIL_AMEREN_RE.search(t):
        return UTIL_AMEREN
    return None


def _detect_bill_threshold(text: str) -> tuple[Optional[str], str]:
    """
    (verdict, amount_string) where verdict is QUAL_YES / QUAL_NO / None and
    amount_string is "$NNN/month" when a concrete figure was given, else "".
    """
    t = text or ""

    m = _BILL_OVER_RE.search(t)
    if m:
        try:
            val = int(m.group(2))
            return (QUAL_YES if val >= BILL_THRESHOLD else None), ""
        except ValueError:
            pass

    m = _BILL_UNDER_RE.search(t)
    if m:
        try:
            val = int(m.group(2))
            return (QUAL_NO if val <= BILL_THRESHOLD else None), ""
        except ValueError:
            pass

    amount = _detect_bill_amount(t)
    if not amount and _BILL_CONTEXT_RE.search(t):
        for raw in _BILL_BARE_NUM_RE.findall(t):
            try:
                val = int(raw)
            except ValueError:
                continue
            if 10 <= val <= 2000:
                amount = f"${val}/month"
                break

    if amount:
        try:
            val = int(amount.lstrip("$").split("/")[0])
            return (QUAL_YES if val >= BILL_THRESHOLD else QUAL_NO), amount
        except ValueError:
            return None, ""

    if _BILL_VAGUE_HIGH_RE.search(t) and _BILL_CONTEXT_RE.search(t):
        return QUAL_YES, ""

    return None, ""


# ── Bundled-criteria detection on the OUTBOUND side ──────────────────────
# A message is a bundled qualification offer when it names at least two of
# the three criteria AND frames them conditionally. Both halves are required:
# "About what does the Ameren bill usually run you?" names two criteria but
# asks for one of them, and must stay a bill question.

_CRIT_OWN_MENTION_RE = re.compile(
    r"\b(own\s+(?:the|your|it)|homeowner|home\s?owner|you'?re\s+the\s+owner)\b",
    re.IGNORECASE,
)
_CRIT_UTIL_MENTION_RE = re.compile(r"\bameren\b", re.IGNORECASE)
_CRIT_BILL_MENTION_RE = re.compile(
    r"(\$\s*\d{2,4}|\b\d{2,4}\s*(?:\+|or\s+more|a\s+month|/\s*month|per\s+month)"
    r"|\bbill\b|\bspend(?:ing)?\b|\bnorth\s+of\b)",
    re.IGNORECASE,
)
_CONDITIONAL_FRAME_RE = re.compile(
    r"\b(if|assuming|as\s+long\s+as|so\s+long\s+as|provided\s+that|whenever)\b",
    re.IGNORECASE,
)


def bundled_criteria_mentioned(text: str) -> int:
    """How many of the three criteria this message names (0-3)."""
    t = text or ""
    return sum((
        bool(_CRIT_OWN_MENTION_RE.search(t)),
        bool(_CRIT_UTIL_MENTION_RE.search(t)),
        bool(_CRIT_BILL_MENTION_RE.search(t)),
    ))


def is_bundled_qualification_message(text: str) -> bool:
    """True when an outbound message states the criteria together, conditionally."""
    t = (text or "").strip()
    if not t:
        return False
    return bundled_criteria_mentioned(t) >= 2 and bool(_CONDITIONAL_FRAME_RE.search(t))


# ── Blanket confirmations of a bundled offer ─────────────────────────────
# "That's me" and "all of those apply" are affirmatives that carry no
# affirmative token, so is_affirmative_reply() does not see them. They are
# only ever read as a blanket yes when answering a bundled offer.
_BLANKET_YES_RE = re.compile(
    r"^\s*(?:yes[,\s]+)?"
    r"(that'?s\s+me|that\s+is\s+me|thats\s+me"
    r"|all\s+(?:of\s+)?(?:those|that|three)\s+(?:apply|applies|are\s+true|fit)"
    r"|all\s+(?:of\s+)?that\s+applies"
    r"|that\s+all\s+applies|all\s+true|yes\s+to\s+all"
    r"|(?:that|those)\s+(?:all\s+)?(?:apply|applies)\s+to\s+me"
    r"|applies\s+to\s+me)"
    r"[\s.!]*$",
    re.IGNORECASE,
)


def is_blanket_confirmation(text: str) -> bool:
    """True for 'that's me' / 'all of those apply' style whole-bundle yeses."""
    t = (text or "").strip()
    if not t or len(t) > 60:
        return False
    return bool(_BLANKET_YES_RE.match(t))


def extract_qualification_facts(text: str,
                                previous_outbound_type: str = "unknown") -> dict:
    """
    Facts this inbound message establishes, as {field: value}. Only fields the
    message genuinely settles appear; everything else is simply absent.

    THE AMBIGUITY RULE lives here and nowhere else:

      • A bare affirmative after a BUNDLED offer confirms all three, because
        all three is exactly what was put to them.
      • A bare affirmative after a single question confirms that one fact.
      • A bare affirmative after a conversion invitation confirms NOTHING
        factual — it signals interest, which is handled separately. Booking
        someone is not the same as claiming to know they own their home.
      • Anything stated explicitly OVERRIDES the blanket yes. "Yes, but I
        have Cuivre River" is a yes to the invitation and a no on utility.
    """
    t = (text or "").strip()
    facts: dict = {}
    if not t:
        return facts

    blanket = is_blanket_confirmation(t)
    bare    = is_affirmative_reply(t) or blanket

    if bare:
        if previous_outbound_type == OUT_BUNDLED_QUAL:
            facts[Q_HOMEOWNER] = QUAL_YES
            facts[Q_UTILITY]   = UTIL_AMEREN
            facts[Q_BILL]      = QUAL_YES
            facts["_source"]   = "bundled_affirmative"
        elif previous_outbound_type == OUT_OWNERSHIP_Q:
            facts[Q_HOMEOWNER] = QUAL_YES
            facts["_source"]   = "ownership_affirmative"
        elif previous_outbound_type == OUT_UTILITY_Q:
            facts[Q_UTILITY]   = UTIL_AMEREN
            facts["_source"]   = "utility_affirmative"
        # A bare "yes" to "what does the bill run?" settles nothing, and a
        # bare "yes" to a conversion invitation is interest, not a fact.

    # Explicit statements always win over an inferred blanket yes.
    own = _detect_ownership(t)
    if own:
        facts[Q_HOMEOWNER] = own
        facts["_source"]   = "explicit"

    util = _detect_utility(t)
    if util:
        facts[Q_UTILITY]   = util
        facts["_source"]   = "explicit"

    bill_verdict, bill_amount = _detect_bill_threshold(t)
    if bill_verdict:
        facts[Q_BILL]      = bill_verdict
        facts["_source"]   = "explicit"
        if bill_amount:
            facts["bill_amount"] = bill_amount

    return facts


def apply_qualification_facts(state: dict, facts: dict, contact_id: str = "") -> list[str]:
    """
    Write established facts into the record. Returns the field names changed.

    Fill-and-correct, not blind overwrite: a field moves off "unknown"
    freely, but an existing answer is only replaced by an EXPLICIT one — a
    blanket bundled yes can never overturn something the homeowner actually
    told us.
    """
    changed: list[str] = []
    explicit = facts.get("_source") == "explicit"

    for key, field in ((Q_HOMEOWNER, "q_homeowner"),
                       (Q_UTILITY,   "q_utility"),
                       (Q_BILL,      "q_bill_100_plus")):
        value = facts.get(key)
        if not value:
            continue
        current = state.get(field, QUAL_UNKNOWN) or QUAL_UNKNOWN
        if current == value:
            continue
        if current != QUAL_UNKNOWN and not explicit:
            continue
        state[field] = value
        changed.append(field)
        print(f"[QUAL] {field}={value!r} | FIELD_UPDATED={field} | src={facts.get('_source', '?')}")

    amount = facts.get("bill_amount")
    if amount and not state.get("monthly_bill"):
        state["monthly_bill"] = amount
        changed.append("monthly_bill")
        print(f"[QUAL] monthly_bill={amount!r} | FIELD_UPDATED=monthly_bill")

    if changed and contact_id:
        ev("QUALIFICATION_UPDATED", contact_id,
           fields=",".join(changed), source=facts.get("_source", "?"),
           homeowner=state.get("q_homeowner"), utility=state.get("q_utility"),
           bill_100_plus=state.get("q_bill_100_plus"))
    return changed


def sync_qualification_fields(state: dict) -> None:
    """
    Keep the record and the original fields telling the same story.

    NEW → LEGACY is unconditional: everything downstream (tags, stage
    restore, prompt memory) still reads the originals.

    LEGACY → NEW is deliberately partial. `homeowner` and `monthly_bill` mean
    the same thing in both models and map straight across. `location_confirmed`
    does NOT: it is set to True at form submission because an address is in
    the service area, which says nothing about who supplies the electricity.
    Mapping it to q_utility="ameren" would manufacture a confirmation nobody
    gave — precisely the assumption this record exists to prevent.
    """
    # ── legacy → record ──
    if state.get("q_homeowner", QUAL_UNKNOWN) == QUAL_UNKNOWN and state.get("homeowner"):
        state["q_homeowner"] = QUAL_YES if state["homeowner"] == "yes" else QUAL_NO

    if state.get("q_bill_100_plus", QUAL_UNKNOWN) == QUAL_UNKNOWN and state.get("monthly_bill"):
        try:
            val = int(str(state["monthly_bill"]).lstrip("$").split("/")[0])
            state["q_bill_100_plus"] = QUAL_YES if val >= BILL_THRESHOLD else QUAL_NO
        except ValueError:
            pass

    # A booked contact, or one explicitly flagged qualified, demonstrably
    # answered all three at some point, so the record must not re-open them.
    #
    # Stage SEND_BOOKING alone is NOT such evidence any more: the conversion
    # path reaches it whenever a high-intent lead gets the link, which now
    # happens before the criteria are known. Backfilling from the stage would
    # erase the very unknowns that put the conditions in the message.
    if state.get("qualified") or state.get("stage") == Stage.BOOKED:
        for field, value in (("q_homeowner", QUAL_YES),
                             ("q_utility", UTIL_AMEREN),
                             ("q_bill_100_plus", QUAL_YES)):
            if state.get(field, QUAL_UNKNOWN) == QUAL_UNKNOWN:
                state[field] = value

    # ── record → legacy ──
    if state.get("q_homeowner") in (QUAL_YES, QUAL_NO) and state.get("homeowner") is None:
        state["homeowner"] = state["q_homeowner"]

    # Ameren Missouri is in the service area by definition, so this direction
    # is sound where the reverse is not.
    if state.get("q_utility") == UTIL_AMEREN:
        state["location_confirmed"] = True


def qualification_verdict(state: dict) -> tuple[str, list[str]]:
    """
    (verdict, missing_fields).

      VERDICT_DISQUALIFIED — any one criterion has failed. Decisive: a renter
                             is a renter regardless of the other two.
      VERDICT_QUALIFIED    — all three confirmed.
      VERDICT_INCOMPLETE   — nothing failed, something is still unknown.
    """
    home = state.get("q_homeowner", QUAL_UNKNOWN) or QUAL_UNKNOWN
    util = state.get("q_utility", QUAL_UNKNOWN) or QUAL_UNKNOWN
    bill = state.get("q_bill_100_plus", QUAL_UNKNOWN) or QUAL_UNKNOWN

    if home == QUAL_NO or util == UTIL_OTHER or bill == QUAL_NO:
        return VERDICT_DISQUALIFIED, []

    missing = [
        name for name, value, good in (
            (Q_HOMEOWNER, home, QUAL_YES),
            (Q_UTILITY,   util, UTIL_AMEREN),
            (Q_BILL,      bill, QUAL_YES),
        ) if value != good
    ]
    return (VERDICT_INCOMPLETE if missing else VERDICT_QUALIFIED), missing


def qualification_summary(state: dict) -> str:
    """One greppable string for the logs."""
    verdict, missing = qualification_verdict(state)
    return (
        f"homeowner={state.get('q_homeowner', QUAL_UNKNOWN)} "
        f"utility={state.get('q_utility', QUAL_UNKNOWN)} "
        f"bill_100_plus={state.get('q_bill_100_plus', QUAL_UNKNOWN)} "
        f"verdict={verdict}"
        + (f" missing={','.join(missing)}" if missing else "")
    )


# How each missing fact should be asked for, when only one is left. The
# wording is guidance for Claude, not a template — the agent varies it.
_MISSING_FIELD_GUIDANCE = {
    Q_UTILITY:   ("utility",   "confirm they're with Ameren Missouri for electric — "
                               "a short confirming question, e.g. \"And you're with Ameren for electric, right?\""),
    Q_HOMEOWNER: ("ownership", "confirm they own the home. Use the phrasing \"Are you the homeowner?\" — "
                               "the \"do you own\" variants get filtered by carriers (error 30007)."),
    Q_BILL:      ("bill",      "ask roughly what the Ameren bill runs each month. Open-ended, no brackets, "
                               "a ballpark is fine."),
}


# ─────────────────────────────────────────────
#  CONVERSION INTENT ENGINE  [CONVERT-1]
#
#  WHY THIS EXISTS
#    A lead who ignored four nurture texts finally replied "Yes please" to
#    "Did you still want me to put together the numbers for your home, or
#    should I close this out?" — and was asked "Are you the homeowner?".
#
#    Three causes, all addressed here:
#      1. The nurture text was sent by a GHL workflow, so state["messages"]
#         had no record of it. The agent could not see what was being
#         answered.  → prior_outbound is resolved from local history first
#         and from GHL second (fetch_last_outbound_message).
#      2. "Yes please" failed the anchored _BRIEF_YES regex, so nothing was
#         learned from the reply at all.  → is_affirmative_reply() accepts
#         the natural range of affirmatives.
#      3. state["pending_question"] was written every turn and read nowhere.
#         → classify_outbound_intent() derives the same fact from the actual
#         previous outbound TEXT, which survives restarts and also covers
#         messages this service never sent.
#
#  THE RULE
#    A "yes" means whatever the previous outbound message asked. Yes to a
#    conversion invitation is permission to book. Yes to "are you on Ameren?"
#    is a qualification answer. The two must never be confused, in either
#    direction. Everything below exists to tell them apart.
#
#  WHAT THIS DELIBERATELY DOES NOT DO
#    It never fires for a booked, DNC, disqualified or rate-limited contact.
#    It is positioned AFTER every hard stop in michael_agent(), so those
#    guards are unreachable from here by construction.
# ─────────────────────────────────────────────

# Outbound message classes. The value is what appears in the logs as
# previous_outbound_type=… so it is greppable in Render.
OUT_BOOKING_PITCH  = "booking_pitch"
# [BUNDLE-1] A message that states the qualification criteria together and
# conditionally. A bare "yes" to one of these confirms ALL of them — which
# is true of no other outbound class, so it is kept distinct.
OUT_BUNDLED_QUAL   = "bundled_qualification"
OUT_CONVERSION_INV = "conversion_invitation"
OUT_OWNERSHIP_Q    = "ownership_question"
OUT_UTILITY_Q      = "utility_provider_question"
OUT_BILL_Q         = "bill_amount_question"
OUT_OTHER          = "other"
OUT_UNKNOWN        = "unknown"

# Inbound intent classes.
IN_HIGH_INTENT     = "high_intent_affirmative"
IN_BUNDLED_AFFIRM  = "bundled_qualification_affirmative"
IN_SCHEDULING      = "explicit_scheduling_request"
IN_QUAL_AFFIRM     = "qualification_affirmative"
IN_AMBIG_AFFIRM    = "ambiguous_affirmative"
IN_NONE            = "none"

# Actions.
ACT_SEND_BOOKING   = "send_booking_link"
ACT_CONTINUE_QUAL  = "continue_qualification"
ACT_ASK_MISSING    = "ask_missing_criterion"
ACT_NO_OVERRIDE    = "no_override"


# ── Previous-outbound classifiers ────────────────────────────────────────
# Checked in the order used by classify_outbound_intent(). A booking pitch is
# identified by the link itself; a conversion invitation by invitational
# grammar ("want me to…", "should I…", "would you like to…"), which no
# qualification question in this flow ever uses.

_OUT_BOOKING_PITCH_RE = re.compile(
    r"(leadconnectorhq\.com|get-solar-info"
    r"|here'?s\s+the\s+calendar"
    r"|pick\s+whatever\s+time"
    r"|grab\s+(?:a|whatever)\s+(?:quick\s+)?time"
    r"|calendar\s+link)",
    re.IGNORECASE,
)

_OUT_CONVERSION_INV_RE = re.compile(
    r"("
    r"(?:did|do)\s+you\s+(?:still\s+)?want\s+(?:me\s+)?to"
    r"|want\s+me\s+to\s+(?:put|pull|run|go\s+over|walk|show|send|get|look|take|set|check)"
    r"|want\s+to\s+(?:see|know|go\s+over|take\s+a\s+look|find\s+out|grab|set|move)"
    r"|would\s+you\s+like\s+(?:me\s+)?to"
    r"|(?:should|shall|can|could)\s+(?:i|we)\s+"
    r"(?:put|pull|run|go\s+over|send|show|take\s+a\s+look|set|get|close)"
    r"|interested\s+in\s+(?:seeing|taking\s+a\s+look|going\s+over)"
    r"|worth\s+(?:taking\s+)?a\s+look"
    r"|should\s+i\s+close\s+(?:this|it)\s+out"
    r"|what\s+your\s+home\s+(?:could|would|might)\s+qualify\s+for"
    r"|could\s+qualify\s+for"
    r")",
    re.IGNORECASE,
)

_OUT_OWNERSHIP_Q_RE = re.compile(
    r"(are\s+you\s+the\s+home\s?owner"
    r"|do\s+you\s+own\s+(?:the|your)\s+(?:home|house|place|property)"
    r"|own\s+the\s+(?:home|place)\s*\?"
    r"|are\s+you\s+the\s+owner)",
    re.IGNORECASE,
)

_OUT_UTILITY_Q_RE = re.compile(
    r"(ameren"
    r"|electric\s+(?:provider|company|utility)"
    r"|who'?s\s+your\s+(?:electric|power|utility)"
    r"|for\s+electric\s*\?)",
    re.IGNORECASE,
)

_OUT_BILL_Q_RE = re.compile(
    r"(bill\s+usually\s+run"
    r"|what\s+does\s+the\s+.{0,20}bill"
    r"|monthly\s+bill"
    r"|electric\s+bill\s+run"
    r"|average\s+bill"
    r"|bill\s+run\s+you)",
    re.IGNORECASE,
)


def classify_outbound_intent(text: str) -> str:
    """
    Classify the message WE sent last. This is the anchor for interpreting a
    bare "yes" — the single most ambiguous thing a lead can send.

    Order is deliberate. An invitation is recognised before any qualification
    pattern because invitational grammar is unambiguous, while a qualification
    question can legitimately mention the same nouns ("want me to go over the
    numbers on your Ameren bill?" is an invitation, not a bill question).

    Returns one of the OUT_* constants; OUT_UNKNOWN when there is no text to
    classify, which is the honest answer after a restart with no GHL history.
    """
    t = (text or "").strip()
    if not t:
        return OUT_UNKNOWN
    if _OUT_BOOKING_PITCH_RE.search(t):
        return OUT_BOOKING_PITCH
    # [BUNDLE-1] Checked before every question class. A bundled offer often
    # also ends in an invitation ("...want me to put it together?"), and the
    # bundled reading is strictly more informative: a yes carries the facts
    # as well as the interest.
    if is_bundled_qualification_message(t):
        return OUT_BUNDLED_QUAL
    if _OUT_CONVERSION_INV_RE.search(t):
        return OUT_CONVERSION_INV
    if _OUT_OWNERSHIP_Q_RE.search(t):
        return OUT_OWNERSHIP_Q
    if _OUT_UTILITY_Q_RE.search(t):
        return OUT_UTILITY_Q
    if _OUT_BILL_Q_RE.search(t):
        return OUT_BILL_Q
    return OUT_OTHER


# ── Inbound affirmative detection ────────────────────────────────────────
# Broader than _BRIEF_YES by design: _BRIEF_YES gates a qualification FACT
# (homeowner yes/no) and is deliberately strict; this gates a CONVERSATIONAL
# reading and has to cover how people actually reply to an invitation.
#
# A negation anywhere disqualifies the whole message. "Yes but I'm renting"
# and "not right now" must never reach the booking path.

_WHY_NOT_RE = re.compile(r"^\s*why\s+not[\s.!?]*$", re.IGNORECASE)

_NEGATIVE_REPLY_RE = re.compile(
    r"\b(no|nope|nah|not|dont|doesn'?t|never|stop|quit|cancel|unsubscribe"
    r"|remove|later|busy|already\s+(?:have|got|booked)|pass|hold\s+off"
    r"|wrong\s+number)\b|don't",
    re.IGNORECASE,
)

_AFFIRM_TOKEN = (
    r"(?:yes|yeah|yep|yup|ya|yea|yah|yes\s+sir|yes\s+ma'?am"
    r"|sure|sure\s+thing|absolutely|definitely|certainly|of\s+course"
    r"|ok|okay|k|kk|alright|all\s+right|correct|affirmative"
    r"|please|pls|please\s+do|interested|i'?m\s+interested"
    r"|sounds\s+good|sounds\s+great|sounds\s+like\s+a\s+plan"
    r"|works\s+for\s+me|that\s+works|that'?d\s+be\s+great"
    r"|i\s+do|i\s+am|i'?m\s+in|im\s+in|we\s+do|we\s+are"
    r"|for\s+sure|why\s+not|go\s+ahead|do\s+it|send\s+it|send\s+it\s+over"
    r"|send\s+them|send\s+me\s+it|let'?s\s+do\s+it|lets\s+do\s+it"
    r"|let'?s\s+go|lets\s+go|sign\s+me\s+up|perfect|great|cool|awesome"
    r"|100%)"
)

# The whole message must be affirmative tokens plus connective filler.
_AFFIRMATIVE_ONLY_RE = re.compile(
    rf"^\s*{_AFFIRM_TOKEN}"
    rf"(?:[\s,.!\-]+(?:and|then|i\s+guess|thanks|thank\s+you|{_AFFIRM_TOKEN}))*"
    rf"[\s,.!?]*$",
    re.IGNORECASE,
)


def is_affirmative_reply(text: str) -> bool:
    """
    True when the entire inbound message is an affirmative and nothing else.

    Whole-message matching, not substring: "yes" inside "yes but my roof is
    shot" is a qualified answer that deserves a real reply, not a booking
    link. Anything carrying a negation is rejected outright.
    """
    t = (text or "").strip()
    if not t or len(t) > 60:
        return False
    # "why not" is an affirmative built out of a negation word. Checked before
    # the negation filter because that filter would otherwise reject it.
    if _WHY_NOT_RE.match(t):
        return True
    if _NEGATIVE_REPLY_RE.search(t):
        return False
    return bool(_AFFIRMATIVE_ONLY_RE.match(t))


# ── Explicit call / scheduling requests ──────────────────────────────────
# These are self-evident regardless of what we said last, so they outrank the
# affirmative logic entirely. A lead who says "just call me" has already told
# us the next step; asking a qualification question now is pure friction.

_SCHEDULING_REQUEST_RE = re.compile(
    r"("
    r"call\s+me|can\s+you\s+call|could\s+you\s+call|give\s+me\s+a\s+call"
    r"|call\s+back|callback|ring\s+me"
    r"|i'?m\s+free|im\s+free|available\s+now|free\s+now"
    r"|when\s+can\s+(?:we|you|i)\s+(?:talk|meet|chat|come)"
    r"|let'?s\s+talk|lets\s+talk|let'?s\s+meet|lets\s+meet"
    r"|send\s+(?:me\s+)?(?:your\s+|the\s+)?calendar|calendar\s+link"
    r"|how\s+do\s+(?:i|we)\s+(?:schedule|book|set\s+(?:this|it)\s+up)"
    r"|(?:schedule|book)\s+(?:me|a\s+time|an?\s+appointment|something)"
    r"|set\s+up\s+a\s+time|grab\s+a\s+time|pick\s+a\s+time"
    r"|when\s+are\s+you\s+(?:available|free)"
    r"|what\s+times?\s+(?:do\s+you\s+have|are\s+(?:you\s+)?available|work)"
    r"|come\s+(?:on\s+)?(?:by|out)"
    r")",
    re.IGNORECASE,
)


def is_explicit_scheduling_request(text: str) -> bool:
    """True when the lead has asked to talk, be called, or be scheduled."""
    t = (text or "").strip()
    if not t:
        return False
    return bool(_SCHEDULING_REQUEST_RE.search(t))


class ConversionDecision(NamedTuple):
    """
    One routing verdict, described fully enough that its log line explains
    itself without anyone reading this file.

      intent                 — IN_* constant
      previous_outbound_type — OUT_* constant
      action                 — ACT_* constant
      reason                 — short human string, for the log only
      qualification_answer   — the fact this reply establishes, "" when none
      missing_field          — with ACT_ASK_MISSING, the one criterion still
                               open; the only thing the next message may ask
    """
    intent                 : str
    previous_outbound_type : str
    action                 : str
    reason                 : str
    qualification_answer   : str = ""
    missing_field          : str = ""


def last_outbound_message(state: dict) -> str:
    """The most recent thing WE said, from local conversation memory."""
    for msg in reversed(state.get("messages") or []):
        if msg.get("role") == "assistant":
            return str(msg.get("content") or "")
    return ""


def decide_conversion_action(state: dict,
                             inbound_text: str,
                             prior_outbound: str = "") -> ConversionDecision:
    """
    The conversion-first decision step. Pure: reads state, returns a verdict,
    mutates nothing. Every branch is unit-tested.

    Priority — compliance, the booked lockout and the daily limit are enforced
    by michael_agent() BEFORE this function is called, so nothing here can
    override them:

      0. a failed criterion                   → never book, whatever they said
      D. explicit call / scheduling request   → send booking link
      E. bundled criteria all confirmed       → send booking link
      E. bundled reply, one criterion open    → ask that one thing only
      E. affirmative to a conversion invite   → send booking link
      E. affirmative to a booking pitch       → send booking link
      E. affirmative while already qualified  → send booking link
      G. affirmative to a single question     → continue qualification
      H. everything else                      → no override, normal flow
    """
    prev      = (prior_outbound or "").strip() or last_outbound_message(state)
    prev_type = classify_outbound_intent(prev)
    verdict, missing = qualification_verdict(state)

    # ── 0: a criterion has failed ────────────────────────────────
    # Decisive and checked first. A renter who says "yes please" is still a
    # renter; enthusiasm is not a qualification. Claude handles the decline
    # on the normal path, where the DISQUALIFY responses live.
    if verdict == VERDICT_DISQUALIFIED:
        return ConversionDecision(
            IN_NONE, prev_type, ACT_NO_OVERRIDE,
            f"criterion failed — {qualification_summary(state)}",
        )

    # ── D: explicit request to call or schedule ──────────────────
    if is_explicit_scheduling_request(inbound_text):
        return ConversionDecision(
            IN_SCHEDULING, prev_type, ACT_SEND_BOOKING,
            "lead explicitly asked to talk or schedule",
        )

    _facts       = extract_qualification_facts(inbound_text, prev_type)
    _told_us     = bool({Q_HOMEOWNER, Q_UTILITY, Q_BILL} & set(_facts))
    _affirmative = is_affirmative_reply(inbound_text) or is_blanket_confirmation(inbound_text)

    # ── E: a reply to the bundled criteria ───────────────────────
    # Checked before the bare-affirmative gate, because a bundled offer is
    # often answered with facts rather than a yes — "I own it and my bill is
    # around $180" is a complete answer and must not fall through.
    if prev_type == OUT_BUNDLED_QUAL and (_affirmative or _told_us):
        if verdict == VERDICT_QUALIFIED:
            return ConversionDecision(
                IN_BUNDLED_AFFIRM, prev_type, ACT_SEND_BOOKING,
                "all three criteria confirmed by the bundled reply",
            )
        if len(missing) == 1:
            return ConversionDecision(
                IN_BUNDLED_AFFIRM, prev_type, ACT_ASK_MISSING,
                f"bundled reply left one criterion open: {missing[0]}",
                missing_field=missing[0],
            )

    if not _affirmative:
        return ConversionDecision(
            IN_NONE, prev_type, ACT_NO_OVERRIDE,
            "inbound is not a bare affirmative",
        )

    # ── E: affirmative — its meaning is whatever we asked last ───
    if prev_type == OUT_CONVERSION_INV:
        return ConversionDecision(
            IN_HIGH_INTENT, prev_type, ACT_SEND_BOOKING,
            "affirmative to a conversion invitation",
        )

    if prev_type == OUT_BOOKING_PITCH:
        return ConversionDecision(
            IN_HIGH_INTENT, prev_type, ACT_SEND_BOOKING,
            "affirmative after the booking link was already sent",
        )

    # ── E: the reply completed the record ───────────────────
    # Checked before the single-question branches below. Answering the last
    # open criterion IS the moment to book; falling through to "continue
    # qualification" would ask a fourth question that does not exist.
    if verdict == VERDICT_QUALIFIED:
        return ConversionDecision(
            IN_HIGH_INTENT, prev_type, ACT_SEND_BOOKING,
            "affirmative completed the record — all three criteria confirmed",
        )

    # ── G: affirmative answering a qualification question ────────
    if prev_type == OUT_OWNERSHIP_Q:
        return ConversionDecision(
            IN_QUAL_AFFIRM, prev_type, ACT_CONTINUE_QUAL,
            "homeowner confirmed — continue the flow, do not restart it",
            qualification_answer="ownership_yes",
        )

    if prev_type == OUT_UTILITY_Q:
        return ConversionDecision(
            IN_QUAL_AFFIRM, prev_type, ACT_CONTINUE_QUAL,
            "utility/service area confirmed — continue qualification",
            qualification_answer="utility_yes",
        )

    if prev_type == OUT_BILL_Q:
        return ConversionDecision(
            IN_QUAL_AFFIRM, prev_type, ACT_CONTINUE_QUAL,
            "affirmative to the bill question — continue qualification",
        )

    # ── E (fallback): already qualified, so an affirmative can only
    #    reasonably mean "yes, let's do this" ──────────────────────
    if state.get("qualified") or state.get("stage") == Stage.SEND_BOOKING:
        return ConversionDecision(
            IN_HIGH_INTENT, prev_type, ACT_SEND_BOOKING,
            "affirmative from an already-qualified lead",
        )

    # ── H: affirmative with no evidence of what it answers ───────
    # Deliberately NOT treated as high intent. Guessing here is exactly how a
    # lead who said yes to "are you on Ameren?" would get a booking link
    # instead of the next qualification question.
    return ConversionDecision(
        IN_AMBIG_AFFIRM, prev_type, ACT_NO_OVERRIDE,
        "affirmative but no conversion-invitation evidence — normal flow",
    )


# ── Conversion reply generation ──────────────────────────────────────────
#
# The lead-in is written by Claude so it can match the homeowner's register;
# the URL is appended by code and never comes from the model, exactly as
# build_booking_message() has always worked.

_CONVERSION_FALLBACKS: tuple = (
    "Yep, absolutely. Grab whatever time works best for you here and I'll have everything ready:",
    "Absolutely. Easiest way is to grab a quick time that works for you and I'll put everything together beforehand:",
    "For sure. Pick a time that works for you and I'll have it all ready to go:",
)

# [BUNDLE-2] How each unconfirmed criterion reads as a CONDITION rather than a
# question. Conditions can travel in the same message as the link; questions
# cannot, because a question has to be answered before anything else happens.
_CRITERION_CLAUSE = {
    Q_HOMEOWNER: "you own the home",
    Q_UTILITY:   "you're on Ameren",
    Q_BILL:      "the electric bill usually runs over $100 a month",
}

# Fixed order, so a two-criterion clause always reads the same way round.
_CRITERION_ORDER = (Q_HOMEOWNER, Q_UTILITY, Q_BILL)


def criteria_clause(missing: list[str]) -> str:
    """'you own the home, you're on Ameren, and the bill usually runs over $100 a month'"""
    parts = [_CRITERION_CLAUSE[k] for k in _CRITERION_ORDER if k in (missing or [])]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return f"{parts[0]}, {parts[1]}, and {parts[2]}"


# Used when the record is incomplete: the conditions and the link ride in the
# SAME message, so the homeowner self-qualifies without losing a turn.
_CONVERSION_FALLBACKS_WITH_CRITERIA: tuple = (
    "Absolutely. If {clause}, I'd be glad to show you what a $0-down option could "
    "look like and answer any questions. Grab whatever time works for you here:",
    "For sure. As long as {clause}, I can put the numbers together for you — "
    "pick a time that works and I'll have it ready:",
    "Yep. Assuming {clause}, I can show you what this would actually look like. "
    "Grab a time that suits you here:",
)


def _conversion_fallback_line(contact_id: str, inbound_text: str,
                              missing: list | None = None) -> str:
    """
    Deterministic per contact+message, so one turn never rewrites itself.

    When criteria are still unknown the fallback states them as conditions and
    still carries the link — never a bare booking link with no context, and
    never a qualification question that stalls the conversation.
    """
    seed = hashlib.sha1(f"{contact_id}|{inbound_text}".encode("utf-8")).digest()[0]
    clause = criteria_clause(list(missing or []))
    if clause:
        template = _CONVERSION_FALLBACKS_WITH_CRITERIA[
            seed % len(_CONVERSION_FALLBACKS_WITH_CRITERIA)
        ]
        return template.format(clause=clause)
    return _CONVERSION_FALLBACKS[seed % len(_CONVERSION_FALLBACKS)]


def build_conversion_prompt(state: dict, decision: ConversionDecision,
                            prior_outbound: str = "") -> str:
    """
    A deliberately small prompt. Its only job is one short, human lead-in
    line — no qualification, no URL, no pitch. Everything the qualification
    prompt can do is unreachable from here.
    """
    prev  = (prior_outbound or "").strip() or last_outbound_message(state)
    first = _resolve_first_name("", state.get("contact_name", ""))
    _verdict, _missing = qualification_verdict(state)

    if decision.intent == IN_SCHEDULING:
        situation = (
            "The homeowner just asked to talk, be called, or be scheduled. "
            "Acknowledge briefly and point them at the calendar."
        )
    else:
        situation = (
            "The homeowner just said yes to your invitation to move forward. "
            "They have given you permission — take it, immediately and without fuss."
        )

    # [BUNDLE-2] High intent with an incomplete record. The conditions and the
    # link go out TOGETHER: asking the three questions first would burn three
    # turns and the momentum that produced this reply, while a bare link would
    # book someone who may not qualify. Stating them as conditions lets the
    # homeowner self-qualify without being interrogated.
    _confirmed_lines = []
    if state.get("q_homeowner") == QUAL_YES:
        _confirmed_lines.append("they own the home")
    if state.get("q_utility") == UTIL_AMEREN:
        _confirmed_lines.append("they're on Ameren")
    if state.get("q_bill_100_plus") == QUAL_YES:
        _confirmed_lines.append("their bill is over $100/month")

    if _missing:
        _clause = criteria_clause(_missing)
        criteria_block = f"""
QUALIFICATION — FOLD IT INTO THIS SAME MESSAGE
Still unknown: {_clause}.
{("Already confirmed (do NOT restate or re-ask): " + ", ".join(_confirmed_lines) + ".") if _confirmed_lines else ""}

State the unknown ones as CONDITIONS in the same sentence, then point at the
calendar. One message, conditions and invitation together.
• Conditions, never questions. "If you own the home and you're on Ameren..."
  — not "Are you the homeowner?".
• Do NOT split them across messages and do NOT hold the link back. The link
  goes out in THIS message regardless.
• Never restate a criterion already confirmed above.
• You may offer "$0-down" only as a possibility for those who qualify. Never
  a promise, never a number, never "free".

Good: "Absolutely. If {_clause}, I'd be glad to show you what a $0-down option could look like and answer any questions. You can grab a time that works for you here:"
Good: "For sure — as long as {_clause}, I can put the numbers together. Pick whatever time suits you:"
Bad:  "Are you the homeowner?"
Bad:  "Great! Here's my calendar:"   (no conditions at all)
"""
    else:
        criteria_block = """
QUALIFICATION
All three criteria are already confirmed. Do NOT restate them, do NOT attach
conditions, and do NOT ask anything. Just move them to the calendar.
"""

    return f"""You are Michael, a solar advisor for STL Energy Advisors.

{situation}

The last message you sent was:
  {prev or '(not available)'}

The homeowner's reply is the next message.
{criteria_block}
WRITE THE MESSAGE BODY ONLY. The booking link is appended automatically right
after your text — do NOT write a URL, and do NOT mention a link, a calendar
page, or an address.

RULES
• Short. One sentence when everything is confirmed; two at most when you are
  stating conditions. Never more than 45 words.
• Match their register. A two-word reply earns a short reply back.
• End with a colon if it reads naturally, since a link follows.
• Do NOT use their name ({first or 'n/a'}) — you are mid-conversation.
• Do NOT sell, justify, or explain the appointment. They already agreed.
• Do NOT thank them, compliment them, or use exclamation marks.
• No emojis. No "Great!", "Perfect!", "Awesome".
• Never say "phone call" or "virtual" — the visit is in-home.
• No control tags, no brackets.
""".strip()


# Anything URL-shaped must never survive from the model into the lead-in;
# build_conversion_reply() appends the single authoritative link itself.
_ANY_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)

# A criterion put as a question rather than a condition. On the conversion
# path this is always wrong — it stalls a lead who just said yes.
_ASKS_A_CRITERION_RE = re.compile(
    r"(are\s+you\s+the\s+home\s?owner|do\s+you\s+own\b|are\s+you\s+(?:on|with)\s+ameren"
    r"|what\s+does\s+.{0,20}bill|how\s+much\s+is\s+.{0,20}bill)",
    re.IGNORECASE,
)


def build_conversion_reply(contact_id: str, state: dict,
                           decision: ConversionDecision,
                           prior_outbound: str = "",
                           inbound_text: str = "") -> str:
    """
    A short, natural lead-in followed by the booking link.

    Claude writes only the lead-in. Any model failure, empty output, or output
    that tries to smuggle in a URL or a control tag falls back to a fixed
    line — this path must never fail to produce a sendable message.
    """
    _verdict, _missing = qualification_verdict(state)
    lead_in = ""
    try:
        _msgs = [
            {"role": m["role"], "content": m["content"]}
            for m in (state.get("messages") or [])
            if m.get("role") in ("user", "assistant") and m.get("content")
        ][-6:]
        if not _msgs or _msgs[0].get("role") != "user":
            _msgs = [{"role": "user", "content": "[Lead replied to a follow-up]"}] + _msgs
        if _msgs[-1].get("role") != "user" or _msgs[-1].get("content") != inbound_text:
            _msgs = _msgs + [{"role": "user", "content": inbound_text or "yes"}]

        resp = claude.messages.create(
            model      = MODEL,
            max_tokens = 200,
            system     = build_conversion_prompt(state, decision, prior_outbound),
            messages   = _msgs,
        )
        lead_in, _tag = extract_control_tag(resp.content[0].text.strip())
        lead_in = _ANY_URL_RE.sub("", lead_in).strip()
        if len(lead_in) > 400:
            lead_in = ""
        # A message that asks instead of stating conditions defeats the point:
        # the homeowner would have to answer before the link meant anything.
        elif _missing and _ASKS_A_CRITERION_RE.search(lead_in):
            ev("CONVERSION_LEADIN_REJECTED", contact_id, why="asked_instead_of_stating")
            lead_in = ""
    except Exception as err:
        log.warning(f"[{contact_id}] conversion lead-in generation failed: {err}")
        ev("CONVERSION_LEADIN_FALLBACK", contact_id, why=str(err)[:80])

    if not lead_in:
        lead_in = _conversion_fallback_line(contact_id, inbound_text, _missing)

    return sanitize_outbound_message(f"{lead_in}\n{BOOKING_LINK}", contact_id)


# ─────────────────────────────────────────────
#  DYNAMIC SYSTEM PROMPT  [ADAPT-1]
#
#  Builds a context-aware system prompt for every
#  Claude call. The "WHAT YOU ALREADY KNOW" block
#  tells Claude what NOT to ask so it can focus on
#  the next qualification step or drive booking.
# ─────────────────────────────────────────────

def build_system_prompt(state: dict, ghl_pipeline_stage: str = "", ghl_tags: list | None = None,
                       prior_outbound: str = "", ask_only: str = "") -> str:
    """
    Build a state-aware system prompt for Michael.

    The key sections are dynamically generated:
      • WHAT YOU ALREADY KNOW — prevents re-asking confirmed questions
      • CURRENT GOAL          — explicit instruction for this turn
    Everything else (rules, objections, tags) is static.
    """
    stage    = state["stage"]
    homeown  = state.get("homeowner")       # None | "yes" | "no"
    loc_conf = state.get("location_confirmed", False)
    bill     = state.get("monthly_bill", "")

    # ── "WHAT YOU ALREADY KNOW" block ──────────────────────────────
    # Use stage as primary indicator (most reliable) + explicit fields as bonus.
    confirmed: list[str] = []

    # Homeowner
    # Homeowner: implied confirmed only at ASK_BILL and beyond.
    # ASK_LOCATION stage no longer implies homeowner confirmed (area is now first question).
    # [BUNDLE-1] Driven by the qualification record. Each criterion is its own
    # tri-state, so "not asked yet" is never rendered as a confirmation — which
    # is what a bare `location_confirmed` bool used to do.
    _q_home = state.get("q_homeowner", QUAL_UNKNOWN) or QUAL_UNKNOWN
    _q_util = state.get("q_utility", QUAL_UNKNOWN) or QUAL_UNKNOWN
    _q_bill = state.get("q_bill_100_plus", QUAL_UNKNOWN) or QUAL_UNKNOWN

    if _q_home == QUAL_YES or stage in (Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED) or homeown == "yes":
        confirmed.append("✓ HOMEOWNER: confirmed — do NOT ask about home ownership again")
    elif _q_home == QUAL_NO or homeown == "no":
        confirmed.append("✗ NOT A HOMEOWNER: they are renting — disqualify if not already done")

    # Utility — the criterion. Ameren Missouri only.
    if _q_util == UTIL_AMEREN:
        confirmed.append("✓ UTILITY: Ameren Missouri confirmed — do NOT ask about their electric provider again")
    elif _q_util == UTIL_OTHER:
        confirmed.append("✗ UTILITY: not Ameren Missouri — disqualify if not already done")
    elif stage in (Stage.SEND_BOOKING, Stage.BOOKED):
        confirmed.append("✓ UTILITY: confirmed — do NOT ask about their electric provider again")

    # Service area is a separate fact: an address inside the metro says nothing
    # about who supplies the electricity, so it never implies the criterion.
    if stage in (Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED) or loc_conf:
        confirmed.append("✓ SERVICE AREA: address is in the St. Louis metro — do NOT ask where they live")

    # Electric bill
    if _q_bill == QUAL_YES or stage in (Stage.SEND_BOOKING, Stage.BOOKED):
        _amount = f" ({bill})" if bill else ""
        confirmed.append(f"✓ ELECTRIC BILL: $100+/month confirmed{_amount} — do NOT ask about the bill again")
    elif _q_bill == QUAL_NO:
        confirmed.append("✗ ELECTRIC BILL: under $100/month — disqualify if not already done")
    elif bill:
        confirmed.append(f"✓ ELECTRIC BILL: mentioned as {bill} — do NOT ask about bill again")

    if not confirmed:
        # v3.5: Widget leads have location_confirmed=True set at first-outreach time.
        # If we reach here with nothing confirmed, the lead is genuinely at INITIAL
        # (no state yet), which should not happen for widget contacts — but handle
        # gracefully by starting at ownership (not area, since address was collected).
        confirmed_block = (
            "  Nothing confirmed yet — state all three conditions together in one\n"
            "  line rather than asking about them one at a time."
        )
    else:
        confirmed_block = "\n".join(f"  {c}" for c in confirmed)

    # ── CURRENT GOAL ──────────────────────────────────────────────
    # [BUNDLE-1] The three criteria decide what to ask, not the stage. The
    # stage still wins for the terminal states below, which are about
    # routing rather than qualification.
    _qual_verdict, _qual_missing = qualification_verdict(state)
    _ask_one = ask_only if ask_only in _MISSING_FIELD_GUIDANCE else (
        _qual_missing[0] if len(_qual_missing) == 1 else ""
    )
    if stage == Stage.SEND_BOOKING:
        current_goal = (
            "BOOKING — lead is FULLY QUALIFIED. "
            "Write one natural transition sentence, then append [SEND_BOOKING]. "
            "Do NOT include a URL — the booking link is sent automatically."
        )
    elif stage == Stage.BOOKED:
        current_goal = "BOOKED — appointment is set. Handle any follow-up (bill photo, questions). Do not restart qualification."
    elif stage == Stage.DISQUALIFIED:
        current_goal = "DISQUALIFIED — contact was already disqualified. Stay silent or acknowledge cleanly if they respond."
    elif stage == Stage.DNC:
        current_goal = "DNC — contact opted out. Do not respond."
    elif _qual_verdict == VERDICT_DISQUALIFIED:
        # [BUNDLE-1] Name the criterion that failed so the decline is specific.
        if state.get("q_homeowner") == QUAL_NO:
            current_goal = "DISQUALIFY — they rent. Decline warmly in one line and emit [DISQUALIFY:NOT_OWNER]."
        elif state.get("q_utility") == UTIL_OTHER:
            current_goal = "DISQUALIFY — not Ameren Missouri. Decline warmly in one line and emit [DISQUALIFY:OUT_OF_AREA]."
        else:
            current_goal = "DISQUALIFY — bill is under $100/month. Say so honestly in one line and emit [DISQUALIFY:LOW_BILL]."
    elif _qual_verdict == VERDICT_QUALIFIED:
        current_goal = (
            "BOOKING — all three criteria are confirmed. "
            "Write one natural transition sentence, then append [SEND_BOOKING]. "
            "Do NOT include a URL — the booking link is sent automatically. "
            "Do NOT ask anything else first."
        )
    elif _ask_one in _MISSING_FIELD_GUIDANCE:
        _label, _how = _MISSING_FIELD_GUIDANCE[_ask_one]
        current_goal = (
            f"ASK {_label.upper()} — this is the ONLY thing still unknown. {_how} "
            f"Ask it and nothing else. Everything under WHAT YOU ALREADY KNOW is settled: "
            f"re-asking any of it is a wrong reply. As soon as they confirm, go to booking."
        )
    else:
        current_goal = (
            "BUNDLED QUALIFICATION — two or more criteria are still unknown "
            f"({', '.join(_qual_missing)}). Do NOT ask them one at a time. State the "
            "conditions together in ONE casual line and offer to put the numbers "
            "together if they apply. Vary the wording — never reuse a previous phrasing. "
            "Skip any criterion already listed as confirmed above."
        )

    # ── Tag-based qualification memory ────────────────────────────
    # GHL tags supplement the stage-based confirmed block with
    # details collected in previous conversations.
    _tag_facts = parse_qualification_tags(ghl_tags or [])
    _tag_lines: list[str] = []
    if _tag_facts.get("bill_range"):
        _tag_lines.append(f"✓ BILL RANGE (from tag): ~${_tag_facts['bill_range']}/month — do NOT ask about bill again")
    if _tag_facts.get("roof_type"):
        _tag_lines.append(f"✓ ROOF TYPE (from tag): {_tag_facts['roof_type']} — do not ask about roof")
    if _tag_facts.get("timeline"):
        _tag_lines.append(f"✓ TIMELINE (from tag): {_tag_facts['timeline']}")
    if _tag_facts.get("homeowner") and "HOMEOWNER" not in confirmed_block:
        _tag_lines.append(f"✓ HOMEOWNER (from tag): confirmed — do NOT ask about home ownership again")
    if _tag_facts.get("location_confirmed") and "SERVICE AREA" not in confirmed_block:
        _tag_lines.append(f"✓ SERVICE AREA (from tag): confirmed — do NOT ask about utility/area again")
    if _tag_lines:
        confirmed_block = confirmed_block + "\n" + "\n".join(f"  {l}" for l in _tag_lines)

    # ── [CONVERT-1] Previous outbound message ──────────────────────
    # Reasoning about a one-word reply is impossible without knowing what
    # it answers. Local memory is wiped by every restart and never contains
    # GHL-sent nurture texts at all, so the caller may supply it instead.
    _prev_out = (prior_outbound or "").strip() or last_outbound_message(state) \
                or str(state.get("last_outbound_text") or "")
    _prev_out_line = ""
    if _prev_out:
        _prev_out_line = (
            f"\n━━ THE LAST MESSAGE YOU SENT ━━\n  {_prev_out.strip()[:400]}\n"
            f"  (classified: {classify_outbound_intent(_prev_out)})\n"
            f"  A short reply from the homeowner is answering THIS. Read it that\n"
            f"  way before deciding what to say next.\n"
        )

    # ── GHL pipeline stage block ───────────────────────────────────
    _ghl_stage_line = ""
    if ghl_pipeline_stage:
        _ghl_stage_line = f"\nGHL PIPELINE STAGE: {ghl_pipeline_stage}\n"
        # Override CURRENT GOAL if GHL stage tells us something definitive
        if ghl_pipeline_stage == "Qualified" and "BOOKING" not in current_goal:
            current_goal = (
                "BOOKING — contact is marked Qualified in the pipeline. "
                "Move directly to scheduling. Write ONE natural transition sentence then append [SEND_BOOKING]."
            )
        elif ghl_pipeline_stage == "Contacted" and current_goal.startswith("ASK UTILITY"):
            # 'Contacted' means we already reached them — skip utility re-ask if loc_conf is True
            if loc_conf:
                current_goal = "ASK OWNERSHIP — location already confirmed. Ask 'Are you the homeowner?' (use that phrasing)."

    return f"""You are Michael, a solar advisor for STL Energy Advisors, serving St. Louis-area Missouri homeowners on Ameren Missouri.
Your one job: qualify leads and book them for a free in-home solar consultation.
Text like a calm, helpful, local person — short messages, natural tone, no hype.

{_ghl_stage_line}{_prev_out_line}━━ WHAT YOU ALREADY KNOW ABOUT THIS LEAD ━━
{confirmed_block}

CURRENT GOAL: {current_goal}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QUALIFICATION CRITERIA — three things, and nothing else, decide whether this fits:
1. They own the home
2. They are on Ameren Missouri for electric (Missouri side — Ameren Illinois does not count)
3. Their electric bill usually runs about $100/month or more

DO NOT INTERROGATE. Never send these as three separate questions across three
texts. That reads like a form, and people stop replying to forms.

WHEN TWO OR MORE ARE UNKNOWN — state them together in ONE casual line and let
them confirm the lot at once. Vary the wording every single time; what follows
is a shape, not a script:
  "If you own the home, you're on Ameren, and the bill usually runs over $100
   a month, I'm happy to put together what a $0-down option could look like
   and answer any questions."
  "As long as you own the place, have Ameren, and the electric bill is north of
   $100 or so, I can pull the numbers together for you."
  "If that's your home, you're with Ameren, and you're usually over $100 a month
   on electric, I can show you what this would actually look like."
Casual, professional, concise, low pressure. It is an offer with conditions
attached — not a screening. Skip any criterion already confirmed above.

WHEN EXACTLY ONE IS UNKNOWN — ask only that one, in one short line. Never
re-ask the other two.
  "Perfect. And you're with Ameren for electric, right?"

READING THEIR ANSWER
• A yes to the bundled line confirms ALL THREE — all three is what you put to them.
• Anything they state explicitly overrides that. "Yeah but I'm with Cuivre River"
  is a yes to the offer and a no on utility.
• Partial answers are normal and good. "$180 and I own it" leaves only the
  utility open — ask that one thing, then go straight to booking.
• A yes to a SINGLE question confirms only that one thing. "Yes" to "are you on
  Ameren?" says nothing about ownership or the bill.
• Never make them reconfirm anything under WHAT YOU ALREADY KNOW.

PHRASING THAT MATTERS
When you do ask about ownership on its own, use "Are you the homeowner?".
Do NOT rephrase as "do you own the home" / "do you own your home" — those
variants have been blocked by carrier filters (error 30007) on multiple
contacts, while the questions either side of them delivered normally.
For the bill, accept any natural phrasing — "around 170", "like 200 in summer",
"150ish", "couple hundred". Never present bracket choices, and never re-ask for
a more exact figure once you have a ballpark.

WHEN A CRITERION FAILS
  Renter        → "Got it — solar really only works for homeowners. If that ever changes, reach out." [DISQUALIFY:NOT_OWNER]
  Not Ameren MO → "Got it — we focus on Ameren Missouri homeowners on the Missouri side of the St. Louis area, so we may not be the right fit yet. I'll keep your info on file in case anything changes." [DISQUALIFY:OUT_OF_AREA]
  Under $100    → "At that rate, the math is harder to make work. I'll keep your info on file in case it changes." [DISQUALIFY:LOW_BILL]

$0-DOWN — only ever as a possibility, only ever inside the bundled line:
"$0-down options for homeowners who qualify". Never a promise, never a number,
never "free".

NOTE: Service area is confirmed at form-submission time when an address is
provided. That is NOT the same as knowing their utility — an address in the
metro says nothing about who bills them. Confirm Ameren separately.

BOOKING — move here as soon as they qualify. Do NOT stall or ask extra questions.
When they qualify, write ONE natural transition sentence then append [SEND_BOOKING].
The actual booking link and full message are sent automatically — do NOT include a URL in your reply.
Keep it plain and short. No summarising why they qualify, no benefits, no cost or
savings commentary — just move to the appointment the way a person would.
Example output: "Got it — let's get you set up for an in-home review. [SEND_BOOKING]"
Example output: "Perfect. Let me get you on the calendar. [SEND_BOOKING]"
Example output: "Sounds good — let's find a time to come take a look. [SEND_BOOKING]"
IMPORTANT: If the lead asks ANY question (message contains "?") while qualifying — even right after giving
their bill amount — ANSWER the question naturally in 1-2 sentences FIRST, then transition to booking.
Never ignore a question by jumping straight to the booking invite.

━━ ONCE THEY REPLY — QUALIFYING MODE ━━
The moment the homeowner has responded even once, your job is qualification and
booking. It is NOT pitching. They already raised their hand; you do not need to
convince them of anything over SMS.

Default shape of every qualifying message:
    [brief acknowledgment, 4 words or fewer] + [one relevant question]
  Good: "Got it. Do you own the home?"
  Good: "Okay, thanks. And do you own the place?"
  Good: "Makes sense. What does the Ameren bill usually run?"
  Bad:  "That's a solid amount going to Ameren, especially with rates continuing
         to climb. Do you own the home?"

NEVER, unless they explicitly asked:
• Comment on how much they pay. No "that's a lot", "that's a solid amount",
  "that's a big bill", "that's a lot going to Ameren", "wow", "yikes".
  A bill figure is data you collected — acknowledge it neutrally and move on.
• Reference rates rising, climbing, increasing, going up, rate cases or hikes.
• Mention savings, payback, financial outcomes, or what solar could do for them.
• Imply urgency, scarcity, deadlines, limited availability, or acting sooner.
• Add a reason, benefit, or justification the homeowner did not ask for.

ALWAYS still allowed — answering is not pitching:
• If they ASK about rates, costs, savings, solar economics, or why solar might
  make sense, answer accurately and conversationally using SEASONAL CONTEXT,
  the solar framing rules, and the OBJECTIONS responses below. Answer the
  question properly in 1-2 sentences, then continue toward your goal.
• Never dodge a real question or reply with a bare question in return.
The distinction is simple: unsolicited promotional framing is prohibited;
genuine answers to genuine questions are expected.

━━ MATCH THEIR REGISTER ━━
Mirror the homeowner's length and formality — never their slang.
• Short and casual from them → short and casual from you. "Yep" earns a
  one-liner, not a paragraph.
• Longer, more detailed or more formal from them → you can be a little
  fuller and more precise in return.
• Never run hotter than they are. If they are flat or brief, be plain.
  Do not add energy they did not bring.

THEIR NAME
Use it in the opening message. After that, leave it out unless it genuinely
fits — addressing them directly, or picking the thread back up after a long
gap. Do not open replies with their name as a habit.
  Bad:  "Great, Gary! Thanks so much for that."
  Good: "Got it."

NEVER
• Compliment, flatter, or thank them for something ordinary.
• "Awesome", "Perfect!", "Fantastic", "Happy to help", "I'd love to".
• Exclamation marks as a default setting.
• Echo their slang or abbreviations back at them — it reads as mimicry.
• Emojis, unless they used one first, and then at most one.
Calm, competent, human. An advisor texting someone, not a rep working a lead.

━━ BEHAVIOR RULES ━━
• 1-2 sentences per message max (3 only if truly necessary)
• NEVER re-ask about anything listed as confirmed in "WHAT YOU ALREADY KNOW" above
• If the lead volunteers information before you ask, acknowledge it and move on — never ask again
• If they ask a question, answer it briefly and naturally, then continue toward your goal
• If they give a vague reply ("yes", "sure"), infer from conversation context what they're responding to — NEVER joke or suggest the reply was meant for a different question; rephrase your question instead if unclear
• "I already booked" / "I just scheduled" → [BOOKED]
• NEVER repeat "STL Energy Advisors" after the first message
• NEVER use: "Great!", "Absolutely!", "Of course!", "Certainly!" — too robotic
• These are IN-HOME consultations, not phone calls — never say "phone call" or "virtual"
• Once someone is qualified, move directly to booking — do NOT keep asking more questions
• Sound helpful, calm, and local — like a real person, not a script or a form

━━ SEASONAL CONTEXT — REFERENCE ONLY, NEVER VOLUNTEERED ━━
It is currently summer 2026. Ameren Missouri has raised rates and is investing billions
in infrastructure. Summer electric bills in St. Louis are at their highest of the year.

This is accurate background for ANSWERING a question. It is NOT an opener, a
transition, or a reason to reach out.
• If the homeowner ASKS about rates, electricity costs, or why solar might make
  sense — answer honestly and plainly using this context.
• If they have NOT asked — do not mention rates, increases, bill size, seasons,
  or costs at all. Not as a lead-in, not as a comment, not "by the way".
• ONE EXCEPTION: the bundled qualification line names the $100/month condition
  and may offer a $0-down option for those who qualify. That is a condition of
  the offer, not commentary on their bill, and it is the only place a figure
  may appear unprompted. Never pair it with a claim, a saving, or a comparison.
Unsolicited cost commentary gets messages blocked by mobile carriers, which means
the homeowner never receives them. A blocked message helps nobody.

━━ OBJECTIONS ━━
• "Not interested" → "No worries — if that ever changes, we're here." [DISQUALIFY:NOT_INTERESTED]
• "How much does solar cost?" → "I wish I could give you a simple number — it really depends on your home, usage, and how your system gets set up. You're not buying solar so much as replacing your Ameren bill with something fixed. Do you own your home?"
• "Is this a scam?" → "Legit question — STL Energy Advisors is a licensed local solar firm serving the Missouri side of the St. Louis area. Free in-home review, zero obligation."
• "Can someone call me?" → "Totally — easiest way is to grab a time here and I'll come by your home.
   https://stlenergyadvisors.com/get-solar-info?source=sms"
• "Is the tax credit still available?" / "What about the 30% credit?" → "The 30% federal credit expired at the end of 2025 — a lot of homeowners haven't heard that yet. The in-home review looks at your actual numbers so you can see exactly where you'd land."
• "How much will I save?" → "Hard to say without looking at your actual Ameren usage — that's exactly what the in-home review figures out, and if it doesn't pencil out I'll tell you straight."
• Persistent hesitation → "There's no commitment — it's just a real look at whether solar actually makes sense for your home and your Ameren bill."
• "I'm planning to move" / "might sell" → "That's worth factoring in — solar does tend to add to appraised value, but the timeline matters. Roughly how far out are you thinking?"
• "My roof is old" / "roof might need work" → "Good to know upfront — roof condition definitely factors into the picture. Do you have a rough idea how old it is?"
• "I've been pitched before" / "got burned by solar companies" → "I hear that a lot. Some companies are pretty aggressive out there. The in-home review is free and if the numbers don't work for your home, I'll tell you straight — no pressure either way."
• "I saw an ad that said solar is free" → "Those ads are misleading — solar isn't free. It's a financed system that replaces your Ameren bill with a fixed payment you own. Some homeowners may qualify for $0 down depending on approval, but it's not free. Worth seeing if the actual numbers make sense for your home."
• "I already have solar" → "Nice — when did you get it set up?" [Gather info naturally. Do not continue qualification. Note in context.]
• Clear disinterest → "No worries, take care." [DISQUALIFY:NOT_INTERESTED]

━━ NO ASSUMPTIONS / NO FABRICATION — HARD RULES ━━
These override everything else. A reply that breaks any of these is a wrong reply.

NEVER fabricate numbers:
• NEVER give specific dollar savings ranges — no "$20–$30/month", "$60–$90", "$X/year", etc.
• NEVER state or imply a monthly payment amount without a real quote.
• NEVER assume system size ("a typical 8kW system…") — you don't know their home.
• NEVER assume incentive eligibility or claim any specific credit is available.
  If asked about the tax credit: "The 30% federal credit expired at the end of 2025 — a lot of
  homeowners haven't heard that yet. The in-home review looks at your actual numbers."
• NEVER assume financing structure or terms for their situation.

NEVER imply guaranteed outcomes:
• No "you'll save money", "most people lower their bill", "you'll pay less" as a general claim.
• No percentage savings claims (not "save 30%", not "cut your bill in half").
• No language that makes savings sound certain or typical.
• NEVER say "free electricity", "the bill goes away", or anything that implies the Ameren bill disappears.

ALWAYS frame solar correctly:
• Solar replaces a variable utility cost with a fixed, predictable payment you own.
• The frame is OWNERSHIP and PREDICTABILITY — owning the system on your roof versus renting power from Ameren.
• The deliverable is a real in-home review of the bill, the roof, and the usage. If it doesn't pencil out, we say so.
• Outcomes depend on the home, usage, system size, and financing — always.
• Approved phrasings (use your own words, same meaning):
  - "It depends on your current Ameren bill, usage, and how your home is set up."
  - "For most homeowners, the goal isn't just lowering the bill — it's having a predictable
     cost they own instead of a utility bill that moves around."
  - "Some people end up paying a similar amount but with a fixed rate they own — it really
     comes down to your setup."

ALWAYS move toward the bill:
• If someone asks about savings, cost, or outcomes — give a grounded one-liner, then ask
  about their Ameren bill. The bill is the only real data point you have. Get it.
• Example: "Hard to say without knowing your actual Ameren usage — what does your bill usually run?"
• Never estimate their outcome without their bill number. Never.

TONE:
• Calm, direct, helpful — advisor, not salesman.
• No hype. No "amazing", "incredible", "huge savings", "you'd be crazy not to".
• If something doesn't work for their situation, say so plainly.

NEVER say "phone call", "quick call", or "text me". The next step is always the in-home visit.

━━ COMPLIANCE ━━
STOP / QUIT / CANCEL / UNSUBSCRIBE / END → "You've been removed. You won't hear from us again." [DNC]

━━ CONTROL TAGS — include at end of message when applicable ━━
[DISQUALIFY:NOT_OWNER]   [DISQUALIFY:OUT_OF_AREA]  [DISQUALIFY:LOW_BILL]
[DISQUALIFY:NOT_INTERESTED]  [QUALIFIED]  [SEND_BOOKING]  [BOOKED]  [DNC]""".strip()


def _goal_from_prompt(system: str) -> str:
    """Extract the CURRENT GOAL line from a built system prompt (for logging)."""
    for line in system.split("\n"):
        if line.startswith("CURRENT GOAL:"):
            return line.replace("CURRENT GOAL:", "").strip()[:100]
    return "(unknown)"


# ─────────────────────────────────────────────
#  CONTROL TAG PARSER
# ─────────────────────────────────────────────

CONTROL_TAGS = {
    "[DISQUALIFY:NOT_OWNER]"     : Stage.DISQUALIFIED,
    "[DISQUALIFY:OUT_OF_AREA]"   : Stage.DISQUALIFIED,
    "[DISQUALIFY:LOW_BILL]"      : Stage.DISQUALIFIED,
    "[DISQUALIFY:NOT_INTERESTED]": Stage.DISQUALIFIED,
    "[QUALIFIED]"                : Stage.SEND_BOOKING,
    "[SEND_BOOKING]"             : Stage.SEND_BOOKING,
    "[BOOKED]"                   : Stage.BOOKED,
    "[DNC]"                      : Stage.DNC,
}

def extract_control_tag(text: str) -> tuple[str, Optional[Stage]]:
    """
    Strip ALL control tags from the reply text and return the winning stage.

    [FIX-2] The original version returned on the first tag found, stripping only
    that one tag.  If Claude emitted both [QUALIFIED] and [SEND_BOOKING] in the
    same message, [QUALIFIED] was found first, stripped, then we returned —
    leaving [SEND_BOOKING] visible in the outgoing SMS.

    Fixed: iterate all tags, strip every occurrence, keep the first stage found.
    """
    clean       = text
    found_stage = None
    for tag, new_stage in CONTROL_TAGS.items():
        if tag in clean:
            clean = clean.replace(tag, "")   # strip this tag (may appear >1 time)
            if found_stage is None:           # first match wins for stage transition
                found_stage = new_stage
    return clean.strip(), found_stage


# ─────────────────────────────────────────────
#  KEYWORD DETECTORS
# ─────────────────────────────────────────────

# ── Two-tier opt-out detection ────────────────────────────────────
#
# BUG FIXED: the single regex below matched any occurrence of
# "stop"/"cancel"/"end" anywhere in a sentence, permanently DNC-ing
# legitimate conversations. Confirmed false positives:
#     "I want to cancel my Ameren service"      (a BUYING signal)
#     "Can you stop by Saturday?"
#     "I need to cancel Tuesday, can we do Thursday?"
#     "What time does the appointment end?"
# 10 of 12 sampled natural phrases were misclassified as opt-outs.
#
# TIER 1 - CTIA keyword as a STANDALONE message (carrier-recognised).
# TIER 2 - unambiguous natural-language revocation phrases.
# Anything else is ordinary conversation and is NOT an opt-out.
#
# LEGACY: STOP_KEYWORDS is retained (unused by is_stop_request) so any
# external reference keeps importing cleanly.
STOP_KEYWORDS = re.compile(
    r"\b(stop|quit|cancel|end|unsubscribe|opt.?out|remove me|take me off)\b",
    re.IGNORECASE,
)

CTIA_OPT_OUT_KEYWORDS = frozenset({
    "stop", "stopall", "stop all", "unsubscribe", "cancel", "end", "quit",
    "optout", "opt out", "revoke", "arret", "arretez",
})

_OPT_OUT_FILLERS = frozenset({"please", "pls", "plz", "just", "now", "texts",
                              "text", "texting", "messages", "msgs", "me", "all"})

REVOCATION_PHRASES = (
    "stop contacting me", "stop texting me", "stop messaging me",
    "quit texting me", "do not contact me", "don't contact me",
    "do not text me", "don't text me", "do not call or text",
    "don't call or text", "never contact me", "lose my number",
    "take me off your list", "take me off the list", "remove me from your list",
    "remove me from everything", "remove me completely", "delete my info",
    "delete my information", "unsubscribe me", "not interested stop",
)


def _normalize_for_optout(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    t = (text or "").lower().strip()
    t = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in t)
    return " ".join(t.split())


def is_explicit_opt_out(text: str) -> bool:
    """TIER 1 - CTIA keyword sent as a standalone message."""
    t = _normalize_for_optout(text)
    if not t:
        return False
    if t in CTIA_OPT_OUT_KEYWORDS:
        return True
    tokens = t.split()
    if len(tokens) <= 3:
        core = [w for w in tokens if w not in _OPT_OUT_FILLERS]
        if core and " ".join(core) in CTIA_OPT_OUT_KEYWORDS:
            return True
    return False


def is_revocation_intent(text: str) -> bool:
    """TIER 2 - unambiguous natural-language revocation."""
    t = _normalize_for_optout(text)
    if not t:
        return False
    return any(_normalize_for_optout(p) in t for p in REVOCATION_PHRASES)

BOOKED_KEYWORDS = re.compile(
    r"\b(booked|scheduled|just booked|i booked|confirmed|i scheduled|set it up|grabbed a time)\b",
    re.IGNORECASE,
)

def is_stop_request(text: str) -> bool:
    """
    True only for a genuine opt-out.
    Signature and name unchanged - all existing callers work untouched.
    """
    return is_explicit_opt_out(text) or is_revocation_intent(text)
def is_booking_confirmation(text: str) -> bool: return bool(BOOKED_KEYWORDS.search(text))


# ─────────────────────────────────────────────
#  BOOKED-CONTACT MESSAGES
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
#  [META-B1] STRUCTURED EVENT LOG
#
#  One line per funnel-relevant decision, greppable in Render logs.
#  Carries contact_id and a masked phone only — never a name, never a
#  message body, never a token.
# ─────────────────────────────────────────────

def ev(event: str, contact_id: str = "", **fields) -> None:
    """Emit one structured funnel event to stdout AND the logger."""
    parts = [f"contact={contact_id or '(none)'}"]
    for k, v in fields.items():
        if k in ("phone", "to_number"):
            v = _mask_phone(str(v))
        parts.append(f"{k}={v}")
    line = f"[EVENT] {event} | " + " | ".join(parts)
    print(line, flush=True)
    log.info(line)


# ── [META-B1] Meta / paid-social lead detection ──────────────────────────
# A Meta Instant Form lead reaches us either with a source string from the
# GHL Facebook integration or with the meta-lead tag applied by the
# "Facebook/IG Paid Lead" workflow.  Either signal is sufficient.
_META_SOURCE_TOKENS = (
    "facebook", "faceb", "instagram", "insta", "meta",
    "fb_lead", "fb-lead", "fblead", "ig_lead", "ig-lead",
    "paid_social", "paid-social",
)
_META_TAGS = frozenset({"meta-lead", "facebook-lead", "fb-lead", "ig-lead", "paid-social"})


def is_meta_lead(lead_source: str = "", tags: list | None = None) -> bool:
    """True when this contact came from a Meta (Facebook/Instagram) paid form."""
    src = (lead_source or "").lower().strip()
    if src and any(tok in src for tok in _META_SOURCE_TOKENS):
        return True
    for t in (tags or []):
        tl = str(t).lower().strip()
        if tl in _META_TAGS or tl == "facebook":
            return True
    return False


def has_tag(tags: list | None, wanted: str) -> bool:
    """Case-insensitive membership test against a GHL tags list."""
    w = wanted.lower().strip()
    return any(str(t).lower().strip() == w for t in (tags or []))


# ═══════════════════════════════════════════════════════════════════
#  [QH] TCPA SEND WINDOW + SMS COMPLIANCE
#
#  Built on main's existing primitives - no parallel implementations:
#    add_ghl_tags()          additive tag write (GET -> merge -> PUT)
#    has_tag()               case-insensitive membership
#    classify_carrier_error() single source of truth for code permanence
#    SendStatus.SUPPRESSED   the established "we did not send" contract
#    ev()                    structured event logging
#    _record_send_result()   caller-side state persistence
# ═══════════════════════════════════════════════════════════════════

# Documented behaviour (KC_EnergyAdvisors_System_Architecture.docx):
#   "Send hours 9 AM - 9 PM Central only"
# CENTRAL_TZ is ZoneInfo("America/Chicago") so DST comes from the tz
# database - never a fixed UTC offset.
#
# Boundaries (inclusive start, exclusive end):
#   08:59 CLOSED | 09:00 OPEN | 20:59 OPEN | 21:00 CLOSED | 21:01 CLOSED
SEND_WINDOW_START_HOUR = 9    # 09:00 inclusive
SEND_WINDOW_END_HOUR   = 21   # 21:00 exclusive

#   TAG_AFTER_HOURS_HOLD - the durable pending queue. GHL is the store;
#                  nothing pending ever lives in Python memory, so a
#                  Render restart cannot lose a held message.
TAG_AFTER_HOURS_HOLD    = "after-hours-hold"
TAG_AFTER_HOURS_EXPIRED = "after-hours-hold-expired"

# An obsolete hold is cleared WITHOUT sending. A reply most of a day late
# reads as disconnected. 9 PM -> 9:05 AM is ~12h, so 18h leaves slack for
# the GHL retry ladder while expiring anything older than a day.
QH_MAX_HOLD_AGE_HOURS = 18

# Reasons attached to a SendStatus.SUPPRESSED result. Callers already
# branch on `status not in _STATUS_ADVANCES_STATE`; these explain why.
SUPPRESS_REASON_DND    = "compliance_dnd"
SUPPRESS_REASON_WINDOW = "outside_send_window"

# Process-local observability only. The AUTHORITATIVE queue is the GHL
# tag; these reset on restart and are exposed via /health.
_qh_stats: dict = {
    "holds_placed"      : 0,
    "hold_failures"     : 0,
    "resumes_processed" : 0,
    "resumes_skipped"   : 0,
    "resumes_sent"      : 0,
    "last_hold_at"      : "",
    "last_resume_at"    : "",
    "last_hold_failure_at"      : "",
    "last_hold_failure_contact" : "",
}


def within_send_window(now=None) -> bool:
    """True when automated outbound SMS is permitted. DST-aware."""
    if now is None:
        now = datetime.now(tz=CENTRAL_TZ)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=CENTRAL_TZ)
    local = now.astimezone(CENTRAL_TZ)
    return SEND_WINDOW_START_HOUR <= local.hour < SEND_WINDOW_END_HOUR


def send_window_status(now=None) -> dict:
    """Non-sensitive window description for /health."""
    if now is None:
        now = datetime.now(tz=CENTRAL_TZ)
    local = now.astimezone(CENTRAL_TZ)
    return {
        "open"        : within_send_window(local),
        "now_central" : local.isoformat(timespec="seconds"),
        "tz_abbrev"   : local.strftime("%Z"),
        "utc_offset"  : local.strftime("%z"),
        "window"      : f"{SEND_WINDOW_START_HOUR:02d}:00-"
                        f"{SEND_WINDOW_END_HOUR:02d}:00 America/Chicago",
    }


def hold_age_hours(timestamp) -> float | None:
    """Hours since an ISO-8601 or epoch-ms timestamp. None if unparseable."""
    if not timestamp:
        return None
    try:
        if isinstance(timestamp, (int, float)) or str(timestamp).isdigit():
            n = float(timestamp)
            if n > 1e11:
                n /= 1000.0
            when = datetime.fromtimestamp(n, tz=CENTRAL_TZ)
        else:
            when = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        return (datetime.now(tz=CENTRAL_TZ) - when).total_seconds() / 3600.0
    except Exception:
        return None


# ── GSM-7 transliteration ─────────────────────────────────────────
# A single non-GSM-7 character forces the WHOLE message into UCS-2,
# dropping the segment limit from 160 chars to 70. One em dash turns a
# 167-char first SMS into 3 segments instead of 1 - 3x cost on every
# first touch plus extra carrier-filtering scrutiny.
_GSM7_SUBSTITUTIONS = {
    "—": "-",  "–": "-",  "‒": "-",  "−": "-",
    "‘": "'",  "’": "'",  "‚": "'",  "′": "'",
    "“": '"',  "”": '"',  "„": '"',  "″": '"',
    "…": "...", " ": " ",  " ": " ",  " ": " ",
    " ": " ",  " ": " ",  "•": "-",  "·": "-",
    "™": "(TM)", "®": "(R)", "©": "(C)",
}

_GSM7_CHARSET = set(
    "@£$¥èéùìòÇ\nØø\rÅå"
    "Δ_ΦΓΛΩΠΨΣΘΞÆæßÉ"
    " !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§"
    "¿abcdefghijklmnopqrstuvwxyzäöñüà"
) | set("^{}\\[~]|€")


def to_gsm7_safe(text: str, contact_id: str = "") -> str:
    """Transliterate common Unicode punctuation to GSM-7 equivalents.
    Idempotent. Wording is never altered - only glyphs."""
    if not text:
        return text
    out = text
    for bad, good in _GSM7_SUBSTITUTIONS.items():
        if bad in out:
            out = out.replace(bad, good)
    residual = sorted({c for c in out if c not in _GSM7_CHARSET})
    if residual:
        ev("GSM7_RESIDUAL", contact_id, chars=residual,
           note="message will send as UCS-2 (70-char segments)")
    return out


# ── SMS failure classification ────────────────────────────────────
# GHL records the provider reason inside dndSettings.SMS.message as free
# text, e.g. "TWILIO_ERROR_CODE: 30006" or "STOP_KEYWORD". Confirmed
# against 20 live contacts.
#
# PERMANENCE IS NOT DECIDED HERE. classify_carrier_error() is the single
# source of truth for that and already covers 30003/30004/30005/30006/
# 21211/21610/21614/30007.
_PROVIDER_CODE_RE = re.compile(
    r"(?:TWILIO_ERROR_CODE|ERROR_CODE|CODE)\s*[:=]?\s*(\d{4,6})", re.IGNORECASE)

# Provider code -> semantic category. Answers "why", not "retry?".
SMS_FAILURE_CATEGORY = {
    "21610": "CARRIER_SUPPRESSION",
    "21211": "UNREACHABLE_OR_BAD_NUMBER",
    "21214": "UNREACHABLE_OR_BAD_NUMBER",
    "21614": "UNREACHABLE_OR_BAD_NUMBER",
    "30003": "UNREACHABLE_OR_BAD_NUMBER",
    "30005": "UNREACHABLE_OR_BAD_NUMBER",
    "30006": "UNREACHABLE_OR_BAD_NUMBER",
    "30004": "A2P_OR_CARRIER_FILTERING",
    "30007": "A2P_OR_CARRIER_FILTERING",
    "30032": "A2P_OR_CARRIER_FILTERING",
    "30034": "A2P_OR_CARRIER_FILTERING",
}


def extract_provider_code(*sources) -> str:
    """Pull a provider error code out of any free-text status string."""
    for src in sources:
        if not src:
            continue
        m = _PROVIDER_CODE_RE.search(str(src))
        if m:
            return m.group(1)
        m2 = re.search(r"\b(2[01]\d{3}|30\d{3})\b", str(src))
        if m2:
            return m2.group(1)
    return ""


def classify_sms_failure(dnd_sms: dict | None = None,
                         http_status: int | None = None,
                         response_body=None) -> tuple:
    """
    Returns (category, provider_code, permanently_ineligible).

    Categories: EXPLICIT_OPT_OUT | CARRIER_SUPPRESSION |
                UNREACHABLE_OR_BAD_NUMBER | A2P_OR_CARRIER_FILTERING |
                CONFIG_ERROR | PRE_EXISTING_DND | UNKNOWN

    Permanence is delegated to classify_carrier_error().
    FAILS CLOSED: anything unrecognised returns UNKNOWN, which the gate
    treats as "do not send".
    """
    dnd_sms = dnd_sms or {}
    status  = str(dnd_sms.get("status") or "").strip().lower()
    msg     = str(dnd_sms.get("message") or "")
    code    = str(dnd_sms.get("code") or "")

    # 1. Explicit consumer opt-out - highest precedence, never overridden.
    if "stop_keyword" in msg.lower() or status == "permanent":
        return "EXPLICIT_OPT_OUT", "", True

    # 2. Provider error code embedded in the DND message.
    provider = extract_provider_code(msg, code, response_body)
    if provider:
        category  = SMS_FAILURE_CATEGORY.get(provider, "A2P_OR_CARRIER_FILTERING")
        permanent = classify_carrier_error(provider) == "permanent"
        return category, provider, permanent

    # 3. HTTP-level configuration problems.
    if http_status and int(http_status) in (400, 401, 403, 404, 422):
        return "CONFIG_ERROR", str(http_status), False

    # 4. DND is on but the API did not disclose why - fail closed.
    if status:
        return "PRE_EXISTING_DND", "", False

    return "UNKNOWN", "", False


# ── Compliance state from GHL ─────────────────────────────────────
async def fetch_ghl_contact_compliance(contact_id: str) -> dict:
    """
    Fetch phone, email, tags AND compliance state in ONE call.

    GET /contacts/{id} already returns `dnd` and `dndSettings`.
    fetch_ghl_contact_phone() downloads that payload and throws the
    compliance fields away, which is why the agent has been blind to DND.
    """
    blank = {"ok": False, "first_name": "", "full_name": "", "phone": "",
             "email": "", "dnd": None, "dnd_sms": {}, "tags": [],
             "date_added": "", "http": None}
    if not contact_id:
        return blank
    url = f"{GHL_API_BASE}/contacts/{contact_id}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=_ghl_headers(_GHL_HEADERS_V2))
        if not r.is_success:
            ev("COMPLIANCE_LOOKUP_FAILED", contact_id, http=r.status_code)
            return {**blank, "http": r.status_code}
        data    = r.json()
        contact = data.get("contact") or data
        phone   = (contact.get("phone") or contact.get("mobilePhone")
                   or contact.get("homePhone") or "")
        dnd_settings = contact.get("dndSettings") or {}
        dnd_sms = dnd_settings.get("SMS") or dnd_settings.get("sms") or {}
        _tags = contact.get("tags") or []
        _fn = str(contact.get("firstName") or contact.get("first_name") or "").strip()
        _ln = str(contact.get("lastName") or contact.get("last_name") or "").strip()
        return {
            "ok"        : True,
            "first_name": _fn,
            "full_name" : (f"{_fn} {_ln}".strip() or
                           str(contact.get("contactName") or "").strip()),
            "phone"     : str(phone).strip() if phone else "",
            "email"     : str(contact.get("email") or "").strip(),
            "dnd"       : contact.get("dnd"),
            "dnd_sms"   : dnd_sms if isinstance(dnd_sms, dict) else {},
            "tags"      : list(_tags) if isinstance(_tags, list) else [],
            "date_added": contact.get("dateAdded") or contact.get("createdAt") or "",
            "http"      : r.status_code,
        }
    except Exception as err:
        ev("COMPLIANCE_LOOKUP_ERROR", contact_id, error=str(err)[:120])
        return blank


async def can_contact_sms(contact_id: str, state: dict | None = None) -> dict:
    """
    FAIL-CLOSED pre-send compliance gate.

    The ONLY authority on whether an outbound SMS may be transmitted.
    Returns {"allowed": bool, "reason": str, "category": str,
             "code": str, "permanent": bool, "phone": str, "email": str}

    Denies when:
      * local state records a permanent suppression
      * GHL reports SMS DND active/permanent
      * the compliance lookup fails or is ambiguous  <- fails CLOSED

    There is deliberately NO override parameter. A legitimate opt-out
    cannot be bypassed by any caller.
    """
    state = state if isinstance(state, dict) else {}

    if state.get("sms_opt_out") is True:
        return {"allowed": False, "reason": "local ledger: consumer opt-out",
                "category": "EXPLICIT_OPT_OUT", "code": "", "permanent": True,
                "phone": "", "email": ""}
    if state.get("sms_ineligible") is True:
        return {"allowed": False,
                "reason": f"local ledger: number cannot receive SMS "
                          f"({state.get('sms_failure_code') or 'n/a'})",
                "category": state.get("sms_failure_category") or "UNREACHABLE_OR_BAD_NUMBER",
                "code": state.get("sms_failure_code") or "", "permanent": True,
                "phone": "", "email": ""}

    c = await fetch_ghl_contact_compliance(contact_id)
    if not c["ok"]:
        return {"allowed": False,
                "reason": "compliance lookup failed - failing closed",
                "category": "UNKNOWN", "code": "", "permanent": False,
                "phone": "", "email": ""}

    dnd_sms = c["dnd_sms"] or {}
    status  = str(dnd_sms.get("status") or "").strip().lower()
    if c["dnd"] is True or (status and status not in ("inactive", "")):
        cat, code, permanent = classify_sms_failure(dnd_sms)
        return {"allowed": False,
                "reason": f"GHL SMS DND active (status={status or 'account-wide'})",
                "category": cat, "code": code, "permanent": permanent,
                "phone": c["phone"], "email": c["email"]}

    return {"allowed": True, "reason": "no SMS DND on record",
            "category": "", "code": "", "permanent": False,
            "phone": c["phone"], "email": c["email"]}


async def record_sms_outcome(contact_id: str, category: str, code: str,
                             permanent: bool, email: str = "") -> None:
    """
    Persist a classified compliance outcome.

    Complements _record_send_result(), which records SUCCESS metadata
    (last_send_status / last_message_id). This records WHY a send was
    refused and what that means for future contactability.

    [R1] A confirmed opt-out is ALSO written to GHL as the durable DNC
    tag set, because state["stage"] alone does not survive a restart -
    apply_restored_stage() rebuilds stage from GHL evidence, and without
    the tag a DNC set here would be silently downgraded. The tag list
    comes from resolve_ghl_tags(Stage.DNC) so there is exactly one
    definition of "what DNC looks like in GHL".

    Sends NO fallback message. The only GHL write is the DNC tag.
    """
    try:
        st = get_state(contact_id)
    except Exception:
        return
    st["sms_failure_category"] = category
    st["sms_failure_code"]     = code
    st["sms_failure_at"]       = datetime.now(tz=CENTRAL_TZ).isoformat(timespec="seconds")
    if category == "EXPLICIT_OPT_OUT":
        # A CONFIRMED opt-out permanently suppresses automated SMS AND
        # short-circuits michael_agent() at its DNC guard, so future
        # inbound never reaches Claude.
        #
        # FALSE-POSITIVE SAFETY: this branch is reachable ONLY when
        # classify_sms_failure() returns EXPLICIT_OPT_OUT, which requires
        # GHL's own authoritative record ("STOP_KEYWORD" in
        # dndSettings.SMS.message, or status == "permanent"). It is never
        # reached from conversational text matching.
        st["sms_opt_out"]    = True
        st["sms_ineligible"] = True
        st["stage"]          = Stage.DNC
        _dnc_tags = resolve_ghl_tags(Stage.DNC)
        try:
            _tagged = await add_ghl_tags(contact_id, _dnc_tags)
        except Exception as _tag_err:
            _tagged = False
            log.warning(f"[{contact_id}] DNC tag write failed (non-fatal): {_tag_err}")
        ev("DNC_PERSISTED", contact_id, tags=_dnc_tags, durable=_tagged,
           note="stage=DNC now survives restart via GHL tag"
                if _tagged else "IN-MEMORY ONLY - tag write failed")
    elif permanent:
        st["sms_ineligible"]   = True
        st["fallback_pending"] = True      # preserved for a future channel
        st["fallback_email"]   = email or st.get("fallback_email", "")
    try:
        save_state(contact_id, st)
    except Exception:
        pass
    ev("SMS_OUTCOME_RECORDED", contact_id, category=category,
       code=code or "(none)", permanent=permanent,
       fallback_pending=st.get("fallback_pending", False))


# ── After-hours hold (durable queue = one GHL tag) ────────────────
async def remove_ghl_tags(contact_id: str, drop_tags: list) -> bool:
    """
    Remove tags from a GHL contact WITHOUT dropping the others.
    Mirrors add_ghl_tags()'s GET -> merge -> PUT shape so the codebase
    has ONE tag-write convention. Non-fatal.
    """
    if not (contact_id and drop_tags and GHL_API_KEY):
        return False
    url = f"{GHL_API_BASE}/contacts/{contact_id}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=_ghl_headers(_GHL_HEADERS_V2))
            if not r.is_success:
                log.warning(f"[{contact_id}] remove_ghl_tags: GET failed ({r.status_code})")
                return False
            contact  = (r.json() or {}).get("contact") or (r.json() or {})
            existing = [str(t) for t in (contact.get("tags") or [])]
            drop     = {t.lower().strip() for t in drop_tags}
            kept     = [t for t in existing if t.lower().strip() not in drop]
            if len(kept) == len(existing):
                return True      # nothing to remove - already clear
            w = await client.put(url, json={"tags": kept}, headers=_ghl_headers(_GHL_HEADERS_V2))
            if not w.is_success:
                log.warning(f"[{contact_id}] remove_ghl_tags: PUT failed ({w.status_code})")
                return False
        log.info(f"[{contact_id}] tags removed: {drop_tags} (kept {len(kept)})")
        return True
    except Exception as err:
        log.warning(f"[{contact_id}] remove_ghl_tags failed (non-fatal): {err}")
        return False


async def place_after_hours_hold(contact_id: str) -> bool:
    """
    Mark the contact as pending in GHL - the durable queue.

    Delegates to add_ghl_tags(), which already reads existing tags,
    short-circuits when the tag is present (idempotent - a lost response
    is not a second event), and never replaces the tag array.
    """
    if not contact_id:
        return False
    ok = await add_ghl_tags(contact_id, [TAG_AFTER_HOURS_HOLD])
    if ok:
        _qh_stats["holds_placed"] += 1
        _qh_stats["last_hold_at"] = datetime.now(tz=CENTRAL_TZ).isoformat(timespec="seconds")
        ev("AFTER_HOURS_HOLD_PLACED", contact_id, tag=TAG_AFTER_HOURS_HOLD)
    else:
        _qh_stats["hold_failures"] += 1
        _qh_stats["last_hold_failure_at"] = datetime.now(tz=CENTRAL_TZ).isoformat(timespec="seconds")
        _qh_stats["last_hold_failure_contact"] = contact_id
        ev("AFTER_HOURS_HOLD_FAILED", contact_id,
           note="no durable hold - contact will NOT auto-resume")
    return ok


async def clear_after_hours_hold(contact_id: str) -> bool:
    """Remove the pending marker. Idempotent."""
    ok = await remove_ghl_tags(contact_id, [TAG_AFTER_HOURS_HOLD])
    ev("AFTER_HOURS_HOLD_CLEARED", contact_id, ok=ok)
    return ok


async def fetch_latest_conversation_signal(contact_id: str) -> dict:
    """
    Read the CURRENT conversation from GHL at resume time.

    GHL's message thread is the durable record of what happened overnight;
    _state_store does not survive a Render restart.

    Returns {"ok", "latest_inbound", "last_direction", "inbound_count",
             "latest_inbound_at", "history"} where history is oldest-first
    and EXCLUDES the newest inbound (michael_agent appends that itself).
    """
    blank = {"ok": False, "latest_inbound": "", "last_direction": "",
             "inbound_count": 0, "latest_inbound_at": "", "history": []}
    if not contact_id:
        return blank
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(f"{GHL_API_BASE}/conversations/search",
                                 params={"locationId": GHL_LOCATION_ID,
                                         "contactId": contact_id, "limit": 5},
                                 headers=_ghl_headers(_GHL_HEADERS_V2))
            if not r.is_success:
                return blank
            convos = (r.json() or {}).get("conversations") or []
            if not convos:
                return {**blank, "ok": True}
            conv_id = convos[0].get("id")
            if not conv_id:
                return {**blank, "ok": True}
            m = await client.get(f"{GHL_API_BASE}/conversations/{conv_id}/messages",
                                 params={"limit": 25}, headers=_ghl_headers(_GHL_HEADERS_V2))
            if not m.is_success:
                return blank
            msgs = (m.json() or {}).get("messages")
            if isinstance(msgs, dict):
                msgs = msgs.get("messages") or []
            msgs = msgs if isinstance(msgs, list) else []
    except Exception as err:
        ev("QH_CONVERSATION_LOOKUP_ERROR", contact_id, error=str(err)[:120])
        return blank

    latest_inbound, last_direction, inbound_count = "", "", 0
    latest_inbound_at, latest_inbound_id = "", ""
    ordered = []
    for msg in msgs:                       # GHL returns newest-first
        if not isinstance(msg, dict):
            continue
        d = str(msg.get("direction") or "").lower()
        if d not in ("inbound", "outbound"):
            continue
        body = str(msg.get("body") or msg.get("message") or "").strip()
        ordered.append({"id": msg.get("id") or "", "direction": d, "body": body})
        if not last_direction:
            last_direction = d
        if d == "inbound":
            inbound_count += 1
            if not latest_inbound and body:
                latest_inbound    = body
                latest_inbound_at = str(msg.get("dateAdded") or
                                        msg.get("dateUpdated") or "")
                latest_inbound_id = msg.get("id") or ""

    ordered.reverse()                      # oldest-first for Claude
    history = []
    for msg in ordered:
        if latest_inbound_id and msg["id"] == latest_inbound_id:
            continue
        if not msg["body"]:
            continue
        history.append({
            "role"   : "user" if msg["direction"] == "inbound" else "assistant",
            "content": msg["body"],
        })
    return {"ok": True, "latest_inbound": latest_inbound,
            "last_direction": last_direction, "inbound_count": inbound_count,
            "latest_inbound_at": latest_inbound_at, "history": history[-20:]}


def rebuild_history_from_ghl(contact_id: str, history: list) -> int:
    """
    Restore Claude conversation context after a Render restart.

    Without this, a morning reply answers the newest message with ZERO
    preceding context and reads as disconnected.

    Only fills an EMPTY history - never clobbers live in-memory state.
    Anthropic requires messages[0].role == "user", so a leading assistant
    turn is dropped. Returns the number of turns restored.
    """
    if not history:
        return 0
    st = get_state(contact_id)
    if st.get("messages"):
        return 0                            # live state wins
    restored = list(history)
    while restored and restored[0].get("role") == "assistant":
        restored.pop(0)
    if not restored:
        return 0
    st["messages"] = restored
    save_state(contact_id, st)
    ev("QH_CONTEXT_REBUILT", contact_id, turns=len(restored))
    return len(restored)


_INVALID_NAMES = {"unknown", "none", "", "admin", "admin notifications", "notifications"}

def _resolve_first_name(first_name: str = "", full_name: str = "") -> str:
    """Shared name resolution: prefer first_name, fall back to first word of full_name."""
    candidate = first_name.strip() if first_name else ""
    if candidate and candidate.lower() not in _INVALID_NAMES:
        return candidate
    candidate = full_name.strip() if full_name else ""
    if candidate and candidate.lower() not in _INVALID_NAMES:
        return candidate.split()[0]
    return ""


# ─────────────────────────────────────────────────────────────────────────────
#  SINGLE SOURCE OF TRUTH: BRAND CONFIG + BOOKING MESSAGE TEMPLATES
#
#  ALL customer-facing copy for booking confirmations lives here.
#  No brand names, utility names, or booking SMS copy belong anywhere else.
#  When branding or messaging changes, update BRAND and BOOKING_TEMPLATES only.
# ─────────────────────────────────────────────────────────────────────────────

BRAND: dict[str, str] = {
    "name"          : "STL Energy Advisors",
    "agent"         : "Michael",
    "utility"       : "Ameren",
    "service_area"  : "St. Louis metro",
    "visit_duration": "about 30 minutes",
    "visit_cost"    : "no cost, no pressure",
}

# Booking SMS templates keyed by variant name.
# Rendered exclusively by build_booking_message() — never interpolated inline.
BOOKING_TEMPLATES: dict[str, str] = {
    # PATH 2 — Direct calendar booking.
    # First text the contact receives from Michael.  Brief confirmation + bill ask.
    "direct_booking": (
        "{greeting} You're all set — I'll stop by your home for {visit_duration}, "
        "{visit_cost}. "
        "One favor before I get there: snap a photo of your latest {utility} bill "
        "and send it my way — helps me run your numbers ahead of the visit 👍"
    ),
    # PATH 1 — Chat-widget lead who already knows Michael from the SMS flow.
    # Warm continuation — no re-introduction needed.
    "sms_flow": (
        "Perfect{name_suffix}, you're all set! "
        "Send over a photo of your most recent {utility} bill when you get a chance — "
        "I'll look it over before I come by 👍"
    ),
}


def build_booking_confirmation(
    variant:    str,
    first_name: str = "",
    full_name:  str = "",
) -> tuple[str, str]:
    """
    Single entry point for all post-booking confirmation SMS copy.

    Reads exclusively from BRAND and BOOKING_TEMPLATES — no hardcoded strings.
    Returns (rendered_message, template_key_used) so every caller can log both.

    NOTE: This function sends the confirmation AFTER a contact books.
          For the SMS that sends the calendar booking link to a qualified lead,
          use build_booking_message() (defined later in this file).

    Args:
        variant:    "direct_booking" | "sms_flow"
                    Falls back to "direct_booking" for unknown / invalid values.
        first_name: Preferred; resolved via _resolve_first_name.
        full_name:  Fallback when first_name is blank or invalid.

    Returns:
        (message_text, template_key)  — template_key is what was actually rendered.
    """
    first        = _resolve_first_name(first_name, full_name)
    template_key = variant if variant in BOOKING_TEMPLATES else "direct_booking"
    template     = BOOKING_TEMPLATES[template_key]

    if template_key == "direct_booking":
        rendered = template.format(
            greeting       = f"Hey {first}!" if first else "Hey!",
            visit_duration = BRAND["visit_duration"],
            visit_cost     = BRAND["visit_cost"],
            utility        = BRAND["utility"],
        )
    else:  # sms_flow
        rendered = template.format(
            name_suffix = f" {first}" if first else "",
            utility     = BRAND["utility"],
        )

    return rendered, template_key


_NEGATIVE_REPLY = re.compile(
    r"^\s*(no|nope|not now|not interested|nevermind|never mind|forget it|"
    r"skip it|don'?t|won'?t|can'?t|not going to|nah)\s*[.!?]?\s*$",
    re.IGNORECASE,
)
_POSITIVE_BILL_REPLY = re.compile(
    r"\b("
    # General affirmatives
    r"sure|ok|okay|yes|yeah|yep|yup|will do|on it|got it|sounds good|"
    r"no problem|of course|absolutely|definitely|done|yep|cool|alright|"
    # Bill/photo specific
    r"bill|electric|utility|photo|pic\b|picture|image|attach|"
    r"sending|sent|send it|sent it|shared it|shared|share|"
    r"here it is|here you go|here's|here is|"
    r"just sent|just texted|just shared|just uploaded|just attached|"
    r"i'?ll send|i will send|sending now|on my way|"
    # Common follow-up phrases when bill was already sent separately
    r"check your|look at|take a look|sent over|just dropped|dropped it|"
    r"uploaded|submitted|forwarded|texted it|messaged it|already sent|"
    r"did you get|did you receive|you should have|should be there|"
    r"just emailed|emailed it|email"
    r")\b",
    re.IGNORECASE,
)

def is_bill_response(text: str) -> bool:
    """
    Returns True if `text` looks like the lead confirming they sent/are sending
    their electric bill.  Used as PATH A secondary trigger in the booked flow
    (primary trigger is has_attachment / MMS image).

    Conservative: returns False for clear negatives, True for empty body
    (image with no caption), and True if any positive keyword matches.
    """
    if _NEGATIVE_REPLY.match(text):
        return False
    if not text.strip():
        return True   # empty body = MMS photo with no caption
    return bool(_POSITIVE_BILL_REPLY.search(text))


# ─────────────────────────────────────────────
#  BOOKED-CONTACT BILL SUBMISSION DETECTOR  [FIX-1/4/5/7]
#
#  Single authoritative gate: "did a booked lead just send their electric bill?"
#
#  Replaces the weaker is_bill_response() in the booked flow.
#  Four priority tiers (any single tier → True):
#    1. Attachment or MMS/IMAGE type present         → always bill
#    2. Strong explicit phrase (sent, here ya go, …) → phrase match
#    3. Short ambiguous reply when reminder was sent  → lean BILL_ACK
#    4. Hard-negative override                        → always False
#
#  Design intent: false positives (sending a bill ack to someone who said
#  something unrelated) are FAR preferable to confused generic replies for
#  a booked lead who just sent their bill photo.  Be generous.
# ─────────────────────────────────────────────

# Comprehensive strong-match patterns:
# "here ya go", "sent it", "just sent", "see attached", "electric bill", etc.
_BILL_SUBMISSION_STRONG = re.compile(
    r'(?:'
    # "here ya go" / "here you go" / "here it is" / "here's my bill"
    r"here\s+(?:ya|you)\s+go\b"
    r"|here\s+it\s+(?:is|goes)\b"
    r"|here(?:'?s)?\s+(?:the|my)\s+(?:bill|electric|utility|power|photo|pic|image)\b"
    r"|here\s+(?:is|are)\s+(?:the|my)\s+(?:bill|electric|utility|power|photo|pic|image)\b"
    # "sent it" / "just sent" / "just sent it over" / "already sent"
    r"|sent\s+it\b"
    r"|sent\s+(?:it\s+)?over\b"
    r"|just\s+sent(?:\s+it|\s+that|\s+the\s+(?:bill|photo|pic))?\b"
    r"|just\s+(?:texted|shared|uploaded|attached|dropped)\s+(?:it|the\s+(?:bill|photo|pic))?\b"
    r"|already\s+sent\s+(?:it|the\s+(?:bill|photo|pic))?\b"
    r"|sending\s+(?:it|now|the\s+(?:bill|photo|pic))\b"
    r"|i(?:'?ve)?\s+sent\s+(?:it|the\s+(?:bill|photo|pic)|over)\b"
    # "there it is" / "there ya go" / "that's it"
    r"|there\s+it\s+is\b"
    r"|there\s+(?:ya|you)\s+go\b"
    r"|that(?:'?s)?\s+(?:it|the\s+one|my\s+bill)\b"
    r"|this\s+(?:is\s+)?(?:it|the\s+one|my\s+bill|the\s+bill)\b"
    # See attached / look above / check above
    r"|see\s+(?:attached|above|the\s+photo|the\s+bill|the\s+pic|the\s+image)\b"
    r"|look\s+(?:above|at\s+(?:the\s+)?(?:photo|bill|pic))\b"
    r"|check\s+(?:above|it\s+out|the\s+(?:photo|pic|bill))\b"
    # Bill / photo / attachment with any qualifier
    r"|(?:electric|utility|power|energy)\s+bill\b"
    r"|bill\s+(?:photo|pic|image|attached|sent|above)\b"
    r"|bill\s+attach(?:ed|ment)\b"
    r"|(?:photo|pic|image)\s+(?:sent|attached|above|of\s+(?:the\s+)?bill)\b"
    # "gotchu" / "got you" (informal ack used when sending something)
    r"|got(?:chu|cha|[\s\-]?you)\b"
    # "does this work" / "can you see it" / "is this good" / "did you get it"
    r"|does\s+this\s+work\b"
    r"|can\s+you\s+see\s+(?:it|this|the\s+(?:photo|bill))\b"
    r"|is\s+this\s+(?:good|ok|okay|right|clear)\b"
    r"|did\s+(?:you\s+)?(?:get|receive)\s+(?:it|the\s+(?:bill|photo|pic)|my\s+bill)?\b"
    r"|did\s+(?:that|it)\s+(?:come|go)\s+through\b"
    # "this one" / "this work" (contextual)
    r"|this\s+one[?!]?\b"
    r")",
    re.IGNORECASE,
)

# Short replies that are ambiguous but strongly imply bill-sending in context.
# Only used when bill_reminder_sent=True so context is tight.
_BILL_AMBIGUOUS_SHORT = re.compile(
    r'^\s*(?:'
    r"here|there|above|look|check|see|done|sent|ok|okay|k|yep|yeah|yup|sure|cool|"
    r"got\s+it|send|sending|check|see\s+it|see\s+above|"
    r"here\s+(?:ya|you)\s+go|gotcha|gotchu|'?k|this|that|attached|attachment"
    r')\s*[.!?]?\s*$',
    re.IGNORECASE,
)

# Hard negative patterns — always False regardless of other signals.
# Logistics questions clearly asking about the visit / time / preparation.
_BILL_HARD_NEGATIVE = re.compile(
    r'\b(?:'
    r"what\s+time|when\s+are\s+you|when\s+do\s+you|how\s+long\s+(?:will|does|is)|"
    r"where\s+(?:do|will|are)\s+you\b|do\s+i\s+need\s+to\b|"
    r"what\s+should\s+i\b|what\s+do\s+i\s+need\b|"
    r"is\s+there\s+(?:a|an)\s+(?:fee|cost|charge)|"
    r"can\s+(?:you|someone)\s+call\b|"
    r"looking\s+forward\s+to\b|"
    r"see\s+you\s+(?:then|soon|tomorrow|next\s+week)\b|"
    r"can'?t\s+wait\b|excited\s+(?:to|for)\b|"
    r"not\s+(?:yet|ready|sure)|haven'?t\s+(?:yet|had|found)\b|"
    r"hold\s+on\b|give\s+me\s+a\b|later\s*[.!?]?\s*$"
    r')\b',
    re.IGNORECASE,
)


def is_booked_bill_submission(
    message: str,
    has_attachment: bool = False,
    msg_type: str = "SMS",
    bill_reminder_sent: bool = False,
) -> tuple[bool, str]:
    """
    [FIX-1/4/5/7] Comprehensive bill submission detector for booked contacts.

    Returns (True, reason) when ANY of these are true:
      1. has_attachment=True OR msg_type is MMS/IMAGE        → attachment signal
      2. Strong explicit bill/photo/sending phrase in text   → phrase match
      3. Short ambiguous reply AND bill_reminder_sent=True   → lean BILL_ACK
         (generous fallback: "here", "sent", "done", "above", etc.)

    Returns (False, reason) for:
      • Clear logistics questions ("what time are you coming?")
      • Explicit negatives ("not yet", "haven't", "hold on")
      • Messages that clearly aren't about sending a bill

    Design intent: false positives (bill ack to an unrelated short message) are
    FAR preferable to sending a confused generic reply to a booked lead who just
    sent their electric bill photo.  Be generous for booked leads.
    """
    # ── Priority 1: Attachment or MMS/IMAGE type ─────────────────────
    if has_attachment:
        return True, "has_attachment=True (MMS/image attachment present)"
    if msg_type.upper() in ("MMS", "IMAGE"):
        return True, f"msg_type={msg_type!r} (MMS/IMAGE message type)"

    # Normalize message
    clean_msg = (message or "").strip()

    # Empty body without attachment — could be a GHL workflow webhook, not a lead reply.
    # Without an attachment signal we can't call this a bill submission.
    if not clean_msg or clean_msg.lower() in ("start", ""):
        return False, "empty_body_no_attachment_signal (likely GHL workflow webhook)"

    # ── Hard negative gate — logistics questions / not-yet replies ────
    # Check these FIRST so they override everything below.
    if _BILL_HARD_NEGATIVE.search(clean_msg):
        matched = _BILL_HARD_NEGATIVE.search(clean_msg).group(0)
        return False, f"hard_negative_logistics: {matched!r} in {clean_msg[:40]!r}"

    # Very short pure negatives ("no", "nah", "not now", etc.)
    if _NEGATIVE_REPLY.match(clean_msg):
        return False, f"negative_reply: {clean_msg[:30]!r}"

    # ── Priority 2: Strong explicit phrase ────────────────────────────
    _strong_match = _BILL_SUBMISSION_STRONG.search(clean_msg)
    if _strong_match:
        return True, f"strong_phrase: {_strong_match.group(0)!r}"

    # ── Priority 3: Short ambiguous reply, bill reminder already sent ─
    # For a booked lead waiting on a bill, short replies like "here", "sent",
    # "done", "above", "look" should lean toward BILL_ACK — they almost always
    # mean the lead is confirming they just sent / already sent the photo.
    if bill_reminder_sent and _BILL_AMBIGUOUS_SHORT.match(clean_msg):
        return True, f"short_ambiguous_bill_likely (reminder_sent=True): {clean_msg!r}"

    return False, f"no_bill_signal: {clean_msg[:50]!r}"


def build_bill_ack_message(first_name: str = "", full_name: str = "") -> str:
    """
    Sent after the contact sends their electric bill — both paths.
    Short, confident, closes the loop cleanly.
    """
    first = _resolve_first_name(first_name, full_name)
    if first:
        return f"Perfect, got it {first}. We'll see you then 👍"
    return "Perfect, got it. We'll see you then 👍"


# ─────────────────────────────────────────────
#  BOOKED FOLLOW-UP SYSTEM PROMPT  [FIX-8/12]
#
#  CRITICAL: This is the ONLY system prompt used when michael_agent()
#  is called for a contact whose appointment_booked=True or stage=BOOKED.
#
#  It COMPLETELY replaces build_system_prompt() for booked contacts.
#  build_system_prompt() is NEVER called for booked contacts — it contains
#  the full qualification flow including [SEND_BOOKING] logic, which must
#  be unreachable for booked contacts.
#
#  Why this matters:
#    The standard system prompt's SEND_BOOKING goal reads:
#      "Write one natural transition sentence, then append [SEND_BOOKING]."
#    If Claude is at stage=SEND_BOOKING and sees ANY message, it will try
#    to output [SEND_BOOKING] and qualification language — regardless of
#    whether the code-level guard blocks the URL injection.  The clean_reply
#    text ("Based on what you shared, your home looks like a solid candidate")
#    still goes out as the SMS body.
#
#    Using a completely different prompt removes that risk at the Claude level,
#    not just the code level.
# ─────────────────────────────────────────────

def _stage_behavior(ghl_pipeline_stage: str) -> str:
    """Return stage-specific behavior instructions for the booked follow-up prompt."""
    s = ghl_pipeline_stage.strip()
    if s == "Solar Savings Report Scheduled":
        return (
            "The appointment is CONFIRMED and SCHEDULED.\n"
            "Your job: appointment prep and support only.\n"
            "You may ask if they have their latest Ameren bill ready.\n"
            "You may confirm timing, location, or what to expect."
        )
    elif s == "Consultation Completed":
        return (
            "The in-home consultation is DONE.\n"
            "Your job: post-consult support only — answer questions, handle follow-up.\n"
            "Do NOT re-pitch, re-qualify, or send a new booking link."
        )
    elif s == "Proposal Sent":
        return (
            "A proposal has been sent to this homeowner.\n"
            "Your job: objection handling and decision support.\n"
            "Answer questions about the proposal honestly. Reduce friction.\n"
            "Do NOT re-qualify. Do NOT send a new booking link."
        )
    elif s in ("Closed WON", "Closed LOST"):
        return (
            "This opportunity is CLOSED.\n"
            "Your job: answer any remaining questions warmly and briefly.\n"
            "Do NOT restart qualification or send any booking links."
        )
    else:
        return "Appointment is confirmed. Support and appointment prep only."


def _build_booked_followup_prompt(state: dict, ghl_pipeline_stage: str = "") -> str:
    """
    [FIX-8/12] Minimal, locked-down Claude system prompt for booked contacts.

    Used ONLY when appointment_booked=True or stage=BOOKED.
    NEVER used during the qualification flow.

    The HARD STOP block explicitly forbids:
      • Sending a booking link or URL
      • Asking qualification questions
      • Outputting [SEND_BOOKING] or [QUALIFIED] tags
      • Restarting the qualification flow

    Michael can still reply naturally to follow-up chit-chat like
    "Ok thanks", "What time exactly?", "Looking forward to it", etc.
    """
    name = state.get("contact_name", "")
    name_line = f"  Contact name  : {name}" if name else "  Contact name  : (not on file)"

    return f"""You are Michael with STL Energy Advisors. You are texting a lead who HAS ALREADY BOOKED an in-home solar consultation appointment.

{name_line}
  Appointment   : CONFIRMED — do NOT re-confirm, re-qualify, or re-sell.
  Your job      : Answer simple follow-up questions warmly and briefly. Close the conversation naturally.

━━ ⛔ HARD STOP — DO NOT DO ANY OF THE FOLLOWING ⛔ ━━
• DO NOT send a booking link, calendar link, or any URL
• DO NOT output [SEND_BOOKING], [QUALIFIED], or any control tag
• DO NOT ask about home ownership, utility/service area, or Ameren bill
• DO NOT say "Based on what you shared" or any qualifying language
• DO NOT suggest they book — they already booked
• DO NOT restart the sales flow for any reason

━━ PIPELINE STAGE CONTEXT ━━
Current GHL pipeline stage: {ghl_pipeline_stage or "Solar Savings Report Scheduled (assumed)"}

{_stage_behavior(ghl_pipeline_stage)}

━━ WHAT YOU SHOULD DO ━━
• If they say "thanks" / "sounds good" / "see you then" → reply with "You're all set! See you then 👍" or similar short close.
• If they ask what to expect → "Just a relaxed 30-minute in-home visit — I'll walk through your Ameren bill, your roof, and the real numbers for your home."
• If they ask about timing / what to bring → answer naturally and briefly (1-2 sentences max). If they ask what to bring, "Your latest Ameren bill — that's all I need."
• If they ask about cost / "Is this free?" → "No cost to you for the visit — zero. We only move forward if the numbers make sense for your home. I'll show you everything when I'm there."
• If they ask about the roof / roof damage → "Panels mount on a rack system that actually seals around the penetrations — most installs extend the roof life. I'll take a look at your roof when I come by."
• If they want to reschedule → "Of course — just text or call us and we'll find a new time that works. Do mornings or afternoons work better for you generally?"
• If they express excitement → match the energy briefly, close warmly.
• If they seem confused or skeptical → reassure them calmly in 1-2 sentences, no pressure.

━━ STYLE ━━
• 1-2 sentences max
• Warm, confident, local tone
• No filler words ("Great!", "Absolutely!")
• No URLs, no tags, no qualification language
• NEVER say "Looks like that sent twice", "I already answered this", "You asked that before", or any reference to duplication or technical issues — that is internal system behavior, never customer-facing""".strip()


# ─────────────────────────────────────────────
#  FIRST-CONTACT OUTREACH MESSAGE  [ADAPT-5]
#
#  Sent proactively to brand-new unbooked leads
#  from a chat widget / form submission.
#  Subsequent replies go through michael_agent().
# ─────────────────────────────────────────────

def build_new_contact_outreach(
    first_name:  str = "",
    full_name:   str = "",
    address:     str = "",
    lead_source: str = "",
    tags:        list | None = None,
) -> str:
    """
    First proactive SMS sent to a new chat-widget / form lead.

    STL migration format — intro + utility question. The first qualifier is now
    "are you on Ameren Missouri for electric?" because we only serve Missouri-side
    Ameren homeowners around the St. Louis metro. Illinois leads, rural co-ops,
    and non-Ameren utilities are filtered out at this step (Claude tags
    [DISQUALIFY:OUT_OF_AREA] when the reply indicates they're outside the zone).

    The `address` parameter is accepted for backward compatibility but no longer
    inserted into the question text — utility-first matches the spec better than
    address-confirmation as an opener.

    Format (with first name):
      "Hey {first}, this is Michael with STL Energy Advisors. Got your info — your home may be worth taking a look at.
       Quick question: are you on Ameren Missouri for electric?"

    Format (no first name):
      "Hey, this is Michael with STL Energy Advisors. Got your info — your home may be worth taking a look at.
       Quick question: are you on Ameren Missouri for electric?"
    """
    first = _resolve_first_name(first_name, full_name)

    # ── [META-B1] Meta/paid-social variant ───────────────────────────
    # A Meta Instant Form is two taps on prefilled fields, so recall is
    # low.  Naming the Facebook request turns the text from cold outreach
    # into a continuation of something they just did.  Every other source
    # keeps the original copy byte-for-byte.
    if is_meta_lead(lead_source, tags):
        greeting = f"Hey {first}," if first else "Hey,"
        return (
            f"{greeting} Michael here with STL Energy Advisors. "
            f"I just got the request you submitted on Facebook to see if "
            f"your home qualifies.\n"
            f"I can check that for you real quick — are you currently with "
            f"Ameren Missouri for electric?"
        )

    greeting = f"Hey {first}," if first else "Hey,"
    return (
        f"{greeting} this is Michael with STL Energy Advisors. "
        f"Got your info — your home may be worth taking a look at.\n"
        f"Quick question: are you on Ameren Missouri for electric?"
    )


# ─────────────────────────────────────────────
#  BOOKING MESSAGE  [FIX-3]
#
#  Generated 100% by code — Claude NEVER writes
#  this message.  When michael_agent() detects the
#  [SEND_BOOKING] control tag it REPLACES Claude's
#  reply with this function's output.
#
#  Benefits:
#   • Correct URL is always used (no 404 risk)
#   • [SEND_BOOKING] placeholder can never appear
#   • Consistent, human, on-brand tone every time
#   • Personalised with first name when available
# ─────────────────────────────────────────────

def build_booking_message(first_name: str = "", full_name: str = "") -> str:
    """
    Clean, personalized booking SMS sent when a lead qualifies.

    Frame: in-home review of the actual Ameren bill, the roof, and the usage —
    NOT a "savings report". Honest framing per migration: if the math doesn't
    work for the home, we say so on the visit.

    Deliberately does NOT use the first name. It arrives immediately after the
    homeowner has answered a question, so re-introducing them by name reads as
    a template firing rather than a person replying. "Great, {name}" was the
    single most canned line in the flow.

    The 👍 is gone for the same reason — an emoji nobody asked for.

    Output:
      "Got it. Here's the calendar — pick whatever time works best and I'll
       come by, walk you through the new solar program, compare solar against
       your current Ameren bill, and answer any questions you have.
       {BOOKING_LINK}
       Once you grab a time, I'll send a quick confirmation before I head over."

    first_name / full_name are still accepted so every existing caller keeps
    working unchanged; they are simply no longer interpolated.
    """
    return (
        "Got it. Here's the calendar — pick whatever time works best and I'll "
        "come by, walk you through the new solar program, compare solar against "
        "your current Ameren bill, and answer any questions you have.\n"
        f"{BOOKING_LINK}\n\n"
        "Once you grab a time, I'll send a quick confirmation before I head over."
    )


def build_booking_nudge() -> str:
    """
    The single follow-up for a lead who received the booking link and neither
    booked nor replied.

    No name: they are mid-conversation, so re-introducing them reads as a
    template. No enthusiasm, no emoji, no claims, no dollar figures, no
    urgency — this goes out roughly a day after a message that already
    explained the offer, and it is a reminder, not a second pitch.

    It also gives them an easy way out. A lead who says "not right now" is
    worth more than one who feels chased.
    """
    return (
        "Following up on that calendar link — still happy to come take a look "
        "if you want to grab a time.\n"
        f"{BOOKING_LINK}\n\n"
        "If the timing isn't right, just let me know."
    )


# ─────────────────────────────────────────────
#  OUTBOUND MESSAGE SANITISER  [FIX-6]
#
#  Last-line-of-defence: if Claude hallucinated
#  or if a stale .env value slipped through, this
#  catches ANY LeadConnector booking URL in the
#  outgoing text and replaces it with BOOKING_LINK
#  before the SMS is sent.
#
#  Called in two places:
#   1. michael_agent()    — before returning the reply
#   2. send_sms_via_ghl() — right before the HTTP call
# ─────────────────────────────────────────────

# Matches ALL LeadConnector widget URLs — any subdomain, any booking/widget path.
# The raw api.leadconnectorhq.com domain must never appear in customer-facing SMS.
# Every match is replaced with _CORRECT_BOOKING_URL (the clean public URL).
_BAD_BOOKING_URL_RE = re.compile(
    r'https?://[a-zA-Z0-9._-]*leadconnectorhq\.com/widget/[^\s"\'<>]*',
    re.IGNORECASE,
)


def sanitize_outbound_message(text: str, contact_id: str = "") -> str:
    """
    [FIX-6] Detect and replace any LeadConnector widget URL in outbound text.

    If a raw leadconnectorhq.com/widget URL is found it is replaced with the
    clean public booking URL and the replacement is logged. Idempotent — clean
    messages pass through at zero cost.
    """
    text = to_gsm7_safe(text, contact_id)

    if not _BAD_BOOKING_URL_RE.search(text):
        return text   # fast path — nothing to do

    # Use _CORRECT_BOOKING_URL (hardcoded), NOT BOOKING_LINK — if BOOKING_LINK
    # itself is wrong (stale .env, race condition) this still produces a safe message.
    fixed = _BAD_BOOKING_URL_RE.sub(_CORRECT_BOOKING_URL, text)
    tag   = f"[{contact_id}] " if contact_id else ""
    print(f"BOOKING_LINK_SANITIZED=true | contact={contact_id or '(unknown)'}")
    log.warning(
        f"{tag}[SANITIZE] ⚠️  Raw LeadConnector URL detected in outbound message — replaced with clean booking URL"
    )
    print(f"[SANITIZE] ⚠️  Raw LeadConnector URL found{' for ' + contact_id if contact_id else ''}!")
    print(f"[SANITIZE]    Before : {text[:200]!r}")
    print(f"[SANITIZE]    After  : {fixed[:200]!r}")
    return fixed


# ─────────────────────────────────────────────
#  DAILY MESSAGE LIMIT
# ─────────────────────────────────────────────

def within_daily_limit(state: dict) -> bool:
    today = datetime.now(tz=CENTRAL_TZ).strftime("%Y-%m-%d")
    if state["last_msg_date"] != today:
        state["msgs_today"]    = 0
        state["last_msg_date"] = today
    return state["msgs_today"] < MAX_DAILY_MSGS


def increment_message_count(state: dict):
    today = datetime.now(tz=CENTRAL_TZ).strftime("%Y-%m-%d")
    if state["last_msg_date"] != today:
        state["msgs_today"]    = 0
        state["last_msg_date"] = today
    state["msgs_today"] += 1


# ─────────────────────────────────────────────
#  CORE AGENT FUNCTION  [ADAPT-1,2,3]
# ─────────────────────────────────────────────

def michael_agent(contact_id: str, inbound_text: str, ghl_pipeline_stage: str = "",
                  ghl_tags: list | None = None, prior_outbound: str = "") -> Optional[str]:
    """
    Main entry point for processing inbound SMS from a lead.
    Returns Michael's reply string, or None if no reply should be sent.
    Never raises — all errors are caught internally.

    prior_outbound  [CONVERT-1] The last message WE sent, when the caller has
        it. Optional and defaults to "", in which case local conversation
        memory is used. It exists because the GHL nurture workflow sends
        messages this service never sees — the "put together the numbers /
        should I close this out" text being the one that mattered. Without
        it a "Yes please" has no antecedent and falls through to whatever
        qualification question the stage happens to point at.
    """
    try:
        state = get_state(contact_id)

        # ── Verbose state banner ───────────────────────────────────
        print(f"\n[AGENT] {'='*50}")
        print(f"[AGENT]  Contact   : {contact_id}")
        print(f"[AGENT]  Name      : {state.get('contact_name', '?')!r}")
        print(f"[AGENT]  Stage     : {state['stage']}")
        print(f"[AGENT]  Homeowner : {state.get('homeowner', 'unknown')}")
        print(f"[AGENT]  Bill      : {state.get('monthly_bill', 'unknown') or 'unknown'}")
        print(f"[AGENT]  Loc conf  : {state.get('location_confirmed', False)}")
        print(f"[AGENT]  Inbound   : {inbound_text!r}")
        print(f"[AGENT] {'='*50}")

        log.info(f"[{contact_id}] Stage={state['stage']} | Inbound: {inbound_text!r}")

        # ── Merge GHL tag facts into state ────────────────────────
        # Tags carry qualification details from previous conversations.
        # Apply only if the state doesn't already have that field set.
        if ghl_tags:
            _tag_facts = parse_qualification_tags(ghl_tags)
            if _tag_facts.get("homeowner") and state.get("homeowner") is None:
                state["homeowner"] = _tag_facts["homeowner"]
                print(f"[AGENT] 🏷  homeowner from tag: {_tag_facts['homeowner']}", flush=True)
            if _tag_facts.get("location_confirmed") and not state.get("location_confirmed"):
                state["location_confirmed"] = True
                print(f"[AGENT] 🏷  location_confirmed from tag", flush=True)
            for _tf_key in ("bill_range", "roof_type", "timeline"):
                if _tag_facts.get(_tf_key) and not state.get(_tf_key):
                    state[_tf_key] = _tag_facts[_tf_key]
                    print(f"[AGENT] 🏷  {_tf_key}={_tag_facts[_tf_key]!r} from tag", flush=True)

        # ══════════════════════════════════════════════════════════════
        #  michael_agent() HARD STOPS  [FIX-8/9/13]
        #
        #  Priority order — checked BEFORE Claude is ever called:
        #
        #  STOP 0: final_confirmation_sent=True → absolute None (flow done)
        #  STOP 1: bill_received=True           → absolute None (flow done)
        #  STOP 2: stage=DNC                    → absolute None
        #  STOP 3: STOP keyword                 → DNC reply, then None
        #  STOP 4: stage=DISQUALIFIED           → silent None
        #  STOP 5: daily limit                  → silent None
        #  STOP 6: appointment_booked=True or stage=BOOKED
        #           → booked-follow-up path (uses _build_booked_followup_prompt,
        #             never the qualification system prompt)
        #
        #  WHY THIS ORDER MATTERS:
        #  In v2.9 the only hard stop was stage=DNC. Everything else, including
        #  BOOKED contacts, went through the full qualification Claude call.
        #  This meant that if a booked contact's message reached michael_agent()
        #  with stale state (stage=SEND_BOOKING), Claude would output
        #  "[SEND_BOOKING]" and its own booking language — BEFORE the code-level
        #  guard had a chance to strip the URL.  The strip blocked the injected
        #  URL but NOT Claude's own "Based on what you shared..." text.
        #
        #  v3.0 fix: booked contacts NEVER reach build_system_prompt().
        # ══════════════════════════════════════════════════════════════

        # ── STOP 0 + 1: Flow complete — fall through to booked path ─────
        # Non-real webhooks and duplicate bill images are already blocked by
        # PRIORITY GUARDS 0/1 in inbound_webhook().  Any call that reaches this
        # point is a real text question from the homeowner after the booking/bill
        # flow completed.  Route it to the booked follow-up path (STOP 6) so
        # Michael can answer post-appointment questions (cost, roof, rescheduling).
        if state.get("final_confirmation_sent") or state.get("bill_received"):
            print(
                f"[AGENT] ℹ  POST-FLOW real text — "
                f"final_confirmation_sent={state.get('final_confirmation_sent')} | "
                f"bill_received={state.get('bill_received')} → continuing to booked follow-up path"
            )
            log.info(
                f"[{contact_id}] AGENT POST-FLOW: real text question after flow complete — "
                f"routing to booked follow-up (STOP 6)"
            )
            # fall through — STOP 6 (booked lockout) handles it with the right prompt

        # ── STOP 2: DNC ───────────────────────────────────────────
        if state["stage"] in (Stage.DNC,):
            print(f"[AGENT] ⛔ Stage={state['stage']} — no reply (hard stop)")
            return None

        # ── STOP 3: STOP keyword always wins ─────────────────────
        if is_stop_request(inbound_text):
            reply = "You've been unsubscribed. You won't hear from us again."
            state["stage"] = Stage.DNC
            save_state(contact_id, state)
            log.info(f"[{contact_id}] DNC triggered by STOP keyword.")
            return reply

        # ── STOP 4: Already disqualified ─────────────────────────
        if state["stage"] == Stage.DISQUALIFIED:
            print(f"[AGENT] ⛔ DISQUALIFIED — silent (no reply)")
            return None

        # ── STOP 5: Daily message limit ───────────────────────────
        if not within_daily_limit(state):
            print(f"[AGENT] ⛔ Daily limit reached ({state.get('msgs_today')}/{MAX_DAILY_MSGS}) — no reply")
            return None

        # ── STOP 6: BOOKED LOCKOUT  [FIX-8] ──────────────────────
        # A booked contact must NEVER be routed through the qualification
        # Claude call (build_system_prompt).  That prompt contains
        # [SEND_BOOKING] instructions and qualification language that Claude
        # will include in its reply even when appointment_booked=True.
        #
        # Instead, use _build_booked_followup_prompt() — a minimal locked-down
        # prompt that explicitly forbids booking links, qualification questions,
        # and control tags.  Claude can still respond to "Ok thanks" / "What
        # time?" naturally, but it CANNOT generate qualification or booking text.
        #
        # This fires for:
        #   • stage=BOOKED (normal case)
        #   • appointment_booked=True with stale/wrong stage (race condition guard)
        #   • Any path that somehow calls michael_agent() for a booked contact
        _is_booked_contact = (
            state.get("appointment_booked") or
            state["stage"] == Stage.BOOKED or
            ghl_pipeline_stage in GHL_BOOKED_STAGES
        )
        if ghl_pipeline_stage in GHL_BOOKED_STAGES and not state.get("appointment_booked"):
            print(
                f"[AGENT] 🔒 GHL stage={ghl_pipeline_stage!r} → BOOKED LOCKOUT "
                f"(overriding internal stage={state['stage']})",
                flush=True,
            )
        if _is_booked_contact:
            print(
                f"\n[AGENT] 🔒 BOOKED LOCKOUT ENGAGED — "
                f"stage={state['stage']} | appointment_booked={state.get('appointment_booked')}"
                f"\n[AGENT]    Using _build_booked_followup_prompt() — qualification/booking PERMANENTLY DISABLED"
            )
            log.info(
                f"[{contact_id}] BOOKED LOCKOUT — using booked follow-up prompt "
                f"(stage={state['stage']}, apt_booked={state.get('appointment_booked')})"
            )

            # Booking confirmation shortcut (still valid for booked contacts)
            if is_booking_confirmation(inbound_text) and state["stage"] != Stage.BOOKED:
                _bc_reply = "You're all set — see you then! 👍"
                state["stage"] = Stage.BOOKED
                increment_message_count(state)
                save_state(contact_id, state)
                log.info(f"[{contact_id}] Booking confirmation (booked lockout path).")
                return _bc_reply

            # Build minimal booked-follow-up system prompt — qualification is UNREACHABLE
            booked_system = _build_booked_followup_prompt(state, ghl_pipeline_stage=ghl_pipeline_stage)

            # Append message to history — guard against concurrent-webhook race
            # where the same message was already appended by a prior webhook call.
            _last_msg = state["messages"][-1] if state["messages"] else {}
            if not (_last_msg.get("role") == "user" and _last_msg.get("content") == inbound_text):
                state["messages"].append({"role": "user", "content": inbound_text})
            msgs_for_api = state["messages"]
            if msgs_for_api and msgs_for_api[0].get("role") == "assistant":
                msgs_for_api = [{"role": "user", "content": "[Lead has an appointment booked]"}] + msgs_for_api

            try:
                _booked_resp = claude.messages.create(
                    model      = MODEL,
                    max_tokens = 200,   # booked replies are always short
                    system     = booked_system,
                    messages   = msgs_for_api,
                )
                _booked_raw = _booked_resp.content[0].text.strip()
                print(f"[AGENT] (booked path) Claude raw: {_booked_raw!r}")
            except Exception as _booked_err:
                log.error(f"[{contact_id}] Booked follow-up Claude error: {_booked_err}")
                print(f"[AGENT] ❌ Booked follow-up Claude error — returning None")
                state["messages"].pop()
                save_state(contact_id, state)
                return None

            # [FIX-13] Strip ALL qualification/booking tags regardless — they
            # must never appear in booked follow-up replies.  DO NOT call
            # build_booking_message() or inject any URL for any reason.
            _booked_clean, _booked_tag = extract_control_tag(_booked_raw)

            if _booked_tag in (Stage.SEND_BOOKING, Stage.DISQUALIFIED):
                # Claude hallucinated a qualification tag — strip and ignore the tag
                print(
                    f"[AGENT] 🔒 [FIX-13] Booked path — suppressed Claude tag {_booked_tag!r} "
                    f"(qualification tags are unreachable for booked contacts)"
                )
                log.warning(
                    f"[{contact_id}] Booked path: Claude emitted forbidden tag {_booked_tag!r} — stripped"
                )
                _booked_tag = None   # do NOT apply the tag

            # Apply safe tags (BOOKED, DNC) if Claude emitted them
            if _booked_tag == Stage.BOOKED:
                state["stage"] = Stage.BOOKED
            elif _booked_tag == Stage.DNC:
                state["stage"] = Stage.DNC

            _booked_clean = sanitize_outbound_message(_booked_clean, contact_id)
            state["messages"].append({"role": "assistant", "content": _booked_clean})
            increment_message_count(state)
            save_state(contact_id, state)

            print(f"[AGENT] ✅ (booked path) Reply: {_booked_clean!r}")
            return _booked_clean

        # ════════════════════════════════════════════════════════════
        #  BELOW THIS LINE: UNBOOKED CONTACTS ONLY (qualification flow)
        #  If execution reaches here, appointment_booked=False and
        #  stage is NOT BOOKED.  All booked contacts exited above.
        # ════════════════════════════════════════════════════════════

        # ── Booking confirmation shortcut ─────────────────────────
        if state["stage"] == Stage.SEND_BOOKING and is_booking_confirmation(inbound_text):
            reply = "You're all set. Your advisor will walk through everything when you meet. Talk soon."
            state["stage"] = Stage.BOOKED
            increment_message_count(state)
            save_state(contact_id, state)
            log.info(f"[{contact_id}] Booking confirmed via keyword shortcut.")
            return reply

        # ══════════════════════════════════════════════════════════════
        #  CONVERSION-FIRST DECISION STEP  [CONVERT-1]
        #
        #  Runs AFTER every hard stop above — STOP/DNC, disqualified, daily
        #  limit and the booked lockout have all already returned. Nothing
        #  here can reach a booked or opted-out contact, by position.
        #
        #  Runs BEFORE qualification so a homeowner who has just given
        #  permission to move forward is taken up on it instead of being
        #  walked back through questions. What a "yes" MEANS is decided from
        #  the previous outbound message, not from the stage — see
        #  decide_conversion_action().
        # ══════════════════════════════════════════════════════════════
        _prior_out = (prior_outbound or "").strip() or last_outbound_message(state)
        if _prior_out and not state.get("last_outbound_text"):
            state["last_outbound_text"] = _prior_out
        _prev_type = classify_outbound_intent(_prior_out)
        state["last_outbound_intent"] = _prev_type

        # ── Parse inbound for state signals [ADAPT-2] ─────────────
        # The original parser runs FIRST and untouched. It takes its own
        # _loc_conf_before snapshot to stop one "yes" confirming two separate
        # questions, and writing to the record before it would corrupt that.
        update_state_from_inbound(state, inbound_text, previous_outbound_type=_prev_type)

        # ── [BUNDLE-1] Update the qualification record ─────────────
        # Sync, extract, apply, sync again: the first sync pulls in anything
        # the legacy parser just learned, the second pushes the record back
        # out to the fields tags and stage-restore read.
        sync_qualification_fields(state)
        _facts = extract_qualification_facts(inbound_text, previous_outbound_type=_prev_type)
        apply_qualification_facts(state, _facts, contact_id)
        sync_qualification_fields(state)
        _verdict, _missing = qualification_verdict(state)
        print(f"[QUAL] {qualification_summary(state)}", flush=True)

        _decision = decide_conversion_action(state, inbound_text, prior_outbound=_prior_out)
        _ask_only = _decision.missing_field if _decision.action == ACT_ASK_MISSING else ""

        ev(
            "CONVERSION_DECISION", contact_id,
            intent=_decision.intent,
            previous_outbound_type=_decision.previous_outbound_type,
            action=_decision.action,
            reason=_decision.reason,
            stage=str(state["stage"]),
            verdict=_verdict,
            missing=",".join(_missing) or "(none)",
            homeowner=state.get("q_homeowner"),
            utility=state.get("q_utility"),
            bill_100_plus=state.get("q_bill_100_plus"),
        )
        print(
            f"[AGENT] 🎯 intent={_decision.intent} | "
            f"previous_outbound_type={_decision.previous_outbound_type} | "
            f"action={_decision.action} | why={_decision.reason}",
            flush=True,
        )

        # ── HIGH-INTENT CONVERSION PATH ───────────────────────────
        # A short, human lead-in plus the booking link, and nothing else.
        # Claude never sees the qualification prompt on this path, so it
        # cannot emit a qualification question even if it wanted to.
        if _decision.action == ACT_SEND_BOOKING:
            _conv_reply = build_conversion_reply(
                contact_id, state, _decision,
                prior_outbound=_prior_out,
                inbound_text=inbound_text,
            )
            state["stage"] = Stage.SEND_BOOKING
            state["booking_detected"] = True
            state["pending_question"] = "send_booking"
            # [BUNDLE-2] `qualified` means the three criteria are confirmed —
            # NOT that a link went out. Susan said yes to an invitation, which
            # is high intent and nothing more; the criteria travelled with the
            # link for her to self-qualify against. Setting the flag here would
            # have the record claim she told us things she never said.
            if _verdict == VERDICT_QUALIFIED:
                state["qualified"] = True
            state["messages"].append({"role": "user",      "content": inbound_text})
            state["messages"].append({"role": "assistant", "content": _conv_reply})
            state["last_outbound_text"] = _conv_reply
            increment_message_count(state)
            save_state(contact_id, state)
            ev(
                "CONVERSION_BOOKING_SENT", contact_id,
                intent=_decision.intent,
                previous_outbound_type=_decision.previous_outbound_type,
                action=_decision.action,
                verdict=_verdict,
                criteria_included=",".join(_missing) or "(none)",
            )
            print(f"[AGENT] ✅ (conversion path) Reply: {_conv_reply!r}", flush=True)
            return _conv_reply

        # ── Detect intent [FIX-5] ─────────────────────────────────
        intent    = detect_intent(inbound_text)
        qualified = state.get("qualified", False) or state["stage"] == Stage.SEND_BOOKING
        print(f"[AGENT] Intent    : {intent}")
        print(f"[AGENT] Qualified : {qualified}")
        log.info(f"[{contact_id}] Intent={intent} | Qualified={qualified}")

        # ── Short-circuit: answer cost / process questions directly ──
        # Bypass Claude entirely for common questions — faster, more consistent,
        # and guaranteed not to ignore the question and jump to booking.
        if intent == "cost_question":
            print(f"[AGENT] Path: COST_QUESTION — answering directly, skipping Claude")
            log.info(f"[{contact_id}] Path=COST_QUESTION (direct answer)")
            canned = build_cost_answer(state)
            state["messages"].append({"role": "user",      "content": inbound_text})
            state["messages"].append({"role": "assistant", "content": canned})
            increment_message_count(state)
            if qualified:
                # Don't re-send booking stage — they're still at SEND_BOOKING, just got Q answered
                pass
            save_state(contact_id, state)
            print(f"[AGENT] ✅ Cost answer: {canned!r}")
            return canned

        if intent == "process_question":
            print(f"[AGENT] Path: PROCESS_QUESTION — answering directly, skipping Claude")
            log.info(f"[{contact_id}] Path=PROCESS_QUESTION (direct answer)")
            canned = build_process_answer(state)
            state["messages"].append({"role": "user",      "content": inbound_text})
            state["messages"].append({"role": "assistant", "content": canned})
            increment_message_count(state)
            save_state(contact_id, state)
            print(f"[AGENT] ✅ Process answer: {canned!r}")
            return canned

        # ── Append inbound message to history ─────────────────────
        state["messages"].append({"role": "user", "content": inbound_text})

        # ── Defensive: ensure history starts with "user" ──────────
        # Anthropic API requires messages[0].role == "user".
        msgs_for_api = state["messages"]
        if msgs_for_api and msgs_for_api[0].get("role") == "assistant":
            log.warning(f"[{contact_id}] History starts with assistant — prepending synthetic user msg")
            print(f"[AGENT] ⚠ History starts with assistant — prepending synthetic user msg")
            msgs_for_api = [
                {"role": "user", "content": "[Lead submitted form on website]"}
            ] + msgs_for_api

        # ── Build dynamic, context-aware system prompt [ADAPT-1] ──
        # NOTE: build_system_prompt() is ONLY called for unbooked contacts.
        # Booked contacts exit via the BOOKED LOCKOUT above.
        system = build_system_prompt(state, ghl_pipeline_stage=ghl_pipeline_stage, ghl_tags=ghl_tags,
                                     prior_outbound=_prior_out, ask_only=_ask_only)
        _current_goal = _goal_from_prompt(system)
        print(f"[AGENT] Goal: {_current_goal}")

        # Track which qualification question we're about to ask (v3.6+ pending_question).
        # Persisted so the next inbound turn knows what "yes/no" is responding to.
        _pq = "none"
        if "BUNDLED QUALIFICATION" in _current_goal: _pq = "bundled"
        elif "ASK UTILITY" in _current_goal:   _pq = "utility"
        elif "ASK OWNERSHIP" in _current_goal: _pq = "ownership"
        elif "ASK BILL" in _current_goal:      _pq = "bill"
        elif "BOOKING" in _current_goal:       _pq = "send_booking"
        state["pending_question"] = _pq
        print(f"[AGENT] PendingQ  : {_pq}")
        print(f"[AGENT] Path: CLAUDE | intent={intent} | history={len(msgs_for_api)} msgs | first_role={msgs_for_api[0]['role'] if msgs_for_api else 'EMPTY'}")
        for i, m in enumerate(msgs_for_api[-6:]):   # log last 6 to keep console readable
            idx     = len(msgs_for_api) - min(6, len(msgs_for_api)) + i
            snippet = m["content"][:80].replace("\n", " ")
            print(f"[AGENT]   [{idx}] {m['role']}: {snippet!r}")

        # ── Call Claude ───────────────────────────────────────────
        try:
            response  = claude.messages.create(
                model     = MODEL,
                max_tokens= 400,
                system    = system,
                messages  = msgs_for_api,
            )
            raw_reply = response.content[0].text.strip()
            print(f"[AGENT] Claude raw reply: {raw_reply!r}")
        except Exception as claude_err:
            log.error(f"[{contact_id}] Claude API error: {claude_err}")
            print(f"[AGENT] ❌ Claude API error — no SMS will be sent")
            print(f"[ERROR] {traceback.format_exc()}")
            state["messages"].pop()
            save_state(contact_id, state)
            return None

        # ── Parse control tags from reply ─────────────────────────
        # extract_control_tag strips ALL tags (fix for [SEND_BOOKING] leaking)
        clean_reply, new_stage = extract_control_tag(raw_reply)

        # ── [FIX-4 + FIX-5] Smart SEND_BOOKING override ───────────
        # Claude is responsible ONLY for signalling [SEND_BOOKING] / [QUALIFIED].
        # The booking URL is ALWAYS injected by build_booking_message() — never from Claude.
        #
        # Smart behaviour:
        #   • If lead asked a general question (contains "?") AND Claude answered it
        #     before signalling SEND_BOOKING → preserve Claude's answer, append booking.
        #   • Otherwise (straight qualification answer) → replace entirely with clean
        #     code-generated booking message.
        #
        # ── [FIX-7] SEND_BOOKING RE-SEND GUARD (belt + suspenders) ──────
        # v3.0: This guard is now the SECONDARY layer.  Booked contacts NEVER
        # reach this code path because they exit via the BOOKED LOCKOUT above.
        # This guard is retained as defense-in-depth for edge cases where stage
        # is stale but appointment_booked was not yet set (race window < 1ms).
        if new_stage == Stage.SEND_BOOKING:
            _already_sent_booking = (
                state["stage"] in (Stage.SEND_BOOKING, Stage.BOOKED) or
                state.get("appointment_booked")
            )

            if _already_sent_booking:
                print(
                    f"[AGENT] 🔒 [SEND_BOOKING] suppressed (belt+suspenders guard) — "
                    f"stage={state['stage']} / appointment_booked={state.get('appointment_booked')}"
                )
                log.warning(
                    f"[{contact_id}] Blocked [SEND_BOOKING] re-send (secondary guard) "
                    f"(stage={state['stage']}, apt_booked={state.get('appointment_booked')})"
                )
                new_stage = None
            else:
                # ── NORMAL FIRST SEND ─────────────────────────────────────
                contact_name = state.get("contact_name", "")
                booking_msg  = build_booking_message(full_name=contact_name)

                if intent == "question" and clean_reply.strip():
                    clean_reply = f"{clean_reply.strip()}\n\n{booking_msg}"
                    print(f"[AGENT] 📅 SEND_BOOKING (question path) — appending booking to Claude's answer")
                else:
                    clean_reply = booking_msg
                    print(f"[AGENT] 📅 SEND_BOOKING (answer path) — replacing with code-generated booking message")

                print(f"[AGENT] 📅 Booking message:\n{clean_reply}")

        # ── Update stage ──────────────────────────────────────────
        if new_stage:
            state["stage"] = new_stage
            print(f"[AGENT] Stage → {new_stage} (from control tag)")
            log.info(f"[{contact_id}] Stage updated to: {new_stage}")
        elif state["stage"] == Stage.INITIAL:
            # Advance to the correct next stage based on what was confirmed this turn.
            # If utility/area is now confirmed (loc_conf was just set or was already True),
            # go to ASK_OWNERSHIP; otherwise go to ASK_LOCATION so the next turn's
            # system prompt knows the utility question is still pending.
            state["stage"] = Stage.ASK_OWNERSHIP if state.get("location_confirmed") else Stage.ASK_LOCATION

        # ── Infer confirmed facts from stage transitions [ADAPT-3] ─
        # Only stages that genuinely come AFTER the ownership question may
        # imply ownership.
        #
        # [BUNDLE-3] ASK_LOCATION used to be in this list, from the flow where
        # ownership was asked before location. The order was later reversed —
        # utility/service area is now the first question — which made
        # ASK_LOCATION mean the exact opposite: the utility question is still
        # pending, so ownership has NOT been asked yet. The stale entry
        # silently set homeowner="yes" for anyone sitting on that first
        # question, and build_system_prompt() then printed "HOMEOWNER:
        # confirmed", so the question was never asked and renters could reach
        # the booking link unqualified. build_system_prompt()'s own confirmed
        # block had already been corrected; this inference had not.
        if state["stage"] in (Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED):
            if state.get("homeowner") is None:
                state["homeowner"] = "yes"
        # If we advanced past location, area is confirmed.
        if state["stage"] in (Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED):
            state["location_confirmed"] = True
        # Track qualified flag
        if state["stage"] in (Stage.SEND_BOOKING, Stage.BOOKED):
            state["qualified"] = True

        # [BUNDLE-3] Reconcile the record with whatever the stage transition
        # just decided, in the same turn rather than on the next inbound.
        # Without this the legacy fields and the record disagree until the
        # homeowner replies again, and the prompt is built from the stale half.
        sync_qualification_fields(state)

        # ── [FIX-6] Sanitise before sending — catch any hallucinated URL ──
        clean_reply = sanitize_outbound_message(clean_reply, contact_id)

        # ── Append assistant reply to history ─────────────────────
        state["messages"].append({"role": "assistant", "content": clean_reply})
        state["last_outbound_text"] = clean_reply
        if is_bundled_qualification_message(clean_reply):
            state["bundled_offer_sent"] = True
            ev("BUNDLED_OFFER_SENT", contact_id, criteria=bundled_criteria_mentioned(clean_reply))
        increment_message_count(state)
        save_state(contact_id, state)

        print(f"[AGENT] ✅ Reply: {clean_reply!r}")
        return clean_reply

    except Exception as unhandled:
        log.error(f"[{contact_id}] Unhandled exception in michael_agent: {unhandled}")
        print(f"[ERROR] Unhandled michael_agent exception:\n{traceback.format_exc()}")
        return None


# ─────────────────────────────────────────────
#  GHL API HELPERS
# ─────────────────────────────────────────────

async def fetch_ghl_contact_first_name(contact_id: str) -> str:
    """
    Fetch a contact's first name from GHL when the webhook payload doesn't include it.
    Common in Customer Replied webhooks which often omit contact name fields.
    Returns the first name string, or "" if the lookup fails or name is invalid.
    """
    if not contact_id:
        return ""
    url = f"{GHL_API_BASE}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        "Version"      : "2021-07-28",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, headers=headers)
        if r.is_success:
            contact = r.json().get("contact") or r.json()
            name = (contact.get("firstName") or "").strip()
            if name and name.lower() not in _INVALID_NAMES:
                print(f"[NAME-LOOKUP] 👤 First name resolved from GHL: {name!r} for contact_id={contact_id!r}")
                return name
            print(f"[NAME-LOOKUP] ℹ  GHL contact has no usable first name for contact_id={contact_id!r}")
            return ""
        print(f"[NAME-LOOKUP] ⚠  GHL contact lookup failed: status={r.status_code} for contact_id={contact_id!r}")
        return ""
    except Exception as _name_err:
        print(f"[NAME-LOOKUP] ⚠  GHL contact lookup exception: {_name_err}")
        return ""


async def fetch_ghl_contact_phone(contact_id: str) -> str:
    """
    Look up a contact's phone number from GHL when it is not available in the
    webhook payload.  Used by send_sms_via_ghl() as a fallback so that toNumber
    is always present in the outbound SMS payload.

    Returns the phone in E.164 format, or "" if the lookup fails.
    """
    if not contact_id:
        return ""
    url = f"{GHL_API_BASE}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        "Version"      : "2021-07-28",
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=headers)
        if r.is_success:
            data = r.json()
            # GHL wraps in {"contact": {...}} or returns the contact directly
            contact = data.get("contact") or data
            phone = (
                contact.get("phone") or
                contact.get("mobilePhone") or
                contact.get("homePhone") or
                ""
            )
            phone = str(phone).strip() if phone else ""
            print(f"[SMS-LOOKUP] 📞 Contact phone resolved from GHL: {phone!r} for contact_id={contact_id!r}")
            log.info(f"[{contact_id}] GHL contact phone lookup: {phone!r}")
            return phone
        else:
            print(f"[SMS-LOOKUP] ⚠  GHL contact lookup failed: status={r.status_code} body={r.text[:200]!r}")
            log.warning(f"[{contact_id}] GHL contact lookup failed: status={r.status_code}")
            return ""
    except Exception as _lookup_err:
        print(f"[SMS-LOOKUP] ⚠  GHL contact lookup exception: {_lookup_err}")
        log.warning(f"[{contact_id}] GHL contact lookup exception: {_lookup_err}")
        return ""


async def fetch_ghl_opportunity_stage(contact_id: str) -> str:
    """
    Fetch the current GHL pipeline stage name for a contact.
    Returns the stage name string (e.g. "Solar Savings Report Scheduled"),
    or "" if no opportunity exists or the lookup fails.

    This is the PRIMARY routing signal — called before every michael_agent() invocation.
    """
    if not contact_id:
        return ""
    url = f"{GHL_API_BASE}/opportunities/search"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        "Version"      : "2021-07-28",
    }
    params = {
        "location_id": GHL_LOCATION_ID,
        "contact_id" : contact_id,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, headers=headers, params=params)
        if r.is_success:
            data          = r.json()
            opportunities = data.get("opportunities") or []
            if not opportunities:
                print(f"[STAGE-LOOKUP] ℹ  No opportunities for contact={contact_id!r}", flush=True)
                return ""
            opp        = opportunities[0]
            stage_name = (
                (opp.get("pipelineStage") or {}).get("name") or
                opp.get("stageName") or
                opp.get("stage_name") or
                ""
            )
            stage_name = stage_name.strip()
            print(f"[STAGE-LOOKUP] 📊 GHL stage={stage_name!r} | contact={contact_id!r}", flush=True)
            return stage_name
        print(f"[STAGE-LOOKUP] ⚠  Opportunity lookup failed: status={r.status_code}", flush=True)
        return ""
    except Exception as _e:
        print(f"[STAGE-LOOKUP] ⚠  Opportunity lookup exception: {_e}", flush=True)
        return ""


# ═════════════════════════════════════════════════════════════════════════
#  [BOOKED-GUARD] AUTHORITATIVE BOOKED-STATE CHECK
#
#  THE INVARIANT
#    Once a contact has a booked appointment, no outreach, qualification,
#    nurture or booking-pitch SMS may ever leave this service for them.
#
#  WHY THIS EXISTS
#    A contact booked at 07:11 received a fresh-lead qualification SMS at
#    08:31. Routing had judged "booked" from THREE untrustworthy sources:
#      • webhook payload tags   — absent from the payload entirely
#      • process memory         — wiped by any restart
#      • [FIX-4.A]              — actively discarded booked evidence for
#                                 form-shaped payloads
#    All three are controlled by the caller or by chance. None is authoritative.
#
#  THE RULE
#    Immediately before every guarded send, ask GHL directly. A guarded SMS
#    may leave ONLY on an affirmative "confirmed not booked". Booked, or any
#    inability to determine the answer, suppresses the send.
#
#  THREE INDEPENDENT SOURCES — a positive from any one is decisive
#    1. Contact tags        GET /contacts/{id}
#    2. Opportunity stage   GET /opportunities/search   (GHL_BOOKED_STAGES)
#    3. Appointments        GET /contacts/{id}/appointments
#
#    Source 3 is not redundant. The Vercel booking route creates the
#    appointment via POST /calendars/events/appointments and applies NO tag;
#    the tag arrives later from a GHL workflow. Between those two moments a
#    real appointment exists while sources 1 and 2 are both still clean.
#
#  NO CACHING. Every guarded send performs a fresh lookup. A cache would
#  reintroduce exactly the staleness this guard exists to eliminate.
# ═════════════════════════════════════════════════════════════════════════

_GHL_HEADERS_V2 = {"Version": "2021-07-28"}
_GHL_HEADERS_V1 = {"Version": "2021-04-15"}


def _ghl_headers(version: dict) -> dict:
    return {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        **version,
    }


# Appointment statuses that mean the appointment is NOT active.
# Anything not listed here, with a future start time, counts as booked —
# an unrecognised status must never be read as "free to text them".
_APPT_DEAD_STATUSES: frozenset = frozenset({
    "cancelled", "canceled", "invalid", "deleted", "removed",
    "noshow", "no-show", "no_show",
})

# Keys GHL may use for the appointment list / status / start time.
_APPT_LIST_KEYS  = ("appointments", "events", "data", "items")
_APPT_STATUS_KEYS = ("appointmentStatus", "status", "appointment_status")
_APPT_START_KEYS  = ("startTime", "startAt", "start_time", "start")


# ── [CONVERT-1] Last outbound message recovery ───────────────────────────
#
#  WHY
#    Nurture and breakup texts are sent by GHL workflows. They never pass
#    through this service, so state["messages"] cannot contain them, and a
#    restart empties that list for everything else too. Without the text of
#    the question, an inbound "Yes please" is uninterpretable and the agent
#    falls back to whatever the stage points at — which is how a lead who
#    said yes to "want me to put together the numbers?" got asked "Are you
#    the homeowner?".
#
#  BEST EFFORT, ALWAYS
#    Every failure mode returns "" and the caller proceeds exactly as it did
#    before this function existed. It is called only when local memory has no
#    outbound message AND the inbound looks like an affirmative or a
#    scheduling request, so it adds no latency to the common path.
_MSG_LIST_KEYS = ("messages", "data", "items")
_MSG_BODY_KEYS = ("body", "message", "text")


def _extract_outbound_body(messages: list) -> str:
    """Newest outbound SMS body from a GHL messages list, or ''."""
    for m in messages or []:
        if not isinstance(m, dict):
            continue
        direction = str(m.get("direction") or m.get("messageDirection") or "").lower()
        if direction and direction != "outbound":
            continue
        for k in _MSG_BODY_KEYS:
            body = m.get(k)
            if isinstance(body, str) and body.strip():
                return body.strip()
    return ""


async def fetch_last_outbound_message(contact_id: str) -> str:
    """
    The most recent outbound SMS body GHL has for this contact, or "".

    Never raises and never blocks a reply: any error, timeout, unexpected
    shape or missing credential yields "".
    """
    if not contact_id or not GHL_API_KEY or not GHL_LOCATION_ID:
        return ""
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.post(
                f"{GHL_API_BASE}/conversations/search",
                headers=_ghl_headers(_GHL_HEADERS_V2),
                json={"locationId": GHL_LOCATION_ID, "contactId": contact_id, "limit": 1},
            )
            if not r.is_success:
                r = await client.get(
                    f"{GHL_API_BASE}/conversations/search",
                    headers=_ghl_headers(_GHL_HEADERS_V2),
                    params={"locationId": GHL_LOCATION_ID, "contactId": contact_id, "limit": 1},
                )
            if not r.is_success:
                ev("PRIOR_OUTBOUND_LOOKUP_FAILED", contact_id, step="conversations", code=r.status_code)
                return ""

            data  = r.json() if r.content else {}
            convs = (data or {}).get("conversations") or (data or {}).get("data") or []
            if not convs or not isinstance(convs[0], dict):
                return ""
            conv_id = str(convs[0].get("id") or convs[0].get("_id") or "").strip()
            if not conv_id:
                return ""

            rm = await client.get(
                f"{GHL_API_BASE}/conversations/{conv_id}/messages",
                headers=_ghl_headers(_GHL_HEADERS_V2),
                params={"limit": 20},
            )
            if not rm.is_success:
                ev("PRIOR_OUTBOUND_LOOKUP_FAILED", contact_id, step="messages", code=rm.status_code)
                return ""

            payload = rm.json() if rm.content else {}
            bucket  = (payload or {}).get("messages") or payload or {}
            if isinstance(bucket, dict):
                msgs = next((bucket[k] for k in _MSG_LIST_KEYS if isinstance(bucket.get(k), list)), [])
            else:
                msgs = bucket if isinstance(bucket, list) else []

        body = _extract_outbound_body(msgs)
        if body:
            ev("PRIOR_OUTBOUND_RECOVERED", contact_id,
               classified=classify_outbound_intent(body), chars=len(body))
        return body
    except Exception as err:
        log.warning(f"[{contact_id}] last-outbound lookup failed (non-fatal): {err}")
        ev("PRIOR_OUTBOUND_LOOKUP_FAILED", contact_id, step="exception", why=str(err)[:80])
        return ""


async def _fetch_contact_tags_ex(contact_id: str) -> tuple[bool, list[str]]:
    """(ok, tags). ok=False means the lookup could not be trusted."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{GHL_API_BASE}/contacts/{contact_id}",
                                 headers=_ghl_headers(_GHL_HEADERS_V2))
        if not r.is_success:
            return False, []
        data    = r.json()
        contact = data.get("contact") or data
        if not isinstance(contact, dict):
            return False, []
        return True, [str(t) for t in (contact.get("tags") or [])]
    except Exception as err:
        log.warning(f"[{contact_id}] booked-guard: tags lookup failed: {err}")
        return False, []


async def _fetch_opportunity_stage_ex(contact_id: str) -> tuple[bool, str]:
    """
    (ok, stage_name). ok=True with "" means the contact genuinely has no
    opportunity — distinct from a failed lookup, which returns ok=False.
    That distinction is the whole point: the pre-existing
    fetch_ghl_opportunity_stage() collapses both cases to "".
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                f"{GHL_API_BASE}/opportunities/search",
                headers=_ghl_headers(_GHL_HEADERS_V2),
                params={"location_id": GHL_LOCATION_ID, "contact_id": contact_id},
            )
        if not r.is_success:
            return False, ""
        data = r.json()
        if not isinstance(data, dict):
            return False, ""
        opportunities = data.get("opportunities") or []
        if not opportunities:
            return True, ""          # no opportunity — a real, clean answer
        opp = opportunities[0]
        stage = (
            (opp.get("pipelineStage") or {}).get("name") or
            opp.get("stageName") or opp.get("stage_name") or ""
        )
        return True, str(stage).strip()
    except Exception as err:
        log.warning(f"[{contact_id}] booked-guard: opportunity lookup failed: {err}")
        return False, ""


def _appointment_is_active(appt: dict, now_utc: datetime) -> bool:
    """
    An appointment counts as booked when it is not cancelled AND starts in
    the future.

    Deliberately asymmetric: a status we do not recognise counts as ACTIVE.
    Misreading an unknown status as free-to-text violates the invariant;
    misreading it as booked only costs one suppressed message.
    """
    status = ""
    for k in _APPT_STATUS_KEYS:
        if appt.get(k):
            status = str(appt[k]).lower().strip().replace(" ", "")
            break
    if status in _APPT_DEAD_STATUSES:
        return False

    raw_start = ""
    for k in _APPT_START_KEYS:
        if appt.get(k):
            raw_start = str(appt[k])
            break
    if not raw_start:
        # Active status but no parseable start time — cannot prove it is past,
        # so treat as active.
        return True
    try:
        txt = raw_start.replace("Z", "+00:00")
        dt  = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt > now_utc
    except Exception:
        return True          # unparseable → assume active


async def _fetch_appointments_ex(contact_id: str) -> tuple[bool, bool, str]:
    """
    (ok, has_active_future_appointment, detail).

    A 200 whose body contains no recognisable list container returns ok=False.
    Reading an unfamiliar response shape as "zero appointments" would be a
    silent false negative — the most dangerous outcome available here.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{GHL_API_BASE}/contacts/{contact_id}/appointments",
                                 headers=_ghl_headers(_GHL_HEADERS_V1))
        if not r.is_success:
            return False, False, f"http_{r.status_code}"
        data = r.json()

        appts = None
        if isinstance(data, list):
            appts = data
        elif isinstance(data, dict):
            for k in _APPT_LIST_KEYS:
                if isinstance(data.get(k), list):
                    appts = data[k]
                    break
        if appts is None:
            return False, False, "unrecognised_response_shape"

        now_utc = datetime.now(timezone.utc)
        active  = [a for a in appts if isinstance(a, dict)
                   and _appointment_is_active(a, now_utc)]
        return True, bool(active), f"total={len(appts)} active_future={len(active)}"
    except Exception as err:
        log.warning(f"[{contact_id}] booked-guard: appointments lookup failed: {err}")
        return False, False, f"exception:{type(err).__name__}"


async def booked_verdict(contact_id: str) -> tuple[Optional[bool], str]:
    """
    Authoritative booked check, straight from GHL. Never raises.

      True  — booked. Suppress guarded sends.
      False — CONFIRMED not booked. All three sources answered and were clean.
      None  — indeterminate. Suppress guarded sends (fail safe).

    Reads no webhook payload and no process memory.
    """
    if not contact_id:
        return None, "no_contact_id"
    if not GHL_API_KEY:
        return None, "no_api_key"

    results = await asyncio.gather(
        _fetch_contact_tags_ex(contact_id),
        _fetch_opportunity_stage_ex(contact_id),
        _fetch_appointments_ex(contact_id),
        return_exceptions=True,
    )
    tags_res, stage_res, appt_res = results

    tags_ok,  tags        = (False, [])   if isinstance(tags_res,  BaseException) else tags_res
    stage_ok, stage       = (False, "")   if isinstance(stage_res, BaseException) else stage_res
    if isinstance(appt_res, BaseException):
        appt_ok, has_appt, appt_detail = False, False, "exception"
    else:
        appt_ok, has_appt, appt_detail = appt_res

    # ── Positive evidence: any single source is decisive ──────────────
    positives: list[str] = []
    if tags_ok and is_already_booked(tags):
        positives.append("tag")
    if stage_ok and stage in GHL_BOOKED_STAGES:
        positives.append(f"stage={stage!r}")
    if appt_ok and has_appt:
        positives.append(f"appointment({appt_detail})")
    if positives:
        return True, " + ".join(positives)

    # ── Negative requires ALL THREE to have answered ──────────────────
    if tags_ok and stage_ok and appt_ok:
        _stage_txt = stage or "none"
        return False, f"clean(tags={len(tags)}, stage={_stage_txt!r}, {appt_detail})"

    return None, (f"indeterminate(tags_ok={tags_ok}, stage_ok={stage_ok}, "
                  f"appt_ok={appt_ok}, appt={appt_detail})")


def _snapshot_conversation(state: dict) -> dict:
    """
    [DELIVERY] Capture the fields a send would advance, so they can be undone
    when the message never actually left.

    State is still advanced BEFORE the await (the original race fix: a second
    concurrent webhook must see the new stage). This snapshot only makes that
    advance reversible.
    """
    return {
        "stage"             : state.get("stage"),
        "messages"          : list(state.get("messages") or []),
        "msgs_today"        : state.get("msgs_today", 0),
        "location_confirmed": state.get("location_confirmed", False),
        "entry_path"        : state.get("entry_path", "unknown"),
    }


def _rollback_conversation(contact_id: str, state: dict, snap: dict,
                           result: dict, what: str) -> None:
    """
    [DELIVERY] Undo an advance whose message never left.

    Applied ONLY for SUPPRESSED and REJECTED — never for ACCEPTED. An accepted
    message is in flight and will usually arrive; rewinding it would re-ask the
    same question when it does.
    """
    state.update(snap)
    state["last_send_status"] = result.get("status", SendStatus.REJECTED.value)
    save_state(contact_id, state)
    ev(
        "SEND_NOT_DELIVERED_STATE_ROLLED_BACK", contact_id,
        what=what,
        status=result.get("status"),
        reason=result.get("reason") or result.get("why") or "(none)",
        restored_stage=str(snap.get("stage")),
    )


def _record_send_result(contact_id: str, state: dict, result: dict) -> None:
    """[DELIVERY] Persist the outcome of an accepted send."""
    state["last_send_status"] = result.get("status", SendStatus.ACCEPTED.value)
    state["last_message_id"]  = result.get("message_id", "") or ""
    save_state(contact_id, state)


# ═════════════════════════════════════════════════════════════════════════
#  [STAGE-RESTORE] Durable stage recovery from GHL evidence
#
#  WHY
#    _state_store is process memory and is wiped by every restart and deploy.
#    A contact who had completed qualification and received the booking link
#    replied "Ok" the next morning, was read back as Stage.INITIAL, and was
#    asked the ownership question again. He replied STOP.
#
#    The recovery branch that handled this assumed "engaged but no state"
#    meant mid-qualification and hardcoded Stage.ASK_OWNERSHIP. It had no way
#    to know the contact had progressed further.
#
#    The evidence was in GHL the whole time. resolve_ghl_tags() writes
#    QUALIFIED and BOOKING_LINK_SENT when the booking link goes out, and
#    fetch_ghl_opportunity_stage() is already called on every inbound. Neither
#    was ever read back — ghl_stage_to_internal() was dead code.
#
#  RULE
#    Return the FURTHEST stage the contact has durably reached. Ordered
#    furthest-first, first match wins, so a contact can never be walked
#    backwards into a question they have already answered.
# ═════════════════════════════════════════════════════════════════════════

# GHL contact tags written by resolve_ghl_tags() when the booking link is sent.
_TAGS_QUALIFIED: tuple = ("BOOKING_LINK_SENT", "QUALIFIED")
_TAGS_DISQUALIFIED: tuple = ("NOT_QUALIFIED",)


def restore_stage_from_ghl(tags: list | None,
                           ghl_stage: str = "") -> tuple[Optional[Stage], str]:
    """
    (stage, why) — the furthest stage supported by durable GHL evidence,
    or (None, reason) when there is none. Reads only GHL data, never memory.
    """
    if has_tag(tags, TAG_DNC):
        return Stage.DNC, "tag:solar-dnc"
    if is_already_booked(list(tags or [])):
        return Stage.BOOKED, "tag:booked"
    if ghl_stage and ghl_stage in GHL_BOOKED_STAGES:
        return Stage.BOOKED, f"pipeline:{ghl_stage}"

    if any(has_tag(tags, t) for t in _TAGS_QUALIFIED):
        return Stage.SEND_BOOKING, "tag:qualified/booking_link_sent"

    # ghl_stage_to_internal() existed but was never called. "Qualified" maps
    # to SEND_BOOKING, which is exactly the evidence that was being ignored.
    _mapped = ghl_stage_to_internal(ghl_stage)
    if _mapped in (Stage.SEND_BOOKING, Stage.BOOKED, Stage.DISQUALIFIED):
        return _mapped, f"pipeline:{ghl_stage}"

    if has_tag(tags, TAG_DISQUALIFIED) or any(has_tag(tags, t) for t in _TAGS_DISQUALIFIED):
        return Stage.DISQUALIFIED, "tag:disqualified"

    # Engaged, but no evidence of progress past qualification.
    if has_tag(tags, TAG_ENGAGED) or has_tag(tags, TAG_OUTREACH_SENT):
        return Stage.ASK_OWNERSHIP, "tag:engaged/outreach_sent"

    return None, "no_durable_evidence"


def apply_restored_stage(contact_id: str, state: dict,
                         tags: list | None, ghl_stage: str = "") -> str:
    """
    Set state["stage"] from durable GHL evidence. Returns the reason string.

    A contact restored to SEND_BOOKING or beyond has demonstrably answered the
    qualification questions, so the facts those questions establish are marked
    confirmed too — otherwise the prompt would re-ask them on the next turn.
    """
    _stage, _why = restore_stage_from_ghl(tags, ghl_stage)
    if _stage is None:
        _stage, _why = Stage.ASK_OWNERSHIP, "default:no_evidence"

    state["stage"] = _stage
    state["location_confirmed"] = True
    # [BUNDLE-2] BOOKING_LINK_SENT on its own means the link went out, which the
    # conversion path now does before the criteria are known. Only the explicit
    # QUALIFIED tag — or an actual booking — is evidence they answered.
    if _stage == Stage.BOOKED or (_stage == Stage.SEND_BOOKING and has_tag(tags, "QUALIFIED")):
        state["qualified"] = True
        if not state.get("homeowner"):
            state["homeowner"] = "yes"
    if _stage == Stage.SEND_BOOKING:
        state["booking_detected"] = True

    # [BUNDLE-2] Reconcile the record with whatever the restore just decided,
    # so the next prompt reflects it without waiting for an inbound message.
    sync_qualification_fields(state)

    ev("STAGE_RESTORED_FROM_GHL", contact_id, stage=str(_stage), why=_why,
       qualification=qualification_summary(state))
    return _why


def classify_reply_kind(state: dict) -> SendKind:
    """
    The generic agent-reply path carries qualification replies, booked-contact
    replies and STOP confirmations, so its class must be derived per message.
    """
    if state.get("stage") == Stage.DNC:
        return SendKind.OPT_OUT
    if state.get("appointment_booked") or state.get("stage") == Stage.BOOKED:
        return SendKind.BOOKED_REPLY
    # [CONVERT-1] A reply sent at SEND_BOOKING carries the booking link, so it
    # is a booking pitch. Both classes sit in _BOOKED_GUARDED, so the booked
    # check is unchanged — this only makes the suppression log say which.
    if state.get("stage") == Stage.SEND_BOOKING:
        return SendKind.BOOKING_PITCH
    return SendKind.QUALIFICATION


async def send_sms_via_ghl(contact_id: str, message: str, to_number: str = "",
                           *, kind: SendKind = SendKind.QUALIFICATION) -> dict:
    """
    Send an outbound SMS through GHL's Conversations API.
    Includes outbound dedup [ADAPT-6] to prevent duplicate sends.

    [FIX-4.B] toNumber is now ALWAYS included in the payload.
    If to_number is not provided or is empty, this function first attempts
    to resolve the contact's phone via GHL's Contacts API before sending.
    A missing toNumber is the most common cause of GHL returning 400.

    Returns a result dict — NEVER returns None:
      {
        "sent"         : bool,   # True = GHL accepted the request
        "deduped"      : bool,   # True = suppressed by outbound dedup
        "status_code"  : int|None,
        "response_body": dict|str|None,
      }
    Raises httpx.HTTPStatusError on 4xx/5xx GHL responses so callers
    can catch and decide whether to retry or abort.
    """
    # ══════════════════════════════════════════════════════════════
    #  [BOOKED-GUARD] Final authoritative check before the API call.
    #
    #  Runs AFTER outbound dedup (free, no network) and BEFORE the POST.
    #  This is the last point at which a message can be stopped, and the
    #  only one that consults GHL rather than the caller.
    #
    #  `kind` defaults to QUALIFICATION — the guarded value — so any call
    #  site added later without an explicit class is protected by default.
    # ══════════════════════════════════════════════════════════════
    if kind in _BOOKED_GUARDED:
        _verdict, _why = await booked_verdict(contact_id)
        if _verdict is not False:          # True (booked) OR None (unknown)
            ev(
                "SEND_SUPPRESSED_BOOKED", contact_id,
                kind=kind.value,
                verdict=("booked" if _verdict else "indeterminate"),
                why=_why,
            )
            return {
                "status"       : SendStatus.SUPPRESSED.value,
                "accepted"     : False,
                "delivered"    : None,
                "message_id"   : "",
                "sent"         : False,
                "suppressed"   : True,
                "reason"       : "booked_guard",
                "verdict"      : _verdict,
                "why"          : _why,
                "deduped"      : False,
                "status_code"  : None,
                "response_body": None,
            }
        ev("SEND_ALLOWED_NOT_BOOKED", contact_id, kind=kind.value, why=_why)

    # ══════════════════════════════════════════════════════════════
    #  [COMPLIANCE-GATE] Fail-closed DND check.
    #
    #  Runs immediately after the booked guard and BEFORE the window
    #  gate on purpose: a genuine opt-out is refused outright and never
    #  receives an after-hours hold tag. There is no bypass parameter.
    #  Applies to EVERY kind, including OPT_OUT confirmations - GHL
    #  itself sends the STOP confirmation, so we must not.
    # ══════════════════════════════════════════════════════════════
    _state_for_gate = get_state(contact_id)
    _gate = await can_contact_sms(contact_id, _state_for_gate)
    if not _gate["allowed"]:
        ev("SEND_SUPPRESSED_COMPLIANCE", contact_id, kind=kind.value,
           category=_gate["category"], code=_gate["code"] or "(none)",
           why=_gate["reason"])
        await record_sms_outcome(contact_id, _gate["category"], _gate["code"],
                                 _gate["permanent"], email=_gate.get("email", ""))
        return {
            "status"       : SendStatus.SUPPRESSED.value,
            "accepted"     : False,
            "delivered"    : None,
            "message_id"   : "",
            "sent"         : False,
            "suppressed"   : True,
            "reason"       : SUPPRESS_REASON_DND,
            "category"     : _gate["category"],
            "code"         : _gate["code"],
            "why"          : _gate["reason"],
            "deduped"      : False,
            "status_code"  : None,
            "response_body": None,
        }

    # Gate resolved an authoritative phone - use it when caller had none.
    if not (to_number or "").strip() and _gate.get("phone"):
        to_number = _gate["phone"]

    # ══════════════════════════════════════════════════════════════
    #  [SEND-WINDOW-GATE] TCPA 9AM-9PM America/Chicago.
    #
    #  Placed here so EVERY outbound path is covered - outreach,
    #  qualification, booking pitch, nurture, bill ack, booked replies.
    #  Nothing is generated or frozen for later: the contact is marked
    #  in GHL and reprocessed from scratch in the next permitted window.
    # ══════════════════════════════════════════════════════════════
    if not within_send_window():
        _ws   = send_window_status()
        _held = await place_after_hours_hold(contact_id)
        ev("SEND_DEFERRED_OUTSIDE_WINDOW", contact_id, kind=kind.value,
           now_central=_ws["now_central"], window=_ws["window"],
           hold_placed=_held)
        return {
            "status"       : SendStatus.SUPPRESSED.value,
            "accepted"     : False,
            "delivered"    : None,
            "message_id"   : "",
            "sent"         : False,
            "suppressed"   : True,
            "reason"       : SUPPRESS_REASON_WINDOW,
            "hold_placed"  : _held,
            "why"          : f"outside send window ({_ws['now_central']})",
            "deduped"      : False,
            "status_code"  : None,
            "response_body": None,
        }

    # ── Outbound duplicate suppression [ADAPT-6] ──────────────────
    if is_duplicate_outbound(contact_id, message):
        print(f"\n[DUPLICATE] ⚡ Outbound dedup — same message to {contact_id} already sent this minute — SKIPPING")
        log.warning(f"[{contact_id}] Outbound duplicate suppressed: {message[:80]!r}")
        return {
            "status"       : SendStatus.SUPPRESSED.value,
            "accepted"     : False,
            "delivered"    : None,
            "message_id"   : "",
            "sent"         : False,
            "suppressed"   : True,
            "reason"       : "outbound_dedup",
            "deduped"      : True,
            "status_code"  : None,
            "response_body": None,
        }

    # ── [FIX-6] Last-line-of-defence URL sanitiser ─────────────────
    # Catches any LeadConnector booking URL that somehow survived to this
    # point (stale .env value, Claude hallucination, code path we missed).
    message = sanitize_outbound_message(message, contact_id)

    # ── [FIX-4.B] Resolve toNumber — required by GHL Conversations API ──
    # GHL's POST /conversations/messages returns 400 if toNumber is absent.
    # If the caller did not provide a phone number, look it up from GHL.
    _resolved_to_number = (to_number or "").strip()
    if not _resolved_to_number:
        print(f"[SMS] ⚠  to_number not provided — attempting GHL contact lookup for {contact_id!r}")
        _resolved_to_number = await fetch_ghl_contact_phone(contact_id)
        if _resolved_to_number:
            print(f"[SMS] ✅  Resolved toNumber from GHL contact: {_resolved_to_number!r}")
        else:
            print(f"[SMS] ❌  Could not resolve toNumber — GHL will likely return 400 without it")
            log.error(f"[{contact_id}] send_sms_via_ghl: toNumber is empty and GHL lookup failed")

    url = f"{GHL_API_BASE}/conversations/messages"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        "Version"      : "2021-07-28",
    }
    # [FIX-4.B] Always include toNumber — omitting it causes GHL 400
    payload: dict = {
        "type"      : "SMS",
        "contactId" : contact_id,
        "fromNumber": GHL_FROM_NUMBER,
        "toNumber"  : _resolved_to_number,   # always present (may be "" if lookup failed)
        "message"   : message,
    }

    # ── Full pre-flight debug log ──────────────────────────────────
    _key_tail = GHL_API_KEY[-8:] if GHL_API_KEY else "NOT_SET"
    _from_disp = GHL_FROM_NUMBER or "(NOT SET — check GHL_FROM_NUMBER env var)"
    print(f"\n{'='*64}")
    print(f"[SMS] 🚀  SEND SMS PRE-FLIGHT")
    print(f"[SMS]  Contact ID   : {contact_id!r}")
    print(f"[SMS]  First name   : (see routing log above)")
    print(f"[SMS]  toNumber     : {_resolved_to_number!r}  {'✓' if _resolved_to_number else '⚠ EMPTY — may cause 400'}")
    print(f"[SMS]  fromNumber   : {_from_disp!r}")
    print(f"[SMS]  Endpoint     : POST {url}")
    print(f"[SMS]  API key tail : ...{_key_tail}  (full key redacted)")
    print(f"[SMS]  Version hdr  : 2021-07-28")
    print(f"[SMS]  Message len  : {len(message)} chars")
    print(f"[SMS]  Message text : {message!r}")
    print(f"[SMS]  ── Exact JSON payload ────────────────────────────")
    import json as _json_mod
    print(f"[SMS]  {_json_mod.dumps(payload, indent=4)}")
    print(f"{'='*64}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(url, json=payload, headers=headers)

    # ── Full response log — always printed so 400 cause is visible ──
    resp_body: dict | str | None = None
    try:
        resp_body = r.json()
        _resp_display = _json_mod.dumps(resp_body, indent=2) if isinstance(resp_body, dict) else str(resp_body)
    except Exception as _json_err:
        resp_body = r.text
        _resp_display = r.text

    print(f"\n[SMS]  ── GHL Response ──────────────────────────────────")
    print(f"[SMS]  Status code  : {r.status_code}  {'✅ Success' if r.is_success else '❌ FAILED'}")
    print(f"[SMS]  Response body: {_resp_display}")
    if not r.is_success:
        print(f"[SMS]  !! EXACT GHL ERROR — status={r.status_code}")
        print(f"[SMS]  !! Check 'Response body' above for the GHL error message.")
        print(f"[SMS]  !! Common 400 causes:")
        print(f"[SMS]  !!   • toNumber is empty or invalid E.164 format")
        print(f"[SMS]  !!   • fromNumber is not a valid GHL phone number")
        print(f"[SMS]  !!   • contactId does not exist in this GHL sub-account")
        print(f"[SMS]  !!   • GHL_FROM_NUMBER env var not set or wrong format")
        print(f"[SMS]  !!   payload sent → {_json_mod.dumps(payload)}")
    print(f"{'='*64}\n")

    if r.is_success:
        # ── [DELIVERY] ACCEPTED, not delivered ────────────────────────
        # GHL has queued the message. The carrier may still reject it
        # (e.g. 30007) minutes later. Nothing here proves it arrived.
        _msg_id = ""
        if isinstance(resp_body, dict):
            _nested = resp_body.get("message")
            _cands = [
                resp_body.get("messageId"),
                resp_body.get("messageid"),
                resp_body.get("msgId"),
                resp_body.get("id"),
                (_nested or {}).get("id") if isinstance(_nested, dict) else None,
            ]
            _msg_id = next((str(c) for c in _cands if c), "")
        log.info(f"[{contact_id}] SMS ACCEPTED by GHL: status={r.status_code} "
                 f"message_id={_msg_id!r} to={_resolved_to_number!r}")
        ev("SMS_ACCEPTED", contact_id, kind=kind.value, message_id=_msg_id or "(none)",
           http=r.status_code, note="queued_not_delivered")
        return {
            "status"       : SendStatus.ACCEPTED.value,
            "accepted"     : True,
            "delivered"    : None,        # unknown until a delivery feed exists
            "message_id"   : _msg_id,
            "sent"         : True,        # backward-compat alias for accepted
            "suppressed"   : False,
            "deduped"      : False,
            "status_code"  : r.status_code,
            "response_body": resp_body,
        }
    else:
        log.error(
            f"[{contact_id}] GHL SMS rejected: status={r.status_code} "
            f"to={_resolved_to_number!r} body={r.text[:300]!r}"
        )
        r.raise_for_status()
        # raise_for_status() always raises here; this return is for type-checker only
        return {
            "status"       : SendStatus.REJECTED.value,
            "accepted"     : False,
            "delivered"    : False,
            "message_id"   : "",
            "sent"         : False,
            "suppressed"   : False,
            "deduped"      : False,
            "status_code"  : r.status_code,
            "response_body": resp_body,
        }


async def update_ghl_contact(contact_id: str, tags: list[str]):
    url = f"{GHL_API_BASE}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        "Version"      : "2021-04-15",
    }
    async with httpx.AsyncClient() as client:
        r = await client.put(url, json={"tags": tags}, headers=headers)
        r.raise_for_status()
        log.info(f"[{contact_id}] GHL contact updated: tags={tags}")


# ─────────────────────────────────────────────
#  [META-B1] ADDITIVE TAG WRITER  — SAFETY CRITICAL
#
#  update_ghl_contact() above issues PUT /contacts/{id} with {"tags": [...]}.
#  On the GHL contacts API that payload REPLACES the tag array, so writing
#  ["QUALIFIED"] can drop "appointment booked", "solar-dnc", "meta-lead" and
#  every other marker on the contact.  Those tags are exactly what this
#  system now relies on for cross-restart durability, so a blind PUT would
#  destroy the thing it is meant to protect.
#
#  add_ghl_tags() therefore reads the contact first, unions the requested
#  tags with what is already there (case-insensitively), and writes back
#  the merged set.  It skips the write entirely when nothing would change,
#  so the common path costs one GET and no mutation.
#
#  Never call update_ghl_contact() directly for incremental tagging.
# ─────────────────────────────────────────────

async def add_ghl_tags(contact_id: str, new_tags: list[str]) -> bool:
    """Add tags to a GHL contact WITHOUT dropping existing ones. Non-fatal."""
    if not (contact_id and new_tags and GHL_API_KEY):
        return False
    url     = f"{GHL_API_BASE}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        "Version"      : "2021-07-28",
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=headers)
            if not r.is_success:
                log.warning(f"[{contact_id}] add_ghl_tags: contact GET failed ({r.status_code})")
                return False
            data     = r.json()
            contact  = data.get("contact") or data
            existing = [str(t) for t in (contact.get("tags") or [])]

            have   = {t.lower().strip() for t in existing}
            to_add = [t for t in new_tags if t.lower().strip() not in have]
            if not to_add:
                return True   # already present — no write needed

            merged = existing + to_add
            w = await client.put(url, json={"tags": merged}, headers=headers)
            if not w.is_success:
                log.warning(f"[{contact_id}] add_ghl_tags: PUT failed ({w.status_code})")
                return False
        log.info(f"[{contact_id}] tags added: {to_add} (preserved {len(existing)} existing)")
        return True
    except Exception as err:
        log.warning(f"[{contact_id}] add_ghl_tags failed (non-fatal): {err}")
        return False


def resolve_ghl_tags(stage: Stage, qualified: bool = True) -> list[str]:
    """
    GHL tags for a stage.

    [BUNDLE-2] `qualified=False` writes BOOKING_LINK_SENT without QUALIFIED,
    for a high-intent lead who got the link with the criteria attached but
    has not confirmed them. restore_stage_from_ghl() matches on EITHER tag,
    so recovery still resumes at SEND_BOOKING — the only thing that changes
    is that nothing in GHL claims a qualification nobody gave.
    """
    if stage == Stage.SEND_BOOKING and not qualified:
        return ["BOOKING_LINK_SENT"]
    tag_map = {
        Stage.SEND_BOOKING : ["QUALIFIED", "BOOKING_LINK_SENT"],
        Stage.BOOKED       : ["APPOINTMENT_BOOKED"],
        # [META-B1] TAG_DISQUALIFIED is the lowercase marker the GHL nurture
        # workflow gates on, alongside the existing NOT_QUALIFIED tag.
        Stage.DISQUALIFIED : ["NOT_QUALIFIED", TAG_DISQUALIFIED],
        # [META-B1] TAG_DNC is the durable marker the GHL Customer Replied
        # trigger filter ("Doesn't have tag solar-dnc") reads.  Writing it
        # here is what makes an opt-out survive a Render restart.
        Stage.DNC          : ["DNC", TAG_DNC],
    }
    return tag_map.get(stage, [])


# ─────────────────────────────────────────────
#  INBOUND WEBHOOK  (GHL → Michael)
# ─────────────────────────────────────────────

@app.post("/webhook/inbound")
async def inbound_webhook(request: Request):
    """
    GHL fires this endpoint for:
      • Inbound SMS/chat messages from a lead
      • Chat widget / calendar form submissions
      • Appointment booking events

    Always returns HTTP 200 — non-200 causes GHL to retry endlessly.
    All errors are caught; nothing propagates to a 500.

    [FIX-6] v3.6 — raw-first parse + guaranteed terminal visibility
    ──────────────────────────────────────────────────────────────────
    The FIRST thing this handler does is read the raw request bytes and
    print them to stdout with flush=True.  This fires before any parsing,
    routing, or try/except logic.  If you see this banner in the terminal,
    the route is being called.  If you don't see it, the issue is upstream
    (wrong port, wrong ngrok target, different uvicorn process).

    Parse strategy:
      1. Try JSON decode of raw bytes (handles all GHL webhook formats)
      2. Try URL-encoded form decode (some GHL widget submissions)
      3. Wrap raw string in dict (never silently drop a payload)
    The old code returned {"reason": "invalid_json"} immediately on parse
    failure — meaning every non-JSON GHL webhook was silently eaten, the
    terminal showed nothing, and no SMS was sent.
    """
    # ══════════════════════════════════════════════════════════════════
    # STEP 0 — RAW REQUEST CAPTURE  (runs BEFORE outer try/except)
    #
    # Uses three output layers to guarantee visibility regardless of how
    # uvicorn or the OS buffers stdout:
    #   Layer 1: os.write(1/2, ...) — raw POSIX file-descriptor writes
    #   Layer 2: print(..., flush=True) — Python stdout, force-flushed
    #   Layer 3: sys.stderr.write(...)  — stderr, always unbuffered
    #
    # If you see NOTHING in the terminal after this: the request is going
    # to a DIFFERENT PROCESS or DIFFERENT PORT than this Python instance.
    # Check: `ps aux | grep uvicorn` and `netstat -tlnp | grep 8000`
    # ══════════════════════════════════════════════════════════════════
    def _loud(msg: str) -> None:
        """Write msg to stdout (raw fd + print) and stderr simultaneously."""
        _b = (msg + "\n").encode("utf-8", errors="replace")
        try:
            os.write(1, _b)
        except Exception:
            pass
        try:
            os.write(2, _b)
        except Exception:
            pass
        try:
            print(msg, flush=True)
        except Exception:
            pass

    _raw_bytes: bytes = b""
    try:
        _raw_bytes = await request.body()
    except Exception as _body_err:
        _loud(f"[INBOUND] !! BODY READ FAILED: {_body_err}")

    _req_ct  = request.headers.get("content-type", "(not set)")
    _req_ua  = request.headers.get("user-agent",   "(not set)")
    _req_method = request.method
    _req_path   = request.url.path

    _loud("=== WEBHOOK HIT ===")
    _loud(f"Method       : {_req_method}")
    _loud(f"URL path     : {_req_path}")
    _loud(f"Content-Type : {_req_ct}")
    _loud(f"User-Agent   : {_req_ua}")
    _loud(f"Body length  : {len(_raw_bytes)} bytes")
    _loud(f"Raw body     : {_raw_bytes[:800]!r}")
    _loud(f"All headers  : {dict(request.headers)}")
    _loud("=== END WEBHOOK HIT ===")

    # Declared before outer try so the finally block can always reach them,
    # regardless of which code path returns or which exception fires.
    _proc_lock  = None   # asyncio.Lock assigned after contact_id is resolved
    _lock_acq   = False  # True only after await _proc_lock.acquire() succeeds

    try:
        # ══════════════════════════════════════════════════════════════
        # STEP 1 — MULTI-STRATEGY BODY PARSE
        #
        # ROOT CAUSE OF CHAT WIDGET SILENCE:
        # The previous code was:
        #   try:
        #       body = await request.json()
        #   except Exception as parse_err:
        #       log.warning(...)            ← goes to stderr, invisible in terminal
        #       return JSONResponse(...)    ← returns 200, no SMS, no print output
        #
        # GHL chat widget webhooks can arrive as:
        #   • application/json  (booking events — worked before)
        #   • application/x-www-form-urlencoded  (some widget events — silently dropped)
        #   • Empty body with contact_id in query params  (some GHL workflow configs)
        #   • JSON with wrong/missing Content-Type header
        #
        # The fix: read raw bytes first (already done above), then try three
        # parse strategies in order.  NEVER return on parse failure — always
        # continue processing with whatever we could extract.
        # ══════════════════════════════════════════════════════════════
        body: dict = {}
        _parse_method = "empty_body"

        if _raw_bytes:
            # Strategy 1: JSON (handles all normal GHL webhooks)
            try:
                _parsed_candidate = json.loads(_raw_bytes)
                if isinstance(_parsed_candidate, dict):
                    body = _parsed_candidate
                    _parse_method = "json_object"
                else:
                    # JSON but not a dict (list, string, number) — wrap it
                    body = {"_raw_value": _parsed_candidate}
                    _parse_method = f"json_non_dict ({type(_parsed_candidate).__name__})"
            except json.JSONDecodeError as _json_err:
                print(f"[INBOUND] ⚠ JSON parse failed: {_json_err}", flush=True)
                print(f"[INBOUND]   → trying URL-form decode", flush=True)

                # Strategy 2: URL-encoded form data
                # GHL chat widget submissions sometimes arrive as form posts
                try:
                    _form = parse_qs(
                        _raw_bytes.decode("utf-8", errors="replace"),
                        keep_blank_values=True,
                    )
                    if _form:
                        body = {k: (v[0] if len(v) == 1 else v) for k, v in _form.items()}
                        _parse_method = "url_form"
                        print(f"[INBOUND] ✅ URL-form decode succeeded: {body}", flush=True)
                    else:
                        raise ValueError("empty form parse result")
                except Exception as _form_err:
                    print(f"[INBOUND] ⚠ URL-form decode also failed: {_form_err}", flush=True)
                    print(f"[INBOUND]   → falling back to raw-string wrapper", flush=True)

                    # Strategy 3: Wrap raw string — never drop the payload
                    _raw_str = _raw_bytes.decode("utf-8", errors="replace")
                    body = {"_raw_body": _raw_str}
                    _parse_method = "raw_fallback"

        # Also absorb any query-string params (GHL sometimes passes contact_id there)
        _qs_params = dict(request.query_params)
        if _qs_params:
            print(f"[INBOUND] Query params: {_qs_params}", flush=True)
            for _qk, _qv in _qs_params.items():
                if _qk not in body:
                    body[_qk] = _qv

        print(f"[INBOUND] Parse method   : {_parse_method}", flush=True)
        print(f"\n{'='*60}", flush=True)
        print(f"[INBOUND] Webhook parsed — body keys: {sorted(body.keys()) if body else '(empty)'}", flush=True)
        print(f"[INBOUND] Full body: {body}", flush=True)
        print(f"{'='*60}\n", flush=True)

        # ── RAW PAYLOAD DIAGNOSTIC DUMP ───────────────────────────
        # Printed BEFORE any routing decision so you always have the
        # evidence needed to debug classification failures.
        _raw_event_type = str(
            body.get("type") or body.get("eventType") or
            body.get("event_type") or body.get("event") or "(not set)"
        ).strip()
        _raw_keys              = sorted(body.keys()) if isinstance(body, dict) else []
        _appt_probe_ok, _appt_probe_reason = is_booked_appointment_lead(body)
        _appt_probe_display    = _appt_probe_reason if _appt_probe_ok else f"NOT a booking — {_appt_probe_reason}"
        print(f"\n[INBOUND] ╔══ PAYLOAD DIAGNOSTIC ══════════════════════════════")
        print(f"[INBOUND] ║  parse_method      : {_parse_method!r}")
        print(f"[INBOUND] ║  raw event type    : {_raw_event_type!r}")
        print(f"[INBOUND] ║  all top-level keys: {_raw_keys}")
        print(f"[INBOUND] ║  has 'message' key : {bool(body.get('message') or body.get('body') or body.get('text'))}")
        print(f"[INBOUND] ║  has 'direction'   : {body.get('direction')!r}")
        _has_media = bool(
            body.get("attachments") or body.get("mediaUrls") or
            body.get("media") or body.get("mediaUrl")
        )
        print(f"[INBOUND] ║  has 'attachments' : {bool(body.get('attachments') or body.get('mediaUrls') or body.get('media'))}")
        print(f"[INBOUND] ║  message_type      : {str(body.get('messageType') or body.get('message_type') or '(not set)')!r}")
        print(f"[INBOUND] ║  source field      : {str(body.get('source') or body.get('lead_source') or '(not set)')!r}")
        print(f"[INBOUND] ║  booking detection : {_appt_probe_display}")
        print(f"[INBOUND] ╚══════════════════════════════════════════════════════\n")

        # ── Normalize ─────────────────────────────────────────────
        parsed        = normalize_payload(body)
        custom_fields = extract_custom_fields(body)

        contact_id       = parsed["contact_id"]
        first_name       = parsed["first_name"]
        full_name        = parsed["full_name"]
        phone            = parsed["phone"]
        address          = parsed["address"]
        direction        = parsed["direction"]
        booking_detected = parsed["booking_detected"]
        tags             = parsed["tags"]
        lead_source      = parsed["lead_source"]

        # Enrich from GHL API when name is missing — common in Customer Replied webhooks
        # which often carry only contactId and the message body, no name fields.
        if (not first_name or first_name.lower() in _INVALID_NAMES) and contact_id and direction == "inbound":
            _fetched_name = await fetch_ghl_contact_first_name(contact_id)
            if _fetched_name:
                first_name = _fetched_name
                if not full_name or full_name == "Unknown":
                    full_name = _fetched_name

        print(f"[INBOUND] Contact    : {full_name!r} ({contact_id})")
        print(f"[INBOUND] Phone      : {phone or '(not provided)'}")
        print(f"[INBOUND] Direction  : {direction}")
        print(f"[INBOUND] Tags       : {tags}")
        print(f"[INBOUND] Address    : {address or '(not provided)'}")

        # ── Fetch GHL pipeline stage — PRIMARY routing driver ─────
        _ghl_stage: str = ""
        _ghl_tags: list = list(tags) if tags else []
        if contact_id:
            _ghl_stage = await fetch_ghl_opportunity_stage(contact_id)
            print(f"[PIPELINE] 📊 GHL pipeline stage={_ghl_stage!r} | contact={contact_id}", flush=True)

        print(f"[INBOUND] Lead source: {lead_source or '(not provided)'}")
        if custom_fields:
            print(f"[INBOUND] Custom fields: {custom_fields}")

        # ── PIPELINE TRACE — message extraction diagnostic ────────────────
        # Confirms exactly which field the SMS text was found in.
        # If "nested_msg_obj" → GHL Customer Replied format; text came from body["message"]["body"].
        # If direction=inbound but has_real_message=False → message field missing or unrecognized.
        _msg_body_field_raw = body.get("message")
        _msg_src = (
            "nested_msg_obj (body[message][body])" if isinstance(_msg_body_field_raw, dict) else
            "body[message]"  if isinstance(_msg_body_field_raw, str) and _msg_body_field_raw else
            "body[body]"     if body.get("body") else
            "body[text]"     if body.get("text") else
            "body[messageBody]" if body.get("messageBody") else
            "body[content]"  if body.get("content") else
            "(not found — all fields empty)"
        )
        _msg_extracted = parsed.get("message", "")
        print(
            f"[PIPELINE] ── MESSAGE EXTRACTION TRACE ──────────────────────────\n"
            f"[PIPELINE]  source_field    : {_msg_src}\n"
            f"[PIPELINE]  extracted_msg   : {_msg_extracted!r}\n"
            f"[PIPELINE]  has_real_message: {parsed.get('has_real_message')}\n"
            f"[PIPELINE]  direction       : {direction}\n"
            f"[PIPELINE]  contact_id      : {contact_id}\n"
            f"[PIPELINE]  stage           : {get_state(contact_id)['stage']}\n"
            f"[PIPELINE] ────────────────────────────────────────────────────────",
            flush=True,
        )
        if direction == "inbound" and not parsed.get("has_real_message"):
            _loud(
                f"[PIPELINE] ⚠ INBOUND SMS with no recognized message body!\n"
                f"[PIPELINE]   contact={contact_id} | msg_src={_msg_src}\n"
                f"[PIPELINE]   Raw body[message] type: {type(_msg_body_field_raw).__name__}\n"
                f"[PIPELINE]   Raw body[message] value: {str(_msg_body_field_raw)[:200]!r}\n"
                f"[PIPELINE]   All body keys: {sorted(body.keys()) if body else []}\n"
                f"[PIPELINE]   This message will NOT be processed unless routing allows it!"
            )

        # ── [PATH-1/2] Register phone → contact_id for continuity ────
        # Done before any routing so we always capture the chat-widget CID.
        # This is what lets the phone-based lookup below find the right state
        # when the appointment webhook arrives with a different contact_id.
        if contact_id and phone:
            _register_phone(contact_id, phone)

        # ── Routing category tracker ──────────────────────────────
        # Set to "booked", "lead", or "unknown" as we route the request.
        # Never left unset — default guards against any missed code path.
        msg_type = "unknown"

        # ── Guard: must have contact_id ───────────────────────────
        if not contact_id:
            log.warning("Webhook received with no contact_id — skipping")
            print("[INBOUND] ⚠ No contact_id — skipping")
            return JSONResponse({"status": "success", "skipped": True, "reason": "no_contact_id"})

        # ── Guard: skip outbound (GHL echoes our own sends) ───────
        if direction == "outbound":
            print(f"[ROUTING] ⏭ OUTBOUND echo for {contact_id} — skipping")
            print(f"[ROUTING]   ⚠  If you expected a booked SMS here, check GHL webhook config:")
            print(f"[ROUTING]      The webhook payload contains direction={direction!r}")
            print(f"[ROUTING]      GHL 'Appointment Booked' workflow webhooks should NOT include direction=outbound")
            log.info(f"[{contact_id}] Skipping outbound echo | direction={direction!r}")
            return JSONResponse({"status": "success", "skipped": True, "reason": "outbound"})

        # ── [FIX-11] Event-ID idempotency dedup ──────────────────────
        # Drop exact-duplicate webhook events before touching state.
        # GHL can fire multiple webhooks per lead action:
        #   • Raw inbound message webhook (MMS/SMS)
        #   • "Customer replied" workflow webhook (same contactId, different body)
        # The content-fingerprint dedup below catches same-body duplicates.
        # This catches different-body duplicates for the SAME underlying event
        # by matching on GHL's own event/message ID.
        _event_id = parsed.get("event_id", "")
        if _event_id and _is_duplicate_event_id(_event_id, contact_id):
            print(
                f"\n[DEDUP] ⚡ [FIX-11] Event-ID duplicate detected — "
                f"event_id={_event_id!r} already processed → SKIP"
            )
            log.info(f"[{contact_id}] Event-ID dedup: {_event_id!r} already processed")
            return JSONResponse({
                "status"    : "success",
                "skipped"   : True,
                "reason"    : "duplicate_event_id",
                "event_id"  : _event_id,
                "contact_id": contact_id,
            })

        # ── [FIX-10] Universal phone-based contact_id resolution ──────
        # v2.9 ONLY ran this for payloads that already looked like appointment
        # webhooks.  This missed the key failure mode: an MMS bill photo arrives
        # with a different contactId than the chat session (GHL creates a second
        # contact record or uses a different ID for the SMS thread).  In that
        # case, fresh INITIAL state was used, all priority guards missed because
        # appointment_booked=False on the fresh state, and michael_agent() sent
        # the full qualification/booking text.
        #
        # Fix: run phone resolution for EVERY webhook where a phone is present.
        # _resolve_contact_id_by_phone already has the right guard: it only
        # redirects when the webhook contact_id has no/INITIAL state AND the
        # known contact_id has history.  So legitimate new contacts (no history)
        # are unaffected.
        #
        # [FIX-12] EXCEPTION: never redirect a LIVE inbound human SMS.
        # A live inbound text always carries the correct, current GHL contact_id
        # (GHL just received the message on that contact).  Redirecting it via the
        # in-memory phone map can point at a STALE contact_id from a prior session
        # for the same number (common when re-testing from one phone), which:
        #   1. drags in that old contact's booked state/tags → wrong booked
        #      follow-up path instead of the live reply / qualification path, and
        #   2. 400s with "Contact not found" at /conversations/messages when that
        #      old contact was deleted or merged in GHL.
        # The phone-map redirect is only needed for appointment / MMS webhooks
        # that legitimately arrive with a different or missing contact_id — those
        # have has_real_message=False, so this gate preserves them.
        _is_live_text_reply = (direction == "inbound" and bool(parsed.get("has_real_message")))
        if phone and _is_live_text_reply:
            print(
                f"[PHONE-MAP] ⏭ [FIX-12] live inbound SMS — keeping GHL-provided "
                f"contact_id {contact_id!r} (phone-map redirect skipped)",
                flush=True,
            )
        elif phone:
            _resolved_cid = _resolve_contact_id_by_phone(contact_id, phone)
            if _resolved_cid != contact_id:
                print(
                    f"[PHONE-MAP] ✅ [FIX-10] Universal resolution: "
                    f"{contact_id!r} → {_resolved_cid!r} "
                    f"(phone={_normalize_phone(phone)!r})"
                )
                log.info(
                    f"[{contact_id}] Phone-map redirect → {_resolved_cid!r} "
                    f"(universal resolution, not appt-gated)"
                )
                contact_id = _resolved_cid

        # ══════════════════════════════════════════════════════════════
        #  UNIVERSAL INBOUND DEDUP — runs before ALL routing paths
        #
        #  GHL fires 2-4 webhooks per customer SMS:
        #    • Raw inbound (has messageId "A")
        #    • Customer Replied workflow (different messageId "B")
        #  The per-path dedup in the chat-widget path never fires for booked
        #  contacts (they take a different branch).  is_duplicate_inbound() now
        #  checks BOTH TIER 1 and TIER 2, so the first webhook registers the
        #  content fingerprint regardless of path, and the second is blocked here.
        # ══════════════════════════════════════════════════════════════
        if direction == "inbound" and bool(parsed.get("has_real_message")):
            _ud_msg_id  = parsed.get("event_id", "")
            _ud_msg_txt = parsed.get("message", "")
            _ud_is_dup, _ud_reason = is_duplicate_inbound(
                contact_id, _ud_msg_txt, ghl_message_id=_ud_msg_id
            )
            print(
                f"[DEDUP] {'🔒 BLOCKED' if _ud_is_dup else '✅ ALLOWED'} "
                f"| {_ud_reason} | contact={contact_id}",
                flush=True,
            )
            log.info(
                f"[{contact_id}] Universal dedup: "
                f"{'duplicate — skipping' if _ud_is_dup else 'new event — proceeding'} | {_ud_reason}"
            )
            if _ud_is_dup:
                ev("DUPLICATE_INBOUND_DROPPED", contact_id, layer="content_fingerprint")
                return JSONResponse({
                    "status"      : "success",
                    "skipped"     : True,
                    "reason"      : "duplicate_inbound_universal",
                    "dedup_reason": _ud_reason,
                    "contact_id"  : contact_id,
                })

        # ── [CONCURRENCY-1] Inbound debounce ─────────────────────────────
        # Suppresses a new inbound trigger if we completed processing for this
        # contact within the last 2 seconds.  Covers the edge case where GHL
        # fires a delayed second webhook AFTER the first finishes (the lock is
        # already free but state was just written).  Human reply latency is
        # always 3+ seconds, so a 2-second window never blocks legitimate replies.
        if direction == "inbound" and parsed.get("has_real_message"):
            _deb, _deb_elapsed = _is_debounced(contact_id)
            if _deb:
                print(
                    f"[DEBOUNCE] ⏱ {contact_id} — {_deb_elapsed:.3f}s since last process "
                    f"(window={_INBOUND_DEBOUNCE_SECS}s) — suppressing rapid trigger",
                    flush=True,
                )
                log.info(f"[{contact_id}] Debounce suppressed ({_deb_elapsed:.3f}s < {_INBOUND_DEBOUNCE_SECS}s)")
                return JSONResponse({
                    "status"      : "success",
                    "skipped"     : True,
                    "reason"      : "inbound_debounce",
                    "elapsed_secs": round(_deb_elapsed, 3),
                    "contact_id"  : contact_id,
                })

        # ── [CONCURRENCY-2] Per-contact processing lock ───────────────────
        # Prevents concurrent michael_agent() calls for the same contact.
        # check + acquire is atomic within asyncio's single-threaded event loop
        # (no `await` between them means no other coroutine can interleave).
        # The lock is released in the `finally` block at the end of this function,
        # regardless of which code path returns or which exception fires.
        _proc_lock = _get_or_create_contact_lock(contact_id)
        if _proc_lock.locked():
            print(
                f"[LOCK] ⚡ {contact_id} processing lock HELD — "
                f"concurrent trigger suppressed (another coroutine is mid-process)",
                flush=True,
            )
            log.warning(f"[{contact_id}] Processing lock held — concurrent duplicate suppressed")
            return JSONResponse({
                "status"    : "success",
                "skipped"   : True,
                "reason"    : "contact_processing_locked",
                "contact_id": contact_id,
            })

        await _proc_lock.acquire()
        _lock_acq = True
        print(f"[LOCK] 🔒 {contact_id} lock acquired — processing serialized", flush=True)

        # ══════════════════════════════════════════════════════════════
        #  [META-B1] DURABLE DNC RESTORE  — runs before ALL routing
        #
        #  Stage.DNC lives in _state_store, which is wiped on every Render
        #  restart and spin-down.  An opted-out lead who texts again after a
        #  restart would otherwise be processed as brand new, and
        #  is_stop_request() only matches if THAT message repeats a STOP
        #  keyword — so a plain "hey" would get a reply.
        #
        #  The solar-dnc GHL tag is the durable record.  Restoring from it
        #  here makes opt-out survive anything that kills the process.
        # ══════════════════════════════════════════════════════════════
        if has_tag(tags, TAG_DNC):
            _dnc_state = get_state(contact_id)
            if _dnc_state.get("stage") != Stage.DNC:
                _dnc_state["stage"] = Stage.DNC
                save_state(contact_id, _dnc_state)
                ev("DNC_RESTORED_FROM_TAG", contact_id, tag=TAG_DNC)

        # ══════════════════════════════════════════════════════════════
        #  [META-B1] ENGAGEMENT MARKER  — cancels pending GHL nurture
        #
        #  Written on ANY real inbound message, including STOP.  The GHL
        #  nurture workflow re-checks this tag immediately before each
        #  follow-up send, so writing it here is what guarantees a canned
        #  message never lands on top of a live conversation.
        #  Additive and non-fatal — a tag failure must never cost a reply.
        # ══════════════════════════════════════════════════════════════
        if direction == "inbound" and parsed.get("has_real_message"):
            ev(
                "INBOUND_RECEIVED", contact_id,
                phone=phone,
                stage=str(get_state(contact_id).get("stage")),
                meta=is_meta_lead(lead_source, tags),
            )
            if not has_tag(tags, TAG_ENGAGED):
                try:
                    if await add_ghl_tags(contact_id, [TAG_ENGAGED]):
                        ev("NURTURE_CANCELLED_REPLY_RECEIVED", contact_id, tag=TAG_ENGAGED)
                except Exception as _eng_err:
                    log.warning(f"[{contact_id}] engagement tag write failed (non-fatal): {_eng_err}")

        # ══════════════════════════════════════════════════════════════
        #  PRIORITY ROUTING GUARDS  [FIX-7 / FIX-9]
        #
        #  These checks run BEFORE the appointment_booked tag routing and BEFORE any
        #  form_submission / chat_widget routing.  They use explicit durable
        #  flags (appointment_booked, bill_requested, bill_received,
        #  final_confirmation_sent) rather than stage alone, which protects
        #  against:
        #    • Race conditions — two webhooks processed concurrently where one
        #      reads stale state before the other has saved it
        #    • Unexpected GHL webhook formats — MMS attachment not recognized,
        #      payload fires with wrong contact_id, etc.
        #    • Stage regression — anything that would accidentally move a BOOKED
        #      contact back through the qualification flow
        #    • GHL workflow webhooks for the same event (duplicate by content)
        #
        #  PRIORITY ORDER:
        #  GUARD 0: final_confirmation_sent = True → absolute no-op [FIX-9]
        #  GUARD 1: bill_received = True           → absolute no-op
        #  GUARD 2: appointment_booked = True      → booked sub-flow only
        #  GUARD 3 onwards: normal routing (qualification, etc.)
        # ══════════════════════════════════════════════════════════════
        _priority_state = get_state(contact_id)

        # ── GUARD 0: Final confirmation already sent  [FIX-9] ────────
        # Blocks GHL echoes, synthetic starters, and duplicate bill images.
        # Real text questions from the homeowner are allowed through so Michael
        # can answer post-appointment concerns (cost, roof, rescheduling, etc.).
        if _priority_state.get("final_confirmation_sent"):
            _g0_has_real_txt = parsed.get("has_real_message") and not parsed.get("has_attachment")
            _g0_has_attach   = bool(parsed.get("has_attachment"))
            _g0_msg          = (parsed.get("message") or "").strip()

            if not _g0_has_real_txt:
                # Non-real webhook (GHL echo, placeholder) or duplicate bill image → block
                _g0_block_reason = "duplicate_bill_attachment" if _g0_has_attach else "non_real_webhook"
                print(
                    f"\n[GUARD] 🔒 PRIORITY GUARD 0: final_confirmation_sent=True — "
                    f"blocking {_g0_block_reason}"
                )
                log.info(f"[{contact_id}] PRIORITY GUARD 0: final_confirmation_sent — blocking {_g0_block_reason}")
                return JSONResponse({
                    "status"      : "success",
                    "skipped"     : True,
                    "reason"      : "priority_guard_0_final_confirmation_sent",
                    "block_reason": _g0_block_reason,
                    "contact_id"  : contact_id,
                })

            # Real text question after flow complete → bypass guard, route to booked follow-up
            print(
                f"\n[GUARD] ⚡ PRIORITY GUARD 0 BYPASSED — final_confirmation_sent=True "
                f"but real text question detected"
                f"\n[GUARD]   intent=post_flow_question | msg={_g0_msg[:60]!r} → booked follow-up"
            )
            log.info(
                f"[{contact_id}] PRIORITY GUARD 0: bypassed for real post-flow text question: {_g0_msg[:60]!r}"
            )
            # fall through to booked follow-up path

        # ── GUARD 1: Bill already received — flow complete ────────────
        # Blocks duplicate bill-photo webhooks and GHL echoes.
        # Real text questions from the homeowner are allowed through so Michael
        # can answer post-appointment concerns (cost, roof, rescheduling, etc.).
        _bill_already_received = (
            _priority_state.get("bill_received") or
            (  # backward compat: older contacts before v2.9 flag was added
               _priority_state.get("bill_ack_sent") and
               _priority_state.get("bill_photo_received")
            )
        )
        if _bill_already_received:
            _g1_has_real_txt = parsed.get("has_real_message") and not parsed.get("has_attachment")
            _g1_has_attach   = bool(parsed.get("has_attachment"))
            _g1_msg          = (parsed.get("message") or "").strip()

            if not _g1_has_real_txt:
                # Non-real webhook or duplicate bill image → block
                _g1_block_reason = "duplicate_bill_attachment" if _g1_has_attach else "non_real_webhook"
                print(
                    f"\n[GUARD] 🔒 PRIORITY GUARD 1: bill_received=True — "
                    f"blocking {_g1_block_reason}"
                )
                log.info(f"[{contact_id}] PRIORITY GUARD 1: bill_received — blocking {_g1_block_reason}")
                return JSONResponse({
                    "status"      : "success",
                    "skipped"     : True,
                    "reason"      : "priority_guard_1_bill_received",
                    "block_reason": _g1_block_reason,
                    "contact_id"  : contact_id,
                })

            # Real text question after bill received → bypass guard, route to booked follow-up
            print(
                f"\n[GUARD] ⚡ PRIORITY GUARD 1 BYPASSED — bill_received=True "
                f"but real text question detected"
                f"\n[GUARD]   intent=post_flow_question | msg={_g1_msg[:60]!r} → booked follow-up"
            )
            log.info(
                f"[{contact_id}] PRIORITY GUARD 1: bypassed for real post-flow text question: {_g1_msg[:60]!r}"
            )
            # fall through to booked follow-up path

        # ══ END PRIORITY GUARDS (GUARD 0 + GUARD 1 above) ══════════════

        # ══ TAG-BASED ROUTING ════════════════════════════════════════════
        _state = get_state(contact_id)
        _log_last_name = (
            full_name[len(first_name):].strip()
            if first_name and full_name.startswith(first_name)
            else (full_name.split()[-1] if full_name and " " in full_name else "")
        )

        # ── TRIPLE-SIGNAL BOOKED CONTACT DETECTION ───────────────────
        #
        # [FIX-1] Use ALL three independent signals to decide whether this
        # webhook is for a booked contact.  Previously only tags were checked,
        # which meant:
        #   • Direct calendar bookings with no tags but with appointment fields
        #     would fall through to the "no_routing_tag_matched" no-op branch.
        #   • Returning contacts with state.appointment_booked=True but the tag
        #     absent from the current payload would reach qualification code.
        #
        # SIGNAL 1: payload-level detection (is_booked_appointment_lead)
        #   Checks for appointmentId, calendarId, startTime+endTime pair,
        #   eventType, appointmentStatus, etc.  Reuses the result already
        #   computed above in the PAYLOAD DIAGNOSTIC block.
        #
        # SIGNAL 2: tag-based detection (is_already_booked)
        #   [FIX-2] Uses the existing is_already_booked() helper which handles
        #   "appointment booked", "appointment_booked", "appointment confirmed",
        #   and mixed case.  Replaces the previous strict "appointment_booked" in tags.
        #
        # SIGNAL 3: durable state flag / stage
        #   Once appointment_booked=True is written to state it NEVER clears.
        #   Stage=BOOKED is also a reliable indicator for contacts who have been
        #   through the booked flow already (e.g. re-firing GHL workflow).
        #
        # [FIX-4] Because _payload_says_booked is included, a direct calendar
        # booking webhook with appointment fields but no GHL tag will now
        # correctly enter the booked flow instead of hitting no_routing_tag_matched.
        _payload_says_booked   = _appt_probe_ok                     # already computed above
        _payload_booking_reason = _appt_probe_reason                # already computed above
        _tag_says_booked       = is_already_booked(tags)            # [FIX-2] flexible helper
        _state_says_booked     = bool(
            _state.get("appointment_booked") or
            _state["stage"] in (Stage.BOOKED,) or
            # bill_received / final_confirmation_sent mean the contact went through the
            # full booking → bill-collection flow — definitely past qualification.
            _state.get("bill_received") or
            _state.get("final_confirmation_sent")
        )

        # ── [FIX-NEW] Chat widget payload detection (tag-independent) ─────
        # MUST be computed BEFORE _is_booked_contact so that the fresh-form-
        # submission guard ([FIX-4.A]) can suppress false booked detections
        # caused by stale in-memory state or prior tags from a previous contact.
        _tag_says_cw           = "source_chat_widget" in tags
        _cw_payload_ok, _cw_payload_reason = is_chat_widget_lead_payload(body, parsed)
        _is_lead_payload       = _tag_says_cw or _cw_payload_ok
        # [FIX-3.3] Diagnostic: is the message field a GHL placeholder?
        _parsed_msg            = parsed.get("message", "")
        _msg_is_placeholder    = is_placeholder_widget_message(_parsed_msg)
        _has_real_msg          = parsed.get("has_real_message", False)

        # ── [FIX-4.A] Fresh widget/form submissions MUST NOT enter booked path ──
        #
        # A payload that is positively identified as a chat widget / form submission
        # AND contains ZERO explicit booking proof (appointmentId, calendarId,
        # startTime+endTime, etc.) is guaranteed to be a new lead inquiry — never
        # a booked appointment follow-up.
        #
        # Why this guard is necessary:
        #   • _state_says_booked can fire for a brand-new widget lead if GHL re-uses
        #     a contact_id that had prior BOOKED state (e.g. same phone number from a
        #     test, or a phone-map redirect via _resolve_contact_id_by_phone).
        #   • _tag_says_booked can fire if GHL includes stale appointment tags from a
        #     prior booking in the webhook payload.
        #
        # The ONLY way a widget-classified lead can enter the booked path is if the
        # current payload itself contains explicit booking fields — meaning GHL is
        # firing an appointment event that happens to also match the widget signals.
        # Tag-only or state-only booked signals are suppressed for widget leads.
        # [BOOKED-GUARD] Authoritative evidence outranks payload shape.
        # _ghl_stage is fetched from GHL earlier in this handler and was
        # previously computed but never consulted here.
        _authoritative_booked = bool(_tag_says_booked or (_ghl_stage in GHL_BOOKED_STAGES))

        _is_fresh_form_sub = (
            _is_lead_payload and         # positively identified as widget/form lead
            not _payload_says_booked and # no explicit booking proof in this payload
            not _authoritative_booked    # [BOOKED-GUARD] never override GHL evidence
        )
        if _is_fresh_form_sub:
            _is_booked_contact    = False
            _fresh_form_override  = True
            _booked_override_reason = (
                f"[FIX-4.A] fresh_form_override — "
                f"is_lead_payload=True + payload_says_booked=False + "
                f"authoritative_booked=False → NEW_LEAD_PATH forced "
                f"(stale state_says_booked={_state_says_booked} suppressed; "
                f"GHL tag/stage evidence was absent, not ignored)"
            )
        else:
            _is_booked_contact    = (
                _payload_says_booked or
                _tag_says_booked     or
                _state_says_booked
            )
            _fresh_form_override  = False
            _booked_override_reason = ""

        # ── [FIX-6] Routing diagnostic — make it painfully obvious WHY ────
        _booked_signals: list[str] = []
        if _payload_says_booked:
            _booked_signals.append(f"payload ({_payload_booking_reason})")
        if _tag_says_booked:
            _booked_signals.append(f"tag ({[t for t in tags if 'appoint' in t.lower() or 'booked' in t.lower() or 'confirmed' in t.lower()]})")
        if _state_says_booked:
            _booked_signals.append(f"state (appointment_booked={_state.get('appointment_booked')}, stage={_state['stage']})")

        # ── [FIX-8.A] SMS REPLY CONTINUITY SIGNAL ──────────────────────────
        #
        # ROOT CAUSE OF "AI GOES SILENT AFTER FIRST SMS":
        #
        # When a lead receives our first outreach ("Do you own your home?")
        # and replies "Yes", the GHL webhook for that reply is a plain SMS
        # with direction=inbound, has_real_message=True.  It carries NO
        # source_chat_widget tag (that tag was only on the original form
        # submission webhook) and is_chat_widget_lead_payload() HARD EXCLUDES
        # has_real_message=True (it returns False before reaching Signal 6).
        #
        # Result with old code:
        #   _cw_payload_ok = False
        #   _tag_says_cw   = False (not in this payload)
        #   _is_lead_payload = False
        #   → "no_routing_signal_matched" no-op → 200 with no SMS → SILENT
        #
        # Fix: if a webhook is direction=inbound with a real message AND the
        # contact already has state (has been messaged before), ALWAYS route
        # to michael_agent().  This is the SMS conversation continuity path.
        # It is NOT the same as the first-outreach path — it's purely for
        # continuing an already-started conversation.
        #
        # Specifically NOT triggered for:
        #   • direction=outbound (GHL echo of our own sends)
        #   • has_real_message=False (placeholder/synthetic starters)
        #   • INITIAL stage with no messages (brand new, never messaged)
        #   • Booked contacts (handled by the BOOKED PATH above)
        _state_has_convo = bool(
            _state.get("messages") or
            _state["stage"] not in (Stage.INITIAL,)
        )
        # _state_has_convo is kept for diagnostic logging below but intentionally
        # NOT used as a routing gate — after a server restart the in-memory store
        # is empty so _state_has_convo would be False for every real customer reply,
        # silently dropping their message. Any real inbound message deserves a response.
        _is_inbound_sms_reply = (
            direction == "inbound" and
            bool(_has_real_msg) and
            not _is_booked_contact   # booked contacts have their own path
        )

        print(f"\n[ROUTING] {'─'*54}")
        print(f"[ROUTING]  contact_id          : {contact_id}")
        print(f"[ROUTING]  first name          : {first_name or '(not set)'!r}")
        print(f"[ROUTING]  last name           : {_log_last_name or '(not set)'!r}")
        print(f"[ROUTING]  full name           : {full_name!r}")
        print(f"[ROUTING]  phone               : {phone or '(not provided)'}")
        print(f"[ROUTING]  state stage         : {_state['stage']}")
        print(f"[ROUTING]  entry_path          : {_state.get('entry_path', 'unknown')!r}")
        print(f"[ROUTING]  lead_source         : {_state.get('lead_source') or lead_source or '(not set)'!r}")
        print(f"[ROUTING]  tags                : {tags}")
        print(f"[ROUTING]  ── MESSAGE ANALYSIS ─────────────────────────────")
        print(f"[ROUTING]  parsed[message]     : {_parsed_msg!r}")
        print(f"[ROUTING]  has_real_message     : {_has_real_msg}  (False if placeholder or empty)")
        print(f"[ROUTING]  msg_is_placeholder  : {_msg_is_placeholder}  (True = GHL synthetic starter)")
        print(f"[ROUTING]  direction            : {direction!r}")
        print(f"[ROUTING]  ── BOOKED DETECTION ─────────────────────────────")
        print(f"[ROUTING]  payload_says_booked : {_payload_says_booked}  reason={_payload_booking_reason!r}")
        print(f"[ROUTING]  tag_says_booked     : {_tag_says_booked}  (is_already_booked helper)")
        print(f"[ROUTING]  state_says_booked   : {_state_says_booked}  (flag={_state.get('appointment_booked')}, stage={_state['stage']})")
        print(f"[ROUTING]  is_fresh_form_sub   : {_is_fresh_form_sub}  ← True = booked path blocked for this lead")
        if _fresh_form_override:
            print(f"[ROUTING]  ⚡ OVERRIDE ACTIVE  : {_booked_override_reason}")
        print(f"[ROUTING]  _is_booked_contact  : {_is_booked_contact}")
        print(f"[ROUTING]  ── CHAT WIDGET / LEAD DETECTION ─────────────────")
        print(f"[ROUTING]  tag_says_cw         : {_tag_says_cw}  (source_chat_widget in tags)")
        print(f"[ROUTING]  payload_says_cw     : {_cw_payload_ok}  reason={_cw_payload_reason!r}")
        print(f"[ROUTING]  _is_lead_payload    : {_is_lead_payload}  ← tag OR payload signal")
        print(f"[ROUTING]  ── SMS REPLY CONTINUITY (FIX-8.A) ───────────────")
        print(f"[ROUTING]  direction=inbound   : {direction == 'inbound'}")
        print(f"[ROUTING]  has_real_msg        : {bool(_has_real_msg)}")
        print(f"[ROUTING]  state_has_convo     : {_state_has_convo}  (stage={_state['stage']}, msgs={len(_state.get('messages', []))})")
        print(f"[ROUTING]  _is_inbound_sms_reply: {_is_inbound_sms_reply}  ← [FIX-8.A] catches 'Yes/Yep/I do' replies")
        print(f"[ROUTING]  ── FINAL ROUTE ──────────────────────────────────")
        if _is_booked_contact:
            print(f"[ROUTING]  booked signals      : {', '.join(_booked_signals)}")
            print(f"[ROUTING]  → ROUTE             : *** BOOKED PATH ***")
            print(f"[ROUTING]  → PATH CLASS        : BOOKED_PATH | contact={contact_id} | phone={phone or '(n/a)'} | name={first_name or '(n/a)'}")
        elif _is_lead_payload:
            _cw_why = f"tag" if _tag_says_cw else f"payload ({_cw_payload_reason})"
            print(f"[ROUTING]  → ROUTE             : *** LEAD / CHAT WIDGET PATH ***  ({_cw_why})")
            print(f"[ROUTING]  → PATH CLASS        : NEW_LEAD_PATH | contact={contact_id} | phone={phone or '(n/a)'} | name={first_name or '(n/a)'}")
        elif _is_inbound_sms_reply:
            print(f"[ROUTING]  → ROUTE             : *** SMS REPLY CONTINUITY PATH ***  [FIX-8.A]")
            print(f"[ROUTING]  → PATH CLASS        : SMS_REPLY_PATH | contact={contact_id} | stage={_state['stage']} | msg={_parsed_msg[:40]!r}")
        else:
            print(f"[ROUTING]  → ROUTE             : SAFE NO-OP  (no booking, no lead, no reply signal)")
            print(f"[ROUTING]  → PATH CLASS        : NO_OP | contact={contact_id} | phone={phone or '(n/a)'} | name={first_name or '(n/a)'}")
        print(f"[ROUTING] {'─'*54}\n")

        if _is_booked_contact:
            # never send fresh lead opener
            msg_type = "booked"
            print(f"[DEBUG] msg_type resolved as: {msg_type}")
            state = _state

            if state["stage"] not in (Stage.BOOKED, Stage.DNC):
                state["stage"] = Stage.BOOKED
            if not state.get("contact_name") and full_name != "Unknown":
                state["contact_name"] = full_name
            if not state.get("phone") and phone:
                state["phone"] = phone

            # ── [FIX-7] Stamp appointment_booked IMMEDIATELY ─────────
            # Set BEFORE the async send so that any concurrent webhook
            # processed after this point sees appointment_booked=True and
            # is caught by PRIORITY GUARD 2 before reaching qualification.
            # This closes the race-condition window.
            if not state.get("appointment_booked"):
                state["appointment_booked"] = True
                print(f"[BOOKED]  appointment_booked = True  ← stamped now, protects against race condition")

            # ── Restore post-flow flags from GHL tags after server restart ────
            # When the Render process restarts, _state_store is wiped.  The next
            # Customer Replied webhook carries GHL contact tags (bill_received,
            # flow_complete) but state has no memory of prior flags.  Without
            # restoration, state.bill_reminder_sent=False → bill reminder is sent
            # again, or state.appointment_booked stays False → wrong routing.
            # Restoring from tags here is safe and idempotent (OR logic preserves
            # any already-true flags).
            _tag_set_lower = {t.lower().strip() for t in tags}
            if _tag_set_lower & {"bill_received", "flow_complete", "bill_received_confirmed"}:
                print(
                    f"[BOOKED]  ♻  State restored from GHL tags (server restart recovery): "
                    f"bill_reminder_sent, bill_received, final_confirmation_sent → True"
                )
                log.info(f"[{contact_id}] BOOKED: state flags restored from tags (post-restart)")
                state["bill_reminder_sent"]      = True
                state["bill_received"]            = True
                state["final_confirmation_sent"]  = True
                state["appointment_booked"]       = True
                if state["stage"] not in (Stage.BOOKED, Stage.DNC):
                    state["stage"] = Stage.BOOKED

            # ── [PATH-1/2] Stamp entry_path if not already set ────────
            # entry_path is NEVER overwritten once set.  This prevents a
            # chat-widget lead from being re-classified as a direct booking
            # if GHL fires a second appointment webhook after their state
            # has already been set to "chat_widget" by the SMS flow.
            #
            # A contact reaching this block with no entry_path set means
            # they arrived directly through the calendar booking page (no
            # prior chat widget interaction on record) — direct_booking path.
            if not state.get("entry_path") or state.get("entry_path") == "unknown":
                _has_prior_sms = bool(state.get("messages"))
                if _has_prior_sms:
                    # Has SMS history but entry_path not set — server restarted
                    # or older contact. Trust the messages list.
                    state["entry_path"] = "chat_widget"
                    print(f"[BOOKED]  entry_path set : chat_widget (inferred from existing SMS history)")
                else:
                    state["entry_path"] = "direct_booking"
                    print(f"[BOOKED]  entry_path set : direct_booking (no prior SMS history; first booking webhook)")
            else:
                print(f"[BOOKED]  entry_path keep: {state['entry_path']!r} (already set — not overwriting)")

            # Sync source_* boolean aliases with entry_path
            state["source_chat_widget"]   = (state.get("entry_path") == "chat_widget")
            state["source_direct_calendar"] = (state.get("entry_path") == "direct_booking")

            if not state.get("lead_source") and lead_source:
                state["lead_source"] = lead_source

            save_state(contact_id, state)

            # ── BOOKED PATH DIAGNOSTIC (look for this block in terminal) ──
            _diag_phone_state   = state.get("phone", "") or "(not in state)"
            _diag_phone_payload = phone or "(not in payload)"
            _diag_reminder_sent = state.get("bill_reminder_sent", False)
            _diag_ack_sent      = state.get("bill_ack_sent", False)
            _diag_photo_recvd   = state.get("bill_photo_received", False)
            _diag_stage         = state["stage"]
            _diag_entry_path    = state.get("entry_path", "unknown")
            _diag_why_entered = (
                f"signals: {', '.join(_booked_signals) if _booked_signals else 'UNKNOWN'}"
            )
            _diag_last_name = (
                full_name[len(first_name):].strip()
                if first_name and full_name.startswith(first_name)
                else (full_name.split()[-1] if full_name and " " in full_name else "(not set)")
            )
            print(f"\n{'!'*60}")
            print(f"[BOOKED] ▶▶▶  BOOKED APPOINTMENT PATH ENTERED")
            print(f"[BOOKED]  First name         : {first_name or '(not set)'!r}")
            print(f"[BOOKED]  Last name          : {_diag_last_name!r}")
            print(f"[BOOKED]  Full name          : {full_name!r}")
            print(f"[BOOKED]  Contact ID         : {contact_id!r}")
            print(f"[BOOKED]  Phone (state)      : {_diag_phone_state!r}")
            print(f"[BOOKED]  Phone (payload)    : {_diag_phone_payload!r}")
            print(f"[BOOKED]  Stage now          : {_diag_stage}")
            print(f"[BOOKED]  Entry path         : {_diag_entry_path!r}  ← chat_widget=warm msg | direct_booking=intro msg")
            print(f"[BOOKED]  Why entered        : {_diag_why_entered}")
            print(f"[BOOKED]  ── SUPPRESS CHECKS ────────────────────────────")
            print(f"[BOOKED]  bill_reminder_sent : {_diag_reminder_sent}  ← must be False to send reminder")
            print(f"[BOOKED]  bill_ack_sent      : {_diag_ack_sent}  ← must be False to send ack")
            print(f"[BOOKED]  bill_photo_received: {_diag_photo_recvd}")
            if _diag_reminder_sent:
                print(f"[BOOKED]  ⚠  bill_reminder_sent=True — will NOT send reminder; entering reply-handling branch")
            if _diag_ack_sent:
                print(f"[BOOKED]  ⚠  bill_ack_sent=True — will skip entirely (already acked)")
            print(f"{'!'*60}\n")
            log.info(
                f"[{contact_id}] BOOKED PATH | name={full_name!r} | "
                f"phone_state={_diag_phone_state!r} | phone_payload={_diag_phone_payload!r} | "
                f"stage={_diag_stage} | entry_path={_diag_entry_path!r} | "
                f"reminder_sent={_diag_reminder_sent} | ack_sent={_diag_ack_sent}"
            )
            # ── END DIAGNOSTIC ────────────────────────────────────────

            # ── Post-reminder reply handling ──────────────────────────
            # Bill reminder has already been sent.  Now we decide what to do
            # with whatever the contact just sent back.
            if state.get("bill_reminder_sent"):
                reply_text       = parsed["message"]
                _has_image       = parsed.get("has_attachment", False)
                _is_real_inbound = parsed.get("has_real_message", False)
                _msg_type_raw    = parsed.get("msg_type", "SMS")

                if state.get("bill_ack_sent"):
                    # Bill ack already sent — only block duplicate images/GHL echoes.
                    # Real text questions (cost, roof, reschedule) get PATH C answer.
                    if _has_image or not _is_real_inbound:
                        _skip_reason = "duplicate_bill_image" if _has_image else "non_real_webhook_after_ack"
                        log.info(f"[{contact_id}] Bill ack already sent — skipping {_skip_reason}")
                        print(
                            f"[BOOKED] ⛔  SKIP REASON: bill_ack_sent=True — {_skip_reason}"
                            f"\n[BOOKED]   intent=detected_as_non_question | msg={reply_text[:60]!r}"
                        )
                        return JSONResponse({
                            "status"            : "success",
                            "entered_booked_path": True,
                            "phone_found"       : bool(state.get("phone") or phone),
                            "sms_sent"          : False,
                            "skip_reason"       : _skip_reason,
                        })
                    # Real text → fall through to PATH C (booked follow-up)
                    print(
                        f"[BOOKED] ⚡ bill_ack_sent=True but real text question — routing to booked follow-up"
                        f"\n[BOOKED]   intent=post_flow_question | msg={reply_text[:60]!r}"
                    )
                    log.info(
                        f"[{contact_id}] POST-FLOW real text bypasses bill_ack_sent guard: {reply_text[:60]!r}"
                    )

                # ── [FIX-1/4/5/7] Pre-compute bill submission BEFORE routing ────
                # Done here so the diagnostic block below can show the result,
                # and so PATH A never has to re-evaluate it.
                _is_bill_sub, _bill_sub_reason = is_booked_bill_submission(
                    message            = reply_text,
                    has_attachment     = _has_image,
                    msg_type           = _msg_type_raw,
                    bill_reminder_sent = True,   # bill_reminder_sent is True here (we're inside that block)
                )

                # ── [FIX-3] BOOKED BILL DIAGNOSTIC BLOCK ──────────────
                # Printed BEFORE any routing decision. Look for "BOOKED BILL DIAG" in terminal.
                _diag_route_chosen = (
                    "BILL_ACK"                   if _is_bill_sub
                    else "SECOND_APPT_WEBHOOK_SKIP" if not _is_real_inbound
                    else "BOOKED_FOLLOWUP_AGENT"
                )
                print(f"\n[BOOKED BILL DIAG] {'═'*52}")
                print(f"[BOOKED BILL DIAG]  contact_id          : {contact_id!r}")
                print(f"[BOOKED BILL DIAG]  full_name           : {full_name!r}")
                print(f"[BOOKED BILL DIAG]  stage               : {state['stage']}")
                print(f"[BOOKED BILL DIAG]  appointment_booked  : {state.get('appointment_booked', False)}")
                print(f"[BOOKED BILL DIAG]  bill_reminder_sent  : {state.get('bill_reminder_sent', False)}")
                print(f"[BOOKED BILL DIAG]  bill_ack_sent       : {state.get('bill_ack_sent', False)}")
                print(f"[BOOKED BILL DIAG]  bill_photo_received : {state.get('bill_photo_received', False)}")
                print(f"[BOOKED BILL DIAG]  has_attachment      : {_has_image}")
                print(f"[BOOKED BILL DIAG]  has_real_message    : {_is_real_inbound}")
                print(f"[BOOKED BILL DIAG]  msg_type            : {_msg_type_raw!r}")
                print(f"[BOOKED BILL DIAG]  raw_message         : {reply_text!r}")
                print(f"[BOOKED BILL DIAG]  ── BILL DETECTION ───────────────────────────────")
                print(f"[BOOKED BILL DIAG]  is_booked_bill_sub  : {_is_bill_sub}")
                print(f"[BOOKED BILL DIAG]  detection_reason    : {_bill_sub_reason}")
                print(f"[BOOKED BILL DIAG]  ── ROUTE DECISION ───────────────────────────────")
                print(f"[BOOKED BILL DIAG]  ROUTE CHOSEN        : *** {_diag_route_chosen} ***")
                if _diag_route_chosen == "BILL_ACK":
                    print(f"[BOOKED BILL DIAG]  → sending bill ack immediately (bypasses Claude)")
                elif _diag_route_chosen == "SECOND_APPT_WEBHOOK_SKIP":
                    print(f"[BOOKED BILL DIAG]  → silent skip (GHL workflow/appt webhook, no real message)")
                else:
                    print(f"[BOOKED BILL DIAG]  → routing to booked follow-up agent (Claude)")
                print(f"[BOOKED BILL DIAG] {'═'*52}\n")
                log.info(
                    f"[{contact_id}] BOOKED BILL DIAG | "
                    f"has_attachment={_has_image} | msg_type={_msg_type_raw!r} | "
                    f"real_inbound={_is_real_inbound} | "
                    f"is_bill_sub={_is_bill_sub} | reason={_bill_sub_reason!r} | "
                    f"route={_diag_route_chosen}"
                )

                print(f"[BOOKED] ── Inbound reply received ──────────────────────")
                print(f"[BOOKED]  Contact        : {full_name!r} ({contact_id})")
                print(f"[BOOKED]  Saved stage    : {state['stage']}")
                print(f"[BOOKED]  Treated as     : BOOKED  (state-based, tag-independent)")
                print(f"[BOOKED]  Message text   : {reply_text!r}")
                print(f"[BOOKED]  Is MMS/image   : {_has_image}")
                print(f"[BOOKED]  Real inbound   : {_is_real_inbound}  (False = GHL appointment payload, not a lead reply)")
                print(f"[BOOKED]  Bill sub det.  : {_is_bill_sub}  ({_bill_sub_reason})")

                # ── PATH A: bill photo or bill-related text ────────────
                # [FIX-5] is_booked_bill_submission() is the SOLE gate here.
                # If it returns True, we NEVER call Claude — bill ack is sent immediately.
                # This prevents any "Not sure what you're sending" replies for booked leads.
                if _is_bill_sub:
                    print(f"[BOOKED]  Reply path     : BILL RESPONSE → sending bill ack")
                    _ack_phone = state.get("phone", "") or phone
                    if not _ack_phone:
                        print(f"[BOOKED] ⚠  phone is EMPTY — GHL will resolve from contactId only")
                        log.warning(f"[{contact_id}] Bill ack send: phone is empty")

                    ack = build_bill_ack_message(first_name=first_name, full_name=full_name)
                    print(f"[BOOKED]  Sending ack to {_ack_phone!r}: {ack!r}")

                    # ── [FIX-7] Set all bill-received flags BEFORE the async send ──
                    # Any concurrent webhook arriving after this save will be caught
                    # by PRIORITY GUARD 1 (bill_received=True) and silently no-op'd.
                    state["bill_ack_sent"]          = True
                    state["bill_photo_received"]     = True
                    state["bill_received"]           = True   # explicit durable flag
                    state["final_confirmation_sent"] = True   # ack IS the final confirmation
                    save_state(contact_id, state)

                    # ── BOOKED SMS ATTEMPT (bill ack) ───────────────────────
                    print(f"\n{'─'*60}")
                    print(f"[BOOKED SMS ATTEMPT]  type       : bill ack (flow completion)")
                    print(f"[BOOKED SMS ATTEMPT]  first name : {first_name or '(not set)'!r}")
                    print(f"[BOOKED SMS ATTEMPT]  last name  : {_diag_last_name!r}")
                    print(f"[BOOKED SMS ATTEMPT]  full name  : {full_name!r}")
                    print(f"[BOOKED SMS ATTEMPT]  contact_id : {contact_id!r}")
                    print(f"[BOOKED SMS ATTEMPT]  to number  : {_ack_phone!r}  {'⚠ EMPTY' if not _ack_phone else '✓'}")
                    print(f"[BOOKED SMS ATTEMPT]  message    : {ack!r}")
                    print(f"{'─'*60}")
                    log.info(
                        f"[{contact_id}] BOOKED SMS ATTEMPT (bill ack) | "
                        f"name={full_name!r} | to={_ack_phone!r}"
                    )
                    # ── End ATTEMPT block ───────────────────────────────────

                    _bill_ack_result: dict = {}
                    try:
                        _bill_ack_result = await send_sms_via_ghl(
                            contact_id, ack, to_number=_ack_phone,
                            kind=SendKind.BILL_ACK,          # booked-exempt
                        )
                    except Exception as sms_err:
                        _tb = traceback.format_exc()
                        log.error(
                            f"[{contact_id}] BOOKED SMS FAILED (bill ack) | "
                            f"name={full_name!r} | to={_ack_phone!r} | error={sms_err}"
                        )
                        print(f"\n[BOOKED SMS RESULT]  ❌ EXCEPTION — bill ack NOT sent")
                        print(f"[BOOKED SMS RESULT]  contact    : {full_name!r} ({contact_id})")
                        print(f"[BOOKED SMS RESULT]  to number  : {_ack_phone!r}")
                        print(f"[BOOKED SMS RESULT]  error      : {sms_err}")
                        print(f"[BOOKED SMS RESULT]  traceback  :\n{_tb}")
                        return JSONResponse({
                            "status"            : "success",
                            "entered_booked_path": True,
                            "phone_found"       : bool(_ack_phone),
                            "sms_sent"          : False,
                            "skip_reason"       : "bill_ack_sms_exception",
                            "error"             : str(sms_err),
                        })

                    # ── BOOKED SMS RESULT (bill ack) ────────────────────────
                    _ba_sent    = _bill_ack_result.get("sent", False)
                    _ba_deduped = _bill_ack_result.get("deduped", False)
                    print(f"\n[BOOKED SMS RESULT]  type       : bill ack")
                    print(f"[BOOKED SMS RESULT]  sent       : {_ba_sent}")
                    print(f"[BOOKED SMS RESULT]  deduped    : {_ba_deduped}")
                    print(f"[BOOKED SMS RESULT]  status_code: {_bill_ack_result.get('status_code')}")
                    print(f"[BOOKED SMS RESULT]  body       : {_bill_ack_result.get('response_body')}")
                    if _ba_deduped:
                        print(f"[BOOKED SMS RESULT]  ⚠ *** OUTBOUND DEDUP SUPPRESSED THIS SMS ***")
                        print(f"[BOOKED SMS RESULT]  Bill ack was NOT physically sent to {_ack_phone!r}")
                        log.warning(
                            f"[{contact_id}] BOOKED SMS deduped (bill ack) | "
                            f"name={full_name!r} | to={_ack_phone!r}"
                        )
                    elif _ba_sent:
                        print(f"[BOOKED SMS RESULT]  ✅ GHL accepted bill ack for {full_name!r}")
                        log.info(
                            f"[{contact_id}] BOOKED SMS sent OK (bill ack) | "
                            f"name={full_name!r} | to={_ack_phone!r}"
                        )
                    # ── End RESULT block ────────────────────────────────────

                    # Apply BILL_RECEIVED tag in GHL (non-fatal)
                    try:
                        await update_ghl_contact(contact_id, ["BILL_RECEIVED", "FLOW_COMPLETE"])
                    except Exception as _tag_err:
                        log.warning(f"[{contact_id}] GHL BILL_RECEIVED tag failed (non-fatal): {_tag_err}")
                        print(f"[BOOKED] ⚠  GHL BILL_RECEIVED tag failed (non-fatal): {_tag_err}")

                    print(f"[SMS] ✅ Bill ack sent to {full_name!r} at {_ack_phone!r}")
                    print(f"[BOOKED] 🏁 Flow complete — bill_received=True, final_confirmation_sent=True")
                    return JSONResponse({
                        "status"             : "success",
                        "entered_booked_path": True,
                        "phone_found"        : bool(_ack_phone),
                        "sms_sent"           : _ba_sent,
                        "sms_deduped"        : _ba_deduped,
                        "skip_reason"        : None,
                        "payload_type"       : "booked_contact_bill_ack",
                        "flow_complete"      : True,
                    })

                # ── PATH B: no real message body ───────────────────────
                # GHL fires a second appointment webhook (e.g. tag-handler
                # workflow) after the reminder was already sent.  This payload
                # has no message body (message="start") and is NOT a lead reply.
                if not _is_real_inbound:
                    print(f"[BOOKED]  Reply path     : SECOND_APPT_WEBHOOK_SKIP (no real message body)")
                    log.info(f"[{contact_id}] Second appointment webhook after reminder — skipping")
                    return JSONResponse({
                        "status"            : "success",
                        "entered_booked_path": True,
                        "phone_found"       : bool(state.get("phone") or phone),
                        "sms_sent"          : False,
                        "skip_reason"       : "second_appt_webhook_no_message_body",
                    })

                # ── PATH C: real lead reply, confirmed NOT a bill submission ──
                # Examples: "What time exactly?", "See you then", "Ok thanks"
                # is_booked_bill_submission() already returned False for this message,
                # so we can safely route to the booked follow-up agent.
                # [FIX-5] This path is ONLY reachable when _is_bill_sub=False AND
                # the message is a real inbound — Claude is called with the booked
                # follow-up prompt (locked-down, no booking links, no qualification).
                print(f"[BOOKED]  Reply path     : BOOKED_FOLLOWUP_AGENT (real inbound, not bill-related)")
                log.info(f"[{contact_id}] Booked follow-up reply (non-bill) — calling michael_agent")

                _followup_reply = michael_agent(contact_id, reply_text)
                if not _followup_reply:
                    print(f"[BOOKED] ⚠  michael_agent() returned None for booked follow-up")
                    log.warning(f"[{contact_id}] michael_agent returned None for booked follow-up | msg={reply_text!r}")
                    return JSONResponse({
                        "status"            : "success",
                        "entered_booked_path": True,
                        "sms_sent"          : False,
                        "skip_reason"       : "booked_followup_agent_returned_none",
                        "reply_text"        : reply_text,
                    })

                _fp_phone = state.get("phone", "") or phone
                print(f"[BOOKED]  Agent reply    : {_followup_reply!r}")
                print(f"[BOOKED]  Sending to     : {_fp_phone!r}")

                # ── BOOKED SMS ATTEMPT (follow-up) ──────────────────────
                print(f"\n{'─'*60}")
                print(f"[BOOKED SMS ATTEMPT]  type       : booked follow-up reply (PATH C)")
                print(f"[BOOKED SMS ATTEMPT]  first name : {first_name or '(not set)'!r}")
                print(f"[BOOKED SMS ATTEMPT]  last name  : {_diag_last_name!r}")
                print(f"[BOOKED SMS ATTEMPT]  full name  : {full_name!r}")
                print(f"[BOOKED SMS ATTEMPT]  contact_id : {contact_id!r}")
                print(f"[BOOKED SMS ATTEMPT]  to number  : {_fp_phone!r}  {'⚠ EMPTY' if not _fp_phone else '✓'}")
                print(f"[BOOKED SMS ATTEMPT]  message    : {_followup_reply!r}")
                print(f"{'─'*60}")
                log.info(
                    f"[{contact_id}] BOOKED SMS ATTEMPT (follow-up) | "
                    f"name={full_name!r} | to={_fp_phone!r}"
                )
                # ── End ATTEMPT block ───────────────────────────────────

                _followup_result: dict = {}
                try:
                    _followup_result = await send_sms_via_ghl(
                        contact_id, _followup_reply, to_number=_fp_phone,
                        kind=SendKind.BOOKED_REPLY,          # booked-exempt
                    )
                except Exception as sms_err:
                    _tb = traceback.format_exc()
                    log.error(
                        f"[{contact_id}] BOOKED SMS FAILED (follow-up) | "
                        f"name={full_name!r} | to={_fp_phone!r} | error={sms_err}"
                    )
                    print(f"\n[BOOKED SMS RESULT]  ❌ EXCEPTION — follow-up NOT sent")
                    print(f"[BOOKED SMS RESULT]  contact    : {full_name!r} ({contact_id})")
                    print(f"[BOOKED SMS RESULT]  to number  : {_fp_phone!r}")
                    print(f"[BOOKED SMS RESULT]  error      : {sms_err}")
                    print(f"[BOOKED SMS RESULT]  traceback  :\n{_tb}")
                    return JSONResponse({
                        "status"            : "success",
                        "entered_booked_path": True,
                        "sms_sent"          : False,
                        "skip_reason"       : "booked_followup_sms_exception",
                        "error"             : str(sms_err),
                    })

                # ── BOOKED SMS RESULT (follow-up) ───────────────────────
                _fu_sent    = _followup_result.get("sent", False)
                _fu_deduped = _followup_result.get("deduped", False)
                print(f"\n[BOOKED SMS RESULT]  type       : booked follow-up")
                print(f"[BOOKED SMS RESULT]  sent       : {_fu_sent}")
                print(f"[BOOKED SMS RESULT]  deduped    : {_fu_deduped}")
                print(f"[BOOKED SMS RESULT]  status_code: {_followup_result.get('status_code')}")
                print(f"[BOOKED SMS RESULT]  body       : {_followup_result.get('response_body')}")
                if _fu_deduped:
                    print(f"[BOOKED SMS RESULT]  ⚠ *** OUTBOUND DEDUP SUPPRESSED THIS SMS ***")
                    print(f"[BOOKED SMS RESULT]  Follow-up was NOT physically sent to {_fp_phone!r}")
                    log.warning(
                        f"[{contact_id}] BOOKED SMS deduped (follow-up) | "
                        f"name={full_name!r} | to={_fp_phone!r}"
                    )
                elif _fu_sent:
                    print(f"[BOOKED SMS RESULT]  ✅ GHL accepted follow-up for {full_name!r}")
                    log.info(
                        f"[{contact_id}] BOOKED SMS sent OK (follow-up) | "
                        f"name={full_name!r} | to={_fp_phone!r}"
                    )
                # ── End RESULT block ────────────────────────────────────

                print(f"[SMS] ✅ Booked follow-up reply sent to {full_name!r}")
                return JSONResponse({
                    "status"            : "success",
                    "entered_booked_path": True,
                    "phone_found"       : bool(_fp_phone),
                    "sms_sent"          : _fu_sent,
                    "sms_deduped"       : _fu_deduped,
                    "skip_reason"       : None,
                    "payload_type"      : "booked_contact_followup",
                    "reply"             : _followup_reply,
                })

            # First time seeing this booked contact — send bill reminder.
            # [PATH-1/2] Pick message variant based on entry_path:
            #   chat_widget    → sms_flow   ("Perfect, you're all set!")
            #   direct_booking → direct_booking ("Hey! You're all set...")
            #   unknown        → fall back to messages list as secondary signal
            _send_to_number = state.get("phone", "") or phone
            _entry_path     = state.get("entry_path", "unknown")
            _is_chat_widget = (
                _entry_path == "chat_widget" or
                (_entry_path == "unknown" and bool(state.get("messages")))
            )
            _bm_variant             = "sms_flow" if _is_chat_widget else "direct_booking"
            reminder, _bm_tpl_key   = build_booking_confirmation(_bm_variant, first_name=first_name, full_name=full_name)
            print(f"[BOOKED]  Message variant    : {_bm_variant!r} | template={_bm_tpl_key!r} | entry_path={_entry_path!r}")
            print(f"[BOOKED]  Message text       : {reminder!r}")

            if not _send_to_number:
                print(f"[BOOKED] ⚠  WARNING: phone is EMPTY for {full_name!r} ({contact_id})")
                print(f"[BOOKED]    state.phone  = {state.get('phone')!r}")
                print(f"[BOOKED]    payload phone = {phone!r}")
                print(f"[BOOKED]    GHL will attempt to resolve phone from contactId — may fail silently")
                log.warning(f"[{contact_id}] Bill reminder: phone is empty — relying on contactId resolution")

            print(f"\n{'>'*60}")
            print(f"[BOOKED] ✅  NO SUPPRESS CONDITIONS HIT — SENDING BILL REMINDER NOW")
            print(f"[BOOKED]  Contact  : {full_name!r} ({contact_id})")
            print(f"[BOOKED]  Send to  : {_send_to_number!r}  {'⚠ EMPTY' if not _send_to_number else '✓'}")
            print(f"[BOOKED]  Message  : {reminder!r}")
            print(f"{'>'*60}\n")
            log.info(f"[{contact_id}] SENDING bill reminder | to={_send_to_number!r} | name={full_name!r}")

            # Race-fix: commit flags BEFORE the async send.
            # Setting appointment_booked + bill_requested here (if not already set)
            # ensures any concurrent webhook is caught by the priority guards.
            state["bill_reminder_sent"]  = True
            state["bill_requested"]      = True    # explicit durable alias
            state["appointment_booked"]  = True    # durable flag (may already be set)
            save_state(contact_id, state)

            # ── BOOKED SMS ATTEMPT ──────────────────────────────────────
            print(f"\n{'─'*60}")
            print(f"[BOOKED SMS ATTEMPT]  type       : bill reminder (first booked SMS)")
            print(f"[BOOKED SMS ATTEMPT]  first name : {first_name or '(not set)'!r}")
            print(f"[BOOKED SMS ATTEMPT]  last name  : {_diag_last_name!r}")
            print(f"[BOOKED SMS ATTEMPT]  full name  : {full_name!r}")
            print(f"[BOOKED SMS ATTEMPT]  contact_id : {contact_id!r}")
            print(f"[BOOKED SMS ATTEMPT]  to number  : {_send_to_number!r}  {'⚠ EMPTY — GHL will try to resolve from contactId' if not _send_to_number else '✓'}")
            print(f"[BOOKED SMS ATTEMPT]  message    : {reminder!r}")
            print(f"{'─'*60}")
            log.info(
                f"[{contact_id}] BOOKED SMS ATTEMPT (bill reminder) | "
                f"name={full_name!r} | to={_send_to_number!r}"
            )
            # ── End ATTEMPT block ───────────────────────────────────────

            _bill_reminder_result: dict = {}
            try:
                _bill_reminder_result = await send_sms_via_ghl(
                    contact_id, reminder, to_number=_send_to_number,
                    kind=SendKind.BOOKED_REPLY,              # booked-exempt
                )
            except Exception as sms_err:
                _tb = traceback.format_exc()
                log.error(
                    f"[{contact_id}] BOOKED SMS FAILED (bill reminder) | "
                    f"name={full_name!r} | to={_send_to_number!r} | error={sms_err}"
                )
                print(f"\n[BOOKED SMS RESULT]  ❌ EXCEPTION — bill reminder NOT sent")
                print(f"[BOOKED SMS RESULT]  contact    : {full_name!r} ({contact_id})")
                print(f"[BOOKED SMS RESULT]  to number  : {_send_to_number!r}")
                print(f"[BOOKED SMS RESULT]  error      : {sms_err}")
                print(f"[BOOKED SMS RESULT]  traceback  :\n{_tb}")
                return JSONResponse({
                    "status"            : "success",
                    "entered_booked_path": True,
                    "phone_found"       : bool(_send_to_number),
                    "sms_sent"          : False,
                    "skip_reason"       : "bill_reminder_sms_exception",
                    "error"             : str(sms_err),
                    "contact_id"        : contact_id,
                    "phone_used"        : _send_to_number,
                })

            # ── BOOKED SMS RESULT ───────────────────────────────────────
            _br_sent    = _bill_reminder_result.get("sent", False)
            _br_deduped = _bill_reminder_result.get("deduped", False)
            print(f"\n[BOOKED SMS RESULT]  type       : bill reminder")
            print(f"[BOOKED SMS RESULT]  sent       : {_br_sent}")
            print(f"[BOOKED SMS RESULT]  deduped    : {_br_deduped}")
            print(f"[BOOKED SMS RESULT]  status_code: {_bill_reminder_result.get('status_code')}")
            print(f"[BOOKED SMS RESULT]  body       : {_bill_reminder_result.get('response_body')}")
            if _br_deduped:
                print(f"[BOOKED SMS RESULT]  ⚠ *** OUTBOUND DEDUP SUPPRESSED THIS SMS ***")
                print(f"[BOOKED SMS RESULT]  Likely cause: duplicate webhook fired within dedup window")
                print(f"[BOOKED SMS RESULT]  The bill reminder was NOT physically sent to {_send_to_number!r}")
                log.warning(
                    f"[{contact_id}] BOOKED SMS deduped (bill reminder) | "
                    f"name={full_name!r} | to={_send_to_number!r}"
                )
            elif _br_sent:
                print(f"[BOOKED SMS RESULT]  ✅ GHL accepted bill reminder for {full_name!r}")
                log.info(
                    f"[{contact_id}] BOOKED SMS sent OK (bill reminder) | "
                    f"name={full_name!r} | to={_send_to_number!r}"
                )
            # ── End RESULT block ────────────────────────────────────────

            try:
                await update_ghl_contact(contact_id, ["BILL_REMINDER_SENT"])
            except Exception as tag_err:
                log.warning(f"[{contact_id}] GHL tag update failed (non-fatal): {tag_err}")
                print(f"[BOOKED] ⚠  GHL tag update failed (non-fatal): {tag_err}")

            print(f"[SMS] ✅ Bill reminder sent to {full_name!r} at {_send_to_number!r}")
            return JSONResponse({
                "status"            : "success",
                "entered_booked_path": True,
                "phone_found"       : bool(_send_to_number),
                "sms_sent"          : _br_sent,
                "sms_deduped"       : _br_deduped,
                "skip_reason"       : None,
                "contact_id"        : contact_id,
                "name"              : full_name,
                "phone_used"        : _send_to_number,
                "payload_type"      : "booked_contact_bill_reminder",
            })

        # ── [FIX-8.A] Routing gate — lead payload OR inbound SMS reply ──────
        #
        # ORIGINAL BUG: gate was `if not _is_lead_payload` — this silently
        # dropped every inbound SMS reply ("Yes", "I do", "Yeah", etc.) because:
        #   • _is_lead_payload requires source_chat_widget tag OR form-shape payload
        #   • SMS reply webhooks have neither — they're plain direction=inbound messages
        #   • is_chat_widget_lead_payload() hard-excludes has_real_message=True
        # Result: any reply after the first outreach hit this gate and returned
        # no-op, leaving the lead permanently hanging.
        #
        # FIX: allow through when _is_inbound_sms_reply=True (computed above).
        # Only truly junk payloads (outbound echoes, unknown events, no contact info)
        # hit the no-op now.
        if not (_is_lead_payload or _is_inbound_sms_reply):
            # ── Fail-safe: if this was a real inbound message, scream about it ──
            if direction == "inbound" and _has_real_msg:
                _loud(
                    f"[FAILSAFE] !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
                    f"[FAILSAFE] INBOUND SMS REPLY DROPPED — THIS IS A BUG\n"
                    f"[FAILSAFE]   contact_id      : {contact_id}\n"
                    f"[FAILSAFE]   stage           : {_state['stage']}\n"
                    f"[FAILSAFE]   message         : {_parsed_msg!r}\n"
                    f"[FAILSAFE]   state_has_convo : {_state_has_convo}\n"
                    f"[FAILSAFE]   is_lead_payload : {_is_lead_payload}\n"
                    f"[FAILSAFE]   is_inbound_reply: {_is_inbound_sms_reply}\n"
                    f"[FAILSAFE]   sms_sent=False | send_sms_via_ghl_called=False\n"
                    f"[FAILSAFE] !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                )
            else:
                print(
                    f"[ROUTING] ℹ  No routing signal — direction={direction!r} | "
                    f"has_real_msg={_has_real_msg} | tags={tags} | "
                    f"payload_reason={_cw_payload_reason!r} — safe no-op",
                    flush=True,
                )
            log.info(
                f"[{contact_id}] No routing signal matched — "
                f"tag_cw={_tag_says_cw} | payload_cw={_cw_payload_ok} ({_cw_payload_reason}) | "
                f"inbound_reply={_is_inbound_sms_reply} | direction={direction!r}"
            )
            return JSONResponse({
                "status"              : "success",
                "skipped"             : True,
                "reason"              : "no_routing_signal_matched",
                "tags"                : tags,
                "contact_id"          : contact_id,
                "tag_says_cw"         : _tag_says_cw,
                "payload_says_cw"     : _cw_payload_ok,
                "payload_cw_reason"   : _cw_payload_reason,
                "is_inbound_sms_reply": _is_inbound_sms_reply,
                "sms_sent"            : False,
                "send_sms_called"     : False,
            })
        # ── _is_lead_payload=True OR _is_inbound_sms_reply=True — fall through ──

        # ── SAFETY GUARD: Never send booking-link text to a booked contact ───
        # Even with source_chat_widget, if state shows appointment_booked or
        # stage==BOOKED, we must NOT let this flow through to michael_agent()
        # which could produce a booking link message.  Log and no-op instead.
        #
        # [FIX-5.B] Exception: fresh form submissions (_is_fresh_form_sub=True)
        # bypass this guard.  A widget lead who submitted an address-form is by
        # definition not responding as a booked contact — their payload has no
        # booking proof fields.  Stale in-memory state from a prior contact with
        # the same phone number must not block a new lead from being greeted.
        _cw_state        = get_state(contact_id)
        _cw_state_booked = (
            _cw_state.get("appointment_booked") or
            _cw_state["stage"] in (Stage.BOOKED,)
        )
        if _cw_state_booked and not _is_fresh_form_sub:
            print(
                f"[GUARD] ⛔ CHAT-WIDGET BOOKED GUARD: contact {contact_id!r} is already booked "
                f"(state.appointment_booked={_cw_state.get('appointment_booked')}, "
                f"stage={_cw_state['stage']}) — blocking qualification path"
            )
            log.info(
                f"[{contact_id}] Chat-widget booked guard: contact already booked — no-op "
                f"(appointment_booked={_cw_state.get('appointment_booked')}, stage={_cw_state['stage']})"
            )
            return JSONResponse({
                "status"     : "success",
                "skipped"    : True,
                "reason"     : "chat_widget_booked_guard_already_booked",
                "contact_id" : contact_id,
            })
        elif _cw_state_booked and _is_fresh_form_sub:
            print(
                f"[GUARD] ℹ️  CHAT-WIDGET BOOKED GUARD bypassed — _is_fresh_form_sub=True "
                f"(payload has no booking proof; stale state suppressed → NEW_LEAD_PATH)"
            )
            # Reset stale booked state so this lead goes through fresh qualification.
            # The phone-map resolved to a prior booked contact, but this is a new
            # widget submission with a legitimate new lead.  Clear the booked flags
            # so the outreach path operates on a clean INITIAL slate.
            _cw_state["appointment_booked"] = False
            _cw_state["bill_reminder_sent"] = False
            _cw_state["bill_ack_sent"]      = False
            _cw_state["final_confirmation_sent"] = False
            _cw_state["stage"]              = Stage.INITIAL
            save_state(contact_id, _cw_state)
            log.info(
                f"[{contact_id}] Stale booked state reset for fresh widget lead "
                f"(_is_fresh_form_sub=True) — proceeding to NEW_LEAD_PATH"
            )

        # ── Detect payload type ───────────────────────────────────
        payload_type = detect_payload_type(parsed)
        print(f"[ROUTING] Payload type : {payload_type.upper()}")

        # ── Form submission fast-path ─────────────────────────────
        if payload_type == "form_submission":
            msg_type = "lead"
            print(f"[DEBUG] msg_type resolved as: {msg_type}")
            state = get_state(contact_id)
            if is_meta_lead(lead_source, tags):
                ev("META_LEAD_RECEIVED", contact_id, phone=phone, source=lead_source or "(none)")

            # Enrich state before stage check
            if not state.get("contact_name") and full_name != "Unknown":
                state["contact_name"] = full_name
            if not state.get("phone") and phone:
                state["phone"] = phone
            if not state.get("address") and address:
                state["address"] = address      # [ADAPT-4] save address for system prompt
            save_state(contact_id, state)

            # ── [META-B1] First-outreach suppression ──────────────────
            # ai-outreach-sent / ai-engaged are durable GHL tags, so this
            # holds across restarts where _state_store does not.
            if state["stage"] == Stage.INITIAL and (
                has_tag(tags, TAG_ENGAGED) or has_tag(tags, TAG_OUTREACH_SENT)
            ):
                # [STAGE-RESTORE] Recover the FURTHEST durable stage, not a
                # hardcoded ASK_OWNERSHIP — a contact who already received the
                # booking link must not be walked back into qualification.
                _restore_why = apply_restored_stage(contact_id, state, tags, _ghl_stage)
                if not state.get("entry_path") or state["entry_path"] == "unknown":
                    state["entry_path"] = "chat_widget"
                save_state(contact_id, state)
                ev(
                    "FIRST_OUTREACH_SUPPRESSED", contact_id,
                    reason="already_outreached_or_engaged",
                    path="form_submission",
                    restored_stage=str(state["stage"]),
                    restored_why=_restore_why,
                )
                return JSONResponse({
                    "status"      : "success",
                    "skipped"     : True,
                    "reason"      : "first_outreach_already_sent",
                    "contact_id"  : contact_id,
                })

            if state["stage"] == Stage.INITIAL:
                # ── [PATH-1] Stamp entry_path as chat_widget ──────────────
                # A form submission for an INITIAL contact always means the person
                # came through the chat widget on the website.  Direct bookings
                # never touch this branch — they arrive as appointment webhooks
                # and go through the appointment_booked tag path before reaching here.
                # entry_path is set here and NEVER overwritten, so even after
                # they book, the system knows they came through the chat widget.
                state["entry_path"]         = "chat_widget"
                state["source_chat_widget"] = True       # explicit boolean alias
                if not state.get("lead_source") and lead_source:
                    state["lead_source"] = lead_source
                print(f"[ROUTING] 🟢 entry_path=chat_widget / source_chat_widget=True stamped for {contact_id!r}")
                print(f"[ROUTING] 🆕 New unbooked contact — sending first outreach")
                log.info(f"[{contact_id}] New form contact — PATH-1 chat_widget — sending proactive first-outreach SMS")

                outreach = build_new_contact_outreach(
                    first_name=first_name,
                    full_name=full_name,
                    address=address,
                    lead_source=lead_source,
                    tags=tags,
                )
                print(f"[ROUTING] First-outreach: {outreach!r}")

                # Race-fix: advance stage + store history BEFORE the async send.
                # [DELIVERY] Snapshot first so the advance is reversible when the
                # message turns out never to have left.
                _snap = _snapshot_conversation(state)
                state["stage"]              = Stage.ASK_OWNERSHIP
                state["location_confirmed"] = True  # address from form → skip area question
                increment_message_count(state)
                # Prepend synthetic user context so history is valid for Anthropic API
                state["messages"].append({
                    "role"   : "user",
                    "content": (
                        f"[Lead submitted website chat widget — "
                        f"name: {full_name or 'Unknown'}, "
                        f"phone: {phone or 'not provided'}, "
                        f"address: {address or 'not provided'}]"
                    ),
                })
                state["messages"].append({"role": "assistant", "content": outreach})
                save_state(contact_id, state)

                try:
                    _send = await send_sms_via_ghl(contact_id, outreach, to_number=phone,
                                                   kind=SendKind.OUTREACH)
                except Exception as sms_err:
                    log.error(f"[{contact_id}] First-outreach SMS failed: {sms_err}")
                    print(f"[ERROR] First-outreach SMS failed:\n{traceback.format_exc()}")
                    _rollback_conversation(contact_id, state, _snap,
                                           {"status": SendStatus.REJECTED.value,
                                            "reason": f"exception:{type(sms_err).__name__}"},
                                           "first_outreach")
                    ev("FIRST_OUTREACH_NOT_SENT", contact_id, phone=phone,
                       status=SendStatus.REJECTED.value, reason="exception")
                    return JSONResponse({"status": "success", "note": "first-outreach SMS failed",
                                         "sms_status": SendStatus.REJECTED.value})

                # ── [DELIVERY] Only an ACCEPTED send may be booked as outreach ──
                # Previously the result was discarded: a suppressed or rejected
                # message still advanced the stage, appended the question to
                # Claude's history and wrote ai-outreach-sent — so the system
                # believed a lead had been contacted when it had not.
                if _send.get("status") not in _STATUS_ADVANCES_STATE:
                    _rollback_conversation(contact_id, state, _snap, _send, "first_outreach")
                    ev(
                        "FIRST_OUTREACH_NOT_SENT", contact_id,
                        phone=phone,
                        status=_send.get("status"),
                        reason=_send.get("reason") or _send.get("why") or "(none)",
                        note="state_rolled_back; ai-outreach-sent NOT written",
                    )
                    return JSONResponse({
                        "status"    : "success",
                        "replied"   : False,
                        "sms_status": _send.get("status"),
                        "reason"    : _send.get("reason") or _send.get("why"),
                    })

                _record_send_result(contact_id, state, _send)
                print(f"[SMS] ✅ First-outreach ACCEPTED by GHL for {full_name!r}")
                # [META-B1] Durable marker — written ONLY after acceptance, so a
                # lead whose first SMS never left is not marked as outreached
                # (which would also start the GHL nurture sequence).
                try:
                    await add_ghl_tags(contact_id, [TAG_OUTREACH_SENT])
                except Exception as _ot_err:
                    log.warning(f"[{contact_id}] outreach tag write failed (non-fatal): {_ot_err}")
                ev(
                    "FIRST_OUTREACH_SENT", contact_id,
                    phone=phone,
                    meta=is_meta_lead(lead_source, tags),
                    path="form_submission",
                    status=_send.get("status"),
                    message_id=_send.get("message_id") or "(none)",
                )
                return JSONResponse({"status": "success", "replied": True, "reply": outreach, "payload_type": "form_submission_new_contact"})

            elif state["stage"] == Stage.SEND_BOOKING:
                # Qualified contact just booked via calendar — transition to BOOKED
                if not is_appointment_payload(body):
                    print(f"[ROUTING] ⚠ SEND_BOOKING contact but no appointment fields — not a booking event, skipping")
                    return JSONResponse({"status": "success", "skipped": True, "reason": "send_booking_not_appt_payload"})

                print(f"[ROUTING] 📅 Qualified contact just booked — sending bill reminder")
                state["stage"]             = Stage.BOOKED
                state["bill_reminder_sent"] = True
                state["bill_requested"]    = True    # explicit durable alias
                state["appointment_booked"] = True   # explicit durable flag
                save_state(contact_id, state)

                # [PATH-1/2] Pick variant using entry_path
                _ep2         = state.get("entry_path", "unknown")
                _is_cw_2     = (
                    _ep2 == "chat_widget" or
                    (_ep2 == "unknown" and bool(state.get("messages")))
                )
                _bm2_variant           = "sms_flow" if _is_cw_2 else "direct_booking"
                reminder, _bm2_tpl_key = build_booking_confirmation(_bm2_variant, first_name=first_name, full_name=full_name)
                print(f"[BOOKED]  Message variant    : {_bm2_variant!r} | template={_bm2_tpl_key!r} | entry_path={_ep2!r}")
                print(f"[BOOKED]  Message text       : {reminder!r}")
                try:
                    await send_sms_via_ghl(contact_id, reminder, to_number=state.get("phone", "") or phone,
                                           kind=SendKind.BOOKED_REPLY)
                except Exception as sms_err:
                    log.error(f"[{contact_id}] Bill reminder (SEND_BOOKING) failed: {sms_err}")
                    print(f"[ERROR] Bill reminder SMS failed:\n{traceback.format_exc()}")
                    return JSONResponse({"status": "success", "note": "bill reminder SMS failed"})

                print(f"[SMS] ✅ Bill reminder sent (SEND_BOOKING→BOOKED) to {full_name!r}")
                return JSONResponse({"status": "success", "replied": True, "reply": reminder, "payload_type": "form_submission_booking_confirmed"})

            else:
                # Contact already active at some other stage.
                # Critical guard: if direction=inbound with a real message, this could be an
                # inbound reply that was misclassified as form_submission because the SMS body
                # was in a nested body["message"]["body"] field (GHL Customer Replied format)
                # rather than a top-level field that _safe_str() would parse as a string.
                # Fall through to the chat-widget reply handler instead of silently dropping.
                if direction == "inbound" and parsed.get("has_real_message"):
                    _loud(
                        f"[PIPELINE] ⚠ INBOUND-FALLTHROUGH: form_submission classified but "
                        f"direction=inbound + has_real_message=True at stage={state['stage']} — "
                        f"routing to michael_agent() instead of silent drop | contact={contact_id}"
                    )
                    log.warning(
                        f"[{contact_id}] INBOUND-FALLTHROUGH: form_submission at stage={state['stage']} "
                        f"has_real_message=True — routing to michael_agent() (possible GHL nested msg format)"
                    )
                    # Do NOT return — fall through to the chat-widget reply handler below
                else:
                    print(f"[ROUTING] Form submission for contact at stage={state['stage']} — skipping (already active)")
                    log.info(f"[{contact_id}] form_submission_contact_already_active | stage={state['stage']} | direction={direction}")
                    return JSONResponse({"status": "success", "skipped": True, "reason": "form_submission_contact_already_active"})

        # ── Chat widget: real inbound SMS ─────────────────────────
        msg_type = "lead"
        print(f"[DEBUG] msg_type resolved as: {msg_type}")
        inbound_text = parsed["message"]

        print(f"\n{'#'*60}")
        print(f"[INBOUND] 📲 CHAT WIDGET — REAL INBOUND SMS")
        _cw_state  = get_state(contact_id)
        print(f"[INBOUND]  Contact ID  : {contact_id}")
        print(f"[INBOUND]  Name        : {full_name!r}")
        print(f"[INBOUND]  Phone       : {phone or '(not provided)'}")
        print(f"[INBOUND]  Stage       : {_cw_state['stage']}")
        print(f"[INBOUND]  Homeowner   : {_cw_state.get('homeowner', 'unknown')}")
        print(f"[INBOUND]  Bill        : {_cw_state.get('monthly_bill', 'unknown') or 'unknown'}")
        print(f"[INBOUND]  Message     : {inbound_text!r}")
        print(f"{'#'*60}\n")

        # Inbound dedup already ran universally above — no per-path check needed here.

        # ── Enrich state ──────────────────────────────────────────
        state = get_state(contact_id)
        if not state.get("contact_name") and full_name != "Unknown":
            state["contact_name"] = full_name
        if not state.get("phone") and phone:
            state["phone"] = phone
        if not state.get("address") and address:
            state["address"] = address
        if booking_detected:
            state["booking_detected"] = True
        save_state(contact_id, state)

        log.info(f"[{contact_id}] Inbound | type={payload_type} | name={full_name!r} | msg={inbound_text!r}")

        # ── MMS / image ack (non-booked contacts) ────────────────
        # GHL fires a webhook when a lead sends an image (e.g. electric bill
        # photo during qualification).  The message body is empty or missing;
        # `has_attachment` is True.  We respond immediately and skip the full
        # qualification agent — the lead just shared an image, not a text reply.
        #
        # NOTE: BOOKED contacts sending images are handled upstream in the
        # appointment_booked tag path, which calls build_bill_ack_message().  This block
        # only fires for contacts who are NOT yet booked.
        if parsed.get("has_attachment"):
            _mms_ack = "Perfect, got it — this helps a ton. I'll review everything before I come by \U0001f44d"
            print(f"[ROUTING] \U0001f4f7 MMS/image received — sending ack")
            log.info(f"[{contact_id}] MMS attachment received — sending ack")
            if not is_duplicate_outbound(contact_id, _mms_ack):
                try:
                    await send_sms_via_ghl(contact_id, _mms_ack, to_number=state.get("phone", "") or phone,
                                           kind=SendKind.BILL_ACK)
                    print(f"[SMS] \u2705 MMS ack sent to {full_name!r}")
                except Exception as _mms_err:
                    log.error(f"[{contact_id}] MMS ack SMS failed: {_mms_err}")
                    print(f"[ERROR] MMS ack SMS failed:\n{traceback.format_exc()}")
            else:
                print(f"[DUPLICATE] MMS ack already sent this minute — skipping")
            return JSONResponse({"status": "success", "replied": True, "reply": _mms_ack, "payload_type": "mms_ack"})

        # ── [FIX-5.A] INITIAL-stage fresh lead with real message ─────────
        #
        # When a chat widget lead sends a real opening message (e.g. "I want to
        # see if I qualify for solar savings"), detect_payload_type returns
        # "chat_widget" because has_real_message=True.  Before v3.5, the code
        # fell through to michael_agent() — Claude would sometimes generate a
        # generic closing confirmation ("You're all set... a specialist will
        # reach out shortly") instead of driving the conversation forward.
        #
        # FIX: For any INITIAL-stage contact on this path, bypass Claude and
        # send the hardcoded first-outreach message (ownership question with
        # address), exactly as we do in the form_submission fast-path.
        # Claude is never appropriate for a brand-new lead's first exchange —
        # Michael always drives the opening.
        #
        # Business rules enforced here:
        #   • Never say "someone will reach out shortly"
        #   • Never close the conversation on the first message
        #   • Always continue with the qualification conversation
        #   • PATH: NEW_LEAD_PATH (logged explicitly)
        # ── [META-B1] First-outreach suppression (real-message path) ──
        # Restart recovery: outreach already went out, so re-sending the
        # intro would repeat a question the lead has already seen.
        # Advance the stage and fall through to normal Claude handling.
        if state["stage"] == Stage.INITIAL and (
            has_tag(tags, TAG_ENGAGED) or has_tag(tags, TAG_OUTREACH_SENT)
        ):
            # [STAGE-RESTORE] This is the branch that asked a fully-qualified
            # contact "Are you the homeowner?" after a deploy wiped memory.
            # Restore the furthest stage GHL can prove instead.
            _restore_why = apply_restored_stage(contact_id, state, tags, _ghl_stage)
            if not state.get("entry_path") or state["entry_path"] == "unknown":
                state["entry_path"] = "chat_widget"
            save_state(contact_id, state)
            ev(
                "FIRST_OUTREACH_SUPPRESSED", contact_id,
                reason="already_outreached_or_engaged",
                path="real_message",
                restored_stage=str(state["stage"]),
                restored_why=_restore_why,
            )

        if state["stage"] == Stage.INITIAL:
            print(f"\n{'─'*60}")
            print(f"[ROUTING] 🆕 NEW_LEAD_PATH — INITIAL stage, real inbound chat widget message")
            print(f"[ROUTING]  → PATH CLASS   : NEW_LEAD_PATH | contact={contact_id} | phone={phone or '(n/a)'} | name={first_name or '(n/a)'}")
            print(f"[ROUTING]  contact_id     : {contact_id}")
            print(f"[ROUTING]  first_name     : {first_name or '(not set)'!r}")
            print(f"[ROUTING]  address        : {address or '(not provided)'!r}")
            print(f"[ROUTING]  inbound msg    : {inbound_text!r}")
            print(f"[ROUTING]  Claude skipped : True — hardcoded outreach sent instead")
            print(f"{'─'*60}\n")
            log.info(
                f"[{contact_id}] NEW_LEAD_PATH (real msg at INITIAL) | "
                f"name={full_name!r} | phone={phone!r} | address={address!r}"
            )

            outreach = build_new_contact_outreach(
                first_name=first_name,
                full_name=full_name,
                address=address,
                lead_source=lead_source,
                tags=tags,
            )
            print(f"[ROUTING] First-outreach (chat_widget real msg): {outreach!r}")

            # Stamp state before async send — race-condition safe
            state["entry_path"]         = state.get("entry_path") or "chat_widget"
            state["source_chat_widget"] = True
            state["stage"]              = Stage.ASK_OWNERSHIP
            state["location_confirmed"] = True   # address submitted → area implicitly confirmed
            if not state.get("lead_source") and lead_source:
                state["lead_source"] = lead_source
            increment_message_count(state)
            # Store synthetic user context + outreach in conversation history
            state["messages"].append({
                "role"   : "user",
                "content": (
                    f"[Chat widget lead (real msg) — "
                    f"name: {full_name or 'Unknown'}, "
                    f"phone: {phone or 'not provided'}, "
                    f"address: {address or 'not provided'}, "
                    f"opening message: {inbound_text!r}]"
                ),
            })
            state["messages"].append({"role": "assistant", "content": outreach})
            save_state(contact_id, state)

            try:
                _send = await send_sms_via_ghl(contact_id, outreach, to_number=phone,
                                               kind=SendKind.OUTREACH)
                if _send.get("status") not in _STATUS_ADVANCES_STATE:
                    ev("FIRST_OUTREACH_NOT_SENT", contact_id, path="real_message",
                       status=_send.get("status"),
                       reason=_send.get("reason") or _send.get("why") or "(none)")
                else:
                    _record_send_result(contact_id, state, _send)
                    ev("FIRST_OUTREACH_SENT", contact_id, path="real_message",
                       status=_send.get("status"),
                       message_id=_send.get("message_id") or "(none)")
            except Exception as sms_err:
                log.error(f"[{contact_id}] First-outreach (chat_widget real msg) SMS failed: {sms_err}")
                print(f"[ERROR] First-outreach SMS failed:\n{traceback.format_exc()}")
                return JSONResponse({
                    "status"       : "success",
                    "note"         : "first-outreach SMS failed (chat_widget real msg)",
                    "path"         : "NEW_LEAD_PATH",
                })

            print(f"[SMS] ✅ First-outreach sent to {full_name!r} ({contact_id})")
            return JSONResponse({
                "status"       : "success",
                "replied"      : True,
                "reply"        : outreach,
                "payload_type" : "chat_widget_new_lead_real_msg",
                "path"         : "NEW_LEAD_PATH",
            })
        # ── INITIAL stage handled above — below here: ASK_OWNERSHIP and beyond ──

        # ── [FIX-8.B] REPLY HANDLING — hard logging before EVERY step ────────
        #
        # This is the SMS conversation continuity path.  A lead replied to our
        # outreach — this block should ALWAYS run for any inbound SMS reply after
        # the first outreach was sent.  Log everything before calling the agent.
        _pre_agent_state = get_state(contact_id)
        _pre_stage       = _pre_agent_state["stage"]
        _pre_homeowner   = _pre_agent_state.get("homeowner", "unknown")
        _pre_bill        = _pre_agent_state.get("monthly_bill") or "unknown"
        _pre_loc         = _pre_agent_state.get("location_confirmed", False)
        _pre_msgs        = len(_pre_agent_state.get("messages", []))

        print(f"\n{'*'*64}", flush=True)
        print(f"[REPLY] *** INBOUND SMS REPLY — PROCESSING NOW ***", flush=True)
        print(f"[REPLY]  contact_id      : {contact_id}", flush=True)
        print(f"[REPLY]  full_name       : {full_name!r}", flush=True)
        print(f"[REPLY]  phone           : {phone or '(not provided)'}", flush=True)
        print(f"[REPLY]  inbound text    : {inbound_text!r}", flush=True)
        print(f"[REPLY]  ── PRIOR STATE ─────────────────────────────────", flush=True)
        print(f"[REPLY]  prior stage     : {_pre_stage}  ← was this the ownership question?", flush=True)
        print(f"[REPLY]  prior homeowner : {_pre_homeowner}  (None = not yet confirmed)", flush=True)
        print(f"[REPLY]  prior bill      : {_pre_bill}", flush=True)
        print(f"[REPLY]  loc_confirmed   : {_pre_loc}", flush=True)
        print(f"[REPLY]  msg history len : {_pre_msgs}", flush=True)
        print(f"[REPLY]  routing via     : {'SMS_REPLY_PATH [FIX-8.A]' if _is_inbound_sms_reply and not _is_lead_payload else 'LEAD_PATH'}", flush=True)
        print(f"{'*'*64}\n", flush=True)

        # ── Booked-stage bill photo handler ───────────────────────
        # If contact is in a booked pipeline stage AND sent an image/attachment,
        # interpret as a utility bill photo and send the canned ack immediately.
        # Never run the qualification agent for this case.
        if _ghl_stage in GHL_BOOKED_STAGES and _has_media:
            _bill_ack    = "Perfect, got it. I'll take a look before I come by 👍"
            _ack_phone   = get_state(contact_id).get("phone", "") or phone
            print(
                f"[PIPELINE] 📸 Booked media received (stage={_ghl_stage!r}) — sending bill ack | contact={contact_id}",
                flush=True,
            )
            log.info(f"[{contact_id}] Booked bill photo ack | stage={_ghl_stage!r}")
            if not is_duplicate_outbound(contact_id, _bill_ack):
                try:
                    await send_sms_via_ghl(contact_id, _bill_ack, to_number=_ack_phone,
                                           kind=SendKind.BILL_ACK)
                except Exception as _ack_err:
                    print(f"[PIPELINE] ❌ Bill ack send failed: {_ack_err}", flush=True)
            return JSONResponse({"status": "success", "action": "bill_photo_ack", "stage": _ghl_stage})

        # ── [CONVERT-1] Resolve what we actually said last ────────
        # Local memory first (free). GHL only when memory is empty AND the
        # reply is the ambiguous kind whose meaning depends on the question —
        # so the lookup never runs for ordinary messages.
        _prior_outbound = last_outbound_message(_pre_agent_state)
        if not _prior_outbound and (
            is_affirmative_reply(inbound_text) or is_explicit_scheduling_request(inbound_text)
        ):
            _prior_outbound = await fetch_last_outbound_message(contact_id)
        if _prior_outbound:
            print(
                f"[REPLY]  prior outbound  : {classify_outbound_intent(_prior_outbound)} | "
                f"{_prior_outbound[:90]!r}",
                flush=True,
            )
        else:
            print(f"[REPLY]  prior outbound  : (unknown — no local history, none recovered)", flush=True)

        # ── Run the agent ─────────────────────────────────────────
        print(f"[REPLY] ▶ Calling michael_agent() | stage={_pre_stage} | ghl_stage={_ghl_stage!r} | msg={inbound_text!r}", flush=True)
        reply = michael_agent(contact_id, inbound_text, ghl_pipeline_stage=_ghl_stage,
                              ghl_tags=_ghl_tags, prior_outbound=_prior_outbound)

        # ── Post-agent state snapshot ─────────────────────────────
        _post_state    = get_state(contact_id)
        _post_stage    = _post_state["stage"]
        _post_homeowner= _post_state.get("homeowner", "unknown")

        # ── Diagnose None reply ───────────────────────────────────
        if reply is None:
            print(f"\n[REPLY] !! michael_agent() returned None — NO SMS will be sent", flush=True)
            print(f"[REPLY]    skip_reason      : agent_returned_none", flush=True)
            print(f"[REPLY]    stage before     : {_pre_stage}", flush=True)
            print(f"[REPLY]    stage after      : {_post_stage}", flush=True)
            print(f"[REPLY]    homeowner before : {_pre_homeowner}", flush=True)
            print(f"[REPLY]    homeowner after  : {_post_homeowner}", flush=True)
            print(f"[REPLY]    msgs_today       : {_post_state.get('msgs_today')}", flush=True)
            print(f"[REPLY]    final_conf_sent  : {_post_state.get('final_confirmation_sent')}", flush=True)
            print(f"[REPLY]    bill_received    : {_post_state.get('bill_received')}", flush=True)
            print(f"[REPLY]    send_sms_called  : False", flush=True)
            print(f"[REPLY]    Possible causes:", flush=True)
            print(f"[REPLY]      • stage is BOOKED/DNC/DISQUALIFIED → intentional silence", flush=True)
            print(f"[REPLY]      • daily message limit reached", flush=True)
            print(f"[REPLY]      • Claude API error (see [AGENT] lines above)", flush=True)
            print(f"[REPLY]      • flow already complete (final_confirmation_sent/bill_received)", flush=True)
            log.warning(f"[{contact_id}] michael_agent() returned None | stage={_post_stage}")

            # ── [FAILSAFE] Send recovery message for unexpected None ──────────
            # If this was a real inbound SMS to an active (non-terminal) contact and
            # the agent returned None for a non-intentional reason (Claude error, limit
            # glitch, etc.), send a soft recovery message so the lead isn't left in silence.
            # Intentional silences (DNC, DISQUALIFIED) are excluded.
            _failsafe_terminal = {Stage.DNC, Stage.DISQUALIFIED}
            _post_stage_val    = _post_state["stage"]
            _failsafe_intended_silence = (
                _post_stage_val in _failsafe_terminal or
                _post_state.get("final_confirmation_sent") or
                _post_state.get("bill_received")
            )
            if direction == "inbound" and bool(_has_real_msg) and not _failsafe_intended_silence:
                _failsafe_msg   = "Sorry — had a small tech hiccup on my end. Mind sending that one more time?"
                _failsafe_phone = _post_state.get("phone", "") or phone
                print(f"[FAILSAFE] ⚡ Sending recovery SMS — agent returned None for real inbound | stage={_post_stage_val}", flush=True)
                log.warning(f"[{contact_id}] FAILSAFE: sending tech hiccup SMS (agent None, stage={_post_stage_val})")
                if not is_duplicate_outbound(contact_id, _failsafe_msg):
                    try:
                        await send_sms_via_ghl(contact_id, _failsafe_msg, to_number=_failsafe_phone,
                                               kind=SendKind.SYSTEM)
                        print(f"[FAILSAFE] ✅ Recovery SMS sent to {full_name!r} ({contact_id})", flush=True)
                    except Exception as _fs_err:
                        print(f"[FAILSAFE] ❌ Recovery SMS failed: {_fs_err}", flush=True)
                        log.error(f"[{contact_id}] FAILSAFE send failed: {_fs_err}")
                else:
                    print(f"[FAILSAFE] ⚠ Recovery SMS suppressed by outbound dedup", flush=True)
            else:
                print(
                    f"[FAILSAFE] ℹ No recovery SMS — "
                    f"intended_silence={_failsafe_intended_silence} | "
                    f"direction={direction} | has_real_msg={bool(_has_real_msg)}",
                    flush=True,
                )

            return JSONResponse({
                "status"         : "success",
                "replied"        : False,
                "reason"         : f"agent_returned_none (stage={_post_stage})",
                "skip_reason"    : "agent_returned_none",
                "stage_before"   : str(_pre_stage),
                "stage_after"    : str(_post_stage),
                "sms_sent"       : False,
                "send_sms_called": False,
            })

        print(f"[REPLY] ✅ michael_agent() reply: {reply!r}", flush=True)
        print(f"[REPLY]    stage before    : {_pre_stage}", flush=True)
        print(f"[REPLY]    stage after     : {_post_stage}", flush=True)
        print(f"[REPLY]    homeowner before: {_pre_homeowner}", flush=True)
        print(f"[REPLY]    homeowner after : {_post_homeowner}", flush=True)
        print(f"[REPLY]    send_sms_called : True (about to call)", flush=True)

        state         = _post_state
        contact_phone = state.get("phone", "") or phone
        ghl_tags      = resolve_ghl_tags(state["stage"], qualified=bool(state.get("qualified")))
        print(f"[REPLY]  Phone             : {contact_phone or '(not available)'}", flush=True)
        print(f"[REPLY]  GHL tags to apply : {ghl_tags}", flush=True)
        print(f"[REPLY]  ▶ Calling send_sms_via_ghl() now ...", flush=True)

        # ── Send SMS ──────────────────────────────────────────────
        try:
            _reply_kind = classify_reply_kind(state)
            _send = await send_sms_via_ghl(contact_id, reply, to_number=contact_phone,
                                           kind=_reply_kind)
            # ── [DELIVERY] Record what actually happened ──────────────────
            # The reply was already appended to state["messages"] inside
            # michael_agent(). We do NOT rewind that for an ACCEPTED send —
            # the message is in flight and rewinding risks a duplicate reply
            # if it lands. We DO record the true status so nothing downstream
            # mistakes "queued" for "the homeowner read it".
            _record_send_result(contact_id, state, _send)
            if _send.get("status") not in _STATUS_ADVANCES_STATE:
                ev(
                    "REPLY_NOT_SENT", contact_id,
                    kind=_reply_kind.value,
                    status=_send.get("status"),
                    reason=_send.get("reason") or _send.get("why") or "(none)",
                    note="homeowner did NOT receive this question",
                )
            else:
                ev(
                    "REPLY_ACCEPTED", contact_id,
                    kind=_reply_kind.value,
                    message_id=_send.get("message_id") or "(none)",
                )
        except Exception as sms_err:
            log.error(f"[{contact_id}] GHL SMS send failed: {sms_err}")
            print(f"[REPLY] !! SMS send FAILED — lead reply not delivered", flush=True)
            print(f"[ERROR] SMS send failed:\n{traceback.format_exc()}")
            # [DELIVERY] An exception here means GHL refused the message outright.
            # Record that truthfully — without it the contact keeps whatever status
            # the PREVIOUS successful send left behind, so a failed reply would
            # still read as "accepted".
            try:
                _record_send_result(contact_id, state, {
                    "status": SendStatus.REJECTED.value, "message_id": "",
                })
                ev("REPLY_NOT_SENT", contact_id,
                   status=SendStatus.REJECTED.value,
                   reason=f"exception:{type(sms_err).__name__}",
                   note="homeowner did NOT receive this question")
            except Exception:
                pass
            return JSONResponse({
                "status"         : "success",
                "replied"        : False,
                "note"           : "SMS send failed — check [SMS] lines",
                "skip_reason"    : "sms_exception",
                "sms_sent"       : False,
                "send_sms_called": True,
            })

        # ── Update GHL tags (non-fatal) ───────────────────────────
        if ghl_tags:
            try:
                print(f"[ROUTING] Adding GHL tags: {ghl_tags}")
                # [META-B1] add_ghl_tags (not update_ghl_contact) — preserves
                # existing markers instead of replacing the whole tag array.
                await add_ghl_tags(contact_id, ghl_tags)
                if Stage.DNC in (state.get("stage"),):
                    ev("DNC_SET", contact_id, source="stage_tag_write")
            except Exception as tag_err:
                log.warning(f"[{contact_id}] GHL tag update failed (non-fatal): {tag_err}")

        # ── [FIX-6.C] Final msg_type sanity check + unknown-event fallback SMS ──
        #
        # If msg_type is still "unknown" at this point it means the payload
        # passed all guards but didn't match any routing branch.  This should
        # never happen in production — but when it does (e.g. new GHL event
        # types, unexpected payload shapes), the OLD code just logged and
        # returned silently.  No SMS was ever sent.
        #
        # NEW behavior: if we have a contact_id, a phone number, and the
        # contact is at INITIAL stage (never been messaged), attempt the
        # first-outreach SMS anyway.  This is the "unknown event, but looks
        # like a fresh lead" fallback — always prefer sending over silence.
        if msg_type == "unknown":
            print(f"\n[FALLBACK] ⚠ msg_type=unknown — routing did not match any branch")
            print(f"[FALLBACK]   contact_id  : {contact_id}")
            print(f"[FALLBACK]   full_name   : {full_name!r}")
            print(f"[FALLBACK]   phone       : {phone or '(not provided)'}")
            print(f"[FALLBACK]   parse_method: {_parse_method!r}")
            print(f"[FALLBACK]   raw_keys    : {sorted(body.keys()) if body else []}")
            log.error(f"[{contact_id}] msg_type=unknown at reply time — unexpected code path")

            # Attempt fallback SMS if this looks like a fresh widget lead
            _fb_state = get_state(contact_id)
            _fb_phone = _fb_state.get("phone", "") or phone
            if contact_id and _fb_state.get("stage") == Stage.INITIAL and _fb_phone:
                print(f"[FALLBACK] 🔁 Contact is INITIAL + has phone — attempting first-outreach as fallback")
                _fb_outreach = build_new_contact_outreach(
                    first_name=first_name,
                    full_name=full_name,
                    address=address,
                    lead_source=lead_source,
                    tags=tags,
                )
                _fb_state["stage"]              = Stage.ASK_OWNERSHIP
                _fb_state["location_confirmed"] = True
                _fb_state["entry_path"]         = _fb_state.get("entry_path") or "chat_widget"
                _fb_state["source_chat_widget"] = True
                increment_message_count(_fb_state)
                _fb_state["messages"].append({
                    "role": "user",
                    "content": f"[Unknown-event fallback — contact_id={contact_id}, parse={_parse_method}]",
                })
                _fb_state["messages"].append({"role": "assistant", "content": _fb_outreach})
                save_state(contact_id, _fb_state)
                try:
                    _send = await send_sms_via_ghl(contact_id, _fb_outreach, to_number=_fb_phone,
                                                   kind=SendKind.OUTREACH)
                    ev("FIRST_OUTREACH_SENT" if _send.get("status") in _STATUS_ADVANCES_STATE
                       else "FIRST_OUTREACH_NOT_SENT", contact_id, path="fallback",
                       status=_send.get("status"),
                       message_id=_send.get("message_id") or "(none)")
                    print(f"[FALLBACK] ✅ Fallback first-outreach sent to {full_name!r} ({contact_id})")
                    log.info(f"[{contact_id}] Fallback first-outreach sent | unknown event | phone={_fb_phone!r}")
                    return JSONResponse({
                        "status"       : "success",
                        "replied"      : True,
                        "reply"        : _fb_outreach,
                        "payload_type" : "unknown_event_fallback",
                        "path"         : "NEW_LEAD_PATH",
                    })
                except Exception as _fb_err:
                    print(f"[FALLBACK] ❌ Fallback SMS also failed: {_fb_err}")
                    log.error(f"[{contact_id}] Fallback SMS failed: {_fb_err}")
            else:
                print(f"[FALLBACK]   Skipping fallback: stage={_fb_state.get('stage')}, phone={_fb_phone!r}")
                print(f"[FALLBACK]   (Fallback only fires for INITIAL-stage contacts with a phone number)")

        print(f"[DEBUG] msg_type resolved as: {msg_type}")
        print(f"[ROUTING] ✅ Reply sent to {full_name!r} ({contact_id}): {reply!r}")
        log.info(f"[{contact_id}] Reply sent successfully.")
        return JSONResponse({"status": "success", "replied": True, "reply": reply, "payload_type": payload_type})

    except Exception as fatal_err:
        _tb = traceback.format_exc()
        log.error(f"FATAL webhook error: {fatal_err}\n{_tb}")
        print(f"\n{'!'*60}", flush=True)
        print(f"[ERROR] 💥 FATAL UNHANDLED EXCEPTION IN inbound_webhook", flush=True)
        print(f"[ERROR]    {fatal_err}", flush=True)
        print(f"[ERROR] Full traceback:\n{_tb}", flush=True)
        print(f"{'!'*60}\n", flush=True)
        # Include the actual error in the response — visible in GHL execution logs
        return JSONResponse({
            "status"       : "success",
            "error_logged" : True,
            "error_type"   : type(fatal_err).__name__,
            "error_message": str(fatal_err),
            "traceback"    : _tb,
        })
    finally:
        # Always release the per-contact lock, regardless of return path or exception.
        # Also marks the contact's last-processed timestamp so the 2-second debounce
        # window starts counting from NOW (after SMS is sent, not before).
        # _lock_acq is True ONLY after acquire() succeeded, which only happens after
        # contact_id is fully resolved — so contact_id is always in scope here.
        if _lock_acq and _proc_lock is not None:
            _proc_lock.release()
            _mark_contact_processed(contact_id)  # noqa: F821 — always in scope when _lock_acq=True
            print(f"[LOCK] 🔓 lock released + debounce started | contact={contact_id}", flush=True)


# ─────────────────────────────────────────────
#  FOLLOW-UP TRIGGER ENDPOINT
# ─────────────────────────────────────────────

@app.post("/webhook/booking-followup")
async def booking_followup(request: Request):
    """
    Appointment-booked confirmation trigger called by GHL when a contact books.

    Dedup guards (checked in order — first match wins):
      1. booking_followup_sent=True  → skip (this endpoint already ran for this booking)
      2. bill_reminder_sent=True     → skip (inbound webhook already sent the confirmation
                                       in a race; avoid duplicate)

    All other guards are bypassed:
      • final_confirmation_sent — irrelevant here (bill-photo flow, not booking flow)
      • daily_limit             — booking confirmation is always high-priority
      • stage check             — bypassed

    Message copy comes exclusively from BOOKING_TEMPLATES via build_booking_message().
    Variant selection mirrors the inbound booked path:
      • entry_path == "chat_widget"         → variant="sms_flow"   (warm continuation)
      • entry_path == "direct_booking" / unset → variant="direct_booking" (brief intro)
    """
    try:
        try:
            body = await request.json()
        except Exception as _bf_parse_err:
            print(f"[BOOKING-FOLLOWUP] SKIP reason=invalid_json | error={_bf_parse_err} | sms_sent=False | send_sms_via_ghl_called=False", flush=True)
            return JSONResponse({"status": "success", "skipped": True, "reason": "invalid_json"})

        parsed     = normalize_payload(body)
        contact_id = parsed["contact_id"]
        if not contact_id:
            print(f"[BOOKING-FOLLOWUP] SKIP reason=no_contact_id | sms_sent=False | send_sms_via_ghl_called=False", flush=True)
            return JSONResponse({"status": "success", "skipped": True, "reason": "no_contact_id"})

        state      = get_state(contact_id)
        phone      = state.get("phone", "") or parsed.get("phone", "")
        first_name = parsed.get("first_name", "") or state.get("first_name", "")
        full_name  = parsed.get("full_name", "") or state.get("full_name", "")

        # Stamp appointment_booked + update stage before anything else.
        # Any concurrent /webhook/inbound processing this contact will see
        # appointment_booked=True in state and not send qualification messages.
        if not state.get("appointment_booked"):
            state["appointment_booked"] = True
        if state["stage"] not in (Stage.BOOKED, Stage.DNC):
            state["stage"] = Stage.BOOKED
        if not state.get("phone") and phone:
            state["phone"] = phone
        if not state.get("contact_name") and full_name != "Unknown":
            state["contact_name"] = full_name
        save_state(contact_id, state)

        # ── DEDUP GUARD 1: this endpoint already ran for this booking ─
        if state.get("booking_followup_sent"):
            print(
                f"[BOOKING-FOLLOWUP] ⏭ SKIP dedup=booking_followup_sent | "
                f"contact={contact_id} | sms_sent=False"
            )
            log.info(f"[{contact_id}] booking-followup: skipped — booking_followup_sent already True")
            return JSONResponse({
                "status"     : "success",
                "skipped"    : True,
                "reason"     : "booking_followup_already_sent",
                "contact_id" : contact_id,
            })

        # ── Pick message variant + render via centralized template ─
        _entry_path     = state.get("entry_path", "unknown")
        _is_chat_widget = (
            _entry_path == "chat_widget" or
            (_entry_path == "unknown" and bool(state.get("messages")))
        )
        _bf_variant                    = "sms_flow" if _is_chat_widget else "direct_booking"
        appt_confirmation, _bf_tpl_key = build_booking_confirmation(
            _bf_variant, first_name=first_name, full_name=full_name
        )
        _variant = _bf_variant  # retained for log.info line below

        # ── Diagnostic banner ──────────────────────────────────────
        print(f"\n[BOOKING-FOLLOWUP] {'─'*50}")
        print(f"[BOOKING-FOLLOWUP]  contact_id             : {contact_id}")
        print(f"[BOOKING-FOLLOWUP]  first_name             : {first_name or '(not set)'!r}")
        print(f"[BOOKING-FOLLOWUP]  full_name              : {full_name!r}")
        print(f"[BOOKING-FOLLOWUP]  phone                  : {phone or '(not set)'!r}")
        print(f"[BOOKING-FOLLOWUP]  stage (state)          : {state['stage']}")
        print(f"[BOOKING-FOLLOWUP]  entry_path             : {_entry_path!r}")
        print(f"[BOOKING-FOLLOWUP]  variant                : {_bf_variant!r}")
        print(f"[BOOKING-FOLLOWUP]  template_key           : {_bf_tpl_key!r}")
        print(f"[BOOKING-FOLLOWUP]  brand.name             : {BRAND['name']!r}")
        print(f"[BOOKING-FOLLOWUP]  brand.utility          : {BRAND['utility']!r}")
        print(f"[BOOKING-FOLLOWUP]  bill_reminder_sent     : {state.get('bill_reminder_sent', False)}")
        print(f"[BOOKING-FOLLOWUP]  booking_followup_sent  : {state.get('booking_followup_sent', False)}")
        print(f"[BOOKING-FOLLOWUP]  final_confirmation_sent: {state.get('final_confirmation_sent', False)}  ← BYPASSED")
        print(f"[BOOKING-FOLLOWUP]  message                : {appt_confirmation!r}")
        print(f"[BOOKING-FOLLOWUP] {'─'*50}")

        # ── DEDUP GUARD 2: inbound race — skip if inbound already sent ─
        if state.get("bill_reminder_sent"):
            print(
                f"[BOOKING-FOLLOWUP]  ⏭ SKIP dedup=bill_reminder_sent | "
                f"inbound already sent confirmation for this booking | contact={contact_id}"
            )
            log.info(f"[{contact_id}] booking-followup: skipped — bill_reminder_sent by inbound already")
            return JSONResponse({
                "status"     : "success",
                "skipped"    : True,
                "reason"     : "bill_reminder_already_sent_by_inbound",
                "contact_id" : contact_id,
            })

        print(f"[BOOKING-FOLLOWUP]  ⚡ Sending appointment confirmation now\n")

        print(f"[BOOKING-FOLLOWUP SMS ATTEMPT]  contact_id : {contact_id!r}")
        print(f"[BOOKING-FOLLOWUP SMS ATTEMPT]  to number  : {phone!r}")
        print(f"[BOOKING-FOLLOWUP SMS ATTEMPT]  message    : {appt_confirmation!r}")

        _result: dict = {}
        try:
            _result = await send_sms_via_ghl(contact_id, appt_confirmation, to_number=phone,
                                             kind=SendKind.BOOKED_REPLY)
        except Exception as sms_err:
            _tb = traceback.format_exc()
            print(f"\n[BOOKING-FOLLOWUP SMS RESULT]  ❌ EXCEPTION — SMS NOT sent")
            print(f"[BOOKING-FOLLOWUP SMS RESULT]  error      : {sms_err}")
            print(f"[BOOKING-FOLLOWUP SMS RESULT]  traceback  :\n{_tb}")
            log.error(f"[{contact_id}] booking-followup SMS failed: {sms_err}")
            return JSONResponse({"status": "success", "note": "SMS failed — see logs"})

        _sent    = _result.get("sent", False)
        _deduped = _result.get("deduped", False)
        print(f"[BOOKING-FOLLOWUP SMS RESULT]  sent       : {_sent}")
        print(f"[BOOKING-FOLLOWUP SMS RESULT]  deduped    : {_deduped}")
        if _deduped:
            print(f"[BOOKING-FOLLOWUP SMS RESULT]  ⚠ *** OUTBOUND DEDUP SUPPRESSED THIS SMS ***")
        elif _sent:
            print(f"[BOOKING-FOLLOWUP SMS RESULT]  ✅ Appointment confirmation sent to {full_name!r} at {phone!r}")

        # Stamp all booking flags so no other path double-sends.
        # booking_followup_sent → primary dedup for this endpoint on retry/re-fire
        # bill_reminder_sent    → inbound booked path won't send again
        state["booking_followup_sent"] = True
        state["bill_reminder_sent"]    = True
        state["bill_requested"]        = True
        state["appointment_booked"]    = True
        increment_message_count(state)
        save_state(contact_id, state)
        log.info(
            f"[{contact_id}] Booking-followup confirmation sent | "
            f"variant={_bf_variant!r} | template={_bf_tpl_key!r} | "
            f"brand={BRAND['name']!r} | sent={_sent} | deduped={_deduped}"
        )
        return JSONResponse({
            "status"       : "success",
            "sent"         : _sent,
            "deduped"      : _deduped,
            "variant"      : _bf_variant,
            "template_key" : _bf_tpl_key,
            "brand"        : BRAND["name"],
        })

    except Exception as err:
        log.error(f"booking-followup fatal error: {err}")
        print(f"[ERROR] booking-followup FATAL:\n{traceback.format_exc()}")
        return JSONResponse({"status": "success", "error_logged": True})


# ═════════════════════════════════════════════════════════════════════════
#  [DELIVERY-2] OUTBOUND DELIVERY-STATUS RECEIVER
#
#  WHY
#    GHL's POST /conversations/messages returns 2xx when it ACCEPTS a message
#    for sending. Carrier filtering (error 30007, "Message filtered") happens
#    afterwards and is never visible in that response. Without this route the
#    agent believes every accepted message arrived — so it advances the
#    conversation and asks the next question even though the homeowner never
#    saw the previous one.
#
#  MECHANISM (documented, not guessed)
#    GHL exposes a native workflow trigger, "Messaging Error Code - SMS",
#    which fires on an undelivered message and supports 30007 among its
#    error codes. A Webhook action on that workflow POSTs here.
#
#  WHAT THIS DOES NOT DO
#    It never resends. A filtered message resent unchanged fails again and
#    damages sender reputation. It records the truth, flags the contact for
#    a human, and stops.
#
#  It also never rewinds the conversation stage. Rewinding would let the next
#  inbound re-trigger the same question — an automatic retry by another name.
# ═════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════════
#  [NUDGE] BOOKING-LINK FOLLOW-UP
#
#  THE GAP THIS FILLS
#    The Meta nurture sequence is cancelled the moment a lead replies, because
#    Michael writes ai-engaged on any real inbound and the GHL Nurture Cancel
#    workflow removes them. So a lead who engages, qualifies, receives the
#    booking link and then goes quiet had NO follow-up at all — the single
#    most valuable non-booked state in the funnel.
#
#  OWNERSHIP
#    GHL owns the 24-hour wait (it has durable timers; this process does not).
#    Python owns the decision to send, because only it can consult the
#    authoritative booked guard.
#
#  SUPPRESSION — checked in order, first match wins
#    1. no contact id        -> ignore, log, change nothing
#    2. tag lookup failed    -> DO NOT SEND (fail safe)
#    3. solar-dnc            -> DO NOT SEND (booked_verdict does not cover DNC)
#    4. booking-nudge-sent   -> DO NOT SEND (already nudged, durable in GHL)
#    5. no BOOKING_LINK_SENT -> DO NOT SEND (never actually got a link)
#    6. booked / indeterminate -> DO NOT SEND (the booked guard decides, via a
#                               fresh three-source GHL check, because the send
#                               is classified SendKind.NURTURE)
#
#  It never touches state["stage"]. If the lead replies afterwards,
#  restore_stage_from_ghl() reads QUALIFIED / BOOKING_LINK_SENT and resumes at
#  SEND_BOOKING, so qualification is never restarted.
#
#  Fires at most once per contact, ever. There is no second nudge and no retry.
# ═════════════════════════════════════════════════════════════════════════

@app.post("/webhook/booking-nudge")
async def booking_nudge_webhook(request: Request):
    """Called by the GHL booking-nudge workflow ~24h after the link is sent."""
    try:
        try:
            body = await request.json()
        except Exception:
            _raw = (await request.body()).decode("utf-8", errors="replace")
            body = dict(parse_qs(_raw)) if _raw else {}
        if not isinstance(body, dict):
            body = {}

        def _pick(*keys) -> str:
            for k in keys:
                v = body.get(k)
                if isinstance(v, list):
                    v = v[0] if v else None
                if v not in (None, "", []):
                    return str(v).strip()
            return ""

        contact_id = _pick("contactId", "contact_id", "contact", "id")
        phone      = _pick("phone", "phone_number", "phoneNumber")
        # Optional inline cancel, so a workflow can suppress without a tag.
        _cancel    = str(_pick("cancel", "cancelled", "manual")).lower() in ("1", "true", "yes")

        if not contact_id:
            ev("NUDGE_MALFORMED", "", keys=sorted(body.keys())[:12],
               note="no contact id — ignored, nothing changed")
            return JSONResponse({"status": "success", "sent": False,
                                 "reason": "no_contact_id"})

        def _skip(reason: str, **extra):
            ev("NUDGE_SUPPRESSED", contact_id, reason=reason, **extra)
            return JSONResponse({"status": "success", "sent": False, "reason": reason,
                                 "contact_id": contact_id})

        # ── Fresh authoritative tags. Never the webhook payload. ──────
        _tags_ok, _tags = await _fetch_contact_tags_ex(contact_id)
        if not _tags_ok:
            return _skip("tag_lookup_failed")          # fail safe
        if has_tag(_tags, TAG_DNC):
            return _skip("dnc")
        if has_tag(_tags, TAG_NUDGE_SENT):
            return _skip("already_nudged")
        # A human has this conversation. Two follow-ups in a day reads as
        # pestering, and the manual one is better than anything automated.
        if has_tag(_tags, TAG_NUDGE_CANCELLED):
            return _skip("manually_handled")
        if not any(has_tag(_tags, t) for t in _TAGS_QUALIFIED):
            return _skip("no_booking_link_sent")
        if has_tag(_tags, TAG_DISQUALIFIED):
            return _skip("disqualified")

        if _cancel:
            return _skip("manually_handled_payload")

        state = get_state(contact_id)
        if state.get("stage") == Stage.DNC:
            return _skip("dnc_state")

        _to = phone or state.get("phone", "")
        ev("NUDGE_ATTEMPT", contact_id, phone=_to, stage=str(state.get("stage")))

        # SendKind.NURTURE is guarded, so send_sms_via_ghl() runs the fresh
        # three-source booked check (tags + opportunity stage + appointments)
        # and suppresses on booked OR on any indeterminate result.
        _send = await send_sms_via_ghl(
            contact_id, build_booking_nudge(), to_number=_to,
            kind=SendKind.NURTURE,
        )

        if _send.get("status") not in _STATUS_ADVANCES_STATE:
            ev("NUDGE_NOT_SENT", contact_id, status=_send.get("status"),
               reason=_send.get("reason") or _send.get("why") or "(none)")
            return JSONResponse({"status": "success", "sent": False,
                                 "reason": _send.get("reason") or "booked_guard",
                                 "why": _send.get("why"),
                                 "contact_id": contact_id})

        _record_send_result(contact_id, state, _send)
        # Durable one-shot marker, written only after acceptance.
        try:
            await add_ghl_tags(contact_id, [TAG_NUDGE_SENT])
        except Exception as _t_err:
            log.warning(f"[{contact_id}] nudge tag write failed (non-fatal): {_t_err}")

        ev("NUDGE_SENT", contact_id, message_id=_send.get("message_id") or "(none)",
           status=_send.get("status"))
        return JSONResponse({"status": "success", "sent": True,
                             "contact_id": contact_id,
                             "sms_status": _send.get("status"),
                             "message_id": _send.get("message_id")})

    except Exception as err:
        log.error(f"booking-nudge fatal: {err}")
        print(f"[ERROR] booking-nudge FATAL:\n{traceback.format_exc()}")
        return JSONResponse({"status": "success", "error_logged": True})


@app.post("/webhook/message-status")
async def message_status_webhook(request: Request):
    """
    Receives outbound delivery failures from GHL.

    Tolerant of payload shape: GHL's docs do not specify which fields the
    trigger exposes, so every field is optional except a contact id. Always
    returns 200 — a non-200 makes GHL retry, which would double-count.
    """
    try:
        try:
            body = await request.json()
        except Exception:
            _raw = (await request.body()).decode("utf-8", errors="replace")
            body = dict(parse_qs(_raw)) if _raw else {}
        if not isinstance(body, dict):
            body = {}

        def _pick(*keys) -> str:
            for k in keys:
                v = body.get(k)
                if isinstance(v, list):
                    v = v[0] if v else None
                if v not in (None, "", []):
                    return str(v).strip()
            return ""

        contact_id = _pick("contactId", "contact_id", "contact", "id")
        error_code = _pick("errorCode", "error_code", "code", "messagingErrorCode")
        message_id = _pick("messageId", "message_id", "msgId")
        status_txt = _pick("status", "messageStatus", "message_status") or "undelivered"

        if not contact_id:
            ev("DELIVERY_STATUS_MALFORMED", "", keys=sorted(body.keys())[:12],
               note="no contact id — ignored, no state changed")
            return JSONResponse({"status": "success", "ignored": True,
                                 "reason": "no_contact_id"})

        kind = classify_carrier_error(error_code)
        state = get_state(contact_id)
        _prev_status = state.get("last_send_status")

        # Record the truth. The stage is deliberately left where it is.
        state["last_send_status"]  = SendStatus.FAILED.value
        state["undelivered_count"] = int(state.get("undelivered_count", 0)) + 1
        if message_id:
            state["last_message_id"] = message_id
        save_state(contact_id, state)

        ev(
            "DELIVERY_FAILED", contact_id,
            error_code=error_code or "(none)",
            classification=kind,
            message_id=message_id or "(none)",
            carrier_status=status_txt,
            previous_status=_prev_status,
            stage=str(state.get("stage")),
            undelivered_count=state["undelivered_count"],
            retry="never",
        )

        # Flag for a human. Additive and non-fatal.
        try:
            await add_ghl_tags(contact_id, [TAG_UNDELIVERABLE])
        except Exception as _tag_err:
            log.warning(f"[{contact_id}] undeliverable tag write failed: {_tag_err}")

        if kind == "permanent":
            ev("MANUAL_ATTENTION_REQUIRED", contact_id,
               reason=f"permanent_carrier_failure_{error_code or 'unknown'}",
               note="last question did NOT reach the homeowner; no automatic resend")

        return JSONResponse({
            "status"           : "success",
            "recorded"         : True,
            "contact_id"       : contact_id,
            "error_code"       : error_code,
            "classification"   : kind,
            "undelivered_count": state["undelivered_count"],
            "retried"          : False,
        })

    except Exception as err:
        log.error(f"message-status webhook fatal: {err}")
        print(f"[ERROR] message-status FATAL:\n{traceback.format_exc()}")
        return JSONResponse({"status": "success", "error_logged": True})


# ─────────────────────────────────────────────
#  DEBUG ENDPOINTS
# ─────────────────────────────────────────────

def _debug_auth_error(request: Request) -> Optional[JSONResponse]:
    """
    [DEBUG-RESET] Shared guard for state-mutating debug endpoints.

    Returns a JSONResponse to return immediately, or None when the caller
    is authorised.  Behaviour:
      • DEBUG_RESET_SECRET unset  -> 503, endpoint hard-disabled
      • header missing / wrong    -> 401
    Constant-time compare; the secret is never logged or echoed.
    """
    if not DEBUG_RESET_SECRET:
        return JSONResponse(
            {"error": "debug endpoint disabled: DEBUG_RESET_SECRET is not configured"},
            status_code=503,
        )
    _supplied = request.headers.get("X-Debug-Secret", "")
    if not (_supplied and hmac.compare_digest(_supplied, DEBUG_RESET_SECRET)):
        log.warning("[DEBUG-AUTH] 401 - missing or invalid X-Debug-Secret")
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return None

@app.get("/debug/state/{contact_id}")
async def debug_get_state(contact_id: str):
    """
    Inspect in-memory state for a contact.
    GET /debug/state/{contact_id}
    """
    if contact_id not in _state_store:
        return JSONResponse({"found": False, "contact_id": contact_id, "note": "No state — contact is INITIAL"})
    s = _state_store[contact_id].copy()
    s["stage"] = str(s.get("stage", "UNKNOWN"))
    return JSONResponse({"found": True, "contact_id": contact_id, "state": s})


@app.post("/debug/reset/{contact_id}")
async def debug_reset_state(contact_id: str, request: Request):
    """
    Delete all state for a contact (reset to INITIAL).
    POST /debug/reset/{contact_id}
    X-Debug-Secret: <DEBUG_RESET_SECRET>

    NOTE: this clears _state_store ONLY.  It does not unpin the phone map,
    so a contact reset here can still be resurrected by
    _resolve_contact_id_by_phone().  Use POST /debug/reset-contact for a
    reset that actually produces a fresh lead.
    """
    _auth = _debug_auth_error(request)
    if _auth is not None:
        return _auth

    existed = contact_id in _state_store
    if existed:
        del _state_store[contact_id]
    log.info(f"[DEBUG] State reset for {contact_id} (existed={existed})")
    return JSONResponse({"reset": True, "contact_id": contact_id, "existed": existed})


# ─────────────────────────────────────────────
#  [DEBUG-RESET]  SCOPED SINGLE-CONTACT STATE RESET
#
#  Purpose
#  ───────
#  Lets a tester re-run a lead (e.g. a Facebook lead) against their OWN phone
#  number and be treated as a brand-new contact, without restarting the service
#  and without touching any other lead.
#
#  Why the pre-existing /debug/reset/{contact_id} is not enough
#  ────────────────────────────────────────────────────────────
#  That endpoint clears _state_store only.  _phone_to_contact is
#  first-registration-wins, so the tester's phone stays pinned to the OLD
#  contact_id; the next lead is then redirected back onto the old contact by
#  _resolve_contact_id_by_phone() and the stale conversation reappears.
#  A useful reset MUST unpin the phone as well.
#
#  Scope — every operation is keyed to ONE contact/phone
#  ────────────────────────────────────────────────────
#    cleared : _state_store[cid]                     (stage, messages, flags)
#              _phone_to_contact[norm]               (only this phone / this cid)
#              _processed_fingerprints               (only this cid's keys)
#              _outbound_fingerprints                (only this cid's keys)
#              _processed_event_ids                  (only this cid's keys)
#              _contact_last_processed_ts[cid]       (debounce)
#              _contact_processing_locks[cid]        (only if NOT held)
#    untouched : the GHL contact, its tags, its opportunities, its appointments,
#                every other contact, and all production routing.
#
#  Authoritative-truth safety
#  ──────────────────────────
#  A booked appointment lives in GHL, not here.  If this contact looks booked
#  the endpoint REFUSES (409) unless force=true.  Even with force=true nothing
#  in GHL is modified: because the booked tags survive, the very next inbound
#  webhook re-restores appointment_booked via the existing tag-restore path.
#  In-memory clearing therefore cannot destroy production truth — it can only
#  make this process briefly forget it.
# ─────────────────────────────────────────────

def _mask_phone(phone: str) -> str:
    """Mask a phone for logging: keep the last 4 digits only."""
    digits = re.sub(r"\D", "", phone or "")
    return f"***{digits[-4:]}" if len(digits) >= 4 else "***"


async def _debug_fetch_ghl_booked(contact_id: str) -> tuple[Optional[bool], list[str]]:
    """
    [DEBUG-RESET] Read-only GHL lookup for the booked-state safety check.

    Returns (is_booked, tags).  is_booked is None when GHL could not be
    reached — the caller treats that as "unknown" and fails safe.
    Performs a GET only; never writes to GHL.
    """
    if not (contact_id and GHL_API_KEY):
        return None, []
    url     = f"{GHL_API_BASE}/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type" : "application/json",
        "Version"      : "2021-07-28",
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, headers=headers)
        if not r.is_success:
            return None, []
        data    = r.json()
        contact = data.get("contact") or data
        tags    = [str(t) for t in (contact.get("tags") or [])]
        return is_already_booked(tags), tags
    except Exception:
        return None, []


@app.post("/debug/reset-contact")
async def debug_reset_contact(request: Request):
    """
    Clear this process's in-memory state for ONE contact/phone so it behaves
    like a brand-new lead.  Nothing in GHL is modified.

        POST /debug/reset-contact
        X-Debug-Secret: <DEBUG_RESET_SECRET>
        {"contact_id": "...", "phone": "...", "force": false}

    At least one of contact_id / phone is required; supplying both is best
    (phone alone resolves the contact_id through the phone map).
    """
    # ── AUTH ─────────────────────────────────────────────────────────
    # Hard-disabled when the secret is not configured, so an un-configured
    # deploy can never expose this.  Constant-time compare.  Never logged.
    _auth = _debug_auth_error(request)
    if _auth is not None:
        return _auth

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    contact_id = str(body.get("contact_id", "") or "").strip()
    phone      = str(body.get("phone", "") or "").strip()
    force      = bool(body.get("force", False))
    norm       = _normalize_phone(phone)

    if not (contact_id or norm):
        return JSONResponse(
            {"error": "at least one of contact_id or phone is required"},
            status_code=400,
        )

    # Resolve contact_id from the phone map when only a phone was supplied.
    if not contact_id and norm:
        contact_id = _phone_to_contact.get(norm, "")

    # ── SAFETY CHECK: do not silently erase a real booking ───────────
    _mem_state  = _state_store.get(contact_id) if contact_id else None
    _mem_booked = bool(
        _mem_state and (
            _mem_state.get("appointment_booked")
            or _mem_state.get("stage") in (Stage.BOOKED,)
        )
    )
    _ghl_booked, _ghl_tags = await _debug_fetch_ghl_booked(contact_id)

    # Fail safe: an unreachable GHL ("unknown", None) counts as possibly booked.
    _looks_booked = _mem_booked or (_ghl_booked is not False)

    if _looks_booked and not force:
        log.info(
            f"[DEBUG-RESET] 409 refused | contact_id={contact_id!r} | "
            f"phone={_mask_phone(phone)} | mem_booked={_mem_booked} | "
            f"ghl_booked={_ghl_booked} (None=lookup unavailable)"
        )
        return JSONResponse(
            {
                "reset"        : False,
                "warning"      : "contact appears BOOKED, or booked-state could not be verified",
                "contact_id"   : contact_id,
                "phone"        : _mask_phone(phone),
                "memory_booked": _mem_booked,
                "ghl_booked"   : _ghl_booked,
                "note": (
                    "No state was changed. Re-send with force=true to clear "
                    "ephemeral in-memory state only. GHL tags, opportunities and "
                    "appointments are never modified by this endpoint, so the "
                    "authoritative booked state survives and is re-restored from "
                    "tags on the next inbound webhook."
                ),
            },
            status_code=409,
        )

    # ── SCOPED CLEAR ─────────────────────────────────────────────────
    cleared: dict[str, int] = {}

    if contact_id and _state_store.pop(contact_id, None) is not None:
        cleared["_state_store"] = 1

    # Unpin the phone: drop the entry for this phone AND any entry pointing at
    # this contact_id.  Both are exact-key removals — no other lead is touched.
    _phone_keys = set()
    if norm and norm in _phone_to_contact:
        _phone_keys.add(norm)
    if contact_id:
        _phone_keys |= {k for k, v in _phone_to_contact.items() if v == contact_id}
    for _k in _phone_keys:
        _phone_to_contact.pop(_k, None)
    if _phone_keys:
        cleared["_phone_to_contact"] = len(_phone_keys)

    # Dedup caches: remove only the keys this contact planted, via the index.
    if contact_id:
        _owned = [k for k, v in _dedup_owner.items() if v == contact_id]
        _n_in = _n_out = _n_evt = 0
        for _k in _owned:
            if _k in _processed_fingerprints:
                _processed_fingerprints.discard(_k)
                _n_in += 1
            if _k in _outbound_fingerprints:
                _outbound_fingerprints.discard(_k)
                _n_out += 1
            if _k in _processed_event_ids:
                _processed_event_ids.discard(_k)
                _n_evt += 1
            _dedup_owner.pop(_k, None)
        if _n_in:
            cleared["_processed_fingerprints"] = _n_in
        if _n_out:
            cleared["_outbound_fingerprints"] = _n_out
        if _n_evt:
            cleared["_processed_event_ids"] = _n_evt

    if contact_id and _contact_last_processed_ts.pop(contact_id, None) is not None:
        cleared["_contact_last_processed_ts"] = 1

    # Only drop a lock that is NOT currently held — removing a held lock would
    # let a concurrent in-flight webhook for this contact lose mutual exclusion.
    # A held lock is released by its own finally block anyway.
    _lock = _contact_processing_locks.get(contact_id) if contact_id else None
    if _lock is not None and not _lock.locked():
        _contact_processing_locks.pop(contact_id, None)
        cleared["_contact_processing_locks"] = 1
    elif _lock is not None:
        cleared["_contact_processing_locks_skipped_locked"] = 1

    log.info(
        f"[DEBUG-RESET] ✅ reset ok | contact_id={contact_id!r} | "
        f"phone={_mask_phone(phone)} | force={force} | cleared={cleared}"
    )
    print(
        f"[DEBUG-RESET] ✅ contact={contact_id!r} phone={_mask_phone(phone)} "
        f"force={force} cleared={cleared}"
    )

    return JSONResponse({
        "reset"      : True,
        "contact_id" : contact_id,
        "phone"      : _mask_phone(phone),
        "forced"     : force,
        "cleared"    : cleared,
        "ghl_booked" : _ghl_booked,
        "note": (
            "In-memory state only. GHL contact, tags, opportunities and "
            "appointments were not modified."
        ),
    })


@app.post("/debug/send-test-sms")
async def debug_send_test_sms(request: Request):
    """
    Fire a test SMS directly through GHL (bypasses agent).
    POST body: {"contact_id": "...", "to_number": "+1...", "message": "..."}
    """
    try:
        body       = await request.json()
        contact_id = body.get("contact_id", "")
        to_number  = body.get("to_number",  "")
        message    = body.get("message",    "Test from Michael agent — SMS config OK ✅")

        if not contact_id:
            return JSONResponse({"error": "contact_id is required"}, status_code=400)

        print(f"\n[DEBUG-SMS] Manual test SMS | contact={contact_id!r} | to={to_number!r}")
        await send_sms_via_ghl(contact_id, message, to_number=to_number,
                               kind=SendKind.SYSTEM)
        return JSONResponse({"status": "sent", "contact_id": contact_id, "to_number": to_number, "message": message})

    except Exception as err:
        print(f"[ERROR] Debug SMS failed:\n{traceback.format_exc()}")
        return JSONResponse({"status": "error", "detail": str(err)})


@app.post("/debug/set-state/{contact_id}")
async def debug_set_stage(contact_id: str, request: Request):
    """
    Manually set state fields for a contact — useful for testing a specific stage.
    POST body: {"stage": "ASK_BILL", "homeowner": "yes", "monthly_bill": "$200/month"}
    """
    try:
        updates = await request.json()
        state   = get_state(contact_id)
        if "stage" in updates:
            try:
                state["stage"] = Stage(updates["stage"])
            except ValueError:
                return JSONResponse({"error": f"Unknown stage: {updates['stage']}"}, status_code=400)
        for field in ("homeowner", "location_confirmed", "monthly_bill", "phone", "address",
                      "bill_reminder_sent", "bill_ack_sent", "contact_name",
                      "entry_path", "lead_source"):
            if field in updates:
                state[field] = updates[field]
        save_state(contact_id, state)
        s = state.copy()
        s["stage"] = str(s["stage"])
        return JSONResponse({"updated": True, "contact_id": contact_id, "state": s})
    except Exception as err:
        return JSONResponse({"error": str(err)}, status_code=400)


# ─────────────────────────────────────────────
#  HEALTH CHECK
# ─────────────────────────────────────────────

@app.get("/")
async def root():
    """Render health check — must return 200 instantly."""
    return {"status": "ok"}

@app.post("/webhook/after-hours-resume")
async def after_hours_resume(request: Request):
    """
    Morning resume for contacts held overnight.

    GHL calls this during the permitted window for any contact carrying
    the `after-hours-hold` tag. Python is the authoritative decision
    layer: it re-evaluates EVERYTHING from live sources and generates a
    FRESH reply. No response is ever frozen at 2 AM and thawed at 9 AM.

    Revalidation order (all must pass, else the hold is cleared and
    nothing is sent):
      1. inside the send window      -> never send outside 9AM-9PM Central
      2. hold tag still present      -> durable idempotency (survives restart)
      3. genuine opt-out / Stage.DNC -> clear, never send
      4. live GHL DND                -> clear, never send
      5. booked / appointment lockout-> clear, never send   (booked_verdict)
      6. daily message limit         -> leave hold, retry next window
      7. hold age > QH_MAX_HOLD_AGE  -> expire, clear, never send
      8. current conversation state  -> if we already replied, unnecessary
      9. can_contact_sms()           -> runs again inside send_sms_via_ghl

    Always returns HTTP 200 so GHL never retry-storms.
    """
    try:
        raw = await request.body()
    except Exception:
        raw = b""
    try:
        body = json.loads(raw) if raw else {}
        if not isinstance(body, dict):
            body = {}
    except Exception:
        try:
            body = dict(parse_qs(raw.decode("utf-8", errors="replace"))) if raw else {}
        except Exception:
            body = {}

    def _pick(*keys) -> str:
        for k in keys:
            v = body.get(k)
            if isinstance(v, list):
                v = v[0] if v else None
            if v not in (None, "", []):
                return str(v).strip()
        return ""

    _nested = body.get("contact")
    _nested = _nested if isinstance(_nested, dict) else {}
    contact_id = (_pick("contact_id", "contactId", "id")
                  or str(_nested.get("id") or "").strip())

    _qh_stats["resumes_processed"] += 1
    _qh_stats["last_resume_at"] = datetime.now(tz=CENTRAL_TZ).isoformat(timespec="seconds")

    def _skip(reason: str, **extra):
        _qh_stats["resumes_skipped"] += 1
        ev("QH_RESUME_SKIPPED", contact_id, reason=reason, **extra)
        payload = {"status": "skipped", "reason": reason, "sms_sent": False}
        payload.update(extra)
        return JSONResponse(payload)

    if not contact_id:
        return _skip("no_contact_id")

    # ── 1. Send window (free check, before any GHL work) ──────────
    if not within_send_window():
        return _skip("outside_send_window", hold_retained=True)

    # ── 2. Hold tag still present? (durable idempotency) ──────────
    compliance = await fetch_ghl_contact_compliance(contact_id)
    if not compliance["ok"]:
        return _skip("compliance_lookup_failed", hold_retained=True)

    if not has_tag(compliance.get("tags"), TAG_AFTER_HOURS_HOLD):
        return _skip("hold_already_cleared", duplicate_fire=True)

    # ── 3. Genuine opt-out / Stage.DNC ────────────────────────────
    state = get_state(contact_id)
    if state.get("sms_opt_out") is True or state.get("stage") == Stage.DNC:
        await clear_after_hours_hold(contact_id)
        return _skip("consumer_opt_out", hold_cleared=True)

    # ── 4. Live GHL DND ───────────────────────────────────────────
    _dnd_sms = compliance.get("dnd_sms") or {}
    _dnd_status = str(_dnd_sms.get("status") or "").strip().lower()
    if compliance.get("dnd") is True or (_dnd_status and _dnd_status != "inactive"):
        _cat, _code, _perm = classify_sms_failure(_dnd_sms)
        await record_sms_outcome(contact_id, _cat, _code, _perm,
                                 email=compliance.get("email", ""))
        await clear_after_hours_hold(contact_id)
        return _skip("dnd_active", category=_cat, code=_code, hold_cleared=True)

    # ── 5. Booked lockout - reuse main's authoritative verdict ────
    _verdict, _why = await booked_verdict(contact_id)
    if _verdict is not False:          # True (booked) OR None (unknown)
        await clear_after_hours_hold(contact_id)
        return _skip("booked_lockout", hold_cleared=True,
                     verdict=("booked" if _verdict else "indeterminate"),
                     why=_why)

    # ── 6. Daily message limit (hold retained, retry next window) ─
    if not within_daily_limit(state):
        return _skip("daily_limit", hold_retained=True)

    # ── 7. Current conversation + staleness ───────────────────────
    convo = await fetch_latest_conversation_signal(contact_id)
    if not convo["ok"]:
        return _skip("conversation_lookup_failed", hold_retained=True)

    _age = hold_age_hours(convo.get("latest_inbound_at") or
                          compliance.get("date_added"))
    if _age is not None and _age > QH_MAX_HOLD_AGE_HOURS:
        await add_ghl_tags(contact_id, [TAG_AFTER_HOURS_EXPIRED])
        await clear_after_hours_hold(contact_id)
        return _skip("hold_expired", age_hours=round(_age, 1), hold_cleared=True)

    # ── 8. Already answered since the overnight inbound ───────────
    if convo["last_direction"] == "outbound":
        await clear_after_hours_hold(contact_id)
        return _skip("already_replied", hold_cleared=True)

    _ghl_tags = list(compliance.get("tags") or [])

    if not convo["latest_inbound"]:
        if convo["last_direction"] == "":
            # No conversation at all -> a FIRST OUTREACH deferred overnight
            # (e.g. a Meta lead form submitted at 2 AM). Generate the opener
            # now, fresh, using the same builder the live path uses.
            ev("QH_RESUME_FIRST_OUTREACH", contact_id)
            reply = build_new_contact_outreach(
                first_name  = compliance.get("first_name", ""),
                full_name   = compliance.get("full_name", ""),
                address     = "",
                lead_source = state.get("lead_source", ""),
                tags        = _ghl_tags,
            )
            _kind = SendKind.OUTREACH
            if state.get("stage") == Stage.INITIAL:
                state["stage"] = Stage.ASK_OWNERSHIP
            increment_message_count(state)
            state["messages"].append({
                "role": "user",
                "content": (f"[Lead submitted form after hours - "
                            f"name: {compliance.get('full_name') or 'Unknown'}, "
                            f"contacted at first permitted send window]")})
            state["messages"].append({"role": "assistant", "content": reply})
            save_state(contact_id, state)
        else:
            await clear_after_hours_hold(contact_id)
            return _skip("no_inbound", hold_cleared=True)
    else:
        # Restore Claude context if _state_store was wiped by a restart.
        # No-op when live state already has history.
        _restored = rebuild_history_from_ghl(contact_id, convo.get("history") or [])
        ev("QH_RESUME_GENERATING", contact_id,
           inbound_count=convo["inbound_count"], ctx_restored=_restored)
        # Evaluated ONCE: several overnight messages collapse into the
        # single most recent inbound -> at most one morning response.
        reply = michael_agent(contact_id, convo["latest_inbound"],
                              ghl_pipeline_stage="", ghl_tags=_ghl_tags)
        _kind = classify_reply_kind(get_state(contact_id))

    if not reply:
        await clear_after_hours_hold(contact_id)
        return _skip("agent_no_reply", hold_cleared=True)

    # ── 9. Phase 1 compliance gate runs inside send_sms_via_ghl ───
    _send = await send_sms_via_ghl(contact_id, reply,
                                   to_number=compliance.get("phone", ""),
                                   kind=_kind)
    _record_send_result(contact_id, get_state(contact_id), _send)

    if _send.get("status") not in _STATUS_ADVANCES_STATE:
        _reason = _send.get("reason") or _send.get("why") or "(none)"
        ev("QH_RESUME_NOT_SENT", contact_id, status=_send.get("status"),
           reason=_reason, note="homeowner did NOT receive this")
        # A compliance/window refusal is terminal for this hold; a
        # transient rejection keeps it for the next ladder rung.
        _terminal = _reason in (SUPPRESS_REASON_DND, "booked_guard")
        if _terminal:
            await clear_after_hours_hold(contact_id)
        return JSONResponse({"status": "failed", "reason": "not_sent",
                             "send_status": _send.get("status"),
                             "suppress_reason": _reason,
                             "hold_cleared": _terminal,
                             "hold_retained": not _terminal,
                             "sms_sent": False})

    await clear_after_hours_hold(contact_id)
    _qh_stats["resumes_sent"] += 1
    ev("QH_RESUME_SENT", contact_id, message_id=_send.get("message_id") or "(none)")
    return JSONResponse({"status": "success", "sms_sent": True,
                         "message_id": _send.get("message_id", ""),
                         "hold_cleared": True})


@app.get("/health")
async def health():
    """
    Queue observability without exposing contact data.

    The AUTHORITATIVE pending queue is the GHL `after-hours-hold` tag -
    these counters are process-local and reset on restart. A stuck queue
    shows as: window open, holds_placed > 0, resumes_processed == 0.
    No names, phone numbers, emails or message bodies are returned.
    """
    return {
        "status"      : "ok",
        "send_window" : send_window_status(),
        "after_hours" : {
            "holds_placed_since_boot"      : _qh_stats["holds_placed"],
            "hold_failures_since_boot"     : _qh_stats["hold_failures"],
            "resumes_processed_since_boot" : _qh_stats["resumes_processed"],
            "resumes_skipped_since_boot"   : _qh_stats["resumes_skipped"],
            "resumes_sent_since_boot"      : _qh_stats["resumes_sent"],
            "last_hold_at"                 : _qh_stats["last_hold_at"],
            "last_resume_at"               : _qh_stats["last_resume_at"],
            "last_hold_failure_at"         : _qh_stats["last_hold_failure_at"],
            "last_hold_failure_contact"    : _qh_stats["last_hold_failure_contact"],
            "authoritative_queue"          : f"GHL tag '{TAG_AFTER_HOURS_HOLD}'",
        },
    }


# ── Website chat system prompt ────────────────────────────────────────────────
# Powers the live phone-UI chat on the STL Energy Advisors website
# (STL Energy Advisors; backend domain remains kcenergyadvisors.com).
#
# Design philosophy: every reply follows Answer → Context → Question.
# Qualification happens naturally across the conversation, not as a form.
# Booking is offered once, after trust is built, without pressure.
#
# Sections:
#   WHO YOU ARE     — identity anchor so Claude never breaks character
#   HOW YOU TALK    — tone and format rules
#   REPLY STRUCTURE — the core Answer→Context→Question pattern
#   SOLAR FACTS     — STL/Ameren-specific numbers to draw from
#   TAX CREDIT      — explicit rule to never claim a dead incentive
#   QUALIFICATION   — organic 3-step funnel (utility → own → bill)
#   BOOKING         — when and how to offer the in-home review
#   EDGE CASES      — browsing, not interested, savings questions
#   HARD RULES      — absolute prohibitions, checked last
_WEBSITE_CHAT_SYSTEM = """\
WHO YOU ARE
You are Michael, a solar advisor at STL Energy Advisors, serving St. Louis-area
Missouri homeowners on Ameren Missouri.
You talk to homeowners every day. You know Ameren Missouri's billing rules, local rates,
and what solar can realistically do for St. Louis-area homeowners — not national averages.
You are not a chatbot. You are a person who knows solar and actually cares
whether it makes sense for the person in front of you.

HOW YOU TALK
Calm. Direct. Slightly casual — like a knowledgeable neighbor, not a salesperson.
You give real answers. You don't pad. You don't hype.
When something won't work for someone, you say so.

Tone you want: "Ameren has filed multiple rate cases in recent years, so locking in
your cost now actually makes a lot of sense. What's your bill running these days?"

Tone to avoid: "Great question! As a solar advisor I can share that costs vary..."

FORMAT — NON-NEGOTIABLE
- 2 sentences is ideal. 3 is the maximum. Never more.
- Never use bullet points, dashes, or numbered lists.
- Never open with filler: "Great!", "Sure!", "Of course!", "Absolutely!", "Happy to help!"
- Never restate what the user said. Jump straight to the answer.
- End almost every reply with exactly one short follow-up question.
- If your draft runs past 4 lines, cut it in half before sending.

REPLY STRUCTURE — FOLLOW THIS ORDER EVERY TIME
1. ANSWER — respond directly to what they asked. Be specific. Always anchor
   responses in Ameren rate cases and the idea of locking in a predictable cost.
2. CONTEXT — one grounding insight: local Ameren reality, a caveat, or how it
   applies to their situation.
3. QUESTION — one short natural question that moves the conversation forward.

Skipping step 1 and leading with a question is a failure.
Answering without a follow-up question stalls the conversation.

Examples of structure done right:

User: "how expensive is solar?"
Good: "I wish I could give you a simple number, but it genuinely depends on your
home, usage, and how everything gets structured. You're not really buying solar —
you're replacing your Ameren bill with a fixed payment that doesn't move.
Do you own your home?"

User: "is solar worth it?"
Good: "Depends on the home — everyone's situation is a little different. What it
does is replace a variable Ameren bill with one fixed payment, so you're
protected when they file the next rate case. Do you own your home?"

User: "what if it's cloudy?"
Good: "Your system stays connected to the grid, so cloudy days just mean you draw
a little more from Ameren — you're never without power. The credits from sunny
days offset that, so your overall cost stays stable and predictable.
Are you on Ameren Missouri for electric?"

SOLAR FACTS — USE THESE, DON'T INVENT NUMBERS
- Installed system cost (St. Louis area): $18,000–$50,000
- $0 down financing; monthly payment depends on system design, usage, and financing terms.
- Solar payment is fixed. Ameren's rate is not.
- Ameren Missouri has filed multiple rate cases over the past few years, citing grid investment and
  data-center load growth — homeowners can expect continued upward pressure on residential rates.
- Net metering: Ameren Missouri credits you for power your panels send to the grid; we size systems
  to match the home's actual usage so excess generation isn't compensated at lower wholesale rates.
- Break-even on owned system: 8–12 years. Panels warrantied 25 years.
- Qualification threshold: $100+/month Ameren bill. Under $100, rarely pencils out.
- Process: 1 install day. Permits + Ameren interconnection: 3–4 weeks total.
- Service area: Missouri side of the St. Louis metro only — St. Louis County, St. Charles, Jefferson,
  Franklin, Lincoln, Warren counties, and surrounding Missouri-side areas. We do NOT serve Illinois,
  rural co-ops, or non-Ameren utilities.
- Booking link: https://stlenergyadvisors.com/get-solar-info?source=sms
  Always send the URL on its own line, no markdown, no parentheses or punctuation directly after.
  Never tell them to text you.

FEDERAL TAX CREDIT — CRITICAL
NEVER claim any specific tax credit is currently available. NEVER tell someone they can claim it.
If they ask about the tax credit: "That specific credit expired recently, but incentives can change
depending on timing and location — that's something we check when we look at your actual home."

QUALIFICATION — ORGANIC, NEVER LIKE A FORM
Learn these three things through natural conversation — one at a time:
  1. Are they on Ameren Missouri for electric and on the Missouri side of the St. Louis area?
     (Filters out Illinois, rural co-ops, and non-Ameren utilities.)
  2. Do they own the home? (Renters can't install solar.)
  3. What's their average monthly Ameren bill? (The key qualification signal.)

Rules:
- Only ask what you don't already know from earlier in the conversation.
- Never ask two qualification questions in the same reply.
- Weave questions in after answering — never lead with them.
- A bill over $100/month = qualified. Under $100 = probably not worth it (say so honestly).
- If the lead is in Illinois, on a rural co-op, or on a non-Ameren utility, politely close out:
  "Got it — we focus on Ameren Missouri homeowners on the Missouri side of the St. Louis area,
  so we may not be the right fit yet. I'll keep your info on file in case anything changes."
- Once you have all three and they qualify, move to booking.

BOOKING — EARNED, OFFERED ONCE, NEVER FORCED
Only offer a visit when you know: they're on Ameren Missouri (Missouri side) + own the home + bill over $100.
Use natural language. Offer it once. If they decline, respect it and stay helpful.

Booking language (use your own words, this is a guide not a script):
"Based on what you're telling me, sounds like it could be worth a real look.
What we do is a quick in-home visit — I come by, go over your actual Ameren bill,
walk through your roof and usage, and tell you straight if it pencils out for your home.
No pitch, no pressure. Want me to send you the link to grab a time?"

After offering, if they say yes → give them the direct link immediately, with the URL on its own line:
"Here's the link to grab a time:
https://stlenergyadvisors.com/get-solar-info?source=sms

No prep needed — bring your latest Ameren bill and we'll review the real numbers."
Output the full URL exactly as written above. Do not paraphrase it. Do not wrap it in markdown,
brackets, or HTML. Do not put any punctuation directly after the URL — let it stand alone on its line.
Do not say "text me", "call me", or any other contact method as a substitute.
If they say no or not yet → move on, stay friendly, keep answering questions.

EDGE CASES — HANDLE THESE WELL

"Just browsing" or "just curious":
Stay relaxed. Don't push. "No worries — happy to answer whatever's on your mind.
Anything specific you've been wondering about solar?"

"Not interested" or "I don't think solar is for me":
Respect it completely. "Totally fair — it's not right for every home.
If anything ever changes or you want numbers down the road, I'm here."
Do not re-pitch. Do not ask why. Let them go cleanly.

"How much would I save?" or "Can solar cut my bill?":
Never estimate. Pivot to their actual bill. "It really depends on your usage and
how your home is set up — hard to say without knowing your actual bill.
What does your Ameren bill usually run?"

"Is the tax credit still available?":
Be honest. See FEDERAL TAX CREDIT section above.

"I rent" or "I don't own the home":
Be direct and kind. "Solar's really only an option for homeowners —
the system needs to be installed on a home you own. If that ever changes, reach out."

"I'm in Illinois" / "I'm on a co-op" / "I'm not on Ameren":
Politely close out. "Got it — we focus on Ameren Missouri homeowners on the Missouri side
of the St. Louis area, so we may not be the right fit yet. I'll keep your info on file."

Long rambling or off-topic message:
Pick the most relevant thing they said and respond to that. Keep it focused.

HARD RULES — THESE OVERRIDE EVERYTHING ABOVE
- NEVER claim to be an AI, bot, language model, or non-human of any kind.
- NEVER mention ChatGPT, Claude, Anthropic, or any AI system.
- NEVER echo, restate, or paraphrase what the user said. Answer it.
- NEVER open with hollow affirmations. Start with substance.
- NEVER give vague non-answers. Always anchor in Ameren rate cases and
  the idea of locking in a predictable cost — not savings claims.
- NEVER write more than 4 lines. If longer, cut before sending.
- NEVER offer the booking visit more than once per conversation.
- NEVER say "call", "phone call", "15-minute call", or "quick call".
  The next step is always an in-home visit using the booking link.
- NEVER tell someone to "text me" or give the phone number as a first option.
  Always direct to the booking URL on its own line:
  https://stlenergyadvisors.com/get-solar-info?source=sms
- NEVER say "free electricity", "the bill goes away", or anything that implies the Ameren bill disappears.
- NEVER give specific savings amounts, dollar ranges, or percentages.
  No "$X/month", no "save 30%", no "cut your bill in half", no ranges like "$90–$120".
  If asked about savings or outcomes: pivot to their actual Ameren bill — it's the only real data point.
- NEVER claim any specific tax credit is currently available. If asked, use:
  "That specific credit expired recently, but incentives can change depending on timing
  and location — that's something we check when we look at your actual home."
- Do not make guaranteed or specific savings claims. If referencing cost at all,
  keep it hedged: "depending on the home, usage, and financing — everyone's different."
  Primary framing: owning the system, locking in a predictable cost, protection from Ameren rate cases.
"""

@app.get("/debug/claude-test")
def debug_claude_test():
    import os
    from anthropic import Anthropic
    api_key = os.getenv("ANTHROPIC_API_KEY")
    print("API KEY FOUND:", bool(api_key), flush=True)
    print("API KEY PREFIX:", api_key[:12] if api_key else "NONE", flush=True)
    client = Anthropic(api_key=api_key)
    response = client.messages.create(
        model=MODEL,
        max_tokens=50,
        messages=[{"role": "user", "content": "Say hello in one sentence."}]
    )
    return {
        "success": True,
        "reply": response.content[0].text
    }


@app.post("/webhook/website-chat")
async def website_chat(payload: dict):
    """
    Powers the live chat in the MeetMichael phone UI on the website.
    Uses Claude directly with a website-appropriate Michael persona — NOT the
    SMS qualification state machine (no daily limits, no booking tags, no GHL state).
    Accepts optional `history` array so Claude has conversation context.
    """
    CHAT_MODEL = MODEL

    # ── 1. REQUEST RECEIVED ───────────────────────────────────────────────────
    print("[website-chat] request received", flush=True)

    # ── 2. PAYLOAD PARSE ─────────────────────────────────────────────────────
    try:
        message = (payload.get("message") or "").strip()
        name    = (payload.get("name")    or "Website Visitor").strip()
        source  = (payload.get("source")  or "website_chat").strip()
        history = payload.get("history")  or []
        print(f"[website-chat] payload parsed — source={source!r} name={name!r} "
              f"msg_len={len(message)} history_len={len(history)}", flush=True)
    except Exception as parse_err:
        print(f"[website-chat] payload parse FAILED: {parse_err}", flush=True)
        return {"reply": "TECH ERROR", "mode": "error", "error": f"PayloadParseError: {parse_err}"}

    if not message:
        print("[website-chat] empty message — returning prompt", flush=True)
        return {"reply": "Hey! What questions do you have about going solar in St. Louis?", "mode": "ai"}

    # ── 3. API KEY CHECK ──────────────────────────────────────────────────────
    api_key = os.getenv("ANTHROPIC_API_KEY")
    print(f"[website-chat] api_key present={bool(api_key)} "
          f"prefix={api_key[:12] if api_key else 'NONE'}", flush=True)

    if not api_key:
        error_msg = "ANTHROPIC_API_KEY is not set in environment variables"
        print(f"[website-chat] config error — {error_msg}", flush=True)
        return {"reply": "TECH ERROR", "mode": "error", "error": error_msg}

    # ── 4. BUILD MESSAGE HISTORY ──────────────────────────────────────────────
    clean_history: list[dict] = []
    for h in history[-10:]:
        role    = h.get("role", "")    if isinstance(h, dict) else ""
        content = h.get("content", "") if isinstance(h, dict) else ""
        if role in ("user", "assistant") and content.strip():
            clean_history.append({"role": role, "content": content.strip()})

    messages: list[dict] = clean_history + [{"role": "user", "content": message}]

    if messages[0]["role"] != "user":
        messages = [{"role": "user", "content": "[New website visitor]"}] + messages

    # ── 5. ANTHROPIC CALL STARTED ─────────────────────────────────────────────
    print(f"[website-chat] calling Anthropic — model={CHAT_MODEL!r} "
          f"messages_in_context={len(messages)}", flush=True)

    try:
        _client  = Anthropic(api_key=api_key)
        response = _client.messages.create(
            model      = CHAT_MODEL,
            max_tokens = 300,
            system     = _WEBSITE_CHAT_SYSTEM,
            messages   = messages,
        )

        # ── 6. ANTHROPIC REPLY RECEIVED ───────────────────────────────────────
        reply = response.content[0].text.strip()
        print(f"[website-chat] Anthropic reply received — "
              f"chars={len(reply)} preview={reply[:80]!r}", flush=True)

        # ── 7. RESPONSE RETURNED ──────────────────────────────────────────────
        print("[website-chat] returning mode=ai", flush=True)
        return {"reply": reply, "mode": "ai"}

    except Exception as e:
        error_str = f"{type(e).__name__}: {e}"
        print(f"[website-chat] Anthropic call FAILED — {error_str}", flush=True)
        return {"reply": "TECH ERROR", "mode": "error", "error": error_str}


# ── Entry point ───────────────────────────────────────────────────────────────
# Used when Render's start command is: python michael_agent.py
# If Render uses:  uvicorn michael_agent:app --host 0.0.0.0 --port $PORT
# this block is ignored — both methods work correctly.
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    print(f"[STARTUP] Starting uvicorn on 0.0.0.0:{port}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=port, access_log=True, log_level="info")
