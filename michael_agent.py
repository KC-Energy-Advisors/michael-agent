"""
michael_agent.py  ·  v3.8  (dedup rewrite: 8-second window + GHL message-ID tier)
────────────────────────────────────────────────────────────────────────────────
KC Energy Advisors — AI Appointment-Setting Agent
Agent Name : Michael
Model      : Claude (claude-opus-4-6)
Platform   : GoHighLevel (GHL) via inbound webhook

────────────────────────────────────────────────────────────────────────────────

HOW IT WORKS
────────────
1.  GHL fires a webhook to /webhook/inbound when a contact sends an SMS
    or when a chat widget / calendar form is submitted.
2.  For form submissions (new unbooked leads), Michael fires a predefined
    first-outreach SMS (introduces Michael/KC Energy Advisors, asks SERVICE AREA
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

  [FIX-5.C]  build_new_contact_outreach() — ownership-first format
             New message format:
               "Hey {first}, this is Michael with KC Energy Advisors.
                Thanks for sending that over. Based on what you shared, you
                may qualify for solar savings.
                Quick question so I can point you in the right direction:
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
    BOOKING_LINK=https://kcenergyadvisors.com/get-solar-info
    BOOKED_TAG=appointment booked
    PORT=8000
"""

import os
import re
import sys
import json
import hashlib
import logging
import traceback
from enum import Enum
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
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

app    = FastAPI(title="Michael — KC Energy Advisors AI Agent")

# ── CORS — allows browser requests from the Vercel frontend ──────────────
# allow_origin_regex covers all preview/branch deploys automatically.
# allow_credentials is False because the frontend sends no cookies or auth headers.
# FastAPI/Starlette CORSMiddleware handles OPTIONS preflight automatically.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kc-energy-advisors-v2.vercel.app",
        "https://kcenergyadvisors.com",
        "http://localhost:3000",
    ],
    allow_origin_regex=r"https://kc-energy-advisors-v2[\w-]*\.vercel\.app",
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
_CORRECT_BOOKING_URL = "https://kcenergyadvisors.com/get-solar-info"

# BOOKING_LINK is the runtime variable used throughout the module.
# We read from .env for flexibility, but immediately override any bad value.
BOOKING_LINK = os.getenv("BOOKING_LINK", _CORRECT_BOOKING_URL)

# [FIX-5 + FIX-6A] Startup guard — detect AND self-heal a bad .env value.
# Previously this block only LOGGED the bad value; it never fixed BOOKING_LINK,
# so the bad URL flowed into every booking SMS for the entire process lifetime.
# Now we override BOOKING_LINK to _CORRECT_BOOKING_URL immediately.
_OLD_BOOKING_DOMAINS = ("api.leadconnectorhq.com", "leadconnectorhq.com/widget/booking")
if any(old in BOOKING_LINK for old in _OLD_BOOKING_DOMAINS):
    log.warning("=" * 70)
    log.warning("⚠️  BOOKING LINK WARNING — old LeadConnector URL detected in .env!")
    log.warning(f"⚠️  Bad value     : {BOOKING_LINK}")
    log.warning(f"⚠️  Self-healing  : BOOKING_LINK overridden to {_CORRECT_BOOKING_URL}")
    log.warning("⚠️  Fix your .env: BOOKING_LINK=https://kcenergyadvisors.com/get-solar-info")
    log.warning("=" * 70)
    print("=" * 70)
    print("⚠️  STARTUP: bad BOOKING_LINK in .env — self-healing to correct URL")
    print(f"⚠️  Was  : {BOOKING_LINK}")
    print(f"⚠️  Now  : {_CORRECT_BOOKING_URL}")
    print("⚠️  Update your .env to silence this warning.")
    print("=" * 70)
    BOOKING_LINK = _CORRECT_BOOKING_URL   # ← THE FIX: override the bad env value
GHL_API_KEY     = os.getenv("GHL_API_KEY",     "")   # must be set in .env — no hardcoded fallback
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID", "")   # must be set in .env — no hardcoded fallback
GHL_FROM_NUMBER = os.getenv("GHL_FROM_NUMBER", "+18163190932")
GHL_API_BASE    = "https://services.leadconnectorhq.com"

# [FIX-7] Warn loudly at startup if the required GHL credentials are missing.
# These can never have useful hardcoded fallbacks — they are account-specific secrets.
for _cred_name, _cred_val in (("GHL_API_KEY", GHL_API_KEY), ("GHL_LOCATION_ID", GHL_LOCATION_ID)):
    if not _cred_val:
        log.warning(f"⚠️  STARTUP WARNING: {_cred_name} is not set — all GHL API calls will fail until this is configured in .env")
model="claude-3-7-sonnet-latest"
MAX_DAILY_MSGS  = 6
CENTRAL_TZ      = ZoneInfo("America/Chicago")
BOOKED_TAG      = os.getenv("BOOKED_TAG", "appointment booked")


# ─────────────────────────────────────────────
#  QUALIFICATION STAGES
# ─────────────────────────────────────────────

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
#  IN-MEMORY STATE STORE
# ─────────────────────────────────────────────

def get_state(contact_id: str) -> dict:
    if contact_id not in _state_store:
        _state_store[contact_id] = {
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
            # Booking / appointment state
            "qualified"             : False,
            "booking_detected"      : False,
            "booking_follow_up_sent": False,
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
        }
    return _state_store[contact_id]

_state_store: dict[str, dict] = {}

def save_state(contact_id: str, state: dict):
    _state_store[contact_id] = state


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

    raw_message = _safe_str(
        body.get("message") or body.get("body") or body.get("text") or
        body.get("messageBody") or body.get("message_body") or
        body.get("smsBody") or body.get("sms_body") or ""
    )
    # [FIX-3.3] has_real_message is True ONLY for genuine human-typed messages.
    # Placeholder values like "start" are injected by GHL for form submissions
    # and chat widget initiations — they are NOT real SMS replies.
    # Treating them as real messages caused is_chat_widget_lead_payload() to
    # reject legitimate widget leads with "has_real_message=True (live SMS reply)".
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
        body.get("message_direction") or "inbound"
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


def is_duplicate_inbound(
    contact_id: str,
    message: str,
    ghl_message_id: str = "",
) -> tuple[bool, str]:
    """
    [FIX-9] Replaces the old is_duplicate().

    Returns (is_duplicate: bool, reason: str).
    Callers MUST use the reason string in their skip log.

    True  → this webhook is a duplicate delivery; skip it.
    False → this webhook is new; process it (fingerprint has been registered).

    Logging contract (caller must print on True):
      [DEDUP] SKIPPED | {reason} | contact={contact_id}

    Logging contract (caller should print on False when debugging):
      [DEDUP] ALLOWED | {reason} | contact={contact_id}
    """
    fp, desc = _inbound_fingerprint(contact_id, message, ghl_message_id)

    if fp in _processed_fingerprints:
        return True, f"fingerprint_match | {desc}"

    # Not a duplicate — register fingerprint and allow through
    if len(_processed_fingerprints) >= _MAX_DEDUP_CACHE:
        # LRU eviction: keep newest half
        _keep = list(_processed_fingerprints)[_MAX_DEDUP_CACHE // 2 :]
        _processed_fingerprints.clear()
        _processed_fingerprints.update(_keep)

    _processed_fingerprints.add(fp)
    return False, f"new_event | {desc}"


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


def _is_duplicate_event_id(event_id: str) -> bool:
    """
    [FIX-11] Returns True if this event_id was already processed.
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
    return False


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

# Service-area confirmation — explicit city/state/distance mentions
_LOC_YES_EXPLICIT = re.compile(
    r"(i'?m?\s*(in|near|close\s+to|by)\b"
    r"|within\s+\d+\s*(min|minutes?|hr|hours?)"
    r"|(about|around|less\s+than)\s+\d+\s*(min|minutes?|hr|hours?)"
    r"|\b(kc|kansas\s*city|st\.?\s*louis|saint\s+louis|missouri)\b)",
    re.IGNORECASE,
)

# Bill amount: "$150", "$200/month", "150 a month", "200 dollars a month"
_BILL_AMOUNT = re.compile(
    r'\$\s*(\d{2,4})(?:\s*(?:/\s*month|a\s+month|per\s+month|monthly))?'
    r'|(\d{2,4})\s*(?:dollars?|bucks?)\s*(?:a\s+month|per\s+month|monthly)'
    r'|(\d{2,4})\s*(?:a\s+month|per\s+month|monthly)\b',
    re.IGNORECASE,
)


def _detect_homeowner(text: str, stage: Stage, location_confirmed: bool = False) -> Optional[str]:
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
    # [FIX-8.C] At ASK_OWNERSHIP, brief yes/no is always trusted —
    # the stage itself proves what question was just asked.
    if stage == Stage.ASK_OWNERSHIP:
        if _BRIEF_YES.match(text.strip()):
            return "yes"
        if _BRIEF_NO.match(text.strip()):
            return "no"
    return None


def _detect_location_confirmed(text: str, location_confirmed_already: bool, homeowner_set: bool) -> bool:
    """
    Return True if this inbound text confirms service area (within ~90 min of KC/STL).

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
    # Brief 'yes' when on the very first question (location not confirmed, no owner answer yet)
    if not homeowner_set and _BRIEF_YES.match(text.strip()):
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


def update_state_from_inbound(state: dict, inbound_text: str) -> None:
    """
    [ADAPT-2] Parse the lead's inbound message and update state fields in place.
    Called BEFORE Claude so build_system_prompt() sees the latest signals.
    Only updates fields that are currently unknown to avoid false overrides.
    """
    stage      = state["stage"]
    loc_conf   = bool(state.get("location_confirmed"))
    homeown    = state.get("homeowner")

    # Service area — parse before ownership so location_confirmed is up to date
    # when _detect_homeowner() decides whether to trust brief yes/no.
    if _detect_location_confirmed(inbound_text, loc_conf, homeowner_set=homeown is not None):
        state["location_confirmed"] = True
        loc_conf = True
        print(f"[STATE] 📍 location_confirmed=True parsed from: {inbound_text[:60]!r}")

    # Homeowner — only parse when we haven't confirmed yet.
    # Pass location_confirmed so brief 'yes' is only trusted after area is confirmed.
    if state.get("homeowner") is None:
        detected = _detect_homeowner(inbound_text, stage, location_confirmed=loc_conf)
        if detected:
            state["homeowner"] = detected
            print(f"[STATE] 🏠 homeowner={detected!r} parsed from: {inbound_text[:60]!r}")

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
            f"With a {bill} bill you're probably overpaying — "
            f"solar is usually structured so your monthly solar payment is less than what you're paying now."
        )
    else:
        bill_line = (
            "For most KC homeowners solar is structured so your monthly payment is less "
            "than what you're currently paying the utility."
        )

    base = (
        f"No upfront cost — that's actually one of the most common misconceptions. "
        f"{bill_line} "
        f"The consultation is 100% free with zero obligation — your advisor just runs the real numbers "
        f"for your specific home so you can decide if it makes sense."
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
        "It's a quick 30-minute in-person visit at your home. "
        "Your advisor pulls up your actual utility data, runs the numbers, "
        "and shows you exactly what solar would save you for your specific house. "
        "No pressure at all — if it doesn't pencil out, they'll tell you straight up."
    )

    if qualified:
        contact_name = state.get("contact_name", "")
        booking = build_booking_message(full_name=contact_name)
        return f"{base}\n\n{booking}"
    return base


# ─────────────────────────────────────────────
#  DYNAMIC SYSTEM PROMPT  [ADAPT-1]
#
#  Builds a context-aware system prompt for every
#  Claude call. The "WHAT YOU ALREADY KNOW" block
#  tells Claude what NOT to ask so it can focus on
#  the next qualification step or drive booking.
# ─────────────────────────────────────────────

def build_system_prompt(state: dict) -> str:
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
    if stage in (Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED):
        confirmed.append("✓ HOMEOWNER: confirmed — do NOT ask about home ownership again")
    elif homeown == "yes":
        confirmed.append("✓ HOMEOWNER: confirmed (volunteered earlier) — do NOT ask again")
    elif homeown == "no":
        confirmed.append("✗ NOT A HOMEOWNER: they are renting — disqualify if not already done")

    # Service area
    if stage in (Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED) or loc_conf:
        confirmed.append("✓ SERVICE AREA: confirmed — do NOT ask about location/area again")

    # Electric bill
    if stage in (Stage.SEND_BOOKING, Stage.BOOKED):
        confirmed.append("✓ ELECTRIC BILL: qualified — do NOT ask about bill again")
    elif bill:
        confirmed.append(f"✓ ELECTRIC BILL: mentioned as {bill} — do NOT ask about bill again")

    if not confirmed:
        # v3.5: Widget leads have location_confirmed=True set at first-outreach time.
        # If we reach here with nothing confirmed, the lead is genuinely at INITIAL
        # (no state yet), which should not happen for widget contacts — but handle
        # gracefully by starting at ownership (not area, since address was collected).
        confirmed_block = "  Nothing confirmed yet — start with home ownership (step 1)."
    else:
        confirmed_block = "\n".join(f"  {c}" for c in confirmed)

    # ── CURRENT GOAL ──────────────────────────────────────────────
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
    elif stage == Stage.ASK_BILL or (homeown == "yes" and loc_conf and not bill):
        current_goal = "ASK BILL — ask for their average monthly electric bill (ballpark is fine). $75+/month qualifies."
    elif loc_conf and homeown != "yes":
        # Area confirmed — move to ownership question next
        current_goal = "ASK OWNERSHIP — find out if they own the home."
    elif homeown == "yes" and not loc_conf:
        # Homeowner confirmed (volunteered early) but area not yet checked
        current_goal = "ASK AREA — confirm they are within ~90 minutes of Kansas City or St. Louis, MO."
    elif stage == Stage.ASK_LOCATION and not loc_conf:
        # Legacy stage label for old contacts — treat as still needing area answer
        current_goal = "ASK AREA — confirm they are within ~90 minutes of Kansas City or St. Louis, MO."
    else:
        # New contact or unknown state — ask ownership.
        # Widget leads always have location_confirmed=True set at first-outreach time,
        # so this fallback fires only for edge cases (legacy state, manual resets).
        # Ask ownership first; area will only be needed if loc_conf is explicitly False.
        current_goal = "ASK OWNERSHIP — find out if they own the home."

    return f"""You are Michael, a solar setter for KC Energy Advisors in Kansas City, Missouri.
Your one job: qualify leads and book them for a FREE IN-PERSON solar consultation.
Text like a sharp, confident, warm local person — short messages, natural tone.

━━ WHAT YOU ALREADY KNOW ABOUT THIS LEAD ━━
{confirmed_block}

CURRENT GOAL: {current_goal}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QUALIFICATION ORDER — skip any step already confirmed above:
1. OWN THE HOME?
   Ask: "Do you own the home?" (or confirm from their reply to Michael's first message)
   → Owner: continue
   → Renter: "Got it — solar really only works for homeowners. If that ever changes, reach out." [DISQUALIFY:NOT_OWNER]

2. ELECTRIC BILL?
   Ask: "What's your average monthly electric bill — ballpark is totally fine."
   → $75+/month: QUALIFIED → go to BOOKING immediately
   → Under $75: "At that rate, solar savings would be pretty minimal right now. I'll hold onto your info in case rates shift." [DISQUALIFY:LOW_BILL]

NOTE: Service area is confirmed at form-submission time (the lead provided their address).
If "SERVICE AREA: confirmed" appears above, do NOT ask about location — proceed directly to ownership.

BOOKING — move here as soon as they qualify. Do NOT stall or ask extra questions.
When they qualify, write ONE natural transition sentence then append [SEND_BOOKING].
The actual booking link and full message are sent automatically — do NOT include a URL in your reply.
Example output: "Based on what you shared, sounds like your home is worth a closer look. [SEND_BOOKING]"
Example output: "That sounds promising — let me get you set up for a free in-person look. [SEND_BOOKING]"
IMPORTANT: If the lead asks ANY question (message contains "?") while qualifying — even right after giving
their bill amount — ANSWER the question naturally in 1-2 sentences FIRST, then transition to booking.
Never ignore a question by jumping straight to the booking invite.

━━ BEHAVIOR RULES ━━
• 1-2 sentences per message max (3 only if truly necessary)
• NEVER re-ask about anything listed as confirmed in "WHAT YOU ALREADY KNOW" above
• If the lead volunteers information before you ask, acknowledge it and move on — never ask again
• If they ask a question, answer it briefly and naturally, then continue toward your goal
• If they give a vague reply ("yes", "sure"), infer from conversation context what they're responding to
• "I already booked" / "I just scheduled" → [BOOKED]
• NEVER repeat "KC Energy Advisors" after the first message
• NEVER use: "Great!", "Absolutely!", "Of course!", "Certainly!" — too robotic
• These are IN-PERSON consultations, not phone calls — never say "phone call" or "virtual"
• Once someone is qualified, move directly to booking — do NOT keep asking more questions
• Sound helpful, confident, and local — like a real person, not a script or a form

━━ OBJECTIONS ━━
• "Not interested" → "No worries — if that ever changes, we're here." [DISQUALIFY:NOT_INTERESTED]
• "How much does solar cost?" → "That's exactly what the consultation covers — your advisor will go through real numbers for your specific home. It's completely free."
• "Is this a scam?" → "Legit question — KC Energy Advisors is a licensed local solar firm out of KC. Free consultation, zero obligation."
• "Can someone call me?" → "Of course — what's the best time to reach you today?"
• Persistent hesitation → "There's no commitment — it's just a free look at whether it actually makes sense for your home."
• Clear disinterest → "No worries, take care." [DISQUALIFY:NOT_INTERESTED]

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

STOP_KEYWORDS = re.compile(
    r"\b(stop|quit|cancel|end|unsubscribe|opt.?out|remove me|take me off)\b",
    re.IGNORECASE,
)

BOOKED_KEYWORDS = re.compile(
    r"\b(booked|scheduled|just booked|i booked|confirmed|i scheduled|set it up|grabbed a time)\b",
    re.IGNORECASE,
)

def is_stop_request(text: str)         -> bool: return bool(STOP_KEYWORDS.search(text))
def is_booking_confirmation(text: str) -> bool: return bool(BOOKED_KEYWORDS.search(text))


# ─────────────────────────────────────────────
#  BOOKED-CONTACT MESSAGES
# ─────────────────────────────────────────────

def _resolve_first_name(first_name: str = "", full_name: str = "") -> str:
    """Shared name resolution: prefer first_name, fall back to first word of full_name."""
    candidate = first_name.strip() if first_name else ""
    if candidate and candidate.lower() not in ("unknown", "none", ""):
        return candidate
    candidate = full_name.strip() if full_name else ""
    if candidate and candidate.lower() not in ("unknown", "none", ""):
        return candidate.split()[0]
    return ""


def build_bill_reminder_message(first_name: str = "", full_name: str = "") -> str:
    """
    PATH 2 — DIRECT BOOKING.
    Sent when a contact books directly via the calendar page with no prior SMS conversation.
    This is likely the first text they've ever received from Michael, so it includes a
    brief confirmation and introduces the ask naturally.
    """
    first    = _resolve_first_name(first_name, full_name)
    greeting = f"Hey {first}!" if first else "Hey!"
    return (
        f"{greeting} You're all booked — I'll stop by for about 30 minutes, "
        f"totally free, no pressure. "
        f"One quick thing before I come by: can you send over a photo of your most recent "
        f"electric bill? Helps me have your numbers ready when I get there 👍"
    )


def build_bill_reminder_message_sms_flow(first_name: str = "", full_name: str = "") -> str:
    """
    PATH 1 — CHAT WIDGET → SMS FLOW → BOOKED.
    Sent to contacts who went through the chat widget qualification conversation
    and THEN booked.  They've already been talking to Michael — this is the
    next line in the same conversation, not a fresh re-introduction.
    No name re-greeting, no appointment confirmation — they know they booked.
    """
    first = _resolve_first_name(first_name, full_name)
    lead  = f" {first}" if first else ""
    return (
        f"Perfect{lead}, you're all set! "
        f"Go ahead and send over a photo of your most recent electric bill when you get a chance — "
        f"I'll look everything over before I come by 👍"
    )


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

def _build_booked_followup_prompt(state: dict) -> str:
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

    return f"""You are Michael with KC Energy Advisors. You are texting a lead who HAS ALREADY BOOKED an in-person solar consultation appointment.

{name_line}
  Appointment   : CONFIRMED — do NOT re-confirm, re-qualify, or re-sell.
  Your job      : Answer simple follow-up questions warmly and briefly. Close the conversation naturally.

━━ ⛔ HARD STOP — DO NOT DO ANY OF THE FOLLOWING ⛔ ━━
• DO NOT send a booking link, calendar link, or any URL
• DO NOT output [SEND_BOOKING], [QUALIFIED], or any control tag
• DO NOT ask about home ownership, service area, or electric bill
• DO NOT say "Based on what you shared" or any qualifying language
• DO NOT suggest they book — they already booked
• DO NOT restart the sales flow for any reason

━━ WHAT YOU SHOULD DO ━━
• If they say "thanks" / "sounds good" / "see you then" → reply with "You're all set! See you then 👍" or similar short close.
• If they ask what to expect → "Just a relaxed 30-minute visit — your advisor will walk you through the numbers for your home."
• If they ask about timing / what to bring → answer naturally and briefly (1-2 sentences max).
• If they express excitement → match the energy briefly, close warmly.
• If they seem confused → reassure them calmly in 1-2 sentences.

━━ STYLE ━━
• 1-2 sentences max
• Warm, confident, local tone
• No filler words ("Great!", "Absolutely!")
• No URLs, no tags, no qualification language""".strip()


# ─────────────────────────────────────────────
#  FIRST-CONTACT OUTREACH MESSAGE  [ADAPT-5]
#
#  Sent proactively to brand-new unbooked leads
#  from a chat widget / form submission.
#  Subsequent replies go through michael_agent().
# ─────────────────────────────────────────────

def build_new_contact_outreach(
    first_name: str = "",
    full_name:  str = "",
    address:    str = "",
) -> str:
    """
    First proactive SMS sent to a new chat-widget lead.

    v3.5 format — intro + ownership question using the address from the form.
    The area/location check is bypassed: a lead who submitted a valid home
    address via the widget is assumed to be a local prospect.
    (location_confirmed is set to True by the caller so the system prompt
    reflects this and Claude never re-asks about area.)

    Format (with address):
      "Hey {first}, this is Michael with KC Energy Advisors.
       Thanks for sending that over. Based on what you shared, you may qualify
       for solar savings.
       Quick question so I can point you in the right direction:
       Do you own the home at {street}?"

    Format (no address):
      "Hey {first}, this is Michael with KC Energy Advisors.
       Thanks for sending that over. Based on what you shared, you may qualify
       for solar savings.
       Quick question so I can point you in the right direction:
       Do you own your home?"

    Fallback:
      • No first name → "Hey," instead of "Hey {first},"
    """
    first    = _resolve_first_name(first_name, full_name)
    greeting = f"Hey {first}," if first else "Hey,"

    # Extract street line only — avoid repeating city/state/zip in the question.
    # "123 Main St, Kansas City, MO 64101" → "123 Main St"
    street = ""
    if address:
        street = address.split(",")[0].strip()

    if street:
        ownership_q = f"Do you own the home at {street}?"
    else:
        ownership_q = "Do you own your home?"

    return (
        f"{greeting} this is Michael with KC Energy Advisors.\n"
        f"Thanks for sending that over. Based on what you shared, you may qualify for solar savings.\n"
        f"Quick question so I can point you in the right direction:\n"
        f"{ownership_q}"
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

    With first name:
      "Based on what you shared, Barb, your home looks like a solid candidate.
       You can grab a time here for a quick in-person savings report
       (takes about 30 minutes, super laid back):
       https://kcenergyadvisors.com/get-solar-info
       Let me know once you book and I'll get everything ready 👍"

    Without first name: same message, no name inserted.
    """
    first = _resolve_first_name(first_name, full_name)

    if first:
        opener = f"Based on what you shared, {first}, your home looks like a solid candidate."
    else:
        opener = "Based on what you shared, your home looks like a solid candidate."

    return (
        f"{opener}\n\n"
        f"You can grab a time here for a quick in-person savings report. "
        f"It takes about 30 minutes, it's free, and we'll show you what solar "
        f"could look like for your home specifically:\n"
        f"{BOOKING_LINK}\n\n"
        f"Once you grab a time, I'll shoot you a quick message before I come by 👍"
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

# Matches the specific broken URL and any other
# leadconnectorhq.com/widget/booking/... variant
_BAD_BOOKING_URL_RE = re.compile(
    r'https?://[a-zA-Z0-9._-]*leadconnectorhq\.com/widget/booking/[^\s"\'<>]*',
    re.IGNORECASE,
)


def sanitize_outbound_message(text: str, contact_id: str = "") -> str:
    """
    [FIX-6] Detect and replace any LeadConnector booking URL in outbound text.

    If a bad URL is found it is replaced with BOOKING_LINK and a warning is
    logged so the source can be tracked down.  The function is idempotent —
    if the message is already clean it is returned unchanged at zero cost.
    """
    if not _BAD_BOOKING_URL_RE.search(text):
        return text   # fast path — nothing to do

    # Use _CORRECT_BOOKING_URL (hardcoded), NOT BOOKING_LINK — if BOOKING_LINK
    # itself is wrong (stale .env, race condition) this still produces a safe message.
    fixed = _BAD_BOOKING_URL_RE.sub(_CORRECT_BOOKING_URL, text)
    tag   = f"[{contact_id}] " if contact_id else ""
    log.warning(
        f"{tag}[SANITIZE] ⚠️  Bad LeadConnector booking URL detected in outbound message — replaced with BOOKING_LINK"
    )
    print(f"[SANITIZE] ⚠️  Bad booking URL found in outbound message{' for ' + contact_id if contact_id else ''}!")
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

def michael_agent(contact_id: str, inbound_text: str) -> Optional[str]:
    """
    Main entry point for processing inbound SMS from a lead.
    Returns Michael's reply string, or None if no reply should be sent.
    Never raises — all errors are caught internally.
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

        # ── STOP 0 + 1: Flow complete — absolute no-op ────────────
        if state.get("final_confirmation_sent") or state.get("bill_received"):
            print(
                f"[AGENT] 🔒 FLOW COMPLETE — "
                f"final_confirmation_sent={state.get('final_confirmation_sent')} | "
                f"bill_received={state.get('bill_received')} → no reply"
            )
            log.info(f"[{contact_id}] AGENT hard-stop: flow complete (final_confirmation_sent/bill_received)")
            return None

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
            state["stage"] == Stage.BOOKED
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
            booked_system = _build_booked_followup_prompt(state)

            # Append message to history
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

        # ── Parse inbound for state signals [ADAPT-2] ─────────────
        update_state_from_inbound(state, inbound_text)

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
        system = build_system_prompt(state)
        print(f"[AGENT] Goal: {_goal_from_prompt(system)}")
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
            state["stage"] = Stage.ASK_OWNERSHIP

        # ── Infer confirmed facts from stage transitions [ADAPT-3] ─
        # If we advanced past ownership, homeowner is confirmed.
        if state["stage"] in (Stage.ASK_LOCATION, Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED):
            if state.get("homeowner") is None:
                state["homeowner"] = "yes"
        # If we advanced past location, area is confirmed.
        if state["stage"] in (Stage.ASK_BILL, Stage.SEND_BOOKING, Stage.BOOKED):
            state["location_confirmed"] = True
        # Track qualified flag
        if state["stage"] in (Stage.SEND_BOOKING, Stage.BOOKED):
            state["qualified"] = True

        # ── [FIX-6] Sanitise before sending — catch any hallucinated URL ──
        clean_reply = sanitize_outbound_message(clean_reply, contact_id)

        # ── Append assistant reply to history ─────────────────────
        state["messages"].append({"role": "assistant", "content": clean_reply})
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


async def send_sms_via_ghl(contact_id: str, message: str, to_number: str = "") -> dict:
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
    # ── Outbound duplicate suppression [ADAPT-6] ──────────────────
    if is_duplicate_outbound(contact_id, message):
        print(f"\n[DUPLICATE] ⚡ Outbound dedup — same message to {contact_id} already sent this minute — SKIPPING")
        log.warning(f"[{contact_id}] Outbound duplicate suppressed: {message[:80]!r}")
        return {"sent": False, "deduped": True, "status_code": None, "response_body": None}

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
        log.info(f"[{contact_id}] SMS sent via GHL: status={r.status_code} to={_resolved_to_number!r}")
        return {"sent": True, "deduped": False, "status_code": r.status_code, "response_body": resp_body}
    else:
        log.error(
            f"[{contact_id}] GHL SMS rejected: status={r.status_code} "
            f"to={_resolved_to_number!r} body={r.text[:300]!r}"
        )
        r.raise_for_status()
        # raise_for_status() always raises here; this return is for type-checker only
        return {"sent": False, "deduped": False, "status_code": r.status_code, "response_body": resp_body}


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


def resolve_ghl_tags(stage: Stage) -> list[str]:
    tag_map = {
        Stage.SEND_BOOKING : ["QUALIFIED", "BOOKING_LINK_SENT"],
        Stage.BOOKED       : ["APPOINTMENT_BOOKED"],
        Stage.DISQUALIFIED : ["NOT_QUALIFIED"],
        Stage.DNC          : ["DNC"],
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

        print(f"[INBOUND] Contact    : {full_name!r} ({contact_id})")
        print(f"[INBOUND] Phone      : {phone or '(not provided)'}")
        print(f"[INBOUND] Direction  : {direction}")
        print(f"[INBOUND] Tags       : {tags}")
        print(f"[INBOUND] Address    : {address or '(not provided)'}")
        print(f"[INBOUND] Lead source: {lead_source or '(not provided)'}")
        if custom_fields:
            print(f"[INBOUND] Custom fields: {custom_fields}")

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
        if _event_id and _is_duplicate_event_id(_event_id):
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
        if phone:
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
        # Once "Perfect, got it. We'll see you then 👍" has been sent,
        # the flow is 100% complete.  Every subsequent message — including
        # GHL workflow echoes, attachment notifications, or any other stray
        # webhook — is silently dropped here.  Nothing will ever send again.
        if _priority_state.get("final_confirmation_sent"):
            _ri = parsed.get("has_real_message") or parsed.get("has_attachment")
            print(
                f"\n[GUARD] 🔒 PRIORITY GUARD 0: final_confirmation_sent=True — "
                f"flow is DONE, ALL outbound permanently blocked "
                f"({'real inbound' if _ri else 'non-real webhook'})"
            )
            log.info(f"[{contact_id}] PRIORITY GUARD 0: final_confirmation_sent → no-op")
            return JSONResponse({
                "status"     : "success",
                "skipped"    : True,
                "reason"     : "priority_guard_0_final_confirmation_sent",
                "contact_id" : contact_id,
            })

        # ── GUARD 1: Bill already received — flow complete ────────────
        # Once the bill has been acked, no future inbound message triggers
        # ANY outbound response.  Absolute silent no-op.
        # Catches: duplicate GHL webhooks after bill photo, any stray message
        #          after the flow is complete.
        _bill_already_received = (
            _priority_state.get("bill_received") or
            (  # backward compat: older contacts before v2.9 flag was added
               _priority_state.get("bill_ack_sent") and
               _priority_state.get("bill_photo_received")
            )
        )
        if _bill_already_received:
            _ri = parsed.get("has_real_message") or parsed.get("has_attachment")
            print(
                f"\n[GUARD] 🔒 PRIORITY GUARD 1: bill_received=True — "
                f"flow is complete, ALL outbound blocked "
                f"({'real inbound' if _ri else 'non-real webhook'})"
            )
            log.info(f"[{contact_id}] PRIORITY GUARD 1: bill_received → no-op")
            return JSONResponse({
                "status"     : "success",
                "skipped"    : True,
                "reason"     : "priority_guard_1_bill_received",
                "contact_id" : contact_id,
            })

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
            _state["stage"] in (Stage.BOOKED,)
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
        _is_fresh_form_sub = (
            _is_lead_payload and       # positively identified as widget/form lead
            not _payload_says_booked   # no explicit booking proof in this payload
        )
        if _is_fresh_form_sub:
            _is_booked_contact    = False
            _fresh_form_override  = True
            _booked_override_reason = (
                f"[FIX-4.A] fresh_form_override — "
                f"is_lead_payload=True + payload_says_booked=False → "
                f"NEW_LEAD_PATH forced "
                f"(tag_says_booked={_tag_says_booked} suppressed, "
                f"state_says_booked={_state_says_booked} suppressed)"
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
        _is_inbound_sms_reply = (
            direction == "inbound" and
            bool(_has_real_msg) and
            _state_has_convo and
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
                if state.get("bill_ack_sent"):
                    log.info(f"[{contact_id}] Bill ack already sent — silent skip")
                    print(f"[BOOKED] ⛔  SKIP REASON: bill_ack_sent=True — already acked, nothing to send")
                    return JSONResponse({
                        "status"            : "success",
                        "entered_booked_path": True,
                        "phone_found"       : bool(state.get("phone") or phone),
                        "sms_sent"          : False,
                        "skip_reason"       : "bill_ack_already_sent",
                    })

                reply_text       = parsed["message"]
                _has_image       = parsed.get("has_attachment", False)
                _is_real_inbound = parsed.get("has_real_message", False)
                _msg_type_raw    = parsed.get("msg_type", "SMS")

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
                            contact_id, ack, to_number=_ack_phone
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
                        contact_id, _followup_reply, to_number=_fp_phone
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
            #   chat_widget    → warm continuation ("Perfect, you're all set!")
            #   direct_booking → brief intro ("Hey, you're booked!")
            #   unknown        → fall back to messages list as secondary signal
            _send_to_number = state.get("phone", "") or phone
            _entry_path     = state.get("entry_path", "unknown")
            _is_chat_widget = (
                _entry_path == "chat_widget" or
                # Fallback: if entry_path not set but messages exist, treat as chat widget
                (_entry_path == "unknown" and bool(state.get("messages")))
            )
            if _is_chat_widget:
                reminder = build_bill_reminder_message_sms_flow(first_name=first_name, full_name=full_name)
                print(f"[BOOKED]  Message variant : PATH-1 SMS-FLOW (entry_path={_entry_path!r} — warm continuation)")
            else:
                reminder = build_bill_reminder_message(first_name=first_name, full_name=full_name)
                print(f"[BOOKED]  Message variant : PATH-2 DIRECT BOOKING (entry_path={_entry_path!r} — brief intro)")

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
                    contact_id, reminder, to_number=_send_to_number
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

            # Enrich state before stage check
            if not state.get("contact_name") and full_name != "Unknown":
                state["contact_name"] = full_name
            if not state.get("phone") and phone:
                state["phone"] = phone
            if not state.get("address") and address:
                state["address"] = address      # [ADAPT-4] save address for system prompt
            save_state(contact_id, state)

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
                )
                print(f"[ROUTING] First-outreach: {outreach!r}")

                # Race-fix: advance stage + store history BEFORE the async send
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
                    await send_sms_via_ghl(contact_id, outreach, to_number=phone)
                except Exception as sms_err:
                    log.error(f"[{contact_id}] First-outreach SMS failed: {sms_err}")
                    print(f"[ERROR] First-outreach SMS failed:\n{traceback.format_exc()}")
                    return JSONResponse({"status": "success", "note": "first-outreach SMS failed"})

                print(f"[SMS] ✅ First-outreach sent to {full_name!r}")
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
                if _is_cw_2:
                    reminder = build_bill_reminder_message_sms_flow(first_name=first_name, full_name=full_name)
                    print(f"[BOOKED]  Message variant : PATH-1 SMS-FLOW (entry_path={_ep2!r})")
                else:
                    reminder = build_bill_reminder_message(first_name=first_name, full_name=full_name)
                    print(f"[BOOKED]  Message variant : PATH-2 DIRECT BOOKING (entry_path={_ep2!r})")
                try:
                    await send_sms_via_ghl(contact_id, reminder, to_number=state.get("phone", "") or phone)
                except Exception as sms_err:
                    log.error(f"[{contact_id}] Bill reminder (SEND_BOOKING) failed: {sms_err}")
                    print(f"[ERROR] Bill reminder SMS failed:\n{traceback.format_exc()}")
                    return JSONResponse({"status": "success", "note": "bill reminder SMS failed"})

                print(f"[SMS] ✅ Bill reminder sent (SEND_BOOKING→BOOKED) to {full_name!r}")
                return JSONResponse({"status": "success", "replied": True, "reply": reminder, "payload_type": "form_submission_booking_confirmed"})

            else:
                # Contact already active at some other stage — silent skip
                print(f"[ROUTING] Form submission for contact at stage={state['stage']} — skipping (already active)")
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

        # ── [FIX-9] Inbound dedup — two-tier, short window ───────────────
        # Uses is_duplicate_inbound() which returns (bool, reason_str).
        # TIER 1: GHL message ID (exact event identity — no false positives).
        # TIER 2: 8-second content window (catches GHL's rapid re-delivery).
        # Old 1-minute window caused valid replies like "Yes" to be suppressed
        # when GHL fires two different webhook events for the same SMS (common
        # with workflow automations) — second one had a different event_id so
        # tier-1 passed it, but tier-2 content-deduped it inside the minute.
        _ghl_msg_id = parsed.get("event_id", "")   # messageId/webhookId from GHL
        _is_dup, _dup_reason = is_duplicate_inbound(contact_id, inbound_text, ghl_message_id=_ghl_msg_id)
        print(
            f"[DEDUP] {'SKIPPED' if _is_dup else 'ALLOWED'} | {_dup_reason} | contact={contact_id}",
            flush=True,
        )
        if _is_dup:
            log.info(f"[{contact_id}] Duplicate inbound — skipping | {_dup_reason}")
            return JSONResponse({
                "status"         : "success",
                "skipped"        : True,
                "reason"         : "duplicate",
                "skip_reason"    : "duplicate_inbound",
                "dedup_reason"   : _dup_reason,
                "sms_sent"       : False,
                "send_sms_called": False,
            })

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
                    await send_sms_via_ghl(contact_id, _mms_ack, to_number=state.get("phone", "") or phone)
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
                await send_sms_via_ghl(contact_id, outreach, to_number=phone)
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

        # ── Run the agent ─────────────────────────────────────────
        print(f"[REPLY] ▶ Calling michael_agent() | stage={_pre_stage} | msg={inbound_text!r}", flush=True)
        reply = michael_agent(contact_id, inbound_text)

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
        ghl_tags      = resolve_ghl_tags(state["stage"])
        print(f"[REPLY]  Phone             : {contact_phone or '(not available)'}", flush=True)
        print(f"[REPLY]  GHL tags to apply : {ghl_tags}", flush=True)
        print(f"[REPLY]  ▶ Calling send_sms_via_ghl() now ...", flush=True)

        # ── Send SMS ──────────────────────────────────────────────
        try:
            await send_sms_via_ghl(contact_id, reply, to_number=contact_phone)
        except Exception as sms_err:
            log.error(f"[{contact_id}] GHL SMS send failed: {sms_err}")
            print(f"[REPLY] !! SMS send FAILED — lead reply not delivered", flush=True)
            print(f"[ERROR] SMS send failed:\n{traceback.format_exc()}")
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
                print(f"[ROUTING] Updating GHL tags: {ghl_tags}")
                await update_ghl_contact(contact_id, ghl_tags)
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
                    await send_sms_via_ghl(contact_id, _fb_outreach, to_number=_fb_phone)
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


# ─────────────────────────────────────────────
#  FOLLOW-UP TRIGGER ENDPOINT
# ─────────────────────────────────────────────

@app.post("/webhook/booking-followup")
async def booking_followup(request: Request):
    """
    Appointment-booked confirmation trigger called by GHL when a contact books.

    This endpoint ALWAYS sends the appointment confirmation message.
    Priority rules:
      • final_confirmation_sent — BYPASSED (set when bill photo received, not relevant here)
      • booking_follow_up_sent  — BYPASSED (high-priority override)
      • daily_limit             — BYPASSED
      • stage check             — BYPASSED

    The ONE dedup guard kept: if bill_reminder_sent=True AND bill_reminder_sent
    was just set by /webhook/inbound processing the same booking event moments
    ago, we skip to avoid an exact-duplicate send.  This is the only suppress
    condition, and it must be logged clearly.

    Message variant (mirrors the inbound booked path):
      • entry_path == "chat_widget"  → build_bill_reminder_message_sms_flow()
        (warm continuation — they already know Michael from the SMS conversation)
      • entry_path == "direct_booking" or unset → build_bill_reminder_message()
        (brief intro — first text they've received from Michael)
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

        # ── Pick message variant based on entry_path ──────────────
        _entry_path     = state.get("entry_path", "unknown")
        _is_chat_widget = (
            _entry_path == "chat_widget" or
            (_entry_path == "unknown" and bool(state.get("messages")))
        )
        if _is_chat_widget:
            appt_confirmation = build_bill_reminder_message_sms_flow(
                first_name=first_name, full_name=full_name
            )
            _variant = "sms_flow (chat_widget — warm continuation)"
        else:
            appt_confirmation = build_bill_reminder_message(
                first_name=first_name, full_name=full_name
            )
            _variant = "direct_booking (first contact — brief intro)"

        # ── Diagnostic banner ─────────────────────────────────────
        print(f"\n[BOOKING-FOLLOWUP] {'─'*50}")
        print(f"[BOOKING-FOLLOWUP]  contact_id             : {contact_id}")
        print(f"[BOOKING-FOLLOWUP]  first_name             : {first_name or '(not set)'!r}")
        print(f"[BOOKING-FOLLOWUP]  full_name              : {full_name!r}")
        print(f"[BOOKING-FOLLOWUP]  phone                  : {phone or '(not set)'!r}")
        print(f"[BOOKING-FOLLOWUP]  stage (state)          : {state['stage']}")
        print(f"[BOOKING-FOLLOWUP]  entry_path             : {_entry_path!r}")
        print(f"[BOOKING-FOLLOWUP]  message_variant        : {_variant}")
        print(f"[BOOKING-FOLLOWUP]  bill_reminder_sent     : {state.get('bill_reminder_sent', False)}")
        print(f"[BOOKING-FOLLOWUP]  final_confirmation_sent: {state.get('final_confirmation_sent', False)}  ← BYPASSED")
        print(f"[BOOKING-FOLLOWUP]  message                : {appt_confirmation!r}")
        print(f"[BOOKING-FOLLOWUP] {'─'*50}")

        # ── Only dedup: skip if inbound already sent bill reminder
        # for this same booking event (race condition guard).
        if state.get("bill_reminder_sent"):
            print(
                f"[BOOKING-FOLLOWUP]  ⏭ SKIPPED — bill_reminder_sent=True, "
                f"inbound already sent appointment confirmation for this booking"
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
            _result = await send_sms_via_ghl(contact_id, appt_confirmation, to_number=phone)
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

        # Stamp bill_reminder_sent so /webhook/inbound booked path
        # doesn't double-send if it fires immediately after.
        state["bill_reminder_sent"]  = True
        state["bill_requested"]      = True
        state["appointment_booked"]  = True
        increment_message_count(state)
        save_state(contact_id, state)
        log.info(f"[{contact_id}] Booking-followup appt confirmation sent | variant={_variant!r}")
        return JSONResponse({
            "status"  : "success",
            "sent"    : _sent,
            "deduped" : _deduped,
            "variant" : _variant,
        })

    except Exception as err:
        log.error(f"booking-followup fatal error: {err}")
        print(f"[ERROR] booking-followup FATAL:\n{traceback.format_exc()}")
        return JSONResponse({"status": "success", "error_logged": True})


# ─────────────────────────────────────────────
#  DEBUG ENDPOINTS
# ─────────────────────────────────────────────

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
async def debug_reset_state(contact_id: str):
    """
    Delete all state for a contact (reset to INITIAL).
    POST /debug/reset/{contact_id}
    """
    existed = contact_id in _state_store
    if existed:
        del _state_store[contact_id]
    log.info(f"[DEBUG] State reset for {contact_id} (existed={existed})")
    return JSONResponse({"reset": True, "contact_id": contact_id, "existed": existed})


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
        await send_sms_via_ghl(contact_id, message, to_number=to_number)
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

@app.get("/health")
async def health():
    _ak = os.getenv("ANTHROPIC_API_KEY", "")
    return {
        "status"              : "Michael is online",
        "version"             : "3.9-debug",
        "model"               : MODEL,
        "anthropic_key_set"   : bool(_ak),
        "anthropic_key_prefix": (_ak[:8] + "...") if len(_ak) >= 8 else ("SET-BUT-SHORT" if _ak else "NOT SET"),
        "ghl_key_set"         : bool(os.getenv("GHL_API_KEY")),
        "ghl_location_set"    : bool(os.getenv("GHL_LOCATION_ID")),
        "from_number"         : GHL_FROM_NUMBER,
        "max_daily_msgs"      : MAX_DAILY_MSGS,
        "active_contacts"     : len(_state_store),
        "dedup_cache_size"    : len(_processed_fingerprints),
        "outbound_dedup"      : len(_outbound_fingerprints),
        "booked_tag"          : BOOKED_TAG,
        "booking_link"        : BOOKING_LINK,
    }


# ── Website chat system prompt ────────────────────────────────────────────────
# Powers the live phone-UI chat on the KC Energy Advisors website.
#
# Design philosophy: every reply follows Answer → Context → Question.
# Qualification happens naturally across the conversation, not as a form.
# Booking is offered once, after trust is built, without pressure.
#
# Sections:
#   WHO YOU ARE     — identity anchor so Claude never breaks character
#   HOW YOU TALK    — tone and format rules
#   REPLY STRUCTURE — the core Answer→Context→Question pattern
#   SOLAR FACTS     — KC/Evergy-specific numbers to draw from
#   TAX CREDIT      — explicit rule to never claim a dead incentive
#   QUALIFICATION   — organic 3-step funnel (own → location → bill)
#   BOOKING         — when and how to offer the call
#   EDGE CASES      — browsing, not interested, savings questions
#   HARD RULES      — absolute prohibitions, checked last
_WEBSITE_CHAT_SYSTEM = """\
WHO YOU ARE
You are Michael, a solar advisor at KC Energy Advisors in Kansas City.
You talk to homeowners every day. You know Evergy's billing rules, local rates,
and what solar genuinely saves people in this market — not national averages.
You are not a chatbot. You are a person who knows solar and actually cares
whether it makes sense for the person in front of you.

HOW YOU TALK
Calm. Direct. Slightly casual — like a knowledgeable neighbor, not a salesperson.
You give real answers. You don't pad. You don't hype.
When something won't work for someone, you say so.

Tone you want: "Evergy raised rates again last year, so locking in your cost now
actually makes a lot of sense. What's your bill running these days?"

Tone to avoid: "Great question! As a solar advisor I can share that costs vary..."

FORMAT — NON-NEGOTIABLE
- 2 sentences is ideal. 3 is the maximum. Never more.
- Never use bullet points, dashes, or numbered lists.
- Never open with filler: "Great!", "Sure!", "Of course!", "Absolutely!", "Happy to help!"
- Never restate what the user said. Jump straight to the answer.
- End almost every reply with exactly one short follow-up question.
- If your draft runs past 4 lines, cut it in half before sending.

REPLY STRUCTURE — FOLLOW THIS ORDER EVERY TIME
1. ANSWER — respond directly to what they asked. Be specific. Use a real number.
2. CONTEXT — one grounding insight: local Evergy reality, a caveat, or how it applies to them.
3. QUESTION — one short natural question that moves the conversation forward.

Skipping step 1 and leading with a question is a failure.
Answering without a follow-up question stalls the conversation.

Examples of structure done right:

User: "how expensive is solar?"
Good: "For a KC home, installed cost typically runs $18,000–$26,000 depending on
system size. With $0 down financing, the monthly payment usually comes in around
$130–$160 — which for most people is less than their current Evergy bill.
What's your average monthly bill right now?"

User: "is solar worth it?"
Good: "For most KC homeowners paying $150+ to Evergy, yes — especially now that
Evergy raised rates 14% last year and has filed for another increase.
Solar locks in your cost for 25 years regardless of what they do.
Do you own your home?"

User: "what if it's cloudy?"
Good: "Your system stays connected to the grid, so cloudy days just mean you draw
a little more from Evergy — you're never without power. The credits from sunny
days offset that, so your net bill is still dramatically lower.
What part of the KC area are you in?"

SOLAR FACTS — USE THESE, DON'T INVENT NUMBERS
- Installed system cost KC: $18,000–$26,000
- $0 down financing; monthly payment typically $130–$160
- Bill reduction: 60–80% for most KC homeowners, starting month one
- Solar payment is fixed. Evergy's rate is not.
- Evergy Missouri raised rates ~14% in 2025. Kansas ~9.6% same year.
  Evergy has filed for another Missouri increase (pending 2026).
- Net metering: Evergy credits you for power your panels send to the grid.
  Missouri side: credited at full retail rate.
  Kansas side (Overland Park, Lenexa, etc.): retail credit within the billing period;
  annual surplus paid at wholesale — so we right-size systems in Kansas.
- Break-even on owned system: 8–12 years. Panels warrantied 25 years.
- Qualification threshold: $75+/month Evergy bill. Under $75, rarely pencils out.
- Process: 1 install day. Permits + Evergy interconnection: 3–4 weeks total.
- Service area: Kansas City metro, both Missouri and Kansas sides.
- Direct text line: (816) 319-0932

FEDERAL TAX CREDIT — CRITICAL
The 30% federal residential solar credit (IRS 25D) expired December 31, 2025.
NEVER mention it as an available benefit. NEVER tell someone they can claim it.
If they ask: "That credit actually expired at the end of last year — a lot of
people haven't heard yet. The monthly savings math still works though, especially
with how aggressively Evergy has been raising rates."

QUALIFICATION — ORGANIC, NEVER LIKE A FORM
Learn these three things through natural conversation — one at a time:
  1. Do they own the home? (Renters can't install solar.)
  2. Are they in the KC metro area? (Establishes Evergy territory + Missouri vs Kansas.)
  3. What's their average monthly Evergy bill? (The key qualification signal.)

Rules:
- Only ask what you don't already know from earlier in the conversation.
- Never ask two qualification questions in the same reply.
- Weave questions in after answering — never lead with them.
- A bill over $75/month = qualified. Under $75 = probably not worth it (say so honestly).
- Once you have all three and they qualify, move to booking.

BOOKING — EARNED, OFFERED ONCE, NEVER FORCED
Only offer a call when you know: they own the home + KC area + bill over $75.
Use natural language. Offer it once. If they decline, respect it and stay helpful.

Booking language (use your own words, this is a guide not a script):
"Based on what you're saying, it actually sounds like this could make sense for
your home. What we usually do is a quick 10–15 minute call where one of our
advisors runs the real numbers — your address, your bill, current system costs.
No pitch, no pressure. Want me to send you a link to grab a time?"

After offering, if they say yes → "Text me at (816) 319-0932 or head to
kcenergyadvisors.com and I'll make sure someone gets you those numbers."
If they say no or not yet → move on, stay friendly, keep answering questions.

EDGE CASES — HANDLE THESE WELL

"Just browsing" or "just curious":
Stay relaxed. Don't push. "No worries — happy to answer whatever's on your mind.
Anything specific you've been wondering about solar?"

"Not interested" or "I don't think solar is for me":
Respect it completely. "Totally fair — it's not right for every home.
If anything ever changes or you want numbers down the road, I'm here."
Do not re-pitch. Do not ask why. Let them go cleanly.

"How much would I save?" without knowing their bill:
Give a realistic range, then pivot. "Depends mostly on your bill —
a $150/month Evergy customer typically saves $90–$120/month with solar.
What does your bill usually run?"

"Is the tax credit still available?":
Be honest. See FEDERAL TAX CREDIT section above.

"I rent" or "I don't own the home":
Be direct and kind. "Solar's really only an option for homeowners —
the system needs to be installed on a home you own. If that ever changes, reach out."

Long rambling or off-topic message:
Pick the most relevant thing they said and respond to that. Keep it focused.

HARD RULES — THESE OVERRIDE EVERYTHING ABOVE
- NEVER claim to be an AI, bot, language model, or non-human of any kind.
- NEVER mention ChatGPT, Claude, Anthropic, or any AI system.
- NEVER echo, restate, or paraphrase what the user said. Answer it.
- NEVER open with hollow affirmations. Start with substance.
- NEVER give vague non-answers. Use a real number or say it's an estimate.
- NEVER write more than 4 lines. If longer, cut before sending.
- NEVER offer the booking call more than once per conversation.
- NEVER claim the 30% federal tax credit is available. It is not.
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
        model="claude-3-7-sonnet-latest",
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
    # ── Model used for website chat ───────────────────────────────────────────
    model="claude-3-7-sonnet-latest"

    message = (payload.get("message") or "").strip()
    name    = (payload.get("name")    or "Website Visitor").strip()
    source  = (payload.get("source")  or "website_chat").strip()
    history = payload.get("history")  or []   # list of {role, content} from frontend

    print(f"[WEBSITE CHAT] ▶ source={source!r} name={name!r} message={message!r} history_len={len(history)}", flush=True)

    if not message:
        return {"reply": "Hey! What questions do you have about going solar in KC?", "mode": "ai"}

    # ── Pre-flight: confirm API key is present before making the call ─────────
    api_key = os.getenv("ANTHROPIC_API_KEY")
    print("API KEY FOUND:", bool(api_key), flush=True)
    print("API KEY PREFIX:", api_key[:12] if api_key else "NONE", flush=True)

    if not api_key:
        error_msg = "ANTHROPIC_API_KEY is not set in environment variables"
        print("CLAUDE ERROR:", error_msg, flush=True)
        return {"reply": "TECH ERROR", "mode": "error", "error": error_msg}

    # ── Build messages list (history + current) ───────────────────────────────
    # Cap history to last 10 turns to stay well within token limits.
    # Filter aggressively: only user/assistant roles, non-empty content.
    clean_history: list[dict] = []
    for h in history[-10:]:
        role    = h.get("role", "")    if isinstance(h, dict) else ""
        content = h.get("content", "") if isinstance(h, dict) else ""
        if role in ("user", "assistant") and content.strip():
            clean_history.append({"role": role, "content": content.strip()})

    messages: list[dict] = clean_history + [{"role": "user", "content": message}]

    # Anthropic requires messages[0].role == "user"
    if messages[0]["role"] != "user":
        messages = [{"role": "user", "content": "[New website visitor]"}] + messages

    print(f"[WEBSITE CHAT]   model={CHAT_MODEL!r} messages_in_context={len(messages)}", flush=True)

    try:
        # Use a fresh client here so the API key is pulled at call-time,
        # not just at module load (guards against env vars set after startup).
        _client   = Anthropic(api_key=api_key)
        response  = _client.messages.create(
            model      = "claude-3-7-sonnet-latest",
            max_tokens = 300,
            system     = _WEBSITE_CHAT_SYSTEM,
            messages   = messages,
        )
        reply = response.content[0].text.strip()
        print(f"[WEBSITE CHAT] ✅ AI reply (mode=real): {reply[:120]!r}", flush=True)
        return {"reply": reply, "mode": "ai"}

    except Exception as e:
        error_str = f"{type(e).__name__}: {e}"
        print("CLAUDE ERROR:", error_str, flush=True)
        return {"reply": "TECH ERROR", "mode": "error", "error": error_str}
