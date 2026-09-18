#!/usr/bin/env python3
"""
After-hours + SMS-compliance suite, reconciled to main @ 1a788dd.
Rewritten for main's SUPPRESSED status-dict convention (no exceptions).
stdlib only - no network, no Anthropic key.
"""
import asyncio, datetime as dt, inspect, json, os, sys
from zoneinfo import ZoneInfo
try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception: pass
for k, v in [("GHL_API_KEY","test"),("GHL_LOCATION_ID","test"),
             ("ANTHROPIC_API_KEY","test"),("GHL_FROM_NUMBER","+15555550100"),
             ("BOOKING_LINK","https://booking.test/x")]:
    os.environ.setdefault(k, v)

import michael_agent as M

CT = ZoneInfo("America/Chicago")
P = F = 0
FAILS = []
def chk(n, got, want, g=""):
    global P, F
    ok = got == want
    if ok: P += 1
    else:
        F += 1; FAILS.append(f"{g} :: {n}  got={got!r} want={want!r}")
    print(f"  {'PASS' if ok else 'FAIL'}  {n}" + ("" if ok else f"   got={got!r} want={want!r}"))
def sec(t): print(f"\n{'='*72}\n  {t}\n{'='*72}")
run = asyncio.run
def ct(y,mo,d,h,mi): return dt.datetime(y,mo,d,h,mi,tzinfo=CT)

# ══════════════════════════════════════════════════════════
sec("A1 - SEND WINDOW BOUNDARIES (America/Chicago)")
for label, when, want in [
    ("00:00 midnight", ct(2026,6,15,0,0),  False),
    ("08:58",          ct(2026,6,15,8,58), False),
    ("08:59",          ct(2026,6,15,8,59), False),
    ("09:00 OPEN",     ct(2026,6,15,9,0),  True),
    ("09:01",          ct(2026,6,15,9,1),  True),
    ("12:00 noon",     ct(2026,6,15,12,0), True),
    ("20:59 (8:59PM)", ct(2026,6,15,20,59),True),
    ("21:00 (9:00PM)", ct(2026,6,15,21,0), False),
    ("21:01 (9:01PM)", ct(2026,6,15,21,1), False),
    ("23:59",          ct(2026,6,15,23,59),False),
]:
    chk(f"{label} -> {'OPEN' if want else 'CLOSED'}", M.within_send_window(when), want, "A1")

sec("A2 - DST (no fixed UTC offset)")
cst, cdt = ct(2026,1,15,9,0), ct(2026,7,15,9,0)
chk("Jan 09:00 CST OPEN", M.within_send_window(cst), True, "A2")
chk("Jul 09:00 CDT OPEN", M.within_send_window(cdt), True, "A2")
chk("offsets differ", cst.strftime("%z") != cdt.strftime("%z"), True, "A2")
U = dt.timezone.utc
chk("UTC 14:00 Jan -> CLOSED (08:00 CST)",
    M.within_send_window(dt.datetime(2026,1,15,14,0,tzinfo=U)), False, "A2")
chk("UTC 14:00 Jul -> OPEN (09:00 CDT)",
    M.within_send_window(dt.datetime(2026,7,15,14,0,tzinfo=U)), True, "A2")
chk("no hardcoded offset",
    any(x in inspect.getsource(M.within_send_window) for x in ("-0600","-0500")), False, "A2")

# ══════════════════════════════════════════════════════════
sec("A3 - GATES USE main's SUPPRESSED CONVENTION (no exceptions)")
src = open("michael_agent.py", encoding="utf-8", newline="").read()
g0 = src.find("async def send_sms_via_ghl"); g1 = src.find("async def update_ghl_contact")
body = src[g0:g1]
chk("no SmsBlockedError class", "class SmsBlockedError" in src, False, "A3")
chk("no SmsOutsideWindowError class", "class SmsOutsideWindowError" in src, False, "A3")
chk("no SmsDeliveryError class", "class SmsDeliveryError" in src, False, "A3")
chk("gates return SUPPRESSED", body.count("SendStatus.SUPPRESSED.value") >= 4, True, "A3")
chk("no raise in gate paths", "raise SmsBlocked" in body or "raise SmsOutside" in body, False, "A3")
chk("booked guard still first",
    body.find("_BOOKED_GUARDED") < body.find("can_contact_sms"), True, "A3")
chk("compliance gate BEFORE window gate",
    body.find("can_contact_sms") < body.find("within_send_window()"), True, "A3")
chk("window gate BEFORE dedup",
    body.find("within_send_window()") < body.find("is_duplicate_outbound"), True, "A3")
chk("main's messageId capture preserved", "_msg_id" in body, True, "A3")
chk("main's raise_for_status preserved", "r.raise_for_status()" in body, True, "A3")

# ══════════════════════════════════════════════════════════
sec("A4 - NO PARALLEL IMPLEMENTATIONS (redundancy removed)")
chk("uses add_ghl_tags for holds",
    "add_ghl_tags" in inspect.getsource(M.place_after_hours_hold), True, "A4")
chk("no private _add_ghl_tag", hasattr(M, "_add_ghl_tag"), False, "A4")
chk("no private _try_place_hold", hasattr(M, "_try_place_hold"), False, "A4")
chk("no private _hold_tag_present", hasattr(M, "_hold_tag_present"), False, "A4")
chk("resume uses has_tag", "has_tag(" in inspect.getsource(M.after_hours_resume), True, "A4")
chk("permanence delegated to classify_carrier_error",
    "classify_carrier_error" in inspect.getsource(M.classify_sms_failure), True, "A4")
chk("no duplicate permanence table", hasattr(M, "SMS_FAILURE_CODES"), False, "A4")
chk("main's classify_carrier_error intact", M.classify_carrier_error("30006"), "permanent", "A4")
chk("main's _record_send_result intact", callable(M._record_send_result), True, "A4")
chk("main's add_ghl_tags intact", inspect.iscoroutinefunction(M.add_ghl_tags), True, "A4")
chk("main's message_status_webhook intact",
    inspect.iscoroutinefunction(M.message_status_webhook), True, "A4")
chk("no last_sms_message_id duplicate field",
    "last_sms_message_id" in src, False, "A4")

# ══════════════════════════════════════════════════════════
sec("A5 - FAILURE CLASSIFICATION (30005/30006 are NOT opt-outs)")
for code in ("30005","30006","30003","21610","21614"):
    cat, c, perm = M.classify_sms_failure(
        dnd_sms={"status":"active","message":f"TWILIO_ERROR_CODE: {code}"})
    chk(f"{code} NOT EXPLICIT_OPT_OUT", cat == "EXPLICIT_OPT_OUT", False, "A5")
    chk(f"{code} permanent (via classify_carrier_error)", perm, True, "A5")
    chk(f"{code} code extracted", c, code, "A5")
chk("30006 category", M.classify_sms_failure(
    dnd_sms={"status":"active","message":"TWILIO_ERROR_CODE: 30006"})[0],
    "UNREACHABLE_OR_BAD_NUMBER", "A5")
chk("STOP_KEYWORD -> EXPLICIT_OPT_OUT",
    M.classify_sms_failure(dnd_sms={"status":"permanent","message":"STOP_KEYWORD"})[0],
    "EXPLICIT_OPT_OUT", "A5")
chk("no signal -> UNKNOWN (fails closed)", M.classify_sms_failure(dnd_sms={}),
    ("UNKNOWN","",False), "A5")
chk("HTTP 400 -> CONFIG_ERROR",
    M.classify_sms_failure(dnd_sms={}, http_status=400), ("CONFIG_ERROR","400",False), "A5")

# ══════════════════════════════════════════════════════════
sec("A6 - COMPLIANCE GATE (fail-closed)")
class Resp:
    def __init__(s,c,p=None,t=""): s.status_code=c; s._p=p; s.text=t or json.dumps(p)
    @property
    def is_success(s): return 200 <= s.status_code < 300
    def json(s):
        if s._p is None: raise ValueError("not json")
        return s._p

def stub_compliance(result):
    async def _f(cid): return result
    M.fetch_ghl_contact_compliance = _f
_real_compliance = M.fetch_ghl_contact_compliance

OK_C = {"ok":True,"first_name":"Sandra","full_name":"Sandra J","phone":"+15551112222",
        "email":"a@b.com","dnd":False,"dnd_sms":{},"tags":[],"date_added":"","http":200}

stub_compliance(OK_C)
chk("clean contact allowed", run(M.can_contact_sms("c1", {}))["allowed"], True, "A6")

stub_compliance({**OK_C,"dnd_sms":{"status":"permanent","message":"STOP_KEYWORD"}})
g = run(M.can_contact_sms("c2", {}))
chk("permanent opt-out DENIED", g["allowed"], False, "A6")
chk("category EXPLICIT_OPT_OUT", g["category"], "EXPLICIT_OPT_OUT", "A6")

stub_compliance({**OK_C,"dnd_sms":{"status":"active","message":"TWILIO_ERROR_CODE: 30006"}})
g = run(M.can_contact_sms("c3", {}))
chk("30006 DENIED", g["allowed"], False, "A6")
chk("30006 permanent", g["permanent"], True, "A6")

stub_compliance({**OK_C,"dnd":True})
chk("account DND DENIED", run(M.can_contact_sms("c4", {}))["allowed"], False, "A6")

stub_compliance({"ok":False,"first_name":"","full_name":"","phone":"","email":"",
                 "dnd":None,"dnd_sms":{},"tags":[],"date_added":"","http":500})
g = run(M.can_contact_sms("c5", {}))
chk("lookup failure FAILS CLOSED", g["allowed"], False, "A6")
chk("failure category UNKNOWN", g["category"], "UNKNOWN", "A6")

stub_compliance(OK_C)
chk("local opt-out ledger DENIES",
    run(M.can_contact_sms("c6", {"sms_opt_out": True}))["allowed"], False, "A6")
chk("local ineligible DENIES (no retry)",
    run(M.can_contact_sms("c7", {"sms_ineligible": True}))["allowed"], False, "A6")
chk("no override parameter",
    any(p in inspect.signature(M.can_contact_sms).parameters
        for p in ("force","override","bypass","ignore_dnd")), False, "A6")

# ══════════════════════════════════════════════════════════
sec("A7 - END-TO-END SEND GATING")
CALLS = []
def wire(contact, post_ok=True):
    class C:
        def __init__(s,*a,**k): pass
        async def __aenter__(s): return s
        async def __aexit__(s,*a): return False
        async def get(s,url,**k):
            CALLS.append(("GET",url))
            if "/appointments" in url: return Resp(200, {"events": []})
            if "/opportunities" in url: return Resp(200, {"opportunities": []})
            if "/contacts/" in url: return Resp(200, {"contact": contact})
            return Resp(200, {})
        async def post(s,url,**k):
            CALLS.append(("POST",url))
            return Resp(201 if post_ok else 400, {"messageId":"MSG-1"})
        async def put(s,url,**k):
            CALLS.append(("PUT", url, k.get("json", {}).get("tags", [])))
            return Resp(200, {})
    M.httpx.AsyncClient = C
M.fetch_ghl_contact_compliance = _real_compliance

CLEAN = {"phone":"+15551112222","email":"a@b.com","dnd":False,
         "dndSettings":{"SMS":{"status":"inactive"}},"tags":[],
         "firstName":"Sandra","lastName":"J","dateAdded":""}
_real_window = M.within_send_window
def force(open_): M.within_send_window = lambda now=None: open_

# 7a. outside window -> SUPPRESSED + hold, NO message POST
force(False); CALLS.clear(); M._state_store.pop("a7a", None); wire(CLEAN)
r = run(M.send_sms_via_ghl("a7a", "after hours", to_number="+15551112222",
                           kind=M.SendKind.QUALIFICATION))
chk("outside window -> SUPPRESSED", r["status"], M.SendStatus.SUPPRESSED.value, "A7")
chk("reason outside_send_window", r["reason"], M.SUPPRESS_REASON_WINDOW, "A7")
chk("sent False", r["sent"], False, "A7")
chk("NOT in _STATUS_ADVANCES_STATE", r["status"] in M._STATUS_ADVANCES_STATE, False, "A7")
chk("NO message POST", any("/conversations/messages" in c[1] for c in CALLS), False, "A7")
chk("hold tag written",
    any(M.TAG_AFTER_HOURS_HOLD in (c[2] if len(c)>2 else []) for c in CALLS), True, "A7")

# 7b. inside window, clean -> sends
force(True); CALLS.clear(); M._state_store.pop("a7b", None); wire(CLEAN)
r = run(M.send_sms_via_ghl("a7b", "in hours", to_number="+15551112222",
                           kind=M.SendKind.QUALIFICATION))
chk("inside window accepted", r["status"], M.SendStatus.ACCEPTED.value, "A7")
chk("advances state", r["status"] in M._STATUS_ADVANCES_STATE, True, "A7")
chk("messageId captured", r["message_id"], "MSG-1", "A7")
chk("no hold placed",
    any(M.TAG_AFTER_HOURS_HOLD in (c[2] if len(c)>2 else []) for c in CALLS), False, "A7")

# 7c. opt-out at 2AM -> compliance wins, NO hold tag
force(False); CALLS.clear(); M._state_store.pop("a7c", None)
wire({**CLEAN,"dndSettings":{"SMS":{"status":"permanent","message":"STOP_KEYWORD"}}})
r = run(M.send_sms_via_ghl("a7c", "msg", to_number="+1", kind=M.SendKind.QUALIFICATION))
chk("opt-out -> SUPPRESSED", r["status"], M.SendStatus.SUPPRESSED.value, "A7")
chk("reason is compliance not window", r["reason"], M.SUPPRESS_REASON_DND, "A7")
chk("opted-out contact gets NO hold tag",
    any(M.TAG_AFTER_HOURS_HOLD in (c[2] if len(c)>2 else []) for c in CALLS), False, "A7")
chk("opted-out contact DOES get durable DNC tag (R1)",
    any(M.TAG_DNC in (c[2] if len(c)>2 else []) for c in CALLS), True, "A7")
chk("NO message POST", any("/conversations/messages" in c[1] for c in CALLS), False, "A7")
st = M.get_state("a7c")
chk("sms_opt_out set", st["sms_opt_out"], True, "A7")
chk("stage DNC (short-circuits Claude)", st["stage"], M.Stage.DNC, "A7")
chk("NOT queued for fallback", st["fallback_pending"], False, "A7")

# 7d. 30006 at 2AM -> not an opt-out, preserved for fallback
force(False); CALLS.clear(); M._state_store.pop("a7d", None)
wire({**CLEAN,"dndSettings":{"SMS":{"status":"active","message":"TWILIO_ERROR_CODE: 30006"}}})
r = run(M.send_sms_via_ghl("a7d", "msg", to_number="+1", kind=M.SendKind.QUALIFICATION))
st = M.get_state("a7d")
chk("30006 sms_opt_out stays False", st["sms_opt_out"], False, "A7")
chk("30006 stage NOT DNC", st["stage"] == M.Stage.DNC, False, "A7")
chk("30006 ineligible (no retry)", st["sms_ineligible"], True, "A7")
chk("30006 fallback_pending", st["fallback_pending"], True, "A7")
chk("30006 fallback_email kept", st["fallback_email"], "a@b.com", "A7")
M.within_send_window = _real_window

# ══════════════════════════════════════════════════════════
sec("A8 - STOP FALSE POSITIVES (must remain conversation)")
for p in ["I want to cancel my Ameren service","Can you stop by Saturday?",
          "I need to cancel Tuesday, can we do Thursday?",
          "What time does the appointment end?","Stop by anytime after 5",
          "Let us do end of next week","My contract ends in June",
          "Can I cancel if I change my mind later?","Is there a cancellation fee?",
          "We just moved in at the end of August",
          "I want to end my relationship with the utility company",
          "Do not stop at the driveway, come to the side door",
          "end of the month works better for me",
          "Can we cancel and reschedule for next week?",
          "Stop me if I'm wrong but isn't Ameren raising rates?"]:
    chk(f"NOT opt-out: {p[:42]!r}", M.is_stop_request(p), False, "A8")
for p in ["STOP","stop","Stop.","STOP!","  stop  ","Stop please","STOPALL",
          "UNSUBSCRIBE","quit","CANCEL","END","optout","opt out","revoke",
          "stop contacting me","take me off your list","Lose my number",
          "do not text me anymore"]:
    chk(f"IS opt-out: {p!r}", M.is_stop_request(p), True, "A8")

# ══════════════════════════════════════════════════════════
sec("A9 - GSM-7")
GSM = set("@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
          "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà") | set("^{}\\[~]|€")
cur = ("Hey Sandra, this is Michael with STL Energy Advisors. Got your info — "
       "your home may be worth taking a look at.")
out = M.to_gsm7_safe(cur, "a9")
chk("em dash removed", "—" in out, False, "A9")
chk("fully GSM-7", all(c in GSM for c in out), True, "A9")
chk("wording preserved", abs(len(out)-len(cur)) <= 2, True, "A9")
chk("idempotent", M.to_gsm7_safe(out,"a9"), out, "A9")
chk("plain text untouched", M.to_gsm7_safe("Are you on Ameren?","a9"), "Are you on Ameren?", "A9")
u = "https://stlenergyadvisors.com/get-solar-info?source=sms"
chk("booking URL preserved", M.to_gsm7_safe(u,"a9"), u, "A9")

# ══════════════════════════════════════════════════════════
sec("A10 - RESUME ENDPOINT")
class Req:
    def __init__(s,p): s._b = json.dumps(p).encode()
    async def body(s): return s._b

HELD = {**CLEAN, "tags":["after-hours-hold"]}
INB  = [{"direction":"inbound","body":"do you serve Arnold?","id":"m1",
         "dateAdded": dt.datetime.now(U).isoformat()}]

def wire_resume(contact, msgs=None, stage_resp=None, fail=None, post_ok=True):
    msgs = msgs if msgs is not None else []
    class C:
        def __init__(s,*a,**k): pass
        async def __aenter__(s): return s
        async def __aexit__(s,*a): return False
        async def get(s,url,**k):
            CALLS.append(("GET",url))
            if fail=="compliance" and "/contacts/" in url: return Resp(500,{})
            if fail=="convo" and "conversation" in url: return Resp(500,{})
            if "/appointments" in url: return Resp(200,{"events":[]})
            if "/opportunities" in url: return Resp(200, stage_resp or {"opportunities":[]})
            if "/contacts/" in url: return Resp(200,{"contact":contact})
            if "/conversations/search" in url: return Resp(200,{"conversations":[{"id":"CV1"}]})
            if "/messages" in url: return Resp(200,{"messages":msgs})
            return Resp(200,{})
        async def post(s,url,**k):
            CALLS.append(("POST",url)); return Resp(201 if post_ok else 400,{"messageId":"MSG-R"})
        async def put(s,url,**k):
            CALLS.append(("PUT",url)); return Resp(200,{})
    M.httpx.AsyncClient = C

def resume(contact, msgs=None, stage=None, fail=None, cid="ares"):
    wire_resume(contact, msgs, stage, fail)
    r = run(M.after_hours_resume(Req({"contact_id": cid})))
    return json.loads(bytes(r.body).decode())

force(True)
M._state_store.clear()
out = resume({**HELD,"tags":[]}, INB)
chk("duplicate fire -> no-op", out["reason"], "hold_already_cleared", "A10")
chk("duplicate fire no send", out["sms_sent"], False, "A10")

M._state_store.clear()
out = resume({**HELD,"dndSettings":{"SMS":{"status":"active","message":"TWILIO_ERROR_CODE: 30006"}}}, INB)
chk("DND overnight skipped", out["reason"], "dnd_active", "A10")
chk("DND hold cleared", out["hold_cleared"], True, "A10")

M._state_store.clear()
out = resume(HELD, INB, fail="compliance")
chk("API failure -> hold RETAINED", out["hold_retained"], True, "A10")
chk("API failure no send", out["sms_sent"], False, "A10")

M._state_store.clear()
out = resume(HELD, [{"direction":"outbound","body":"already answered","id":"o1"}])
chk("already replied -> skip", out["reason"], "already_replied", "A10")
chk("already replied hold cleared", out["hold_cleared"], True, "A10")

M._state_store.clear()
_old = (dt.datetime.now(U) - dt.timedelta(hours=30)).isoformat()
out = resume(HELD, [dict(INB[0], dateAdded=_old)])
chk("expired hold skipped", out["reason"], "hold_expired", "A10")
chk("expired age reported", out["age_hours"] > 18, True, "A10")
chk("expired hold cleared", out["hold_cleared"], True, "A10")

force(False)
M._state_store.clear()
out = resume(HELD, INB)
chk("resume outside window skipped", out["reason"], "outside_send_window", "A10")
chk("resume outside window hold retained", out["hold_retained"], True, "A10")
force(True)

out = json.loads(bytes(run(M.after_hours_resume(Req({}))).body).decode())
chk("missing contact_id handled", out["reason"], "no_contact_id", "A10")

sec("A11 - RESUME: FRESH GENERATION + CONTEXT REBUILD")
_real_agent = M.michael_agent
def _stub_agent(cid, text, ghl_pipeline_stage="", ghl_tags=None):
    st = M.get_state(cid)
    st["messages"].append({"role":"user","content":text})
    st["messages"].append({"role":"assistant","content":"Stubbed reply."})
    M.save_state(cid, st)
    return "Stubbed reply."
M.michael_agent = _stub_agent

THREAD = [
    {"direction":"inbound","body":"yes we own it","id":"m5"},
    {"direction":"outbound","body":"Own the home?","id":"m4"},
    {"direction":"inbound","body":"yeah ameren missouri","id":"m3"},
    {"direction":"outbound","body":"On Ameren?","id":"m2"},
    {"direction":"inbound","body":"whats this about?","id":"m1"},
]
NOW = dt.datetime.now(U).isoformat()
THREAD = [dict(m, dateAdded=NOW) for m in THREAD]

wire_resume(HELD, THREAD)
cs = run(M.fetch_latest_conversation_signal("actx"))
chk("newest inbound", cs["latest_inbound"], "yes we own it", "A11")
chk("history excludes newest",
    any(h["content"]=="yes we own it" for h in cs["history"]), False, "A11")
chk("history oldest-first", cs["history"][0]["content"], "whats this about?", "A11")
chk("inbound->user", cs["history"][0]["role"], "user", "A11")
chk("outbound->assistant", cs["history"][1]["role"], "assistant", "A11")
chk("history capped 20", len(cs["history"]) <= 20, True, "A11")
chk("5 inbounds counted", cs["inbound_count"], 3, "A11")

M._state_store.clear()
n = M.rebuild_history_from_ghl("actx", cs["history"])
chk("turns restored", n, 4, "A11")
chk("starts with user", M.get_state("actx")["messages"][0]["role"], "user", "A11")
M._state_store.clear()
_live = M.get_state("alive"); _live["messages"]=[{"role":"user","content":"LIVE"}]
M.save_state("alive", _live)
chk("live state NOT clobbered", M.rebuild_history_from_ghl("alive", cs["history"]), 0, "A11")
chk("empty history no-op", M.rebuild_history_from_ghl("anew", []), 0, "A11")

M._state_store.clear(); CALLS.clear()
out = resume(HELD, THREAD)
chk("resume SENT", out["sms_sent"], True, "A11")
chk("hold cleared after send", out["hold_cleared"], True, "A11")
chk("exactly ONE message POST",
    sum(1 for c in CALLS if "/conversations/messages" in c[1]), 1, "A11")

sec("A12 - DEFERRED FIRST OUTREACH")
M._state_store.clear(); CALLS.clear()
out = resume(HELD, [])
chk("first outreach SENT", out["sms_sent"], True, "A12")
st = M.get_state("ares")
chk("stage advanced", st["stage"], M.Stage.ASK_OWNERSHIP, "A12")
chk("history recorded", len(st["messages"]) >= 2, True, "A12")
chk("uses real opener", "Ameren" in st["messages"][-1]["content"], True, "A12")

sec("A13 - RECOVERY SWEEP IDEMPOTENCY")
_tags = {"held": True}
class SweepC:
    def __init__(s,*a,**k): pass
    async def __aenter__(s): return s
    async def __aexit__(s,*a): return False
    async def get(s,url,**k):
        CALLS.append(("GET",url))
        if "/appointments" in url: return Resp(200,{"events":[]})
        if "/opportunities" in url: return Resp(200,{"opportunities":[]})
        if "/contacts/" in url:
            return Resp(200,{"contact":{**CLEAN,
                "tags":["after-hours-hold"] if _tags["held"] else []}})
        if "/conversations/search" in url: return Resp(200,{"conversations":[{"id":"CV1"}]})
        if "/messages" in url: return Resp(200,{"messages":THREAD})
        return Resp(200,{})
    async def post(s,url,**k):
        CALLS.append(("POST",url)); return Resp(201,{"messageId":"MSG-S"})
    async def put(s,url,**k):
        CALLS.append(("PUT",url))
        if _tags["held"]: _tags["held"] = False     # hold cleared durably
        return Resp(200,{})
M._state_store.clear(); CALLS.clear()
results = []
for _ in range(4):
    M.httpx.AsyncClient = SweepC
    r = run(M.after_hours_resume(Req({"contact_id":"asweep"})))
    results.append(json.loads(bytes(r.body).decode()))
chk("4 sweeps -> exactly 1 send", sum(1 for r in results if r.get("sms_sent")), 1, "A13")
chk("4 sweeps -> exactly 1 message POST",
    sum(1 for c in CALLS if "/conversations/messages" in c[1]), 1, "A13")
chk("subsequent see hold cleared", results[1]["reason"], "hold_already_cleared", "A13")

M.michael_agent = _real_agent
M.within_send_window = _real_window

sec("A16 - R1: DURABLE DNC TAG")
chk("record_sms_outcome is async",
    inspect.iscoroutinefunction(M.record_sms_outcome), True, "A16")
_rs = inspect.getsource(M.record_sms_outcome)
chk("uses main's resolve_ghl_tags mapping", "resolve_ghl_tags(Stage.DNC)" in _rs, True, "A16")
chk("uses main's add_ghl_tags", "add_ghl_tags(" in _rs, True, "A16")
chk("no hardcoded tag literal", '"solar-dnc"' in _rs, False, "A16")
chk("tag write is non-fatal", "except Exception" in _rs, True, "A16")
chk("DNC tag set matches main", M.resolve_ghl_tags(M.Stage.DNC), ["DNC", M.TAG_DNC], "A16")

# opt-out -> DNC tags actually written to GHL
TAGW = []
class TagC:
    def __init__(s,*a,**k): pass
    async def __aenter__(s): return s
    async def __aexit__(s,*a): return False
    async def get(s,url,**k):
        if "/appointments" in url: return Resp(200,{"events":[]})
        if "/opportunities" in url: return Resp(200,{"opportunities":[]})
        if "/contacts/" in url: return Resp(200,{"contact":{**CLEAN,"tags":[]}})
        return Resp(200,{})
    async def post(s,url,**k): return Resp(201,{"messageId":"X"})
    async def put(s,url,**k):
        TAGW.append(k.get("json",{}).get("tags",[])); return Resp(200,{})
M.httpx.AsyncClient = TagC
M._state_store.pop("a16", None); TAGW.clear()
run(M.record_sms_outcome("a16", "EXPLICIT_OPT_OUT", "", True, "x@y.com"))
_st16 = M.get_state("a16")
chk("stage DNC in memory", _st16["stage"], M.Stage.DNC, "A16")
chk("DNC tags written to GHL", any(M.TAG_DNC in t for t in TAGW), True, "A16")
chk("legacy DNC tag also written", any("DNC" in t for t in TAGW), True, "A16")
chk("restore_stage_from_ghl now returns DNC",
    M.restore_stage_from_ghl([M.TAG_DNC])[0], M.Stage.DNC, "A16")

# technical failure must NOT write a DNC tag
M._state_store.pop("a16b", None); TAGW.clear()
run(M.record_sms_outcome("a16b", "UNREACHABLE_OR_BAD_NUMBER", "30006", True, "x@y.com"))
chk("30006 writes NO DNC tag", any(M.TAG_DNC in t for t in TAGW), False, "A16")
chk("30006 stage NOT DNC", M.get_state("a16b")["stage"] == M.Stage.DNC, False, "A16")
chk("30006 still fallback_pending", M.get_state("a16b")["fallback_pending"], True, "A16")

sec("A14 - /health (no PII)")
h = run(M.health())
chk("status ok", h["status"], "ok", "A14")
for k in ("open","now_central","tz_abbrev","utc_offset","window"):
    chk(f"send_window.{k}", k in h["send_window"], True, "A14")
for k in ("holds_placed_since_boot","resumes_processed_since_boot",
          "resumes_sent_since_boot","authoritative_queue","last_hold_failure_contact"):
    chk(f"after_hours.{k}", k in h["after_hours"], True, "A14")
blob = json.dumps(h).lower()
for pii in ("phone","+1555","email","@","firstname","lastname"):
    chk(f"no PII: {pii!r}", pii in blob, False, "A14")

sec("A15 - main's FUNCTIONALITY PRESERVED")
for fn in ["booked_verdict","classify_carrier_error","_record_send_result",
           "add_ghl_tags","has_tag","message_status_webhook","ev",
           "classify_reply_kind","is_duplicate_inbound","is_duplicate_outbound",
           "within_daily_limit","build_new_contact_outreach","michael_agent",
           "booking_nudge_webhook","restore_stage_from_ghl","_mask_phone"]:
    chk(f"{fn} present", hasattr(M, fn), True, "A15")
chk("SendStatus intact", [s.value for s in M.SendStatus],
    ["not_attempted","suppressed","rejected","accepted","delivered","failed"], "A15")
chk("SendKind intact", len(list(M.SendKind)), 8, "A15")
chk("_STATUS_ADVANCES_STATE intact", len(M._STATUS_ADVANCES_STATE), 2, "A15")
chk("TAG_UNDELIVERABLE intact", M.TAG_UNDELIVERABLE, "sms-undeliverable", "A15")
chk("_CARRIER_PERMANENT intact", "30006" in M._CARRIER_PERMANENT, True, "A15")

print(f"\n{'='*72}\n  RESULTS: {P} passed, {F} failed ({P+F})\n{'='*72}")
if FAILS:
    print("\nFAILURES:")
    for f in FAILS: print("  " + f)
sys.exit(1 if F else 0)
