# auth_layer.py
# -*- coding: utf-8 -*-

import os
import json
import time
import asyncio
import hashlib
import secrets
import contextvars
import httpx
from typing import Dict, Any, Optional

from aiohttp import web

import dashboard_server as ds

bot_state = ds.bot_state

# ==================== TEMPLATE LOADER ====================
_TEMPLATE_CACHE: Dict[str, str] = {}


def _load_template(name: str) -> str:
    if name in _TEMPLATE_CACHE:
        return _TEMPLATE_CACHE[name]
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, "templates", name)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        _TEMPLATE_CACHE[name] = content
        return content
    except Exception as e:
        print(f"[auth] Template load error {name}: {e}", flush=True)
        return f"<h1>Template {name} not found</h1>"


HOME_HTML = _load_template("home.html")
LOGIN_HTML = _load_template("login.html")
PLANS_HTML = _load_template("plans.html")
DASHBOARD_HTML = _load_template("dashboard.html")
EXPIRED_HTML = _load_template("expired.html")
ADMIN_LOGIN_HTML = _load_template("admin_login.html")
ADMIN_PANEL_HTML = _load_template("admin_panel.html")
ADD_NEW_JOB_HTML = _load_template("add_new_job.html")
# ==================== END TEMPLATE LOADER ====================


ADMIN_USERNAME = "14444446555"
ADMIN_PASSWORD = "8325703815"
ADMIN_PATH = "/admin-shappno"

# Load saved admin credentials from data/admin_creds.json (if exists)
try:
    _creds_file = os.path.join("data", "admin_creds.json")
    if os.path.exists(_creds_file):
        with open(_creds_file, "r", encoding="utf-8") as _cf:
            _cd = json.load(_cf)
            if isinstance(_cd, dict):
                if _cd.get("admin_username"):
                    ADMIN_USERNAME = str(_cd["admin_username"])
                if _cd.get("admin_password"):
                    ADMIN_PASSWORD = str(_cd["admin_password"])
                print(f"[auth] Loaded saved admin credentials", flush=True)
except Exception as _e:
    print(f"[auth] Could not load admin creds: {_e}", flush=True)

DATA_DIR = "data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
SESSIONS_FILE = os.path.join(DATA_DIR, "sessions.json")
NOTIF_FILE = os.path.join(DATA_DIR, "notification.json")

SESSION_COOKIE = "vxl_session"
SESSION_TTL = 7 * 24 * 3600

USER_WORKERS: Dict[str, Dict[str, asyncio.Task]] = {}
USER_PAUSED: Dict[str, set] = {}
USER_DELETED: Dict[str, set] = {}
_USER_ACCOUNT_TASKS: Dict[str, set] = {}
_LAST_SYNC: Dict[str, float] = {}
_CURRENT_OWNER = contextvars.ContextVar("current_owner", default=None)

BR_API_BASE = "http://0.0.0.0:8080"
BR_START_PATH = "/BattelRoyal"
BR_STOP_PATH = "/stop"
BR_STATUS_PATH = "/status"

_mode_registry: Dict[str, str] = {}
_mode_lock = asyncio.Lock()


async def _set_mode(uid: str, mode: str):
    async with _mode_lock:
        _mode_registry[str(uid)] = mode


async def _get_mode(uid: str) -> Optional[str]:
    async with _mode_lock:
        return _mode_registry.get(str(uid))


async def _clear_mode(uid: str):
    async with _mode_lock:
        _mode_registry.pop(str(uid), None)


# ==================== NOTIFICATION HELPERS ====================
def load_notification() -> Dict[str, Any]:
    if not os.path.exists(NOTIF_FILE):
        return {"text": "IND server is disabled — please wait patiently!"}
    try:
        with open(NOTIF_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    return {"text": "IND server is disabled — please wait patiently!"}


def save_notification(data: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = NOTIF_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, NOTIF_FILE)


# ==================== BR API HELPERS ====================
async def _br_request(path: str, uid: str = "", pw: str = "", accs: str = "", timeout: float = 30.0) -> Dict[str, Any]:
    params = {}
    if accs:
        params["accs"] = accs
    else:
        params["uid"] = uid
        params["pw"] = pw
    try:
        async with httpx.AsyncClient(timeout=timeout) as c:
            r = await c.get(BR_API_BASE + path, params=params)
            try:
                return r.json()
            except Exception:
                return {"Sorry": "Invalid JSON response", "raw": r.text[:200]}
    except Exception as e:
        return {"Sorry": "API Unreachable", "error": str(e)}


async def _br_status_ping() -> Dict[str, Any]:
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get(BR_API_BASE + "/")
            latency = int((time.time() - start) * 1000)
            if r.status_code == 200:
                return {"ok": True, "latency": latency}
            return {"ok": False, "latency": latency}
    except Exception:
        return {"ok": False, "latency": 0}


# ==================== USER SYNC ====================
async def sync_user_accounts_to_disk(username):
    try:
        users = load_users()
        u = users.get(username)
        if not u:
            return
        accounts = u.get("accounts", [])
        if not accounts:
            return
        changed = False
        for saved in accounts:
            saved_key = str(saved.get("uid")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
            live = None
            for uid, acc in ds.bot_state.accounts.items():
                if acc.get("_owner") != username:
                    continue
                if str(uid) == saved_key:
                    live = acc
                    break
                cred = ds.bot_state.account_credentials.get(str(uid))
                if cred:
                    if saved.get("type") == "guest" and str(cred.get("auth_uid")) == saved_key:
                        live = acc
                        break
                    if saved.get("type") == "token" and str(cred.get("auth_token", ""))[:20] == saved_key:
                        live = acc
                        break
            if not live:
                continue
            if saved.get("initial_exp") is None and live.get("initial_exp") is not None:
                saved["initial_exp"] = live["initial_exp"]
                changed = True
            new_last_exp = live.get("current_exp")
            if new_last_exp is not None and saved.get("last_exp") != new_last_exp:
                saved["last_exp"] = new_last_exp
                changed = True
            new_level = live.get("level")
            if new_level is not None and saved.get("last_level") != new_level:
                saved["last_level"] = new_level
                changed = True
            new_nick = live.get("nickname")
            if new_nick and saved.get("last_nickname") != new_nick:
                saved["last_nickname"] = new_nick
                changed = True
            new_region = live.get("region")
            if new_region and saved.get("last_region") != new_region:
                saved["last_region"] = new_region
                changed = True
            new_gained = live.get("gained_exp")
            if new_gained is not None and saved.get("gained_exp") != new_gained:
                saved["gained_exp"] = new_gained
                changed = True
            new_played = live.get("matches_played")
            if new_played is not None and saved.get("matches_played") != new_played:
                saved["matches_played"] = new_played
                changed = True
            if live.get("created_at") and saved.get("created_at") != live.get("created_at"):
                saved["created_at"] = live.get("created_at")
                changed = True
            saved["last_update"] = time.time()
            changed = True
        if changed:
            save_users(users)
    except Exception as e:
        print(f"[auth] sync_user_accounts_to_disk error for {username}: {e}", flush=True)


def _ensure():
    os.makedirs(DATA_DIR, exist_ok=True)


def _read(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write(path, data):
    _ensure()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def load_users():
    return _read(USERS_FILE, {})


def save_users(u):
    _write(USERS_FILE, u)


def load_sessions():
    return _read(SESSIONS_FILE, {})


def save_sessions(s):
    _write(SESSIONS_FILE, s)


def hash_pw(pw, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 120000)
    return h.hex(), salt


def verify_pw(pw, h, salt):
    hh, _ = hash_pw(pw, salt)
    return secrets.compare_digest(hh, h)


def create_session(username, is_admin=False):
    token = secrets.token_urlsafe(32)
    s = load_sessions()
    now = time.time()
    s[token] = {"username": username, "is_admin": is_admin, "created_at": now, "expires_at": now + SESSION_TTL}
    now_ts = time.time()
    s = {k: v for k, v in s.items() if v.get("expires_at", 0) > now_ts}
    save_sessions(s)
    return token


def get_session(token):
    if not token:
        return None
    s = load_sessions()
    v = s.get(token)
    if not v:
        return None
    if time.time() > v.get("expires_at", 0):
        s.pop(token, None)
        save_sessions(s)
        return None
    return v


def destroy_session(token):
    s = load_sessions()
    s.pop(token, None)
    save_sessions(s)


def cur_user(req):
    return get_session(req.cookies.get(SESSION_COOKIE))


def cur_admin(req):
    s = cur_user(req)
    if s and s.get("is_admin"):
        return s
    return None


def _set_ck(resp, token):
    resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL, httponly=True, samesite="Lax", path="/")


def _clr_ck(resp):
    resp.del_cookie(SESSION_COOKIE, path="/")
    
# ==================== REGISTER PATCH ====================
_orig_register = ds.BotState.register_account


def _patched_register(self, uid, nickname, region, level, exp, likes=0):
    uid_str = str(uid)
    owner = _CURRENT_OWNER.get()

    if owner is None:
        try:
            for un, udata in load_users().items():
                for acc in udata.get("accounts", []):
                    if acc.get("type") == "guest":
                        acc_uid = str(acc.get("uid", ""))
                    else:
                        acc_uid = str(acc.get("token", ""))[:20]
                    if acc_uid == uid_str:
                        owner = un
                        break
                if owner:
                    break
        except Exception:
            pass

    if owner:
        deleted = USER_DELETED.get(owner, set())
        paused = USER_PAUSED.get(owner, set())
        if uid_str in deleted or uid_str in paused:
            return

    saved_initial = None
    saved_matches = 0
    saved_created = None
    if owner:
        try:
            users = load_users()
            u = users.get(owner)
            if u:
                for saved in u.get("accounts", []):
                    saved_key = str(saved.get("uid")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
                    if saved_key == uid_str:
                        saved_initial = saved.get("initial_exp")
                        saved_matches = saved.get("matches_played", 0)
                        saved_created = saved.get("created_at")
                        break
        except Exception:
            pass

    is_new = uid_str not in self.accounts
    _orig_register(self, uid, nickname, region, level, exp, likes)
    acc = self.accounts.get(uid_str)
    if not acc:
        return

    if saved_initial is not None:
        acc["initial_exp"] = saved_initial
        acc["gained_exp"] = max(0, exp - saved_initial)
    if saved_matches > 0:
        acc["matches_played"] = saved_matches
    if saved_created:
        acc["created_at"] = saved_created

    if owner:
        acc["_owner"] = owner

    if is_new and "created_at" not in acc:
        acc["created_at"] = time.time()

    acc["last_update"] = time.time()

    if owner:
        try:
            users = load_users()
            u = users.get(owner)
            if u:
                for saved in u.get("accounts", []):
                    saved_key = str(saved.get("uid")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
                    if saved_key == uid_str:
                        if saved.get("initial_exp") is None:
                            saved["initial_exp"] = acc["initial_exp"]
                        saved["last_exp"] = acc["current_exp"]
                        saved["last_level"] = acc["level"]
                        saved["last_nickname"] = acc["nickname"]
                        saved["last_region"] = acc["region"]
                        saved["gained_exp"] = acc["gained_exp"]
                        saved["matches_played"] = acc["matches_played"]
                        saved["created_at"] = acc.get("created_at")
                        saved["last_update"] = time.time()
                        break
                save_users(users)
        except Exception:
            pass


ds.BotState.register_account = _patched_register


# ==================== WORKER MANAGEMENT ====================
async def stop_user_workers(username):
    workers = USER_WORKERS.pop(username, {})
    tasks = list(workers.values())
    for t in tasks:
        if not t.done():
            t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    for key in list(bot_state.account_workers.keys()):
        if str(key).startswith(username + "::"):
            t = bot_state.account_workers.pop(key, None)
            if t and not t.done():
                t.cancel()
            if t:
                try:
                    await asyncio.wait_for(t, timeout=3)
                except Exception:
                    pass

    await asyncio.sleep(0.2)


async def start_user_workers(username):
    users = load_users()
    u = users.get(username)
    if not u:
        return
    if time.time() >= u.get("expires_at", 0):
        return

    if username not in USER_WORKERS:
        USER_WORKERS[username] = {}
    if username not in USER_PAUSED:
        USER_PAUSED[username] = set()
    if username not in USER_DELETED:
        USER_DELETED[username] = set()

    workers = USER_WORKERS[username]
    paused = USER_PAUSED[username]
    deleted = USER_DELETED[username]

    for acc in u.get("accounts", []):
        key = acc.get("uid") if acc.get("type") == "guest" else acc.get("token", "")[:20]
        if not key:
            continue
        if key in paused or key in deleted:
            continue

        old = workers.get(key)
        if old and not old.done():
            old.cancel()
            try:
                await asyncio.wait_for(old, timeout=2)
            except Exception:
                pass
        workers.pop(key, None)

        mode = acc.get("mode", "LW")
        if acc.get("type") == "guest":
            t = asyncio.create_task(_guest_worker(username, acc["uid"], acc["password"], mode))
        else:
            t = asyncio.create_task(_token_worker(username, acc["token"], mode))
        workers[key] = t
        bot_state.account_workers[username + "::" + key] = t


async def _guest_worker(username, uid, password, mode="LW"):
    try:
        import main as m
    except Exception as e:
        print(f"[auth] Cannot import main: {e}", flush=True)
        return
    my_task = asyncio.current_task()
    _USER_ACCOUNT_TASKS.setdefault(username, set()).add(my_task)
    try:
        while True:
            try:
                if uid in USER_DELETED.get(username, set()):
                    print(f"[auth] Guest worker {uid} deleted -> exit", flush=True)
                    break
                if uid in USER_PAUSED.get(username, set()):
                    print(f"[auth] Guest worker {uid} paused -> exit", flush=True)
                    break
                cv = _CURRENT_OWNER.set(username)
                try:
                    print(f"[auth] Guest login starting user={username} uid={uid} mode={mode}", flush=True)
                    ad = await m.process_account_uid_pass(uid, password)
                    if not ad:
                        print(f"[auth] Guest login FAILED user={username} uid={uid} -> retry in 15s", flush=True)
                        await asyncio.sleep(15)
                        continue

                    acc_id = str(ad.get("account_id", ""))
                    if uid in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                        print(f"[auth] Guest worker {uid}/{acc_id} deleted after login -> exit", flush=True)
                        break
                    if uid in USER_PAUSED.get(username, set()) or acc_id in USER_PAUSED.get(username, set()):
                        print(f"[auth] Guest worker {uid}/{acc_id} paused after login -> exit", flush=True)
                        break

                    level = int(ad.get("level", 1) or 1)
                    if mode == "BR" and level >= 3:
                        mode = "LW"
                        await _set_mode(acc_id, "LW")
                        _update_saved_mode(username, acc_id, "LW")
                    elif mode == "LW" and level < 3:
                        mode = "BR"
                        await _set_mode(acc_id, "BR")
                        _update_saved_mode(username, acc_id, "BR")

                    print(f"[auth] Guest login OK user={username} uid={uid} acc_id={acc_id} mode={mode}", flush=True)
                    await m.run_account_worker(ad, uid, mode=mode)

                    if uid in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                        print(f"[auth] Guest worker {uid}/{acc_id} deleted -> exit", flush=True)
                        break

                    print(f"[auth] Guest worker exited user={username} uid={uid} -> restart in 3s", flush=True)
                    await asyncio.sleep(3)
                finally:
                    _CURRENT_OWNER.reset(cv)
            except asyncio.CancelledError:
                print(f"[auth] Guest worker cancelled user={username} uid={uid}", flush=True)
                raise
            except Exception as e:
                print(f"[auth] Guest worker error user={username} uid={uid}: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(10)
    finally:
        try:
            _USER_ACCOUNT_TASKS.get(username, set()).discard(my_task)
        except Exception:
            pass


async def _token_worker(username, token, mode="LW"):
    try:
        import main as m
    except Exception as e:
        print(f"[auth] Cannot import main: {e}", flush=True)
        return
    my_task = asyncio.current_task()
    _USER_ACCOUNT_TASKS.setdefault(username, set()).add(my_task)
    try:
        while True:
            try:
                key = token[:20]
                if key in USER_DELETED.get(username, set()):
                    print(f"[auth] Token worker {key} deleted -> exit", flush=True)
                    break
                if key in USER_PAUSED.get(username, set()):
                    print(f"[auth] Token worker {key} paused -> exit", flush=True)
                    break
                cv = _CURRENT_OWNER.set(username)
                try:
                    print(f"[auth] Token login starting user={username} token={key} mode={mode}", flush=True)
                    ad = await m.process_account_token(token)
                    if not ad:
                        print(f"[auth] Token login FAILED user={username} -> retry in 15s", flush=True)
                        await asyncio.sleep(15)
                        continue

                    acc_id = str(ad.get("account_id", ""))
                    if key in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                        print(f"[auth] Token worker {key}/{acc_id} deleted after login -> exit", flush=True)
                        break
                    if key in USER_PAUSED.get(username, set()) or acc_id in USER_PAUSED.get(username, set()):
                        print(f"[auth] Token worker {key}/{acc_id} paused after login -> exit", flush=True)
                        break

                    level = int(ad.get("level", 1) or 1)
                    if mode == "BR" and level >= 3:
                        mode = "LW"
                        await _set_mode(acc_id, "LW")
                        _update_saved_mode(username, key, "LW")
                    elif mode == "LW" and level < 3:
                        mode = "BR"
                        await _set_mode(acc_id, "BR")
                        _update_saved_mode(username, key, "BR")

                    print(f"[auth] Token login OK user={username} acc_id={acc_id} mode={mode}", flush=True)
                    await m.run_account_worker(ad, acc_id, mode=mode)

                    if key in USER_DELETED.get(username, set()) or acc_id in USER_DELETED.get(username, set()):
                        print(f"[auth] Token worker {key}/{acc_id} deleted -> exit", flush=True)
                        break

                    print(f"[auth] Token worker exited user={username} -> restart in 3s", flush=True)
                    await asyncio.sleep(3)
                finally:
                    _CURRENT_OWNER.reset(cv)
            except asyncio.CancelledError:
                print(f"[auth] Token worker cancelled user={username}", flush=True)
                raise
            except Exception as e:
                print(f"[auth] Token worker error user={username}: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(10)
    finally:
        try:
            _USER_ACCOUNT_TASKS.get(username, set()).discard(my_task)
        except Exception:
            pass


def _update_saved_mode(username, key, mode):
    try:
        users = load_users()
        u = users.get(username)
        if not u:
            return
        for acc in u.get("accounts", []):
            saved_key = str(acc.get("uid")) if acc.get("type") == "guest" else str(acc.get("token", ""))[:20]
            if saved_key == str(key) or str(acc.get("uid")) == str(key):
                acc["mode"] = mode
                break
        save_users(users)
    except Exception:
        pass
        
# ==================== PAGE HANDLERS ====================
async def h_root(req):
    return web.Response(text=HOME_HTML, content_type="text/html")


async def h_login_get(req):
    s = cur_user(req)
    if s and not s.get("is_admin"):
        raise web.HTTPFound("/dashboard")
    return web.Response(text=LOGIN_HTML, content_type="text/html")


async def h_login_post(req):
    """শুধু password দিয়ে login"""
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    p = d.get("password") or ""
    if not p:
        return web.json_response({"status": "error", "error": "Password Required"})

    users = load_users()
    matched_user = None
    for un, udata in users.items():
        if verify_pw(p, udata.get("password_hash", ""), udata.get("salt", "")):
            matched_user = un
            break

    if not matched_user:
        print(f"[auth] Login FAILED (bad password)", flush=True)
        return web.json_response({"status": "error", "error": "Invalid Password"})

    x = users.get(matched_user)
    if time.time() >= x.get("expires_at", 0):
        return web.json_response({"status": "error", "error": "Plan Expired"})

    token = create_session(matched_user, False)
    print(f"[auth] Login OK user={matched_user}", flush=True)
    resp = web.json_response({"status": "ok", "redirect": "/dashboard"})
    _set_ck(resp, token)
    if time.time() < x.get("expires_at", 0):
        asyncio.create_task(start_user_workers(matched_user))
    return resp


async def h_signup(req):
    raise web.HTTPFound("/plans")


async def h_plans(req):
    s = cur_user(req)
    un = s["username"] if s else "{{USERNAME}}"
    return web.Response(text=PLANS_HTML.replace("{{USERNAME}}", un), content_type="text/html")


async def h_logout(req):
    token = req.cookies.get(SESSION_COOKIE)
    if token:
        s = get_session(token)
        if s and not s.get("is_admin"):
            print(f"[auth] Logout user={s['username']} (syncing state, workers kept alive)", flush=True)
            await sync_user_accounts_to_disk(s["username"])
        destroy_session(token)
    r = web.HTTPFound("/login")
    _clr_ck(r)
    raise r


async def h_dash(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users()
    x = users.get(s["username"])
    if not x:
        raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0):
        raise web.HTTPFound("/expired")
    html = DASHBOARD_HTML.replace("{{USERNAME}}", s["username"])
    return web.Response(text=html, content_type="text/html")


async def h_add_new_job_page(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    users = load_users()
    x = users.get(s["username"])
    if not x:
        raise web.HTTPFound("/logout")
    if time.time() >= x.get("expires_at", 0):
        raise web.HTTPFound("/expired")
    return web.Response(text=ADD_NEW_JOB_HTML, content_type="text/html")


async def h_expired(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        raise web.HTTPFound("/login")
    return web.Response(text=EXPIRED_HTML, content_type="text/html")
    
# ==================== STATS ====================
async def h_stats(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un, {})
    now = time.time()
    exp_at = u.get("expires_at", 0)
    expired = now >= exp_at
    left = max(0, int(exp_at - now))
    paused = USER_PAUSED.get(un, set())
    deleted = USER_DELETED.get(un, set())
    is_vip = bool(u.get("is_vip", False))

    last_sync = _LAST_SYNC.get(un, 0)
    if now - last_sync > 30:
        _LAST_SYNC[un] = now
        asyncio.create_task(sync_user_accounts_to_disk(un))

    if not expired and un not in USER_WORKERS:
        asyncio.create_task(start_user_workers(un))

    # ==================== FRESH EXP REFRESH FROM API (PARALLEL, 5s MAX) ====================
    try:
        import main as m
        refresh_tasks = []
        owner_count = 0

        for uid, a in list(ds.bot_state.accounts.items()):
            if a.get("_owner") != un:
                continue

            owner_count += 1
            cred = ds.bot_state.account_credentials.get(str(uid))

            if cred:
                refresh_tasks.append(m.refresh_account_profile(cred))

        print(f"[STATS-REFRESH] user={un} owned={owner_count} tasks={len(refresh_tasks)}", flush=True)

        if refresh_tasks:
            print(f"[STATS-REFRESH] Running {len(refresh_tasks)} parallel refresh tasks...", flush=True)

            await asyncio.wait_for(
                asyncio.gather(*refresh_tasks, return_exceptions=True),
                timeout=5.0
            )

            print(f"[STATS-REFRESH] Done", flush=True)

    except Exception as _e:
        import traceback
        print(f"[STATS-REFRESH] ERROR: {type(_e).__name__}: {_e}", flush=True)
        traceback.print_exc()
    # ==================== END FRESH REFRESH ====================

    out = []
    seen_keys = set()

    for uid, a in list(ds.bot_state.accounts.items()):
        if a.get("_owner") != un:
            continue
        if uid in deleted:
            continue
        is_paused = uid in paused
        status = "PAUSED" if is_paused else a.get("status", "LIVE")
        mode = await _get_mode(uid) or "LW"
        out.append({
            "uid": uid,
            "nickname": a.get("nickname", "?"),
            "region": a.get("region", "IND"),
            "level": a.get("level", 1),
            "initial_exp": a.get("initial_exp", 0),
            "current_exp": a.get("current_exp", 0),
            "gained_exp": a.get("gained_exp", 0),
            "matches_played": a.get("matches_played", 0),
            "status": status,
            "active_matches": a.get("active_matches", 0),
            "created_at": a.get("created_at"),
            "last_update": a.get("last_update"),
            "mode": mode,
        })
        seen_keys.add(str(uid))
        try:
            cred = ds.bot_state.account_credentials.get(str(uid))
            if cred:
                if cred.get("account_id"):
                    seen_keys.add(str(cred.get("account_id")))
                if cred.get("auth_uid"):
                    seen_keys.add(str(cred.get("auth_uid")))
                if cred.get("auth_token"):
                    seen_keys.add(str(cred.get("auth_token"))[:20])
        except Exception:
            pass

    for acc in u.get("accounts", []):
        acc_type = acc.get("type")
        if acc_type == "guest":
            key_str = str(acc.get("uid", ""))
        else:
            key_str = str(acc.get("token", ""))[:20]
        if not key_str:
            continue
        if key_str in seen_keys or key_str in deleted:
            continue
        matched_live = False
        try:
            for bid, acc_live in ds.bot_state.accounts.items():
                if acc_live.get("_owner") != un:
                    continue
                cred = ds.bot_state.account_credentials.get(str(bid))
                if not cred:
                    continue
                if acc_type == "guest" and str(cred.get("auth_uid", "")) == key_str:
                    matched_live = True
                    break
                if acc_type == "token" and str(cred.get("auth_token", ""))[:20] == key_str:
                    matched_live = True
                    break
        except Exception:
            pass
        if matched_live:
            continue

        is_paused = key_str in paused
        saved_exp = acc.get("last_exp", 0)
        saved_level = acc.get("last_level", 1)
        saved_nickname = acc.get("last_nickname", "Loading...")
        saved_region = acc.get("last_region", "IND")
        saved_initial = acc.get("initial_exp", saved_exp)
        saved_gained = acc.get("gained_exp", max(0, saved_exp - saved_initial))
        saved_played = acc.get("matches_played", 0)
        saved_created = acc.get("created_at")
        saved_updated = acc.get("last_update")
        saved_mode = acc.get("mode", "LW")

        out.append({
            "uid": key_str,
            "nickname": saved_nickname,
            "region": saved_region,
            "level": saved_level,
            "initial_exp": saved_initial,
            "current_exp": saved_exp,
            "gained_exp": saved_gained,
            "matches_played": saved_played,
            "status": "PAUSED" if is_paused else "CONNECTING",
            "active_matches": 0,
            "created_at": saved_created,
            "last_update": saved_updated,
            "mode": saved_mode,
        })

    out.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)
    return web.json_response({
        "username": un,
        "slots": u.get("slots", 0),
        "seconds_left": left,
        "expired": expired,
        "is_vip": is_vip,
        "accounts": out,
    })


def _count_active_accounts(u, un_deleted):
    seen = set()
    for a in u.get("accounts", []):
        if a.get("type") == "guest":
            k = str(a.get("uid", ""))
        else:
            k = str(a.get("token", ""))[:20]
        if k and k not in un_deleted:
            seen.add(k)
    return len(seen)


# ==================== BR API HANDLERS ====================
async def h_api_br_status(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    st = await _br_status_ping()
    return web.json_response(st)


async def h_api_br_start(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"Sorry": "unauthorized"}, status=401)
    uid = req.query.get("uid", "").strip()
    pw = req.query.get("pw", "").strip()
    accs = req.query.get("accs", "").strip()
    if accs:
        data = await _br_request(BR_START_PATH, accs=accs)
    else:
        if not uid or not pw:
            return web.json_response({"Sorry": "Missing uid/pw or accs"})
        data = await _br_request(BR_START_PATH, uid=uid, pw=pw)
    return web.json_response(data)


async def h_api_br_stop(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"Sorry": "unauthorized"}, status=401)
    uid = req.query.get("uid", "").strip()
    pw = req.query.get("pw", "").strip()
    accs = req.query.get("accs", "").strip()
    if accs:
        data = await _br_request(BR_STOP_PATH, accs=accs, timeout=15.0)
    else:
        if not uid or not pw:
            return web.json_response({"Sorry": "Missing uid/pw or accs"})
        data = await _br_request(BR_STOP_PATH, uid=uid, pw=pw, timeout=15.0)
    return web.json_response(data)


async def h_api_br_check(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"Sorry": "unauthorized"}, status=401)
    uid = req.query.get("uid", "").strip()
    pw = req.query.get("pw", "").strip()
    accs = req.query.get("accs", "").strip()
    if accs:
        data = await _br_request(BR_STATUS_PATH, accs=accs, timeout=15.0)
    else:
        if not uid or not pw:
            return web.json_response({"Sorry": "Missing uid/pw or accs"})
        data = await _br_request(BR_STATUS_PATH, uid=uid, pw=pw, timeout=15.0)
    return web.json_response(data)
    
# ==================== ACCOUNT HANDLERS ====================
async def h_acc_check(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})
    if time.time() >= u.get("expires_at", 0):
        return web.json_response({"status": "error", "error": "Plan Expired"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    accs = u.setdefault("accounts", [])
    un_deleted = USER_DELETED.get(un, set())

    if _count_active_accounts(u, un_deleted) >= u.get("slots", 0):
        return web.json_response({"status": "error", "error": "No Free Slots"})

    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    acc_data = None
    if "uid" in d and "password" in d:
        uid = str(d["uid"]).strip()
        pw = str(d["password"]).strip()
        if not uid or not pw:
            return web.json_response({"status": "error", "error": "UID And Password Required"})
        for a in accs:
            if a.get("type") == "guest" and str(a.get("uid")) == uid:
                return web.json_response({"status": "error", "error": "UID Already Added"})
        cached = m.cache_get(uid)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_uid_pass(uid, pw)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid UID/Password"})
    elif "token" in d:
        tk = str(d["token"]).strip()
        if not tk:
            return web.json_response({"status": "error", "error": "Token Required"})
        for a in accs:
            if a.get("type") == "token" and a.get("token") == tk:
                return web.json_response({"status": "error", "error": "Token Already Added"})
        cache_key = f"tok_{tk[:20]}"
        cached = m.cache_get(cache_key)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_token(tk)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid Token"})
    else:
        return web.json_response({"status": "error", "error": "Invalid Payload"})

    if not acc_data:
        return web.json_response({"status": "error", "error": "Login Failed"})

    return web.json_response({
        "status": "ok",
        "level": int(acc_data.get("level", 1) or 1),
        "region": str(acc_data.get("region", "") or "").upper(),
        "nickname": str(acc_data.get("nickname", "") or ""),
        "account_id": str(acc_data.get("account_id", "")),
    })


async def h_acc_add_job(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})
    if time.time() >= u.get("expires_at", 0):
        return web.json_response({"status": "error", "error": "Plan Expired"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    mode = str(d.get("mode", "")).upper()
    if mode not in ("BR", "LW"):
        return web.json_response({"status": "error", "error": "Mode Must Be BR Or LW"})

    accs = u.setdefault("accounts", [])
    slots = u.get("slots", 0)
    un_deleted = USER_DELETED.get(un, set())

    if _count_active_accounts(u, un_deleted) >= slots:
        return web.json_response({"status": "error", "error": "No Free Slots"})

    if un not in USER_DELETED:
        USER_DELETED[un] = set()
    if un not in USER_PAUSED:
        USER_PAUSED[un] = set()

    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    async def _validate_level(acc_data):
        lvl = int(acc_data.get("level", 1) or 1)
        if mode == "BR" and lvl >= 3:
            return False, "Battle Royale requires Level 1-2. Your account is Level " + str(lvl) + ". Please select Lone Wolf."
        if mode == "LW" and lvl < 3:
            return False, "Lone Wolf requires Level 3+. Your account is Level " + str(lvl) + ". Please select Battle Royale."
        return True, None

    acc_data = None
    key_to_start = None
    acc_id_str = None

    if "uid" in d and "password" in d:
        uid = str(d["uid"]).strip()
        pw = str(d["password"]).strip()
        if not uid or not pw:
            return web.json_response({"status": "error", "error": "UID And Password Required"})
        for a in accs:
            if a.get("type") == "guest" and str(a.get("uid")) == uid:
                return web.json_response({"status": "error", "error": "UID Already Added"})

        cached = m.cache_get(uid)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_uid_pass(uid, pw)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid UID/Password"})

        if not acc_data:
            return web.json_response({"status": "error", "error": "Login Failed"})

        ok, err = await _validate_level(acc_data)
        if not ok:
            return web.json_response({"status": "error", "error": err})

        accs.append({"type": "guest", "uid": uid, "password": pw, "mode": mode, "created_at": time.time()})
        await _set_mode(str(acc_data.get("account_id", uid)), mode)
        key_to_start = uid
        acc_id_str = str(acc_data.get("account_id", uid))
        print(f"[api] Account added user={un} uid={uid} mode={mode}", flush=True)

    elif "token" in d:
        tk = str(d["token"]).strip()
        if not tk:
            return web.json_response({"status": "error", "error": "Token Required"})
        for a in accs:
            if a.get("type") == "token" and a.get("token") == tk:
                return web.json_response({"status": "error", "error": "Token Already Added"})

        cache_key = f"tok_{tk[:20]}"
        cached = m.cache_get(cache_key)
        if cached and cached.get("level"):
            acc_data = cached
        else:
            try:
                acc_data = await m.process_account_token(tk)
            except Exception:
                return web.json_response({"status": "error", "error": "Login Failed - Invalid Token"})

        if not acc_data:
            return web.json_response({"status": "error", "error": "Login Failed"})

        ok, err = await _validate_level(acc_data)
        if not ok:
            return web.json_response({"status": "error", "error": err})

        accs.append({"type": "token", "token": tk, "mode": mode, "created_at": time.time()})
        await _set_mode(str(acc_data.get("account_id", tk[:20])), mode)
        key_to_start = tk[:20]
        acc_id_str = str(acc_data.get("account_id", tk[:20]))
        print(f"[api] Account added user={un} token={tk[:20]} mode={mode}", flush=True)
    else:
        return web.json_response({"status": "error", "error": "Invalid Payload"})

    u["accounts"] = accs
    users[un] = u
    save_users(users)

    USER_DELETED.setdefault(un, set()).discard(key_to_start)
    USER_DELETED[un].discard(acc_id_str)
    USER_PAUSED.setdefault(un, set()).discard(key_to_start)
    USER_PAUSED[un].discard(acc_id_str)

    stale = USER_WORKERS.get(un, {}).pop(key_to_start, None)
    if stale and not stale.done():
        stale.cancel()
    stale2 = USER_WORKERS.get(un, {}).pop(acc_id_str, None)
    if stale2 and not stale2.done():
        stale2.cancel()

    await start_user_workers(un)

    return web.json_response({
        "status": "ok",
        "mode": mode,
        "nickname": str(acc_data.get("nickname", "") or ""),
        "level": int(acc_data.get("level", 1) or 1),
        "region": str(acc_data.get("region", "") or "").upper(),
        "account_id": str(acc_data.get("account_id", "")),
    })


# ==================== VIP BULK ADD ====================
async def h_acc_bulk_add(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})
    if time.time() >= u.get("expires_at", 0):
        return web.json_response({"status": "error", "error": "Plan Expired"})
    if not u.get("is_vip", False):
        return web.json_response({"status": "error", "error": "VIP Only"})

    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    accounts_list = d.get("accounts", [])
    if not isinstance(accounts_list, list):
        return web.json_response({"status": "error", "error": "Invalid accounts list"})

    mode = str(d.get("mode", "LW")).upper()
    if mode not in ("BR", "LW"):
        mode = "LW"

    try:
        import main as m
    except Exception:
        return web.json_response({"status": "error", "error": "Main module unavailable"})

    accs = u.setdefault("accounts", [])
    slots = u.get("slots", 0)
    un_deleted = USER_DELETED.get(un, set())

    added = 0
    failed = 0
    errors = []

    for item in accounts_list:
        try:
            uid = str(item.get("uid", "")).strip()
            pw = str(item.get("password", "")).strip()
            if not uid or not pw:
                failed += 1
                continue

            if _count_active_accounts(u, un_deleted) >= slots:
                errors.append("No more slots")
                break

            already = False
            for a in accs:
                if a.get("type") == "guest" and str(a.get("uid")) == uid:
                    already = True
                    break
            if already:
                failed += 1
                continue

            cached = m.cache_get(uid)
            if cached and cached.get("level"):
                acc_data = cached
            else:
                acc_data = await m.process_account_uid_pass(uid, pw)

            if not acc_data:
                failed += 1
                continue

            lvl = int(acc_data.get("level", 1) or 1)
            actual_mode = mode
            if mode == "BR" and lvl >= 3:
                actual_mode = "LW"
            elif mode == "LW" and lvl < 3:
                actual_mode = "BR"

            accs.append({
                "type": "guest",
                "uid": uid,
                "password": pw,
                "mode": actual_mode,
                "created_at": time.time()
            })
            await _set_mode(str(acc_data.get("account_id", uid)), actual_mode)
            acc_id_str = str(acc_data.get("account_id", uid))
            USER_DELETED.setdefault(un, set()).discard(uid)
            USER_DELETED[un].discard(acc_id_str)
            USER_PAUSED.setdefault(un, set()).discard(uid)
            USER_PAUSED[un].discard(acc_id_str)
            added += 1
            u["accounts"] = accs
            users[un] = u
            save_users(users)
        except Exception as e:
            failed += 1
            errors.append(str(e))

    u["accounts"] = accs
    users[un] = u
    save_users(users)

    if added > 0:
        asyncio.create_task(start_user_workers(un))

    return web.json_response({
        "status": "ok",
        "added": added,
        "failed": failed,
        "errors": errors[:5]
    })


async def h_acc_delete(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    keep_in_file = bool(d.get("keep_in_file", False))

    print(f"[api] Account delete user={un} uid={uid} keep={keep_in_file}", flush=True)

    if un not in USER_DELETED:
        USER_DELETED[un] = set()
    if un not in USER_PAUSED:
        USER_PAUSED[un] = set()

    real_uid = None
    real_token_prefix = None
    real_acc_id = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            if cred.get("auth_uid"):
                real_uid = str(cred.get("auth_uid"))
            if cred.get("auth_token"):
                real_token_prefix = str(cred.get("auth_token"))[:20]
            if cred.get("account_id"):
                real_acc_id = str(cred.get("account_id"))
    except Exception:
        pass

    if real_uid is None and real_token_prefix is None and real_acc_id is None:
        try:
            for k, cred in list(ds.bot_state.account_credentials.items()):
                if str(cred.get("account_id", "")) == uid:
                    if cred.get("auth_uid"):
                        real_uid = str(cred.get("auth_uid"))
                    if cred.get("auth_token"):
                        real_token_prefix = str(cred.get("auth_token"))[:20]
                    real_acc_id = str(cred.get("account_id"))
                    break
        except Exception:
            pass

    keys_to_kill = set()
    keys_to_kill.add(uid)
    if real_uid:
        keys_to_kill.add(real_uid)
    if real_token_prefix:
        keys_to_kill.add(real_token_prefix)
    if real_acc_id:
        keys_to_kill.add(real_acc_id)

    for acc_id, acc_data in list(ds.bot_state.accounts.items()):
        if acc_data.get("_owner") != un:
            continue
        cred = ds.bot_state.account_credentials.get(str(acc_id))
        if cred:
            if real_uid and str(cred.get("auth_uid", "")) == real_uid:
                keys_to_kill.add(str(acc_id))
            if real_token_prefix and str(cred.get("auth_token", ""))[:20] == real_token_prefix:
                keys_to_kill.add(str(acc_id))

    for k in keys_to_kill:
        USER_DELETED[un].add(k)
        USER_PAUSED[un].discard(k)

    print(f"[api] Killing keys: {keys_to_kill}", flush=True)

    workers = USER_WORKERS.get(un, {})
    for k in list(keys_to_kill):
        t = workers.pop(k, None)
        if t and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass

    for key in list(bot_state.account_workers.keys()):
        if str(key).startswith(un + "::"):
            suffix = str(key).split("::", 1)[1]
            if suffix in keys_to_kill:
                t = bot_state.account_workers.pop(key, None)
                if t and not t.done():
                    t.cancel()
                    try:
                        await asyncio.wait_for(t, timeout=3)
                    except Exception:
                        pass

    for k in keys_to_kill:
        try:
            bot_state.close_writers_for_account(k)
        except Exception:
            pass

    for k in keys_to_kill:
        try:
            ds.cache_invalidate(k)
        except Exception:
            pass
        try:
            await _clear_mode(k)
        except Exception:
            pass

    removed = False
    accs_list = list(u.get("accounts", []))
    new_accs_list = []
    for acc in accs_list:
        acc_type = acc.get("type")
        acc_uid = str(acc.get("uid", ""))
        acc_token_prefix = str(acc.get("token", ""))[:20]

        match = False
        if acc_type == "guest":
            if acc_uid == uid or (real_uid and acc_uid == real_uid):
                match = True
        elif acc_type == "token":
            if acc_token_prefix == uid[:20] or (real_token_prefix and acc_token_prefix == real_token_prefix):
                match = True

        if match:
            if not keep_in_file:
                removed = True
                print(f"[api] Removed from users.json: type={acc_type} uid={acc_uid}", flush=True)
                continue
            else:
                removed = True
                print(f"[api] Kept in users.json, only stopped worker: {acc_uid}", flush=True)
                new_accs_list.append(acc)
        else:
            new_accs_list.append(acc)

    u["accounts"] = new_accs_list
    users[un] = u
    save_users(users)

    try:
        acct_tasks = _USER_ACCOUNT_TASKS.get(un, set())
        for t in list(acct_tasks):
            if t and not t.done():
                print(f"[api] Force-cancelling account task for owner={un}", flush=True)
                t.cancel()
        await asyncio.sleep(1.5)
    except Exception as _e:
        print(f"[api] Error canceling account tasks: {_e}", flush=True)

    for _ in range(3):
        for k in keys_to_kill:
            if k in bot_state.accounts:
                bot_state.accounts.pop(k, None)
        for acc_id, acc_data in list(bot_state.accounts.items()):
            if acc_data.get("_owner") != un:
                continue
            found = False
            for saved in new_accs_list:
                saved_key = str(saved.get("uid", "")) if saved.get("type") == "guest" else str(saved.get("token", ""))[:20]
                if str(acc_id) == saved_key:
                    found = True
                    break
            if not found:
                bot_state.accounts.pop(acc_id, None)
        await asyncio.sleep(0.3)

    print(f"[api] After delete: {un} has {len(new_accs_list)} accounts, killed {len(keys_to_kill)} keys", flush=True)

    return web.json_response({"status": "ok", "removed": removed})


async def h_acc_restart(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    print(f"[api] Account restart user={un} uid={uid}", flush=True)

    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Missing"})

    real_uid = None
    real_token_prefix = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            if cred.get("auth_uid"):
                real_uid = str(cred.get("auth_uid"))
            if cred.get("auth_token"):
                real_token_prefix = str(cred.get("auth_token"))[:20]
    except Exception:
        pass

    keys_to_check = [uid]
    if real_uid:
        keys_to_check.append(real_uid)
    if real_token_prefix:
        keys_to_check.append(real_token_prefix)

    saved_acc = None
    for acc in u.get("accounts", []):
        acc_type = acc.get("type")
        acc_uid = str(acc.get("uid", ""))
        acc_tok = str(acc.get("token", ""))[:20]
        if acc_type == "guest" and acc_uid in keys_to_check:
            saved_acc = acc
            break
        if acc_type == "token" and acc_tok in keys_to_check:
            saved_acc = acc
            break

    if not saved_acc:
        return web.json_response({"status": "error", "error": "Saved account not found"})

    if un not in USER_DELETED:
        USER_DELETED[un] = set()
    USER_DELETED[un].add(uid)
    if real_uid:
        USER_DELETED[un].add(real_uid)

    workers = USER_WORKERS.get(un, {})
    for k in keys_to_check:
        t = workers.pop(k, None)
        if t and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=5)
            except Exception:
                pass

    for k in keys_to_check:
        if k in bot_state.accounts:
            bot_state.accounts.pop(k, None)

    await asyncio.sleep(1.5)

    if un in USER_DELETED:
        for k in keys_to_check:
            USER_DELETED[un].discard(k)

    await start_user_workers(un)

    return web.json_response({"status": "ok"})


async def h_acc_pause(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    print(f"[api] Account pause user={un} uid={uid}", flush=True)

    real_uid = None
    real_token_prefix = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            if cred.get("auth_uid"):
                real_uid = str(cred.get("auth_uid"))
            if cred.get("auth_token"):
                real_token_prefix = str(cred.get("auth_token"))[:20]
    except Exception:
        pass

    if un not in USER_PAUSED:
        USER_PAUSED[un] = set()
    USER_PAUSED[un].add(uid)
    if real_uid:
        USER_PAUSED[un].add(real_uid)
    if real_token_prefix:
        USER_PAUSED[un].add(real_token_prefix)

    keys_to_kill = [uid]
    if real_uid:
        keys_to_kill.append(real_uid)
    if real_token_prefix:
        keys_to_kill.append(real_token_prefix)

    workers = USER_WORKERS.get(un, {})
    for k in keys_to_kill:
        t = workers.pop(k, None)
        if t and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass

    for key in list(bot_state.account_workers.keys()):
        if str(key).startswith(un + "::"):
            suffix = str(key).split("::", 1)[1]
            if suffix in keys_to_kill:
                t = bot_state.account_workers.pop(key, None)
                if t and not t.done():
                    t.cancel()
                    try:
                        await asyncio.wait_for(t, timeout=3)
                    except Exception:
                        pass

    for k in keys_to_kill:
        if k in bot_state.accounts:
            bot_state.accounts[k]["status"] = "PAUSED"

    return web.json_response({"status": "ok"})


async def h_acc_resume(req):
    s = cur_user(req)
    if not s or s.get("is_admin"):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = s["username"]
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    uid = str(d.get("uid", "")).strip()
    if not uid:
        return web.json_response({"status": "error", "error": "Missing UID"})

    print(f"[api] Account resume user={un} uid={uid}", flush=True)

    real_uid = None
    real_token_prefix = None
    try:
        cred = ds.bot_state.account_credentials.get(uid)
        if cred:
            if cred.get("auth_uid"):
                real_uid = str(cred.get("auth_uid"))
            if cred.get("auth_token"):
                real_token_prefix = str(cred.get("auth_token"))[:20]
    except Exception:
        pass

    keys_to_release = [uid]
    if real_uid:
        keys_to_release.append(real_uid)
    if real_token_prefix:
        keys_to_release.append(real_token_prefix)

    if un in USER_PAUSED:
        for k in keys_to_release:
            USER_PAUSED[un].discard(k)
    if un in USER_DELETED:
        for k in keys_to_release:
            USER_DELETED[un].discard(k)

    users = load_users()
    u = users.get(un, {})
    if time.time() < u.get("expires_at", 0):
        asyncio.create_task(start_user_workers(un))

    return web.json_response({"status": "ok"})


# ==================== ADMIN HANDLERS ====================
async def h_adm_login_get(req):
    s = cur_user(req)
    if s and s.get("is_admin"):
        raise web.HTTPFound(ADMIN_PATH + "/panel")
    return web.Response(text=ADMIN_LOGIN_HTML, content_type="text/html")


async def h_adm_login_post(req):
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    u = (d.get("username") or "").strip()
    p = d.get("password") or ""
    if u == ADMIN_USERNAME and p == ADMIN_PASSWORD:
        token = create_session(u, True)
        print(f"[auth] Admin login OK", flush=True)
        r = web.json_response({"status": "ok", "redirect": ADMIN_PATH + "/panel"})
        _set_ck(r, token)
        return r
    print(f"[auth] Admin login FAILED username={u}", flush=True)
    return web.json_response({"status": "error", "error": "Access Denied"})


async def h_adm_logout(req):
    token = req.cookies.get(SESSION_COOKIE)
    if token:
        destroy_session(token)
    r = web.HTTPFound(ADMIN_PATH)
    _clr_ck(r)
    raise r


async def h_adm_panel(req):
    if not cur_admin(req):
        raise web.HTTPFound(ADMIN_PATH)
    return web.Response(text=ADMIN_PANEL_HTML, content_type="text/html")


async def h_adm_create(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    u = (d.get("username") or "").strip()
    p = (d.get("password") or "").strip()
    slots = int(d.get("slots", 3))
    hours = int(d.get("hours", 24))
    vip = bool(d.get("vip", False))
    if not u or not p:
        return web.json_response({"status": "error", "error": "Username And Password Required"})
    if slots < 1 or slots > 1000:
        return web.json_response({"status": "error", "error": "Slots Must Be 1-1000"})
    if hours < 1 or hours > 8760:
        return web.json_response({"status": "error", "error": "Hours Must Be 1-8760"})
    users = load_users()
    if u in users:
        return web.json_response({"status": "error", "error": "Username Already Exists"})
    ph, salt = hash_pw(p)
    now = time.time()
    users[u] = {
        "password_hash": ph,
        "salt": salt,
        "slots": slots,
        "plan_hours": hours,
        "is_vip": vip,
        "created_at": now,
        "expires_at": now + hours * 3600,
        "accounts": [],
        "_expired_flag": False,
    }
    save_users(users)
    print(f"[api] Admin created user={u} slots={slots} hours={hours} vip={vip}", flush=True)
    return web.json_response({"status": "ok"})


async def h_adm_list(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    users = load_users()
    out = []
    for un, u in users.items():
        used = sum(1 for a in ds.bot_state.accounts.values() if a.get("_owner") == un)
        out.append({
            "username": un,
            "slots": u.get("slots", 0),
            "used_slots": used,
            "expires_at": u.get("expires_at", 0),
            "created_at": u.get("created_at", 0),
            "is_vip": bool(u.get("is_vip", False)),
        })
    out.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    return web.json_response({"status": "ok", "users": out})


async def h_adm_ext(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    hours = int(d.get("hours", 24))
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"})
    now = time.time()
    base = max(now, u.get("expires_at", 0))
    u["expires_at"] = base + hours * 3600
    u["_expired_flag"] = False
    save_users(users)
    if un in USER_DELETED:
        USER_DELETED[un].clear()
    print(f"[api] Admin extended user={un} +{hours}h", flush=True)
    asyncio.create_task(start_user_workers(un))
    return web.json_response({"status": "ok"})


async def h_adm_slot(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    count = int(d.get("count", 1))
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"})
    u["slots"] = min(1000, u.get("slots", 0) + count)
    save_users(users)
    print(f"[api] Admin added {count} slot to user={un}", flush=True)
    return web.json_response({"status": "ok"})


async def h_adm_toggle_vip(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"})
    u["is_vip"] = not bool(u.get("is_vip", False))
    save_users(users)
    print(f"[api] Admin toggled VIP for user={un} -> {u['is_vip']}", flush=True)
    return web.json_response({"status": "ok", "is_vip": u["is_vip"]})


async def h_adm_del(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    users = load_users()
    if un not in users:
        return web.json_response({"status": "error", "error": "User Not Found"})
    print(f"[api] Admin deleted user={un}", flush=True)
    await stop_user_workers(un)
    users.pop(un, None)
    save_users(users)
    USER_PAUSED.pop(un, None)
    USER_DELETED.pop(un, None)
    USER_WORKERS.pop(un, None)
    return web.json_response({"status": "ok"})


async def h_adm_reset_pw(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    un = d.get("username", "").strip()
    new_pw = d.get("password", "").strip()
    if not un or not new_pw:
        return web.json_response({"status": "error", "error": "Username And Password Required"})
    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"})
    ph, salt = hash_pw(new_pw)
    u["password_hash"] = ph
    u["salt"] = salt
    users[un] = u
    save_users(users)
    print(f"[api] Admin reset password user={un}", flush=True)
    return web.json_response({"status": "ok"})


async def h_adm_download_accounts(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    un = req.query.get("username", "").strip()
    if not un:
        return web.json_response({"status": "error", "error": "Username Required"}, status=400)

    users = load_users()
    u = users.get(un)
    if not u:
        return web.json_response({"status": "error", "error": "User Not Found"}, status=404)

    out_accounts = []
    for acc in u.get("accounts", []):
        acc_type = acc.get("type")
        saved_key = str(acc.get("uid", "")) if acc_type == "guest" else str(acc.get("token", ""))[:20]

        live = None
        try:
            for bid, acc_live in ds.bot_state.accounts.items():
                if acc_live.get("_owner") != un:
                    continue
                cred = ds.bot_state.account_credentials.get(str(bid))
                if not cred:
                    continue
                if acc_type == "guest" and str(cred.get("auth_uid", "")) == saved_key:
                    live = acc_live
                    break
                if acc_type == "token" and str(cred.get("auth_token", ""))[:20] == saved_key:
                    live = acc_live
                    break
        except Exception:
            pass

        if live:
            entry = {
                "uid": saved_key,
                "type": acc_type,
                "mode": acc.get("mode", "LW"),
                "nickname": live.get("nickname", ""),
                "region": live.get("region", ""),
                "level": live.get("level", 1),
                "initial_exp": live.get("initial_exp", 0),
                "current_exp": live.get("current_exp", 0),
                "gained_exp": live.get("gained_exp", 0),
                "matches_played": live.get("matches_played", 0),
                "status": live.get("status", ""),
                "created_at": live.get("created_at"),
                "last_update": live.get("last_update"),
            }
        else:
            entry = {
                "uid": saved_key,
                "type": acc_type,
                "mode": acc.get("mode", "LW"),
                "nickname": acc.get("last_nickname", ""),
                "region": acc.get("last_region", ""),
                "level": acc.get("last_level", 1),
                "initial_exp": acc.get("initial_exp", 0),
                "current_exp": acc.get("last_exp", 0),
                "gained_exp": acc.get("gained_exp", 0),
                "matches_played": acc.get("matches_played", 0),
                "status": "OFFLINE",
                "created_at": acc.get("created_at"),
                "last_update": acc.get("last_update"),
            }
        out_accounts.append(entry)

    payload = {
        "username": un,
        "exported_at": time.time(),
        "exported_at_str": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_accounts": len(out_accounts),
        "total_gained_exp": sum(a.get("gained_exp", 0) for a in out_accounts),
        "total_matches": sum(a.get("matches_played", 0) for a in out_accounts),
        "accounts": out_accounts,
    }

    return web.json_response(
        payload,
        headers={
            "Content-Disposition": f'attachment; filename="{un}_accounts.json"'
        }
    )


# ==================== NOTIFICATION HANDLERS ====================
async def h_api_get_notification(req):
    return web.json_response({"status": "ok", "text": load_notification().get("text", "")})


async def h_adm_set_notification(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})
    text = str(d.get("text", "")).strip()
    save_notification({"text": text})
    print(f"[api] Admin updated notification", flush=True)
    return web.json_response({"status": "ok"})


async def h_adm_get_notification(req):
    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    return web.json_response({"status": "ok", "text": load_notification().get("text", "")})


async def h_adm_change_credentials(req):
    global ADMIN_USERNAME, ADMIN_PASSWORD   # ✅ প্রথমে global

    if not cur_admin(req):
        return web.json_response({"status": "error", "error": "unauthorized"}, status=401)
    try:
        d = await req.json()
    except Exception:
        return web.json_response({"status": "error", "error": "Invalid JSON"})

    new_username = str(d.get("new_username", "")).strip()
    new_password = str(d.get("new_password", "")).strip()
    current_password = str(d.get("current_password", "")).strip()

    if not current_password:
        return web.json_response({"status": "error", "error": "Current Password Required"})

    # এখন ADMIN_PASSWORD পড়া যাবে (global declared above)
    if current_password != ADMIN_PASSWORD:
        return web.json_response({"status": "error", "error": "Current Password Wrong"})

    # বাকি কোড আগের মতোই...
    if not new_username and not new_password:
        return web.json_response({"status": "error", "error": "Enter New Username Or Password"})

    changed = []
    if new_username and new_username != ADMIN_USERNAME:
        ADMIN_USERNAME = new_username
        changed.append("username")

    if new_password and new_password != ADMIN_PASSWORD:
        ADMIN_PASSWORD = new_password
        changed.append("password")

    if not changed:
        return web.json_response({"status": "error", "error": "Nothing Changed"})

    try:
        creds_file = os.path.join(DATA_DIR, "admin_creds.json")
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(creds_file, "w", encoding="utf-8") as f:
            json.dump({
                "admin_username": ADMIN_USERNAME,
                "admin_password": ADMIN_PASSWORD,
                "updated_at": time.time(),
            }, f, indent=2)
    except Exception as _e:
        print(f"[auth] Failed to save admin creds: {_e}", flush=True)

    print(f"[auth] Admin credentials changed: {changed}", flush=True)
    return web.json_response({"status": "ok", "changed": changed})

# ==================== EXPIRY WATCHER ====================
async def expiry_watcher():
    while True:
        try:
            users = load_users()
            now = time.time()
            changed = False
            for un, u in users.items():
                exp = now >= u.get("expires_at", 0)
                was = u.get("_expired_flag", False)
                if exp and not was:
                    print(f"[expiry] Plan expired user={un} -> stopping workers", flush=True)
                    await stop_user_workers(un)
                    u["_expired_flag"] = True
                    changed = True
                elif not exp and was:
                    u["_expired_flag"] = False
                    changed = True
                    asyncio.create_task(start_user_workers(un))
            if changed:
                save_users(users)
        except Exception:
            pass
        await asyncio.sleep(5)


# ==================== ROUTES ====================
async def start_web_dashboard(host="0.0.0.0", port=20335):
    app = web.Application()

    app.router.add_get("/", h_root)
    app.router.add_get("/login", h_login_get)
    app.router.add_post("/login", h_login_post)
    app.router.add_get("/signup", h_signup)
    app.router.add_get("/plans", h_plans)
    app.router.add_get("/logout", h_logout)
    app.router.add_get("/dashboard", h_dash)
    app.router.add_get("/add-new-job", h_add_new_job_page)
    app.router.add_get("/expired", h_expired)

    app.router.add_get("/api/stats", h_stats)
    app.router.add_get("/api/br-status", h_api_br_status)
    app.router.add_get("/api/br-start", h_api_br_start)
    app.router.add_get("/api/br-stop", h_api_br_stop)
    app.router.add_get("/api/br-check", h_api_br_check)
    app.router.add_get("/api/notification", h_api_get_notification)

    app.router.add_post("/api/account/check", h_acc_check)
    app.router.add_post("/api/account/add-job", h_acc_add_job)
    app.router.add_post("/api/account/bulk-add", h_acc_bulk_add)
    app.router.add_post("/api/account/delete", h_acc_delete)
    app.router.add_post("/api/account/pause", h_acc_pause)
    app.router.add_post("/api/account/resume", h_acc_resume)
    app.router.add_post("/api/account/restart", h_acc_restart)

    app.router.add_get(ADMIN_PATH, h_adm_login_get)
    app.router.add_post(ADMIN_PATH, h_adm_login_post)
    app.router.add_get(ADMIN_PATH + "/logout", h_adm_logout)
    app.router.add_get(ADMIN_PATH + "/panel", h_adm_panel)

    app.router.add_post("/api/admin/create-user", h_adm_create)
    app.router.add_get("/api/admin/list-users", h_adm_list)
    app.router.add_post("/api/admin/extend-time", h_adm_ext)
    app.router.add_post("/api/admin/add-slot", h_adm_slot)
    app.router.add_post("/api/admin/toggle-vip", h_adm_toggle_vip)
    app.router.add_post("/api/admin/delete-user", h_adm_del)
    app.router.add_post("/api/admin/reset-password", h_adm_reset_pw)
    app.router.add_get("/api/admin/download-user-accounts", h_adm_download_accounts)
    app.router.add_post("/api/admin/set-notification", h_adm_set_notification)
    app.router.add_get("/api/admin/get-notification", h_adm_get_notification)
    app.router.add_post("/api/admin/change-credentials", h_adm_change_credentials)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    print(f"Web Dashboard Running On http://{host}:{port}", flush=True)
    print(f"Admin Panel: http://{host}:{port}{ADMIN_PATH}", flush=True)

    asyncio.create_task(expiry_watcher())

    try:
        users = load_users()
        now = time.time()
        for un, u in users.items():
            if now < u.get("expires_at", 0):
                asyncio.create_task(start_user_workers(un))
    except Exception:
        pass


__all__ = ["bot_state", "start_web_dashboard"]