import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
import sys
import asyncio
import httpx
import random
import json
import socket
import struct
import time
import uuid
import itertools
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from aiohttp import web

if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from google_play_scraper import app as play_scraper
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from protobuf_decoder.protobuf_decoder import Parser
from message_ids import MESSAGE_ID_TO_NAME

import thunderFF_pb2
import MajoRLoGinrEq_pb2
import StartMatch_pb2

try:
    import MajorLoginRes_pb2 as _MajorLoginRes_orig
    MajorLoginRes = _MajorLoginRes_orig.MajorLoginRes
except TypeError:
    MajorLoginRes = thunderFF_pb2.MajorLoginRes
except Exception:
    MajorLoginRes = thunderFF_pb2.MajorLoginRes

from dashboard_server import bot_state, start_web_dashboard

WEB_HOST = "0.0.0.0"
WEB_PORT = 5555
ACCOUNTS_FILE = "accounts.json"
TOKEN_CACHE_FILE = "token_cache.json"
DEVICES_FILE = "devices.json"
TOKEN_CACHE_TTL = 1200

START_MATCH_INTERVAL = 1.0
NEW_MATCH_DELAY = 1.0
MAX_MATCH_DURATION = 70000
MATCH_IDLE_TIMEOUT = 8.0
MAX_CONCURRENT_MATCHES = 15
PRIORITY_REGIONS = ["BD", "IND", "SG", "TH", "PH", "VN", "MY", "ID", "HK", "TW", "BR", "EU", "RU", "TR", "ME", "NA", "SAC", "US", "SSA"]

MAX_CONSECUTIVE_PARSE_FAILURES = 5.0
NON_MATCH_RECONNECT_DELAY = 1.0

FALLBACK_UID = ""
FALLBACK_PASSWORD = ""

MAX_LEVEL_ALLOWED = 2

_running_tasks: Dict[str, asyncio.Task] = {}
_account_data_store: Dict[str, Dict] = {}
_start_exp_store: Dict[str, int] = {}
_start_level_store: Dict[str, int] = {}
_target_level_store: Dict[str, int] = {}
_uid_to_key: Dict[str, str] = {}
_token_to_key: Dict[str, str] = {}
_lock = asyncio.Lock()
_exp_monitor_tasks: Dict[str, asyncio.Task] = {}


def _generate_new_device() -> dict:
    device_list = [
        ("Xiaomi", "M2006C3LII", "PowerVR Rogue GE8320", "Android OS 10 / API-29 (QP1A.190711.020/V12.0.26.0.QCDINXM)"),
        ("Xiaomi", "22101316I", "Adreno (TM) 610", "Android OS 13 / API-33"),
        ("Samsung", "SM-G998B", "Adreno (TM) 660", "Android OS 12 / API-31"),
        ("Realme", "RMX3700", "Mali-G710", "Android OS 14 / API-34"),
        ("OnePlus", "CPH2451", "Adreno (TM) 740", "Android OS 13 / API-33"),
    ]
    brand, model, gpu, os_ver = random.choice(device_list)
    return {
        "unique_device_id": f"Google|{str(uuid.uuid4())}",
        "brand": brand,
        "model": model,
        "gpu_renderer": gpu,
        "system_software": os_ver,
        "screen_width": random.choice([1080, 1600, 720]),
        "screen_height": random.choice([2400, 720, 1600]),
        "screen_dpi": str(random.randint(300, 420)),
        "memory": random.randint(2800, 6500),
        "processor_details": f"ARM64 FP ASIMD AES VMH | {random.randint(2200, 3200)} | {random.randint(6, 12)}",
        "client_ip": f"{random.randint(103, 223)}.{random.randint(10, 250)}.{random.randint(10, 250)}.{random.randint(10, 250)}"
    }


def sync_devices_with_accounts() -> dict:
    accounts = load_accounts()
    devices = {}
    if os.path.exists(DEVICES_FILE):
        try:
            with open(DEVICES_FILE, "r", encoding="utf-8") as f:
                devices = json.load(f)
                if not isinstance(devices, dict):
                    devices = {}
        except Exception:
            devices = {}

    cached_data = _load_token_cache()
    synced_devices = {}

    for acc in accounts:
        acc_key = None
        aliases = []
        if "uid" in acc and acc["uid"]:
            acc_key = str(acc["uid"]).strip()
            aliases.append(acc_key)
            if acc_key in bot_state.auth_to_game_id:
                aliases.append(str(bot_state.auth_to_game_id[acc_key]))
        elif "token" in acc and acc["token"]:
            tok = str(acc["token"]).strip()
            tok_pfx = tok[:16]
            aliases.append(tok_pfx)
            aliases.append(tok)
            cached_entry = cached_data.get(f"tok_{tok[:20]}") or cached_data.get(tok)
            if cached_entry:
                if cached_entry.get("open_id"):
                    aliases.insert(0, str(cached_entry["open_id"]))
                if cached_entry.get("account_id"):
                    aliases.append(str(cached_entry["account_id"]))
            if tok_pfx in bot_state.account_token_map:
                aliases.insert(0, str(bot_state.account_token_map[tok_pfx]))
            if tok in bot_state.account_token_map:
                aliases.insert(0, str(bot_state.account_token_map[tok]))

            acc_key = aliases[0] if aliases else tok_pfx

        if not acc_key:
            continue

        dev_profile = None
        for a in aliases:
            if a in devices:
                dev_profile = devices[a]
                break
        if not dev_profile and acc_key in devices:
            dev_profile = devices[acc_key]

        if not dev_profile:
            dev_profile = _generate_new_device()

        synced_devices[acc_key] = dev_profile

    try:
        with open(DEVICES_FILE, "w", encoding="utf-8") as f:
            json.dump(synced_devices, f, indent=4)
    except Exception:
        pass

    return synced_devices


def get_device_for_account(account_identifier: str) -> dict:
    devices = {}
    if os.path.exists(DEVICES_FILE):
        try:
            with open(DEVICES_FILE, "r", encoding="utf-8") as f:
                devices = json.load(f)
                if not isinstance(devices, dict):
                    devices = {}
        except Exception:
            devices = {}

    acc_key = str(account_identifier).strip()
    if acc_key in devices:
        return devices[acc_key]

    for alias in [bot_state.auth_to_game_id.get(acc_key),
                  bot_state.game_to_auth_id.get(acc_key),
                  bot_state.account_token_map.get(acc_key),
                  bot_state.account_token_map.get(acc_key[:16]) if len(acc_key) >= 16 else None]:
        if alias and str(alias) in devices:
            dev = devices.pop(str(alias))
            devices[acc_key] = dev
            try:
                with open(DEVICES_FILE, "w", encoding="utf-8") as f:
                    json.dump(devices, f, indent=4)
            except Exception:
                pass
            return dev

    new_device = _generate_new_device()
    devices[acc_key] = new_device
    try:
        with open(DEVICES_FILE, "w", encoding="utf-8") as f:
            json.dump(devices, f, indent=4)
    except Exception:
        pass

    return new_device


CLOUDFLARE_PRIMARY_DNS = "1.1.1.1"
CLOUDFLARE_SECONDARY_DNS = "1.0.0.1"
_DNS_CACHE: Dict[str, Tuple[str, float]] = {}
_DNS_CACHE_TTL = 300.0


async def resolve_host_cloudflare(hostname: str) -> str:
    if not hostname:
        return hostname

    parts = hostname.split('.')
    if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        return hostname

    now = time.time()
    if hostname in _DNS_CACHE:
        ip, exp = _DNS_CACHE[hostname]
        if now < exp:
            return ip

    def _query_cloudflare(server_ip: str) -> Optional[str]:
        s = None
        try:
            tx_id = random.randint(1000, 65535)
            header = struct.pack(">HHHHHH", tx_id, 0x0100, 1, 0, 0, 0)
            qname = b"".join(bytes([len(part)]) + part.encode('ascii') for part in hostname.split('.')) + b"\x00"
            query_pkt = header + qname + struct.pack(">HH", 1, 1)

            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(1.2)
            s.sendto(query_pkt, (server_ip, 53))
            resp, _ = s.recvfrom(1024)

            if len(resp) >= 12:
                ancount = struct.unpack(">H", resp[6:8])[0]
                if ancount > 0:
                    offset = 12 + len(qname) + 4
                    for _ in range(ancount):
                        if offset >= len(resp):
                            break
                        if (resp[offset] & 0xC0) == 0xC0:
                            offset += 2
                        else:
                            while offset < len(resp) and resp[offset] != 0:
                                offset += 1 + resp[offset]
                            offset += 1
                        if offset + 10 > len(resp):
                            break
                        rtype, rclass, ttl, rdlen = struct.unpack(">HHIH", resp[offset:offset+10])
                        offset += 10
                        if rtype == 1 and rdlen == 4 and offset + 4 <= len(resp):
                            return socket.inet_ntoa(resp[offset:offset+4])
                        offset += rdlen
        except Exception:
            pass
        finally:
            if s:
                try:
                    s.close()
                except Exception:
                    pass
        return None

    loop = asyncio.get_running_loop()
    ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_PRIMARY_DNS)
    if not ip:
        ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_SECONDARY_DNS)
    if not ip:
        try:
            ip_info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
            if ip_info:
                ip = ip_info[0][4][0]
        except Exception:
            ip = hostname

    if ip:
        _DNS_CACHE[hostname] = (ip, now + _DNS_CACHE_TTL)
    return ip or hostname


def optimize_tcp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "SIO_KEEPALIVE_VALS") and os.name == 'nt':
            try:
                sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 10000, 2000))
            except Exception:
                pass
        elif hasattr(socket, "TCP_KEEPIDLE"):
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
                if hasattr(socket, "TCP_KEEPINTVL"):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
                if hasattr(socket, "TCP_KEEPCNT"):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
            except Exception:
                pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    except Exception:
        pass


async def safe_close_writer(writer):
    if not writer:
        return
    try:
        if not writer.is_closing():
            writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=1.5)
    except Exception:
        pass


def optimize_udp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
        if hasattr(socket, 'SIO_UDP_CONNRESET') and os.name == 'nt':
            try:
                sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
    except Exception:
        pass


client = httpx.AsyncClient(
    verify=False,
    timeout=3.0,
    limits=httpx.Limits(max_connections=300, max_keepalive_connections=150)
)
_LOGIN_SEMAPHORE = asyncio.Semaphore(4)

headers = {
    'User-Agent': 'UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Expect': '100-continue',
    'X-Unity-Version': '2018.4.12f1',
    'X-GA-SV': '1789535859',
    'X-GA': 'v1 1',
    'ReleaseVersion': 'OB55'
}

AES_KEY = b'Yg&tc%DEuh6%Zc^8'
AES_IV = b'6oyZDr22E3ychjM%'

CRC7_TABLE = bytes([
    0, 9, 18, 27, 36, 45, 54, 63, 72, 65, 90, 83, 108, 101, 126, 119,
    25, 16, 11, 2, 61, 52, 47, 38, 81, 88, 67, 74, 117, 124, 103, 110,
    50, 59, 32, 41, 22, 31, 4, 13, 122, 115, 104, 97, 94, 87, 76, 69,
    43, 34, 57, 48, 15, 6, 29, 20, 99, 106, 113, 120, 71, 78, 85, 92,
    100, 109, 118, 127, 64, 73, 82, 91, 44, 37, 62, 55, 8, 1, 26, 19,
    125, 116, 111, 102, 89, 80, 75, 66, 53, 60, 39, 46, 17, 24, 3, 10,
    86, 95, 68, 77, 114, 123, 96, 105, 30, 23, 12, 5, 58, 51, 40, 33,
    79, 70, 93, 84, 107, 98, 121, 112, 7, 14, 21, 28, 35, 42, 49, 56,
    65, 72, 83, 90, 101, 108, 119, 126, 9, 0, 27, 18, 45, 36, 63, 54,
    88, 81, 74, 67, 124, 117, 110, 103, 16, 25, 2, 11, 52, 61, 38, 47,
    115, 122, 97, 104, 87, 94, 69, 76, 59, 50, 41, 32, 31, 22, 13, 4,
    106, 99, 120, 113, 78, 71, 92, 85, 34, 43, 48, 57, 6, 15, 20, 29,
    37, 44, 55, 62, 1, 8, 19, 26, 109, 100, 127, 118, 73, 64, 91, 82,
    60, 53, 46, 39, 24, 17, 10, 3, 116, 125, 102, 111, 80, 89, 66, 75,
    23, 30, 5, 12, 51, 58, 33, 40, 95, 86, 77, 68, 123, 114, 105, 96,
    14, 7, 28, 21, 42, 35, 56, 49, 70, 79, 84, 93, 98, 107, 112, 121,
])

_DELTA = 0x9E3779B9
_ROUNDS = 16
_FIELD_SIZES = {0: 1, 1: 2, 2: 2, 3: 1, 4: 2}
_FIELD_NAMES = {0: "sendOption", 1: "cmd", 2: "orderId", 3: "flags", 4: "length"}

sai_tail_dul = bytes.fromhex(
    "0101030101045452000103000100000410312e3133302e3232"
    "1432303139313231303430ca0163736f7665727365612e737472"
    "6f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
    "3b302e302e302e303b33342e3132362e37362e34353b33342e38"
    "372e3137372e31343b33342e38372e3137302e3233303b33352e"
    "3138352e3138332e353700000000000001000000000000000000"
    "0000000100000000000100000000000100b8eeec91c5d7ffde110200"
)


def print_success(text):
    try:
        print(f"[+] {text}")
    except Exception:
        pass
    try:
        bot_state.log(text, "success")
    except Exception:
        pass


def print_error(text):
    try:
        print(f"[-] {text}")
    except Exception:
        pass
    try:
        bot_state.log(text, "error")
    except Exception:
        pass


def print_warning(text):
    try:
        print(f"[!] {text}")
    except Exception:
        pass
    try:
        bot_state.log(text, "warning")
    except Exception:
        pass


def print_info(text):
    try:
        print(f"[i] {text}")
    except Exception:
        pass
    try:
        bot_state.log(text, "info")
    except Exception:
        pass


def get_proto_field(d, key, default=None):
    if not d or not isinstance(d, dict):
        return default
    if key in d:
        val = d[key].get('data')
        return val if val is not None else default
    if str(key) in d:
        val = d[str(key)].get('data')
        return val if val is not None else default
    return default


_match_counters: Dict[str, int] = {}
_match_counter_lock = asyncio.Lock()


async def _inc_match(uid: str) -> int:
    async with _match_counter_lock:
        _match_counters[uid] = _match_counters.get(uid, 0) + 1
        return _match_counters[uid]


async def _dec_match(uid: str) -> int:
    async with _match_counter_lock:
        if uid in _match_counters and _match_counters[uid] > 0:
            _match_counters[uid] -= 1
        return _match_counters.get(uid, 0)


async def _get_match_count(uid: str) -> int:
    async with _match_counter_lock:
        return _match_counters.get(uid, 0)


async def _get_total_match_count() -> int:
    async with _match_counter_lock:
        return sum(_match_counters.values())


_token_cache_memo: Dict[str, Any] = {}
_token_cache_memo_time: float = 0.0
_TOKEN_CACHE_MEMO_TTL = 5.0


def _json_serializer(obj):
    if isinstance(obj, (bytes, bytearray)):
        return {"__bytes_hex__": bytes(obj).hex()}
    raise TypeError(f"Type {type(obj)} not serializable")


def _json_deserializer(obj):
    if isinstance(obj, dict):
        if "__bytes_hex__" in obj and len(obj) == 1:
            try:
                return bytes.fromhex(obj["__bytes_hex__"])
            except Exception:
                return b""
        return {k: _json_deserializer(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_deserializer(x) for x in obj]
    return obj


def _load_token_cache() -> Dict[str, Any]:
    global _token_cache_memo, _token_cache_memo_time
    now = time.time()
    if _token_cache_memo and (now - _token_cache_memo_time) < _TOKEN_CACHE_MEMO_TTL:
        return _token_cache_memo

    if not os.path.exists(TOKEN_CACHE_FILE):
        return {}
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return {}
        data = json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("Cache root must be dict")
        parsed = _json_deserializer(data)
        _token_cache_memo = parsed
        _token_cache_memo_time = now
        return parsed
    except Exception:
        try:
            os.remove(TOKEN_CACHE_FILE)
        except Exception:
            pass
        return {}


def _save_token_cache(cache: Dict[str, Any]):
    global _token_cache_memo, _token_cache_memo_time
    try:
        tmp_file = TOKEN_CACHE_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, default=_json_serializer)
        os.replace(tmp_file, TOKEN_CACHE_FILE)
        _token_cache_memo = cache
        _token_cache_memo_time = time.time()
    except Exception:
        pass


def cache_get(uid: str) -> Optional[Dict]:
    cache = _load_token_cache()
    entry = cache.get(str(uid))
    if not entry:
        return None
    if time.time() - entry.get("cached_at", 0) > TOKEN_CACHE_TTL:
        cache_invalidate(uid)
        return None
    if str(entry.get("account_id", "")).isdigit():
        entry["account_id"] = int(entry["account_id"])
    if not isinstance(entry.get("login_payload_data"), (bytes, bytearray)):
        cache_invalidate(uid)
        return None
    return entry


def cache_set(uid: str, account_data: Dict):
    cache = _load_token_cache()
    entry = dict(account_data)
    entry["cached_at"] = time.time()
    cache[str(uid)] = entry
    _save_token_cache(cache)


def cache_invalidate(uid: str):
    cache = _load_token_cache()
    if str(uid) in cache:
        del cache[str(uid)]
        _save_token_cache(cache)


async def aes_encrypt(payload, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return cipher.encrypt(pad(payload, AES.block_size))


_VERSION_CONFIG_CACHE = None
_VERSION_CONFIG_CACHE_TIME = 0.0
_VERSION_CONFIG_TTL = 1800.0


async def get_playstore_version():
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None,
            lambda: play_scraper('com.dts.freefireth', lang='hi', country='id')
        )
        return result.get("version")
    except Exception:
        return "1.132.6"


async def version_config():
    global _VERSION_CONFIG_CACHE, _VERSION_CONFIG_CACHE_TIME
    now = time.time()
    if _VERSION_CONFIG_CACHE and (now - _VERSION_CONFIG_CACHE_TIME) < _VERSION_CONFIG_TTL:
        return _VERSION_CONFIG_CACHE

    try:
        app_version = await get_playstore_version() or "1.132.6"
        api_url = (
            "https://version.ggwhitehawk.com/live/ver.php"
            f"?version={app_version}"
            "&lang=hi&device=android&channel=android"
            "&appstore=googleplay&region=BD"
            "&whitelist_version=1.3.0&whitelist_sp_version=1.0.0"
        )
        response = await client.get(api_url, timeout=8.0)
        response.raise_for_status()
        data = response.json()
        server_url = data.get("server_url")
        remote_version = data.get("remote_version")
        latest_release_version = data.get("latest_release_version")
        if server_url and remote_version and latest_release_version:
            _VERSION_CONFIG_CACHE = (latest_release_version, remote_version, server_url)
            _VERSION_CONFIG_CACHE_TIME = now
            return _VERSION_CONFIG_CACHE
    except Exception:
        if _VERSION_CONFIG_CACHE:
            return _VERSION_CONFIG_CACHE
    return None


async def get_access_token(uid, password):
    url = "https://100067.connect.garena.com/api/v2/oauth/guest/token:grant"
    hdrs = {
        "Host": "100067.connect.garena.com",
        "User-Agent": "GarenaMSDK/4.0.19P4(G011A ;Android 13;en;IN;)",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"
    }
    payload = {
        "client_id": 100067,
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_type": 2,
        "password": password,
        "response_type": "token",
        "uid": int(uid)
    }
    for attempt in range(5):
        try:
            response = await client.post(url, headers=hdrs, json=payload)
            if response.status_code == 200:
                res_data = response.json()
                inner = res_data.get("data", res_data)
                open_id = inner.get("open_id")
                access_token = inner.get("access_token")
                platform = inner.get("platform", 4)
                if open_id and access_token:
                    return open_id, access_token, platform
            if response.status_code == 429:
                await asyncio.sleep(1)
                continue
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return None


async def parse_results(parsed_results):
    result_dict = {}
    for result in parsed_results:
        field_data = {"wire_type": result.wire_type}
        if result.wire_type == "varint":
            field_data["data"] = result.data
        elif result.wire_type == "string":
            field_data["data"] = result.data
        elif result.wire_type == "bytes":
            field_data["data"] = result.data
        elif result.wire_type == "length_delimited":
            if hasattr(result.data, "results"):
                field_data["data"] = await parse_results(result.data.results)
            elif isinstance(result.data, list):
                field_data["data"] = await parse_results(result.data)
            else:
                field_data["data"] = str(result.data)
        result_dict[str(result.field)] = field_data
    return result_dict


async def decode_protobuf(data):
    parsed_results = Parser().parse(data)
    parsed_results_dict = await parse_results(parsed_results)
    return json.dumps(parsed_results_dict)


async def build_majorlogin_payload(open_id, access_token, platform, client_version, device_info):
    try:
        major_login = MajoRLoGinrEq_pb2.MajorLogin()

        major_login.open_id = open_id
        major_login.access_token = access_token
        major_login.client_version = client_version

        major_login.event_time = str(datetime.now())[:-7]
        major_login.game_name = "free fire"
        major_login.platform_id = 1
        major_login.system_software = "Android OS 10 / API-29 (QP1A.190711.020/V12.0.26.0.QCDINXM)"
        major_login.system_hardware = "Handheld"
        major_login.telecom_operator = "Ncell"
        major_login.network_type = "WIFI"
        major_login.screen_width = 1600
        major_login.screen_height = 720
        major_login.screen_dpi = "320"
        major_login.processor_details = "ARMv7 VFPv3 NEON | 2001 | 8"
        major_login.memory = 3790
        major_login.gpu_renderer = "PowerVR Rogue GE8320"
        major_login.gpu_version = "OpenGL ES 3.2 build 1.11@5425693"
        major_login.unique_device_id = "Google|00000000-0000-0000-0000-000000000000"
        major_login.client_ip = "111.119.38.133"
        major_login.language = "en"
        major_login.open_id_type = "4"
        major_login.device_type = "Handheld"

        major_login.device_model = "Xiaomi M2006C3LII"
        major_login.country_code = "BD"

        major_login.platform_sdk_id = 1
        major_login.network_operator_a = "Ncell"
        major_login.network_type_a = "WIFI"
        major_login.client_using_version = "1ac4b80ecf0478a44203bf8fac6120f5"

        major_login.external_storage_total = 53041
        major_login.external_storage_available = 7291
        major_login.internal_storage_total = 2176
        major_login.game_disk_storage_available = 7395
        major_login.game_disk_storage_total = 53041
        major_login.external_sdcard_avail_storage = 7395
        major_login.external_sdcard_total_storage = 53041

        major_login.field_70 = 4
        major_login.login_by = 2
        major_login.library_path = "/data/app/com.dts.freefireth-yAPXAhp2RyIlrtNAM0VzKQ==/lib/arm"
        major_login.reg_avatar = 1
        major_login.library_token = "066a589fa3f5658377634fe7b1d88556|/data/app/com.dts.freefireth-yAPXAhp2RyIlrtNAM0VzKQ==/base.apk"
        major_login.channel_type = 6
        major_login.cpu_type = 1
        major_login.cpu_architecture = "32"
        major_login.client_version_code = "2019121227"

        major_login.field_85 = 3
        major_login.graphics_api = "OpenGLES2"
        major_login.supported_astc_bitset = 3071
        major_login.login_open_id_type = 4
        major_login.loading_time = 9329
        major_login.release_channel = "3rd_party"
        major_login.extra_info = "KqsHT3r+fXQIu/dyZrEa8fJBhbJ5uqDES7YsAUfu+Mck9A+Bly6lFfYk7Q7Nj68pqI8I3g4Oz3gLxWef6Eh/jKyzHug="
        major_login.android_engine_init_flag = 111207

        major_login.field_96 = json.dumps({"cur_rate": None, "support_etc2": False}, separators=(',', ':'))

        major_login.if_push = 1
        major_login.origin_platform_type = "4"
        major_login.primary_platform_type = "4"

        major_login.field_102 = bytes.fromhex("42 54 4c 10 53 0e 5b 04 30")
        major_login.field_104 = 47591
        major_login.field_105 = 1
        major_login.field_106 = "https://dl-bs.ggpolarbear.com/live/ABHotUpdates/|https://core-bs.ggpolarbear.com/live/ABHotUpdates/|1c2462939e53942fc995400436a3dc7b"
        major_login.field_107 = "c8e41b7a93f02d56e1a94c7b8203f5d1"

        string = major_login.SerializeToString()
        return await aes_encrypt(string, AES_KEY, AES_IV)
    except Exception:
        return None


async def build_majorlogin_token_payload(open_id, access_token, platform, client_version, device_info):
    try:
        major_login = MajoRLoGinrEq_pb2.MajorLogin()

        try:
            platform_int = int(platform)
        except Exception:
            platform_int = 4

        major_login.open_id = open_id
        major_login.access_token = access_token
        major_login.client_version = client_version

        major_login.event_time = str(datetime.now())[:-7]
        major_login.game_name = "free fire"
        major_login.platform_id = platform_int
        major_login.system_software = "Android OS 10 / API-29 (QP1A.190711.020/V12.0.26.0.QCDINXM)"
        major_login.system_hardware = "Handheld"
        major_login.telecom_operator = "Ncell"
        major_login.network_type = "WIFI"
        major_login.screen_width = 1600
        major_login.screen_height = 720
        major_login.screen_dpi = "320"
        major_login.processor_details = "ARMv7 VFPv3 NEON | 2001 | 8"
        major_login.memory = 3790
        major_login.gpu_renderer = "PowerVR Rogue GE8320"
        major_login.gpu_version = "OpenGL ES 3.2 build 1.11@5425693"
        major_login.unique_device_id = "Google|00000000-0000-0000-0000-000000000000"
        major_login.client_ip = "111.119.38.133"
        major_login.language = "en"
        major_login.open_id_type = str(platform_int)
        major_login.device_type = "Handheld"

        major_login.device_model = "Xiaomi M2006C3LII"
        major_login.country_code = "BD"

        major_login.platform_sdk_id = platform_int
        major_login.network_operator_a = "Ncell"
        major_login.network_type_a = "WIFI"
        major_login.client_using_version = "1ac4b80ecf0478a44203bf8fac6120f5"

        major_login.external_storage_total = 53041
        major_login.external_storage_available = 7291
        major_login.internal_storage_total = 2176
        major_login.game_disk_storage_available = 7395
        major_login.game_disk_storage_total = 53041
        major_login.external_sdcard_avail_storage = 7395
        major_login.external_sdcard_total_storage = 53041

        major_login.field_70 = 4
        major_login.login_by = 2
        major_login.library_path = "/data/app/com.dts.freefireth-yAPXAhp2RyIlrtNAM0VzKQ==/lib/arm"
        major_login.reg_avatar = 1
        major_login.library_token = "066a589fa3f5658377634fe7b1d88556|/data/app/com.dts.freefireth-yAPXAhp2RyIlrtNAM0VzKQ==/base.apk"
        major_login.channel_type = 6
        major_login.cpu_type = 1
        major_login.cpu_architecture = "32"
        major_login.client_version_code = "2019121227"

        major_login.field_85 = 3
        major_login.graphics_api = "OpenGLES2"
        major_login.supported_astc_bitset = 3071
        major_login.login_open_id_type = platform_int
        major_login.loading_time = 9329
        major_login.release_channel = "3rd_party"
        major_login.extra_info = "KqsHT3r+fXQIu/dyZrEa8fJBhbJ5uqDES7YsAUfu+Mck9A+Bly6lFfYk7Q7Nj68pqI8I3g4Oz3gLxWef6Eh/jKyzHug="
        major_login.android_engine_init_flag = 111207

        major_login.field_96 = json.dumps({"cur_rate": None, "support_etc2": False}, separators=(',', ':'))

        major_login.if_push = 1
        major_login.origin_platform_type = str(platform_int)
        major_login.primary_platform_type = str(platform_int)

        major_login.field_102 = bytes.fromhex("42 54 4c 10 53 0e 5b 04 30")
        major_login.field_104 = 47591
        major_login.field_105 = 1
        major_login.field_106 = "https://dl-bs.ggpolarbear.com/live/ABHotUpdates/|https://core-bs.ggpolarbear.com/live/ABHotUpdates/|1c2462939e53942fc995400436a3dc7b"
        major_login.field_107 = "c8e41b7a93f02d56e1a94c7b8203f5d1"

        string = major_login.SerializeToString()
        return await aes_encrypt(string, AES_KEY, AES_IV)
    except Exception:
        return None


async def send_majorlogin(data, release_version, server_url):
    try:
        url = f"{server_url}MajorLogin"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = release_version
        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200:
            return None
        response_content = response.content
        if len(response_content) < 40:
            return None

        proto_payload = response_content[64:] if len(response_content) > 64 else response_content

        res_proto = MajorLoginRes()
        try:
            res_proto.ParseFromString(proto_payload)
            if res_proto.region and res_proto.token:
                return res_proto
        except Exception:
            pass

        for offset in range(min(128, len(proto_payload))):
            try:
                candidate = MajorLoginRes()
                candidate.ParseFromString(proto_payload[offset:])
                if candidate.region and candidate.token:
                    return candidate
            except Exception:
                pass

        try:
            res_proto = MajorLoginRes()
            res_proto.ParseFromString(response_content)
            return res_proto
        except Exception:
            return None
    except Exception:
        return None


async def send_getlogin(data, base_url, token, release_version):
    try:
        url = f"{base_url.rstrip('/')}/GetLoginData"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = release_version
        req_headers['Authorization'] = f"Bearer {token}"
        req_headers['Host'] = "clientbp.ppmainecoonghj.com"
        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200:
            return None
        response_content = response.content

        res_proto = thunderFF_pb2.GetLoginDataRes()
        parsed_successfully = False
        try:
            res_proto.ParseFromString(response_content)
            if res_proto.functional_addrs or res_proto.informational_addrs:
                parsed_successfully = True
        except Exception:
            pass

        if not parsed_successfully:
            for offset in range(min(128, len(response_content))):
                try:
                    candidate = thunderFF_pb2.GetLoginDataRes()
                    candidate.ParseFromString(response_content[offset:])
                    if candidate.functional_addrs or candidate.informational_addrs:
                        res_proto = candidate
                        break
                except Exception:
                    pass

        dict_res = {}
        try:
            parsed = Parser().parse(response_content.hex())
            dict_res = await parse_results(parsed)
        except Exception:
            pass

        return res_proto, dict_res
    except Exception:
        return None


async def build_tcp_startup_packet(account_id, token, server_time, key, iv, region="BD", typ='OnLine'):
    uid_hex = f"{int(account_id):016x}"
    timestamp_hex = f"{int(server_time):08x}"
    encode_token = token.encode()
    encrypted_packet = (await aes_encrypt(encode_token, key, iv)).hex()
    encrypted_packet_length = f"{len(encrypted_packet) // 2:08x}"
    reg = str(region).upper() if region else "BD"
    if typ == 'OnLine':
        prefix = '7219' if reg == 'BD' else ('7214' if reg == 'IND' else '7215')
        return f"{prefix}{uid_hex}{timestamp_hex}00000000{encrypted_packet_length}{encrypted_packet}"
    else:
        prefix = '8119' if reg == 'BD' else ('8114' if reg == 'IND' else '8115')
        return f"{prefix}{uid_hex}{timestamp_hex}{encrypted_packet_length}{encrypted_packet}"


async def send_keep_alive(region="BD"):
    try:
        reg = str(region).upper() if region else "BD"
        ka_hex = "0219" if reg == "BD" else ("0214" if reg == "IND" else "0215")
        return bytes.fromhex(ka_hex)
    except Exception:
        return bytes.fromhex("0219")


async def start_game_battle_royale(region, client_version, writer, key, iv):
    packet = bytes.fromhex("080112800a0a010110013a110a044944433110aa011a064555524f50453a100a044944433210311a064555524f504540014a0801090a0b1219202758016291090a8001303838463832424630324139363736373032303130313030303030303030303030303136303030313030313530303032323246393745454530463030303030303436373632353134303030303030303030303030303030303030303030303030303030303030303030303030303066663030303030303030636163666131366410241afb02735d5e571400024a775d45414d1a041b1c001f11010449715f4243481a001e1d071c1703004b1a4066785c524570735c51486775421b5c5a4c07504042685a63610816054e19025e75196001477c015165406370195f5547404e4550640103020f1304064863754268676c755f65576e40467e5f0a417a4701026d675d6e73670b1108495a4c6a0b78470b740065645e525a057258425f584a447d4e6759440c11044e7c596d7f4b625f7d04055a47505c4e1d6b5b4107447d7201057d7f0f14084e430457674f7e517d72015172415d027473577c4d615f79535256780911030f4d5e027a797f614165067806505d53777750475e75064257076500460817014e741e7e5078487e7a7c465e7669767153497064605a7376677773550d160148037e18675966787f4c42607a645f577e7b441b460776026b18685d0b110205490060020f70676175654674706671797f41067346677c4e06585e780f15074c57047b40517075415f6364027259674b5b0166407f7340600407770a22047a5d5c52300b3a0a167305067162727516134208312e3133302e3232480350015ae90403626253513635686e556f4e36416456324b796f566c636f477776484f624e56526c4d727073504b4f43654177616848494176795556497273743752737149734a7a786b3247525268377a2f637664626d504f6a73552f79626d38547a4c69586d2f474351696d494b53486833447955726f39515152756c34545350626d6d624b7949565937545671577059455372323646572f59624578507338514f706d317372785455736c30796a434144444d4f34616a654b615753366361496c554b4963797a494e396d52516f715277687939797257476d337a644345337a6a61436f492f5a585233656f65365a42647a64677654636b6b665733356e4d4c6a6a565072564b6433523172756174394e50514150724a5546627859696c4c5a3859707336654d5447666b6649793574666a526c314d4648706b51774c6373374439656378566c41636f374e664f6d2b30654756466c4434744478706771385533595973587645384842502f70666c767a737138316a32524f4d7857437556445442492f684735625462773166456e4249725162762b636144775147696f74554e316d4c4b77734379456f4766706746614251457645672b736a764c4c78704743334c304a5344532f74526169504354553344374e6249306547516651622f5a466f4c36455630775a324d6f583932414c572f5049752f56634663584e70596b356f7966326151416a536971486a2f363276354843644f525551303578754e6171795251625653704654303137655237675255636b4966366c6f447476342b514e4a4670766d74757077707774396a5a5974437a4b56743657726d6e36785837706658456251555434684f3758a201050803108703a201050804108103a20105080510c001a20105081d10cc01a2010408161078a20105080e10af01a201020815")
    proto = thunderFF_pb2.StartMatch()
    proto.ParseFromString(packet)
    if hasattr(proto.main, 'region_list') and len(proto.main.region_list) > 0:
        proto.main.region_list[0].region = region
        if len(proto.main.region_list) > 1:
            proto.main.region_list[1].region = region
    if hasattr(proto.main, 'client_version'):
        proto.main.client_version.remote_version = client_version
    packet = proto.SerializeToString()
    encrypted_packet = (await aes_encrypt(packet, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = hex(packet_length)[2:]
    hex_length = hex_length if len(hex_length) > 1 else "0" + hex_length
    reg = str(region).upper() if region else "BD"
    reg_prefix = "031900" if reg == "BD" else ("031400" if reg == "IND" else "031500")
    final_packet = reg_prefix + "0" * (6 - len(hex_length)) + hex_length + encrypted_packet
    writer.write(bytes.fromhex(final_packet))
    await writer.drain()


async def has_ssan_zig(n):
    z = (n << 1) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while z >= 0x80:
        out.append((z & 0x7F) | 0x80)
        z >>= 7
    out.append(z)
    return bytes(out)


async def uleb_encode(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)


async def tea_enc(v0, v1, k0, k1, k2, k3):
    s = 0
    for _ in range(_ROUNDS):
        s = (s + _DELTA) & 0xFFFFFFFF
        v0 = (v0 + (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v1 = (v1 + (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
    return v0, v1


async def tea_dec(v0, v1, k0, k1, k2, k3):
    s = (_DELTA * _ROUNDS) & 0xFFFFFFFF
    for _ in range(_ROUNDS):
        v1 = (v1 - (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v0 = (v0 - (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        s = (s - _DELTA) & 0xFFFFFFFF
    return v0, v1


async def tea_cbc_encrypt(padded, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(padded))
    prev_cipher = bytearray(8)
    prev_intermediate = bytearray(8)
    for i in range(0, len(padded), 8):
        xored = bytearray(8)
        for j in range(8):
            xored[j] = padded[i + j] ^ prev_cipher[j]
        e0, e1 = await tea_enc(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3,
        )
        enc = bytearray(8)
        struct.pack_into("<I", enc, 0, e0)
        struct.pack_into("<I", enc, 4, e1)
        for j in range(8):
            out[i + j] = enc[j] ^ prev_intermediate[j]
        prev_cipher[:] = out[i:i + 8]
        prev_intermediate[:] = xored
    return bytes(out)


async def build_padded(content):
    pad_len = (8 - (len(content) + 10) % 8) % 8
    return bytes([pad_len, 0, 0]) + b"\x00" * pad_len + content + b"\x00" * 7


async def encode_header(layout, send_option, cmd, order_id, flags, length, k, v80):
    out = bytearray()
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: length}[code]
        if _FIELD_SIZES[code] == 1:
            out.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            out.append(v & 0xFF)
            out.append((v >> 8) & 0xFF)
    return bytes(out)


async def crc7_buff(crc, buf):
    c = crc & 0x7F
    for b in buf:
        c = CRC7_TABLE[((2 * (c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F


async def sv_frame(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0]) + await encode_header(layout, send_option, cmd, order_id, flags, len(body), k, v80)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)


async def build_match_startup_packets(token, udp_key, match_code, account_id, block_val,
                                      server_ip="", region="BD", client_version="1.132.6",
                                      client_version_code="2019121227", access_token="",
                                      mode="BR"):
    token = token.strip()
    udp_key = bytes.fromhex(udp_key)
    match_code = [int(ch) for ch in str(match_code).strip()]

    thunder_jwt = token[:660] if len(token) > 660 else token
    sharma_jwt = token[660:] if len(token) > 660 else ""
    encoded_thunder_jwt = thunder_jwt.encode() if isinstance(thunder_jwt, str) else thunder_jwt
    encoded_sharma_jwt = sharma_jwt.encode() if isinstance(sharma_jwt, str) else sharma_jwt

    garena420 = await has_ssan_zig(len(encoded_thunder_jwt)) + encoded_thunder_jwt

    reg = str(region).upper() if region else "BD"

    csoversea_block = bytes.fromhex(
        "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
        "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
        "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
        "00000000000100010000000100b09df8c5fad88bdf110200"
    )
    m_val1 = 1
    m_val2 = 1

    mid = bytes.fromhex('0000000001000102030101') + await has_ssan_zig(len(reg)) + reg.encode()
    mid += bytes.fromhex('0001030003000004')
    mid += await has_ssan_zig(len(client_version)) + client_version.encode()
    mid += await has_ssan_zig(len(client_version_code)) + client_version_code.encode()
    mid += csoversea_block

    clean_ip = server_ip.split(':')[0] if server_ip else "0.0.0.0"
    mid += await has_ssan_zig(len(clean_ip)) + clean_ip.encode()

    clean_acc_tok = access_token.strip() if access_token else ""
    if clean_acc_tok:
        mid += await has_ssan_zig(len(clean_acc_tok)) + clean_acc_tok.encode()

    mid += await has_ssan_zig(len(encoded_sharma_jwt)) + encoded_sharma_jwt

    tg_garena420 = (
        await uleb_encode(int(account_id)) +
        await uleb_encode(int(block_val)) +
        await uleb_encode(1) +
        await uleb_encode(m_val1) +
        await uleb_encode(int(block_val)) +
        await uleb_encode(m_val2) +
        mid
    )

    process = await sv_frame(0x5E, match_code, 2, 447, 0, 1, garena420, udp_key)
    loading = await sv_frame(0x5A, match_code, 2, 448, 1, 1, tg_garena420, udp_key)
    return process.hex(), loading.hex()


async def produce_xor_key(secret_key):
    k = secret_key[0] if secret_key and len(secret_key) > 0 else 10
    return k, ((k << 8) | k) & 0xFFFF


async def parse_layout(layout):
    if isinstance(layout, str):
        return [int(ch) for ch in layout.strip()]
    return list(layout)


async def tea_cbc_decrypt(body, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(body))
    prev_intermediate = bytearray(8)
    prev_cipher = bytearray(8)
    xored = bytearray(8)
    dec = bytearray(8)
    for i in range(0, len(body), 8):
        for j in range(8):
            xored[j] = body[i + j] ^ prev_intermediate[j]
        d0, d1 = await tea_dec(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3
        )
        struct.pack_into("<I", dec, 0, d0)
        struct.pack_into("<I", dec, 4, d1)
        for j in range(8):
            out[i + j] = dec[j] ^ prev_cipher[j]
        prev_cipher[:] = body[i:i + 8]
        prev_intermediate[:] = dec
    return bytes(out)


async def build_hello_packet(text, key, layout):
    data = text.encode("utf-8")
    if len(data) > 25:
        raise ValueError(f"Text is too long ({len(data)} bytes)")
    content = b"\x10\x00\x00\x00" + data + b"\x00" * (29 - 4 - len(data))
    k, v80 = await produce_xor_key(key)
    layout = await parse_layout(layout)
    padded = await build_padded(content)
    enc_body = await tea_cbc_encrypt(padded, key)
    header_bytes = await encode_header(layout, 1, 1, 0, 1, len(enc_body), k, v80)
    packet = bytearray([0x63, 0x00]) + header_bytes + enc_body
    packet[1] = await crc7_buff(0, packet[2:]) & 0x7F
    return bytes(packet).hex()


async def classify(frame):
    cmd = frame["cmd"]
    msg_name = MESSAGE_ID_TO_NAME.get(cmd, f"UNKNOWN_{cmd}")
    if msg_name == "UDP_HELLO":
        return "HELLO"
    if msg_name == "UDP_ACK":
        return "ACK"
    if msg_name == "UDP_PING":
        return "PING"
    if msg_name == "RUDP_JOIN_MATCH":
        return "JOIN_MATCH"
    if msg_name.startswith("RUDP_"):
        return msg_name
    if msg_name.startswith("UDP_"):
        return msg_name
    return "DATA"


async def build_packet(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0])
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: len(body)}[code]
        if _FIELD_SIZES[code] == 1:
            hdr.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            hdr.append(v & 0xFF)
            hdr.append((v >> 8) & 0xFF)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)


async def layouts_from_mask(mask):
    ru = [int(c) for c in str(mask).strip()]
    nr = [c for c in ru if c != 2]
    return ru, nr


async def reply_for(frame, key, mask, ack_key=0x68, ping_key=0x6D, hello_key=0x5B, ack_style="short"):
    ru, nr = await layouts_from_mask(mask)
    typ = await classify(frame)
    if typ == "HELLO":
        if ack_style == "echo":
            content = frame["content"] if frame["content"] else b"\x10\x00\x00\x00"
            return typ, await build_packet(hello_key, nr, 1, 1, None, 1, content, key)
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x01\x00", key)
    if typ == "ACK":
        content = frame["content"] if frame["content"] else b"\x01\x00"
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, content, key)
    if typ == "PING":
        c = frame["content"]
        counter = c[:4] if len(c) >= 4 else c
        return typ, await build_packet(ping_key, nr, 0, 3, None, 0, counter + b"\x00\x00\x00", key, encrypted=False)
    if typ == "JOIN_MATCH":
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x02\x00", key)
    return typ, None


async def keepalive_ping(sock, ip, port, key_bytes, mask, stop_event):
    nr = (await layouts_from_mask(mask))[1]
    ping_keys = [0x66, 0x6D, 0x69, 0x6C, 0x6B, 0x6E, 0x6F, 0x70]
    loop = asyncio.get_event_loop()
    i = 0
    while not stop_event.is_set():
        pk = ping_keys[i % len(ping_keys)]
        counter = int(time.time() * 1000) & 0xFFFFFFFF
        pkt = await build_packet(pk, nr, 0, 3, None, 0, struct.pack("<I", counter) + b"\x00\x00\x00", key_bytes, encrypted=False)
        try:
            await loop.sock_sendto(sock, pkt, (ip, port))
        except Exception:
            pass
        i += 1
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            pass


async def try_header(buf, layout, k, v80):
    off = 2
    out = {}
    for code in layout:
        size = _FIELD_SIZES[code]
        if off + size > len(buf):
            return None
        out[_FIELD_NAMES[code]] = (buf[off] ^ k) if size == 1 else ((buf[off] | (buf[off + 1] << 8)) ^ v80) & 0xFFFF
        off += size
    out["headerLen"] = off
    return out


async def oicq_unpad(padded):
    if not padded or len(padded) < 8:
        return None
    if not all(padded[-1 - i] == 0 for i in range(7)):
        return None
    pad_len = padded[0] & 0x07
    s = 3 + pad_len
    e = len(padded) - 7
    return padded[s:e] if s < e else b""


async def decode_packet(packet, key, mask=None):
    data = bytes(packet) if isinstance(packet, bytes) else bytes.fromhex(packet)
    if len(data) < 8:
        return None
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    crc_ok = (data[1] & 0x7F) == await crc7_buff(0, data[2:])
    candidates = []
    if mask:
        ru, nr = await layouts_from_mask(mask)
        layouts = [("RUDP", ru), ("nonRUDP", nr)]
    else:
        layouts = [("RUDP", list(p)) for p in itertools.permutations([0, 1, 2, 3, 4])]
        layouts += [("nonRUDP", list(p)) for p in itertools.permutations([0, 1, 3, 4])]
    for kind, layout in layouts:
        f = await try_header(data, layout, k, v80)
        if not f:
            continue
        if f["flags"] > 7 or f["sendOption"] > 7:
            continue
        if f["length"] != len(data) - f["headerLen"]:
            continue
        body = data[f["headerLen"]:f["headerLen"] + f["length"]]
        content = None
        padded = None
        if f["flags"] & 1:
            if len(body) < 8 or len(body) % 8 != 0:
                continue
            padded = await tea_cbc_decrypt(body, key)
            content = await oicq_unpad(padded)
            if content is None:
                continue
        else:
            content = body
        score = (1 if crc_ok else 0) + (1 if content is not None else 0)
        candidates.append({
            "kind": kind, "layout": layout, "headerLen": f["headerLen"],
            "msgKey": data[0], "cmd": f["cmd"], "flags": f["flags"],
            "sendOption": f["sendOption"], "orderId": f.get("orderId"),
            "length": f["length"], "content": content, "crcOk": crc_ok,
            "padded": padded, "score": score, "total": len(data),
        })
    if not candidates:
        return None
    candidates.sort(key=lambda c: (c["kind"] == "RUDP" or c["kind"] == "nonRUDP", c["score"]), reverse=True)
    return candidates[0]


async def play_game(server_ip_port, thunder, sharma, udp_key, match_code,
                    account_id, player_region, client_version, key, iv,
                    match_index: int):
    match_start_time = time.time()
    ping_task = None
    sock = None
    ping_stop = asyncio.Event()
    uid_str = str(account_id)
    completed_cleanly = False

    try:
        ip, port = server_ip_port.split(":")
        port = int(port)
        resolved_ip = await resolve_host_cloudflare(ip)

        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind(('0.0.0.0', 0))
        except Exception:
            pass
        optimize_udp_socket(sock)
        sock.setblocking(False)

        udp_key_bytes = bytes.fromhex(udp_key)
        hello_packet = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
        await loop.sock_sendto(sock, bytes.fromhex(hello_packet), (resolved_ip, port))
        print(f"Udp Conn Sucessful => {uid_str}")

        ack_state = "waiting_for_hello_reply"
        thunder_sent = False
        sharma_sent = False
        join_match_received = False
        local_closed = False
        send_lock = asyncio.Lock()

        ping_task = asyncio.create_task(
            keepalive_ping(sock, resolved_ip, port, udp_key_bytes, match_code, ping_stop)
        )
        last_activity = time.time()
        MAX_IDLE_BEFORE_HELLO_RESEND = 7.0

        async def send_thunder_sharma_inline():
            nonlocal ack_state, thunder_sent, sharma_sent
            if thunder_sent:
                return
            async with send_lock:
                if thunder_sent:
                    return
                try:
                    await loop.sock_sendto(sock, bytes.fromhex(thunder), (resolved_ip, port))
                    thunder_sent = True
                    await asyncio.sleep(0.3)
                    prepare_ack = await build_packet(
                        0x68, (await layouts_from_mask(match_code))[1],
                        0, 2, None, 1, b"\x01\x00", udp_key_bytes
                    )
                    await loop.sock_sendto(sock, prepare_ack, (resolved_ip, port))
                    await asyncio.sleep(0.4)
                    await loop.sock_sendto(sock, bytes.fromhex(sharma), (resolved_ip, port))
                    sharma_sent = True
                    ack_state = "thunder_sharma_sent"
                except Exception:
                    pass

        while not local_closed:
            if time.time() - match_start_time > MAX_MATCH_DURATION:
                break
            try:
                response, server_addr = await asyncio.wait_for(
                    loop.sock_recvfrom(sock, 65535), timeout=1.5
                )
                if response:
                    last_activity = time.time()
                    frame = await decode_packet(response, udp_key_bytes, match_code)
                    if frame:
                        ptype = await classify(frame)

                        if frame['cmd'] in [103, 107]:
                            completed_cleanly = True
                            local_closed = True
                            continue

                        if frame['cmd'] == 101:
                            try:
                                ack_pkt = await build_packet(
                                    0x68, (await layouts_from_mask(match_code))[1],
                                    0, 2, None, 1, b"\x01\x00", udp_key_bytes
                                )
                                await loop.sock_sendto(sock, ack_pkt, server_addr)
                            except Exception:
                                pass
                            continue

                        if ptype in ["ACK", "PING", "HELLO", "JOIN_MATCH"]:
                            if ptype == "HELLO" and ack_state == "waiting_for_hello_reply":
                                typ, reply = await reply_for(
                                    frame, udp_key_bytes, match_code, ack_style="short"
                                )
                                if reply:
                                    await loop.sock_sendto(sock, reply, server_addr)
                                ack_state = "ack_sent_waiting"
                            elif ptype == "ACK":
                                if ack_state == "waiting_for_hello_reply":
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        await loop.sock_sendto(sock, reply, server_addr)
                                    ack_state = "ready_to_send_thunder"
                                elif ack_state == "ack_sent_waiting":
                                    ack_state = "ready_to_send_thunder"
                                else:
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        await loop.sock_sendto(sock, reply, server_addr)
                            elif ptype == "PING":
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    await loop.sock_sendto(sock, reply, server_addr)
                            elif ptype == "JOIN_MATCH" and not join_match_received:
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    await loop.sock_sendto(sock, reply, server_addr)
                                    join_match_received = True
            except asyncio.TimeoutError:
                if ack_state == "ready_to_send_thunder" and not thunder_sent:
                    await send_thunder_sharma_inline()
                elif ack_state == "waiting_for_hello_reply":
                    if (time.time() - last_activity) > MAX_IDLE_BEFORE_HELLO_RESEND:
                        try:
                            pkt = await build_hello_packet(
                                f"{account_id}_2585", udp_key_bytes, match_code
                            )
                            await loop.sock_sendto(sock, bytes.fromhex(pkt), (resolved_ip, port))
                        except Exception:
                            pass
                        last_activity = time.time()
                    if (time.time() - match_start_time) > 25.0:
                        break
                elif ack_state == "thunder_sharma_sent":
                    if (time.time() - last_activity) > MATCH_IDLE_TIMEOUT:
                        completed_cleanly = True
                        break
                continue
            except BlockingIOError:
                await asyncio.sleep(0.05)
            except OSError:
                await asyncio.sleep(0.5)
                continue
            except Exception:
                await asyncio.sleep(0.5)
                continue

            if ack_state == "ready_to_send_thunder" and not thunder_sent:
                await send_thunder_sharma_inline()

        return f"match #{match_index} finished"
    except Exception:
        return f"match #{match_index} error"
    finally:
        if completed_cleanly:
            try:
                bot_state.increment_match(uid_str)
                print(f"Match #{match_index} Complete => {uid_str}")

                async def _post_match_exp_check(uid):
                    try:
                        await asyncio.sleep(1.5)
                        await refresh_account_profile(uid)
                    except Exception:
                        pass
                asyncio.create_task(_post_match_exp_check(uid_str))
            except Exception:
                pass

        ping_stop.set()
        if ping_task:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        remaining = await _dec_match(uid_str)
        try:
            bot_state.update_status(uid_str, "IN_MATCH" if remaining > 0 else "ONLINE", remaining)
        except Exception:
            pass


async def functional_br_parallel(addrs, starter_packet, account_region, client_version,
                                  key, iv, account_id="", account_data=None,
                                  max_reconnects=10):
    reconnects = 0
    ip, port = addrs.split(":")
    play_matches: List[asyncio.Task] = []
    no_response_count = 0
    search_attempts = 0
    last_start_time = 0.0
    uid_str = str(account_id)

    current_token = starter_packet
    current_key = key
    current_iv = iv
    current_account_data = account_data
    tcp_printed = False

    try:
        while True:
            while bot_state.is_paused(uid_str):
                try:
                    bot_state.update_status(uid_str, "PAUSED", 0)
                except Exception:
                    pass
                await asyncio.sleep(1.0)

            while True:
                play_matches[:] = [m for m in play_matches if not m.done()]
                if len(play_matches) < MAX_CONCURRENT_MATCHES:
                    break
                await asyncio.sleep(0.5)

            writer = None
            reader = None
            gateway_ping_task = None

            try:
                if current_account_data:
                    fresh = None
                    if current_account_data.get('auth_type') == 'guest' and current_account_data.get('auth_uid'):
                        fresh = cache_get(str(current_account_data['auth_uid']))
                    elif current_account_data.get('auth_type') == 'token' and current_account_data.get('auth_token'):
                        fresh = cache_get(f"tok_{current_account_data['auth_token'][:20]}")

                    if fresh:
                        current_account_data = fresh
                        current_key = fresh['aes_ak']
                        current_iv = fresh['iv_i']
                        current_token = await build_tcp_startup_packet(
                            fresh['account_id'],
                            fresh['token'],
                            fresh['server_time'],
                            current_key,
                            current_iv,
                            region=fresh.get('region', account_region),
                            typ='OnLine'
                        )
                    else:
                        try:
                            if current_account_data.get('auth_uid'):
                                cache_invalidate(str(current_account_data['auth_uid']))
                            if current_account_data.get('auth_token'):
                                cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                        except Exception:
                            pass
                        raise ConnectionError("Cache expired, triggering fresh login")

                resolved_ip = await resolve_host_cloudflare(ip)
                reader, writer = await asyncio.open_connection(resolved_ip, int(port))
                bot_state.register_writer(uid_str, writer)

                raw_sock = writer.get_extra_info('socket')
                if raw_sock:
                    optimize_tcp_socket(raw_sock)

                writer.write(bytes.fromhex(current_token))
                await writer.drain()

                try:
                    init_ka = await send_keep_alive(account_region)
                    if init_ka and writer and not writer.is_closing():
                        writer.write(init_ka)
                        await asyncio.wait_for(writer.drain(), timeout=3)
                except Exception:
                    pass

                async def func_gateway_keepalive():
                    ka_bytes = await send_keep_alive(account_region)
                    while True:
                        await asyncio.sleep(5)
                        try:
                            if writer and not writer.is_closing():
                                writer.write(ka_bytes)
                                await writer.drain()
                        except Exception:
                            break

                gateway_ping_task = asyncio.create_task(func_gateway_keepalive())

                if not tcp_printed:
                    print(f"Tcp Conn Sucessful => {uid_str}")
                    tcp_printed = True

                reconnects = 0
                no_response_count = 0
                last_start_time = 0.0

                async def send_start_match():
                    nonlocal search_attempts, last_start_time
                    search_attempts += 1
                    current_region = "BD"
                    try:
                        await asyncio.sleep(random.uniform(0.2, 0.4))
                        await start_game_battle_royale(
                            current_region, client_version, writer,
                            current_key, current_iv
                        )
                        active = await _get_match_count(uid_str)
                        try:
                            bot_state.update_status(uid_str, "SEARCHING (BR)", active)
                        except Exception:
                            pass
                    except Exception:
                        pass
                    last_start_time = asyncio.get_running_loop().time()

                play_matches[:] = [m for m in play_matches if not m.done()]
                if len(play_matches) < MAX_CONCURRENT_MATCHES:
                    await send_start_match()

                while True:
                    play_matches[:] = [m for m in play_matches if not m.done()]

                    if bot_state.is_paused(uid_str):
                        try:
                            bot_state.update_status(uid_str, "PAUSED", 0)
                        except Exception:
                            pass
                        bot_state.unregister_writer(uid_str, writer)
                        if gateway_ping_task:
                            gateway_ping_task.cancel()
                        await safe_close_writer(writer)
                        writer = None
                        reader = None
                        while bot_state.is_paused(uid_str):
                            await asyncio.sleep(1.0)
                        break

                    active_count = await _get_match_count(uid_str)
                    has_active_match = any(not m.done() for m in play_matches)
                    current_status = "IN_MATCH (BR)" if (active_count > 0 or has_active_match) else "ONLINE (BR)"
                    try:
                        bot_state.update_status(uid_str, current_status, active_count)
                    except Exception:
                        pass

                    now = asyncio.get_running_loop().time()
                    play_matches[:] = [m for m in play_matches if not m.done()]

                    if len(play_matches) < MAX_CONCURRENT_MATCHES and (now - last_start_time >= START_MATCH_INTERVAL):
                        await send_start_match()

                    try:
                        data = await asyncio.wait_for(reader.read(8192), timeout=0.5)
                    except asyncio.TimeoutError:
                        no_response_count += 1
                        if no_response_count > 60:
                            no_response_count = 0
                        continue

                    if not data:
                        raise ConnectionError("Connection closed by server")

                    hex_data = data.hex()
                    packet_length = len(data)
                    no_response_count = 0

                    if hex_data.startswith("0300") and 10 < packet_length < 30:
                        try:
                            bot_state.update_status(uid_str, "SEARCHING (BR)", 0)
                        except Exception:
                            pass
                        continue

                    if hex_data.startswith("0300") and packet_length >= 300:
                        print(f"Match Found For Uid => {uid_str}")

                        try:
                            res = json.loads(await decode_protobuf(hex_data[10:]))
                            token = None
                            udp_key = None
                            match_code = None
                            server_ip_port = None
                            match_account_id = None
                            block_val = None

                            if '42' in res and 'data' in res['42']:
                                match_code = res['42']['data']
                            if '5' in res and 'data' in res['5']:
                                res_field5 = res['5']['data']
                                server_ip_port = res_field5.get('2', {}).get('data')
                                udp_key = res_field5.get('3', {}).get('data')
                                token = res_field5.get('4', {}).get('data')
                                if '42' in res_field5:
                                    match_code = res_field5['42']['data']
                            if '1' in res and 'data' in res['1']:
                                match_account_id = res['1']['data']
                            if '5' in res and 'data' in res['5']:
                                block_val = res['5']['data'].get('1', {}).get('data')

                            effective_acc_id = match_account_id or account_id or "BD_BOT"

                            if token and udp_key and match_code and server_ip_port:
                                acc_tok = ""
                                if current_account_data:
                                    acc_tok = current_account_data.get('access_token', '') or ""
                                thunder, sharma = await build_match_startup_packets(
                                    token, udp_key, match_code, effective_acc_id, block_val or 0,
                                    server_ip=server_ip_port,
                                    region=account_region,
                                    client_version=client_version,
                                    access_token=acc_tok,
                                    mode="BR"
                                )

                                match_index = await _inc_match(uid_str)

                                new_match = asyncio.create_task(
                                    play_game(
                                        server_ip_port,
                                        thunder,
                                        sharma,
                                        udp_key,
                                        match_code,
                                        effective_acc_id,
                                        "BD",
                                        client_version,
                                        current_key,
                                        current_iv,
                                        match_index=match_index
                                    )
                                )
                                play_matches.append(new_match)

                                if gateway_ping_task:
                                    gateway_ping_task.cancel()
                                bot_state.unregister_writer(uid_str, writer)
                                await safe_close_writer(writer)
                                writer = None
                                reader = None

                                await asyncio.sleep(NEW_MATCH_DELAY)
                                reconnects = 0
                                break

                            else:
                                continue

                        except Exception:
                            continue

                    if 30 <= packet_length <= 40:
                        continue

            except asyncio.CancelledError:
                if gateway_ping_task:
                    gateway_ping_task.cancel()
                if writer:
                    bot_state.unregister_writer(uid_str, writer)
                raise
            except Exception as e:
                if gateway_ping_task:
                    gateway_ping_task.cancel()
                if writer:
                    bot_state.unregister_writer(uid_str, writer)
                play_matches[:] = [m for m in play_matches if not m.done()]
                await safe_close_writer(writer)
                writer = None
                reader = None

                if "Cache expired" in str(e):
                    break

                reconnects += 1
                if reconnects > max_reconnects:
                    if current_account_data:
                        try:
                            if current_account_data.get('auth_uid'):
                                cache_invalidate(str(current_account_data['auth_uid']))
                            if current_account_data.get('auth_token'):
                                cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                        except Exception:
                            pass
                    reconnects = 0
                    break

                await asyncio.sleep(min(reconnects * 0.5, 2.0))
            finally:
                if gateway_ping_task:
                    gateway_ping_task.cancel()
                if writer:
                    bot_state.unregister_writer(uid_str, writer)
                    await safe_close_writer(writer)
    except asyncio.CancelledError:
        raise
    finally:
        for m in play_matches:
            if not m.done():
                m.cancel()


async def informational(addrs, starter_packet, key, iv, region="BD", account_id="", max_reconnects=3):
    uid_str = str(account_id)
    reconnects = 0
    ip, port = addrs.split(":")
    while True:
        while uid_str and bot_state.is_paused(uid_str):
            await asyncio.sleep(1.0)

        writer = None
        ping_task = None
        try:
            resolved_ip = await resolve_host_cloudflare(ip)
            reader, writer = await asyncio.open_connection(resolved_ip, int(port))
            if uid_str:
                bot_state.register_writer(uid_str, writer)

            raw_sock = writer.get_extra_info('socket')
            if raw_sock:
                optimize_tcp_socket(raw_sock)

            writer.write(bytes.fromhex(starter_packet))
            await writer.drain()
            reconnects = 0

            try:
                init_ka = await send_keep_alive(region)
                if init_ka and writer and not writer.is_closing():
                    writer.write(init_ka)
                    await asyncio.wait_for(writer.drain(), timeout=3)
            except Exception:
                pass

            async def info_keepalive():
                ka_bytes = await send_keep_alive(region)
                while True:
                    await asyncio.sleep(5)
                    try:
                        if writer and not writer.is_closing():
                            writer.write(ka_bytes)
                            await writer.drain()
                    except Exception:
                        break

            ping_task = asyncio.create_task(info_keepalive())

            while True:
                if uid_str and bot_state.is_paused(uid_str):
                    if ping_task:
                        ping_task.cancel()
                    if uid_str:
                        bot_state.unregister_writer(uid_str, writer)
                    await safe_close_writer(writer)
                    writer = None
                    while bot_state.is_paused(uid_str):
                        await asyncio.sleep(1.0)
                    break

                try:
                    data = await asyncio.wait_for(reader.read(8192), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if not data:
                    raise ConnectionError("Connection closed")
        except asyncio.CancelledError:
            if ping_task:
                ping_task.cancel()
            if uid_str:
                bot_state.unregister_writer(uid_str, writer)
            await safe_close_writer(writer)
            raise
        except Exception:
            if ping_task:
                ping_task.cancel()
            if uid_str:
                bot_state.unregister_writer(uid_str, writer)
            await safe_close_writer(writer)
            reconnects += 1
            if reconnects > max_reconnects:
                await asyncio.sleep(3)
                reconnects = 0
            else:
                await asyncio.sleep(1)


def _register_credentials(account_data: Dict):
    try:
        acc_id = str(account_data['account_id'])
        bot_state.account_credentials[acc_id] = account_data
        if account_data.get('auth_uid'):
            bot_state.account_credentials[str(account_data['auth_uid'])] = account_data
        if account_data.get('auth_token'):
            bot_state.account_credentials[f"tok_{account_data['auth_token'][:20]}"] = account_data
    except Exception:
        pass


async def refresh_account_profile(account_data_or_uid: Any):
    try:
        if isinstance(account_data_or_uid, str):
            uid = str(account_data_or_uid)
            account_data = bot_state.account_credentials.get(uid)
        else:
            account_data = account_data_or_uid
            uid = str(account_data.get('account_id'))

        if not account_data:
            return

        url = account_data.get('server_url')
        token = account_data.get('token')
        release_version = account_data.get('release_version')
        payload = account_data.get('login_payload_data')

        if not (url and token and release_version and payload):
            return

        res = await send_getlogin(payload, url, token, release_version)
        if res:
            res_proto, dict_res = res
            level = int(get_proto_field(dict_res, 6, 1))
            exp = int(get_proto_field(dict_res, 7, 0))
            likes = int(get_proto_field(dict_res, 8, 0))
            nickname = res_proto.nickname or get_proto_field(dict_res, 4, "")

            acc_id = str(account_data['account_id'])
            if exp > 0:
                old_exp = bot_state.accounts.get(acc_id, {}).get("current_exp", 0)
                bot_state.update_exp(acc_id, exp, level)
                if old_exp and exp > old_exp:
                    diff = exp - old_exp
                    acc_state = bot_state.accounts.get(acc_id, {})
                    rem_e = acc_state.get('remaining_exp', 0)
                    nxt_l = acc_state.get('next_level', (level or 1) + 1)
                    pct_val = acc_state.get('progress_pct', 0)
                    print(f"+{diff:,} EXP Gained => {acc_id} | Lvl {acc_state.get('level', level)} ({pct_val}% - {rem_e:,} EXP to Lvl {nxt_l})")
            if likes > 0 and acc_id in bot_state.accounts:
                bot_state.accounts[acc_id]["likes"] = likes
            if nickname and acc_id in bot_state.accounts:
                bot_state.accounts[acc_id]["nickname"] = nickname

            if level > MAX_LEVEL_ALLOWED:
                print(f"{acc_id} => Reached The Target Level {level}")
                asyncio.create_task(_auto_stop_account(acc_id))
    except Exception:
        pass


async def _auto_stop_account(acc_id: str):
    task = _running_tasks.pop(acc_id, None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    monitor = _exp_monitor_tasks.pop(acc_id, None)
    if monitor:
        monitor.cancel()
        try:
            await monitor
        except asyncio.CancelledError:
            pass
    _account_data_store.pop(acc_id, None)
    print(f"Stopped => {acc_id}")


async def _exp_monitor(acc_id: str):
    while True:
        await asyncio.sleep(30)
        try:
            account_data = _account_data_store.get(acc_id)
            if not account_data:
                break
            await refresh_account_profile(account_data)
            fresh = bot_state.account_credentials.get(acc_id)
            if fresh:
                lvl = fresh.get('level', 1)
                if lvl > MAX_LEVEL_ALLOWED:
                    print(f"{acc_id} => Reached Target Level => {lvl}")
                    await _auto_stop_account(acc_id)
                    break
        except asyncio.CancelledError:
            break
        except Exception:
            pass


async def process_account_uid_pass(uid: str, password: str) -> Optional[Dict]:
    cached = cache_get(uid)
    if cached:
        acc_id = str(cached['account_id'])
        print(f"Acc => {acc_id}")
        bot_state.register_account(
            uid=acc_id,
            nickname=cached.get('nickname', f"Player_{acc_id}"),
            region=cached.get('region', 'BD'),
            level=cached.get('level', 1),
            exp=cached.get('exp', 0),
            likes=cached.get('likes', 0),
            auth_uid=str(uid)
        )
        _register_credentials(cached)
        return cached

    try:
        async with _LOGIN_SEMAPHORE:
            verconfig_res = await version_config()
            if verconfig_res is None:
                return None
            release_version, client_version, server_url = verconfig_res

            tokengrant_response = await get_access_token(uid, password)
            if tokengrant_response is None:
                return None
            open_id, access_token, platform = tokengrant_response

            device_info = get_device_for_account(uid)

            login_payload_data = await build_majorlogin_payload(open_id, access_token, platform, client_version, device_info)
            if login_payload_data is None:
                return None

            majorlogin_response = await send_majorlogin(login_payload_data, release_version, server_url)
            if majorlogin_response is None:
                return None

            getlogin_result = await send_getlogin(login_payload_data, majorlogin_response.url, majorlogin_response.token, release_version)
            if getlogin_result is None:
                return None
            res_proto, dict_res = getlogin_result

        acc_id = str(majorlogin_response.account_id)
        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        likes = int(get_proto_field(dict_res, 8, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, f"Player_{acc_id}")
        region = majorlogin_response.region or get_proto_field(dict_res, 3, "BD")

        print(f"Acc => {acc_id}")

        bot_state.register_account(uid=acc_id, nickname=nickname, region=region, level=level, exp=exp, likes=likes, auth_uid=str(uid))

        account_data = {
            'account_id': majorlogin_response.account_id,
            'nickname': nickname,
            'region': region,
            'level': level,
            'exp': exp,
            'likes': likes,
            'open_id': open_id,
            'access_token': access_token,
            'platform': str(platform),
            'token': majorlogin_response.token,
            'server_time': majorlogin_response.server_time,
            'aes_ak': majorlogin_response.aes_ak,
            'iv_i': majorlogin_response.iv_i,
            'functional_addrs': res_proto.functional_addrs or get_proto_field(dict_res, 14),
            'informational_addrs': res_proto.informational_addrs or get_proto_field(dict_res, 32),
            'release_version': release_version,
            'client_version': client_version,
            'server_url': majorlogin_response.url,
            'login_payload_data': login_payload_data,
            'auth_type': 'guest',
            'auth_uid': uid,
            'auth_password': password
        }
        _register_credentials(account_data)
        cache_set(uid, account_data)
        return account_data
    except Exception:
        return None


async def process_account_token(access_token: str) -> Optional[Dict]:
    cache_key = f"tok_{access_token[:20]}"
    cached = cache_get(cache_key)
    if cached:
        acc_id = str(cached['account_id'])
        print(f"Acc => {acc_id}")
        bot_state.register_account(
            uid=acc_id,
            nickname=cached.get('nickname', f"Player_{acc_id}"),
            region=cached.get('region', 'BD'),
            level=cached.get('level', 1),
            exp=cached.get('exp', 0),
            likes=cached.get('likes', 0),
            token=access_token
        )
        _register_credentials(cached)
        return cached

    try:
        async with _LOGIN_SEMAPHORE:
            verconfig_res = await version_config()
            if verconfig_res is None:
                return None
            release_version, client_version, server_url = verconfig_res

            url = f"https://100067.connect.garena.com/oauth/token/inspect?token={access_token}"
            hdrs = {
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "close",
                "Content-Type": "application/x-www-form-urlencoded",
                "Host": "100067.connect.garena.com",
                "User-Agent": "GarenaMSDK/4.0.19P4(G011A ;Android 13;en;IN;)"
            }
            resp = await client.get(url, headers=hdrs, timeout=10.0)
            if resp.status_code != 200:
                return None
            data = resp.json()

            if 'error' in data:
                return None

            open_id = data.get('open_id')
            platform = data.get('platform', 4)

            if not open_id:
                return None

            device_info = get_device_for_account(open_id)

            login_payload_data = await build_majorlogin_token_payload(open_id, access_token, platform, client_version, device_info)
            if not login_payload_data:
                return None

            majorlogin_response = await send_majorlogin(login_payload_data, release_version, server_url)
            if majorlogin_response is None:
                return None

            getlogin_result = await send_getlogin(
                login_payload_data,
                majorlogin_response.url,
                majorlogin_response.token,
                release_version
            )
            if getlogin_result is None:
                return None

            res_proto, dict_res = getlogin_result

        acc_id = str(majorlogin_response.account_id)
        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        likes = int(get_proto_field(dict_res, 8, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, f"Player_{acc_id}")
        region = majorlogin_response.region or get_proto_field(dict_res, 3, "BD")

        print(f"Acc => {acc_id}")

        bot_state.register_account(uid=acc_id, nickname=nickname, region=region, level=level, exp=exp, likes=likes, token=access_token)

        account_data = {
            'account_id': majorlogin_response.account_id,
            'nickname': nickname,
            'region': region,
            'level': level,
            'exp': exp,
            'likes': likes,
            'open_id': open_id,
            'access_token': access_token,
            'platform': str(platform),
            'token': majorlogin_response.token,
            'server_time': majorlogin_response.server_time,
            'aes_ak': majorlogin_response.aes_ak,
            'iv_i': majorlogin_response.iv_i,
            'functional_addrs': res_proto.functional_addrs or get_proto_field(dict_res, 14),
            'informational_addrs': res_proto.informational_addrs or get_proto_field(dict_res, 32),
            'release_version': release_version,
            'client_version': client_version,
            'server_url': majorlogin_response.url,
            'login_payload_data': login_payload_data,
            'auth_type': 'token',
            'auth_token': access_token
        }
        _register_credentials(account_data)
        cache_set(cache_key, account_data)
        return account_data
    except Exception:
        return None


async def run_account_worker(account_data: Dict, label: str):
    acc_id = str(account_data['account_id'])
    informational_task = None
    exp_task = None
    try:
        reg = account_data.get('region', 'BD')
        tcp_packet_online = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='OnLine'
        )

        tcp_packet_chat = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='ChaT'
        )

        informational_task = asyncio.create_task(
            informational(
                account_data['informational_addrs'],
                tcp_packet_chat,
                account_data['aes_ak'],
                account_data['iv_i'],
                region=reg,
                account_id=acc_id
            )
        )

        async def exp_refresher():
            while True:
                await asyncio.sleep(30)
                fresh = bot_state.account_credentials.get(acc_id)
                if fresh:
                    await refresh_account_profile(fresh)

        exp_task = asyncio.create_task(exp_refresher())

        functional_task = asyncio.create_task(
            functional_br_parallel(
                account_data['functional_addrs'],
                tcp_packet_online,
                account_data['region'],
                account_data['client_version'],
                account_data['aes_ak'],
                account_data['iv_i'],
                account_id=acc_id,
                account_data=account_data
            )
        )

        await functional_task

    except asyncio.CancelledError:
        raise
    except Exception:
        pass
    finally:
        for t in (informational_task, exp_task):
            if t and not t.done():
                t.cancel()
        for t in (informational_task, exp_task):
            if t:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass


async def account_loop_guest(uid: str, password: str):
    uid_str = str(uid)
    bot_state.account_workers[uid_str] = asyncio.current_task()
    acc_id = None
    while True:
        try:
            try:
                bot_state.update_status(uid_str, "CONNECTING")
            except Exception:
                pass
            account_data = await process_account_uid_pass(uid_str, password)
            if not account_data:
                try:
                    bot_state.update_status(uid_str, "ERROR")
                except Exception:
                    pass
                await asyncio.sleep(15)
                continue

            acc_id = str(account_data['account_id'])
            bot_state.account_workers[acc_id] = asyncio.current_task()
            bot_state.account_workers[uid_str] = asyncio.current_task()
            bot_state.auth_to_game_id[uid_str] = acc_id
            bot_state.game_to_auth_id[acc_id] = uid_str

            await run_account_worker(account_data, uid_str)
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            try:
                bot_state.update_status(uid_str, "OFFLINE")
                if acc_id:
                    bot_state.update_status(acc_id, "OFFLINE")
            except Exception:
                pass
            break
        except Exception:
            await asyncio.sleep(10)


async def account_loop_token(token: str):
    tok_key = token[:16]
    while True:
        try:
            account_data = await process_account_token(token)
            if not account_data:
                await asyncio.sleep(15)
                continue

            acc_id = str(account_data['account_id'])
            bot_state.account_workers[acc_id] = asyncio.current_task()
            bot_state.account_workers[tok_key] = asyncio.current_task()
            bot_state.account_token_map[acc_id] = token
            bot_state.account_token_map[tok_key] = acc_id

            await run_account_worker(account_data, acc_id)
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(8)


def load_accounts():
    accounts = []
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    accounts = data
        except Exception:
            pass

    if not accounts and FALLBACK_UID and FALLBACK_PASSWORD:
        accounts.append({"uid": FALLBACK_UID, "password": FALLBACK_PASSWORD})

    return accounts


async def get_current_account_info(account_data: Dict) -> Dict:
    acc_id = str(account_data['account_id'])
    fresh = bot_state.account_credentials.get(acc_id)
    if fresh:
        account_data = fresh
    level = account_data.get('level', 1)
    exp = account_data.get('exp', 0)
    nickname = account_data.get('nickname', '')
    return {
        'exp': exp,
        'level': level,
        'in_game_uid': acc_id,
        'name': nickname
    }


async def run_bot_for_account(account_data: Dict):
    acc_id = str(account_data['account_id'])
    informational_task = None
    exp_task = None
    try:
        reg = account_data.get('region', 'BD')
        tcp_packet_online = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='OnLine'
        )

        tcp_packet_chat = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='ChaT'
        )

        informational_task = asyncio.create_task(
            informational(
                account_data['informational_addrs'],
                tcp_packet_chat,
                account_data['aes_ak'],
                account_data['iv_i'],
                region=reg,
                account_id=acc_id
            )
        )

        async def exp_refresher():
            while True:
                await asyncio.sleep(30)
                fresh = bot_state.account_credentials.get(acc_id)
                if fresh:
                    await refresh_account_profile(fresh)

        exp_task = asyncio.create_task(exp_refresher())

        functional_task = asyncio.create_task(
            functional_br_parallel(
                account_data['functional_addrs'],
                tcp_packet_online,
                account_data['region'],
                account_data['client_version'],
                account_data['aes_ak'],
                account_data['iv_i'],
                account_id=acc_id,
                account_data=account_data
            )
        )

        await functional_task

    except asyncio.CancelledError:
        raise
    except Exception:
        pass
    finally:
        for t in (informational_task, exp_task):
            if t and not t.done():
                t.cancel()
        for t in (informational_task, exp_task):
            if t:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass


async def start_bot_task(account_data: Dict) -> str:
    acc_id = str(account_data['account_id'])
    task = asyncio.create_task(run_bot_for_account(account_data))
    _running_tasks[acc_id] = task
    _account_data_store[acc_id] = account_data
    monitor = asyncio.create_task(_exp_monitor(acc_id))
    _exp_monitor_tasks[acc_id] = monitor
    return acc_id


async def stop_bot_task(acc_id: str):
    task = _running_tasks.pop(acc_id, None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    monitor = _exp_monitor_tasks.pop(acc_id, None)
    if monitor:
        monitor.cancel()
        try:
            await monitor
        except asyncio.CancelledError:
            pass
    _account_data_store.pop(acc_id, None)


def make_json_response(data: dict, status: int = 200) -> web.Response:
    return web.json_response(data, status=status)


def error_response(msg: str) -> web.Response:
    return web.json_response({"Sorry": msg})


async def handle_battleroyal(request: web.Request) -> web.Response:
    uid = request.query.get('uid')
    pw = request.query.get('pw')
    accs = request.query.get('accs')

    if not uid and not accs:
        return error_response("Endpoint Not Registered")

    async with _lock:
        if accs:
            cache_key = f"tok_{accs[:20]}"
            cached = cache_get(cache_key)
            if cached:
                acc_id = str(cached['account_id'])
                level = cached.get('level', 1)
                if level > MAX_LEVEL_ALLOWED:
                    return error_response("Level 3 Or Above Accounts Don't Run")
                if acc_id in _running_tasks and not _running_tasks[acc_id].done():
                    info = await get_current_account_info(cached)
                    start_exp = _start_exp_store.get(acc_id, info['exp'])
                    gained = info['exp'] - start_exp
                    return make_json_response({
                        "Started": "Already Running",
                        "Exp": str(info['exp']),
                        "StartExp": str(start_exp),
                        "GainedExp": str(gained),
                        "Level": str(info['level']),
                        "InGame Uid": str(info['in_game_uid']),
                        "Name": info['name']
                    })
                account_data = cached
                _uid_to_key[acc_id] = acc_id
            else:
                account_data = await process_account_token(accs)
                if not account_data:
                    return error_response("Invalid Token")
                acc_id = str(account_data['account_id'])
                _uid_to_key[acc_id] = acc_id
                level = account_data.get('level', 1)
                if level > MAX_LEVEL_ALLOWED:
                    return error_response("Level 3 Or Above Accounts Don't Run")
        else:
            cached = cache_get(uid)
            if cached:
                acc_id = str(cached['account_id'])
                level = cached.get('level', 1)
                if level > MAX_LEVEL_ALLOWED:
                    return error_response("Level 3 Or Above Accounts Don't Run")
                if acc_id in _running_tasks and not _running_tasks[acc_id].done():
                    info = await get_current_account_info(cached)
                    start_exp = _start_exp_store.get(acc_id, info['exp'])
                    gained = info['exp'] - start_exp
                    return make_json_response({
                        "Started": "Already Running",
                        "Exp": str(info['exp']),
                        "StartExp": str(start_exp),
                        "GainedExp": str(gained),
                        "Level": str(info['level']),
                        "InGame Uid": str(info['in_game_uid']),
                        "Name": info['name']
                    })
                account_data = cached
                _uid_to_key[acc_id] = acc_id
            else:
                account_data = await process_account_uid_pass(uid, pw)
                if not account_data:
                    return error_response("Invalid Credentials")
                acc_id = str(account_data['account_id'])
                _uid_to_key[acc_id] = acc_id
                level = account_data.get('level', 1)
                if level > MAX_LEVEL_ALLOWED:
                    return error_response("Level 3 Or Above Accounts Don't Run")

        info = await get_current_account_info(account_data)
        _start_exp_store[acc_id] = info['exp']
        _start_level_store[acc_id] = info['level']
        _target_level_store[acc_id] = info['level'] + 1

        await start_bot_task(account_data)

        return make_json_response({
            "Started": "True",
            "Exp": str(info['exp']),
            "Level": str(info['level']),
            "InGame Uid": str(info['in_game_uid']),
            "Name": info['name']
        })


async def handle_stop(request: web.Request) -> web.Response:
    uid = request.query.get('uid')
    pw = request.query.get('pw')
    accs = request.query.get('accs')

    if not uid and not accs:
        return error_response("Endpoint Not Registered")

    async with _lock:
        acc_id = None
        if accs:
            cache_key = f"tok_{accs[:20]}"
            cached = cache_get(cache_key)
            if cached:
                acc_id = str(cached['account_id'])
        elif uid:
            cached = cache_get(uid)
            if cached:
                acc_id = str(cached['account_id'])

        if not acc_id or acc_id not in _running_tasks or _running_tasks[acc_id].done():
            return error_response("Not Started")

        account_data = _account_data_store.get(acc_id)
        if not account_data:
            return error_response("Not Started")

        info = await get_current_account_info(account_data)
        start_exp = _start_exp_store.get(acc_id, info['exp'])
        gained = info['exp'] - start_exp

        await stop_bot_task(acc_id)

        return make_json_response({
            "Stopped": "True",
            "Exp": str(info['exp']),
            "Gained": str(gained),
            "Level": str(info['level']),
            "InGame Uid": str(info['in_game_uid']),
            "Name": info['name']
        })


async def handle_status(request: web.Request) -> web.Response:
    uid = request.query.get('uid')
    pw = request.query.get('pw')
    accs = request.query.get('accs')

    if not uid and not accs:
        return error_response("Endpoint Not Registered")

    async with _lock:
        acc_id = None
        if accs:
            cache_key = f"tok_{accs[:20]}"
            cached = cache_get(cache_key)
            if cached:
                acc_id = str(cached['account_id'])
        elif uid:
            cached = cache_get(uid)
            if cached:
                acc_id = str(cached['account_id'])

        if not acc_id or acc_id not in _running_tasks or _running_tasks[acc_id].done():
            return error_response("Not Running")

        account_data = _account_data_store.get(acc_id)
        if not account_data:
            return error_response("Not Running")

        info = await get_current_account_info(account_data)
        start_exp = _start_exp_store.get(acc_id, info['exp'])
        gained = info['exp'] - start_exp
        target = _target_level_store.get(acc_id, info['level'] + 1)

        return make_json_response({
            "Running": "True",
            "Exp": str(info['exp']),
            "Gained": str(gained),
            "Level": str(info['level']),
            "Target": str(target),
            "InGame Uid": str(info['in_game_uid']),
            "Name": info['name']
        })


async def handle_not_found(request: web.Request) -> web.Response:
    return error_response("Endpoint Not Registered")


async def handle_root(request: web.Request) -> web.Response:
    return error_response("Endpoint Not Registered")


async def start_api_server():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_get('/BattelRoyal', handle_battleroyal)
    app.router.add_get('/stop', handle_stop)
    app.router.add_get('/status', handle_status)
    app.router.add_route('*', '/{tail:.*}', handle_not_found)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    print("[i] API server started on port 8080")


async def main():
    print("          SHAPPNO BR         ")

    try:
        await start_web_dashboard(host=WEB_HOST, port=WEB_PORT)
    except Exception:
        pass

    await start_api_server()

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass