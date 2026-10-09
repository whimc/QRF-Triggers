"""
QRF trigger service for the WHIMC Minecraft server.

Every LOOP_SECONDS the service reads recent player activity from the WHIMC MySQL
database (positions, commands, observations, science-tool use, chat, block edits,
deaths, region events), updates per-player state, evaluates every enabled trigger,
and sends each fired trigger to the QRF dispatcher over a WebSocket. Teachers see
those triggers in the QRF app.

Moving parts
------------
* Main loop (``Fetcher.on_wakeup``): queries the DB, runs the state updaters, then
  the triggers, then sends what fired and saves player state.
* Position thread (``Fetcher.update_positions_every_3_seconds``): polls online player
  positions every POSITION_POLL_SECONDS so movement triggers have a finer trail
  than the 10-second main loop.
* Trigger Manager GUI (``launch_trigger_manager``): a separate process that edits
  ``trigger_config.json``. The main loop reloads that file when it changes.

Files
-----
* ``credentials.json``: DB login and QRF WebSocket URL (see credentials.json.template).
* ``trigger_config.json``: enabled / priority / category (+ optional tuning fields)
  per trigger. Keys must match the names passed to ``@trigger``.
* ``WHIMC Coordinate Tracking updated.csv``: points of interest, NPCs and expected
  science tools per world.
* ``BlockBasedTriggers.csv``: thresholds for ``check_block_triggers``.
* ``--saveload`` file: per-player state, so a camp session can be resumed.
* ``logs/``: one log file per run.

Adding a trigger
----------------
Write a ``Fetcher`` method decorated with ``@trigger("name")`` that takes
``(self, cfg)`` and calls ``self.emit(cfg, username, message)``; add ``"name"`` to
``trigger_config.json``; describe it in ``docs/TRIGGERS.md``.

See README.md for setup and docs/TRIGGERS.md for what every trigger does.
"""

# Based on the 28 July 2025 version from Neithan, modified by Geph/Stefan for the
# U Maine QRF camp (28 July - 1 August 2025), reorganized October 2026.

import argparse
import json
import logging
import math
import numbers
import os
import random
import re
import subprocess
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import pytz
import sqlalchemy as db
from shapely.geometry import Point, Polygon
from websockets.sync.client import connect

# =============================================================================
# Constants
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_FILE = BASE_DIR / "credentials.json"
TRIGGER_CONFIG_FILE = BASE_DIR / "trigger_config.json"
WORLD_DATA_FILE = BASE_DIR / "WHIMC Coordinate Tracking updated.csv"
BLOCK_TRIGGERS_FILE = BASE_DIR / "BlockBasedTriggers.csv"
LOG_DIR = BASE_DIR / "logs"

# Player activity is logged in Central time on the WHIMC server.
CENTRAL_TZ = pytz.timezone("America/Chicago")

LOOP_SECONDS = 10
POSITION_POLL_SECONDS = 3

# World names must match the server and the World column of the coordinate CSV.
# These lists used the 2024 names (TiltedEarth_JungleIsland, Mynoa_half, ...) after
# the worlds were renamed, so they silently excluded nothing.
NPC_DISABLED_WORLDS = {
    "LunarCrater", "TiltedWarm", "TiltedFrozen", "TiltedMelting", "MynoaHalf", "BrownDwarf",
}
POI_DISABLED_WORLDS = {
    "LunarCrater", "TiltedWarm", "TiltedFrozen", "TiltedMelting",
    "MynoaClose", "MynoaHalf", "Cancri", "BrownDwarf",
}
# Lower-case; compared case-insensitively because the server reports "Hub", not "hub".
COMMAND_EXCLUDED_WORLDS = {"hub", "earthcontrol", "etlife", "rocketlaunch", "play", "mars"}
# Build worlds from before --wid existed; kept so older camps still match.
LEGACY_BUILD_WORLD_NAMES = {"mars", "sdp7"}

MULTI_USE_TOOLS = ["gravity", "pressure", "atmosphere"]
SINGLE_USE_TOOLS = [
    "rotational_period", "scale", "tectonic", "tides", "year", "tilt", "magnetic_field",
    "tpa", "agent", "pause", "tpall", "gamemode", "difficulty", "op", "kill", "help", "pvp",
    "/sphere", "sphere", "/hsphere", "hsphere",
]
COUNTED_TOOLS = MULTI_USE_TOOLS + SINGLE_USE_TOOLS

# Slash commands (and aliases) that can appear in the Expected_Action column of the CSV.
EXPECTED_ACTION_COMMANDS = [
    "airflow", "wind", "altitude", "height", "atmosphere", "composition",
    "cosmicrays", "gravity", "humidity", "water", "vapor", "magnetic_field",
    "oxygen", "pressure", "air_pressure", "atmosphere_pressure", "radiation",
    "radius", "rotational_period", "daylength", "scale", "tectonic", "seismic",
    "temperature", "temp", "tides", "ocean_level", "tilt", "axial_tilt",
    "year", "orbital_period", "observe",
]

QUESTION_WORDS_RE = re.compile(r"\b(what|when|where|why|who|which|how)\b", re.IGNORECASE)

log = logging.getLogger("qrf")


def clock() -> float:
    """Current Unix time in seconds. tests/simulate_camp.py replaces this to fast-forward time."""
    return time.time()


def to_epoch(ts) -> float:
    """Convert a timestamp from the database to Unix seconds.

    Naive datetimes come from MySQL ``from_unixtime()``, which renders in the DB
    server's zone (Central), so they are localized to Central. pytz zones must be
    applied with ``localize``: ``.replace(tzinfo=CENTRAL_TZ)`` silently uses the 1883
    LMT offset (-5:51), which put tool-use times 51 minutes in the future in summer.
    Numbers above 1e12 are milliseconds.
    """
    if ts is None:
        return clock()
    if isinstance(ts, numbers.Number):  # also numpy ints and the Decimal MySQL returns for time / 1000
        value = float(ts)
        if math.isnan(value):
            return clock()
        return value / 1000.0 if value > 1e12 else value
    if isinstance(ts, str):
        try:
            ts = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return clock()
    if isinstance(ts, pd.Timestamp):
        if pd.isna(ts):
            return clock()
        ts = ts.to_pydatetime()
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = CENTRAL_TZ.localize(ts)
        return ts.timestamp()
    return clock()


def _json_default(value):
    """Make player state JSON-serializable (sets, numpy scalars)."""
    if isinstance(value, set):
        return sorted(value)
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


# =============================================================================
# Logging
# =============================================================================

TRIGGER_COLOR = "\033[93m"


class _ConsoleFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[90m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[91m",
        logging.CRITICAL: "\033[91m",
    }

    def format(self, record):
        message = super().format(record)
        color = getattr(record, "color", None) or self.COLORS.get(record.levelno)
        return f"{color}{message}\033[0m" if color else message


def setup_logging(verbose: bool, log_file: Optional[Path]) -> None:
    """Console gets INFO (DEBUG with --verbose); the log file always gets DEBUG."""
    if os.name == "nt":
        os.system("")  # enables ANSI colors in the Windows console
    if hasattr(sys.stdout, "reconfigure"):
        # Trigger messages contain characters (Δ, “ ”) that cp1252 consoles can't encode.
        sys.stdout.reconfigure(errors="replace")

    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(_ConsoleFormatter("%(asctime)s %(message)s", "%H:%M:%S"))
    log.addHandler(console)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(file_handler)


# =============================================================================
# Trigger configuration (trigger_config.json)
# =============================================================================

@dataclass(frozen=True)
class TriggerSettings:
    """One trigger's entry in trigger_config.json. ``raw`` holds optional tuning fields."""

    name: str
    enabled: bool
    priority: int
    category: str
    raw: dict = field(default_factory=dict)

    def get(self, key, default=None):
        return self.raw.get(key, default)


class TriggerConfig:
    """Cached view of trigger_config.json.

    The file used to be opened and parsed on every lookup (~70 times per loop); it is
    now read once and re-read only when its modification time changes, e.g. after a
    save in the Trigger Manager.
    """

    def __init__(self, path: Path = TRIGGER_CONFIG_FILE):
        self.path = path
        self._data: dict = {}
        self._mtime: Optional[float] = None
        self._warned: set = set()
        self.reload_if_changed()

    def reload_if_changed(self) -> None:
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            if self._mtime is not None or not self._warned:
                log.warning(f"{self.path.name} not found; every trigger is enabled with priority 1.")
                self._warned.add("<missing file>")
            self._data, self._mtime = {}, None
            return
        if mtime == self._mtime:
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                self._data = json.load(f)
            if self._mtime is not None:
                log.info(f"Reloaded {self.path.name}.")
            self._mtime = mtime
        except json.JSONDecodeError as e:
            # The GUI may be mid-write; keep the previous settings and retry next loop.
            log.warning(f"Could not parse {self.path.name} ({e}); keeping previous settings.")

    def settings(self, name: str) -> TriggerSettings:
        entry = self._data.get(name)
        if entry is None:
            # A missing entry used to raise KeyError (no "category") and stop the loop.
            if name not in self._warned:
                log.warning(f"'{name}' is not in {self.path.name}; using enabled, priority 1.")
                self._warned.add(name)
            entry = {}
        return TriggerSettings(
            name=name,
            enabled=bool(entry.get("enabled", True)),
            priority=int(entry.get("priority", 1)),
            category=str(entry.get("category", "Uncategorized")),
            raw=entry,
        )

    def names(self) -> set:
        return set(self._data)


def is_trigger_enabled(name):  # @Luc, @Geph: deprecated, kept on request for debugging.
    return TriggerConfig().settings(name).enabled


# =============================================================================
# Credentials and world data
# =============================================================================

def load_credentials(path: Path = CREDENTIALS_FILE) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_world_coordinates(path: Path = WORLD_DATA_FILE) -> dict:
    """Read the coordinate CSV into ``{world: {object_type: {object_name: details}}}``.

    ``details`` is ``{"x", "z", "range", "Expected Action"}``. ``range`` is a string of
    corner points such as ``"(-68,-47) (-93,-90)"`` (two corners = rectangle).
    ``object_type`` is the CSV Object column (Place, NPCs, Data-based Signals, ...).
    Blank World/Object cells inherit the value from the row above.

    Each world's ``Global`` row (Object_name ``NA``) lists science tools that make sense
    anywhere in that world; it is stored as ``{"Global": {"Global": details}}`` with no
    coordinates. pandas used to read ``NA`` as missing, so the old ``== "NA"`` branch
    never ran and Global rows only worked because they landed under a NaN key.
    """
    data = pd.read_csv(path, keep_default_na=False, na_values=[""])
    data[["World", "Object"]] = data[["World", "Object"]].ffill()

    def clean(value):
        return None if pd.isna(value) else value

    worlds: dict = {}
    for row in data.itertuples(index=False):
        world, object_type, object_name = row.World, row.Object, row.Object_name
        expected = clean(row.Expected_Action)
        details = {
            "x": clean(row.x),
            "z": clean(row.z),
            "range": clean(row.range),
            "Expected Action": expected.split(", ") if expected else [],
        }
        if object_type == "Global":
            details.update(x=None, z=None, range=None)
            worlds.setdefault(world, {})["Global"] = {"Global": details}
        else:
            worlds.setdefault(world, {}).setdefault(object_type, {})[object_name] = details
    return worlds


# Loaded by main() (or a test harness) before the Fetcher runs.
world_coordinates_dictionary: dict = {}


def define_polygon_boundary(range_str):
    """Parse ``"(x,z) (x,z) ..."`` into corner points; two points mean a rectangle."""
    if not range_str:
        return []
    coordinates = [tuple(map(int, c)) for c in re.findall(r"\((-?\d+),\s*(-?\d+)\)", str(range_str))]
    if len(coordinates) == 2:
        (x1, z1), (x2, z2) = coordinates
        coordinates = [(x1, z1), (x1, z2), (x2, z2), (x2, z1)]
    return coordinates


_polygon_cache: dict = {}


def is_point_inside_space(x, z, range_str) -> bool:
    """True if (x, z) is inside the polygon described by ``range_str`` (Rachel Zhou's POI code)."""
    if not range_str or x is None or z is None:
        return False
    polygon = _polygon_cache.get(range_str)
    if polygon is None:
        boundary = define_polygon_boundary(range_str)
        if len(boundary) < 3:
            return False
        polygon = _polygon_cache[range_str] = Polygon(boundary)
    return polygon.contains(Point(x, z))


def is_near_object(x, z, details, radius=10) -> bool:
    """Inside the object's range polygon, or within ``radius`` blocks (Manhattan) of its point."""
    if x is None or z is None:
        return False
    if details.get("range") is not None:
        return is_point_inside_space(x, z, details["range"])
    if details.get("x") is not None and details.get("z") is not None:
        return abs(x - details["x"]) + abs(z - details["z"]) <= radius
    return False


def nearest_object(world, x, z):
    """Closest NPC/POI with a point coordinate in ``world``: (name, Manhattan distance)."""
    best_name, best_distance = None, float("inf")
    for objects in world_coordinates_dictionary.get(world, {}).values():
        for name, details in objects.items():
            if details.get("x") is None or details.get("z") is None:
                continue
            distance = abs(x - details["x"]) + abs(z - details["z"])
            if distance < best_distance:
                best_name, best_distance = name, distance
    return best_name, best_distance


def poi_containing(world, x, z):
    """Name of the first ranged POI (any non-Global object with a polygon) containing (x, z)."""
    for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
        if object_type == "Global":
            continue
        for name, details in objects.items():
            if details.get("range") is not None and is_point_inside_space(x, z, details["range"]):
                return name
    return None


# =============================================================================
# SQL queries
# =============================================================================
# {since}/{since_ms} is the start of the time slice as Unix time. Filtering on the raw
# time column (instead of from_unixtime(time) >= 'date string') lets MySQL use its
# index and avoids converting between this PC's and the DB server's time zones.
# Every placeholder is a number produced by this program, never user input.

GET_ONLINE_PLAYERS = """
select latest_positions.username as online_user
     , pos.world as world
     , pos.x as x
     , pos.z as z
     , latest_pos_time as position_time
from (
    select username, max(time) as latest_pos_time
    from whimc_player_positions
    where time > (unix_timestamp(current_timestamp) - 30)
    group by username
) as latest_positions
left join whimc_player_positions as pos
on latest_positions.username = pos.username and latest_positions.latest_pos_time = pos.time
"""

GET_COMMANDS = """
select c.time as time
     , u.user as username
     , c.message as message
     , w.world as world, c.x as x, c.y as y, c.z as z
from co_command as c
left join co_user as u on c.user = u.rowid
left join co_world as w on c.wid = w.rowid
where c.time >= {since}
"""

# whimc_observations and whimc_sciencetools store milliseconds.
GET_OBSERVATIONS = """
select time / 1000 as time
     , username
     , observation_color_stripped as observation
     , world, x, y, z
from whimc_observations
where time >= {since_ms}
"""

GET_SCIENCE_TOOLS = """
select time / 1000 as time
     , username
     , tool
     , measurement
     , world, x, y, z
from whimc_sciencetools
where time >= {since_ms}
"""

# One chat query (with username and world joined in) replaces co_chat, co_user (the
# whole table, every loop) and co_chat_with_worlds. Rows overlap between loops;
# Fetcher.update_chat_usage de-duplicates them.
GET_RECENT_CHAT = """
select c.time as time
     , c.user as user_id
     , u.user as username
     , c.message as message
     , w.world as world
from co_chat as c
left join co_user as u on c.user = u.rowid
left join co_world as w on c.wid = w.rowid
where c.time > (unix_timestamp(current_timestamp) - {seconds})
"""

# Fetched incrementally (only rows newer than the last rowid seen) into a rolling
# in-memory window; see RollingBlockLog.
GET_BLOCKS_SINCE = """
select b.rowid as rowid
     , b.time as time
     , b.wid as wid
     , b.x as x, b.y as y, b.z as z
     , b.type as type
     , b.action as action
     , b.user as user_id
     , u.user as username
from co_block as b
left join co_user as u on b.user = u.rowid
where b.rowid > {after_rowid}
  and b.time > (unix_timestamp(current_timestamp) - {seconds})
"""

GET_AIRCLICKS = """
select *
from whimc_action_physical
where type = 'AIR CLICK'
  and time > (unix_timestamp(current_timestamp) * 1000) - {window_ms}
order by time desc
"""

GET_VISITS_TO_UNOWNED_REGION = """
select *
from whimc_player_region_events
where time > unix_timestamp(current_timestamp) - 30
order by time desc
"""

# Previously unfiltered: every death ever recorded was re-read every loop.
GET_RECENT_DEATHS = """
select uuid, username, world, x, y, z, time, type
from whimc_action_physical
where type like 'DEATH%'
  and time > (unix_timestamp(current_timestamp) * 1000) - 60000
"""

GET_WORLD_IDS = """
select rowid as wid, world from co_world
"""


# =============================================================================
# QRF dispatcher connection
# =============================================================================

def build_trigger_payload(message: str, username: str, priority: int) -> dict:
    payload = {
        "event": "new_message",
        "data": {
            "from": "software",
            "software": "WHIMC",
            "timestamp": int(clock() * 1000),
            "eventID": "",
            "student": username,
            "trigger": message,
            "priority": priority,
        },
    }
    payload["data"]["masterlogs"] = {
        **payload["data"],
        "reviewer": "",
        "end": "",
        "feedbackTXT": "",
        "feedbackREC": "",
    }
    return payload


class Dispatcher:
    """Sends triggers to the QRF channel over one long-lived WebSocket.

    Each trigger used to open a new connection, wait for the echo and close it, one
    after another. The connection is now reused and reopened only after an error.
    The channel echoes our own messages (notify_self=1) and relays other clients',
    so incoming messages are drained after each send to keep them from piling up.
    """

    def __init__(self, url: Optional[str]):
        self.url = url
        self._ws = None
        self._warned_no_url = False

    def send(self, message: str, username: str, priority: int) -> bool:
        if not self.url:
            if not self._warned_no_url:
                log.error("No qrf_socket_url in credentials.json: triggers are logged but NOT sent.")
                self._warned_no_url = True
            return False
        data = json.dumps(build_trigger_payload(message, username, priority))
        for attempt in (1, 2):
            try:
                if self._ws is None:
                    self._ws = connect(self.url, open_timeout=10)
                self._ws.send(data)
                self._drain()
                return True
            except Exception as e:
                log.warning(f"Sending trigger failed (attempt {attempt}): {e}")
                self.close()
        log.error(f"Dropped trigger for {username}: {message}")
        return False

    def _drain(self) -> None:
        try:
            while True:
                self._ws.recv(timeout=0)
        except TimeoutError:
            pass

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None


# =============================================================================
# Trigger registry
# =============================================================================
# on_wakeup used to call ~50 methods by hand, and every method repeated the same
# "read config, return if disabled, print a skip message" block. Triggers now
# register themselves; on_wakeup looks up each one's settings, skips disabled ones,
# and isolates failures so one broken trigger cannot stop the loop.

STATE_UPDATERS: list = []
TRIGGERS: list = []


def state_updater(fn: Callable) -> Callable:
    """Register a method that updates player state every loop, before any trigger runs."""
    STATE_UPDATERS.append(fn)
    return fn


def trigger(name: str) -> Callable:
    """Register a method as the trigger ``name`` (its key in trigger_config.json).

    The method is called as ``method(self, cfg)`` only when enabled, in registration
    (source) order.
    """
    def register(fn: Callable) -> Callable:
        TRIGGERS.append((name, fn))
        fn.trigger_name = name
        return fn
    return register


_ENGINE = None


def _engine():
    """Create the SQLAlchemy engine on first use (importing src.py no longer needs credentials).

    pool_pre_ping / pool_recycle reconnect after MySQL closes idle connections during
    long camp sessions instead of failing the query.
    """
    global _ENGINE
    if _ENGINE is None:
        creds = load_credentials()
        _ENGINE = db.create_engine(
            db.URL.create("mysql+mysqlconnector", **creds["database"]),
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    return _ENGINE


# =============================================================================
# Fetcher: data, state and the main loop
# =============================================================================

@dataclass(frozen=True)
class Player:
    user: str
    world: str
    x: float
    z: float


class RecentRows:
    """Remembers row keys already processed so overlapping query windows don't double-count.

    Queries deliberately re-read a little history each loop (see Fetcher.fetch_data)
    because a fixed "newer than last loop" cut-off loses rows logged in the same
    second as the previous fetch, or when this PC's clock runs ahead of the DB's.
    """

    def __init__(self, keep_seconds: float):
        self.keep_seconds = keep_seconds
        self._seen: dict = {}

    def new_only(self, df: pd.DataFrame, key_columns: list) -> pd.DataFrame:
        if df.empty:
            return df
        now = clock()
        self._seen = {k: t for k, t in self._seen.items() if now - t <= self.keep_seconds}
        keys = list(df[key_columns].itertuples(index=False, name=None))
        is_new = [k not in self._seen for k in keys]
        for key, new in zip(keys, is_new):
            if new:
                self._seen[key] = now
        return df[is_new].reset_index(drop=True)


class RollingBlockLog:
    """Rolling window of CoreProtect block edits (co_block), fetched incrementally.

    The old code re-downloaded the last 120 s (camp worlds) and the last 600 s (all
    worlds) of co_block every 10 s, and BlockBasedTriggers.csv windows of up to an
    hour were evaluated against only 120 s of data, so they could never be reached.
    Now only rows with a rowid above the last one seen are fetched and kept in
    memory for ``window_seconds``.
    """

    COLUMNS = ["rowid", "time", "wid", "x", "y", "z", "type", "action", "user_id", "username"]

    def __init__(self, window_seconds: int):
        self.window_seconds = int(window_seconds)
        self.last_rowid = 0
        self.df = pd.DataFrame(columns=self.COLUMNS)

    def update(self, query: Callable) -> pd.DataFrame:
        new = query(GET_BLOCKS_SINCE.format(after_rowid=self.last_rowid, seconds=self.window_seconds))
        if not new.empty:
            new["username"] = new["username"].fillna("").astype(str).str.strip()
            self.last_rowid = int(new["rowid"].max())
            self.df = new if self.df.empty else pd.concat([self.df, new], ignore_index=True)
        cutoff = clock() - self.window_seconds
        self.df = self.df[self.df["time"] >= cutoff].reset_index(drop=True)
        return new

    def players_only(self) -> pd.DataFrame:
        """Rows made by players; CoreProtect logs non-player sources as '#tnt', '#fire', ..."""
        df = self.df
        return df[(df["username"] != "") & ~df["username"].str.startswith("#")]


class Fetcher:
    """Polls the database, keeps per-player state in ``tools_usage`` and runs the triggers."""

    # Re-read this much history every loop; RecentRows drops the duplicates.
    QUERY_OVERLAP_SECONDS = 30
    CHAT_WINDOW_SECONDS = 180
    WORLD_ID_REFRESH_SECONDS = 300
    # Keys holding timers that only make sense within one run of the program.
    SESSION_ONLY_KEYS = [
        "npc_interaction_start", "poi_stay_start", "outside_poi_start", "last_afk_time",
        "last_position", "last_poi_visit", "m6_distance", "m6_world", "m6_object",
        "m7_distance", "m7_world", "m7_object", "far_from_crowd_since", "far_from_crowd_duration",
        "in_pause_box",
    ]

    def __init__(self, initial_newer_than: float, saveload_file=None, wids=(), *,
                 query: Optional[Callable] = None, dispatcher: Optional[Dispatcher] = None,
                 config: Optional[TriggerConfig] = None):
        self.newer_than = float(initial_newer_than)
        self.saveload_file = saveload_file
        # --wid replaces the hard-coded GLOBAL_WID list and the GET_WID_FOR_WORLD query
        # (which looked up a fixed world name); it was required but ignored before.
        self.wids = {int(w) for w in wids}
        self.query = query or (lambda sql: pd.read_sql(sql, _engine()))
        self.dispatcher = dispatcher or Dispatcher(None)
        self.config = config or TriggerConfig()
        self.lock = threading.RLock()
        self.start_time = clock()
        self.last_trigger_time = clock()

        self.block_triggers_df = pd.read_csv(BLOCK_TRIGGERS_FILE)
        block_window = max([600, *self.block_triggers_df["Time_Window_High"].tolist()])
        self.blocks = RollingBlockLog(block_window)

        self.players = pd.DataFrame(columns=["online_user", "world", "x", "z", "position_time"])
        self.online: list = []
        self.commands = pd.DataFrame()
        self.observations = pd.DataFrame()
        self.science_tools = pd.DataFrame()
        self.new_chat = pd.DataFrame()
        self.airclicks = pd.DataFrame()
        self.region_events = pd.DataFrame()
        self.deaths = pd.DataFrame()

        self._recent = {name: RecentRows(keep_seconds=600) for name in (
            "commands", "observations", "science_tools", "chat", "region_events", "deaths")}

        self.world_ids: dict = {}
        self._world_ids_loaded_at = float("-inf")

        # Per-loop facts produced by the state updaters for the triggers.
        self.new_tool_uses: list = []
        self.new_observation_entries: list = []
        self.chat_times: dict = defaultdict(list)

        self.fired: list = []
        self.cooldowns: dict = {}
        self.trigger_errors: dict = defaultdict(int)
        self.observations_record: dict = {}
        self.region_stays: dict = {}
        self.unowned_region_already_triggered: set = set()
        self.help_triggered_users: set = set()
        self.world_explorers: dict = defaultdict(list)
        self.pairs_close_since: dict = {}
        self.block_fired_at: dict = {}

        self.tools_usage: dict = self._load_state()

    # ---------------------------------------------------------------- state --

    def _load_state(self) -> dict:
        if not (self.saveload_file and os.path.exists(self.saveload_file)):
            return {}
        with open(self.saveload_file, encoding="utf-8") as f:
            state = json.load(f)
        self.tools_usage = state
        now = clock()
        for user, data in state.items():
            for key in [k for k in data if k in self.SESSION_ONLY_KEYS or k.endswith("_no_engage")]:
                del data[key]
            data["explored_worlds"] = set(data.get("explored_worlds", []))
            data["recent_positions"] = []
            data["mynoa_start_time"] = None
            # A restart shouldn't count the downtime as "20 minutes without observations".
            data["last_observation_time"] = now
            data["last_tool_use_time"] = now
            self._ensure_user(user)
            for world in data["explored_worlds"]:
                self.world_explorers[world].append(user)
        log.info(f"Loaded state for {len(state)} players from '{self.saveload_file}'.")
        return state

    def _ensure_user(self, user: str) -> dict:
        """Return the user's state, adding any missing fields.

        Users used to be created in five places with different fields (the position
        thread set times to 0, so the 20-minute triggers fired at once; on_wakeup set
        npc_interaction_start to None and the NPC trigger then did ``now - None``).
        """
        now = clock()
        data = self.tools_usage.setdefault(user, {})
        data.setdefault("worlds_visited", [])
        data.setdefault("current_world", None)
        data.setdefault("tool_use_count", 0)
        data.setdefault("total_observation_count", 0)
        data.setdefault("world_observation_counts", {})
        data.setdefault("world_tool_counts", {})
        data.setdefault("tool_counts", {})
        data.setdefault("chat_counts", {})
        data.setdefault("last_observation_time", now)
        data.setdefault("last_tool_use_time", now)
        data.setdefault("mynoa_start_time", None)
        data.setdefault("mynoa_trigger_fired", False)
        data.setdefault("recent_positions", [])
        data.setdefault("recent_observations", [])
        data.setdefault("tool_usage_timestamps", [])
        data.setdefault("visited_sides", {})
        if not isinstance(data.get("explored_worlds"), set):
            data["explored_worlds"] = set(data.get("explored_worlds", []))
        return data

    def _note_world(self, data: dict, world) -> None:
        if isinstance(world, str) and world:
            data["current_world"] = world
            if world not in data["worlds_visited"]:
                data["worlds_visited"].append(world)

    def save_tools_usage(self) -> None:
        """Write player state to --saveload once per loop (temp file + rename, so a crash
        mid-write can't leave a truncated save). Previously it was deep-copied and
        rewritten every 3 s from the position thread as well."""
        if not self.saveload_file:
            return
        with self.lock:
            text = json.dumps(self.tools_usage, indent=2, default=_json_default)
        tmp = f"{self.saveload_file}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, self.saveload_file)
        log.debug(f"Progress saved to '{self.saveload_file}'.")

    # ------------------------------------------------------------- fetching --

    def _safe_query(self, name: str, sql: str) -> pd.DataFrame:
        try:
            return self.query(sql)
        except Exception as e:
            log.error(f"Query '{name}' failed: {e}")
            return pd.DataFrame()

    def fetch_data(self) -> None:
        """Run this loop's queries (about 9; the old loop ran 16, including the whole
        co_user table and two diagnostic queries whose results were only printed)."""
        since = int(self.newer_than) - self.QUERY_OVERLAP_SECONDS

        def fresh(name, sql, keys):
            df = self._safe_query(name, sql)
            if df.empty:
                return df
            return self._recent[name].new_only(df, keys)

        self.commands = fresh("commands", GET_COMMANDS.format(since=since),
                              ["time", "username", "message"])
        self.observations = fresh("observations", GET_OBSERVATIONS.format(since_ms=since * 1000),
                                  ["time", "username", "observation"])
        self.science_tools = fresh("science_tools", GET_SCIENCE_TOOLS.format(since_ms=since * 1000),
                                   ["time", "username", "tool"])
        self.new_chat = fresh("chat", GET_RECENT_CHAT.format(seconds=self.CHAT_WINDOW_SECONDS),
                              ["time", "user_id", "message"])
        self.region_events = fresh("region_events", GET_VISITS_TO_UNOWNED_REGION,
                                   ["time", "username", "region", "trigger"])
        self.deaths = fresh("deaths", GET_RECENT_DEATHS, ["time", "username"])

        airclick_cfg = self.config.settings("check_airclick_burst")
        self.airclicks = self._safe_query("airclicks", GET_AIRCLICKS.format(
            window_ms=int(airclick_cfg.get("time_window_ms", 3 * 60 * 1000))))

        try:
            self.blocks.update(self.query)
        except Exception as e:
            log.error(f"Query 'blocks' failed: {e}")

        for name in ("commands", "observations", "science_tools", "new_chat"):
            df = getattr(self, name)
            if not df.empty:
                log.debug(f"{name.upper()}:\n{df}")

    def update_positions_once(self) -> None:
        """Fetch online players and extend each one's movement trail."""
        players = self._safe_query("players", GET_ONLINE_PLAYERS)
        if players.empty and "online_user" not in players.columns:
            players = pd.DataFrame(columns=["online_user", "world", "x", "z", "position_time"])
        with self.lock:
            self.players = players
            for p in self._players_from(players):
                data = self._ensure_user(p.user)
                trail = data["recent_positions"]
                position = (p.x, p.z)
                if not trail or tuple(trail[-1]) != position:
                    trail.append(position)
                    del trail[:-21]  # 21 positions = 20 intervals for the racing trigger

    def update_positions_every_3_seconds(self) -> None:
        """Position thread. The query runs outside the lock so it never blocks the main loop."""
        while True:
            try:
                self.update_positions_once()
            except Exception:
                log.exception("Position update failed")
            time.sleep(POSITION_POLL_SECONDS)

    @staticmethod
    def _players_from(df: pd.DataFrame) -> list:
        players = []
        for row in df.itertuples(index=False):
            if not isinstance(row.online_user, str) or pd.isna(row.x) or pd.isna(row.z):
                continue
            players.append(Player(row.online_user, row.world, float(row.x), float(row.z)))
        return players

    def world_id(self, world: str) -> Optional[int]:
        """co_world rowid for a world name. Loaded once and reloaded (at most every 5
        minutes) when an unknown world shows up. Replaces ``self.get_wid_for_world(world)``,
        which called a DataFrame and always failed, so the build-world observation
        trigger never fired."""
        if world not in self.world_ids and clock() - self._world_ids_loaded_at > self.WORLD_ID_REFRESH_SECONDS:
            df = self._safe_query("world_ids", GET_WORLD_IDS)
            if not df.empty:
                self.world_ids = {str(r.world): int(r.wid) for r in df.itertuples(index=False)}
            self._world_ids_loaded_at = clock()
        return self.world_ids.get(world)

    def is_build_world(self, world) -> bool:
        if not isinstance(world, str):
            return False
        return world.lower() in LEGACY_BUILD_WORLD_NAMES or self.world_id(world) in self.wids

    # ------------------------------------------------------------ the loop --

    def emit(self, cfg: TriggerSettings, user: str, message: str, cooldown: float = 0,
             priority: Optional[int] = None) -> bool:
        """Queue a trigger for sending. With ``cooldown`` (seconds), skip it if this trigger
        already fired for this user within that time. Returns True if queued."""
        if cooldown:
            key = (cfg.name, user)
            if clock() - self.cooldowns.get(key, float("-inf")) < cooldown:
                return False
            self.cooldowns[key] = clock()
        priority = cfg.priority if priority is None else int(priority)
        self.fired.append((f"{message} Category: {cfg.category}", user, priority, cfg.name))
        return True

    def online_states(self):
        """(username, state) for every player online this loop.

        Time-based triggers (no observations in 20 minutes, ...) used to loop over every
        player ever saved, so students who had gone home kept triggering.
        """
        for p in self.online:
            yield p.user, self.tools_usage[p.user]

    def on_wakeup(self) -> list:
        """One loop: fetch, update state, run enabled triggers, send, save. Returns what fired."""
        cycle_start = clock()
        log.debug(f"Wakeup; fetching data since {datetime.fromtimestamp(self.newer_than, CENTRAL_TZ)}")
        self.config.reload_if_changed()
        self.fetch_data()

        with self.lock:
            self.online = self._players_from(self.players)
            for p in self.online:
                self._note_world(self._ensure_user(p.user), p.world)
            self.fired = []
            for updater in STATE_UPDATERS:
                try:
                    updater(self)
                except Exception:
                    log.exception(f"State updater {updater.__name__} failed")
            for name, fn in TRIGGERS:
                cfg = self.config.settings(name)
                if not cfg.enabled:
                    continue
                try:
                    fn(self, cfg)
                except Exception:
                    # One broken trigger used to crash the whole loop.
                    self.trigger_errors[name] += 1
                    log.exception(f"Trigger {name} failed")
            fired = list(self.fired)

        for message, user, priority, name in fired:
            log.info(f"Triggered '{message}' for '{user}' (priority {priority})",
                     extra={"color": TRIGGER_COLOR})
            self.dispatcher.send(message, user, priority)
        if fired:
            self.last_trigger_time = clock()

        try:
            self.save_tools_usage()
        except OSError as e:
            log.error(f"Could not save progress: {e}")
        self.newer_than = cycle_start
        return fired

    # -------------------------------------------------------------- helpers --

    def command_rows(self):
        """This loop's new commands as (user, world, x, z, message, time) tuples."""
        for row in self.commands.itertuples(index=False):
            message = row.message.strip() if isinstance(row.message, str) else ""
            if isinstance(row.username, str) and message:
                yield row.username, row.world, row.x, row.z, message, to_epoch(row.time)

    @staticmethod
    def command_name(message: str) -> str:
        """'/Gravity now' -> 'gravity'; '//sphere stone 5' -> '/sphere'."""
        first = message.split()[0] if message.split() else ""
        return first[1:].lower() if first.startswith("/") else ""

    # =========================================================================
    # State updaters (run every loop before the triggers, even if all are off)
    # =========================================================================

    @state_updater
    def update_tool_usage(self):
        """Count tool commands per player, per world and per tool.

        Fixes from the old version: each command is counted once (a second loop
        re-counted every command, so "used /gravity more than once" fired after one
        use and the single-use trigger, which waited for a count of exactly 1, never
        fired); the command must match exactly (``/tpa`` used to match ``/tpall`` and
        ``/op`` matched ``/opinion``); and the inner loop no longer shadows the outer
        ``user``/``data`` variables.
        """
        self.new_tool_uses = []
        now = clock()
        for user, world, x, z, message, ts in self.command_rows():
            tool = self.command_name(message)
            if tool not in COUNTED_TOOLS:
                continue
            data = self._ensure_user(user)
            self._note_world(data, world)
            if not isinstance(world, str):
                world = data["current_world"] or "unknown"
            data[f"{tool}_{world}"] = data.get(f"{tool}_{world}", 0) + 1
            data[f"tool_count_{world}"] = data.get(f"tool_count_{world}", 0) + 1
            data["tool_use_count"] += 1
            data["world_tool_counts"][world] = data["world_tool_counts"].get(world, 0) + 1
            per_world = data["tool_counts"].setdefault(tool, {})
            per_world[world] = per_world.get(world, 0) + 1
            data["tool_usage_timestamps"].append(ts)
            data["tool_usage_timestamps"] = [t for t in data["tool_usage_timestamps"] if now - t <= 60]
            data["last_tool_use_time"] = max(data["last_tool_use_time"], ts)
            self.new_tool_uses.append((user, tool, world))

    @state_updater
    def update_observation_usage(self):
        """Count observations per player and world and index them by world for the
        nearby-observation trigger."""
        self.new_observation_entries = []
        now = clock()
        for row in self.observations.itertuples(index=False):
            user, world = row.username, row.world
            if not isinstance(user, str):
                continue
            ts = to_epoch(row.time)
            data = self._ensure_user(user)
            self._note_world(data, world)
            if not isinstance(world, str):
                world = data["current_world"] or "unknown"
            data["last_observation_time"] = max(data["last_observation_time"], ts)
            counts = data["world_observation_counts"]
            counts[world] = counts.get(world, 0) + 1
            data["total_observation_count"] += 1
            data["recent_observations"] = [t for t in data["recent_observations"] + [ts] if now - t <= 120]

            text = row.observation if isinstance(row.observation, str) else ""
            entry = (row.x, row.z, user, text)
            record = self.observations_record.setdefault(world, [])
            record.append(entry)
            self.new_observation_entries.append((world, len(record) - 1, entry))

    @state_updater
    def update_chat_usage(self):
        """Record new chat messages (each once; the old chat queries overlapped, so the
        same message was counted on several loops)."""
        keep = max(self.CHAT_WINDOW_SECONDS,
                   60 * self.config.settings("check_high_chat_volume").get("check_high_chat_volume_minutes", 2))
        now = clock()
        for row in self.new_chat.itertuples(index=False):
            if not isinstance(row.username, str):
                continue
            self.chat_times[row.username].append(to_epoch(row.time))
            data = self._ensure_user(row.username)
            world = row.world if isinstance(row.world, str) else data["current_world"]
            if world:
                data["chat_counts"][world] = data["chat_counts"].get(world, 0) + 1
        for user in list(self.chat_times):
            self.chat_times[user] = [t for t in self.chat_times[user] if now - t <= keep]
            if not self.chat_times[user]:
                del self.chat_times[user]

    # =========================================================================
    # Tool triggers
    # =========================================================================

    @trigger("tool_use_in_build_world")
    def check_tool_use_in_build_world(self, cfg):
        """A tool command was used in a camp build world (--wid, or the legacy mars/sdp7 names)."""
        for user, tool, world in self.new_tool_uses:
            if self.is_build_world(world):
                self.emit(cfg, user, f"{user} has used {tool} in {world}.")

    @trigger("check_tool_use_counts")
    def check_tool_use_counts(self, cfg):
        """/gravity more than twice, or any other counted tool more than three times, in one world."""
        for user, data in self.online_states():
            for tool, worlds in data["tool_counts"].items():
                for world, count in worlds.items():
                    limit, words = (2, "twice") if tool == "gravity" else (3, "three times")
                    key = f"{tool}_tool_{world}"
                    if count > limit and not data.get(key):
                        data[key] = True
                        self.emit(cfg, user, f"{user} has used {tool} more than {words} in {world}.")

    @trigger("no_tool_use_by_third_world")
    def check_no_tool_use_by_third_world(self, cfg):
        """Visited 3+ worlds without any tool command (once per current world)."""
        for user, data in self.online_states():
            visited, world = len(data["worlds_visited"]), data["current_world"]
            key = f"not_used_tools_since_third_{world}"
            if visited >= 3 and data["tool_use_count"] == 0 and not data.get(key):
                data[key] = True
                what = "3 worlds" if visited == 3 else f"{visited} worlds"
                self.emit(cfg, user, f"{user} has visited {what} without using any tools.")

    @trigger("check_high_tool_use")
    def check_high_tool_use(self, cfg):
        """More than 10 tool uses in the current world while within the first 3 worlds,
        or more than 5 after that (once per world)."""
        for user, data in self.online_states():
            world = data["current_world"]
            count = data.get(f"tool_count_{world}", 0)
            early = len(data["worlds_visited"]) <= 3
            key = f"high_use_{world}"
            if not data.get(key) and count > (10 if early else 5):
                data[key] = True
                where = "the first three worlds" if early else "subsequent worlds"
                self.emit(cfg, user, f"{user} has high tool use in {where}: {world}.")

    @trigger("check_combined_multi_use_tools")
    def check_combined_multi_use_tools(self, cfg):
        """/gravity, /pressure or /atmosphere used at least twice in the current world;
        a separate message once all three have been."""
        for user, data in self.online_states():
            world = data["current_world"]
            combined_key = f"combined_{world}_flag"
            if not world or data.get(combined_key):
                continue
            all_twice = True
            for tool in MULTI_USE_TOOLS:
                count = data.get(f"{tool}_{world}", 0)
                flag = f"{tool}_{world}_flag"
                if count < 2:
                    all_twice = False
                elif not data.get(flag):
                    data[flag] = 1
                    self.emit(cfg, user, f"{user} has used '/{tool}' more than once in {world}.")
            if all_twice:
                data[combined_key] = 1
                self.emit(cfg, user, f"Combined use of pressure, gravity, & atmosphere in {world} more than once.")

    @trigger("check_single_use_tools")
    def check_single_use_tools(self, cfg):
        """First use of each single-use tool/command in the current world."""
        for user, data in self.online_states():
            world = data["current_world"]
            for tool in SINGLE_USE_TOOLS:
                flag = f"{tool}_{world}_flag"
                if data.get(f"{tool}_{world}", 0) >= 1 and not data.get(flag):
                    data[flag] = 1
                    self.emit(cfg, user, f"{user} has used '/{tool}' in {world}.")

    @trigger("check_3_tools_in_1_minute")
    def check_3_tools_in_1_minute(self, cfg):
        """3+ counted tool commands within 60 seconds."""
        now = clock()
        for user, data in self.online_states():
            recent = [t for t in data["tool_usage_timestamps"] if now - t <= 60]
            if len(recent) >= 3:
                data["tool_usage_timestamps"] = []
                self.emit(cfg, user, f"{user} has used at least 3 tools in less than a minute.")
            else:
                data["tool_usage_timestamps"] = recent

    @trigger("check_last_tool_use_over_20_minutes")
    def check_last_tool_use_over_20_minutes(self, cfg):
        """No counted tool command for 20 minutes (repeats at most every 20 minutes).

        Had to share its cooldown with the 20-minute observation trigger, so only one
        of the two could ever fire for a player; each now has its own.
        """
        now = clock()
        for user, data in self.online_states():
            if now - data["last_tool_use_time"] > 20 * 60:
                self.emit(cfg, user, f"{user} has not used any tools in the last 20 minutes.", cooldown=20 * 60)

    @trigger("check_use_basic_science_tools")
    def check_use_basic_science_tools(self, cfg):
        """Used a basic science tool (/gravity, /temperature, /humidity, /oxygen, /wind);
        at most every 10 minutes per player."""
        basic_tools = {"gravity", "temperature", "humidity", "oxygen", "wind"}
        for user, world, x, z, message, ts in self.command_rows():
            tool = self.command_name(message)
            if tool in basic_tools:
                self.emit(cfg, user, f"{user} used a basic science tool ({tool}).", cooldown=600)

    @trigger("check_appropriate_tool_use_near_poi")
    def check_appropriate_tool_use_near_poi(self, cfg):
        """Used a tool listed in a nearby NPC/POI's Expected_Action (inside its range, or
        within 10 blocks). One trigger per command; it used to fire once per matching
        object type."""
        for user, world, x, z, message, ts in self.command_rows():
            used_tool = message.split()[0].lower()
            match = self._nearby_object_expecting(world, x, z, used_tool)
            if match:
                self.emit(cfg, user, f"{user} used an appropriate tool {used_tool} near {match} "
                                     f"expecting the use of {used_tool}.")

    @trigger("tool_use_near_expected_action")
    def check_tool_use_near_expected_action(self, cfg):
        """Like check_appropriate_tool_use_near_poi but only for science-tool commands, and
        falls back to the world's Global expected tools. One trigger per command (it used
        to fire once per matching object)."""
        for user, world, x, z, message, ts in self.command_rows():
            tool = self.command_name(message)
            if tool not in EXPECTED_ACTION_COMMANDS:
                continue
            match = self._nearby_object_expecting(world, x, z, f"/{tool}")
            if match:
                self.emit(cfg, user, f"{user} used tool {tool} near {match} in {world}.")
                continue
            global_details = world_coordinates_dictionary.get(world, {}).get("Global", {}).get("Global")
            if global_details and f"/{tool}" in [a.lower() for a in global_details["Expected Action"]]:
                self.emit(cfg, user, f"{user} used tool {tool} in {world} (Global action).")

    def _nearby_object_expecting(self, world, x, z, used_tool):
        if pd.isna(x) or pd.isna(z):
            return None
        for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
            if object_type == "Global":
                continue
            for name, details in objects.items():
                expected = [a.lower() for a in details["Expected Action"]]
                if used_tool in expected and is_near_object(x, z, details):
                    return name
        return None

    @trigger("check_advanced_tool_use")
    def check_advanced_tool_use(self, cfg):
        """Used an advanced tool (cosmic rays, altitude, scale, year, radius)."""
        advanced = {"COSMIC RAYS", "ALTITUDE", "SCALE", "YEAR", "RADIUS"}
        for row in self.science_tools.itertuples(index=False):
            tool = str(row.tool).upper()
            if tool in advanced:
                self.emit(cfg, row.username, f"{row.username} used advanced tool {tool} in {row.world}.")

    @trigger("check_advanced_tool_use2")
    def check_advanced_tool_use2(self, cfg):
        """Used an advanced tool that is taught in camp (pressure, tides, tilt, ...)."""
        advanced = {"PRESSURE", "TIDES", "TILT", "TECTONIC", "MAGNETIC_FIELD",
                    "RADIATION", "ATMOSPHERE", "ROTATIONAL_PERIOD", "DAYLENGTH", "AIRFLOW"}
        for row in self.science_tools.itertuples(index=False):
            tool = str(row.tool).upper()
            if tool in advanced:
                self.emit(cfg, row.username, f"{row.username} used advanced taught tool {tool} in {row.world}.")

    def _tool_inspiration(self, cfg, same_tool: bool):
        # Only compares tool uses inside one loop's 10-second slice (see docs/TRIGGERS.md).
        df = self.science_tools
        if len(df) < 2:
            return
        rows = list(df.sort_values(by="time").itertuples(index=False))
        for i, current in enumerate(rows):
            for previous in rows[:i]:
                if (current.username != previous.username and current.world == previous.world
                        and (current.tool == previous.tool) == same_tool):
                    if same_tool:
                        message = (f"{current.username} used {current.tool} after {previous.username} "
                                   f"used the same tool.")
                    else:
                        message = (f"{current.username} used {current.tool} after {previous.username} "
                                   f"used {previous.tool} in {current.world}.")
                    self.emit(cfg, current.username, message)
                    break

    @trigger("check_tool_inspiration_specific")
    def check_tool_inspiration_specific(self, cfg):
        """Used the same tool another player in the same world just used."""
        self._tool_inspiration(cfg, same_tool=True)

    @trigger("check_tool_inspiration_generic")
    def check_tool_inspiration_generic(self, cfg):
        """Used a different tool right after another player in the same world used one."""
        self._tool_inspiration(cfg, same_tool=False)

    @trigger("check_five_or_more_tools_in_world")
    def check_five_or_more_tools_in_world(self, cfg):
        """5+ counted tool uses in a world (once per world)."""
        for user, data in self.online_states():
            for world, count in data["world_tool_counts"].items():
                key = f"five_tools_{world}"
                if count >= 5 and not data.get(key):
                    data[key] = True
                    self.emit(cfg, user, f"{user} has used 5 or more tools in {world}.")

    # =========================================================================
    # Observation triggers
    # =========================================================================

    @trigger("observation_in_build_world")
    def check_observation_in_build_world(self, cfg):
        """Made an observation in a camp build world (--wid, or the legacy mars/sdp7 names)."""
        for world, _, (x, z, user, text) in self.new_observation_entries:
            if self.is_build_world(world):
                self.emit(cfg, user, f"{user} made an observation in {world}.")

    @trigger("check_nearby_similar_observation")
    def check_nearby_similar_observation(self, cfg):
        """New observation within 10 blocks (Manhattan) of an earlier one in the same world.
        The text similarity is reported but not required."""
        for world, index, (x, z, user, text) in self.new_observation_entries:
            if pd.isna(x) or pd.isna(z):
                continue
            for other_x, other_z, other_user, other_text in self.observations_record[world][:index]:
                if pd.isna(other_x) or pd.isna(other_z):
                    continue
                if 0 < abs(x - other_x) + abs(z - other_z) < 10:
                    similarity = SequenceMatcher(None, text, other_text).ratio()
                    self.emit(cfg, user, f"{user} made an observation near another observation in {world}. "
                                         f"Similarity: {similarity:.2f}.")
                    break

    @trigger("observation_near_poi")
    def check_observation_near_poi(self, cfg):
        """Observation inside an NPC/POI's range or within 10 blocks of it. Reports the
        nearby object whose name is most similar to the observation; it used to send one
        trigger for every nearby object."""
        for world, _, (x, z, user, text) in self.new_observation_entries:
            if pd.isna(x) or pd.isna(z):
                continue
            best = None
            for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                if object_type == "Global":
                    continue
                for name, details in objects.items():
                    if is_near_object(x, z, details):
                        similarity = SequenceMatcher(None, text, str(name)).ratio()
                        if best is None or similarity > best[1]:
                            best = (name, similarity)
            if best:
                self.emit(cfg, user, f"{user} made an observation near {best[0]} in {world}. "
                                     f"Similarity score: {best[1]:.2f}.")

    @trigger("check_question_like_observation")
    def check_question_like_observation(self, cfg):
        """Chat message or observation that looks like a question ('?' or a question word);
        at most once per ``cooldown_ms`` (default 5 minutes) per player.

        Observations were never checked (the code read a 'text' column that doesn't
        exist), and keywords matched inside words ("show", "somewhat").
        """
        cooldown = cfg.get("cooldown_ms", 5 * 60 * 1000) / 1000

        def is_question(text):
            return isinstance(text, str) and ("?" in text or QUESTION_WORDS_RE.search(text) is not None)

        for row in self.new_chat.itertuples(index=False):
            if isinstance(row.username, str) and is_question(row.message):
                self.emit(cfg, row.username, f"{row.username} asked a question in chat: \u201c{row.message}\u201d",
                          cooldown=cooldown)
        for world, _, (x, z, user, text) in self.new_observation_entries:
            if is_question(text):
                self.emit(cfg, user, f"{user} made a question-like observation: \u201c{text}\u201d",
                          cooldown=cooldown)

    @trigger("check_3_observations_in_2_minutes")
    def check_3_observations_in_2_minutes(self, cfg):
        """3+ observations within 2 minutes."""
        now = clock()
        for user, data in self.online_states():
            recent = [t for t in data["recent_observations"] if now - t <= 120]
            if len(recent) >= 3:
                data["recent_observations"] = []
                self.emit(cfg, user, f"{user} has made 3 observations in the last 2 minutes.")

    @trigger("check_no_observations_last_20_minutes")
    def check_no_observations_last_20_minutes(self, cfg):
        """No observation for 20 minutes (repeats at most every 20 minutes)."""
        now = clock()
        for user, data in self.online_states():
            if now - data["last_observation_time"] > 20 * 60:
                self.emit(cfg, user, f"{user} has not made any observations in the last 20 minutes.",
                          cooldown=20 * 60)

    @trigger("check_mynoa_observations")
    def check_mynoa_observations(self, cfg):
        """25+ minutes in a Mynoa world without an observation there (once per visit)."""
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if not str(p.world).startswith("Mynoa"):
                data["mynoa_start_time"] = None
                data["mynoa_trigger_fired"] = False
                continue
            if data["mynoa_start_time"] is None:
                data["mynoa_start_time"] = now
                data["mynoa_trigger_fired"] = False
            elif (now - data["mynoa_start_time"] >= 25 * 60 and not data["mynoa_trigger_fired"]
                  and data["world_observation_counts"].get(p.world, 0) == 0):
                data["mynoa_trigger_fired"] = True
                self.emit(cfg, p.user, f"{p.user} has been in {p.world} for more than 25 minutes "
                                       f"without making an observation.")

    @trigger("no_observations_by_third_world")
    def check_no_observations_by_third_world(self, cfg):
        """Visited 3+ worlds without any observation (once per current world).

        Restored: deleted with the other observation-count triggers on 27 July 2025
        (commit 37acdcd). It used to check observations in the current world only,
        which didn't match its message; it now checks all observations.
        """
        for user, data in self.online_states():
            visited, world = len(data["worlds_visited"]), data["current_world"]
            key = f"no_observations_since_third_{world}"
            if visited >= 3 and data["total_observation_count"] == 0 and not data.get(key):
                data[key] = True
                if visited == 3:
                    message = f"{user} has not made any observations by the third world."
                else:
                    message = f"{user} has visited {visited} worlds without making any observations."
                self.emit(cfg, user, message)

    @trigger("high_observation_count")
    def check_high_observation_count(self, cfg):
        """More than 10 observations in the current world while within the first 3 worlds,
        or more than 5 after that (once per world). Restored (see above)."""
        for user, data in self.online_states():
            world = data["current_world"]
            count = data["world_observation_counts"].get(world, 0)
            early = len(data["worlds_visited"]) <= 3
            key = f"high_observations_{world}"
            if not data.get(key) and count > (10 if early else 5):
                data[key] = True
                if early:
                    message = f"{user} has made more than 10 observations in {world}."
                else:
                    message = f"{user} has made more than 5 observations in {world} after visiting 3 worlds."
                self.emit(cfg, user, message)

    @trigger("reached_5_observations_in_world")
    def check_reached_5_observations_in_world(self, cfg):
        """5 observations in the current world (once per world). Restored (see above); it
        used to share its once-per-world flag with high_observation_count, so whichever
        ran first blocked the other."""
        for user, data in self.online_states():
            world = data["current_world"]
            key = f"reached_5_observations_{world}"
            if data["world_observation_counts"].get(world, 0) >= 5 and not data.get(key):
                data[key] = True
                self.emit(cfg, user, f"{user} has made 5 observations in {world}.")

    @trigger("check_five_or_more_observations_in_world")
    def check_five_or_more_observations_in_world(self, cfg):
        """5+ observations in any world (once per world)."""
        for user, data in self.online_states():
            for world, count in data["world_observation_counts"].items():
                key = f"five_observations_{world}"
                if count >= 5 and not data.get(key):
                    data[key] = True
                    self.emit(cfg, user, f"{user} has made 5 or more observations in {world}.")

    # =========================================================================
    # Chat triggers
    # =========================================================================

    @trigger("check_3_chat_entries_in_1_minute")
    def check_3_chat_entries_in_1_minute(self, cfg):
        """3+ chat messages within 60 seconds (at most once a minute per player; it used to
        repeat every loop while the messages were still in the query window)."""
        now = clock()
        for user, times in self.chat_times.items():
            if sum(1 for t in times if now - t <= 60) >= 3:
                self.emit(cfg, user, f"{user} has made 3 or more chat entries in the last minute.", cooldown=60)

    @trigger("check_high_chat_volume")
    def check_high_chat_volume(self, cfg):
        """``check_high_chat_volume_threshold`` (5) messages within
        ``check_high_chat_volume_minutes`` (2) minutes; at most once per that window.

        It used to see only the last 60 seconds of chat (so the window was really one
        minute) and repeated every loop while the messages stayed in that window.
        """
        threshold = cfg.get("check_high_chat_volume_threshold", 5)
        minutes = cfg.get("check_high_chat_volume_minutes", 2)
        now = clock()
        for user, times in self.chat_times.items():
            count = sum(1 for t in times if now - t <= minutes * 60)
            if count >= threshold:
                self.emit(cfg, user, f"{user} made {count} chat entries in the last {minutes} minutes.",
                          cooldown=minutes * 60)

    @trigger("check_five_chat_messages_in_world")
    def check_five_chat_messages_in_world(self, cfg):
        """5+ chat messages in one world (once per world)."""
        for user, data in self.tools_usage.items():
            for world, count in data.get("chat_counts", {}).items():
                key = f"five_chats_{world}"
                if count >= 5 and not data.get(key):
                    data[key] = True
                    self.emit(cfg, user, f"{user} has sent 5 or more chat messages in {world}.")

    # =========================================================================
    # Movement triggers
    # =========================================================================
    # recent_positions holds (x, z) pairs from the position thread, so the "y-axis"
    # triggers below actually compare x with z. Positions are only added when the
    # player has moved, so the list is the last 20 moves, not the last 60 seconds.

    @trigger("check_racing_non_stopping")
    def check_racing_non_stopping(self, cfg):
        """Fewer than 2 short moves (< 10 blocks) in the last 20 recorded moves."""
        for user, data in self.online_states():
            trail = data["recent_positions"]
            if len(trail) < 20:
                continue
            stops = sum(1 for (x1, z1), (x2, z2) in zip(trail, trail[1:]) if abs(x2 - x1) + abs(z2 - z1) < 10)
            if stops < 2:
                data["recent_positions"] = []
                self.emit(cfg, user, f"{user} has less than 2 stops in the last 20 intervals (racing/non-stopping).")

    def _axis_totals(self, trail):
        dx = sum(abs(b[0] - a[0]) for a, b in zip(trail, trail[1:]))
        dz = sum(abs(b[1] - a[1]) for a, b in zip(trail, trail[1:]))
        return dx, dz

    @trigger("check_high_x_axis_movement")
    def check_high_x_axis_movement(self, cfg):
        """Movement mostly along x: total |dx| >= 3x total |dz| over the last 6+ moves.
        At most every 2 minutes (it repeated every loop)."""
        for user, data in self.online_states():
            if len(data["recent_positions"]) >= 6:
                dx, dz = self._axis_totals(data["recent_positions"])
                ratio = dx / (dz or 0.1)
                if ratio >= 3:
                    self.emit(cfg, user, f"{user} showed high x-axis movement (\u0394x/\u0394y = {ratio:.2f}).",
                              cooldown=120)

    @trigger("check_low_x_axis_movement")
    def check_low_x_axis_movement(self, cfg):
        """Little x movement: |dx|/|dz| < 3 and total |dx| < 15 blocks. At most every 2 minutes."""
        for user, data in self.online_states():
            if len(data["recent_positions"]) >= 6:
                dx, dz = self._axis_totals(data["recent_positions"])
                ratio = dx / dz if dz else float("inf")
                if ratio < 3 and dx < 15:
                    self.emit(cfg, user, f"{user} showed low x-axis movement (\u0394x={dx:.1f}, \u0394y={dz:.1f}, "
                                         f"\u0394x/\u0394y={ratio:.2f}).", cooldown=120)

    @trigger("check_high_y_axis_movement")
    def check_high_y_axis_movement(self, cfg):
        """Movement mostly along "y" (really z): |dz| >= 3x |dx|. At most every 2 minutes."""
        for user, data in self.online_states():
            if len(data["recent_positions"]) >= 6:
                dx, dz = self._axis_totals(data["recent_positions"])
                ratio = dz / (dx or 0.1)
                if ratio >= 3:
                    self.emit(cfg, user, f"{user} showed high y-axis movement (\u0394y/\u0394x = {ratio:.2f}).",
                              cooldown=120)

    @trigger("check_low_y_axis_movement")
    def check_low_y_axis_movement(self, cfg):
        """Little "y" (really z) movement: |dz|/|dx| < 3 and total |dz| < 20. At most every 2 minutes."""
        for user, data in self.online_states():
            if len(data["recent_positions"]) >= 6:
                dx, dz = self._axis_totals(data["recent_positions"])
                ratio = dz / (dx or 0.1)
                if ratio < 3 and dz < 20:
                    self.emit(cfg, user, f"{user} showed low y-axis movement (\u0394y = {dz:.2f}, \u0394x = {dx:.2f}, "
                                         f"\u0394y/\u0394x = {ratio:.2f}).", cooldown=120)

    @trigger("check_dominant_z_axis_movement")
    def check_dominant_z_axis_movement(self, cfg):
        """Z movement compared with x and height. Cannot fire: it needs (x, y, z) positions
        and the position query only records (x, z). Its second rule (dz/dy < 3) would be
        true almost always, so give it a cooldown and a threshold review before adding
        height to the position query."""
        for user, data in self.online_states():
            trail = data["recent_positions"]
            if len(trail) < 6 or not all(len(pos) == 3 for pos in trail):
                continue
            dx = sum(abs(b[0] - a[0]) for a, b in zip(trail, trail[1:])) or 0.1
            dy = sum(abs(b[1] - a[1]) for a, b in zip(trail, trail[1:])) or 0.1
            dz = sum(abs(b[2] - a[2]) for a, b in zip(trail, trail[1:]))
            if dz / dx > 3:
                self.emit(cfg, user, f"{user} showed dominant Z-axis movement: \u0394z/\u0394x = {dz / dx:.2f}.")
            elif dz / dy < 3:
                self.emit(cfg, user, f"{user} showed low Z vs Y movement: \u0394z/\u0394y = {dz / dy:.2f}.")

    @trigger("check_possible_afk_behavior")
    def check_possible_afk_behavior(self, cfg):
        """No movement for 90 seconds. Repeats every further 90 seconds without movement
        (it used to repeat every 30 seconds)."""
        afk_seconds = 90
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if data.get("last_position") != [p.x, p.z]:
                data["last_position"] = [p.x, p.z]
                data["last_afk_time"] = now
                continue
            idle = now - data.setdefault("last_afk_time", now)
            if idle >= afk_seconds:
                data["last_afk_time"] = now
                self.emit(cfg, p.user, f"{p.user} appears to be AFK (no movement for {int(idle)}s).")

    @trigger("check_long_pair_close")
    def check_long_pair_close(self, cfg):
        """Two players in the same world within 35 blocks of each other for 120 seconds
        (sent for the first name alphabetically; repeats every 120 s they stay together).

        The old version added 10 s per loop and never reset when the pair split up, so
        separate short meetings added up to a trigger.
        """
        now = clock()
        players = self.online
        close_now = set()
        for i, a in enumerate(players):
            for b in players[i + 1:]:
                if a.world == b.world and math.hypot(a.x - b.x, a.z - b.z) <= 35:
                    close_now.add(tuple(sorted((a.user, b.user))))
        self.pairs_close_since = {pair: self.pairs_close_since.get(pair, now) for pair in close_now}
        for (first, second), since in self.pairs_close_since.items():
            if now - since >= 120:
                self.pairs_close_since[(first, second)] = now
                self.emit(cfg, first, f"{first} and {second} have been close to each other for more than 120 seconds.")

    @trigger("check_long_far_from_crowd")
    def check_long_far_from_crowd(self, cfg):
        """No other player within 35 blocks in the same world for 120 seconds (resets on
        triggering and when someone comes close)."""
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            alone = all(o.world != p.world or math.hypot(o.x - p.x, o.z - p.z) > 35
                        for o in self.online if o.user != p.user)
            if not alone:
                data.pop("far_from_crowd_since", None)
                continue
            since = data.setdefault("far_from_crowd_since", now)
            if now - since >= 120:
                data["far_from_crowd_since"] = now
                self.emit(cfg, p.user, f"{p.user} has been far from the crowd for more than 120 seconds.")

    @trigger("check_world_exploration")
    def check_world_exploration(self, cfg):
        """Among the first 3 players to visit both the x > 0 and x < 0 halves of a
        split world. "TiltedMelting" was misspelled "TitledEarthMelting", so that world
        never counted."""
        eligible = {"MynoaMangrove", "ColderStrip", "ColderHot", "ColderCold", "TwoMoons",
                    "TwoMoonsLow", "TiltedWarm", "TiltedMelting", "TiltedFrozen"}
        for p in self.online:
            if p.world not in eligible:
                continue
            data = self.tools_usage[p.user]
            sides = data["visited_sides"].setdefault(p.world, {"positive": False, "negative": False})
            if p.x >= 0.1:
                sides["positive"] = True
            elif p.x <= -0.1:
                sides["negative"] = True
            explorers = self.world_explorers[p.world]
            if (sides["positive"] and sides["negative"] and p.world not in data["explored_worlds"]
                    and p.user not in explorers and len(explorers) < 3):
                explorers.append(p.user)
                data["explored_worlds"].add(p.world)
                self.emit(cfg, p.user, f"{p.user} is among the first explorers to visit both sides of {p.world}.")

    @trigger("check_in_pause_box")
    def check_in_pause_box(self, cfg):
        """Arrived in the Hub (pause box). Fires on arrival; it used to fire every 10 s
        for as long as the player stayed there."""
        for p in self.online:
            data = self.tools_usage[p.user]
            in_hub = p.world == "Hub"
            if in_hub and not data.get("in_pause_box"):
                self.emit(cfg, p.user, f"{p.user} is currently in the pause box (world = Hub).")
            data["in_pause_box"] = in_hub

    @trigger("check_prolonged_stop_in_region")
    def check_prolonged_stop_in_region(self, cfg):
        """Stayed 90+ seconds in a WorldGuard region (VISIT event without a LEAVE); once per stay.

        The old version only advanced its timer when a VISIT row was re-read by the
        overlapping 30-second query, so a single VISIT event could never reach 90 s.
        """
        now = clock()
        events = self.region_events
        rows = events.sort_values("time").itertuples(index=False) if not events.empty else []
        for row in rows:
            key = (row.username, row.region)
            if row.trigger == "VISIT":
                self.region_stays.setdefault(key, {"since": to_epoch(row.time), "fired": False})
            elif row.trigger == "LEAVE":
                self.region_stays.pop(key, None)
        online = {p.user for p in self.online}
        for (user, region), stay in list(self.region_stays.items()):
            if user not in online:
                del self.region_stays[(user, region)]
            elif not stay["fired"] and now - stay["since"] >= 90:
                stay["fired"] = True
                self.emit(cfg, user, f"{user} stayed in region '{region}' for over 90s.")

    # =========================================================================
    # NPC and point-of-interest triggers
    # =========================================================================

    def _npcs(self, world):
        for name, details in world_coordinates_dictionary.get(world, {}).get("NPCs", {}).items():
            if details["x"] is not None and details["z"] is not None:
                yield name, details["x"], details["z"]

    @trigger("check_movement_toward_npc_or_poi")
    def check_movement_toward_npc_or_poi(self, cfg):
        """Within 10 blocks of the nearest NPC/POI and getting closer between loops."""
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in NPC_DISABLED_WORLDS:
                continue
            name, distance = nearest_object(p.world, p.x, p.z)
            previous = (data.get("m6_distance"), data.get("m6_world"), data.get("m6_object"))
            if distance > 10:
                for key in ("m6_distance", "m6_world", "m6_object"):
                    data.pop(key, None)
            elif previous[0] is not None and previous[1:] == (p.world, name) and distance < previous[0]:
                for key in ("m6_distance", "m6_world", "m6_object"):
                    data.pop(key, None)
                self.emit(cfg, p.user, f"{p.user} moved *toward* {name} in {p.world}. "
                                       f"Distance changed from {previous[0]} to {distance}.")
            else:
                data.update(m6_distance=distance, m6_world=p.world, m6_object=name)

    @trigger("check_movement_away_from_npc_or_poi")
    def check_movement_away_from_npc_or_poi(self, cfg):
        """Was within 10 blocks of the nearest NPC/POI and has now moved further away."""
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in NPC_DISABLED_WORLDS:
                continue
            name, distance = nearest_object(p.world, p.x, p.z)
            if distance <= 10:
                data.update(m7_distance=distance, m7_world=p.world, m7_object=name)
                continue
            previous = data.get("m7_distance")
            if previous is not None and data.get("m7_world") == p.world and data.get("m7_object") == name:
                if distance > previous:
                    self.emit(cfg, p.user, f"{p.user} moved away from {name} in {p.world}. "
                                           f"Distance changed from {previous} to {distance}.")
                for key in ("m7_distance", "m7_world", "m7_object"):
                    data.pop(key, None)

    @trigger("check_prolonged_interaction_npc")
    def check_prolonged_interaction_npc(self, cfg):
        """Within 4 blocks of an NPC for 60 seconds. Repeats every ~10 s while still there
        (deliberate "breathing time" from the 2024 version; see docs/TRIGGERS.md).

        Crashed whenever a player record had ``npc_interaction_start: None`` (set by
        on_wakeup), because the check was ``"npc_interaction_start" not in data``.
        """
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in NPC_DISABLED_WORLDS:
                continue
            npc = next((name for name, nx, nz in self._npcs(p.world) if abs(p.x - nx) + abs(p.z - nz) < 4), None)
            if npc is None:
                data.pop("npc_interaction_start", None)
                continue
            if data.get("npc_interaction_start") is None:
                data["npc_interaction_start"] = now
            elif now - data["npc_interaction_start"] >= 60:
                data["npc_interaction_start"] = now - 50
                self.emit(cfg, p.user, f"{p.user} has been interacting with NPC {npc} for more than 60 seconds.")

    @trigger("check_multiple_npc_visits")
    def check_multiple_npc_visits(self, cfg):
        """Visited 2+ different NPCs (within 4 blocks) in 5 minutes."""
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in NPC_DISABLED_WORLDS:
                continue
            npc = next((name for name, nx, nz in self._npcs(p.world) if abs(p.x - nx) + abs(p.z - nz) < 4), None)
            if npc is None:
                continue
            visits = data.setdefault("npc_visit_log", [])
            if not any(v["npc"] == npc and now - v["time"] < 10 for v in visits):
                visits.append({"npc": npc, "time": now})
            visits[:] = [v for v in visits if now - v["time"] <= 300]
            unique = {v["npc"] for v in visits}
            if len(unique) >= 2:
                visits[:] = [v for v in visits if v["npc"] not in unique]
                self.emit(cfg, p.user, f"{p.user} visited {len(unique)} different NPCs in the last 5 minutes.")

    @trigger("check_ignores_nearby_npc")
    def check_ignores_nearby_npc(self, cfg):
        """Within 5 blocks of an NPC for 10 seconds. There is no data on actually talking
        to NPCs, so this fires for anyone standing near one, and repeats every loop while
        they stay (see docs/TRIGGERS.md)."""
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in NPC_DISABLED_WORLDS:
                continue
            npc = next((name for name, nx, nz in self._npcs(p.world) if abs(p.x - nx) + abs(p.z - nz) <= 5), None)
            if npc is None:
                for key in [k for k in data if k.startswith("near_") and k.endswith("_no_engage")]:
                    del data[key]
                continue
            key = f"near_{npc}_no_engage"
            if key not in data:
                data[key] = now
            elif now - data[key] >= 10:
                data[key] = now - 5
                self.emit(cfg, p.user, f"{p.user} has been within 5 blocks of NPC {npc} "
                                       f"for 10 seconds but has not interacted.")

    @trigger("check_prolonged_stay_poi")
    def check_prolonged_stay_poi(self, cfg):
        """Inside a POI's range for 90 seconds. Repeats every ~10 s while still inside
        (deliberate "breathing time" from the 2024 version)."""
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in POI_DISABLED_WORLDS:
                continue
            poi = poi_containing(p.world, p.x, p.z)
            if poi is None:
                data.pop("poi_stay_start", None)
                continue
            if data.get("poi_stay_start") is None:
                data["poi_stay_start"] = now
            elif now - data["poi_stay_start"] >= 90:
                data["poi_stay_start"] = now - 80
                self.emit(cfg, p.user, f"{p.user} has been within POI {poi} for more than 90 seconds.")

    @trigger("check_visit_unmarked_pois")
    def check_visit_unmarked_pois(self, cfg):
        """Outside every POI range for 5 minutes; at most every 10 minutes."""
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in POI_DISABLED_WORLDS:
                continue
            if poi_containing(p.world, p.x, p.z) is not None:
                data.pop("outside_poi_start", None)
                continue
            start = data.setdefault("outside_poi_start", now)
            if now - start >= 300 and self.emit(cfg, p.user, f"{p.user} has not visited a point of interest "
                                                             f"in 5 minutes.", cooldown=600):
                data["outside_poi_start"] = now - 80

    @trigger("check_avoiding_poi")
    def check_avoiding_poi(self, cfg):
        """No POI visited for 5 minutes; at most every 5 minutes."""
        now = clock()
        for p in self.online:
            data = self.tools_usage[p.user]
            if p.world in POI_DISABLED_WORLDS:
                continue
            if poi_containing(p.world, p.x, p.z) is not None:
                data["last_poi_visit"] = now
                continue
            outside = now - data.setdefault("last_poi_visit", now)
            if outside >= 300:
                self.emit(cfg, p.user, f"{p.user} has not visited any POI in the last 300 seconds.", cooldown=300)

    # =========================================================================
    # Command triggers
    # =========================================================================
    # These read this loop's new commands. Several used to read separate 10-20 s
    # windows (co_command / co_command_with_worlds), so one command could fire on two
    # loops or fall between them.

    def _outside_excluded_worlds(self, world) -> bool:
        return not (isinstance(world, str) and world.lower() in COMMAND_EXCLUDED_WORLDS)

    @trigger("check_use_of_disabled_mc_commands")
    def check_use_of_disabled_mc_commands(self, cfg):
        """Tried a disabled Minecraft command (/kill, /agent, /gamemode, /op, /summon, /tp,
        /give, /ban, /kick) outside the Hub and other excluded worlds. Matching is now
        exact (/tp no longer matches /tpa) and the world check ignores case ("Hub" was
        not excluded because the list said "hub")."""
        disabled = {"kill", "agent", "gamemode", "op", "summon", "tp", "give", "ban", "kick"}
        for user, world, x, z, message, ts in self.command_rows():
            if self.command_name(message) in disabled and self._outside_excluded_worlds(world):
                self.emit(cfg, user, f"{user} tried '{message}' in world '{world}'.")

    @trigger("check_specific_commands")
    def check_specific_commands(self, cfg):
        """Used one of /kill, /enable pvp, /god, /gamemode, /difficulty, /op, /help,
        /agent chat outside the excluded worlds."""
        watched = ["/kill", "/enable pvp", "/god", "/gamemode", "/difficulty", "/op", "/help", "/agent chat"]
        for user, world, x, z, message, ts in self.command_rows():
            lower = message.lower()
            if any(lower == c or lower.startswith(c + " ") for c in watched) and self._outside_excluded_worlds(world):
                self.emit(cfg, user, f"{user} used the command '{message}' in world '{world}'.")

    @trigger("check_teleporting_to_multiple_players")
    def check_teleporting_to_multiple_players(self, cfg):
        """A /tp or /tpa command naming two or more players."""
        for user, world, x, z, message, ts in self.command_rows():
            parts = message.split()
            if parts[0].lower() in ("/tp", "/tpa") and len(parts) >= 3:
                targets = ", ".join(parts[1:])
                self.emit(cfg, user, f"{user} tried teleporting to multiple players ({targets}) in a single command.")

    @trigger("check_used_help_command")
    def check_used_help_command(self, cfg):
        """Used /help (once per player per run)."""
        for user, world, x, z, message, ts in self.command_rows():
            if message.lower() == "/help" and user not in self.help_triggered_users:
                self.help_triggered_users.add(user)
                self.emit(cfg, user, f"{user} used the /help command.")

    # =========================================================================
    # Block triggers (CoreProtect co_block)
    # =========================================================================
    # Counts only include blocks placed/broken after the trigger last fired for that
    # player, so a trigger doesn't repeat for the same blocks while they stay inside
    # the time window (Luc's 2024 approach, which the 2025 rewrite had dropped).

    BLOCK_MIN_WINDOW_SECONDS = 15  # windows shorter than one loop (5 s rows) missed most edits

    def _camp_blocks(self) -> pd.DataFrame:
        df = self.blocks.players_only()
        return df[df["wid"].isin(self.wids)]

    def _block_burst(self, cfg, actions, limit, description):
        now = clock()
        df = self._camp_blocks()
        df = df[df["action"].isin(actions) & (df["time"] >= now - 120)]
        for user, times in df.groupby("username")["time"]:
            since = self.block_fired_at.get((cfg.name, user), float("-inf"))
            count = int((times > since).sum())
            if count > limit:
                self.block_fired_at[(cfg.name, user)] = now
                self.emit(cfg, user, f"{user} has {description.format(limit=limit)} in the last 2 minutes.")

    # Registered under its trigger_config.json name; during the 31 July 2025 camp it was
    # renamed in code only ("check_over_40_actions_in_2_minutes"), which was missing from
    # the config and would have raised KeyError if re-enabled.
    @trigger("check_over_200_actions_in_2_minutes")
    def check_over_200_actions_in_2_minutes(self, cfg):
        """Over 40 block places + breaks in 2 minutes in the camp worlds (threshold lowered
        from 200 during the 2025 camp; the name kept for the config)."""
        self._block_burst(cfg, [0, 1], 40, "performed over {limit} actions (place/destroy)")

    @trigger("check_over_200_placed_actions_in_2_minutes")
    def check_over_200_placed_actions_in_2_minutes(self, cfg):
        """Over 120 blocks placed in 2 minutes in the camp worlds."""
        self._block_burst(cfg, [1], 120, "placed over {limit} blocks")

    @trigger("check_over_200_destroyed_actions_in_2_minutes")
    def check_over_200_destroyed_actions_in_2_minutes(self, cfg):
        """Over 120 blocks destroyed in 2 minutes in the camp worlds."""
        self._block_burst(cfg, [0], 120, "destroyed over {limit} blocks")

    @trigger("check_block_triggers")
    def check_block_triggers(self, cfg):
        """Per-material thresholds from BlockBasedTriggers.csv in the camp worlds: placed
        (action 1) or broken (action 0) at least High_Threshold blocks of a type within
        Time_Window_High seconds. Priority comes from the CSV row; at most one trigger per
        player every 3 minutes. Rows with High_Threshold -1 are skipped; the
        Low_Threshold columns are not used (see docs/TRIGGERS.md)."""
        now = clock()
        df = self._camp_blocks()
        if df.empty:
            return
        groups = dict(tuple(df.groupby(["type", "action"])))
        fired_users = set()
        for rule in self.block_triggers_df.itertuples(index=False):
            group = groups.get((rule.type, rule.action))
            if group is None or rule.High_Threshold == -1:
                continue
            window = max(int(rule.Time_Window_High), self.BLOCK_MIN_WINDOW_SECONDS)
            recent = group[group["time"] >= now - window]
            for user, times in recent.groupby("username")["time"]:
                key = (user, rule.type, rule.action)
                count = int((times > self.block_fired_at.get(key, float("-inf"))).sum())
                if user in fired_users or count < rule.High_Threshold:
                    continue
                verb = "placed" if rule.action == 1 else "destroyed"
                if self.emit(cfg, user, f"{user} has {verb} {count} {rule.material} blocks "
                                        f"in the last {window} seconds.", cooldown=180, priority=rule.Priority):
                    self.block_fired_at[key] = now
                    fired_users.add(user)

    def _break_runs(self, own_blocks: bool) -> dict:
        """Players who broke 20+ blocks within 30 s in the last 10 minutes (any world),
        counting only blocks they placed themselves (own_blocks) or that others placed."""
        df = self.blocks.players_only()
        df = df[df["time"] >= clock() - 600].sort_values("rowid")
        placed_by, breaks = {}, defaultdict(list)
        for r in df.itertuples(index=False):
            coord = (r.wid, r.x, r.y, r.z)
            if r.action == 1:
                placed_by[coord] = r.username
            elif r.action == 0 and coord in placed_by and (placed_by[coord] == r.username) == own_blocks:
                breaks[r.username].append(r.time)
        runs = {}
        for user, times in breaks.items():
            start = 0
            for end in range(len(times)):
                while times[end] - times[start] > 30:
                    start += 1
                if end - start + 1 >= 20:
                    runs[user] = end - start + 1
                    break
        return runs

    @trigger("check_breaks_own_block")
    def check_breaks_own_block(self, cfg):
        """Broke 20+ of their own placed blocks within 30 seconds; at most every 10 minutes.
        It used to share its "already fired" list with check_block_breaks_by_others, so
        whichever fired first silenced the other for the rest of the run."""
        for user, count in self._break_runs(own_blocks=True).items():
            self.emit(cfg, user, f"{user} broke {count} of their own placed blocks within 30 seconds.", cooldown=600)

    @trigger("check_block_breaks_by_others")
    def check_block_breaks_by_others(self, cfg):
        """Broke 20+ blocks placed by other players within 30 seconds; at most every 10 minutes."""
        for user, count in self._break_runs(own_blocks=False).items():
            self.emit(cfg, user, f"{user} destroyed {count} blocks placed by others within 30 seconds.", cooldown=600)

    # =========================================================================
    # Other triggers
    # =========================================================================

    @trigger("achieves_death")
    def achieves_death(self, cfg):
        """Died, with the cause (from the AIED branch). Each death is reported once; the
        old query re-read every death ever recorded on every loop."""
        for row in self.deaths.itertuples(index=False):
            parts = str(row.type).split(" ", 1)
            cause = parts[1] if len(parts) > 1 else "unspecified"
            when = datetime.fromtimestamp(to_epoch(row.time), CENTRAL_TZ).strftime("%H:%M:%S %Z")
            self.emit(cfg, row.username, f"{row.username} died from {cause} in '{row.world}' "
                                         f"at ({row.x}, {row.y}, {row.z}) \u2014 {when}.")

    @trigger("check_visits_to_unowned_region")
    def check_visits_to_unowned_region(self, cfg):
        """Entered a WorldGuard region they are not a member of (once per player per run)."""
        for row in self.region_events.itertuples(index=False):
            if row.trigger != "VISIT" or row.uuid in self.unowned_region_already_triggered:
                continue
            raw_members = row.region_members if isinstance(row.region_members, str) else ""
            members = [m.strip().lower() for m in raw_members.split(",")]
            if str(row.username).lower() in members:
                continue
            self.unowned_region_already_triggered.add(row.uuid)
            self.emit(cfg, row.username, f"{row.username} visited region '{row.region}'.")

    @trigger("check_airclick_burst")
    def check_airclick_burst(self, cfg):
        """More than ``click_threshold`` (5) air clicks within ``time_window_ms`` (3 minutes).
        At most every 5 minutes per player; the cooldown used to be global, so one
        player's burst silenced everyone, and the query only covered 30 seconds."""
        if self.airclicks.empty:
            return
        threshold = cfg.get("click_threshold", 5)
        window_ms = cfg.get("time_window_ms", 3 * 60 * 1000)
        for user, times in self.airclicks.groupby("username")["time"]:
            if len(times) <= threshold:
                continue
            burst = int((times.max() - times.min()) / 1000)
            span = f"{burst}s" if burst < 60 else f"{burst // 60}m {burst % 60}s"
            latest = datetime.fromtimestamp(times.max() / 1000, CENTRAL_TZ).strftime("%H:%M:%S")
            self.emit(cfg, user, f"{user} made {len(times)} AIR CLICKs in {window_ms // 60000} minutes, "
                                 f"range {span} (latest at {latest}).", cooldown=300)

    # Must stay the last registered trigger: it checks whether anything else fired.
    @trigger("check_random_checkin")
    def check_random_checkin(self, cfg):
        """Random online student, when no trigger has been sent for ``quiet_seconds`` (300).

        Since 2024 it had fired every 5 minutes regardless of other triggers, despite
        being meant for quiet periods: it checked the trigger list after on_wakeup had
        already emptied it.
        """
        quiet = cfg.get("quiet_seconds", 300)
        if self.fired or not self.online or clock() - self.last_trigger_time <= quiet:
            return
        student = random.choice(self.online).user
        self.emit(cfg, student, "Random check-in.")


# =============================================================================
# Trigger Manager GUI
# =============================================================================

def launch_trigger_manager(path: Path = TRIGGER_CONFIG_FILE) -> None:
    """Tk window to enable/disable triggers and set priorities.

    Runs in its own process (``src.py --trigger-manager``): Tk must own the main
    thread of its process, and running it in a background thread could hang or crash.
    Saving only changes "enabled" and "priority"; other fields (category, thresholds)
    are kept. They used to be dropped on every save.
    """
    import tkinter as tk
    from tkinter import ttk

    try:
        with open(path, encoding="utf-8") as f:
            trigger_config = json.load(f)
    except FileNotFoundError:
        trigger_config = {}

    root = tk.Tk()
    root.title("Trigger Manager")
    checkbox_vars, priority_entries = {}, {}

    def save_settings():
        try:
            with open(path, encoding="utf-8") as f:
                current = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            current = trigger_config
        for name, var in checkbox_vars.items():
            try:
                priority = int(priority_entries[name].get())
            except ValueError:
                priority = 1
            entry = dict(current.get(name, trigger_config.get(name, {})))
            entry.update(enabled=bool(var.get()), priority=priority)
            entry.setdefault("category", "Uncategorized")
            current[name] = entry
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=4)
        os.replace(tmp, path)
        status_label.config(text="Settings saved!", foreground="green")
        root.after(3000, lambda: status_label.config(text=""))

    canvas = tk.Canvas(root, height=400)
    scrollbar = ttk.Scrollbar(root, orient="vertical", command=canvas.yview)
    scrollable_frame = ttk.Frame(canvas)
    scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    for name, settings in trigger_config.items():
        frame = ttk.Frame(scrollable_frame)
        frame.pack(fill="x", padx=10, pady=3)
        var = tk.BooleanVar(value=settings.get("enabled", False))
        ttk.Checkbutton(frame, text=name, variable=var).pack(side="left")
        checkbox_vars[name] = var
        ttk.Label(frame, text="Priority:").pack(side="left", padx=(10, 0))
        entry = ttk.Entry(frame, width=5)
        entry.insert(0, str(settings.get("priority", 1)))
        entry.pack(side="left")
        priority_entries[name] = entry

    ttk.Button(root, text="Save", command=save_settings).pack(pady=10)
    status_label = ttk.Label(root, text="")
    status_label.pack()
    root.mainloop()


# =============================================================================
# Command line
# =============================================================================

def _central_time(text: str) -> float:
    try:
        return CENTRAL_TZ.localize(datetime.strptime(text, "%Y-%m-%d %H:%M:%S")).timestamp()
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected 'YYYY-MM-DD hh:mm:ss' (Central time), got {text!r}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="WHIMC QRF trigger service. See README.md.")
    parser.add_argument("--initial-newer-than", type=_central_time, default=None,
                        help="Process activity since this Central time, 'YYYY-MM-DD hh:mm:ss' (default: now).")
    parser.add_argument("--saveload", default=None,
                        help="File to save/load player state (usually the camp name).")
    parser.add_argument("--wid", type=int, nargs="+",
                        help="co_world ID(s) of this camp's build world(s), e.g. --wid 133 134.")
    parser.add_argument("--no-gui", action="store_true", help="Don't open the Trigger Manager window.")
    parser.add_argument("--trigger-manager", action="store_true",
                        help="Only open the Trigger Manager window (used internally).")
    parser.add_argument("--verbose", action="store_true", help="Show debug output in the console.")
    args = parser.parse_args(argv)
    if not args.trigger_manager and not args.wid:
        parser.error("--wid is required")
    return args


def main(argv=None) -> None:
    args = parse_args(argv)
    if args.trigger_manager:
        launch_trigger_manager()
        return

    setup_logging(args.verbose, LOG_DIR / f"qrf-{datetime.now():%Y%m%d-%H%M%S}.log")

    world_coordinates_dictionary.update(load_world_coordinates())
    if world_coordinates_dictionary.get("TwoMoonsLow", {}).get("Global"):
        log.info(f"{WORLD_DATA_FILE.name} imported ({len(world_coordinates_dictionary)} worlds).")
    else:
        log.error(f"Something went wrong importing {WORLD_DATA_FILE.name}.")

    try:
        creds = load_credentials()
    except FileNotFoundError:
        log.critical(f"{CREDENTIALS_FILE.name} not found: copy credentials.json.template and fill it in.")
        sys.exit(1)
    if not creds.get("qrf_socket_url"):
        log.error("credentials.json has no qrf_socket_url: triggers will be logged but NOT sent to QRF.")
    fetcher = Fetcher(
        args.initial_newer_than or clock(),
        args.saveload,
        args.wid,
        dispatcher=Dispatcher(creds.get("qrf_socket_url")),
    )
    log.info(f"Camp build world IDs (--wid): {sorted(fetcher.wids)}")
    unknown = sorted(fetcher.config.names() - {name for name, _ in TRIGGERS})
    if unknown:
        log.warning(f"Entries in {TRIGGER_CONFIG_FILE.name} with no trigger: {', '.join(unknown)}")

    gui = None
    if not args.no_gui:
        gui = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--trigger-manager"])

    fetcher.update_positions_once()
    threading.Thread(target=fetcher.update_positions_every_3_seconds, daemon=True).start()

    try:
        while True:
            started = time.monotonic()
            try:
                fetcher.on_wakeup()
            except Exception:
                log.exception("Loop failed; retrying on the next loop")
            time.sleep(max(0.0, LOOP_SECONDS - (time.monotonic() - started)))
    except KeyboardInterrupt:
        log.info("Stopping!")
    finally:
        fetcher.dispatcher.close()
        if gui is not None and gui.poll() is None:
            gui.terminate()
        try:
            fetcher.save_tools_usage()
            if args.saveload:
                log.info(f"Progress saved to '{args.saveload}'. It is now safe to close this window.")
        except OSError as e:
            log.error(f"Could not save progress: {e}")


if __name__ == "__main__":
    main()
