"""
Simulated camp session for src.py: no database, WebSocket server or real time needed.

A fake database answers the service's SQL queries from scripted player activity, the
clock is fast-forwarded, and a fake dispatcher records what would have been sent.
Each scenario checks which triggers fire (and that they don't repeat).

    python tests/simulate_camp.py            # all scenarios
    python tests/simulate_camp.py -v         # also print every trigger sent

Exit code 0 means every check passed.
"""

import json
import re
import sys
import tempfile
import threading
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import src  # noqa: E402

CAMP_WIDS = [133, 134]
WORLD_IDS = {"Umaine25am": 133, "Umaine25pm": 134, "NoMoon": 20, "RocketLaunch": 21,
             "Hub": 1, "TiltedMelting": 22, "LunarCrater": 23}
START = src.CENTRAL_TZ.localize(pd.Timestamp("2025-07-28 10:00:00").to_pydatetime()).timestamp()


class Sim:
    """Fake clock + fake database."""

    def __init__(self):
        self.now = START
        self.positions = {}          # user -> (world, x, z)
        self.commands, self.observations, self.tools, self.chat = [], [], [], []
        self.blocks, self.deaths, self.airclicks, self.regions = [], [], [], []
        self.next_rowid = 1
        self.user_ids = {}

    # --- scripting helpers ---------------------------------------------------
    def move(self, user, world, x, z):
        self.positions[user] = (world, x, z)

    def leave(self, user):
        self.positions.pop(user, None)

    def command(self, user, message):
        world, x, z = self.positions[user]
        self.commands.append(dict(time=int(self.now), username=user, message=message, world=world, x=x, y=64, z=z))

    def observe(self, user, text):
        world, x, z = self.positions[user]
        self.observations.append(dict(time=int(self.now * 1000), username=user, observation=text,
                                      world=world, x=x, y=64, z=z))

    def say(self, user, message):
        world = self.positions[user][0]
        uid = self.user_ids.setdefault(user, len(self.user_ids) + 1)
        self.chat.append(dict(time=int(self.now), user_id=uid, username=user, message=message, world=world))

    def block(self, user, wid, x, y, z, block_type, action):
        uid = self.user_ids.setdefault(user, len(self.user_ids) + 1)
        self.blocks.append(dict(rowid=self.next_rowid, time=int(self.now), wid=wid, x=x, y=y, z=z,
                                type=block_type, action=action, user_id=uid, username=user))
        self.next_rowid += 1

    def die(self, user, cause):
        world, x, z = self.positions[user]
        self.deaths.append(dict(uuid=f"uuid-{user}", username=user, world=world, x=x, y=64, z=z,
                                time=int(self.now * 1000), type=f"DEATH {cause}"))

    def region_event(self, user, region, event, members=""):
        self.regions.append(dict(username=user, region=region, trigger=event, region_members=members,
                                 uuid=f"uuid-{user}", time=int(self.now)))

    # --- the fake database ------------------------------------------------------
    def query(self, sql: str) -> pd.DataFrame:
        def number(pattern):
            return int(re.search(pattern, sql).group(1))

        now = self.now
        if "whimc_player_positions" in sql:
            rows = [dict(online_user=u, world=w, x=x, z=z, position_time=int(now))
                    for u, (w, x, z) in self.positions.items()]
            return pd.DataFrame(rows, columns=["online_user", "world", "x", "z", "position_time"])
        if "from co_command" in sql:
            since = number(r"c\.time >= (-?\d+)")
            return pd.DataFrame([r for r in self.commands if r["time"] >= since],
                                columns=["time", "username", "message", "world", "x", "y", "z"])
        if "from whimc_observations" in sql:
            since = number(r"time >= (-?\d+)")
            rows = [{**r, "time": r["time"] / 1000} for r in self.observations if r["time"] >= since]
            return pd.DataFrame(rows, columns=["time", "username", "observation", "world", "x", "y", "z"])
        if "from whimc_sciencetools" in sql:
            since = number(r"time >= (-?\d+)")
            rows = [{**r, "time": r["time"] / 1000} for r in self.tools if r["time"] >= since]
            return pd.DataFrame(rows, columns=["time", "username", "tool", "measurement", "world", "x", "y", "z"])
        if "from co_chat" in sql:
            seconds = number(r"current_timestamp\) - (\d+)\)")
            return pd.DataFrame([r for r in self.chat if r["time"] > now - seconds],
                                columns=["time", "user_id", "username", "message", "world"])
        if "from co_block" in sql:
            after, seconds = number(r"b\.rowid > (\d+)"), number(r"current_timestamp\) - (\d+)\)")
            rows = [r for r in self.blocks if r["rowid"] > after and r["time"] > now - seconds]
            return pd.DataFrame(rows, columns=src.RollingBlockLog.COLUMNS)
        if "type like 'DEATH%'" in sql:
            return pd.DataFrame([r for r in self.deaths if r["time"] > now * 1000 - 60000],
                                columns=["uuid", "username", "world", "x", "y", "z", "time", "type"])
        if "'AIR CLICK'" in sql:
            window = number(r"\* 1000\) - (\d+)")
            return pd.DataFrame([r for r in self.airclicks if r["time"] > now * 1000 - window],
                                columns=["uuid", "username", "world", "x", "y", "z", "time", "type"])
        if "whimc_player_region_events" in sql:
            return pd.DataFrame([r for r in self.regions if r["time"] > now - 30],
                                columns=["username", "region", "trigger", "region_members", "uuid", "time"])
        if "from co_world" in sql:
            return pd.DataFrame([dict(wid=w, world=n) for n, w in WORLD_IDS.items()], columns=["wid", "world"])
        raise AssertionError(f"Unexpected query:\n{sql}")


class FakeDispatcher:
    def __init__(self):
        self.sent = []

    def send(self, message, username, priority):
        self.sent.append((message, username, priority))
        return True

    def close(self):
        pass


class Camp:
    """A Fetcher wired to the fake database, with every trigger enabled."""

    def __init__(self, saveload=None, verbose=False, config_overrides=None):
        self.sim = Sim()
        src.clock = lambda: self.sim.now
        self.tmp = Path(tempfile.mkdtemp(prefix="qrf_sim_"))
        config = {name: {"enabled": True, "priority": 1, "category": "Test"} for name, _ in src.TRIGGERS}
        for name, extra in (config_overrides or {}).items():
            config[name].update(extra)
        config_path = self.tmp / "trigger_config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        self.fetcher = src.Fetcher(START, saveload, CAMP_WIDS, query=self.sim.query,
                                   dispatcher=FakeDispatcher(), config=src.TriggerConfig(config_path))
        self.fired = []
        self.verbose = verbose

    def run(self, seconds):
        """Advance time, polling positions every 3 s and running the loop every 10 s."""
        for step in range(int(seconds)):
            self.sim.now += 1
            if int(self.sim.now) % src.POSITION_POLL_SECONDS == 0:
                self.fetcher.update_positions_once()
            if int(self.sim.now) % src.LOOP_SECONDS == 0:
                for message, user, priority, name in self.fetcher.on_wakeup():
                    self.fired.append((name, user, priority, message))
                    if self.verbose:
                        print(f"    [{name}] {user} p{priority}: {message}")

    def count(self, name, user=None):
        return sum(1 for n, u, _, _ in self.fired if n == name and (user is None or u == user))

    def clear(self):
        self.fired = []


FAILURES = []


def check(condition, description):
    print(f"  {'ok  ' if condition else 'FAIL'} {description}")
    if not condition:
        FAILURES.append(description)


# =============================================================================
# Scenarios
# =============================================================================

def scenario_tools(verbose):
    print("Tool use")
    camp = Camp(verbose=verbose)
    sim = camp.sim
    sim.move("alice", "NoMoon", -3, 10)     # inside the Greenhouse range
    sim.move("bob", "Umaine25am", 0, 0)      # camp build world (wid 133)
    camp.run(10)

    sim.command("alice", "/wind")
    camp.run(10)
    check(camp.count("tool_use_near_expected_action", "alice") == 1, "/wind near the Greenhouse -> expected-action trigger")
    check(camp.count("check_appropriate_tool_use_near_poi", "alice") == 1, "/wind near the Greenhouse -> appropriate-tool trigger")
    check(camp.count("check_use_basic_science_tools", "alice") == 1, "/wind is a basic science tool")

    sim.command("alice", "/gravity")
    camp.run(10)
    check(camp.count("check_combined_multi_use_tools", "alice") == 0, "one /gravity is not 'more than once' (was double-counted)")
    sim.command("alice", "/gravity")
    camp.run(10)
    check(camp.count("check_combined_multi_use_tools", "alice") == 1, "second /gravity -> 'more than once'")

    sim.command("alice", "/tides")
    camp.run(10)
    check(camp.count("check_single_use_tools", "alice") == 1, "first /tides -> single-use trigger (never fired before)")
    check(camp.count("check_3_tools_in_1_minute", "alice") == 1, "3 tools within a minute")

    sim.command("alice", "/tpall")
    camp.run(10)
    check(not any("'/tpa'" in m for n, u, _, m in camp.fired if u == "alice"), "/tpall is not counted as /tpa")

    sim.command("bob", "/gravity")
    sim.observe("bob", "The ground is red")
    camp.run(10)
    check(camp.count("tool_use_in_build_world", "bob") == 1, "tool in the build world (by --wid)")
    check(camp.count("observation_in_build_world", "bob") == 1, "observation in the build world (always failed before)")
    camp.run(60)
    check(camp.count("tool_use_in_build_world", "bob") == 1, "build-world triggers don't repeat")
    check(not camp.fetcher.trigger_errors, f"no trigger errors {dict(camp.fetcher.trigger_errors)}")


def scenario_observations_and_chat(verbose):
    print("Observations and chat")
    camp = Camp(verbose=verbose)
    sim = camp.sim
    sim.move("carol", "NoMoon", 200, 200)
    sim.move("dave", "NoMoon", 400, 400)
    camp.run(10)

    sim.observe("carol", "The wind is strong here")
    camp.run(20)
    sim.observe("carol", "Why is the sky so dark")
    camp.run(20)
    sim.observe("carol", "Somewhat showy plants")
    camp.run(20)
    check(camp.count("check_3_observations_in_2_minutes", "carol") == 1, "3 observations in 2 minutes")
    check(camp.count("check_question_like_observation", "carol") == 1,
          "question-like observation detected once ('somewhat'/'show' don't count)")
    check(camp.count("check_nearby_similar_observation", "carol") == 0, "observations at the same spot (distance 0) don't count as 'nearby'")

    for text in ["hi", "hello", "anyone here"]:
        sim.say("dave", text)
    camp.run(10)
    check(camp.count("check_3_chat_entries_in_1_minute", "dave") == 1, "3 chats in a minute")
    camp.run(40)
    check(camp.count("check_3_chat_entries_in_1_minute", "dave") == 1, "...and it doesn't repeat while the chats are still recent")
    sim.say("dave", "look at this")
    sim.say("dave", "cool")
    camp.run(10)
    check(camp.count("check_five_chat_messages_in_world", "dave") == 1, "5 chats in one world")
    check(camp.count("check_high_chat_volume", "dave") == 1, "high chat volume (5 in 2 minutes)")
    check(not camp.fetcher.trigger_errors, f"no trigger errors {dict(camp.fetcher.trigger_errors)}")


def scenario_twenty_minutes_and_restart(verbose):
    print("20-minute triggers, offline players and save/load")
    save = Path(tempfile.mkdtemp(prefix="qrf_save_")) / "camp_state"
    state = {
        "frank": {"worlds_visited": ["NoMoon"], "current_world": "NoMoon", "npc_interaction_start": None,
                  "explored_worlds": ["TiltedMelting"]},
        "gina": {"worlds_visited": ["RocketLaunch"], "current_world": "RocketLaunch",
                 "npc_interaction_start": None, "poi_stay_start": None},
    }
    save.write_text(json.dumps(state), encoding="utf-8")
    camp = Camp(saveload=str(save), verbose=verbose)
    sim = camp.sim
    sim.move("eve", "NoMoon", 100, 100)
    sim.move("gina", "RocketLaunch", -1565, 2731)   # standing on the Attendant NPC
    camp.run(21 * 60)
    check(camp.count("check_no_observations_last_20_minutes", "eve") == 1, "no observations for 20 minutes")
    check(camp.count("check_last_tool_use_over_20_minutes", "eve") == 1, "no tools for 20 minutes (had a shared cooldown)")
    check(camp.count("check_no_observations_last_20_minutes", "frank") == 0, "offline player gets no 20-minute triggers")
    check(camp.count("check_prolonged_interaction_npc", "gina") >= 1, "NPC interaction with a saved None start time doesn't crash")
    check(camp.count("check_random_checkin") == 0, "no random check-in while other triggers keep firing")
    check(not camp.fetcher.trigger_errors, f"no trigger errors {dict(camp.fetcher.trigger_errors)}")

    saved = json.loads(save.read_text(encoding="utf-8"))
    check(saved["frank"]["explored_worlds"] == ["TiltedMelting"], "sets are saved as lists")
    check("eve" in saved and isinstance(saved["eve"]["recent_positions"], list), "new players are saved")
    reloaded = Camp(saveload=str(save))
    check(reloaded.fetcher.tools_usage["frank"]["explored_worlds"] == {"TiltedMelting"}, "...and loaded back as sets")
    check(reloaded.fetcher.world_explorers["TiltedMelting"] == ["frank"], "first-explorer list is rebuilt on load")


def scenario_movement(verbose):
    print("Movement, pause box and exploration")
    camp = Camp(verbose=verbose, config_overrides={"check_random_checkin": {"enabled": False}})
    sim = camp.sim
    sim.move("henry", "NoMoon", 300, 300)
    sim.move("ivy", "NoMoon", 310, 300)
    camp.run(130)
    check(camp.count("check_long_pair_close") == 1, "pair close for 120 s")
    sim.move("ivy", "NoMoon", 900, 900)
    camp.run(60)
    sim.move("ivy", "NoMoon", 310, 300)
    camp.run(60)
    check(camp.count("check_long_pair_close") == 1, "separate short meetings don't add up")

    sim.move("henry", "Hub", 0, 0)
    camp.run(60)
    check(camp.count("check_in_pause_box", "henry") == 1, "pause box fires on arrival only")

    sim.move("jack", "TiltedMelting", 50, 0)
    camp.run(10)
    sim.move("jack", "TiltedMelting", -50, 0)
    camp.run(10)
    check(camp.count("check_world_exploration", "jack") == 1, "both sides of TiltedMelting (misspelled before)")

    camp.run(100)
    check(camp.count("check_possible_afk_behavior", "jack") == 1, "AFK after 90 s without moving")

    sim.region_event("ivy", "secret_base", "VISIT", members="someoneelse")
    camp.run(100)
    check(camp.count("check_visits_to_unowned_region", "ivy") == 1, "visit to someone else's region")
    check(camp.count("check_prolonged_stop_in_region", "ivy") == 1, "stayed 90 s in a region (could never fire before)")
    check(not camp.fetcher.trigger_errors, f"no trigger errors {dict(camp.fetcher.trigger_errors)}")


def scenario_blocks_commands_deaths(verbose):
    print("Blocks, commands, deaths and the random check-in")
    # These repeat for an idle, lone player and would keep the camp from being "quiet".
    camp = Camp(verbose=verbose, config_overrides={"check_possible_afk_behavior": {"enabled": False},
                                                   "check_long_far_from_crowd": {"enabled": False}})
    sim = camp.sim
    sim.move("kate", "Umaine25am", 0, 0)
    sim.move("liam", "NoMoon", 0, 0)
    sim.move("mia", "LunarCrater", 0, 0)
    camp.run(10)

    for i in range(130):
        sim.block("kate", 133, i, 70, 0, 1, 1)
    camp.run(10)
    check(camp.count("check_over_200_placed_actions_in_2_minutes", "kate") == 1, "over 120 blocks placed in 2 minutes")
    camp.run(30)
    check(camp.count("check_over_200_placed_actions_in_2_minutes", "kate") == 1, "...not repeated for the same blocks")

    for i in range(20):
        sim.block("kate", 133, i, 80, 5, 19, 1)  # torches; threshold 20 in 60 s, priority 5
    camp.run(10)
    torch = [f for f in camp.fired if f[0] == "check_block_triggers" and "torch" in f[3]]
    check(len(torch) == 1 and torch[0][2] == 5, "torch rule from BlockBasedTriggers.csv, with the CSV priority")

    for i in range(25):
        sim.block("kate", 20, i, 90, 9, 1, 1)
    sim.now += 1
    for i in range(25):
        sim.block("kate", 20, i, 90, 9, 1, 0)
    camp.run(10)
    check(camp.count("check_breaks_own_block", "kate") == 1, "broke 20+ own blocks in 30 s")
    check(camp.count("check_block_breaks_by_others", "kate") == 0, "...which aren't 'blocks placed by others'")

    sim.command("liam", "/tp kate")
    sim.command("liam", "/tpa kate")
    camp.run(10)
    check(camp.count("check_use_of_disabled_mc_commands", "liam") == 1, "/tp is flagged, /tpa isn't")
    sim.move("liam", "Hub", 0, 0)
    camp.run(10)
    sim.command("liam", "/kill")
    camp.run(10)
    check(camp.count("check_use_of_disabled_mc_commands", "liam") == 1, "commands in the Hub are excluded")

    sim.die("mia", "FALL")
    camp.run(40)
    deaths = [f for f in camp.fired if f[0] == "achieves_death"]
    check(len(deaths) == 1 and "died from FALL" in deaths[0][3], "death reported once, with the cause")

    camp.clear()
    sim.leave("kate")
    sim.leave("liam")
    camp.fetcher.last_trigger_time = sim.now
    camp.run(400)
    check(camp.count("check_random_checkin") >= 1, "random check-in after 5 quiet minutes")
    check(not camp.fetcher.trigger_errors, f"no trigger errors {dict(camp.fetcher.trigger_errors)}")


def scenario_real_config():
    print("Shipped trigger_config.json")
    config = src.TriggerConfig()
    registered = {name for name, _ in src.TRIGGERS}
    check(registered <= config.names(), f"every trigger has a config entry (missing: {sorted(registered - config.names())})")
    check(config.names() <= registered, f"every config entry has a trigger (extra: {sorted(config.names() - registered)})")
    check(src.TRIGGERS[-1][0] == "check_random_checkin", "random check-in runs last")


def scenario_dispatcher():
    print("WebSocket dispatcher")
    from websockets.sync.server import serve

    received = []

    def handler(ws):
        for message in ws:
            received.append(json.loads(message))
            ws.send(message)

    server = serve(handler, "127.0.0.1", 0)
    port = server.socket.getsockname()[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    dispatcher = src.Dispatcher(f"ws://127.0.0.1:{port}")
    check(dispatcher.send("first", "alice", 2) and dispatcher.send("second", "bob", 3), "sends over one connection")
    deadline = time.monotonic() + 5
    while len(received) < 2 and time.monotonic() < deadline:
        time.sleep(0.05)
    check([m["data"]["trigger"] for m in received] == ["first", "second"], "server received both triggers")
    check(bool(received) and received[0]["data"]["masterlogs"]["student"] == "alice", "payload format unchanged")
    server.shutdown()
    check(dispatcher.send("lost", "carol", 1) is False, "a dead server is reported, not raised")
    dispatcher.close()
    check(src.Dispatcher(None).send("x", "y", 1) is False, "no URL configured -> logged, not sent")


def main():
    verbose = "-v" in sys.argv
    src.setup_logging(verbose=False, log_file=None)
    src.log.handlers[0].setLevel("ERROR")
    src.world_coordinates_dictionary.update(src.load_world_coordinates())
    scenario_tools(verbose)
    scenario_observations_and_chat(verbose)
    scenario_twenty_minutes_and_restart(verbose)
    scenario_movement(verbose)
    scenario_blocks_commands_deaths(verbose)
    scenario_real_config()
    scenario_dispatcher()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) failed:")
        for failure in FAILURES:
            print(f"  - {failure}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
