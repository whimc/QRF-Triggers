# Based on 28 July 2025 version from Neithan
# modified by Geph/Stefan/GPT for U Maine QRF camp 28 July to 1 August 2025

#127 = SDP12 junior high at Fiske - ideal for demos
#129 = uncc25am beginners at UNCC
#130 = uncc25pm returners at UNCC
#131 = SDP13 grades 4-6 at Fiske
#133 = Umaine25am
#134 = Umaine25pm

GLOBAL_WID = [133, 134]

# =============================================================================
# Imports
# =============================================================================

import json
#import signal
import sys
from argparse import ArgumentParser, ArgumentTypeError
#from datetime import datetime
from pathlib import Path
from time import sleep, time

import pandas as pd
import sqlalchemy as db
from websockets.sync.client import connect

#from datetime import datetime
import pytz

import os
import random

import difflib
from difflib import SequenceMatcher

import re
from shapely.geometry import Polygon, Point
#import math

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import threading

from collections import defaultdict

from datetime import datetime, timedelta

import socket
from typing import Optional

SOCKET: Optional[socket.socket] = None

# =============================================================================
# Trigger Manager
# =============================================================================

def is_trigger_enabled(name): # @Luc, @Geph: This is a deprecated fn but please keep for now it helps me debug
    try:
        with open("trigger_config.json") as f:
            config = json.load(f)
            return config.get(name, True)
    except:
        return True  # Default to enabled if config is missing

def get_trigger_settings(trigger_name):
    with open("trigger_config.json", "r") as f:
        config = json.load(f)
    settings = config.get(trigger_name, {"enabled": True, "priority": 1})
    return settings["enabled"], settings["priority"], settings["category"]

def get_trigger_field(trigger_name, field_name, default=None):
    with open("trigger_config.json", "r") as f:
        config = json.load(f)
    return config.get(trigger_name, {}).get(field_name, default)

# =============================================================================
# Database Connection
# =============================================================================

with open(Path(__file__).parent / "credentials.json") as f:
    creds = json.load(f)
ENG = db.create_engine(db.URL.create("mysql+mysqlconnector", **creds["database"]))

# ================================================================================================================
# Import Rachel Zhou's Important Places Coordinate system (edited to handshake with Jack's code and Triggers code.
# ================================================================================================================


data = pd.read_csv("WHIMC Coordinate Tracking updated.csv")

# Fill forward 'World' and 'Object' columns to handle blank cells
data["World"].fillna(method="ffill", inplace=True)
data["Object"].fillna(method="ffill", inplace=True)

# Initialize the dictionary to hold world data and global expected actions
world_coordinates_dictionary = {}

# Iterate through the dataframe
for _, row in data.iterrows():
    world = row["World"]
    object_name = row["Object_name"]
    object_type = row["Object"]
    expected_action = (
        row["Expected_Action"] if not pd.isna(row["Expected_Action"]) else ""
    )

    # Ensure that the world and object type keys exist in the dictionary
    if world not in world_coordinates_dictionary:
        world_coordinates_dictionary[world] = {}
    if object_type not in world_coordinates_dictionary[world]:
        world_coordinates_dictionary[world][object_type] = {}

    # Handle global expected actions
    if object_name == "NA" and object_type == "Global":
        world_coordinates_dictionary[world]["Global"] = expected_action.split(", ")

    # Handle other objects (Places, NPCs, Signals)
    else:
        x = row["x"] if not pd.isna(row["x"]) else None
        z = row["z"] if not pd.isna(row["z"]) else None
        object_range = row["range"] if not pd.isna(row["range"]) else None

        # Add details to the respective object in the dictionary
        world_coordinates_dictionary[world][object_type][object_name] = {
            "x": x,
            "z": z,
            "range": object_range,
            "Expected Action": expected_action.split(", ") if expected_action else [],
        }

if world_coordinates_dictionary.get("TwoMoonsLow", {}).get("Global", []):
    print(f"\033[92m\nStart! \nWHIMC Coordinate Tracking updated.csv imported successfully\n\033[0m")
else:
    # print(world_coordinates_dictionary.get("TwoMoonsLow", {}).get("Global", []))
    print(f"\033[91mSomething went wrong with importing WHIMC Coordinate Tracking updated.csv\n\033[0m")

# =============================================================================
# Global Variables
# =============================================================================

# Set after parsing args
SOCKET = None

# =============================================================================
# SQL Queries
# =============================================================================


GET_TABLES = """
SELECT * FROM co_block
ORDER BY rowid DESC
LIMIT 20;
"""

GET_CO_BLOCKS = f"""
SELECT * FROM co_block
WHERE time > UNIX_TIMESTAMP() - 600
ORDER BY time ASC;
"""

GET_AIRCLICKS = """
SELECT *
FROM whimc_action_physical
WHERE type = 'AIR CLICK'
  AND time > (UNIX_TIMESTAMP(current_timestamp) * 1000) - 30000
ORDER BY time DESC;
"""

GET_VISITS_TO_UNOWNED_REGION = """
SELECT *
FROM whimc_player_region_events
WHERE time > UNIX_TIMESTAMP(current_timestamp) - 30
ORDER BY time DESC;
"""



'''
GET_TABLES = """
SELECT *
FROM co_block
ORDER BY time DESC
LIMIT 20;
"""
'''

GET_DEATHS = """
SELECT uuid, username, world, x, y, z, time, type
FROM whimc_action_physical
WHERE type LIKE 'DEATH%';
"""


'''
GET_PEEK = """
DESCRIBE whimc_chat;
"""
'''

GET_PEEK = """
SELECT * from rg_region_players;
"""

'''
GET_CO_COMMAND = """
SELECT * from co_command where time > (unix_timestamp(current_timestamp) - 10);
"""
'''

GET_CO_COMMAND = """
SELECT * from co_command where time > (unix_timestamp(current_timestamp) - 10);
"""

GET_CO_COMMAND_WITH_WORLDS = """
select from_unixtime(c.time) as time
     , u.user as username
     , message
     , w.world, x, y, z
from (
    select * from co_command where time > (unix_timestamp(current_timestamp) - 20)
) as c
left join co_user as u on c.user = u.rowid
left join co_world as w on c.wid = w.rowid
"""

GET_CO_CHAT_WITH_WORLDS = """
select from_unixtime(c.time) as time
     , c.user as user_id
     , message
     , w.world as world
     , c.x, c.y, c.z
from (
    select * from co_chat where time > (unix_timestamp(current_timestamp) - 15)
) as c
left join co_world as w on c.wid = w.rowid
"""

GET_CO_CHAT = """
SELECT * from co_chat where time > (unix_timestamp(current_timestamp) - 60);
"""

GET_CO_USER = """
SELECT * from co_user;
"""

wid_str = ", ".join(str(wid) for wid in GLOBAL_WID)

GET_CO_BLOCK_WITH_USERS = f"""
SELECT b.*, u.user as username
FROM co_block b
LEFT JOIN co_user u ON b.user = u.rowid
WHERE wid IN ({wid_str}) AND b.time > (UNIX_TIMESTAMP(current_timestamp) - 120)
"""

# Extended GET_ONLINE_PLAYERS TO have current world data

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
select from_unixtime(c.time) as time
     , u.user as username
     , message
     , world, x, y, z
from (
    select * from co_command where from_unixtime(time) >= '{newer_than}'
) as c
left join co_user as u on c.user = u.rowid
left join co_world as w on c.wid = w.rowid
"""

GET_OBSERVATIONS = """
select from_unixtime(floor(time / 1000)) as time
     , username
     , observation_color_stripped as observation
     , world, x, y, z
from whimc_observations
-- timestamp has millisecond precision
where from_unixtime(time / 1000) >= '{newer_than}'
"""

GET_SCIENCE_TOOLS = """
select from_unixtime(time / 1000) as time
     , username
     , tool
     , measurement
     , world, x, y, z
from whimc_sciencetools
-- timestamp has millisecond precision
where from_unixtime(time / 1000) >= '{newer_than}'
"""

GET_WORLD_PERIMETERS = """
select region_id
     , world_id
     , min_x
     , min_y
     , min_z
     , max_x
     , max_y
     , max_z
from rg_region_cuboid where region_id = 'perimeter'
-- we could also include selecting regions for kid bases, we'd want to standardize naming conventions,
-- though you could just assume that any region that's not perimeter on a mars build world is a kid base
"""

'''
GET_BLOCKS = """
select user
      , world_id, x, y, z
      , type
      , action
from co_block where wid = 133 and
-- this should be a variable defined at startup, not hardcoded; wid 111 = Umaine25am
where from_unixtime(time) >= '{newer_than}'
-- timestamp is 10 digit unix precision
"""
'''

GET_BLOCKS = """
select user
      , world_id, x, y, z
      , type
      , action
from co_block
where wid = {wid} and
from_unixtime(time) >= '{newer_than}'
"""

# Fetch the blocks with the provided wid (special function with special scope)
def fetch_blocks(self):
    query = GET_BLOCKS.format(wid=self.wid, newer_than=self.newer_than.strftime("%Y-%m-%d %H:%M:%S"))
    self.co_block_with_users = get_data(query)

GET_MATERIALS = """
select id
      , material
from co_material_map
"""

# @neithan this should probably be disabled or changed:
GET_WID_FOR_WORLD = """
select w.rowid as wid
from co_world w
where w.world = 'Umaine25am'
"""


block_trigger_cooldowns = {}  # maps username -> last_trigger_time
block_trigger_cooldown_seconds = 100000

# =============================================================================
# Utility Functions (get from WHIMC, send to Dispatcher)
# =============================================================================

def send_trigger(trigger_name: str, username: str, priority: int):
    payload = {
        "event": "new_message",
        "data": {
            "from": "software",
            "software": "WHIMC",
            "timestamp": int(datetime.now().timestamp() * 1000),
            "eventID": "",
            "student": username,
            "trigger": trigger_name,
            "priority": priority,
        },
    }
    from typing import Any, Dict
    payload: Dict[str, Dict[str, Any]] = {"data": {}}
    payload["data"]["masterlogs"] = {
        "reviewer": "",
        "end": "",
        "feedbackTXT": "",
        "feedbackREC": "",
    }

    json_data = json.dumps(payload)

    try:
        global SOCKET
        SOCKET = connect(
            "wss://free.blr2.piesocket.com/v3/qrfchannel?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1"
        )

        stopwatch = time()
        SOCKET.send(json_data)
        print(f"Data sent to socket after {time() - stopwatch:.3f} seconds")

        stopwatch = time()
        response = SOCKET.recv()
        print(f"Received response after {time() - stopwatch:.3f} seconds")
        # print(response)
        print(response[:30] + "..." if len(response) > 30 else response)

        stopwatch = time()
        SOCKET.close()
        print(f"Closed socket after {time() - stopwatch:.3f} seconds\n")

    except Exception as e:
        print(f"An error occurred: {e}")


def get_data(query, newer_than: datetime | None = None) -> pd.DataFrame:
    return pd.read_sql(query.format(newer_than=newer_than), ENG)


# =============================================================================
# The Fetcher Class
# =============================================================================

class Fetcher:
    CMDS = {
        "commands": GET_COMMANDS,
        "observations": GET_OBSERVATIONS,
        "science_tools": GET_SCIENCE_TOOLS,
        "players": GET_ONLINE_PLAYERS,
        "peek": GET_PEEK,
        "co_chat": GET_CO_CHAT,
        "co_chat_with_worlds": GET_CO_CHAT_WITH_WORLDS,
        "co_user": GET_CO_USER,
        "co_command": GET_CO_COMMAND,
        "co_command_with_worlds": GET_CO_COMMAND_WITH_WORLDS,
        "co_block_with_users": GET_CO_BLOCK_WITH_USERS, # will dynamically load for the world set in --wid in command line startup
        "get_wid_for_world": GET_WID_FOR_WORLD,
        "get_tables": GET_TABLES,
        "get_deaths": GET_DEATHS,
        "get_co_blocks": GET_CO_BLOCKS,
        "get_airclicks": GET_AIRCLICKS,
        "get_visits_to_unowned_region": GET_VISITS_TO_UNOWNED_REGION,
    }


    def load_data(self):
        for key, query in Fetcher.CMDS.items():
            df = get_data(query, self.newer_than)
            setattr(self, key, df)


    def __init__(self, initial_newer_than, saveload_file=None, wid=None):
        self.newer_than = initial_newer_than
        self.saveload_file = saveload_file
        # self.players = get_data(GET_ONLINE_PLAYERS)
        self.wid = wid
        self.block_triggers_df = pd.read_csv('BlockBasedTriggers.csv')
        # indicate a start-time of script
        self.start_time = datetime.now(central_tz).timestamp()


        # Mostly for type hinting
        self.commands = pd.DataFrame()
        self.observations = pd.DataFrame()
        self.science_tools = pd.DataFrame()
        self.players = pd.DataFrame()
        self.peek = pd.DataFrame()
        self.co_chat = pd.DataFrame()
        self.co_user = pd.DataFrame()
        self.co_command = pd.DataFrame()
        self.co_command_with_worlds = pd.DataFrame()
        self.co_chat_with_worlds = pd.DataFrame()
        self.co_block_with_users = pd.DataFrame()
        self.get_wid_for_world = pd.DataFrame()
        self.get_co_blocks = pd.DataFrame()
        self.get_airclicks = pd.DataFrame()
        self.get_visits_to_unowned_region = pd.DataFrame()


        # Luc added
        self.prev_trig_time = pd.DataFrame(columns=['User', 'Material', 'Action', 'H_or_l', 'Time'])

        self.load_data()

        # this is for random trigger that fires when inactivity is detected
        # does not matter which timezone, we just need a timer interval to detect inactivty since the time the python script was ran
        self.last_trigger_time = datetime.now().timestamp()

        # dataframes / dictionaries for triggers
        self.triggers_list = []
        # self.tools_usage = {} <- recoded to look for a save/load file first if any

        if saveload_file and os.path.exists(saveload_file):
            with open(saveload_file, "r") as f:
                self.tools_usage = json.load(f)

            # Convert 'explored_worlds' from list back to set
            # July 28 fix from Neithan
            for username, user_data in self.tools_usage.items():
                if "explored_worlds" in user_data and isinstance(user_data["explored_worlds"], list):
                    user_data["explored_worlds"] = set(user_data["explored_worlds"])

        else:
            self.tools_usage = {}

        self.initialize_tool_usage()
        self.observations_record = {}
        self.pair_durations = defaultdict(int)

        self.lastTriggerTimePerUser = {}

        self.get_deaths = pd.DataFrame()
        self.block_breaks_already_triggered = set()
        self.unowned_region_already_triggered = set()

    def _ensure_user_schema(self, user: str, world: str, position_time: float):
        d = self.tools_usage.setdefault(user, {})
        d.setdefault("worlds_visited", [world])
        d.setdefault("current_world", world)
        d.setdefault("tool_use_count", 0)
        d.setdefault("total_observation_count", 0)
        d.setdefault("world_observation_counts", {world: 0})
        d.setdefault("last_observation_time", position_time)
        d.setdefault("mynoa_start_time", None)
        d.setdefault("mynoa_trigger_fired", False)
        d.setdefault("recent_positions", [])
        d.setdefault("recent_observations", [])
        d.setdefault("tool_usage_timestamps", [])
        d.setdefault("last_tool_use_time", 0)
        return d

    '''
    def save_tools_usage(self):
        if self.saveload_file:
            with open(self.saveload_file, "w") as f:
                json.dump(self.tools_usage, f)
            print(
                f"\033[92mProgress saved to '{self.saveload_file}'. \nIt is now safe to stop the python script.\n \033[0m"
            )
    '''

    '''
    def save_tools_usage(self):
        # Deep copy to avoid mutating the live structure
        from copy import deepcopy

        serializable_tools_usage = deepcopy(self.tools_usage)

        for username, user_data in serializable_tools_usage.items():
            if "explored_worlds" in user_data and isinstance(user_data["explored_worlds"], set):
                user_data["explored_worlds"] = list(user_data["explored_worlds"])

        with open("tools_usage.json", "w") as f:
            json.dump(serializable_tools_usage, f, indent=2)
    '''

    def save_tools_usage(self):
        from copy import deepcopy

        if self.saveload_file:
            # Deep copy to avoid mutating the live structure
            serializable_tools_usage = deepcopy(self.tools_usage)

            for username, user_data in serializable_tools_usage.items():
                if "explored_worlds" in user_data and isinstance(user_data["explored_worlds"], set):
                    user_data["explored_worlds"] = list(user_data["explored_worlds"])

            with open(self.saveload_file, "w") as f:
                json.dump(serializable_tools_usage, f, indent=2)

            print(
                f"\033[92mProgress saved to '{self.saveload_file}'.\nIt is now safe to stop the Python script.\033[0m\n"
            )




    def fetch_data(self):
        for key, query in Fetcher.CMDS.items():
            df = get_data(query, self.newer_than)
            # Set 'self.<key>' to the new dataframe


            if key == "co_block_with_users":
                print ("PEEK")
                print (df)
                print ("WID set on self")
                print (self.wid)



            if key == "get_wid_for_world":
                print (f"PEEK: {key}")
                print (df)
                print ("WID or current world")


            setattr(self, key, df)

        self.save_tools_usage()  # save after fetching the data

    def fetch_data_playersonly(self):
        for key, query in Fetcher.CMDS.items():
            if key == "players":
                df = get_data(query, self.newer_than)
                # Set 'self.<key>' to the new dataframe
                setattr(self, key, df)

        self.save_tools_usage()  # save after fetching the data

    def fetch_data_observationsonly(self):
        for key, query in Fetcher.CMDS.items():
            if key == "observations":
                df = get_data(query, self.newer_than)
                # Set 'self.<key>' to the new dataframe
                setattr(self, key, df)

        self.save_tools_usage()  # save after fetching the data

    def on_wakeup(self):
        # Use global variables
        # global meganumber

        # Need to convert the time to central time always because the actions
        # of the players in the server are logged in central time.
        central_tz = pytz.timezone("America/Chicago")
        now = datetime.now(central_tz)

        # now = datetime.now()
        print(f"\033[96mWakeup at ----------- {now}. \nFetching data since - {self.newer_than.astimezone(central_tz)} \n^- \033[0mtime window for needed location values")

        self.fetch_data()

        '''
        print(f"\nONLINE PLAYERS:\n{self.players}\n")
        print(f"COMMANDS:\n{self.commands}\n")
        print(f"OBSERVATIONS:\n{self.observations}\n")
        print(f"SCIENCE TOOLS:\n{self.science_tools}\n")
        print(f"OBSERVATIONS RECORD:\n{self.observations_record}\n")
        '''

        if not self.players.empty:
            print(f"\033[95m\nONLINE PLAYERS:\033[0m\n{self.players}\n")

        if not self.commands.empty:
            print(f"\033[95m\nCOMMANDS:\033[0m\n{self.commands}\n")

        if not self.observations.empty:
            print(f"\033[95m\nOBSERVATIONS:\033[0m\n{self.observations}\n")

        if not self.science_tools.empty:
            print(f"\033[95m\nSCIENCE TOOLS:\033[0m\n{self.science_tools}\n")

        if self.observations_record:  # Assuming observations_record is a dictionary: nts: Check this occassionally
            print(f"\033[95m\nOBSERVATIONS RECORD:\033[0m\n{self.observations_record}\n")

        if not self.co_chat.empty:
            print(f"\033[95m\nLATEST CHATS:\033[0m\n{self.co_chat}\n")

        # print(f"CO_SESSION:\n{self.co_session}\n")

        # Initialize tools_usage for all online players so we can get the worlds visited, and curr world data even
        # if the student didn't make any observations / commands yet

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            position_time = row["position_time"]


            if user not in self.tools_usage:
                self.tools_usage[user] = {
                    "worlds_visited": [
                        current_world
                    ],  # Initialize with the actual current world
                    "current_world": current_world,
                    "tool_use_count": 0,
                    # 'observation_count': 0
                    "total_observation_count": 0,  # For overall observation count
                    "world_observation_counts": {
                        current_world: 0
                    },  # For per-world observation count
                    "last_observation_time": position_time,
                    # "last_observation_time": position_time,
                    "mynoa_start_time": None,
                    "mynoa_trigger_fired": False,
                    "recent_positions": [], # racing / non-stopping
                    "recent_observations": [],
                    "tool_usage_timestamps": [],
                    "last_tool_use_time": position_time,
                    # "last_tool_use_time": position_time,
                    "far_from_crowd_duration": 0,
                    "npc_interaction_start": None,
                    "poi_stay_start": None,
                    "world_tool_counts": {current_world: 0},
                    "chat_counts": {current_world: 0},
                    "tool_counts": {}

                }
            else:
                # Update the current world
                self.tools_usage[user]["current_world"] = current_world

                # Add to worlds_visited if not already there
                if current_world not in self.tools_usage[user]["worlds_visited"]:
                    self.tools_usage[user]["worlds_visited"].append(current_world)

                # Racing / non-stopping
                if 'recent_positions' not in self.tools_usage[user]:
                    self.tools_usage[user]['recent_positions'] = []

                # Initialize world_tool_counts if not present
                if 'world_tool_counts' not in self.tools_usage[user]:
                    self.tools_usage[user]['world_tool_counts'] = {}

                if current_world not in self.tools_usage[user]['world_tool_counts']:
                    self.tools_usage[user]['world_tool_counts'][current_world] = 0

                if 'chat_counts' not in self.tools_usage[user]:
                    self.tools_usage[user]['chat_counts'] = {}

                if current_world not in self.tools_usage[user]['chat_counts']:
                    self.tools_usage[user]['chat_counts'][current_world] = 0

                if 'tool_counts' not in self.tools_usage[user]:
                    self.tools_usage[user]['tool_counts'] = {}

                if current_world not in self.tools_usage[user]['tool_counts']:
                    self.tools_usage[user]['tool_counts'][current_world] = {}

                '''
                # Reset last observation and tool use times
                print(f"Resetting times for user {user}")
                self.tools_usage[user]["last_observation_time"] = now.timestamp()
                self.tools_usage[user]["last_tool_use_time"] = now.timestamp()
                print(f"Last observation time: {self.tools_usage[user]['last_observation_time']}")
                print(f"Last tool use time: {self.tools_usage[user]['last_tool_use_time']}")
                '''

                # note to self: REGISTER NEW TRIGGER TO FIRE HERE, REGISTER TRIGGER

        # check for triggers and populate triggers_list
        self.update_tool_usage()
        self.update_observation_usage()
        self.check_mynoa_observations()
        self.check_activities_near_important_places()

        # summer2024newtriggers
        self.check_no_observations_last_20_minutes()
        self.check_racing_non_stopping()
        self.check_3_observations_in_2_minutes()
        self.check_3_tools_in_1_minute()
        self.check_last_tool_use_over_20_minutes()
        self.check_3_chat_entries_in_1_minute()
        self.check_long_pair_close()
        self.check_long_far_from_crowd()
        self.check_prolonged_interaction_npc()
        self.check_prolonged_stay_poi()
        self.check_teleporting_to_multiple_players()
        self.check_specific_commands()
        self.check_five_or_more_observations_in_world()
        self.check_five_or_more_tools_in_world()
        self.check_five_chat_messages_in_world()
        # self.check_over_200_actions_in_2_minutes()
        self.check_over_200_placed_actions_in_2_minutes()
        self.check_over_200_destroyed_actions_in_2_minutes()
        self.check_block_triggers()

        # new triggers for new summer camp
        self.achieves_death()
        self.check_block_breaks_by_others()
        self.check_used_help_command()
        self.check_visit_unmarked_pois()
        self.check_world_exploration()
        self.check_multiple_npc_visits()
        self.check_tool_inspiration_generic()
        self.check_tool_inspiration_specific()
        self.check_advanced_tool_use()
        self.check_high_chat_volume()
        self.check_airclick_burst()
        self.check_breaks_own_block()
        self.check_visits_to_unowned_region()
        self.check_ignores_nearby_npc()
        self.check_use_of_disabled_mc_commands()
        self.check_high_x_axis_movement()
        self.check_low_x_axis_movement()
        self.check_high_y_axis_movement()
        self.check_low_y_axis_movement()
        self.check_question_like_observation()
        self.check_advanced_tool_use2()
        self.check_appropriate_tool_use_near_poi()
        self.check_movement_away_from_npc_or_poi()
        self.check_dominant_z_axis_movement()
        self.check_avoiding_poi()
        self.check_use_basic_science_tools()
        self.check_prolonged_stop_in_region()
        self.check_possible_afk_behavior()
        self.check_movement_toward_npc_or_poi()
        self.check_in_pause_box()

        # print(f"TOOLS & OBSERVATION USAGE (SAVED): \n{self.tools_usage}\n")

        # Send all triggers
        for trigger_name, username, priority in self.triggers():
            print(
                f"\033[93mTriggered '{trigger_name}' for '{username}' (priority {priority}) \033[0m"
            )
            send_trigger(trigger_name, username, priority)

        # Next iteration should /only/ show new data
        self.newer_than = now

        # Also reset the triggers list
        self.triggers_list = []

    def triggers(self) -> list[tuple[str, str, int]]:
        """
        Return any triggers as a list of tuple[trigger name, username, priority]
        """
        triggers = []

        # TODO add checks here
        # trigger = ("test", "Poi", 1)
        # triggers.append(trigger)

        return self.triggers_list

    # OPERATIONALIZE TRIGGERS

    def check_movement_toward_npc_or_poi(self):
        trigger_name = "check_movement_toward_npc_or_poi"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        proximity_threshold = 10  # "Near" range
        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_half", "BrownDwarf"
        ]

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            nearest_object = None
            nearest_distance = float("inf")

            for object_type, objects in world_coordinates_dictionary.get(current_world, {}).items():
                for object_name, details in objects.items():
                    obj_x = details.get("x")
                    obj_z = details.get("z")
                    if obj_x is None or obj_z is None:
                        continue
                    distance = abs(x - obj_x) + abs(z - obj_z)
                    if distance < nearest_distance:
                        nearest_distance = distance
                        nearest_object = object_name

            self.tools_usage.setdefault(user, {})

            prev_dist = self.tools_usage[user].get("m6_distance")
            prev_world = self.tools_usage[user].get("m6_world")
            prev_object = self.tools_usage[user].get("m6_object")

            if nearest_distance <= proximity_threshold:
                if (
                        prev_dist is not None and
                        prev_world == current_world and
                        nearest_object == prev_object and
                        nearest_distance < prev_dist
                ):
                    msg = (
                        f"{user} moved *toward* {nearest_object} in {current_world}. "
                        f"Distance changed from {prev_dist} to {nearest_distance}. Category: {category}"
                    )
                    self.triggers_list.append((msg, user, priority))
                    print(msg)

                    self.tools_usage[user].pop("m6_distance", None)
                    self.tools_usage[user].pop("m6_world", None)
                    self.tools_usage[user].pop("m6_object", None)
                else:
                    self.tools_usage[user]["m6_distance"] = nearest_distance
                    self.tools_usage[user]["m6_world"] = current_world
                    self.tools_usage[user]["m6_object"] = nearest_object
                    print(f"\033[90m{user} is near {nearest_object} — observing movement toward it... (dist={nearest_distance})\033[0m")
            else:
                self.tools_usage[user].pop("m6_distance", None)
                self.tools_usage[user].pop("m6_world", None)
                self.tools_usage[user].pop("m6_object", None)


    def check_possible_afk_behavior(self):
        trigger_name = "check_possible_afk_behavior"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = self.players
        if df.empty:
            print(f"\033[90m{trigger_name}: No player data available.\033[0m")
            return

        afk_threshold = 90  # seconds for testing; increase to 300 for production
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in df.iterrows():
            user = row["online_user"]
            x, z = row["x"], row["z"]

            if None in (x, z):
                continue

            self.tools_usage.setdefault(user, {})
            state = self.tools_usage[user]

            # Detect movement
            last_position = state.get("last_position")
            if last_position != (x, z):
                state["last_position"] = (x, z)
                state["last_afk_time"] = current_time
                print(f"\033[90m{user} moved — resetting last_afk_time.\033[0m")
                continue

            # Initialize last_afk_time if missing
            if "last_afk_time" not in state:
                state["last_afk_time"] = current_time
                continue

            time_elapsed = current_time - state["last_afk_time"]

            if time_elapsed >= afk_threshold:
                msg = f"{user} appears to be AFK (no movement for {int(time_elapsed)}s). Category: {category}"
                self.triggers_list.append((msg, user, priority))
                print(msg)
                # Reset the clock to prevent spamming
                state["last_afk_time"] = current_time - 60
            else:
                print(f"\033[90m{user} may be AFK — observing... ({int(time_elapsed)}s elapsed)\033[0m")






    def check_prolonged_stop_in_region(self):
        trigger_name = "check_prolonged_stop_in_region"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        central_tz = pytz.timezone("America/Chicago")
        now_ts = datetime.now(central_tz).timestamp()
        duration_threshold = 90  # seconds

        if not hasattr(self, "region_stay_tracker"):
            self.region_stay_tracker = {}

        for _, row in self.get_visits_to_unowned_region.iterrows():
            username = row["username"]
            region = row["region"]
            event_type = row["trigger"]
            key = f"{username}_{region}"

            if event_type == "VISIT":
                if key not in self.region_stay_tracker:
                    self.region_stay_tracker[key] = now_ts
                    print(f"\033[94m{username} entered region '{region}'. Possibly stopping — observing if duration exceeds threshold...\033[0m")
                else:
                    elapsed = now_ts - self.region_stay_tracker[key]
                    if elapsed >= duration_threshold:
                        msg = f"{username} stayed in region '{region}' for over {duration_threshold}s. Category: {category}"
                        self.triggers_list.append((msg, username, priority))
                        print(msg)
                        self.region_stay_tracker[key] = now_ts - 60  # reset for potential retrigger
                    else:
                        print(f"\033[94m{username} is still in region '{region}' — {int(elapsed)}s elapsed. Waiting to reach {duration_threshold}s...\033[0m")

            elif event_type == "LEAVE":
                if key in self.region_stay_tracker:
                    print(f"\033[90m{username} left region '{region}' before reaching threshold. Timer cleared.\033[0m")
                    self.region_stay_tracker.pop(key, None)



    def check_use_basic_science_tools(self):
        trigger_name = "check_use_basic_science_tools"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # ====================================================
        basic_tools = {"gravity", "temperature", "humidity", "oxygen", "wind"}
        cooldown_period = 600  # 10 minutes in seconds
        # ====================================================

        df = self.commands
        if df.empty:
            print(f"\033[90m{trigger_name}: No command data available.\033[0m")
            return

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in df.iterrows():
            user = row["username"]
            message = row["message"].strip()

            if not message.startswith("/"):
                continue

            used_tool = message.split()[0].replace("/", "").lower()
            if used_tool in basic_tools:
                self.tools_usage.setdefault(user, {})
                last_trigger = self.tools_usage[user].get("last_basic_tool_trigger", 0)

                if current_time - last_trigger >= cooldown_period:
                    trigger_message = f"{user} used a basic science tool ({used_tool}). Category: {category}"
                    self.triggers_list.append((trigger_message, user, priority))
                    print(trigger_message)
                    self.tools_usage[user]["last_basic_tool_trigger"] = current_time



    def check_avoiding_poi(self):
        trigger_name = "check_avoiding_poi"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        duration_threshold = 300  # seconds for testing; raise to 300 for production
        cooldown_period = 300  # seconds (5 mins)

        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_close", "Mynoa_half", "Cancri", "BrownDwarf"
        ]

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            inside_any_poi = False
            for object_type, objects in world_coordinates_dictionary.get(current_world, {}).items():
                if object_type == "Global":
                    continue
                for object_name, details in objects.items():
                    if "range" in details and self.is_point_inside_space(x, z, details["range"]):
                        inside_any_poi = True
                        break
                if inside_any_poi:
                    break

            self.tools_usage.setdefault(user, {})

            if inside_any_poi:
                self.tools_usage[user]["last_poi_visit"] = current_time
                print(f"{user} entered a POI — resetting last_poi_visit.")
            else:
                if "last_poi_visit" not in self.tools_usage[user]:
                    self.tools_usage[user]["last_poi_visit"] = current_time  # initialize only once

                last_poi_visit = self.tools_usage[user]["last_poi_visit"]
                time_outside = current_time - last_poi_visit

                last_trigger_time = self.tools_usage[user].get("last_avoid_poi_trigger", 0)
                cooldown_passed = current_time - last_trigger_time >= cooldown_period

                if time_outside >= duration_threshold and cooldown_passed:
                    msg = f"{user} has not visited any POI in the last {duration_threshold} seconds. Category: {category}"
                    self.triggers_list.append((msg, user, priority))
                    print(msg)
                    self.tools_usage[user]["last_avoid_poi_trigger"] = current_time
                else:
                    print(f"{user} seems to be avoiding POI — observing further... ({int(time_outside)}s elapsed)")



    def check_dominant_z_axis_movement(self):
        trigger_name = "check_dominant_z_axis_movement"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        for user, data in self.tools_usage.items():
            recent_positions = data.get("recent_positions", [])
            # Ensure all entries are 3-tuples
            if len(recent_positions) < 6 or not all(len(pos) == 3 for pos in recent_positions):
                continue

            total_dx = total_dy = total_dz = 0

            for i in range(1, len(recent_positions)):
                prev_x, prev_y, prev_z = recent_positions[i - 1]
                curr_x, curr_y, curr_z = recent_positions[i]
                total_dx += abs(curr_x - prev_x)
                total_dy += abs(curr_y - prev_y)
                total_dz += abs(curr_z - prev_z)

            total_dx = total_dx if total_dx != 0 else 0.1
            total_dy = total_dy if total_dy != 0 else 0.1

            ratio_zx = total_dz / total_dx
            ratio_zy = total_dz / total_dy

            if ratio_zx > 3:
                msg = f"{user} showed dominant Z-axis movement: Δz/Δx = {ratio_zx:.2f}. Category: {category}"
                self.triggers_list.append((msg, user, priority))
                print(msg)
            elif ratio_zy < 3:
                msg = f"{user} showed low Z vs Y movement: Δz/Δy = {ratio_zy:.2f}. Category: {category}"
                self.triggers_list.append((msg, user, priority))
                print(msg)



    def check_movement_away_from_npc_or_poi(self):
        trigger_name = "check_movement_away_from_npc_or_poi"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        proximity_threshold = 10  # "Near" range
        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_half", "BrownDwarf"
        ]

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            nearest_object = None
            nearest_distance = float("inf")

            # Scan all NPCs and POIs in world
            for object_type, objects in world_coordinates_dictionary.get(current_world, {}).items():
                for object_name, details in objects.items():
                    obj_x = details.get("x")
                    obj_z = details.get("z")
                    if obj_x is None or obj_z is None:
                        continue
                    distance = abs(x - obj_x) + abs(z - obj_z)
                    if distance < nearest_distance:
                        nearest_distance = distance
                        nearest_object = object_name

            if nearest_distance <= proximity_threshold:
                # Still within proximity; record it
                self.tools_usage[user]["m7_distance"] = nearest_distance
                self.tools_usage[user]["m7_world"] = current_world
                self.tools_usage[user]["m7_object"] = nearest_object
            else:
                # Has moved away
                prev_dist = self.tools_usage[user].get("m7_distance")
                prev_world = self.tools_usage[user].get("m7_world")
                prev_object = self.tools_usage[user].get("m7_object")

                if prev_dist is not None and prev_world == current_world and nearest_object == prev_object:
                    if nearest_distance > prev_dist:
                        msg = f"{user} moved away from {nearest_object} in {current_world}. Distance changed from {prev_dist} to {nearest_distance}. Category: {category}"
                        self.triggers_list.append((msg, user, priority))
                        print(msg)

                    # Clean up
                    self.tools_usage[user].pop("m7_distance", None)
                    self.tools_usage[user].pop("m7_world", None)
                    self.tools_usage[user].pop("m7_object", None)




    def check_appropriate_tool_use_near_poi(self):
        trigger_name = "check_appropriate_tool_use_near_poi"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = self.commands
        if df.empty:
            print(f"\033[90m{trigger_name}: No command data available.\033[0m")
            return

        for _, row in df.iterrows():
            user = row["username"]
            world = row["world"]
            x = row["x"]
            z = row["z"]
            message = row["message"].strip()

            if not message.startswith("/"):
                continue

            used_tool = message.split()[0].lower()  # e.g., "/pressure"

            for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                for object_name, details in objects.items():
                    expected = [ea.lower() for ea in details.get("Expected Action", [])]

                    # Check if player is near the POI
                    is_close = False

                    if (
                            "range" in details and
                            self.is_point_inside_space(x, z, details["range"])
                    ):
                        is_close = True

                    elif (
                            all(k in details for k in ("x", "z")) and
                            None not in (x, z, details["x"], details["z"])
                    ):
                        dx = abs(x - details["x"])
                        dz = abs(z - details["z"])
                        if dx + dz <= 10:
                            is_close = True

                    # If close and used tool is expected
                    if is_close and used_tool in expected:
                        msg = (
                            f"{user} used an appropriate tool {used_tool} near {object_name} "
                            f"expecting the use of {used_tool}. Category: {category}"
                        )
                        print(msg)
                        self.triggers_list.append((msg, user, priority))
                        break  # Trigger once per command




    def check_advanced_tool_use2(self):
        trigger_name = "check_advanced_tool_use2"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        advanced_tools = {
            "PRESSURE", "TIDES", "TILT", "TECTONIC", "MAGNETIC_FIELD",
            "RADIATION", "ATMOSPHERE", "ROTATIONAL_PERIOD", "DAYLENGTH", "AIRFLOW"
        }

        df = self.science_tools
        if df.empty:
            print(f"\033[90m{trigger_name}: No science tool data available.\033[0m")
            return

        for _, row in df.iterrows():
            tool = str(row["tool"]).upper()
            user = row["username"]
            world = row["world"]

            if tool in advanced_tools:
                msg = f"{user} used advanced taught tool {tool} in {world}. Category: {category}"
                self.triggers_list.append((msg, user, priority))
                print(msg)


    '''
    def check_question_like_observation(self):
        trigger_name = "check_question_like_observation"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        question_keywords = ["what", "when", "where", "why", "who", "which", "how"]

        def contains_question(text):
            lower = text.lower()
            return "?" in text or any(word in lower for word in question_keywords)

        # === CHECK CHATS ===
        merged_df = pd.merge(self.co_chat, self.co_user, left_on='user', right_on='rowid')

        for _, row in merged_df.iterrows():
            username = row["user_y"]  # Username from co_user table
            chat_text = row["message"]
            if contains_question(chat_text):
                message = f"{username} asked a question in chat: “{chat_text}” — Category: {category}"
                self.triggers_list.append((message, username, priority))
                print(f"{message}")

        # === CHECK OBSERVATIONS ===
        for _, row in self.observations.iterrows():
            username = row["username"] if "username" in row else row["user"]
            obs_text = row["text"]
            if contains_question(obs_text):
                message = f"{username} made a question-like observation: “{obs_text}” — Category: {category}"
                self.triggers_list.append((message, username, priority))
                print(f"\033[93m{message}\033[0m")
    '''

    def check_question_like_observation(self):
        trigger_name = "check_question_like_observation"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # Setup cooldown store if not yet initialized
        if not hasattr(self, "question_like_cooldowns"):
            self.question_like_cooldowns = {}

        cooldown_ms = get_trigger_field(trigger_name, "cooldown_ms", 5 * 60 * 1000)  # default: 5 minutes
        now_ms = int(datetime.now(pytz.utc).timestamp() * 1000)

        question_keywords = ["what", "when", "where", "why", "who", "which", "how"]

        def contains_question(text):
            if not isinstance(text, str):
                return False
            lower = text.lower()
            return "?" in lower or any(word in lower for word in question_keywords)

        triggered_this_tick = set()

        # === CHECK CHATS ===
        if not self.co_chat.empty and not self.co_user.empty:
            try:
                merged_df = pd.merge(self.co_chat, self.co_user, left_on='user', right_on='rowid')
                for _, row in merged_df.iterrows():
                    username = row.get("user_y")
                    chat_text = row.get("message")
                    if not username or not chat_text:
                        continue

                    last_fired = self.question_like_cooldowns.get(username, 0)
                    if username in triggered_this_tick or now_ms - last_fired < cooldown_ms:
                        continue

                    if contains_question(chat_text):
                        message = f"{username} asked a question in chat: “{chat_text}” — Category: {category}"
                        self.triggers_list.append((message, username, priority))
                        print(f"{message}")
                        self.question_like_cooldowns[username] = now_ms
                        triggered_this_tick.add(username)
            except Exception as e:
                print(f"\033[91mError in merging co_chat and co_user: {e}\033[0m")

        # === CHECK OBSERVATIONS ===
        if not self.observations.empty:
            for _, row in self.observations.iterrows():
                username = row.get("username") or row.get("user")
                obs_text = row.get("text")
                if not username or not obs_text:
                    continue

                last_fired = self.question_like_cooldowns.get(username, 0)
                if username in triggered_this_tick or now_ms - last_fired < cooldown_ms:
                    continue

                if contains_question(obs_text):
                    message = f"{username} made a question-like observation: “{obs_text}” — Category: {category}"
                    self.triggers_list.append((message, username, priority))
                    print(f"\033[93m{message}\033[0m")
                    self.question_like_cooldowns[username] = now_ms
                    triggered_this_tick.add(username)



    def check_low_y_axis_movement(self):
        trigger_name = "check_low_y_axis_movement"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        for user, data in self.tools_usage.items():
            recent_positions = data.get("recent_positions", [])
            if len(recent_positions) < 6:
                continue  # Not enough movement data

            total_dy = 0
            total_dx = 0

            for i in range(1, len(recent_positions)):
                prev_x, prev_y = recent_positions[i - 1]
                curr_x, curr_y = recent_positions[i]
                total_dy += abs(curr_y - prev_y)
                total_dx += abs(curr_x - prev_x)

            if total_dx == 0:
                total_dx = 0.1  # prevent division by zero

            movement_ratio = total_dy / total_dx

            if movement_ratio < 3 and total_dy < 20:  # 20 blocks = adjustable threshold
                trigger_message = f"{user} showed low y-axis movement (Δy = {total_dy:.2f}, Δx = {total_dx:.2f}, Δy/Δx = {movement_ratio:.2f}). Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(f"\033[91m{trigger_message}\033[0m")
            elif movement_ratio < 3:
                print(f"\033[94m{user} is favoring X over Y (Δy/Δx = {movement_ratio:.2f}, Δy = {total_dy:.2f}). Observing further for low-y movement...\033[0m")


    def check_high_y_axis_movement(self):
        trigger_name = "check_high_y_axis_movement"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        for user, data in self.tools_usage.items():
            recent_positions = data.get("recent_positions", [])
            if len(recent_positions) < 6:
                continue

            total_dy = 0
            total_dx = 0

            for i in range(1, len(recent_positions)):
                prev_x, prev_y = recent_positions[i - 1]
                curr_x, curr_y = recent_positions[i]
                total_dy += abs(curr_y - prev_y)
                total_dx += abs(curr_x - prev_x)

            if total_dx == 0:
                total_dx = 0.1  # avoid division by zero

            movement_ratio = total_dy / total_dx

            if movement_ratio >= 2 and movement_ratio < 3:
                print(f"\033[94m{user} is moving more along the Y-axis than X-axis (Δy/Δx = {movement_ratio:.2f}). Observing further for high-y movement trigger...\033[0m")

            if movement_ratio >= 3:
                trigger_message = f"{user} showed high y-axis movement (Δy/Δx = {movement_ratio:.2f}). Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)


    def check_low_x_axis_movement(self):
        trigger_name = "check_low_x_axis_movement"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        x_block_threshold = 15  # You can adjust this value if needed

        for user, data in self.tools_usage.items():
            recent_positions = data.get("recent_positions", [])
            if len(recent_positions) < 6:
                continue  # Not enough data yet

            total_dx = 0
            total_dy = 0

            for i in range(1, len(recent_positions)):
                prev_x, prev_y = recent_positions[i - 1]
                curr_x, curr_y = recent_positions[i]
                total_dx += abs(curr_x - prev_x)
                total_dy += abs(curr_y - prev_y)

            if total_dx == 0:
                total_dx = 0.1  # avoid division by zero

            movement_ratio = total_dx / total_dy if total_dy != 0 else float('inf')

            if movement_ratio < 3 and total_dx < x_block_threshold:
                trigger_message = f"{user} showed low x-axis movement (Δx={total_dx:.1f}, Δy={total_dy:.1f}, Δx/Δy={movement_ratio:.2f}). Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)
            elif movement_ratio < 3:
                print(f"\033[94m{user} has low x-axis ratio (Δx/Δy = {movement_ratio:.2f}) but moved {total_dx:.1f} blocks along X — waiting...\033[0m")


    def check_high_x_axis_movement(self):
        trigger_name = "check_high_x_axis_movement"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        for user, data in self.tools_usage.items():
            recent_positions = data.get("recent_positions", [])
            if len(recent_positions) < 6:
                continue  # Not enough movement data

            total_dx = 0
            total_dy = 0

            for i in range(1, len(recent_positions)):
                prev_x, prev_y = recent_positions[i - 1]
                curr_x, curr_y = recent_positions[i]
                total_dx += abs(curr_x - prev_x)
                total_dy += abs(curr_y - prev_y)

            if total_dy == 0:
                total_dy = 0.1  # prevent division by zero

            movement_ratio = total_dx / total_dy

            if movement_ratio >= 2 and movement_ratio < 3:
                print(f"\033[94m{user} is moving more along the X-axis than Y-axis (Δx/Δy = {movement_ratio:.2f}). Observing further for high-x movement trigger...\033[0m")

            if movement_ratio >= 3:
                trigger_message = f"{user} showed high x-axis movement (Δx/Δy = {movement_ratio:.2f}). Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)


    def achieves_death(self):
        trigger_name = "achieves_death"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        current_time_ms = int(datetime.now(pytz.utc).timestamp() * 1000)
        cutoff_time_ms = current_time_ms - 30000  # last 30 seconds

        if self.get_deaths.empty:
            print("\033[90mNo death logs available in get_deaths.\033[0m")
            return

        recent_deaths = self.get_deaths[self.get_deaths["time"] >= cutoff_time_ms]

        for _, row in recent_deaths.iterrows():
            username = row["username"]
            timestamp = row["time"]

            dt_str = datetime.fromtimestamp(timestamp / 1000, pytz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            trigger_message = f"{username} achieved death in '{row['world']}' at ({row['x']}, {row['y']}, {row['z']}) — {dt_str} Category: Actions A2"
            self.triggers_list.append((trigger_message, username, priority))
            print(trigger_message)

    def check_visits_to_unowned_region(self):
        trigger_name = "check_visits_to_unowned_region"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = self.get_visits_to_unowned_region
        if df.empty:
            print(f"\033[90m{trigger_name}: No region events found.\033[0m")
            return

        already_triggered = self.unowned_region_already_triggered

        for _, row in df.iterrows():
            if row['trigger'] != 'VISIT':
                continue

            username = row['username'].lower()
            members = [m.strip().lower() for m in (row['region_members'] or '').split(',')]

            if username in members:
                continue  # They’re part of the region, so ignore

            uid = row['uuid']
            if uid in already_triggered:
                continue  # Skip if already triggered in this session

            msg = (
                f"{row['username']} visited region '{row['region']}'"
                f"Category: {category}"
            )
            self.triggers_list.append((msg, row['username'], priority))
            print(msg)
            already_triggered.add(uid)



    '''
    def check_airclick_burst(self):
        trigger_name = "check_airclick_burst"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        if self.get_airclicks.empty:
            print("\033[90mNo airclick logs available in get_airclicks.\033[0m")
            return

        current_time_ms = int(datetime.now(pytz.utc).timestamp() * 1000)
        cutoff_time_ms = current_time_ms - 20000  # last 20 seconds

        recent_clicks = self.get_airclicks[self.get_airclicks["time"] >= cutoff_time_ms]

        if recent_clicks.empty:
            print("\033[90mNo recent airclicks in the last 20s.\033[0m")
            return

        grouped = recent_clicks.groupby("username")

        for username, group in grouped:
            click_count = len(group)

            click_threshold = get_trigger_field(trigger_name, "click_threshold", 5)
            if click_count > click_threshold:
                dt_str = datetime.fromtimestamp(group["time"].max() / 1000, pytz.utc).strftime("%H:%M:%S UTC")
                message = f"{username} made {click_count} AIR CLICKs in the last 20s (latest at {dt_str}). Category: {category}"
                self.triggers_list.append((message, username, priority))
                print(message)
            else:
                print(f"{username} had {click_count} airclicks — below threshold.")
    '''

    def check_airclick_burst(self):
        trigger_name = "check_airclick_burst"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        if self.get_airclicks.empty:
            print("\033[90mNo airclick logs available in get_airclicks.\033[0m")
            return

        now_ms = int(datetime.now(pytz.utc).timestamp() * 1000)
        cooldown_ms = 5 * 60 * 1000  # 5-minute cooldown
        last_fired = getattr(self, "airclick_burst_last_fired", 0)

        if now_ms - last_fired < cooldown_ms:
            remaining = int((cooldown_ms - (now_ms - last_fired)) / 1000)
            print(f"\033[90m{trigger_name} skipped due to cooldown. {remaining} seconds remaining.\033[0m")
            return

        time_window_ms = get_trigger_field(trigger_name, "time_window_ms", 3 * 60 * 1000)  # default: 3 minutes
        cutoff_time_ms = now_ms - time_window_ms
        recent_clicks = self.get_airclicks[self.get_airclicks["time"] >= cutoff_time_ms]

        if recent_clicks.empty:
            print(f"\033[90mNo recent airclicks in the last {time_window_ms // 1000} seconds.\033[0m")
            return

        grouped = recent_clicks.groupby("username")
        fired = False

        for username, group in grouped:
            click_count = len(group)
            click_threshold = get_trigger_field(trigger_name, "click_threshold", 5)

            if click_count > click_threshold:
                first_time = group["time"].min()
                last_time = group["time"].max()

                elapsed_burst = int((last_time - first_time) / 1000)
                window_minutes = int(time_window_ms / 60000)

                # Format elapsed burst as “X seconds” or “X min Y sec”
                range_str = f"{elapsed_burst}s" if elapsed_burst < 60 else f"{elapsed_burst // 60}m {elapsed_burst % 60}s"

                dt_str = datetime.fromtimestamp(last_time / 1000, pytz.utc).strftime("%H:%M:%S UTC")

                message = (
                    f"{username} made {click_count} AIR CLICKs in {window_minutes} minutes, "
                    f"range {range_str} (latest at {dt_str}). Category: {category}"
                )

                self.triggers_list.append((message, username, priority))
                print(message)
                fired = True
            else:
                print(f"{username} had {click_count} airclicks — below threshold.")

        if fired:
            self.airclick_burst_last_fired = now_ms




    # =============================================================================
    # Helper functions for trigger dictionaries / dataframes
    # =============================================================================

    def print_world_coordinates_dictionary(self):
        for world, object_types in world_coordinates_dictionary.items():
            print(f"World: {world}")
            for object_type, objects in object_types.items():
                print(f"  Object Type: {object_type}")
                for object_name, details in objects.items():
                    print(f"    Object Name: {object_name}")
                    for key, value in details.items():
                        print(f"      {key}: {value}")

    # =============================================================================
    # For Checking Important Places
    # =============================================================================

    '''
    def check_activities_near_important_places(self):
        #now includes aliases
        slash_commands_in_expected_actions = [
            "airflow",
            "wind",
            "altitude",
            "height",
            "atmosphere",
            "composition",
            "cosmicrays",
            "gravity",
            "humidity",
            "water",
            "vapor",
            "magnetic_field",
            "oxygen",
            "pressure",
            "air_pressure",
            "atmosphere_pressure",
            "radiation",
            "radius",
            "rotational_period",
            "daylength",
            "scale",
            "tectonic",
            "seismic",
            "temperature",
            "temp",
            "tides",
            "ocean_level",
            "tilt",
            "axial_tilt",
            "year",
            "orbital_period",
            "observe",
        ]

        
        for _, row in self.observations.iterrows():
            user = row["username"]
            world = row["world"]
            x = row["x"]
            z = row["z"]
            observation_text = row["observation"]

            trigger_name = "observation_near_poi"
            enabled, priority, category = get_trigger_settings(trigger_name)

            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                if object_type == "Global":
                    continue

                for object_name, details in objects.items():
                    similarity = SequenceMatcher(None, observation_text, object_name).ratio()

                    if "range" in details and self.is_point_inside_space(x, z, details["range"]):
                        trigger_message = (
                            f"{user} made an observation near {object_name} in {world}. "
                            f"Similarity score: {similarity:.2f}. Category: {category}"
                        )
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, priority))

                    elif "x" in details and "z" in details:
                        place_x, place_z = details["x"], details["z"]
                        if (
                            place_x is not None and place_z is not None and
                            x is not None and z is not None and
                            abs(x - place_x) + abs(z - place_z) <= 10
                        ):
                            trigger_message = (
                                f"{user} made an observation near {object_name} in {world}. "
                                f"Similarity score: {similarity:.2f}. Category: {category}"
                            )
                            print(trigger_message)
                            self.triggers_list.append((trigger_message, user, priority))

        
        for _, cmd_row in self.commands.iterrows():
            user = cmd_row["username"]
            world = cmd_row["world"]
            x = cmd_row["x"]
            z = cmd_row["z"]
            message = cmd_row["message"].strip()

            used_tool = None
            for tool in slash_commands_in_expected_actions:
                if message.startswith(f"/{tool}"):
                    used_tool = tool
                    break

            if not used_tool:
                continue

            trigger_name = "tool_use_near_expected_action"
            enabled, priority, category = get_trigger_settings(trigger_name)

            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            tool_triggered = False
            for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                if object_type == "Global":
                    continue

                for object_name, details in objects.items():
                    expected_actions = details.get("Expected Action", [])

                    if (
                        "range" in details and self.is_point_inside_space(x, z, details["range"])
                        and f"/{used_tool}" in expected_actions
                    ):
                        trigger_message = (
                            f"{user} used tool {used_tool} near {object_name} in {world}. "
                            f"Category: {category}"
                        )
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, priority))
                        tool_triggered = True

                    elif (
                        "x" in details and "z" in details and
                        f"/{used_tool}" in expected_actions
                    ):
                        place_x, place_z = details["x"], details["z"]
                        if abs(x - place_x) + abs(z - place_z) <= 10:
                            trigger_message = (
                                f"{user} used tool {used_tool} near {object_name} in {world}. "
                                f"Category: {category}"
                            )
                            print(trigger_message)
                            self.triggers_list.append((trigger_message, user, priority))
                            tool_triggered = True

            if not tool_triggered:
                global_actions = world_coordinates_dictionary.get(world, {}).get("Global", {})
                for _, details in global_actions.items():
                    expected_actions = details.get("Expected Action", [])
                    if f"/{used_tool}" in expected_actions:
                        trigger_message = (
                            f"{user} used tool {used_tool} in {world} (Global action). "
                            f"Category: {category}"
                        )
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, priority))
                        break
    '''

    def check_activities_near_important_places(self):
        # Now includes aliases
        slash_commands_in_expected_actions = [
            "airflow", "wind", "altitude", "height", "atmosphere", "composition",
            "cosmicrays", "gravity", "humidity", "water", "vapor", "magnetic_field",
            "oxygen", "pressure", "air_pressure", "atmosphere_pressure", "radiation",
            "radius", "rotational_period", "daylength", "scale", "tectonic", "seismic",
            "temperature", "temp", "tides", "ocean_level", "tilt", "axial_tilt",
            "year", "orbital_period", "observe"
        ]

        # Observation-based trigger
        for _, row in self.observations.iterrows():
            user = row["username"]
            world = row["world"]
            x = row["x"]
            z = row["z"]
            observation_text = row["observation"]

            trigger_name = "observation_near_poi"
            enabled, priority, category = get_trigger_settings(trigger_name)

            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            if x is None or z is None:
                continue  # Skip if coordinates are missing

            for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                if object_type == "Global":
                    continue

                for object_name, details in objects.items():
                    similarity = SequenceMatcher(None, observation_text, object_name).ratio()

                    poi_x = details.get("x")
                    poi_z = details.get("z")
                    poi_range = details.get("range")

                    is_near = False
                    if poi_range is not None:
                        is_near = self.is_point_inside_space(x, z, poi_range)
                    elif None not in (poi_x, poi_z):
                        is_near = abs(x - poi_x) + abs(z - poi_z) <= 10

                    if is_near:
                        trigger_message = (
                            f"{user} made an observation near {object_name} in {world}. "
                            f"Similarity score: {similarity:.2f}. Category: {category}"
                        )
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, priority))

        # Tool-use–based trigger
        for _, cmd_row in self.commands.iterrows():
            user = cmd_row["username"]
            world = cmd_row["world"]
            x = cmd_row["x"]
            z = cmd_row["z"]
            message = cmd_row["message"].strip()

            if x is None or z is None:
                continue

            used_tool = None
            for tool in slash_commands_in_expected_actions:
                if message.startswith(f"/{tool}"):
                    used_tool = tool
                    break

            if not used_tool:
                continue

            trigger_name = "tool_use_near_expected_action"
            enabled, priority, category = get_trigger_settings(trigger_name)

            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            tool_triggered = False
            for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                if object_type == "Global":
                    continue

                for object_name, details in objects.items():
                    expected_actions = [ea.lower() for ea in details.get("Expected Action", [])]

                    poi_x = details.get("x")
                    poi_z = details.get("z")
                    poi_range = details.get("range")

                    is_near = False
                    if poi_range is not None:
                        is_near = self.is_point_inside_space(x, z, poi_range)
                    elif None not in (poi_x, poi_z):
                        is_near = abs(x - poi_x) + abs(z - poi_z) <= 10

                    if is_near and f"/{used_tool}" in expected_actions:
                        trigger_message = (
                            f"{user} used tool {used_tool} near {object_name} in {world}. "
                            f"Category: {category}"
                        )
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, priority))
                        tool_triggered = True

            # Global fallback
            if not tool_triggered:
                global_actions = world_coordinates_dictionary.get(world, {}).get("Global", {})
                for _, details in global_actions.items():
                    expected_actions = [ea.lower() for ea in details.get("Expected Action", [])]
                    if f"/{used_tool}" in expected_actions:
                        trigger_message = (
                            f"{user} used tool {used_tool} in {world} (Global action). "
                            f"Category: {category}"
                        )
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, priority))
                        break


    # Rachel Zhou's code edited to work with .self
    def define_polygon_boundary(self, range_str):
        if not range_str:
            return []  # Or return a default polygon if applicable

        coordinates = [
            tuple(map(int, coord))
            for coord in re.findall(r"\((-?\d+),(-?\d+)\)", range_str)
        ]

        if len(coordinates) == 2:
            x1, z1 = coordinates[0]
            x2, z2 = coordinates[1]
            coordinates = [(x1, z1), (x1, z2), (x2, z2), (x2, z1)]

        return coordinates

    def is_point_inside_space(self, x, z, range_str):
        if not range_str:
            return False

        boundary = self.define_polygon_boundary(range_str)
        if not boundary:  # Check if the boundary is empty or invalid
            return False

        polygon = Polygon(boundary)
        point = Point(x, z)
        return polygon.contains(point)

    # =============================================================================
    # /For Checking Important Places
    # =============================================================================

    '''
    def check_mynoa_observations(self):
        for _, player_row in self.players.iterrows():
            user = player_row["online_user"]
            current_world = player_row["world"]
            position_time = player_row["position_time"]

            if user not in self.tools_usage:
                self.tools_usage[user] = {
                    "last_observation_time": position_time,
                    "mynoa_start_time": None,
                    "mynoa_trigger_fired": False,
                }

            # if current_world.startswith("mynoa"):
            if current_world.startswith("Mynoa"):
                if self.tools_usage[user]["mynoa_start_time"] is None:
                    self.tools_usage[user]["mynoa_start_time"] = position_time
                    self.tools_usage[user]["mynoa_trigger_fired"] = False
                else:
                    time_in_mynoa = (
                        position_time - self.tools_usage[user]["mynoa_start_time"]
                    )
                    if (
                        time_in_mynoa >= 25 * 60
                        and not self.tools_usage[user]["mynoa_trigger_fired"]
                    ):
                        # if time_in_mynoa >= 10 and not self.tools_usage[user]['mynoa_trigger_fired']:
                        observations_in_mynoa = (
                            self.tools_usage[user]
                            .get("world_observation_counts", {})
                            .get(current_world, 0)
                        )
                        if observations_in_mynoa == 0:
                            print(
                                f"{user} has been in {current_world} for more than 25 minutes without making an observation."
                            )
                            self.triggers_list.append(
                                (
                                    f"{user} in {current_world} for 25+ minutes without observations",
                                    user,
                                    1,
                                )
                            )
                            self.tools_usage[user]["mynoa_trigger_fired"] = True
            else:
                self.tools_usage[user]["mynoa_start_time"] = None
                self.tools_usage[user]["mynoa_trigger_fired"] = False
    '''

    def check_mynoa_observations(self):
        trigger_name = "check_mynoa_observations"
        enabled, priority, category = get_trigger_settings(trigger_name)

        for _, player_row in self.players.iterrows():
            user = player_row["online_user"]
            current_world = player_row["world"]
            position_time = player_row["position_time"]

            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            if user not in self.tools_usage:
                self.tools_usage[user] = {
                    "last_observation_time": position_time,
                    "mynoa_start_time": None,
                    "mynoa_trigger_fired": False,
                }

            if current_world.startswith("Mynoa"):
                if self.tools_usage[user].get("mynoa_start_time") is None:
                    self.tools_usage[user]["mynoa_start_time"] = position_time
                    self.tools_usage[user]["mynoa_trigger_fired"] = False
                else:
                    time_in_mynoa = position_time - self.tools_usage[user]["mynoa_start_time"]
                    if time_in_mynoa >= 25 * 60 and not self.tools_usage[user]["mynoa_trigger_fired"]:
                        observations_in_mynoa = (
                            self.tools_usage[user]
                            .get("world_observation_counts", {})
                            .get(current_world, 0)
                        )
                        if observations_in_mynoa == 0:
                            trigger_message = f"{user} has been in {current_world} for more than 25 minutes without making an observation. Category: {category}"
                            print(trigger_message)
                            self.triggers_list.append((trigger_message, user, priority))
                            self.tools_usage[user]["mynoa_trigger_fired"] = True
            else:
                self.tools_usage[user]["mynoa_start_time"] = None
                self.tools_usage[user]["mynoa_trigger_fired"] = False


    def update_observation_usage(self):
        """
        Ingest rows from self.observations and update:
          - self.tools_usage[user] (counts, last_observation_time, etc.)
          - self.observations_record[world] (for spatial/semantic proximity checks)
          - self.triggers_list (when triggers fire)
        """
        central_tz = pytz.timezone("America/Chicago")

        # Ensure the container exists
        if not hasattr(self, "observations_record"):
            self.observations_record = {}

        def _to_epoch_central(ts):
            """Robustly convert a value coming from SQL/pandas to a tz-aware epoch (float, seconds)."""
            # Pandas Timestamp
            if isinstance(ts, pd.Timestamp):
                if ts.tzinfo is None:
                    ts = ts.tz_localize(pytz.UTC)
                return ts.astimezone(central_tz).timestamp()

            # datetime
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=pytz.UTC)
                return ts.astimezone(central_tz).timestamp()

            # number (ms or s)
            if isinstance(ts, (int, float)):
                # Heuristic: if it's too large, treat as ms
                if ts > 1e12:
                    ts = ts / 1000.0
                # treat as UTC seconds
                return datetime.fromtimestamp(ts, pytz.UTC).astimezone(central_tz).timestamp()

            # string "YYYY-mm-dd HH:MM:SS"
            if isinstance(ts, str):
                try:
                    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
                    return dt.astimezone(central_tz).timestamp()
                except Exception:
                    pass

            # Fallback: now
            return datetime.now(central_tz).timestamp()

        # Settings for the "nearby & similar observation" trigger
        trigger_name_near = "check_nearby_similar_observation"
        enabled_near, priority_near, category_near = get_trigger_settings(trigger_name_near)

        # Iterate over observations
        for _, row in self.observations.iterrows():
            user = row.get("username")
            world = row.get("world")
            x = row.get("x")
            z = row.get("z")
            observation_text = row.get("observation", "") or ""

            raw_time = row.get("time")
            position_time = _to_epoch_central(raw_time)

            # Ensure user schema (prevents all the KeyErrors you were seeing)
            data = self._ensure_user_schema(user, world, position_time)

            # Keep an index of all observations by world
            world_list = self.observations_record.setdefault(world, [])
            world_list.append((x, z, user, observation_text))

            # --- Optional: nearby & similar observation trigger ---
            if enabled_near:
                # Simple Manhattan distance check
                for obs_x, obs_z, obs_user, obs_text in world_list[:-1]:  # skip the one we just appended
                    distance = abs(x - obs_x) + abs(z - obs_z)
                    if 0 < distance < 10:
                        similarity = difflib.SequenceMatcher(None, observation_text, obs_text).ratio()
                        print(f"obs distance is: {distance}, similarity is: {similarity:.2f}")

                        trigger_message = (
                            f"{user} made an observation near another observation in {world}. "
                            f"Similarity: {similarity:.2f}. Category: {category_near}"
                        )
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, priority_near))
                        break  # one trigger per new obs

            # ---- Update counters for the user ----
            data["current_world"] = world
            data["last_observation_time"] = position_time

            if world not in data["worlds_visited"]:
                data["worlds_visited"].append(world)

            # per-world counters
            data["world_observation_counts"].setdefault(world, 0)
            data["world_observation_counts"][world] += 1

            # total counter
            data["total_observation_count"] += 1

            # sliding window (2 minutes)
            data.setdefault("recent_observations", []).append(position_time)
            now_epoch = datetime.now(central_tz).timestamp()
            data["recent_observations"] = [
                t for t in data["recent_observations"] if now_epoch - t <= 2 * 60
            ]

            # ---- Check if this observation happened in a "build map" (GLOBAL_WID) ----
            try:
                wid = self.get_wid_for_world(world)  # your method; make sure it returns an int or None
            except Exception as e:
                print(f"[WARN] get_wid_for_world({world}) failed: {e}")
                wid = None

            if wid is not None and wid in GLOBAL_WID:
                trigger_message = f"{user} made an observation in {world}."
                self.triggers_list.append((trigger_message, user, 2))
                print(trigger_message)



    def initialize_tool_usage(self):
        # Get the current time in the America/Chicago timezone
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()  # Get the current Unix timestamp

        # Initialize the last observation and tool usage times for all users
        for user in self.tools_usage.keys():
            # Set both the observation and tool use time to the current time
            self.tools_usage[user]["last_observation_time"] = current_time
            self.tools_usage[user]["last_tool_use_time"] = current_time

            # Log the initialization
            print(f"Initialized last_observation_time and last_tool_use_time for {user} to current time.")


    '''
    def check_no_observations_last_20_minutes(self):
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        time_dilation = 0

        # Capture the start time when the script is initialized
        # Assume `self.start_time` was initialized when the script started
        for user, data in self.tools_usage.items():
            last_observation_time = data.get("last_observation_time", 0)
            
            # Only proceed if the last observation time was after the script started
            if last_observation_time > self.start_time:
                if current_time - last_observation_time + time_dilation > 1 * 60:  # 20 minutes
                    
                    trigger_message = f"{user} has not made any observations in the last 20 minutes."
                    self.triggers_list.append((trigger_message, user, 1))
                    print(trigger_message)
                    
                    # Reset last_observation_time to current_time for the next cycle
                    self.tools_usage[user]["last_observation_time"] = current_time + time_dilation
                    
            else:
                print(f"Skipping user {user} because their last observation was before the script started.")
    '''

    def check_high_chat_volume(self):
        trigger_name = "check_high_chat_volume"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        if self.co_chat.empty or self.co_user.empty:
            print(f"\033[90m{trigger_name}: Chat or user data unavailable.\033[0m")
            return

        chat_threshold = get_trigger_field(trigger_name, "check_high_chat_volume_threshold", 5)
        minute_window = get_trigger_field(trigger_name, "check_high_chat_volume_minutes", 2)

        # Merge to get readable usernames
        merged_df = pd.merge(self.co_chat, self.co_user, left_on='user', right_on='rowid')

        # Convert UNIX timestamp to datetime
        central_tz = pytz.timezone("America/Chicago")
        now = datetime.now(central_tz)
        merged_df["parsed_time"] = pd.to_datetime(merged_df["time_x"], unit='s', utc=True).dt.tz_convert(central_tz)

        for username, user_df in merged_df.groupby("user_y"):
            recent_entries = user_df[user_df["parsed_time"] >= now - timedelta(minutes=minute_window)]
            if len(recent_entries) >= chat_threshold:
                msg = f"{username} made {len(recent_entries)} chat entries in the last {minute_window} minutes. Category: {category}"
                self.triggers_list.append((msg, username, priority))
                print(msg)






    def check_used_help_command(self):
        trigger_name = "check_used_help_command"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = getattr(self, "commands", pd.DataFrame())  # <-- this is where your /help logs are
        if df.empty:
            print(f"\033[90m{trigger_name}: No command data available.\033[0m")
            return

        help_uses = df[df["message"].str.lower() == "/help"]

        if help_uses.empty:
            print(f"\033[90m{trigger_name}: No /help command found in this slice.\033[0m")
            return

        if not hasattr(self, "help_triggered_users"):
            self.help_triggered_users = set()

        for _, row in help_uses.iterrows():
            username = row["username"]

            if username in self.help_triggered_users:
                continue

            msg = f"{username} used the /help command. Category: {category}"
            self.triggers_list.append((msg, username, priority))
            print(msg)

            self.help_triggered_users.add(username)


    def check_breaks_own_block(self):
        trigger_name = "check_breaks_own_block"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = getattr(self, "get_co_blocks", pd.DataFrame())
        if df.empty:
            print(f"\033[90m{trigger_name}: No co_block data available.\033[0m")
            return

        already_triggered = self.block_breaks_already_triggered

        placements_by_user = defaultdict(dict)  # user → coord → placement time
        self_breaks_by_user = defaultdict(list)

        for _, row in df.iterrows():
            coord = (row['wid'], row['x'], row['y'], row['z'])
            t = row['time']
            uid = row['user']
            action = row['action']

            if action == 1:
                placements_by_user[uid][coord] = t
            elif action == 0 and coord in placements_by_user[uid]:
                # The user broke a block they placed earlier
                self_breaks_by_user[uid].append(t)

        for uid, timestamps in self_breaks_by_user.items():
            if uid in already_triggered:
                continue

            timestamps.sort()
            start = 0
            for end in range(len(timestamps)):
                while timestamps[end] - timestamps[start] > 30:
                    start += 1
                if end - start + 1 >= 20:
                    username = (
                        self.co_user[self.co_user["rowid"] == uid]["user"].values[0]
                        if uid in self.co_user["rowid"].values
                        else f"User {uid}"
                    )
                    msg = (
                        f"{username} broke {end - start + 1} of their own placed blocks within 30 seconds. "
                        f"Category: {category}"
                    )
                    self.triggers_list.append((msg, username, priority))
                    print(msg)

                    already_triggered.add(uid)
                    break


    def check_block_breaks_by_others(self):
        trigger_name = "check_block_breaks_by_others"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = getattr(self, "get_co_blocks", pd.DataFrame())
        if df.empty:
            print(f"\033[90m{trigger_name}: No co_block data available.\033[0m")
            return

        placements = {}  # coord → (user, time)
        break_events = defaultdict(list)
        already_triggered = self.block_breaks_already_triggered

        for _, row in df.iterrows():
            coord = (row['wid'], row['x'], row['y'], row['z'])
            t = row['time']
            uid = row['user']
            action = row['action']

            if action == 1:
                placements[coord] = (uid, t)
            elif action == 0:
                if coord in placements:
                    placer_uid, placed_time = placements[coord]
                    if placer_uid != uid:
                        break_events[uid].append(t)

        for uid, timestamps in break_events.items():
            if uid in already_triggered:
                continue  # Skip if already triggered

            timestamps.sort()
            start = 0
            for end in range(len(timestamps)):
                while timestamps[end] - timestamps[start] > 30:
                    start += 1
                if end - start + 1 >= 20:
                    username = (
                        self.co_user[self.co_user["rowid"] == uid]["user"].values[0]
                        if uid in self.co_user["rowid"].values
                        else f"User {uid}"
                    )
                    msg = (
                        f"{username} destroyed {end - start + 1} blocks placed by others within 30 seconds. "
                        f"Category: {category}"
                    )
                    self.triggers_list.append((msg, username, priority))
                    print(msg)

                    already_triggered.add(uid)
                    break  # Only fire once per user per 10-min slice



    def check_no_observations_last_20_minutes(self):

        trigger_name = "check_no_observations_last_20_minutes"
        enabled, priority, category = get_trigger_settings(trigger_name)


        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for user, data in self.tools_usage.items():

            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                return

            last_observation_time = data.get("last_observation_time", self.start_time)

            # Check if the 20-minute condition is met
            if current_time - last_observation_time > 20 * 60:  # 20 minutes
                last_trigger_time = self.lastTriggerTimePerUser.get(user, 0)

                # Ensure cooldown is respected
                if current_time - last_trigger_time > 20 * 60:  # 20-minute cooldown
                    trigger_message = f"{user} has not made any observations in the last 20 minutes. Category: {category}"
                    # self.triggers_list.append((trigger_message, user, 1))
                    self.triggers_list.append((trigger_message, user, priority))
                    print(trigger_message)

                    # Update last trigger time
                    self.lastTriggerTimePerUser[user] = current_time
                else:
                    print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user}. — Cooldown Active.\033[0m")

    '''
    def check_last_tool_use_over_20_minutes(self):
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        time_dilation = 0

        # Capture the start time when the script is initialized
        # Assume `self.start_time` was initialized when the script started
        for user, data in self.tools_usage.items():
            last_tool_use_time = data.get("last_tool_use_time", 0)
            
            # Only proceed if the last tool use time was after the script started
            if last_tool_use_time > self.start_time:
                if current_time - last_tool_use_time + time_dilation > 20 * 60:  # 20 minutes
                    
                    trigger_message = f"{user} has not used any tools in the last 20 minutes."
                    self.triggers_list.append((trigger_message, user, 1))
                    print(trigger_message)
                    
                    # Reset last_tool_use_time to current_time for the next cycle
                    self.tools_usage[user]["last_tool_use_time"] = current_time + time_dilation
                    
            else:
                print(f"Skipping user {user} because their last tool use was before the script started.")
    '''

    '''
    def check_last_tool_use_over_20_minutes(self):
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for user, data in self.tools_usage.items():
            last_tool_use_time = data.get("last_tool_use_time", self.start_time)

            # Check if the 20-minute condition is met
            if current_time - last_tool_use_time > 20 * 60:  # 20 minutes
                last_trigger_time = self.lastTriggerTimePerUser.get(user, 0)

                # Ensure cooldown is respected
                if current_time - last_trigger_time > 20 * 60:  # 20-minute cooldown
                    trigger_message = f"{user} has not used any tools in the last 20 minutes."
                    self.triggers_list.append((trigger_message, user, 1))
                    print(trigger_message)

                    # Update last trigger time
                    self.lastTriggerTimePerUser[user] = current_time
                else:
                    print(f"(Last Tool Use) Cooldown active for {user}. Trigger skipped.")
    '''

    def check_last_tool_use_over_20_minutes(self):
        trigger_name = "check_last_tool_use_over_20_minutes"
        enabled, priority, category = get_trigger_settings(trigger_name)



        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for user, data in self.tools_usage.items():

            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                return

            last_tool_use_time = data.get("last_tool_use_time", self.start_time)

            if current_time - last_tool_use_time > 20 * 60:
                last_trigger_time = self.lastTriggerTimePerUser.get(user, 0)

                if current_time - last_trigger_time > 20 * 60:
                    trigger_message = f"{user} has not used any tools in the last 20 minutes. Category: {category}"
                    self.triggers_list.append((trigger_message, user, priority))
                    print(trigger_message)
                    self.lastTriggerTimePerUser[user] = current_time
                else:
                    print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user}. — Cooldown Active.\033[0m")



    '''         
    def check_3_chat_entries_in_1_minute(self):
        # Merge co_chat and co_user to get usernames
        merged_df = pd.merge(self.co_chat, self.co_user, left_on='user', right_on='rowid')



        # Get the current time in the correct timezone
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        # Track chat entries for each user
        chat_count = merged_df['user_x'].value_counts()  # Adjusted column name

        for user, count in chat_count.items():
            if count >= 3:
                # Fetch the username
                username = merged_df[merged_df['user_x'] == user]['user_y'].iloc[0]  # Adjusted column name

                # Create a trigger message
                trigger_message = f"{username} has made 3 or more chat entries in the last minute."
                self.triggers_list.append((trigger_message, username, 7))
                print(trigger_message)
    '''

    def check_3_chat_entries_in_1_minute(self):
        trigger_name = "check_3_chat_entries_in_1_minute"
        enabled, priority, category = get_trigger_settings(trigger_name)


        # Merge co_chat and co_user to get usernames
        merged_df = pd.merge(self.co_chat, self.co_user, left_on='user', right_on='rowid')

        # Get the current time in the correct timezone
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        # Track chat entries for each user
        chat_count = merged_df['user_x'].value_counts()  # Adjusted column name

        for user, count in chat_count.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                return

            if count >= 3:
                # Fetch the username
                username = merged_df[merged_df['user_x'] == user]['user_y'].iloc[0]  # Adjusted column name

                # Create a trigger message
                trigger_message = f"{username} has made 3 or more chat entries in the last minute. Category: {category}"
                self.triggers_list.append((trigger_message, username, priority))
                print(trigger_message)

    '''
    def check_3_observations_in_2_minutes(self):
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()
        for user, data in self.tools_usage.items():
            recent_observations = data.get("recent_observations", [])
            if len(recent_observations) >= 3:
                trigger_message = f"{user} has made 3 observations in the last 2 minutes."
                self.triggers_list.append((trigger_message, user, 3))
                print(trigger_message)
                # Clear recent observations to avoid repeated triggers
                self.tools_usage[user]["recent_observations"] = []
    '''

    def check_3_observations_in_2_minutes(self):
        trigger_name = "check_3_observations_in_2_minutes"
        enabled, priority, category = get_trigger_settings(trigger_name)

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for user, data in self.tools_usage.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            recent_observations = data.get("recent_observations", [])
            if len(recent_observations) >= 3:
                trigger_message = f"{user} has made 3 observations in the last 2 minutes. Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)
                # Clear recent observations to avoid repeated triggers
                self.tools_usage[user]["recent_observations"] = []


    def update_tool_usage(self):
        # Define lists of tools for different usage checks
        multi_use_tools = ["gravity", "pressure", "atmosphere"]
        single_use_tools = [
            "rotational_period",
            "scale",
            "tectonic",
            "tides",
            "year",
            "tilt",
            "magnetic_field",
            "tpa",
            "agent",
            "pause",
            "tpall",
            "gamemode",
            "difficulty",
            "op",
            "kill",
            "pvp",
            "/sphere",
            "sphere",
            "/hsphere",
            "hsphere"
        ]

        science_tools = multi_use_tools + single_use_tools
        central_tz = pytz.timezone("America/Chicago")  # Ensure we use the correct timezone

        for _, row in self.commands.iterrows():
            user = row["username"]
            world = row["world"]
            message = row["message"].strip()
            position_time = row["time"]

            # Convert position_time to a string if it's a Timestamp
            if isinstance(position_time, pd.Timestamp):
                position_time = position_time.strftime("%Y-%m-%d %H:%M:%S")

            # Convert position_time to a timestamp in central timezone
            timestamp = datetime.strptime(position_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=central_tz).timestamp()

            # Initialize the user in the dictionary if not present
            if user not in self.tools_usage:
                self.tools_usage[user] = {}
                self.tools_usage[user]["worlds_visited"] = []
                self.tools_usage[user]["tool_usage_timestamps"] = []  # Initialize tool usage timestamps
                self.tools_usage[user]["last_tool_use_time"] = timestamp  # Initialize last tool use time
                self.tools_usage[user]["world_tool_counts"] = {}
                self.tools_usage[user]["tool_counts"] = {}

            # Check for known tools in the message
            for tool in science_tools:
                if message.startswith(f"/{tool}"):
                    tool_key = f"{tool}_{world}"  # Unique key for each tool and world

                    # Initialize the tool in the user's dictionary if not present
                    if tool_key not in self.tools_usage[user]:
                        self.tools_usage[user][tool_key] = 0
                        self.tools_usage[user][f"{tool_key}_flag"] = 0  # Usage flag

                    # Increment the count for the tool
                    self.tools_usage[user][tool_key] += 1

                    # Always update what world the user currently is in
                    self.tools_usage[user]["current_world"] = world

                    if world not in self.tools_usage[user]["worlds_visited"]:
                        self.tools_usage[user]["worlds_visited"].append(world)

                    if world not in self.tools_usage[user]["world_tool_counts"]:
                        self.tools_usage[user]["world_tool_counts"][world] = 0

                    if tool not in self.tools_usage[user]["tool_counts"]:
                        self.tools_usage[user]["tool_counts"][tool] = {}

                    if world not in self.tools_usage[user]["tool_counts"][tool]:
                        self.tools_usage[user]["tool_counts"][tool][world] = 0

                    # Add the current tool usage timestamp to the list
                    self.tools_usage[user]["tool_usage_timestamps"].append(timestamp)
                    self.tools_usage[user]["world_tool_counts"][world] += 1
                    self.tools_usage[user]["tool_counts"][tool][world] += 1

                    # Update the last tool use time
                    self.tools_usage[user]["last_tool_use_time"] = timestamp

                    # Keep only the tool usages from the last 1 minute
                    current_time = datetime.now(central_tz).timestamp()
                    self.tools_usage[user]["tool_usage_timestamps"] = [
                        t for t in self.tools_usage[user]["tool_usage_timestamps"] if current_time - t <= 60
                    ]

                    # Check if the world is "mars" or "sdp7"
                    if world.lower() in ["mars", "sdp7"]:
                        trigger_message = f"{user} has used {tool} in {world}."
                        self.triggers_list.append((trigger_message, user, 2))
                        print(trigger_message)

        self.check_tool_use_counts()


        # Process the recorded usage to trigger events or logging
        for user, data in self.tools_usage.items():
            current_world = data.get("current_world")
            worlds_visited = data.get("worlds_visited")

            world_tool_key = f"tool_count_{current_world}"
            data.setdefault(world_tool_key, 0)  # Initialize if not already set

            # =============================================================================
            # Check for no tools used by and since 3rd world
            # =============================================================================

            for _, row in self.commands.iterrows():
                if row["username"] == user:
                    message = row["message"]
                    for tool in multi_use_tools + single_use_tools:
                        if f"/{tool}" in message:
                            if "tool_use_count" not in self.tools_usage[user]:
                                self.tools_usage[user]["tool_use_count"] = 0
                            self.tools_usage[user]["tool_use_count"] += 1
                            tool_key = f"{tool}_{current_world}"
                            self.tools_usage[user].setdefault(tool_key, 0)
                            self.tools_usage[user][tool_key] += 1
                            data[world_tool_key] += (
                                1  # for high tool use in particular world count
                            )

            # Check for third world visit without tool usage
            '''
            if len(worlds_visited) >= 3 and data.get("tool_use_count", 0) == 0:
                trigger_key = f"not_used_tools_since_third_{current_world}"
                if not data.get(trigger_key, False):
                    trigger_message = ""
                    if len(worlds_visited) == 3:
                        trigger_message = (
                            f"{user} has visited 3 worlds without using any tools."
                        )
                        print(f"{user} has visited 3 worlds without using any tools.")
                    elif len(worlds_visited) > 3:
                        trigger_message = f"{user} has visited {len(worlds_visited)} worlds without using any tools."
                        print(
                            f"{user} has visited {len(worlds_visited)} without using any tools."
                        )

                    self.triggers_list.append((trigger_message, user, 2))
                    self.tools_usage[user][trigger_key] = True
            '''
            # === Trigger: no_tool_use_by_third_world ===
            trigger_name = "no_tool_use_by_third_world"
            enabled, priority, category = get_trigger_settings(trigger_name)

            if enabled:
                if len(worlds_visited) >= 3 and data.get("tool_use_count", 0) == 0:
                    trigger_key = f"not_used_tools_since_third_{current_world}"
                    if not data.get(trigger_key, False):
                        trigger_message = ""
                        if len(worlds_visited) == 3:
                            trigger_message = f"{user} has visited 3 worlds without using any tools. Category: {category}"
                        elif len(worlds_visited) > 3:
                            trigger_message = f"{user} has visited {len(worlds_visited)} worlds without using any tools. Category: {category}"

                        if trigger_message:
                            print(trigger_message)
                            self.triggers_list.append((trigger_message, user, priority))
                            self.tools_usage[user][trigger_key] = True
            else:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")


            # =============================================================================
            # /Check for no tools used by and since 3rd world
            # =============================================================================

            # =============================================================================
            # Check for high tools use (>10 first 3 worlds, >5 succeeding worlds)
            # =============================================================================

            '''
            # Trigger conditions for high tool use
            high_use_trigger_key = f"high_use_{current_world}"
            if (
                len(worlds_visited) <= 3
                and data[world_tool_key] > 10
                and not data.get(high_use_trigger_key, False)
            ):
                print(
                    f"{user} has high tool use in the first three worlds: {current_world}"
                )
                self.triggers_list.append(
                    (f"{user} has high tool use in {current_world}", user, 7)
                )
                data[high_use_trigger_key] = True
            elif (
                len(worlds_visited) > 3
                and data[world_tool_key] > 5
                and not data.get(high_use_trigger_key, False)
            ):
                print(f"{user} has high tool use in subsequent worlds: {current_world}")
                self.triggers_list.append(
                    (f"{user} has high tool use in {current_world}", user, 7)
                )
                data[high_use_trigger_key] = True
            '''

            trigger_name = "check_high_tool_use"
            enabled, priority, category = get_trigger_settings(trigger_name)

            for user, data in self.tools_usage.items():
                worlds_visited = data.get("worlds_visited", [])
                current_world = data.get("current_world")
                world_tool_key = f"tool_count_{current_world}"
                data.setdefault(world_tool_key, 0)

                if not enabled:
                    print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                    continue

                high_use_trigger_key = f"high_use_{current_world}"
                tool_count = data.get(world_tool_key, 0)

                if (
                        len(worlds_visited) <= 3
                        and tool_count > 10
                        and not data.get(high_use_trigger_key, False)
                ):
                    trigger_message = f"{user} has high tool use in the first three worlds: {current_world}. Category: {category}"
                    print(trigger_message)
                    self.triggers_list.append((trigger_message, user, priority))
                    data[high_use_trigger_key] = True

                elif (
                        len(worlds_visited) > 3
                        and tool_count > 5
                        and not data.get(high_use_trigger_key, False)
                ):
                    trigger_message = f"{user} has high tool use in subsequent worlds: {current_world}. Category: {category}"
                    print(trigger_message)
                    self.triggers_list.append((trigger_message, user, priority))
                    data[high_use_trigger_key] = True


            # =============================================================================
            # /Check for high tools use (>10 first 3 worlds, >5 succeeding worlds)
            # =============================================================================

            '''
            if current_world:
                # this is for the combined use of gravity, pressure, & atmosphere in a single world
                # neithan set to true then negate if not found to be true during iteration
                combined_use_flag = True
                combined_key = f"combined_{current_world}_flag"

                if data.get(combined_key, 0) == 0:
                    for tool in multi_use_tools:
                        tool_key = f"{tool}_{current_world}"
                        tool_count = data.get(tool_key, 0)
                        tool_flag_key = f"{tool_key}_flag"
                        tool_flag = data.get(tool_flag_key, 0)

                        if tool_count < 2:
                            combined_use_flag = False
                        if tool_count == 2 and tool_flag == 0:
                            print(
                                f"{user} has used '/{tool}' more than once in {current_world}"
                            )
                            self.triggers_list.append(
                                (
                                    f"{user} has used '/{tool}' more than once in {current_world}",
                                    user,
                                    4,
                                )
                            )
                            self.tools_usage[user][tool_flag_key] = 1

                    # Check for combined use of pressure, gravity, and atmosphere
                    if combined_use_flag:
                        print(
                            f"{user} has combined use of pressure, gravity, & atmosphere in {current_world} more than once"
                        )
                        self.triggers_list.append(
                            (
                                f"Combined use of pressure, gravity, & atmosphere in {current_world} more than once",
                                user,
                                4,
                            )
                        )
                        self.tools_usage[user][combined_key] = 1

                for tool in single_use_tools:
                    tool_key = f"{tool}_{current_world}"
                    tool_count = data.get(tool_key, 0)
                    tool_flag_key = f"{tool_key}_flag"
                    tool_flag = data.get(tool_flag_key, 0)

                    if tool_count == 1 and tool_flag == 0:
                        print(f"{user} has used '/{tool}' in {current_world}")
                        self.triggers_list.append(
                            (f"{user} has used '/{tool}' in {current_world}", user, 7)
                        )
                        self.tools_usage[user][tool_flag_key] = 1
            '''

            # Combined multi-use tool check
            multi_trigger = "check_combined_multi_use_tools"
            multi_enabled, multi_priority, category = get_trigger_settings(multi_trigger)

            # Single-use tool check
            single_trigger = "check_single_use_tools"
            single_enabled, single_priority, category = get_trigger_settings(single_trigger)

            if current_world:
                combined_use_flag = True
                combined_key = f"combined_{current_world}_flag"

                if multi_enabled and data.get(combined_key, 0) == 0:
                    for tool in multi_use_tools:
                        tool_key = f"{tool}_{current_world}"
                        tool_count = data.get(tool_key, 0)
                        tool_flag_key = f"{tool_key}_flag"
                        tool_flag = data.get(tool_flag_key, 0)

                        if tool_count < 2:
                            combined_use_flag = False
                        if tool_count == 2 and tool_flag == 0:
                            print(f"{user} has used '/{tool}' more than once in {current_world}.")
                            self.triggers_list.append(
                                (f"{user} has used '/{tool}' more than once in {current_world}. Category: {category}", user, multi_priority)
                            )
                            self.tools_usage[user][tool_flag_key] = 1

                    if combined_use_flag:
                        print(f"{user} has combined use of pressure, gravity, & atmosphere in {current_world} more than once")
                        self.triggers_list.append(
                            (f"Combined use of pressure, gravity, & atmosphere in {current_world} more than once. Category: {category}", user, multi_priority)
                        )
                        self.tools_usage[user][combined_key] = 1
                elif not multi_enabled:
                    print(f"\033[90mSkipping {multi_trigger} (priority {multi_priority}) for {user} — disabled in Trigger Manager.\033[0m")

                if single_enabled:
                    for tool in single_use_tools:
                        tool_key = f"{tool}_{current_world}"
                        tool_count = data.get(tool_key, 0)
                        tool_flag_key = f"{tool_key}_flag"
                        tool_flag = data.get(tool_flag_key, 0)

                        if tool_count == 1 and tool_flag == 0:
                            print(f"{user} has used '/{tool}' in {current_world}")
                            self.triggers_list.append(
                                (f"{user} has used '/{tool}' in {current_world}", user, single_priority)
                            )
                            self.tools_usage[user][tool_flag_key] = 1
                else:
                    print(f"\033[90mSkipping {single_trigger} (priority {single_priority}) for {user} — disabled in Trigger Manager.\033[0m")


    '''
    def check_3_tools_in_1_minute(self):
        for user, data in self.tools_usage.items():
            tool_usage_timestamps = data.get("tool_usage_timestamps", [])
            if len(tool_usage_timestamps) >= 3:
                trigger_message = f"{user} has used at least 3 tools in less than a minute."
                self.triggers_list.append((trigger_message, user, 7))
                print(trigger_message)
                # Clear the tool usage timestamps to avoid repeated triggers
                self.tools_usage[user]["tool_usage_timestamps"] = []
    '''

    def check_3_tools_in_1_minute(self):
        trigger_name = "check_3_tools_in_1_minute"
        enabled, priority, category = get_trigger_settings(trigger_name)

        for user, data in self.tools_usage.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            tool_usage_timestamps = data.get("tool_usage_timestamps", [])
            if len(tool_usage_timestamps) >= 3:
                trigger_message = f"{user} has used at least 3 tools in less than a minute. Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)
                self.tools_usage[user]["tool_usage_timestamps"] = []  # avoid repeated triggers

    '''    
    def check_racing_non_stopping(self):
        # ====================================================
        # //TRIGGER PARAM: check_racing_non_stopping
        # X <-  intervals of positions recorded. 20 means we record 20 intervals and decide if the player is racing or not.
        #       each interval ticks every 3 seconds. 20 intervals means we observ the change in loc for 60 seconds
        #       then decide if it's racing or not. The latest 20 intervals are used for the decision if there are more than 
        #       20 intervals recorded. 
        # ====================================================

        X = 20 # this means 20 intervals based on the summer2024 trigger slides, change to taste.
        for user, data in self.tools_usage.items():
            recent_positions = data.get('recent_positions', [])
            # if len(recent_positions) < 20:
            if len(recent_positions) < X:
                continue  # Skip if there are less than 20 positions
                
            if len(recent_positions) > X+1:
                self.tools_usage[user]['recent_positions'] = []

            stops = 0
            for i in range(1, len(recent_positions)):
                prev_x, prev_z = recent_positions[i - 1]
                curr_x, curr_z = recent_positions[i]
                distance = abs(curr_x - prev_x) + abs(curr_z - prev_z)
                if distance < 10:
                    stops += 1

            if stops < 2:
                trigger_message = f"{user} has less than 2 stops in the last 20 intervals (racing/non-stopping)."
                self.triggers_list.append((trigger_message, user, 6))
                print(trigger_message)   
                self.tools_usage[user]['recent_positions'] = []
    '''

    def check_racing_non_stopping(self):
        trigger_name = "check_racing_non_stopping"
        enabled, priority, category = get_trigger_settings(trigger_name)

        for user, data in self.tools_usage.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            X = 20  # Number of intervals (each is 3 seconds apart)
            recent_positions = data.get('recent_positions', [])

            if len(recent_positions) < X:
                continue  # Not enough data yet

            if len(recent_positions) > X + 1:
                self.tools_usage[user]['recent_positions'] = []  # Clear if too many

            stops = 0
            for i in range(1, len(recent_positions)):
                prev_x, prev_z = recent_positions[i - 1]
                curr_x, curr_z = recent_positions[i]
                distance = abs(curr_x - prev_x) + abs(curr_z - prev_z)
                if distance < 10:
                    stops += 1

            if stops < 2:
                trigger_message = f"{user} has less than 2 stops in the last 20 intervals (racing/non-stopping). Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)
                self.tools_usage[user]['recent_positions'] = []  # Reset after triggering


    def update_positions_every_3_seconds(self):
        while True:

            self.fetch_data_playersonly() # re-fetch the player position data.

            for _, row in self.players.iterrows():
                user = row['online_user']
                current_world = row['world']
                x = row['x']
                z = row['z']

                # this thread will fire first when ran technically so ensure a new user is instantiated in case.
                # Initialize the user if not present
                if user not in self.tools_usage:
                    self.tools_usage[user] = {
                        "worlds_visited": [],
                        "current_world": "",
                        "tool_use_count": 0,
                        "total_observation_count": 0,
                        "world_observation_counts": {},
                        "last_observation_time": 0,
                        "mynoa_start_time": None,
                        "mynoa_trigger_fired": False,
                        "recent_positions": [],
                        "recent_observations": [],
                        "tool_usage_timestamps": [],
                        "last_tool_use_time": 0,
                    }

                if 'recent_positions' not in self.tools_usage[user]:
                    self.tools_usage[user]['recent_positions'] = []

                # Only append if the new position is different from the last recorded position
                if not self.tools_usage[user]['recent_positions'] or self.tools_usage[user]['recent_positions'][-1] != (x, z):
                    self.tools_usage[user]['recent_positions'].append((x, z))

                    # Keep only the last 20 positions
                    if len(self.tools_usage[user]['recent_positions']) > 20:
                        self.tools_usage[user]['recent_positions'].pop(0)

            sleep(3)  # Wait for 3 seconds before updating again

    '''
    def check_long_pair_close(self):
        
        # ====================================================
        # //TRIGGER PARAM: check_long_pair_close
        # proximity_threshold <- Define how far players need to be (in blocks)
        # duration_increment <- Increment duration by 3 seconds if beyond proximity
        # duration_threshold <- Define the duration threshold (in seconds)
        # explanation:  duration increment of 3 and duration threshold of 9 means the trigger will fire in 30 seconds
        #               because the on_wake code fires every 10 seconds (3 + 3 + 3 duration increment == 10 + 10 + 10 secs)
        # ====================================================
        
        proximity_threshold = 35  # Define how close players need to be (in blocks)
        duration_increment = 10  # Increment duration by 3 seconds if within proximity
        duration_threshold = 120  # Define the duration threshold (in seconds)
        
        # Dictionary to keep track of pairs and their close duration
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        # Iterate over all players and calculate distances
        for i, row_i in self.players.iterrows():
            user_i = row_i['online_user']
            world_i = row_i['world']
            x_i, z_i = row_i['x'], row_i['z']

            for j, row_j in self.players.iterrows():
                if i >= j:
                    continue  # Skip pairs we've already checked or self-pairs
                user_j = row_j['online_user']
                world_j = row_j['world']
                x_j, z_j = row_j['x'], row_j['z']

                # Only consider players in the same world
                if world_i == world_j:
                    distance = ((x_i - x_j) ** 2 + (z_i - z_j) ** 2) ** 0.5
                    if distance <= proximity_threshold:
                        # Increment the close duration for this pair
                        self.pair_durations[(user_i, user_j)] += duration_increment

        # Check if any pair has been close for longer than the duration threshold
        for (user_i, user_j), duration in self.pair_durations.items():
            if duration >= duration_threshold:
                trigger_message = f"{user_i} and {user_j} have been close to each other for more than 120 seconds."
                self.triggers_list.append((trigger_message, user_i, 5))
                # self.triggers_list.append((trigger_message, user_j, 1)) # comment this out if we want to duplicate the trigger
                print(trigger_message)
                # Reset duration to avoid repeated triggers
                self.pair_durations[(user_i, user_j)] = 0
            else: 
                print ()

    '''

    def check_long_pair_close(self):
        trigger_name = "check_long_pair_close"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # ====================================================
        # //TRIGGER PARAMS
        proximity_threshold = 35      # in blocks
        duration_increment = 10       # in seconds (on each loop)
        duration_threshold = 120      # trigger after this many seconds
        # ====================================================

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for i, row_i in self.players.iterrows():
            user_i = row_i['online_user']
            world_i = row_i['world']
            x_i, z_i = row_i['x'], row_i['z']

            for j, row_j in self.players.iterrows():
                if i >= j:
                    continue
                user_j = row_j['online_user']
                world_j = row_j['world']
                x_j, z_j = row_j['x'], row_j['z']

                if world_i == world_j:
                    distance = ((x_i - x_j) ** 2 + (z_i - z_j) ** 2) ** 0.5
                    if distance <= proximity_threshold:
                        self.pair_durations[(user_i, user_j)] += duration_increment

        for (user_i, user_j), duration in self.pair_durations.items():
            if duration >= duration_threshold:
                trigger_message = f"{user_i} and {user_j} have been close to each other for more than 120 seconds. Category: {category}"
                self.triggers_list.append((trigger_message, user_i, priority))
                print(trigger_message)
                self.pair_durations[(user_i, user_j)] = 0

    '''
    def check_long_far_from_crowd(self):
    
        # ====================================================
        # //TRIGGER PARAM: check_long_far_from_crowd
        # proximity_threshold <- Define how far players need to be (in blocks)
        # duration_increment <- Increment duration by 3 seconds if beyond proximity
        # duration_threshold <- Define the duration threshold (in seconds)
        # explanation:  duration increment of 3 and duration threshold of 9 means the trigger will fire in 30 seconds
        #               because the on_wake code fires every 10 seconds (3 + 3 + 3 duration increment == 10 + 10 + 10 secs)
        # ====================================================
    
        proximity_threshold = 35  # Define how far players need to be (in blocks)
        duration_increment = 10  # Increment duration by 3 seconds if beyond proximity
        duration_threshold = 120  # Define the duration threshold (in seconds)

        # Dictionary to keep track of players and their far from crowd duration
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        # Iterate over all players to calculate distances
        for i, row_i in self.players.iterrows():
            user_i = row_i['online_user']
            world_i = row_i['world']
            x_i, z_i = row_i['x'], row_i['z']
            far_from_crowd = True

            for j, row_j in self.players.iterrows():
                if i == j:
                    continue  # Skip self-pairs
                user_j = row_j['online_user']
                world_j = row_j['world']
                x_j, z_j = row_j['x'], row_j['z']

                # Only consider players in the same world
                if world_i == world_j:
                    distance = ((x_i - x_j) ** 2 + (z_i - z_j) ** 2) ** 0.5
                    if distance <= proximity_threshold:
                        far_from_crowd = False
                        break  # Exit the inner loop if any player is within proximity

            if far_from_crowd:
                # Increment the far from crowd duration for this player
                self.tools_usage[user_i].setdefault('far_from_crowd_duration', 0)
                self.tools_usage[user_i]['far_from_crowd_duration'] += duration_increment
            else:
                self.tools_usage[user_i]['far_from_crowd_duration'] = 0  # Reset if not far

        # Check if any player has been far from the crowd for longer than the duration threshold
        for user, data in self.tools_usage.items():
            far_from_crowd_duration = data.get('far_from_crowd_duration', 0)
            if far_from_crowd_duration >= duration_threshold:
                trigger_message = f"{user} has been far from the crowd for more than 120 seconds."
                self.triggers_list.append((trigger_message, user, 4))
                # print(trigger_message)
                # Reset duration to avoid repeated triggers
                self.tools_usage[user]['far_from_crowd_duration'] = 0
            else:
                print ()
                
                
    '''

    def check_long_far_from_crowd(self):
        trigger_name = "check_long_far_from_crowd"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # ====================================================
        proximity_threshold = 35
        duration_increment = 10
        duration_threshold = 120
        # ====================================================

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for i, row_i in self.players.iterrows():
            user_i = row_i['online_user']
            world_i = row_i['world']
            x_i, z_i = row_i['x'], row_i['z']
            far_from_crowd = True

            for j, row_j in self.players.iterrows():
                if i == j:
                    continue
                user_j = row_j['online_user']
                world_j = row_j['world']
                x_j, z_j = row_j['x'], row_j['z']

                if world_i == world_j:
                    distance = ((x_i - x_j) ** 2 + (z_i - z_j) ** 2) ** 0.5
                    if distance <= proximity_threshold:
                        far_from_crowd = False
                        break

            if far_from_crowd:
                self.tools_usage[user_i].setdefault('far_from_crowd_duration', 0)
                self.tools_usage[user_i]['far_from_crowd_duration'] += duration_increment
            else:
                self.tools_usage[user_i]['far_from_crowd_duration'] = 0

        for user, data in self.tools_usage.items():
            far_from_crowd_duration = data.get('far_from_crowd_duration', 0)
            if far_from_crowd_duration >= duration_threshold:
                trigger_message = f"{user} has been far from the crowd for more than 120 seconds. Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)
                self.tools_usage[user]['far_from_crowd_duration'] = 0

    '''
    def check_prolonged_interaction_npc(self):
    
        # ====================================================
        # //TRIGGER PARAM: check_prolonged_interaction_npc
        # interaction_threshold <- the distance in blocks (manhattan distance) of the player to the npc to be considered near
        # duration threshold <- in seconds, threshold to activate trigger
        # disabled_worlds <- include worlds here where the trigger must not activate
        # ====================================================
        
        interaction_threshold = 4  # Distance threshold to consider interaction
        duration_threshold = 60  # Duration threshold in seconds
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()
        disabled_worlds = ["LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen", "TiltedEarth_Melting", "Mynoa_half", "BrownDwarf"]

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue  # Skip disabled worlds

            interacting_with_npc = False

            for object_name, details in world_coordinates_dictionary.get(current_world, {}).get("NPCs", {}).items():
                npc_x, npc_z = details["x"], details["z"]
                distance = abs(x - npc_x) + abs(z - npc_z)  # Manhattan distance

                # print(f"Checking distance between {user} and {object_name}: {distance}")

                if distance < interaction_threshold:
                    interacting_with_npc = True
                    if "npc_interaction_start" not in self.tools_usage[user]:
                        self.tools_usage[user]["npc_interaction_start"] = current_time
                        print(f"Setting npc_interaction_start for {user} at {current_time}")
                    interaction_start_time = self.tools_usage[user]["npc_interaction_start"]
                    if current_time - interaction_start_time >= duration_threshold:
                        trigger_message = f"{user} has been interacting with NPC {object_name} for more than 60 seconds."
                        self.triggers_list.append((trigger_message, user, 3))
                        print(trigger_message)
                        # Reset interaction start time to avoid repeated triggers
                        self.tools_usage[user]["npc_interaction_start"] = current_time - 50  # 10 seconds breathing time
                    else:
                        # print("User")
                        # print(user)
                        # print("NPC")
                        # print(object_name)
                        # print("Time")
                        print(current_time, interaction_start_time, current_time - interaction_start_time)
                    break

            if not interacting_with_npc:
                if "npc_interaction_start" in self.tools_usage[user]:
                    print(f"Removing npc_interaction_start for {user} as they moved away from all NPCs")
                self.tools_usage[user].pop("npc_interaction_start", None)
    '''

    def check_multiple_npc_visits(self):
        trigger_name = "check_multiple_npc_visits"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # =========================================
        visit_distance_threshold = 4
        minutes_window = 5
        required_unique_npcs = 2
        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_half", "BrownDwarf"
        ]
        # =========================================

        window_seconds = minutes_window * 60
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            nearby_npc_name = None
            for npc_name, details in world_coordinates_dictionary.get(current_world, {}).get("NPCs", {}).items():
                npc_x, npc_z = details["x"], details["z"]
                if abs(x - npc_x) + abs(z - npc_z) < visit_distance_threshold:
                    nearby_npc_name = npc_name
                    break

            if nearby_npc_name:
                if "npc_visit_log" not in self.tools_usage[user]:
                    self.tools_usage[user]["npc_visit_log"] = []

                # Add new visit if not duplicate in last 10s (debounce)
                visit_log = self.tools_usage[user]["npc_visit_log"]
                already_logged = any(
                    visit["npc"] == nearby_npc_name and current_time - visit["time"] < 10
                    for visit in visit_log
                )
                if not already_logged:
                    visit_log.append({"npc": nearby_npc_name, "time": current_time})
                    print(f"Logged {user}'s visit to NPC {nearby_npc_name} at {current_time}")

                # Prune visits older than window
                self.tools_usage[user]["npc_visit_log"] = [
                    visit for visit in visit_log if current_time - visit["time"] <= window_seconds
                ]

                # Count unique NPCs
                unique_npcs = {visit["npc"] for visit in self.tools_usage[user]["npc_visit_log"]}
                if len(unique_npcs) >= required_unique_npcs:
                    msg = f"{user} visited {len(unique_npcs)} different NPCs in the last {minutes_window} minutes. Category: {category}"
                    self.triggers_list.append((msg, user, priority))
                    print(msg)

                    # Prevent spamming by removing old visits
                    self.tools_usage[user]["npc_visit_log"] = [
                        visit for visit in self.tools_usage[user]["npc_visit_log"]
                        if visit["npc"] not in unique_npcs
                    ]


    def check_use_of_disabled_mc_commands(self):
        trigger_name = "check_use_of_disabled_mc_commands"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        command_data = self.co_command_with_worlds
        if command_data.empty:
            return

        trigger_commands = [
            "/kill", "/agent", "/gamemode", "/op", "/summon", "/tp", "/give", "/ban", "/kick"
        ]

        excluded_worlds = ["hub", "earthcontrol", "etlife", "rocketlaunch", "play", "mars"]

        flagged = command_data[
            command_data['message'].str.startswith(tuple(trigger_commands)) &
            ~command_data['world'].isin(excluded_worlds)
            ]

        for _, row in flagged.iterrows():
            username = row['username']
            command = row['message']
            world = row['world']
            msg = f"{username} tried '{command}' in world '{world}'. Category: {category}"
            self.triggers_list.append((msg, username, priority))
            print(msg)



    def check_ignores_nearby_npc(self):
        trigger_name = "check_ignores_nearby_npc"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # ====================================================
        proximity_threshold = 5
        ignore_duration = 10
        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_half", "BrownDwarf"
        ]
        # ====================================================

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            near_npc_but_no_engagement = False

            for npc_name, npc_data in world_coordinates_dictionary.get(current_world, {}).get("NPCs", {}).items():
                npc_x, npc_z = npc_data["x"], npc_data["z"]
                distance = abs(x - npc_x) + abs(z - npc_z)

                if distance <= proximity_threshold:
                    near_npc_but_no_engagement = True

                    key = f"near_{npc_name}_no_engage"
                    if key not in self.tools_usage[user]:
                        self.tools_usage[user][key] = current_time
                        print(f"Tracking {user} near NPC {npc_name} at {current_time}")
                    elif current_time - self.tools_usage[user][key] >= ignore_duration:
                        trigger_message = (
                            f"{user} has been within {proximity_threshold} blocks of NPC {npc_name} "
                            f"for {ignore_duration} seconds but has not interacted. Category: {category}"
                        )
                        self.triggers_list.append((trigger_message, user, priority))
                        print(trigger_message)
                        # Add breathing room so it doesn't re-trigger every second
                        self.tools_usage[user][key] = current_time - 5
                    break

            if not near_npc_but_no_engagement:
                # Clear all related NPC keys for this user
                for npc_name in world_coordinates_dictionary.get(current_world, {}).get("NPCs", {}).keys():
                    key = f"near_{npc_name}_no_engage"
                    self.tools_usage[user].pop(key, None)


    def check_prolonged_interaction_npc(self):
        trigger_name = "check_prolonged_interaction_npc"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # ====================================================
        interaction_threshold = 4
        duration_threshold = 60
        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_half", "BrownDwarf"
        ]
        # ====================================================

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            interacting_with_npc = False

            for object_name, details in world_coordinates_dictionary.get(current_world, {}).get("NPCs", {}).items():
                npc_x, npc_z = details["x"], details["z"]
                distance = abs(x - npc_x) + abs(z - npc_z)

                if distance < interaction_threshold:
                    interacting_with_npc = True
                    if "npc_interaction_start" not in self.tools_usage[user]:
                        self.tools_usage[user]["npc_interaction_start"] = current_time
                        print(f"Setting npc_interaction_start for {user} at {current_time}")
                    interaction_start_time = self.tools_usage[user]["npc_interaction_start"]
                    if current_time - interaction_start_time >= duration_threshold:
                        trigger_message = f"{user} has been interacting with NPC {object_name} for more than 60 seconds. Category: {category}"
                        self.triggers_list.append((trigger_message, user, priority))
                        print(trigger_message)
                        self.tools_usage[user]["npc_interaction_start"] = current_time - 50  # breathing room
                    else:
                        # print(current_time, interaction_start_time, current_time - interaction_start_time)
                        print (f"{user} has possible npc interaction. Waiting to reach threshold.")

                    break

            if not interacting_with_npc:
                if "npc_interaction_start" in self.tools_usage[user]:
                    print(f"Removing npc_interaction_start for {user} as they moved away from all NPCs")
                self.tools_usage[user].pop("npc_interaction_start", None)

    '''
    def check_prolonged_stay_poi(self):
        
        # ====================================================
        # //TRIGGER PARAM: check_prolonged_stay_poi
        # duration_threshold <- in seconds, sets the amount of time the player must stay in POI for trigger to activate
        # disabled_worlds <- include worlds here where the trigger must not activate
        # ====================================================
    
        duration_threshold = 90  # Duration threshold in seconds
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()
        disabled_worlds = ["LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen", "TiltedEarth_Melting", "Mynoa_close", "Mynoa_half", "Cancri", "BrownDwarf"]

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue  # Skip disabled worlds

            staying_in_poi = False

            for object_type, objects in world_coordinates_dictionary.get(current_world, {}).items():
                if object_type == "Global":
                    continue  # Skip global actions

                for object_name, details in objects.items():
                    if "range" in details:
                        if self.is_point_inside_space(x, z, details["range"]):
                            staying_in_poi = True
                            if "poi_stay_start" not in self.tools_usage[user]:
                                self.tools_usage[user]["poi_stay_start"] = current_time
                                # print(f"Setting poi_stay_start for {user} at {current_time}")
                            poi_stay_start_time = self.tools_usage[user]["poi_stay_start"]
                            if current_time - poi_stay_start_time >= duration_threshold:
                                trigger_message = f"{user} has been within POI {object_name} for more than 90 seconds."
                                self.triggers_list.append((trigger_message, user, 3))
                                # print(trigger_message)
                                # Reset stay start time to avoid repeated triggers
                                self.tools_usage[user]["poi_stay_start"] = current_time - 80  # 10 seconds breathing time
                            else:
                                # print("User")
                                # print(user)
                                # print("POI")
                                # print(object_name)
                                # print("Time")
                                print(current_time, poi_stay_start_time, current_time - poi_stay_start_time)
                            break

            if not staying_in_poi:
                if "poi_stay_start" in self.tools_usage[user]:
                    print(f"Removing poi_stay_start for {user} as they moved away from all POIs")
                self.tools_usage[user].pop("poi_stay_start", None)
    '''

    def check_world_exploration(self):
        trigger_name = "check_world_exploration"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        eligible_worlds = ["MynoaMangrove", "ColderStrip", "ColderHot", "ColderCold", "TwoMoons", "TwoMoonsLow", "TiltedWarm", "TitledEarthMelting", "TiltedFrozen"]
        threshold = 0.1  # Minimum absolute x-value to count as "side visit"

        # self.tools_usage will now store:
        # tools_usage[username]["visited_sides"][world] = {"positive": True/False, "negative": True/False}
        # self.tools_usage[username]["explored_worlds"] = set()

        for _, row in self.players.iterrows():
            username = row["online_user"]
            world = row["world"]
            x = row["x"]

            if world not in eligible_worlds:
                continue

            # Init structures if not present
            if "visited_sides" not in self.tools_usage[username]:
                self.tools_usage[username]["visited_sides"] = {}
            if "explored_worlds" not in self.tools_usage[username]:
                self.tools_usage[username]["explored_worlds"] = set()

            if world not in self.tools_usage[username]["visited_sides"]:
                self.tools_usage[username]["visited_sides"][world] = {"positive": False, "negative": False}

            side_data = self.tools_usage[username]["visited_sides"][world]

            # Mark side visit
            if x >= threshold:
                side_data["positive"] = True
            elif x <= -threshold:
                side_data["negative"] = True

            # If both sides visited, and not yet credited
            if side_data["positive"] and side_data["negative"] and world not in self.tools_usage[username]["explored_worlds"]:
                # Init if first time
                if not hasattr(self, "world_explorers"):
                    self.world_explorers = defaultdict(list)

                if username not in self.world_explorers[world]:
                    if len(self.world_explorers[world]) < 3:
                        self.world_explorers[world].append(username)
                        self.tools_usage[username]["explored_worlds"].add(world)

                        msg = f"{username} is among the first explorers to visit both sides of {world}. Category: {category}"
                        self.triggers_list.append((msg, username, priority))
                        print(msg)


    def check_tool_inspiration_specific(self):
        trigger_name = "check_tool_inspiration_specific"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = self.science_tools
        if df.empty or len(df) < 2:
            print(f"\033[90m{trigger_name}: Not enough science tool data.\033[0m")
            return

        df = df.sort_values(by="time").reset_index(drop=True)

        for i in range(1, len(df)):
            current = df.iloc[i]
            for j in range(i):
                previous = df.iloc[j]
                if (
                        current["username"] != previous["username"]
                        and current["world"] == previous["world"]
                        and current["tool"] == previous["tool"]
                ):
                    msg = (
                        f"{current['username']} used {current['tool']} after {previous['username']} used the same tool. "
                        f"Category: {category}"
                    )
                    self.triggers_list.append((msg, current["username"], priority))
                    print(msg)
                    break  # only need the first match


    def check_in_pause_box(self):
        trigger_name = "check_in_pause_box"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        for _, row in self.players.iterrows():
            username = row["online_user"]
            world = row["world"]

            if world == "Hub":
                msg = f"{username} is currently in the pause box (world = Hub). Category: {category}"
                self.triggers_list.append((msg, username, priority))
                print(msg)



    def check_tool_inspiration_generic(self):
        trigger_name = "check_tool_inspiration_generic"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        df = self.science_tools
        if df.empty or len(df) < 2:
            print(f"\033[90m{trigger_name}: Not enough science tool data.\033[0m")
            return

        df = df.sort_values(by="time").reset_index(drop=True)

        for i in range(1, len(df)):
            current = df.iloc[i]
            for j in range(i):
                previous = df.iloc[j]
                if (
                        current["username"] != previous["username"]
                        and current["world"] == previous["world"]
                        and current["tool"] != previous["tool"]  # <— prevent duplicates handled by specific version
                ):
                    msg = (
                        f"{current['username']} used {current['tool']} after {previous['username']} used {previous['tool']} in {current['world']}. "
                        f"Category: {category}"
                    )
                    self.triggers_list.append((msg, current["username"], priority))
                    print(msg)
                    break  # only trigger once per player


    def check_advanced_tool_use(self):
        trigger_name = "check_advanced_tool_use"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        advanced_tools = {
            "COSMIC RAYS",
            "ALTITUDE",
            "SCALE",
            "YEAR",
            "RADIUS",
        }

        df = self.science_tools
        if df.empty:
            print(f"\033[90m{trigger_name}: No science tool data available.\033[0m")
            return

        for _, row in df.iterrows():
            tool = str(row["tool"]).upper()
            user = row["username"]
            world = row["world"]

            if tool in advanced_tools:
                msg = f"{user} used advanced tool {tool} in {world}. Category: {category}"
                self.triggers_list.append((msg, user, priority))
                print(msg)



    '''
    def check_visit_unmarked_pois(self):
        trigger_name = "check_visit_unmarked_pois"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # ====================================================
        duration_threshold = 90  # seconds
        disabled_worlds = [
            "LunarCrater", "TiltedWarm", "TiltedFrozen",
            "TiltedMelting", "MynoaClose", "MynoaHalf", "Cancri", "BrownDwarf"
        ]
        # ====================================================

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            inside_any_poi = False

            for object_type, objects in world_coordinates_dictionary.get(current_world, {}).items():
                if object_type == "Global":
                    continue

                for object_name, details in objects.items():
                    if "range" in details:
                        if self.is_point_inside_space(x, z, details["range"]):
                            inside_any_poi = True
                            break
                if inside_any_poi:
                    break

            if not inside_any_poi:
                if "outside_poi_start" not in self.tools_usage[user]:
                    self.tools_usage[user]["outside_poi_start"] = current_time

                stay_duration = current_time - self.tools_usage[user]["outside_poi_start"]

                if stay_duration >= duration_threshold:
                    trigger_message = f"{user} appears to be exploring an unmarked science-relevant area. Category: {category}"
                    self.triggers_list.append((trigger_message, user, priority))
                    print(trigger_message)

                    self.tools_usage[user]["outside_poi_start"] = current_time - 80
                else:
                    print(f"{user} is outside any POI. Waiting to reach exploration threshold...")
            else:
                if "outside_poi_start" in self.tools_usage[user]:
                    print(f"{user} entered a POI. Clearing outside_poi_start timer.")
                self.tools_usage[user].pop("outside_poi_start", None)
    '''

    def check_visit_unmarked_pois(self):
        trigger_name = "check_visit_unmarked_pois"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        duration_threshold = 300  # changed to 5 mins as per Amanda's email
        cooldown_period = 600  # 10 minutes in seconds

        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_close", "Mynoa_half", "Cancri", "BrownDwarf"
        ]

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            inside_any_poi = False

            for object_type, objects in world_coordinates_dictionary.get(current_world, {}).items():
                if object_type == "Global":
                    continue

                for object_name, details in objects.items():
                    if "range" in details and self.is_point_inside_space(x, z, details["range"]):
                        inside_any_poi = True
                        break
                if inside_any_poi:
                    break

            if not inside_any_poi:
                self.tools_usage.setdefault(user, {})

                if "outside_poi_start" not in self.tools_usage[user]:
                    self.tools_usage[user]["outside_poi_start"] = current_time

                stay_duration = current_time - self.tools_usage[user]["outside_poi_start"]

                last_trigger_time = self.tools_usage[user].get("last_unmarked_poi_trigger", 0)
                cooldown_passed = current_time - last_trigger_time >= cooldown_period

                if stay_duration >= duration_threshold and cooldown_passed:
                    trigger_message = f"{user} has not visited a point of interest in 5 minutes. Category: {category}"
                    self.triggers_list.append((trigger_message, user, priority))
                    print(trigger_message)

                    # Reset timer and update last trigger
                    self.tools_usage[user]["outside_poi_start"] = current_time - 80
                    self.tools_usage[user]["last_unmarked_poi_trigger"] = current_time
                else:
                    print(f"{user} is outside any POI. Waiting to reach exploration threshold or cooldown.")
            else:
                if "outside_poi_start" in self.tools_usage[user]:
                    print(f"{user} entered a POI. Clearing outside_poi_start timer.")
                self.tools_usage[user].pop("outside_poi_start", None)


    def check_prolonged_stay_poi(self):
        trigger_name = "check_prolonged_stay_poi"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        # ====================================================
        duration_threshold = 90
        disabled_worlds = [
            "LunarCrater", "TiltedEarth_JungleIsland", "TiltedEarth_Frozen",
            "TiltedEarth_Melting", "Mynoa_close", "Mynoa_half", "Cancri", "BrownDwarf"
        ]
        # ====================================================

        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()

        for _, row in self.players.iterrows():
            user = row["online_user"]
            current_world = row["world"]
            x, z = row["x"], row["z"]

            if current_world in disabled_worlds:
                continue

            staying_in_poi = False

            for object_type, objects in world_coordinates_dictionary.get(current_world, {}).items():
                if object_type == "Global":
                    continue

                for object_name, details in objects.items():
                    if "range" in details:
                        if self.is_point_inside_space(x, z, details["range"]):
                            staying_in_poi = True
                            if "poi_stay_start" not in self.tools_usage[user]:
                                self.tools_usage[user]["poi_stay_start"] = current_time
                            poi_stay_start_time = self.tools_usage[user]["poi_stay_start"]

                            if current_time - poi_stay_start_time >= duration_threshold:
                                trigger_message = f"{user} has been within POI {object_name} for more than 90 seconds. Category: {category}"
                                self.triggers_list.append((trigger_message, user, priority))
                                print(trigger_message)
                                self.tools_usage[user]["poi_stay_start"] = current_time - 80
                            else:
                                # print(current_time, poi_stay_start_time, current_time - poi_stay_start_time)
                                print(f"{user} is possibly staying in POI. Waiting to reach threshold.")
                            break

            if not staying_in_poi:
                if "poi_stay_start" in self.tools_usage[user]:
                    print(f"Removing poi_stay_start for {user} as they moved away from all POIs")
                self.tools_usage[user].pop("poi_stay_start", None)

    '''
    def check_teleporting_to_multiple_players(self):
        # Fetch the current command data
        command_data = self.co_command

        if command_data.empty:
            return

        # Join with co_user to get the usernames
        co_user_df = self.co_user
        merged_df = pd.merge(command_data, co_user_df, left_on="user", right_on="rowid", suffixes=('', '_user'))
        
        print("merged DF")
        print(merged_df)
        print("Columns in merged DF:", merged_df.columns)

        # Filter commands that start with /tp or /tpa
        tp_commands = merged_df[merged_df['message'].str.startswith(('/tp ', '/tpa '))].copy()

        # Extract the target usernames (B) after /tp or /tpa
        tp_commands.loc[:, 'target_user'] = tp_commands['message'].str.split().str[1]

        # Check if the command has multiple target usernames
        tp_commands['target_users'] = tp_commands['message'].str.split().apply(lambda x: x[1:])
        tp_commands['num_target_users'] = tp_commands['target_users'].apply(len)

        # Trigger if a single /tp or /tpa command has 2 or more target usernames
        for _, row in tp_commands.iterrows():
            if row['num_target_users'] >= 2:
                username = row['user_user']  # Use the correct column name
                target_users = ", ".join(row['target_users'])
                trigger_message = f"{username} tried teleporting to multiple players ({target_users}) in a single command."
                self.triggers_list.append((trigger_message, username, 4))
                print(trigger_message)
    '''

    def check_teleporting_to_multiple_players(self):
        trigger_name = "check_teleporting_to_multiple_players"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        command_data = self.co_command
        if command_data.empty:
            return

        co_user_df = self.co_user
        merged_df = pd.merge(command_data, co_user_df, left_on="user", right_on="rowid", suffixes=('', '_user'))

        # Optional debug output
        # print("merged DF")
        # print(merged_df)
        # print("Columns in merged DF:", merged_df.columns)

        tp_commands = merged_df[merged_df['message'].str.startswith(('/tp ', '/tpa '))].copy()

        tp_commands.loc[:, 'target_user'] = tp_commands['message'].str.split().str[1]
        tp_commands['target_users'] = tp_commands['message'].str.split().apply(lambda x: x[1:])
        tp_commands['num_target_users'] = tp_commands['target_users'].apply(len)

        for _, row in tp_commands.iterrows():
            if row['num_target_users'] >= 2:
                username = row['user_user']  # Correctly resolved column from co_user
                target_users = ", ".join(row['target_users'])
                trigger_message = f"{username} tried teleporting to multiple players ({target_users}) in a single command. Category: {category}"
                self.triggers_list.append((trigger_message, username, priority))
                print(trigger_message)

    '''
    def check_specific_commands(self):
        # Fetch the current command data
        command_data = self.co_command_with_worlds

        if command_data.empty:
            return

        # Define the commands to trigger on
        trigger_commands = [
            "/kill",
            "/enable pvp",
            "/god",
            "/gamemode",
            "/difficulty",
            "/op",
            "/help",
            "/agent chat"
        ]

        # Define the worlds to exclude
        excluded_worlds = ["hub", "earthcontrol", "etlife", "rocketlaunch", "play", "mars"]

        # Filter commands that are in the trigger list and not in the excluded worlds
        specific_commands = command_data[command_data['message'].str.startswith(tuple(trigger_commands)) & ~command_data['world'].isin(excluded_worlds)]

        # Trigger for each matching command
        for _, row in specific_commands.iterrows():
            username = row['username']
            command = row['message']
            world = row['world']
            trigger_message = f"{username} used the command '{command}' in world '{world}'."
            self.triggers_list.append((trigger_message, username, 3))
            print(trigger_message)
    '''

    def check_specific_commands(self):
        trigger_name = "check_specific_commands"
        enabled, priority, category = get_trigger_settings(trigger_name)

        if not enabled:
            print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
            return

        command_data = self.co_command_with_worlds
        if command_data.empty:
            return

        trigger_commands = [
            "/kill",
            "/enable pvp",
            "/god",
            "/gamemode",
            "/difficulty",
            "/op",
            "/help",
            "/agent chat"
        ]

        excluded_worlds = ["hub", "earthcontrol", "etlife", "rocketlaunch", "play", "mars"]

        specific_commands = command_data[
            command_data['message'].str.startswith(tuple(trigger_commands)) &
            ~command_data['world'].isin(excluded_worlds)
            ]

        for _, row in specific_commands.iterrows():
            username = row['username']
            command = row['message']
            world = row['world']
            trigger_message = f"{username} used the command '{command}' in world '{world}'. Category: {category}"
            self.triggers_list.append((trigger_message, username, priority))
            print(trigger_message)

    '''
    def check_five_or_more_observations_in_world(self):
        for user, data in self.tools_usage.items():
            for world, count in data.get("world_observation_counts", {}).items():
                if count >= 5:
                    trigger_key = f"five_observations_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has made 5 or more observations in {world}."
                        self.triggers_list.append((trigger_message, user, 8))
                        print(trigger_message)
                        data[trigger_key] = True

    def check_five_or_more_tools_in_world(self):
        for user, data in self.tools_usage.items():
            for world, count in data.get("world_tool_counts", {}).items():
                if count >= 5:
                    trigger_key = f"five_tools_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has used 5 or more tools in {world}."
                        self.triggers_list.append((trigger_message, user, 8))
                        print(trigger_message)
                        data[trigger_key] = True
    '''

    def check_five_or_more_observations_in_world(self):
        trigger_name = "check_five_or_more_observations_in_world"
        enabled, priority, category = get_trigger_settings(trigger_name)

        for user, data in self.tools_usage.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            for world, count in data.get("world_observation_counts", {}).items():
                if count >= 5:
                    trigger_key = f"five_observations_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has made 5 or more observations in {world}. Category: {category}"
                        self.triggers_list.append((trigger_message, user, priority))
                        print(trigger_message)
                        data[trigger_key] = True

    def check_five_or_more_tools_in_world(self):
        trigger_name = "check_five_or_more_tools_in_world"
        enabled, priority, category = get_trigger_settings(trigger_name)

        for user, data in self.tools_usage.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            for world, count in data.get("world_tool_counts", {}).items():
                if count >= 5:
                    trigger_key = f"five_tools_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has used 5 or more tools in {world}. Category: {category}"
                        self.triggers_list.append((trigger_message, user, priority))
                        print(trigger_message)
                        data[trigger_key] = True

    '''
    def check_five_chat_messages_in_world(self):
        # Merge co_chat and co_user to get usernames
        merged_df = pd.merge(self.co_chat_with_worlds, self.co_user, left_on='user_id', right_on='rowid')



        # Track chat entries for each user in each world
        for _, row in merged_df.iterrows():
            username = row['user']
            world = row['world']
            message = row['message'].strip()

            if username not in self.tools_usage:
                self.tools_usage[username] = {
                    "worlds_visited": [],
                    "current_world": world,
                    "tool_use_count": 0,
                    "total_observation_count": 0,
                    "world_observation_counts": {},
                    "last_observation_time": 0,
                    "mynoa_start_time": None,
                    "mynoa_trigger_fired": False,
                    "recent_positions": [],
                    "recent_observations": [],
                    "tool_usage_timestamps": [],
                    "last_tool_use_time": 0,
                    "world_tool_counts": {},
                    "far_from_crowd_duration": 0,
                    "npc_interaction_start": None,
                    "poi_stay_start": None,
                    "chat_counts": {}  # Initialize chat counts
                }

            if 'chat_counts' not in self.tools_usage[username]:
                self.tools_usage[username]['chat_counts'] = {}

            if world not in self.tools_usage[username]['chat_counts']:
                self.tools_usage[username]['chat_counts'][world] = 0

            # Increment the chat count for the user in the current world
            self.tools_usage[username]['chat_counts'][world] += 1

            # Print the updated chat counts for debugging
            # print(f"Updated chat counts for {username} in {world}: {self.tools_usage[username]['chat_counts'][world]}")

        # Check for users who have sent 5 or more chat messages in the current world
        # ====================================================
        # //TRIGGER PARAM: check_five_chat_messages_in_world
        # chat_count <- number of chats per world to trigger
        # ====================================================
        for user, data in self.tools_usage.items():
            for world, chat_count in data.get('chat_counts', {}).items():
                if chat_count >= 5:
                    trigger_key = f"five_chats_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has sent 5 or more chat messages in {world}."
                        self.triggers_list.append((trigger_message, user, 8))
                        print(trigger_message)
                        data[trigger_key] = True
    '''

    def check_five_chat_messages_in_world(self):
        trigger_name = "check_five_chat_messages_in_world"
        enabled, priority, category = get_trigger_settings(trigger_name)

        # Merge co_chat and co_user to get usernames
        merged_df = pd.merge(self.co_chat_with_worlds, self.co_user, left_on='user_id', right_on='rowid')

        # Track chat entries for each user in each world
        for _, row in merged_df.iterrows():
            username = row['user']
            world = row['world']
            message = row['message'].strip()

            if username not in self.tools_usage:
                self.tools_usage[username] = {
                    "worlds_visited": [],
                    "current_world": world,
                    "tool_use_count": 0,
                    "total_observation_count": 0,
                    "world_observation_counts": {},
                    "last_observation_time": 0,
                    "mynoa_start_time": None,
                    "mynoa_trigger_fired": False,
                    "recent_positions": [],
                    "recent_observations": [],
                    "tool_usage_timestamps": [],
                    "last_tool_use_time": 0,
                    "world_tool_counts": {},
                    "far_from_crowd_duration": 0,
                    "npc_interaction_start": None,
                    "poi_stay_start": None,
                    "chat_counts": {}  # Initialize chat counts
                }

            if 'chat_counts' not in self.tools_usage[username]:
                self.tools_usage[username]['chat_counts'] = {}

            if world not in self.tools_usage[username]['chat_counts']:
                self.tools_usage[username]['chat_counts'][world] = 0

            # Increment the chat count for the user in the current world
            self.tools_usage[username]['chat_counts'][world] += 1

        # Evaluate triggers
        for user, data in self.tools_usage.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            for world, chat_count in data.get('chat_counts', {}).items():
                if chat_count >= 5:
                    trigger_key = f"five_chats_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has sent 5 or more chat messages in {world}. Category: {category}"
                        self.triggers_list.append((trigger_message, user, priority))
                        print(trigger_message)
                        data[trigger_key] = True

    '''
    def check_tool_use_counts(self):
        for user, data in self.tools_usage.items():
            for tool, worlds in data.get("tool_counts", {}).items():
                for world, count in worlds.items():
                    if tool == "gravity" and count > 2:
                        trigger_key = f"gravity_tool_{world}"
                        if not data.get(trigger_key, False):
                            trigger_message = f"{user} has used gravity more than twice in {world}."
                            self.triggers_list.append((trigger_message, user, 4))
                            print(trigger_message)
                            data[trigger_key] = True
                    elif tool != "gravity" and count > 3:
                        trigger_key = f"{tool}_tool_{world}"
                        if not data.get(trigger_key, False):
                            trigger_message = f"{user} has used {tool} more than three times in {world}."
                            self.triggers_list.append((trigger_message, user, 4))
                            print(trigger_message)
                            data[trigger_key] = True
    '''

    def check_tool_use_counts(self):
        trigger_name = "check_tool_use_counts"
        enabled, priority, category = get_trigger_settings(trigger_name)

        for user, data in self.tools_usage.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            for tool, worlds in data.get("tool_counts", {}).items():
                for world, count in worlds.items():
                    if tool == "gravity" and count > 2:
                        trigger_key = f"gravity_tool_{world}"
                        if not data.get(trigger_key, False):
                            trigger_message = f"{user} has used gravity more than twice in {world}. Category: {category}"
                            self.triggers_list.append((trigger_message, user, priority))
                            print(trigger_message)
                            data[trigger_key] = True
                    elif tool != "gravity" and count > 3:
                        trigger_key = f"{tool}_tool_{world}"
                        if not data.get(trigger_key, False):
                            trigger_message = f"{user} has used {tool} more than three times in {world}. Category: {category}"
                            self.triggers_list.append((trigger_message, user, priority))
                            print(trigger_message)
                            data[trigger_key] = True


    '''
    def check_over_200_actions_in_2_minutes(self):
        # Filter to include only destroy (0) and place (1) actions
        filtered_df = self.co_block_with_users[self.co_block_with_users['action'].isin([0, 1])]
        
        # print ("FILTERED_DF")
        # print (filtered_df)
        
        # Count the actions per user
        action_counts = filtered_df['username'].value_counts()
        
        if not action_counts.empty:
            print (f"\033[95m\nBUILD/DESTROY ACTION COUNTS:\033[0m\n{action_counts}\n")

        # Trigger if actions exceed 200 within 2 minutes
        for user, count in action_counts.items():
            # ====================================================
            # //TRIGGER PARAM: check_over_200_actions_in_2_minutes
            # count <- number of build/destroy instances
            # ====================================================
            if count > 200:
                trigger_message = f"{user} has performed over 200 actions (place/destroy) in the last 2 minutes."
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)
    '''

    def check_over_200_actions_in_2_minutes(self):
        trigger_name = "check_over_200_actions_in_2_minutes"
        enabled, priority, category = get_trigger_settings(trigger_name)

        # Filter to include only destroy (0) and place (1) actions
        filtered_df = self.co_block_with_users[self.co_block_with_users['action'].isin([0, 1])]

        # Count the actions per user
        action_counts = filtered_df['username'].value_counts()

        if not action_counts.empty:
            print(f"\033[95m\nBUILD/DESTROY ACTION COUNTS:\033[0m\n{action_counts}\n")

        # Trigger if actions exceed 200 within 2 minutes
        for user, count in action_counts.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            if count > 200:
                trigger_message = f"{user} has performed over 200 actions (place/destroy) in the last 2 minutes. Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)

    '''
    def check_over_200_placed_actions_in_2_minutes(self):
        # Filter to include only place (1) actions and valid usernames
        filtered_df = self.co_block_with_users[(self.co_block_with_users['action'] == 1) &
                                               (~self.co_block_with_users['username'].str.startswith('#'))]

        # Count the place actions per user
        action_counts = filtered_df['username'].value_counts()

        if not action_counts.empty:
            print(f"\033[95m\nPLACE ACTION COUNTS:\033[0m\n{action_counts}\n")

        # Trigger if place actions exceed 200 within 2 minutes
        for user, count in action_counts.items():
            if count > 200:
                trigger_message = f"{user} has placed over 200 blocks in the last 2 minutes."
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)
    '''

    def check_over_200_placed_actions_in_2_minutes(self):
        trigger_name = "check_over_200_placed_actions_in_2_minutes"
        enabled, priority, category = get_trigger_settings(trigger_name)

        # Filter to include only place (1) actions and valid usernames
        filtered_df = self.co_block_with_users[
            (self.co_block_with_users['action'] == 1) &
            (~self.co_block_with_users['username'].str.startswith('#'))
            ]

        # Count the place actions per user
        action_counts = filtered_df['username'].value_counts()

        if not action_counts.empty:
            print(f"\033[95m\nPLACE ACTION COUNTS:\033[0m\n{action_counts}\n")

        # Trigger if place actions exceed 200 within 2 minutes
        for user, count in action_counts.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            if count > 200:
                trigger_message = f"{user} has placed over 200 blocks in the last 2 minutes. Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)

    '''
    def check_over_200_destroyed_actions_in_2_minutes(self):
        # Filter to include only destroy (0) actions and valid usernames
        filtered_df = self.co_block_with_users[(self.co_block_with_users['action'] == 0) &
                                               (~self.co_block_with_users['username'].str.startswith('#'))]

        # Count the destroy actions per user
        action_counts = filtered_df['username'].value_counts()

        if not action_counts.empty:
            print(f"\033[95m\nDESTROY ACTION COUNTS:\033[0m\n{action_counts}\n")

        # Trigger if destroy actions exceed 200 within 2 minutes
        for user, count in action_counts.items():
            if count > 200:
                trigger_message = f"{user} has destroyed over 200 blocks in the last 2 minutes."
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)
    '''

    def check_over_200_destroyed_actions_in_2_minutes(self):
        trigger_name = "check_over_200_destroyed_actions_in_2_minutes"
        enabled, priority, category = get_trigger_settings(trigger_name)

        # Filter to include only destroy (0) actions and valid usernames
        filtered_df = self.co_block_with_users[
            (self.co_block_with_users['action'] == 0) &
            (~self.co_block_with_users['username'].str.startswith('#'))
            ]

        # Count the destroy actions per user
        action_counts = filtered_df['username'].value_counts()

        if not action_counts.empty:
            print(f"\033[95m\nDESTROY ACTION COUNTS:\033[0m\n{action_counts}\n")

        # Trigger if destroy actions exceed 200 within 2 minutes
        for user, count in action_counts.items():
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) for {user} — disabled in Trigger Manager.\033[0m")
                continue

            if count > 200:
                trigger_message = f"{user} has destroyed over 200 blocks in the last 2 minutes. Category: {category}"
                self.triggers_list.append((trigger_message, user, priority))
                print(trigger_message)

    '''
    def check_block_triggers(self):
        current_time = datetime.now(pytz.timezone("America/Chicago")).timestamp()

        for _, trigger_row in self.block_triggers_df.iterrows():
            block_type = trigger_row['type']
            material = trigger_row['material']
            action = trigger_row['action']
            time_window_high = trigger_row['Time_Window_High']
            high_threshold = trigger_row['High_Threshold']
            time_window_low = trigger_row['Time_Window_Low']
            low_threshold = trigger_row['Low_Threshold']
            priority = trigger_row['Priority']

            # Filter the data for the specific block type and action
            filtered_df = self.co_block_with_users[(self.co_block_with_users['type'] == block_type) &
                                                   (self.co_block_with_users['action'] == action) &
                                                   (~self.co_block_with_users['username'].str.startswith('#'))]

            
            # Check for high threshold within the high time window
            if high_threshold != -1:
                high_window_start = current_time - time_window_high

                # Luc added                
                trigger_times = self.prev_trig_time.loc[self.prev_trig_time['Material'] == material]
                trigger_times = trigger_times.loc[trigger_times['Action'] == action]
                trigger_times = trigger_times.loc[trigger_times['H_or_l'] == "high"]
                
                # Luc modified
                
                #high_window_df = filtered_df[filtered_df['time'] >= high_window_start]
                
                # Luc - I tried to make this work without a loop, but couldn't make it work. Could probably be made more efficient
                rows_to_drop = []
                for _, row in filtered_df.iterrows():
                    # (trigger_times.empty) or not (trigger_times.loc[trigger_times['User'] == filtered_df['username']].empty) else trigger_times.loc[trigger_times['User'] == filtered_df['username']]["Time"]
                    window_start_time = high_window_start
                    if (not trigger_times.empty and (not trigger_times.loc[trigger_times['User'] == row['username']].empty)):
                        selected_row = trigger_times.loc[trigger_times['User'] == row['username']]
                        print (selected_row)
                        selected_row_index = selected_row.index.values.astype(int)[0]
                        window_start_time = max(window_start_time, selected_row.loc[selected_row_index]['Time'])
                    
                    # print(f"Original threshold: {high_window_start},    New threshold: {window_start_time}")
                    
                    if row['time'] < window_start_time:
                        rows_to_drop.append(row['rowid'])
                    
                #high_window_df = filtered_df[filtered_df['rowid'] > 0]
                
                #print(f"Rows to drop: {rows_to_drop}")
                #print(f"Before filtering:\n{filtered_df}")
                
                high_window_df = filtered_df[~filtered_df['rowid'].isin(rows_to_drop)]
                
                #print (f"After filtering:\n{high_window_df}")
                
                high_action_counts = high_window_df['username'].value_counts()

                for user, count in high_action_counts.items():
                    if count >= high_threshold:
                        action_type = 'placed' if action == 1 else 'destroyed'
                        trigger_message = f"High Block Usage: {user} has {action_type} {count} {material} blocks in the last {time_window_high} seconds."
                        self.triggers_list.append((trigger_message, user, priority))
                        
                        # Luc need to fix this
                        # if the row exists drop it (or replace one value in it)
                        # othersiwe concat (if modified)
                        # If dropped, always do the concat
                        
                        # Luc - not sure why but I wasn't able to select on multiple columns at the same time. Could probably be made more efficient
                        previous_row = self.prev_trig_time.loc[self.prev_trig_time['Material'] == material]
                        previous_row = previous_row.loc[previous_row['Action'] == action]
                        previous_row = previous_row.loc[previous_row['H_or_l'] == "high"]
                        previous_row = previous_row.loc[previous_row['User'] == user]
                        
                        print (f"Before: {self.prev_trig_time}")
                        
                        if not previous_row.empty:
                            print ("***************** row exists ************")
                            index = self.prev_trig_time[(self.prev_trig_time['Material'] == material) & (self.prev_trig_time['Action'] == action) & (self.prev_trig_time['H_or_l'] == "high") & (self.prev_trig_time['User'] == user)].index.values.astype(int)[0]
                            print (f"Index: {index}")
                            self.prev_trig_time = self.prev_trig_time.drop(index)
                            #previous_row['Time'] = current_time
                            #self.prev_trig_time = self.prev_trig_time[self.prev_trig_time['Material'] != material, self.prev_trig_time['Action'] != action, self.prev_trig_time['H_or_l'] != "high", self.prev_trig_time['User'] == user]
                        
                        print ("**************** concat ****************")
                        self.prev_trig_time = pd.concat([self.prev_trig_time, pd.DataFrame.from_records([{'User': user, 'Material': material, "Action": action, "H_or_l": "high", "Time": current_time}])], ignore_index = True)
                        
                        print (f"After: {self.prev_trig_time}")
                        
                        print(trigger_message)

            # Check for low threshold within the low time window
            #if low_threshold != -1:
            #    low_window_start = current_time - time_window_low
                
            #    low_window_df = filtered_df[filtered_df['time'] >= low_window_start]
            #    low_action_counts = low_window_df['username'].value_counts()

            #    for user, count in low_action_counts.items():
            #        if count <= low_threshold:
            #            action_type = 'placed' if action == 1 else 'destroyed'
            #            trigger_message = f"Low Block Usage: {user} has {action_type} only {count} {material} blocks in the last {time_window_low} seconds."
            #            self.triggers_list.append((trigger_message, user, priority))
            #            
            #            print(trigger_message)
    
    '''

    '''
    def check_block_triggers(self):
        trigger_name = "check_block_triggers"
        current_time = datetime.now(pytz.timezone("America/Chicago")).timestamp()

        if self.co_block_with_users.empty:
            print(f"\033[90m{trigger_name}: No block data available.\033[0m")
            return

        for _, trig in self.block_triggers_df.iterrows():
            block_type = trig['type']  # numeric ID
            material = trig['material']  # human-readable name
            action = trig['action']  # 0 = break, 1 = place
            high_window = trig['Time_Window_High']
            high_threshold = trig['High_Threshold']
            priority = trig['Priority']

            window_start = current_time - high_window

            # Filter blocks by type, action, and time window
            relevant_blocks = self.co_block_with_users[
                (self.co_block_with_users['type'] == block_type) &
                (self.co_block_with_users['action'] == action) &
                (self.co_block_with_users['time'] >= window_start) &
                (~self.co_block_with_users['username'].str.startswith("#"))
            ]

            if relevant_blocks.empty:
                continue

            counts = relevant_blocks['username'].value_counts()

            
            for user, count in counts.items():
                if count >= high_threshold:
                    action_label = "placed" if action == 1 else "destroyed"
                    message = (
                        f"{user} has {action_label} {count} {material} blocks "
                        f"in the last {high_window} seconds. Category: Blocks"
                    )
                    self.triggers_list.append((message, user, priority))
                    print(message)
    '''

    def check_block_triggers(self):
        from datetime import datetime
        import pytz

        global block_trigger_cooldowns  # use the shared cooldown dictionary
        trigger_name = "check_block_triggers"
        current_time = datetime.now(pytz.timezone("America/Chicago")).timestamp()

        if self.co_block_with_users.empty:
            print(f"\033[90m{trigger_name}: No block data available.\033[0m")
            return

        triggered_users_this_round = set()

        for _, trig in self.block_triggers_df.iterrows():
            block_type = trig['type']
            material = trig['material']
            action = trig['action']
            high_window = trig['Time_Window_High']
            high_threshold = trig['High_Threshold']
            priority = trig['Priority']

            window_start = current_time - high_window

            relevant_blocks = self.co_block_with_users[
                (self.co_block_with_users['type'] == block_type) &
                (self.co_block_with_users['action'] == action) &
                (self.co_block_with_users['time'] >= window_start) &
                (~self.co_block_with_users['username'].str.startswith("#"))
                ]

            if relevant_blocks.empty:
                continue

            counts = relevant_blocks['username'].value_counts()

            for user, count in counts.items():
                user = user.strip()

                # Prevent repeat triggers per run
                if user in triggered_users_this_round:
                    continue

                last_trigger = block_trigger_cooldowns.get(user, 0)
                time_since_last = current_time - last_trigger

                if time_since_last < block_trigger_cooldown_seconds:
                    print(f"\033[90m{trigger_name}: {user} still on cooldown ({int(time_since_last)}s ago).\033[0m")
                    continue

                if count >= high_threshold:
                    action_label = "placed" if action == 1 else "destroyed"
                    message = (
                        f"{user} has {action_label} {count} {material} blocks "
                        f"in the last {high_window} seconds. Category: Blocks"
                    )
                    self.triggers_list.append((message, user, priority))
                    block_trigger_cooldowns[user] = current_time
                    triggered_users_this_round.add(user)
                    print(message)






    '''
    def check_block_triggers(self):
        current_time = datetime.now(pytz.timezone("America/Chicago")).timestamp()

        for _, trigger_row in self.block_triggers_df.iterrows():
            block_type = trigger_row['type']
            material = trigger_row['material']
            action = trigger_row['action']
            time_window_high = trigger_row['Time_Window_High']
            high_threshold = trigger_row['High_Threshold']
            time_window_low = trigger_row['Time_Window_Low']
            low_threshold = trigger_row['Low_Threshold']
            priority = trigger_row['Priority']

            trigger_name = f"block_{material}_{'placed' if action == 1 else 'destroyed'}"
            enabled, _ = get_trigger_settings(trigger_name)
            if not enabled:
                print(f"\033[90mSkipping {trigger_name} (priority {priority}) — disabled in Trigger Manager.\033[0m")
                continue

            filtered_df = self.co_block_with_users[
                (self.co_block_with_users['type'] == block_type) &
                (self.co_block_with_users['action'] == action) &
                (~self.co_block_with_users['username'].str.startswith('#'))
            ]

            if high_threshold != -1:
                high_window_start = current_time - time_window_high
                trigger_times = self.prev_trig_time[
                    (self.prev_trig_time['Material'] == material) &
                    (self.prev_trig_time['Action'] == action) &
                    (self.prev_trig_time['H_or_l'] == "high")
                ]

                rows_to_drop = []
                for _, row in filtered_df.iterrows():
                    window_start_time = high_window_start
                    user_trigger_times = trigger_times[trigger_times['User'] == row['username']]
                    if not user_trigger_times.empty:
                        idx = user_trigger_times.index[0]
                        window_start_time = max(window_start_time, user_trigger_times.loc[idx]['Time'])
                    if row['time'] < window_start_time:
                        rows_to_drop.append(row['rowid'])

                high_window_df = filtered_df[~filtered_df['rowid'].isin(rows_to_drop)]
                high_action_counts = high_window_df['username'].value_counts()

                for user, count in high_action_counts.items():
                    if count >= high_threshold:
                        action_type = 'placed' if action == 1 else 'destroyed'
                        trigger_message = f"High Block Usage: {user} has {action_type} {count} {material} blocks in the last {time_window_high} seconds."
                        self.triggers_list.append((trigger_message, user, priority))
                        print(trigger_message)

                        # Update prev_trig_time
                        self.prev_trig_time = self.prev_trig_time[
                            ~((self.prev_trig_time['User'] == user) &
                              (self.prev_trig_time['Material'] == material) &
                              (self.prev_trig_time['Action'] == action) &
                              (self.prev_trig_time['H_or_l'] == "high"))
                        ]
                        self.prev_trig_time = pd.concat([
                            self.prev_trig_time,
                            pd.DataFrame.from_records([{
                                'User': user,
                                'Material': material,
                                'Action': action,
                                'H_or_l': "high",
                                'Time': current_time
                            }])
                        ], ignore_index=True)
    '''

# =============================================================================
# Trigger Manager
# =============================================================================

def launch_trigger_manager():
    '''
    # for running locally on IntelliJ - Geph, not required or desired
    if threading.current_thread() is not threading.main_thread():
        print("[WARN] Tkinter must run on the main thread. Skipping GUI.")
        return
    else:
    '''
    import tkinter as tk
    from tkinter import ttk
    import json

    try:
        with open("trigger_config.json", "r") as f:
            trigger_config = json.load(f)
    except FileNotFoundError:
        trigger_config = {}

    root = tk.Tk()
    root.title("Trigger Manager")

    checkbox_vars = {}
    priority_entries = {}

    def save_settings():
        updated_config = {}
        for trigger, var in checkbox_vars.items():
            priority_val = priority_entries[trigger].get()
            try:
                priority = int(priority_val)
            except ValueError:
                priority = 1

            existing = trigger_config.get(trigger, {})
            category = existing.get("category", "Uncategorized")

            updated_config[trigger] = {
                "enabled": var.get(),
                "priority": priority,
                "category": category
            }

        with open("trigger_config.json", "w") as f:
            json.dump(updated_config, f, indent=4)

        status_label.config(text="Settings saved!", foreground="green")
        root.after(3000, lambda: status_label.config(text=""))

    # === Scrollable Frame Setup ===
    canvas = tk.Canvas(root, height=400)
    scrollbar = ttk.Scrollbar(root, orient="vertical", command=canvas.yview)
    scrollable_frame = ttk.Frame(canvas)

    scrollable_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(
            scrollregion=canvas.bbox("all")
        )
    )

    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    # === Trigger Widgets ===
    for trigger, settings in trigger_config.items():
        frame = ttk.Frame(scrollable_frame)
        frame.pack(fill='x', padx=10, pady=3)

        var = tk.BooleanVar(value=settings.get("enabled", False))
        checkbox = ttk.Checkbutton(frame, text=trigger, variable=var)
        checkbox.pack(side='left')
        checkbox_vars[trigger] = var

        ttk.Label(frame, text="Priority:").pack(side='left', padx=(10, 0))
        entry = ttk.Entry(frame, width=5)
        entry.insert(0, str(settings.get("priority", 1)))
        entry.pack(side='left')
        priority_entries[trigger] = entry

    # === Save Button & Status ===
    ttk.Button(root, text="Save", command=save_settings).pack(pady=10)
    status_label = ttk.Label(root, text="")
    status_label.pack()

    root.mainloop()


# =============================================================================
# Driver Program (main)
# =============================================================================

if __name__ == "__main__":
    import signal

    def handle_sigint(sig, frame):
        global SOCKET
        s = SOCKET                  # <- narrow the type for the checker
        if s is not None:
            s.close()
        print("Stopping!")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    def _dt(inp):
        try:
            return datetime.strptime(inp, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ArgumentTypeError(
                f"Input did match format YYYY-MM-DD hh:mm:ss - {inp!r}"
            )

    parser = ArgumentParser()

    parser.add_argument(
        "--initial-newer-than",
        help="Surround value in quotes. Expects format 'YYYY-MM-DD hh:mm:ss'",
        type=_dt,
        default=datetime.now(),
    )

    parser.add_argument(
        "--saveload",
        help="Specify the file to save/load (name of the camp)",
        type=str,
        default=None,
    )

    # add wid to argument parser
    parser.add_argument(
        "--wid",
        help="Specify the world ID (wid)",
        type=int,
        required=True
    )

    args = parser.parse_args()

    # print("Connecting to socket... ")
    # SOCKET = connect(
    #     "wss://free.blr2.piesocket.com/v3/qrfchannel?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1"
    # )
    # print("Connected!")

    central_tz = pytz.timezone("America/Chicago")
    fetcher = Fetcher(args.initial_newer_than, args.saveload, args.wid)
    print(f"\033[93mCONFIG → World ID set to: {args.wid}\033[0m")

    # Start Trigger Manager GUI in a separate thread
    gui_thread = threading.Thread(target=launch_trigger_manager)
    gui_thread.daemon = True
    gui_thread.start()

    # Start a new thread for updating positions every 3 seconds
    position_thread = threading.Thread(target=fetcher.update_positions_every_3_seconds)
    position_thread.daemon = True  # This makes sure the thread will exit when the main program exits
    position_thread.start()

    '''
    while True:
        fetcher.on_wakeup()
        
        print(f"\033[96mon_wakeup() finished- {datetime.now(central_tz)}\033[0m\n")
        
        current_time = datetime.now().timestamp()
        if (
            current_time - fetcher.last_trigger_time > 300
            # current_time - fetcher.last_trigger_time > 9999999 #turn off random trigger during testing
        ):  # Check if 34 seconds have passed
            if not fetcher.triggers_list:  # Check if no trigger has been sent recently
                online_students = fetcher.players["online_user"].tolist()
                if online_students:
                    random_student = random.choice(online_students)
                    trigger_message = "Random check-in"
                    print(
                        f"\033[92m \nSending random trigger to '{random_student}' on next wakeup. \033[0m"
                    )
                    fetcher.triggers_list.append((trigger_message, random_student, 10))
                    fetcher.last_trigger_time = (
                        current_time  # Update the last trigger time
                    )
        fetcher.save_tools_usage()
        
        
        now = datetime.now(central_tz)
        
        print(f"\033[96mFinished work at ---- {now}. \n^- \033[0mSleeping for 10 seconds.")
        sleep(10)  # run checks every 10 seconds
    '''

    while True:
        fetcher.on_wakeup()

        print(f"\033[96mon_wakeup() finished- {datetime.now(central_tz)}\033[0m\n")

        current_time = datetime.now().timestamp()

        # === Trigger Settings ===
        random_trigger_name = "check_random_checkin"
        random_enabled, random_priority, category = get_trigger_settings(random_trigger_name)

        if (
                current_time - fetcher.last_trigger_time > 300  # 5 minutes
        ):
            if not fetcher.triggers_list:  # Only send if no other triggers are pending
                if random_enabled:
                    online_students = fetcher.players["online_user"].tolist()
                    if online_students:
                        random_student = random.choice(online_students)
                        trigger_message = f"Random check-in. Category: {category}"
                        print(
                            f"\033[92m \nSending random trigger to '{random_student}' on next wakeup. \033[0m"
                        )
                        fetcher.triggers_list.append((trigger_message, random_student, random_priority))
                        fetcher.last_trigger_time = current_time
                else:
                    print(f"\033[90mSkipping {random_trigger_name} (priority {random_priority}) — disabled in Trigger Manager.\033[0m")

        fetcher.save_tools_usage()

        now = datetime.now(central_tz)
        print(f"\033[96mFinished work at ---- {now}. \n^- \033[0mSleeping for 10 seconds.")
        sleep(10)
