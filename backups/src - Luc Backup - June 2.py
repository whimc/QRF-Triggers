# =============================================================================
# Imports
# =============================================================================

import json
import signal
import sys
from argparse import ArgumentParser, ArgumentTypeError
from datetime import datetime
from pathlib import Path
from time import sleep, time

import pandas as pd
import sqlalchemy as db
from websockets.sync.client import connect

from datetime import datetime
import pytz

import os
import random

import difflib
from difflib import SequenceMatcher

import re
from shapely.geometry import Polygon, Point
import math

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import threading
from collections import defaultdict

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
SHOW TABLES;
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

GET_CO_BLOCK_WITH_USERS = """
SELECT b.*, u.user as username
FROM co_block b
LEFT JOIN co_user u ON b.user = u.rowid
WHERE wid = 111 and b.time > (unix_timestamp(current_timestamp) - 120)
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

GET_BLOCKS = """
select user
      , world_id, x, y, z
      , type
      , action
from co_block where wid = 111 and
-- this should be a variable defined at startup, not hardcoded; wid 111 = sdp7
where from_unixtime(time) >= '{newer_than}'
-- timestamp is 10 digit unix precision
"""

GET_MATERIALS = """
select id
      , material
from co_material_map
"""

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
    payload["data"]["masterlogs"] = {
        **payload["data"],
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
        print(response)

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
        "co_block_with_users": GET_CO_BLOCK_WITH_USERS,
    }

    def load_data(self):
        for key, query in Fetcher.CMDS.items():
            df = get_data(query, self.newer_than)
            setattr(self, key, df)
            
    def __init__(self, initial_newer_than, saveload_file=None):
        self.newer_than = initial_newer_than
        self.saveload_file = saveload_file
        # self.players = get_data(GET_ONLINE_PLAYERS)
        

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
        else:
            self.tools_usage = {}

        self.observations_record = {}
        self.pair_durations = defaultdict(int)

    def save_tools_usage(self):
        if self.saveload_file:
            with open(self.saveload_file, "w") as f:
                json.dump(self.tools_usage, f)
            print(
                f"\033[92mProgress saved to '{self.saveload_file}'. \nIt is now safe to stop the python script.\n \033[0m"
            )

    def fetch_data(self):
        for key, query in Fetcher.CMDS.items():
            df = get_data(query, self.newer_than)
            # Set 'self.<key>' to the new dataframe
            
            '''
            if key == "peek":
                print ("PEEK")
                print (df)
            '''
                
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
        self.check_over_200_actions_in_2_minutes()
        
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

        # handle observations
        for _, row in self.observations.iterrows():
            user = row["username"]
            world = row["world"]
            x = row["x"]
            z = row["z"]
            observation_text = row[
                "observation"
            ]  # 'observation' column holds the text of the observation

            for object_type, objects in world_coordinates_dictionary.get(
                world, {}
            ).items():
                if object_type == "Global":
                    continue  # Skip global actions for now; handle them later if needed

                for object_name, details in objects.items():
                    similarity = SequenceMatcher(
                        None, observation_text, object_name
                    ).ratio()

                    # Check if the observation is near a specific place or object
                    if "range" in details and self.is_point_inside_space(
                        x, z, details["range"]
                    ):
                        print(
                            f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}"
                        )
                        self.triggers_list.append(
                            (
                                f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}",
                                user,
                                6,
                            )
                        )
                    elif "x" in details and "z" in details:
                        place_x, place_z = details["x"], details["z"]
                        # Check if x, z, place_x, and place_z are not None before calculation
                        if (
                            place_x is not None
                            and place_z is not None
                            and x is not None
                            and z is not None
                        ):
                            if abs(x - place_x) + abs(z - place_z) <= 10:
                                print(
                                    f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}"
                                )
                                self.triggers_list.append(
                                    (
                                        f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}",
                                        user,
                                        6,
                                    )
                                )

        # handle tool usage
        for _, cmd_row in self.commands.iterrows():
            user = cmd_row["username"]
            world = cmd_row["world"]
            x = cmd_row["x"]
            z = cmd_row["z"]
            message = cmd_row["message"].strip()

            # print ("WORLD COORDS DICT", world_coordinates_dictionary.get(world, {}).get('Global', []))

            # Determine the tool used in the command, if any
            used_tool = None
            for tool in slash_commands_in_expected_actions:
                if message.startswith(f"/{tool}"):
                    used_tool = tool
                    break

            # print ("\033[93mUSED TOOL\033[0m", used_tool)
            if used_tool:
                tool_triggered = False

                # Check against important places in the world
                for object_type, objects in world_coordinates_dictionary.get(
                    world, {}
                ).items():
                    if object_type == "Global":
                        # print("\033[93mOBJECT TYPE GLOBAL\033[0m")
                        continue  # We will handle Global tools later

                    for object_name, details in objects.items():
                        expected_actions = details.get("Expected Action", [])
                        # print(f"\033[93mEXPECTED ACTIONS {expected_actions}\033[0m")

                        if (
                            "range" in details
                            and self.is_point_inside_space(x, z, details["range"])
                            and f"/{used_tool}" in expected_actions
                        ):
                            print(
                                f"{user} used tool {used_tool} near {object_name} in {world}."
                            )
                            self.triggers_list.append(
                                (
                                    f"{user} used tool {used_tool} near {object_name} in {world}",
                                    user,
                                    6,
                                )
                            )
                            tool_triggered = True

                        elif (
                            "x" in details
                            and "z" in details
                            and f"/{used_tool}" in expected_actions
                        ):
                            place_x, place_z = details["x"], details["z"]
                            if abs(x - place_x) + abs(z - place_z) <= 10:
                                print(
                                    f"{user} used tool {used_tool} near {object_name} in {world}."
                                )
                                self.triggers_list.append(
                                    (
                                        f"{user} used tool {used_tool} near {object_name} in {world}",
                                        user,
                                        6,
                                    )
                                )
                                tool_triggered = True

                if not tool_triggered:
                    global_actions = world_coordinates_dictionary.get(world, {}).get(
                        "Global", {}
                    )
                    # print("\033[93mGLOBAL ACTIONS\033[0m", global_actions)

                    for _, details in global_actions.items():
                        expected_actions = details.get("Expected Action", [])
                        # print("\033[93mEXPECTED ACTIONS\033[0m", expected_actions)

                        if f"/{used_tool}" in expected_actions:
                            print(
                                f"{user} used tool {used_tool} in world {world} (Global action)."
                            )
                            self.triggers_list.append(
                                (
                                    f"{user} used tool {used_tool} in world {world} (Global action)",
                                    user,
                                    8,
                                )
                            )
                            break  # Exit after finding and processing the first set of global actions

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

    def update_observation_usage(self):
        central_tz = pytz.timezone("America/Chicago")
        
        for _, row in self.observations.iterrows():
            user = row["username"]
            world = row["world"]
            position_time = row["time"]

            
            # Convert position_time to a string if it's a Timestamp
            if isinstance(position_time, pd.Timestamp):
                position_time = position_time.strftime("%Y-%m-%d %H:%M:%S")

            # Convert position_time to a timestamp in central timezone
            position_time = datetime.strptime(position_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=central_tz).timestamp()

            x = row["x"]
            z = row["z"]
            observation_text = row["observation"]

            # Add the observation to the record, organized by world
            if world not in self.observations_record:
                self.observations_record[world] = []
            self.observations_record[world].append((x, z, user, observation_text))

            # Check for nearby observations
            for obs_x, obs_z, obs_user, obs_text in self.observations_record[world]:
                distance = abs(x - obs_x) + abs(z - obs_z)  # Manhattan distance
                if 0 < distance < 10:
                    similarity = difflib.SequenceMatcher(None, observation_text, obs_text).ratio()
                    print(f"obs distance is: {distance}, similarity is: {similarity}")

                    trigger_message = f"{user} made an observation near another observation in {world}. Difflib similarity is {similarity}"
                    print(trigger_message)
                    self.triggers_list.append((trigger_message, user, 3))
                    break  # Exit after finding one nearby observation to avoid multiple triggers for the same event

            if user not in self.tools_usage:
                self.tools_usage[user] = {
                    "worlds_visited": [world],
                    "current_world": world,
                    "total_observation_count": 0,  # For overall observation count
                    "world_observation_counts": {world: 0},  # For per-world observation count
                    "last_observation_time": position_time,  # Initialize last observation time
                    "recent_observations": [position_time],  # Initialize recent observations list
                }
            else:
                self.tools_usage[user]["total_observation_count"] += 1
                self.tools_usage[user]["current_world"] = world
                self.tools_usage[user]["last_observation_time"] = position_time  # Update last observation time

                if world not in self.tools_usage[user]["worlds_visited"]:
                    self.tools_usage[user]["worlds_visited"].append(world)

                if world not in self.tools_usage[user]["world_observation_counts"]:
                    self.tools_usage[user]["world_observation_counts"][world] = 0

                self.tools_usage[user]["world_observation_counts"][world] += 1

                # Add the current observation time to the list of recent observations
                self.tools_usage[user].setdefault("recent_observations", []).append(position_time)

                # Keep only the observations from the last 2 minutes
                current_time = datetime.now(central_tz).timestamp()
                self.tools_usage[user]["recent_observations"] = [
                    t for t in self.tools_usage[user]["recent_observations"] if current_time - t <= 2 * 60
                ]
            
            # Check if the world is "mars" or "sdp7"
            if world.lower() in ["mars", "sdp7"]:
                trigger_message = f"{user} made an observation in {world}."
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)

        for user, data in self.tools_usage.items():
            worlds_visited = data["worlds_visited"]
            current_world = data["current_world"]
            world_observation_count = data.get("world_observation_counts", {}).get(current_world, 0)

            # Check for lack of observations
            if len(worlds_visited) >= 3:
                trigger_key = f"no_observations_since_third_{current_world}"
                if not data.get(trigger_key, False):
                    if len(worlds_visited) == 3 and world_observation_count == 0:
                        trigger_message = f"{user} has not made any observations by the third world."
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, 2))
                        data[trigger_key] = True
                    elif len(worlds_visited) > 3 and world_observation_count == 0:
                        trigger_message = f"{user} has visited {len(worlds_visited)} worlds without making any observations."
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, 2))
                        data[trigger_key] = True

            # High observation counts check
            high_obs_trigger_key = f"high_observations_{current_world}"
            if len(worlds_visited) <= 3 and world_observation_count > 10:  # 10
                if not data.get(high_obs_trigger_key, False):
                    print(f"{user} has made more than 10 observations in {current_world}.")
                    self.triggers_list.append((f"{user} has high observation count in {current_world}", user, 7))
                    data[high_obs_trigger_key] = True
            elif len(worlds_visited) > 3 and world_observation_count > 5:  # 5
                if not data.get(high_obs_trigger_key, False):
                    print(f"{user} has made more than 5 observations in {current_world} after visiting 3 worlds.")
                    self.triggers_list.append((f"{user} has high observation count in {current_world}", user, 7))
                    data[high_obs_trigger_key] = True
            
            
            if len(worlds_visited) > 0 and world_observation_count >= 5:  # 5
                if not data.get(high_obs_trigger_key, False):
                    print(f"{user} has made 5 observations in {current_world}.")
                    self.triggers_list.append((f"{user} has made 5 observations in {current_world}", user, 7))
                    data[high_obs_trigger_key] = True

    def check_no_observations_last_20_minutes(self):
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()
        
        time_dilation = 3006

        # ====================================================
        # //TRIGGER PARAM: check_last_tool_use_over_20_minutes
        # 20 * 60 below (line 1023) means 20 minutes. Change 20 to any number of minutes you want the threshold to be.
        # ====================================================
        
        for user, data in self.tools_usage.items():
            last_observation_time = data.get("last_observation_time", 0)
            if current_time - last_observation_time + time_dilation > 20 * 60:  # 20 minutes
                
                '''
                print("current_time", current_time)
                print("last_observation_time", last_observation_time)
                print("current_time - last_observation_time + time_dilation", current_time - last_observation_time + time_dilation)
                '''
                
                trigger_message = f"{user} has not made any observations in the last 20 minutes."
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)
                # Reset last_observation_time to 1 minute before the current time
                # self.tools_usage[user]["last_observation_time"] = current_time + time_dilation - 10  # 1 minute breathing time before next trigger
                self.tools_usage[user]["last_observation_time"] = current_time + time_dilation  # 1 minute breathing time before next trigger
                
                # print("last observation time changed to", self.tools_usage[user]["last_observation_time"], "with breathing time of 1 minute")
                
            else:
                print ()
                '''
                print("no trigger", user)
                print("current_time", current_time)
                print("last_observation_time", last_observation_time)
                print("current_time - last_observation_time + time_dilation", current_time - last_observation_time + time_dilation)
                '''
                
    def check_last_tool_use_over_20_minutes(self):
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()
        
        time_dilation = 3006

        # ====================================================
        # //TRIGGER PARAM: check_last_tool_use_over_20_minutes
        # 20 * 60 below (line 1023) means 20 minutes. Change 20 to any number of minutes you want the threshold to be.
        # ====================================================

        for user, data in self.tools_usage.items():
            last_tool_use_time = data.get("last_tool_use_time", 0)
            if current_time - last_tool_use_time + time_dilation > 20 * 60:  # 20 minutes
                '''
                print("current_time", current_time)
                print("last_tool_use_time", last_tool_use_time)
                print("current_time - last_tool_use_time + time_dilation", current_time - last_tool_use_time + time_dilation)
                '''
                
                trigger_message = f"{user} has not used any tools in the last 20 minutes."
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)
                # Reset last_tool_use_time to 1 minute before the current time
                # self.tools_usage[user]["last_tool_use_time"] = current_time + time_dilation - 60  # 1 minute breathing time before next trigger
                self.tools_usage[user]["last_tool_use_time"] = current_time + time_dilation  # 1 minute breathing time before next trigger
                
                # print(f"last tool use time changed to {self.tools_usage[user]['last_tool_use_time']} with breathing time of 1 minute")
                
            else:
                print()
                '''
                print("no trigger", user)
                print("current_time", current_time)
                print("last_tool_use_time", last_tool_use_time)
                print("current_time - last_tool_use_time + time_dilation", current_time - last_tool_use_time + time_dilation)
                '''
             
    def check_3_chat_entries_in_1_minute(self):
        # Merge co_chat and co_user to get usernames
        merged_df = pd.merge(self.co_chat, self.co_user, left_on='user', right_on='rowid')

        '''
        print(merged_df.head())
        print(merged_df.columns)
        '''

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
                self.triggers_list.append((trigger_message, username, 1))
                print(trigger_message)

    def check_3_observations_in_2_minutes(self):
        central_tz = pytz.timezone("America/Chicago")
        current_time = datetime.now(central_tz).timestamp()
        for user, data in self.tools_usage.items():
            recent_observations = data.get("recent_observations", [])
            if len(recent_observations) >= 3:
                trigger_message = f"{user} has made 3 observations in the last 2 minutes."
                self.triggers_list.append((trigger_message, user, 1))
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
            "help",
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
                        self.triggers_list.append((trigger_message, user, 1))
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
                            self.tools_usage[user]["tool_use_count"] += 1
                            tool_key = f"{tool}_{current_world}"
                            self.tools_usage[user].setdefault(tool_key, 0)
                            self.tools_usage[user][tool_key] += 1
                            data[world_tool_key] += (
                                1  # for high tool use in particular world count
                            )

            # Check for third world visit without tool usage
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

            # =============================================================================
            # /Check for no tools used by and since 3rd world
            # =============================================================================

            # =============================================================================
            # Check for high tools use (>10 first 3 worlds, >5 succeeding worlds)
            # =============================================================================

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

            # =============================================================================
            # /Check for high tools use (>10 first 3 worlds, >5 succeeding worlds)
            # =============================================================================

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
                            (f"{user} has used '/{tool}' in {current_world}", user, 5)
                        )
                        self.tools_usage[user][tool_flag_key] = 1

    def check_3_tools_in_1_minute(self):
        for user, data in self.tools_usage.items():
            tool_usage_timestamps = data.get("tool_usage_timestamps", [])
            if len(tool_usage_timestamps) >= 3:
                trigger_message = f"{user} has used at least 3 tools in less than a minute."
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)
                # Clear the tool usage timestamps to avoid repeated triggers
                self.tools_usage[user]["tool_usage_timestamps"] = []
        
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
                self.triggers_list.append((trigger_message, user, 1))
                print(trigger_message)   
                self.tools_usage[user]['recent_positions'] = []

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
        duration_increment = 3  # Increment duration by 3 seconds if within proximity
        duration_threshold = 9  # Define the duration threshold (in seconds)
        
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
                trigger_message = f"{user_i} and {user_j} have been close to each other for more than 30 seconds."
                self.triggers_list.append((trigger_message, user_i, 1))
                # self.triggers_list.append((trigger_message, user_j, 1)) # comment this out if we want to duplicate the trigger
                print(trigger_message)
                # Reset duration to avoid repeated triggers
                self.pair_durations[(user_i, user_j)] = 0
            else: 
                print ()
                '''
                print ("pair_durations")
                print (self.pair_durations)
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
        duration_increment = 3  # Increment duration by 3 seconds if beyond proximity
        duration_threshold = 9  # Define the duration threshold (in seconds)

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
                trigger_message = f"{user} has been far from the crowd for more than 30 seconds."
                self.triggers_list.append((trigger_message, user, 1))
                # print(trigger_message)
                # Reset duration to avoid repeated triggers
                self.tools_usage[user]['far_from_crowd_duration'] = 0
            else:
                print ()
                '''
                print (user)
                print (self.tools_usage[user]['far_from_crowd_duration'])
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
                        self.triggers_list.append((trigger_message, user, 1))
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
                                self.triggers_list.append((trigger_message, user, 1))
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
                self.triggers_list.append((trigger_message, username, 1))
                print(trigger_message)

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
            self.triggers_list.append((trigger_message, username, 1))
            print(trigger_message)

    def check_five_or_more_observations_in_world(self):
        for user, data in self.tools_usage.items():
            for world, count in data.get("world_observation_counts", {}).items():
                if count >= 5:
                    trigger_key = f"five_observations_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has made 5 or more observations in {world}."
                        self.triggers_list.append((trigger_message, user, 7))
                        print(trigger_message)
                        data[trigger_key] = True

    def check_five_or_more_tools_in_world(self):
        for user, data in self.tools_usage.items():
            for world, count in data.get("world_tool_counts", {}).items():
                if count >= 5:
                    trigger_key = f"five_tools_{world}"
                    if not data.get(trigger_key, False):
                        trigger_message = f"{user} has used 5 or more tools in {world}."
                        self.triggers_list.append((trigger_message, user, 7))
                        print(trigger_message)
                        data[trigger_key] = True

    def check_five_chat_messages_in_world(self):
        # Merge co_chat and co_user to get usernames
        merged_df = pd.merge(self.co_chat_with_worlds, self.co_user, left_on='user_id', right_on='rowid')

        '''
        # Print columns to check the correct column names
        print("Chat DataFrame columns:", merged_df.columns)

        # Print the raw chat data for debugging
        print("Raw Chat Data:")
        print(merged_df)
        '''

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
                        self.triggers_list.append((trigger_message, user, 7))
                        print(trigger_message)
                        data[trigger_key] = True

    def check_tool_use_counts(self):
        for user, data in self.tools_usage.items():
            for tool, worlds in data.get("tool_counts", {}).items():
                for world, count in worlds.items():
                    if tool == "gravity" and count > 2:
                        trigger_key = f"gravity_tool_{world}"
                        if not data.get(trigger_key, False):
                            trigger_message = f"{user} has used gravity more than twice in {world}."
                            self.triggers_list.append((trigger_message, user, 7))
                            print(trigger_message)
                            data[trigger_key] = True
                    elif tool != "gravity" and count > 3:
                        trigger_key = f"{tool}_tool_{world}"
                        if not data.get(trigger_key, False):
                            trigger_message = f"{user} has used {tool} more than three times in {world}."
                            self.triggers_list.append((trigger_message, user, 7))
                            print(trigger_message)
                            data[trigger_key] = True

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





# =============================================================================
# Driver Program (main)
# =============================================================================

if __name__ == "__main__":
    import signal

    def handle_sigint(sig, frame):
        # Prevent Ctrl+C from hanging
        if SOCKET:
            SOCKET.close()
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

    args = parser.parse_args()

    # print("Connecting to socket... ")
    # SOCKET = connect(
    #     "wss://free.blr2.piesocket.com/v3/qrfchannel?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1"
    # )
    # print("Connected!")

    central_tz = pytz.timezone("America/Chicago")
    fetcher = Fetcher(args.initial_newer_than, args.saveload)

    # Start a new thread for updating positions every 3 seconds
    position_thread = threading.Thread(target=fetcher.update_positions_every_3_seconds)
    position_thread.daemon = True  # This makes sure the thread will exit when the main program exits
    position_thread.start()

    while True:
        fetcher.on_wakeup()
        
        print(f"\033[96mon_wakeup() finished- {datetime.now(central_tz)}\033[0m\n")
        
        current_time = datetime.now().timestamp()
        if (
            current_time - fetcher.last_trigger_time > 34
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
