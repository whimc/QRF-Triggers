# =============================================================================
# Imports
# =============================================================================

import json
import signal
import sys
from argparse import ArgumentParser, ArgumentTypeError
from datetime import datetime
from pathlib import Path
from time import sleep

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


# =============================================================================
# Database Connection
# =============================================================================

with open(Path(__file__).parent / "credentials.json") as f:
    creds = json.load(f)
ENG = db.create_engine(db.URL.create("mysql+mysqlconnector", **creds["database"]))

# ================================================================================================================
# Import Rachel Zhou's Important Places Coordinate system (edited to handshake with Jack's code and Triggers code.
# ================================================================================================================

data = pd.read_csv('../WHIMC Coordinate Tracking updated.csv')

# Fill forward 'World' and 'Object' columns to handle blank cells
data['World'].fillna(method='ffill', inplace=True)
data['Object'].fillna(method='ffill', inplace=True)

# Initialize the dictionary to hold world data and global expected actions
world_coordinates_dictionary = {}

# Iterate through the dataframe
for _, row in data.iterrows():
    world = row['World']
    object_name = row['Object_name']
    object_type = row['Object']
    expected_action = row['Expected_Action'] if not pd.isna(row['Expected_Action']) else ""

    # Ensure that the world and object type keys exist in the dictionary
    if world not in world_coordinates_dictionary:
        world_coordinates_dictionary[world] = {}
    if object_type not in world_coordinates_dictionary[world]:
        world_coordinates_dictionary[world][object_type] = {}

    # Handle global expected actions
    if object_name == 'NA' and object_type == 'Global':
        world_coordinates_dictionary[world]['Global'] = expected_action.split(', ')

    # Handle other objects (Places, NPCs, Signals)
    else:
        x = row['x'] if not pd.isna(row['x']) else None
        z = row['z'] if not pd.isna(row['z']) else None
        object_range = row['range'] if not pd.isna(row['range']) else None

        # Add details to the respective object in the dictionary
        world_coordinates_dictionary[world][object_type][object_name] = {
            'x': x,
            'z': z,
            'range': object_range,
            'Expected Action': expected_action.split(', ') if expected_action else []
        }

print(world_coordinates_dictionary.get('TwoMoonsLow', {}).get('Global', []))


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

GET_ALL_CO_SESSION = """
DESCRIBE whimc_player_positions;
"""

# Extended GET_ONLINE_PLAYERS TO have current world data

GET_ONLINE_PLAYERS = """
SELECT 
    usr.user AS online_user, 
    last_times.last_time AS login_time, 
    player_pos.world,
    player_pos.x,  -- Added x coordinate
    player_pos.z,  -- Added z coordinate
    player_pos.time AS position_time  -- Added time (renamed to avoid confusion with login_time)
FROM (
    SELECT 
        user, 
        max(time) AS last_time
    FROM 
        co_session
    WHERE 
        time >= unix_timestamp(current_date() - 1) -- only look back 1 day
    GROUP BY 
        user
) AS last_times
LEFT JOIN co_session AS sess ON last_times.user = sess.user AND last_times.last_time = sess.time
LEFT JOIN co_user AS usr ON last_times.user = usr.rowid
LEFT JOIN (
    SELECT 
        p1.username, 
        p1.world, 
        p1.x,
        p1.z,
        p1.time
    FROM 
        whimc_player_positions p1
    INNER JOIN (
        SELECT 
            username, 
            max(time) AS latest_time
        FROM 
            whimc_player_positions
        GROUP BY 
            username
    ) p2 ON p1.username = p2.username AND p1.time = p2.latest_time
) AS player_pos ON usr.user = player_pos.username
WHERE 
    sess.action = 1;
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

# =============================================================================
# Utility Functions (get from WHIMC, send to Dispatcher)
# =============================================================================

def send_trigger(trigger_name: str, username: str, priority: int):
    global SOCKET 

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
        SOCKET = connect("wss://free.blr2.piesocket.com/v3/qrfchannel?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1")
        
        print("Connected to WebSocket")
            
        SOCKET.send(json_data)
        response = SOCKET.recv()
        print(response)
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
        "players": GET_ONLINE_PLAYERS
    }

    def __init__(self, initial_newer_than, saveload_file=None):
        self.newer_than = initial_newer_than
        self.saveload_file = saveload_file
        self.players = get_data(GET_ONLINE_PLAYERS)
        self.co_session = get_data(GET_ALL_CO_SESSION)
        self.commands = pd.DataFrame()
        self.observations = pd.DataFrame()
        self.science_tools = pd.DataFrame()
        
        # this is for random trigger that fires when inactivity is detected
        # does not matter which timezone, we just need a timer interval to detect inactivty since the time the python script was ran
        self.last_trigger_time = datetime.now().timestamp() 
        
        # dataframes / dictionaries for triggers
        self.triggers_list = []
        # self.tools_usage = {} <- recoded to look for a save/load file first if any
        
        if saveload_file and os.path.exists(saveload_file):
            with open(saveload_file, 'r') as f:
                self.tools_usage = json.load(f)
        else:
            self.tools_usage = {}
            
        self.observations_record = {}

    def save_tools_usage(self):
        if self.saveload_file:
            with open(self.saveload_file, 'w') as f:
                json.dump(self.tools_usage, f)

    def fetch_data(self):
        for key, query in Fetcher.CMDS.items():
            df = get_data(query, self.newer_than)
            # Set 'self.<key>' to the new dataframe
            setattr(self, key, df)

        self.save_tools_usage() # save after fetching the data

    def on_wakeup(self):

        # Use global variables
        # global meganumber
        
        # Need to convert the time to central time always because the actions 
        # of the players in the server are logged in central time. 
        central_tz = pytz.timezone('America/Chicago')
        now = datetime.now(central_tz)
    
        # now = datetime.now()
        print(f"Wakeup at {now}. Fetching data since {self.newer_than}")

        self.fetch_data()
        

        print (f"\nONLINE PLAYERS:\n{self.players}\n")
        print (f"CO_SESSION:\n{self.co_session}\n")
        print (f"COMMANDS:\n{self.commands}\n")
        print (f"OBSERVATIONS:\n{self.observations}\n")
        print (f"SCIENCE TOOLS:\n{self.science_tools}\n")
        print (f"OBSERVATIONS RECORD:\n{self.observations_record}\n")
        
        print(f"\033[92mProgress saved to '{self.saveload_file}'. It is now safe to turn off your python script :) \033[0m")
        
        # Initialize tools_usage for all online players so we can get the worlds visited, and curr world data even
        # if the student didn't make any observations / commands yet
        
        
        for _, row in self.players.iterrows():
            user = row['online_user']
            current_world = row['world'] 
            position_time = row['position_time']

            if user not in self.tools_usage:
                self.tools_usage[user] = {
                    'worlds_visited': [current_world],  # Initialize with the actual current world
                    'current_world': current_world,
                    'tool_use_count': 0,
                    # 'observation_count': 0
                    'total_observation_count': 0,  # For overall observation count
                    'world_observation_counts': {current_world: 0},  # For per-world observation count
                    'last_observation_time': position_time,
                    'mynoa_start_time': None,
                    'mynoa_trigger_fired': False
                }
            else:
                # Update the current world
                self.tools_usage[user]['current_world'] = current_world

                # Add to worlds_visited if not already there
                if current_world not in self.tools_usage[user]['worlds_visited']:
                    self.tools_usage[user]['worlds_visited'].append(current_world)
        
        
        # check for triggers and populate triggers_list
        self.update_tool_usage()
        self.update_observation_usage()
        self.check_mynoa_observations()
        self.check_activities_near_important_places()
        
        print (f"TOOLS & OBSERVATION USAGE (SAVED): \n{self.tools_usage}\n")
        
        
        # Send all triggers
        for trigger_name, username, priority in self.triggers():
            print(f"\033[93mTriggered '{trigger_name}' for '{username}' (priority {priority}) \033[0m")
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
    
        slash_commands_in_expected_actions = [
            'tides', 'wind', 'airflow', 'observe', 'year', 'temperature', 'humidity', 
            'radiation', 'pressure', 'radiation_cosmicrays', 'atmosphere', 'gravity', 
            'radius', 'magnetic_field', 'daylength', 'air_pressure'
        ]
    
        # handle observations
        for _, row in self.observations.iterrows():
            user = row['username']
            world = row['world']
            x = row['x']
            z = row['z']
            observation_text = row['observation']  # 'observation' column holds the text of the observation

            for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                if object_type == 'Global':
                    continue  # Skip global actions for now; handle them later if needed

                for object_name, details in objects.items():
                    similarity = SequenceMatcher(None, observation_text, object_name).ratio()

                    # Check if the observation is near a specific place or object
                    if 'range' in details and self.is_point_inside_space(x, z, details['range']):
                        print(f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}")
                        self.triggers_list.append((f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}", user, 1))
                    elif 'x' in details and 'z' in details:
                        place_x, place_z = details['x'], details['z']
                        # Check if x, z, place_x, and place_z are not None before calculation
                        if place_x is not None and place_z is not None and x is not None and z is not None:
                            if abs(x - place_x) + abs(z - place_z) <= 10:
                                print(f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}")
                                self.triggers_list.append((f"{user} made an observation near {object_name} in {world}. Similarity score: {similarity}", user, 1))


        # handle tool usage
        for _, cmd_row in self.commands.iterrows():
            user = cmd_row['username']
            world = cmd_row['world']
            x = cmd_row['x']
            z = cmd_row['z']
            message = cmd_row['message'].strip()
            
            # print ("WORLD COORDS DICT", world_coordinates_dictionary.get(world, {}).get('Global', []))

            # Determine the tool used in the command, if any
            used_tool = None
            for tool in slash_commands_in_expected_actions:
                if message.startswith(f'/{tool}'):
                    used_tool = tool
                    break
            
            # print ("\033[93mUSED TOOL\033[0m", used_tool)
            if used_tool:
                tool_triggered = False

                # Check against important places in the world
                for object_type, objects in world_coordinates_dictionary.get(world, {}).items():
                    if object_type == 'Global':
                        # print("\033[93mOBJECT TYPE GLOBAL\033[0m")
                        continue  # We will handle Global tools later

                    for object_name, details in objects.items():
                        expected_actions = details.get("Expected Action", [])
                        # print(f"\033[93mEXPECTED ACTIONS {expected_actions}\033[0m")
                        
                        if 'range' in details and self.is_point_inside_space(x, z, details['range']) and f"/{used_tool}" in expected_actions:
                            print(f"{user} used tool {used_tool} near {object_name} in {world}.")
                            self.triggers_list.append((f"{user} used tool {used_tool} near {object_name} in {world}", user, 2))
                            tool_triggered = True

                        elif 'x' in details and 'z' in details and f"/{used_tool}" in expected_actions:
                            place_x, place_z = details['x'], details['z']
                            if abs(x - place_x) + abs(z - place_z) <= 10:
                                print(f"{user} used tool {used_tool} near {object_name} in {world}.")
                                self.triggers_list.append((f"{user} used tool {used_tool} near {object_name} in {world}", user, 2))
                                tool_triggered = True
                    

                if not tool_triggered:
                    global_actions = world_coordinates_dictionary.get(world, {}).get('Global', {})
                    # print("\033[93mGLOBAL ACTIONS\033[0m", global_actions)

                    for _, details in global_actions.items():
                        expected_actions = details.get('Expected Action', [])
                        # print("\033[93mEXPECTED ACTIONS\033[0m", expected_actions)

                        if f"/{used_tool}" in expected_actions:
                            print(f"{user} used tool {used_tool} in world {world} (Global action).")
                            self.triggers_list.append((f"{user} used tool {used_tool} in world {world} (Global action)", user, 2))
                            break  # Exit after finding and processing the first set of global actions


    # Rachel Zhou's code edited to work with .self
    def define_polygon_boundary(self, range_str):
        if not range_str:
            return []  # Or return a default polygon if applicable

        coordinates = [tuple(map(int, coord)) for coord in re.findall(r"\((-?\d+),(-?\d+)\)", range_str)]

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
            user = player_row['online_user']
            current_world = player_row['world']
            position_time = player_row['position_time']

            if user not in self.tools_usage:
                self.tools_usage[user] = {
                    'last_observation_time': position_time,
                    'mynoa_start_time': None,
                    'mynoa_trigger_fired': False
                }

            #if current_world.startswith("mynoa"):
            if current_world.startswith("Mynoa"):
                if self.tools_usage[user]['mynoa_start_time'] is None:
                    self.tools_usage[user]['mynoa_start_time'] = position_time
                    self.tools_usage[user]['mynoa_trigger_fired'] = False
                else:
                    time_in_mynoa = position_time - self.tools_usage[user]['mynoa_start_time']
                    if time_in_mynoa >= 25 * 60 and not self.tools_usage[user]['mynoa_trigger_fired']:
                    # if time_in_mynoa >= 10 and not self.tools_usage[user]['mynoa_trigger_fired']:
                        observations_in_mynoa = self.tools_usage[user].get('world_observation_counts', {}).get(current_world, 0)
                        if observations_in_mynoa == 0:
                            print(f"{user} has been in {current_world} for more than 25 minutes without making an observation.")
                            self.triggers_list.append((f"{user} in {current_world} for 25+ minutes without observations", user, 1))
                            self.tools_usage[user]['mynoa_trigger_fired'] = True
            else:
                self.tools_usage[user]['mynoa_start_time'] = None
                self.tools_usage[user]['mynoa_trigger_fired'] = False



    def update_observation_usage(self):
        for _, row in self.observations.iterrows():
            user = row['username']
            world = row['world']
            
            # =============================================================================
            # Detect Observations placed near previous existing observations + send trigger
            # =============================================================================
            
            x = row['x']
            z = row['z']    
            observation_text = row['observation']            

            # Add the observation to the record, organized by world
            if world not in self.observations_record:
                self.observations_record[world] = []
            self.observations_record[world].append((x, z, user, observation_text))  

            # Check for nearby observations
            for obs_x, obs_z, obs_user, obs_text in self.observations_record[world]:
                # if user != obs_user:  # Avoid comparing the observation with itself <- important, let me know if comparison with same obs is allowed
                distance = abs(x - obs_x) + abs(z - obs_z)  # Manhattan distance
                if 0 < distance < 10:
                    similarity = difflib.SequenceMatcher(None, observation_text, obs_text).ratio()
                    print(f"obs distance is: {distance}, similarity is: {similarity}")
                    
                    trigger_message = f"{user} made an observation near another observation in {world}. Difflib similarity is {similarity}"
                    print(trigger_message)
                    self.triggers_list.append((trigger_message, user, 1))
                    break  # Exit after finding one nearby observation to avoid multiple triggers for the same event  

            # =============================================================================
            # /Detect Observations placed near previous existing observations + send trigger
            # =============================================================================                        

            if user not in self.tools_usage:
                self.tools_usage[user] = {
                    'worlds_visited': [world],
                    'current_world': world,
                    'total_observation_count': 0,  # For overall observation count
                    'world_observation_counts': {world: 0},  # For per-world observation count
                }
            else:
                self.tools_usage[user]['total_observation_count'] += 1
                self.tools_usage[user]['current_world'] = world

                if world not in self.tools_usage[user]['worlds_visited']:
                    self.tools_usage[user]['worlds_visited'].append(world)

                if world not in self.tools_usage[user]['world_observation_counts']:
                    self.tools_usage[user]['world_observation_counts'][world] = 0

            self.tools_usage[user]['world_observation_counts'][world] += 1

        for user, data in self.tools_usage.items():
            worlds_visited = data['worlds_visited']
            current_world = data['current_world']
            # world_observation_count = data['world_observation_counts'].get(current_world, 0)
            world_observation_count = data.get('world_observation_counts', {}).get(current_world, 0)
            
            
            # Check for lack of observations
            if len(worlds_visited) >= 3:
                trigger_key = f"no_observations_since_third_{current_world}"
                if not data.get(trigger_key, False):
                    if len(worlds_visited) == 3 and world_observation_count == 0:
                        trigger_message = f"{user} has not made any observations by the third world."
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, 1))
                        data[trigger_key] = True
                    elif len(worlds_visited) > 3 and world_observation_count == 0:
                        trigger_message = f"{user} has visited {len(worlds_visited)} worlds without making any observations."
                        print(trigger_message)
                        self.triggers_list.append((trigger_message, user, 1))
                        data[trigger_key] = True
            
            # High observation counts check
            high_obs_trigger_key = f"high_observations_{current_world}"
            if len(worlds_visited) <= 3 and world_observation_count > 3: #10
                if not data.get(high_obs_trigger_key, False):
                    print(f"{user} has made more than 10 observations in {current_world}.")
                    self.triggers_list.append((f"{user} has high observation count in {current_world}", user, 1))
                    data[high_obs_trigger_key] = True
            elif len(worlds_visited) > 3 and world_observation_count > 1: #5
                if not data.get(high_obs_trigger_key, False):
                    print(f"{user} has made more than 5 observations in {current_world}.")
                    self.triggers_list.append((f"{user} has high observation count in {current_world}", user, 1))
                    data[high_obs_trigger_key] = True


    def update_tool_usage(self):

        # Define lists of tools for different usage checks
        multi_use_tools = ['gravity', 'pressure', 'atmosphere']
        single_use_tools = ['rotational_period', 'scale', 'tectonic', 'tides', 'year', 'tilt', 'magnetic_field', 'tpa', 'agent', 'pause', 'tpall']
            
        for _, row in self.commands.iterrows():
            user = row['username']
            world = row['world']
            message = row['message'].strip()
            x = row['x']
            z = row['z']
            
            # Initialize the user in the dictionary if not present
            if user not in self.tools_usage:
                self.tools_usage[user] = {}
                self.tools_usage[user]['worlds_visited'] = []
            
            # Check for known tools in the message
            for tool in multi_use_tools + single_use_tools:
                # if f'/{tool}' in message:
                if message.startswith(f'/{tool}'):
                    tool_key = f"{tool}_{world}"  # Unique key for each tool and world
                    
                    # Initialize the tool in the user's dictionary if not present
                    if tool_key not in self.tools_usage[user]:
                        self.tools_usage[user][tool_key] = 0
                        self.tools_usage[user][f"{tool_key}_flag"] = 0  # Usage flag

                    # Increment the count for the tool
                    # self.tools_usage[user][tool_key] += 1 # test don't increment on initialize anymore because the update_tools_usage does so already.
                    
                    # Always update what world the user currently is in
                    self.tools_usage[user]['current_world'] = world
                    
                    if world not in self.tools_usage[user]['worlds_visited']:
                        self.tools_usage[user]['worlds_visited'].append(world)
                    


        # Process the recorded usage to trigger events or logging
        for user, data in self.tools_usage.items(): 
            current_world = data.get('current_world')
            worlds_visited = data.get('worlds_visited')
                       
            
            world_tool_key = f"tool_count_{current_world}"
            data.setdefault(world_tool_key, 0)  # Initialize if not already set
            
            # =============================================================================
            # Check for no tools used by and since 3rd world
            # =============================================================================
            
            for _, row in self.commands.iterrows():
                if row['username'] == user:
                    message = row['message']
                    for tool in multi_use_tools + single_use_tools:
                        if f'/{tool}' in message:
                            self.tools_usage[user]['tool_use_count'] += 1
                            tool_key = f"{tool}_{current_world}"
                            self.tools_usage[user].setdefault(tool_key, 0)
                            self.tools_usage[user][tool_key] += 1
                            data[world_tool_key] += 1 # for high tool use in particular world count

            # Check for third world visit without tool usage
            if len(worlds_visited) >= 3 and data.get('tool_use_count', 0) == 0:
                trigger_key = f"not_used_tools_since_third_{current_world}"
                if not data.get(trigger_key, False):
                    trigger_message = ""
                    if len(worlds_visited) == 3:
                        trigger_message = f"{user} has visited 3 worlds without using any tools."
                        print(f"{user} has visited 3 worlds without using any tools.")
                    elif len(worlds_visited) > 3:
                        trigger_message = f"{user} has visited {len(worlds_visited)} worlds without using any tools."
                        print(f"{user} has visited {len(worlds_visited)} without using any tools.")
                    
                    self.triggers_list.append((trigger_message, user, 1))
                    self.tools_usage[user][trigger_key] = True

            # =============================================================================
            # /Check for no tools used by and since 3rd world
            # =============================================================================
            
            # =============================================================================
            # Check for high tools use (>10 first 3 worlds, >5 succeeding worlds)
            # =============================================================================
            
            # Trigger conditions for high tool use
            high_use_trigger_key = f"high_use_{current_world}"
            if len(worlds_visited) <= 3 and data[world_tool_key] > 10 and not data.get(high_use_trigger_key, False):
                print(f"{user} has high tool use in the first three worlds: {current_world}")
                self.triggers_list.append((f"{user} has high tool use in {current_world}", user, 1))
                data[high_use_trigger_key] = True
            elif len(worlds_visited) > 3 and data[world_tool_key] > 5 and not data.get(high_use_trigger_key, False):
                print(f"{user} has high tool use in subsequent worlds: {current_world}")
                self.triggers_list.append((f"{user} has high tool use in {current_world}", user, 1))
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
                            print(f"{user} has used '/{tool}' more than once in {current_world}")   
                            self.triggers_list.append((f"{user} has used '/{tool}' more than once in {current_world}", user, 1))
                            self.tools_usage[user][tool_flag_key] = 1
                            
                    # Check for combined use of pressure, gravity, and atmosphere
                    if combined_use_flag:
                        print(f"{user} has combined use of pressure, gravity, & atmosphere in {current_world} more than once")
                        self.triggers_list.append((f"Combined use of pressure, gravity, & atmosphere in {current_world} more than once", user, 2))  
                        self.tools_usage[user][combined_key] = 1

                for tool in single_use_tools:
                    tool_key = f"{tool}_{current_world}"
                    tool_count = data.get(tool_key, 0)
                    tool_flag_key = f"{tool_key}_flag"
                    tool_flag = data.get(tool_flag_key, 0)
                    
                    if tool_count == 1 and tool_flag == 0:
                        print(f"{user} has used '/{tool}' in {current_world}")
                        self.triggers_list.append((f"{user} has used '/{tool}' in {current_world}", user, 1))
                        self.tools_usage[user][tool_flag_key] = 1
            
                
            


# =============================================================================
# Driver Program (main)
# =============================================================================

if __name__ == "__main__":
    import signal

    def handle_sigint(sig, frame):
        # Prevent Ctrl+C from hanging
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
        default=None
    )    
    
    args = parser.parse_args()

    print("Connecting to socket... ")
    SOCKET = connect(
        "wss://free.blr2.piesocket.com/v3/qrfchannel?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1"
    )
    print("Connected!")

    fetcher = Fetcher(args.initial_newer_than, args.saveload)
    while True:
        fetcher.on_wakeup()
        
        current_time = datetime.now().timestamp()
        if current_time - fetcher.last_trigger_time > 34:  # Check if 34 seconds have passed
            if not fetcher.triggers_list:  # Check if no trigger has been sent recently
                online_students = fetcher.players['online_user'].tolist()
                if online_students:
                    random_student = random.choice(online_students)
                    trigger_message = "Random check-in"
                    print(f"\033[92mSending random trigger to '{random_student}' on next wakeup. \033[0m")
                    fetcher.triggers_list.append((trigger_message, random_student, 10))
                    fetcher.last_trigger_time = current_time  # Update the last trigger time

        sleep(10)  # run checks every 10 seconds
        fetcher.save_tools_usage()

