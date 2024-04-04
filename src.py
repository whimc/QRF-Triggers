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

# =============================================================================
# Database Connection
# =============================================================================

with open(Path(__file__).parent / "credentials.json") as f:
    creds = json.load(f)
ENG = db.create_engine(db.URL.create("mysql+mysqlconnector", **creds["database"]))

# =============================================================================
# Global Variables
# =============================================================================

# Set after parsing args
SOCKET = None


# =============================================================================
# SQL Queries
# =============================================================================

GET_ONLINE_PLAYERS = """
select usr.user as online_user, last_time as login_time from (
    select user
         , max(time) as last_time
    from co_session
    where time >= unix_timestamp(current_date() - 1) -- only look back 1 day
    group by user
) as last_times
left join co_session as sess on last_times.user = sess.user and last_times.last_time = sess.time
left join co_user as usr on last_times.user = usr.rowid
where sess.action = 1
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
    payload = {
        "event": "new_message",
        "data": {
            "from": "software",
            "software": "WHIMC",
            # Expects a millisecond unix timestamp
            "timestamp": int(datetime.now().timestamp() * 1000),
            "eventID": "",
            "student": username,
            "trigger": trigger_name,
            "priority": priority,
        },
    }
    # Have to populate 'masterlogs' with 'data' plus a few empty fields
    payload["data"]["masterlogs"] = {
        **payload["data"],
        "reviewer": "",
        "end": "",
        "feedbackTXT": "",
        "feedbackREC": "",
    }
    SOCKET.send(json.dumps(payload))
    SOCKET.recv()


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

    def __init__(self, initial_newer_than):
        self.newer_than = initial_newer_than
        self.players = get_data(GET_ONLINE_PLAYERS)
        self.commands = pd.DataFrame()
        self.observations = pd.DataFrame()
        self.science_tools = pd.DataFrame()

        # dataframes / dictionaries for triggers
        self.triggers_list = []
        self.gravity_usage = {}

    def fetch_data(self):
        for key, query in Fetcher.CMDS.items():
            df = get_data(query, self.newer_than)
            # Set 'self.<key>' to the new dataframe
            setattr(self, key, df)
            '''
            if key == 'commands':
                print(f"Fetched commands:\n{df}")
            if key == 'players':
                print (f"Fetched online players: {df}")
            '''

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
        print (f"COMMANDS:\n{self.commands}\n")
        print (f"OBSERVATIONS:\n{self.observations}\n")
        print (f"SCIENCE TOOLS:\n{self.science_tools}\n")

        self.update_gravity_usage()
        print (f"GRAVITY USAGE: \n{self.gravity_usage}\n")
        # print (f"{self.gravity_usage['n3iTh4N']}")


        # Send all triggers
        for trigger_name, username, priority in self.triggers():
            print(f"\033[93m Triggered '{trigger_name}' for '{username}' (priority {priority}) \033[0m")
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

    def update_gravity_usage(self):
        for _, row in self.commands.iterrows():
            if '/gravity' in row['message']:
                user = row['username']
                world = row['world']

                # Initialize the user in the dictionary if not present
                if user not in self.gravity_usage:
                    self.gravity_usage[user] = {}

                # Initialize the world for the user if not present
                if world not in self.gravity_usage[user]:
                    self.gravity_usage[user][world] = 0
                    # gravity == 2 flag (don't repeat)
                    self.gravity_usage[user][world + "_flag"] = 0

                # Increment the count
                self.gravity_usage[user][world] += 1

                # Always update what world the user currently is in
                self.gravity_usage[user]['current_world'] = world

        #{'n3iTh4N' <- user: data -> {'EarthControl': 4, 'current_world': 'EarthControl'}}
        for user, data in self.gravity_usage.items():
            current_world = data.get('current_world')

            if current_world:
                gravity_count = data.get(current_world, 0)
                gravity_flag_key = current_world + "_flag"
                gravity_flag = data.get(gravity_flag_key, 0)

                if gravity_count == 2 and gravity_flag == 0:
                    print(f"{user} has used '/gravity' more than once in {current_world}")
                    self.triggers_list.append((f"{user} has used '/gravity' more than once in {current_world}", user, 1))
                    self.gravity_usage[user][gravity_flag_key] = 1

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
    args = parser.parse_args()

    print("Connecting to socket... ")
    SOCKET = connect(
        "wss://free.blr2.piesocket.com/v3/qrfchannel?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1"
    )
    print("Connected!")

    fetcher = Fetcher(args.initial_newer_than)
    while True:
        fetcher.on_wakeup()
        sleep(10)  # run checks every 5 seconds
