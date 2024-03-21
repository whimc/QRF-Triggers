import json
import signal
import sys
from datetime import datetime, date, time
from pathlib import Path
from time import sleep

import pandas as pd
import sqlalchemy as db
from websockets.sync.client import connect


with open(Path(__file__).parent / "credentials.json") as f:
    creds = json.load(f)
ENG = db.create_engine(db.URL.create("mysql+mysqlconnector", **creds["database"]))

TODAY = datetime.combine(date.today(), time.min)

print("Connecting to socket... ")
SOCKET = connect(
    "wss://free.blr2.piesocket.com/v3/qrfchannel?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1"
)
print("Connected!")

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


def send_trigger(trigger_name: str, username: str, priority: int):
    payload = {
        "event": "new_message",
        "data": {
            "from": "software",
            "software": "WHIMC",
            "timestamp": "",
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


def get_data(query, newer_than: datetime = TODAY) -> pd.DataFrame:
    return pd.read_sql(query.format(newer_than=newer_than), ENG)


class Fetcher:
    CMDS = {
        "commands": GET_COMMANDS,
        "observations": GET_OBSERVATIONS,
        "science_tools": GET_SCIENCE_TOOLS,
    }

    def __init__(self):
        self.players = get_data(GET_ONLINE_PLAYERS)
        self.newer_than = TODAY
        self.commands = pd.DataFrame()
        self.observations = pd.DataFrame()
        self.science_tools = pd.DataFrame()

    def fetch_data(self):
        for key, query in Fetcher.CMDS.items():
            df = get_data(query, self.newer_than)
            # Set 'self.<key>' to the new dataframe
            setattr(self, key, df)

    def on_wakeup(self):
        now = datetime.now()
        self.fetch_data()

        # Send all triggers
        for trigger_name, username, priority in self.triggers():
            print(f"Triggered '{trigger_name}' for '{username}' (priority {priority})")
            send_trigger(trigger_name, username, priority)

        # Next iteration should /only/ show new data
        self.newer_than = now

    def triggers(self) -> list[tuple[str, str, int]]:
        """
        Return any triggers as a list of tuple[trigger name, username, priority]
        """
        triggers = []

        # TODO add checks here
        # trigger = ("test", "Poi", 1)
        # triggers.append(trigger)

        return triggers


if __name__ == "__main__":
    import signal

    def handle_sigint(sig, frame):
        # Prevent Ctrl+C from hanging
        SOCKET.close()
        print("Stopping!")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    fetcher = Fetcher()
    while True:
        print(f"Wakeup at {datetime.now()}")
        fetcher.on_wakeup()
        sleep(5)  # run checks every 5 seconds
