import json

from datetime import datetime, time
from pathlib import Path

import pandas as pd
import sqlalchemy as db

from websockets.sync.client import connect


with open(Path(__file__).parent / "credentials.json") as f:
    creds = json.load(f)
ENG = db.create_engine(db.URL.create("mysql+mysqlconnector", **creds["database"]))


def send_trigger(username, trigger_name):
    payload = {
        "event": "new_message",
        "data": {
            "from": "software",
            "software": "WHIMC",
            "timestamp": "",
            "eventID": "",
            "student": username,
            "trigger": trigger_name,
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

    # replace '1' with 'qrfchannel
    with connect(
        "wss://free.blr2.piesocket.com/v3/1?api_key=4TRTtRRXmvNwXCWUFIjgKLDdZJ0zwoKpzn5ydd7Y&notify_self=1"
    ) as websocket:
        websocket.send(json.dumps(payload))
        message = websocket.recv()
        print(f"Received: {message}")


def get_commands(last_query: datetime | None = None) -> tuple[pd.DataFrame, datetime]:
    all_users = """
    select cmd.time as time
        , usr.user as username
        , message
    from co_command as cmd
    left join co_user as usr
    on cmd.user = usr.rowid;
    """
    return pd.read_sql(all_users, ENG), datetime.now()


def get_online_players():
    online_users = """
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
    return pd.read_sql(online_users, ENG)


# print(get_commands())
print(get_online_players())

# send_trigger("Poi", "Test")
