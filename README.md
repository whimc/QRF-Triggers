# QRF Triggers

A Python service for the WHIMC Minecraft server. Every 10 seconds it reads recent
player activity from the WHIMC database (positions, commands, observations, science
tools, chat, block edits, deaths, region visits) and sends **triggers**, short notes
such as "alice has not made any observations in the last 20 minutes", to the QRF app
that teachers watch during camp.

- What every trigger does, plus past features and changes: [docs/TRIGGERS.md](docs/TRIGGERS.md)
- How the code is organized: the docstring at the top of `src.py`

## Setup

Requires Python 3.11 or newer.

1. Create a virtual environment and install the packages:

   ```console
   python -m venv venv
   venv\Scripts\activate            # Windows
   source venv/bin/activate         # Mac / Linux
   pip install -r requirements.txt
   ```

   If `venv` was made with a Python version that is no longer installed, delete the
   `venv` folder and run these again.

2. Copy `credentials.json.template` to `credentials.json` and fill it in:

   - `database`: the WHIMC MySQL login.
   - `qrf_socket_url`: the QRF WebSocket channel URL, including the PieSocket API key.
     Without it, triggers are only written to the log, not sent to teachers.

   `credentials.json` is ignored by git. Never put keys or passwords in `src.py`.

## Running

```console
python src.py --wid 133 134 --saveload umaine2025am --initial-newer-than "2025-07-27 12:30:00"
```

| Option | Meaning |
|---|---|
| `--wid` (required) | `co_world` ID(s) of the camp's build world(s). Block triggers and the build-world triggers only look at these worlds. |
| `--saveload FILE` | Saves each player's progress to `FILE` every loop and loads it on start, so a camp can be stopped and resumed. Use one file per camp session. |
| `--initial-newer-than "YYYY-MM-DD hh:mm:ss"` | Central time to start reading activity from (default: now). |
| `--no-gui` | Don't open the Trigger Manager window. |
| `--verbose` | Show debug output (fetched rows, saves) in the console. |

Press Ctrl+C to stop; progress is saved first. Each run writes a log to `logs/`.

When resuming from a `--saveload` file, activity since `--initial-newer-than` is read
again and added to the saved counts. To avoid counting it twice, set it to roughly when
the service was stopped, or leave it out.

The **Trigger Manager** window opens alongside the service. Tick or untick triggers
and change priorities, then press Save; the running service picks up the changes on its
next loop. You can also edit `trigger_config.json` directly.

### Batch files

`maine25am.bat` and `maine25pm.bat` are the launchers used on the camp server
(`C:\MCSERVERS\QRF-Triggers`). They create the virtual environment if needed, install
the requirements and start the service. Copy one for a new camp and change the
options on the `python src.py` line.

### Setting up a new camp

1. Find the camp build worlds' IDs: `select rowid, world from co_world;`.
2. Make a `.bat` file (or command) with those IDs in `--wid`, a new `--saveload` name,
   and the camp's start time in `--initial-newer-than`.
3. Check `trigger_config.json` (or the Trigger Manager) for which triggers to use.
4. If the camp uses new worlds, add their places, NPCs and expected tools to
   `WHIMC Coordinate Tracking updated.csv`.

## Files

| File | Purpose |
|---|---|
| `src.py` | The service |
| `trigger_config.json` | On/off, priority, category and optional tuning fields per trigger |
| `WHIMC Coordinate Tracking updated.csv` | Places, NPCs and expected science tools per world. Blank World/Object cells repeat the row above; each world's `Global` row lists tools that fit anywhere in it. |
| `BlockBasedTriggers.csv` | Thresholds per block material for `check_block_triggers` |
| `credentials.json.template` | Template for `credentials.json` |
| `maine25am.bat`, `maine25pm.bat` | Camp launchers |
| `Umaine2025am`, `umaine2025pm`, `outputs/` | Saved player state from past camps and tests |
| `tests/simulate_camp.py` | Simulated camp session (see below) |
| `docs/TRIGGERS.md` | What every trigger does, tuning fields, and the history of past features |

The old `backups/` folder (hand-made copies of `src.py`, the coordinate CSV and
state files from June 2025) was removed in October 2026. The files are still in git
history: `git log --oneline -- backups` lists the commits, and
`git show <commit>:"backups/src-backup.py"` prints a file as of that commit.

## Testing without the server

```console
python tests/simulate_camp.py
```

This runs the real trigger code against a fake database, clock and QRF connection,
scripting a camp (tool use, observations, chat, movement, block building, deaths,
quiet periods) and checking that the right triggers fire, without repeats. Add a
scenario there when you add or change a trigger.

## Adding a trigger

1. In `src.py`, add a `Fetcher` method decorated with `@trigger("my_trigger_name")`.
   It receives `cfg` (the trigger's settings) and calls
   `self.emit(cfg, username, "message")`; pass `cooldown=seconds` to limit repeats.
2. Add `"my_trigger_name"` to `trigger_config.json` with `enabled`, `priority` and `category`.
3. Describe it in `docs/TRIGGERS.md` and add a check to `tests/simulate_camp.py`.
