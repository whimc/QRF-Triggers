# QRF triggers

Every trigger the service can send, what makes it fire, and how often it can repeat.
The **key** is the trigger's name in `trigger_config.json` and in `@trigger("...")`
in `src.py`. **Default** is the shipped `trigger_config.json` setting (on/off,
priority). Teachers see each trigger's message followed by `Category: <category>`.

How the loop works: every 10 seconds the service reads new database rows (commands,
observations, science tools, chat, block edits, deaths, region events, air clicks),
updates each player's saved state, then runs every enabled trigger. Player positions
are polled every 3 seconds. "Online" means the player has a position in the last 30
seconds. Most "once per world" flags are saved in the `--saveload` file, so they
survive a restart.

Shared definitions:

- **Counted tools**: `/gravity`, `/pressure`, `/atmosphere` (multi-use) and
  `/rotational_period`, `/scale`, `/tectonic`, `/tides`, `/year`, `/tilt`,
  `/magnetic_field`, `/tpa`, `/agent`, `/pause`, `/tpall`, `/gamemode`, `/difficulty`,
  `/op`, `/kill`, `/help`, `/pvp`, `//sphere`, `/sphere`, `//hsphere`, `/hsphere`
  (single-use). The command's first word must match exactly.
- **Near an object**: inside the object's `range` polygon from the coordinate CSV, or
  within 10 blocks (|dx| + |dz|) of its x/z point.
- **Build world**: a world whose `co_world` ID was passed with `--wid`, or a world
  named `mars` / `sdp7` (older camps).
- **NPC-disabled worlds**: LunarCrater, TiltedWarm, TiltedFrozen, TiltedMelting,
  MynoaHalf, BrownDwarf. **POI-disabled worlds**: those plus MynoaClose and Cancri.
- **Command-excluded worlds** (any capitalization): Hub, EarthControl, ETLife,
  RocketLaunch, Play, Mars.

## Tools

| Key | Default | Fires when | Repeats |
|---|---|---|---|
| `tool_use_in_build_world` | on, 2 | A counted tool is used in a build world | Every use |
| `check_use_basic_science_tools` | on, 2 | `/gravity`, `/temperature`, `/humidity`, `/oxygen` or `/wind` is used | At most every 10 min per player |
| `check_appropriate_tool_use_near_poi` | on, 3 | A command is listed in the Expected_Action of an object the player is near | Once per command |
| `tool_use_near_expected_action` | on, 6 | A science-tool command (or alias, e.g. `/temp`) is expected by a nearby object, or else by the world's Global row | Once per command |
| `check_advanced_tool_use` | on, 2 | Science tool COSMIC RAYS, ALTITUDE, SCALE, YEAR or RADIUS | Every use |
| `check_advanced_tool_use2` | on, 2 | Science tool PRESSURE, TIDES, TILT, TECTONIC, MAGNETIC_FIELD, RADIATION, ATMOSPHERE, ROTATIONAL_PERIOD, DAYLENGTH or AIRFLOW | Every use |
| `check_tool_inspiration_specific` | on, 1 | Player uses the same science tool another player just used in the same world | Every match |
| `check_tool_inspiration_generic` | on, 1 | Player uses a different science tool right after another player in the same world | Every match |
| `check_3_tools_in_1_minute` | on, 7 | 3+ counted tools within 60 s | Resets after firing |
| `check_high_tool_use` | on, 7 | More than 10 counted tools in the current world (first 3 worlds), more than 5 after that | Once per world |
| `check_combined_multi_use_tools` | on, 4 | `/gravity`, `/pressure` or `/atmosphere` used 2+ times in the current world; a second message once all three have been | Once per tool per world |
| `check_single_use_tools` | on, 7 | First use of each single-use tool in the current world | Once per tool per world |
| `check_last_tool_use_over_20_minutes` | on, 1 | No counted tool for 20 minutes | At most every 20 min |
| `no_tool_use_by_third_world` | off, 2 | Visited 3+ worlds without any counted tool | Once per world |
| `check_tool_use_counts` | off, 4 | `/gravity` more than twice, or another counted tool more than 3 times, in one world | Once per tool per world |
| `check_five_or_more_tools_in_world` | off, 8 | 5+ counted tools in one world | Once per world |

## Observations

| Key | Default | Fires when | Repeats |
|---|---|---|---|
| `observation_in_build_world` | on, 2 | Observation in a build world | Every observation |
| `observation_near_poi` | on, 5 | Observation near an object; reports the object whose name is most similar to the text | Once per observation |
| `check_nearby_similar_observation` | on, 5 | Observation 1–9 blocks from an earlier observation (anyone's) in the same world since the service started; text similarity is reported, not required | Once per observation |
| `check_question_like_observation` | on, 3 | Chat message or observation containing `?` or a question word (what, when, where, why, who, which, how) | At most every `cooldown_ms` (5 min) per player |
| `check_3_observations_in_2_minutes` | on, 3 | 3+ observations within 2 minutes | Resets after firing |
| `check_no_observations_last_20_minutes` | on, 1 | No observation for 20 minutes | At most every 20 min |
| `check_mynoa_observations` | on, 1 | 25+ minutes in a world starting with "Mynoa" with no observation in it | Once per visit |
| `no_observations_by_third_world` | on, 2 | Visited 3+ worlds without any observation | Once per world |
| `high_observation_count` | off, 7 | More than 10 observations in the current world (first 3 worlds), more than 5 after that | Once per world |
| `reached_5_observations_in_world` | off, 7 | 5 observations in the current world | Once per world |
| `check_five_or_more_observations_in_world` | off, 8 | 5+ observations in any world | Once per world |

## Chat

| Key | Default | Fires when | Repeats |
|---|---|---|---|
| `check_3_chat_entries_in_1_minute` | on, 4 | 3+ chat messages within 60 s | At most every minute |
| `check_high_chat_volume` | on, 1 | `check_high_chat_volume_threshold` (5) messages within `check_high_chat_volume_minutes` (2) minutes | At most once per that window |
| `check_five_chat_messages_in_world` | on, 8 | 5+ chat messages in one world | Once per world |

`check_question_like_observation` (above) also checks chat.

## Movement and places

Positions are (x, z) pairs recorded whenever a player has moved since the last 3-second poll.

| Key | Default | Fires when | Repeats |
|---|---|---|---|
| `check_racing_non_stopping` | on, 6 | Fewer than 2 short moves (< 10 blocks) among the last 20 recorded moves | Resets after firing |
| `check_possible_afk_behavior` | on, 3 | No movement for 90 s | Every further 90 s without movement |
| `check_long_pair_close` | on, 5 | Two players in the same world within 35 blocks for 120 s (sent for the first name alphabetically) | Every further 120 s together |
| `check_long_far_from_crowd` | off, 4 | No other player within 35 blocks in the same world for 120 s | Every further 120 s |
| `check_world_exploration` | on, 1 | Among the first 3 players to visit both x > 0 and x < 0 in MynoaMangrove, ColderStrip, ColderHot, ColderCold, TwoMoons, TwoMoonsLow, TiltedWarm, TiltedMelting or TiltedFrozen | Once per world |
| `check_in_pause_box` | on, 2 | Arrives in the Hub | Once per arrival |
| `check_visits_to_unowned_region` | on, 1 | WorldGuard VISIT to a region the player isn't a member of | Once per player per run |
| `check_prolonged_stop_in_region` | on, 2 | 90 s in a WorldGuard region (VISIT without LEAVE) | Once per stay |
| `check_movement_toward_npc_or_poi` | on, 4 | Within 10 blocks of the nearest NPC/POI point and closer than on the previous loop (not in NPC-disabled worlds) | Each approach |
| `check_movement_away_from_npc_or_poi` | on, 2 | Was within 10 blocks of the nearest NPC/POI point and is now further away | Each departure |
| `check_prolonged_interaction_npc` | on, 3 | Within 4 blocks of an NPC for 60 s (not in NPC-disabled worlds) | About every 10 s while still there |
| `check_multiple_npc_visits` | on, 1 | Within 4 blocks of 2+ different NPCs in 5 minutes | Visit log cleared after firing |
| `check_ignores_nearby_npc` | on, 2 | Within 5 blocks of an NPC for 10 s | Every loop while still there |
| `check_prolonged_stay_poi` | on, 3 | Inside a POI range for 90 s (not in POI-disabled worlds) | About every 10 s while still inside |
| `check_visit_unmarked_pois` | off, 1 | Outside every POI range for 5 minutes | At most every 10 min |
| `check_avoiding_poi` | off, 2 | No POI range visited for 5 minutes | At most every 5 min |
| `check_high_x_axis_movement` | off, 2 | Over the last 6+ moves, total \|dx\| ≥ 3 × total \|dz\| | At most every 2 min |
| `check_low_x_axis_movement` | off, 2 | \|dx\| / \|dz\| < 3 and total \|dx\| < 15 | At most every 2 min |
| `check_high_y_axis_movement` | off, 2 | Total \|dz\| ≥ 3 × total \|dx\| (the "y" in the name is really z) | At most every 2 min |
| `check_low_y_axis_movement` | off, 2 | \|dz\| / \|dx\| < 3 and total \|dz\| < 20 | At most every 2 min |
| `check_dominant_z_axis_movement` | on, 2 | Cannot fire (needs height data, see below) | — |

## Commands

| Key | Default | Fires when | Repeats |
|---|---|---|---|
| `check_use_of_disabled_mc_commands` | on, 2 | `/kill`, `/agent`, `/gamemode`, `/op`, `/summon`, `/tp`, `/give`, `/ban` or `/kick` outside the command-excluded worlds | Every command |
| `check_specific_commands` | off, 3 | `/kill`, `/enable pvp`, `/god`, `/gamemode`, `/difficulty`, `/op`, `/help` or `/agent chat` outside the command-excluded worlds | Every command |
| `check_teleporting_to_multiple_players` | off, 4 | `/tp` or `/tpa` naming 2+ players | Every command |
| `check_used_help_command` | off, 1 | `/help` | Once per player per run |

## Blocks (CoreProtect)

Block counts only include edits made after the trigger last fired for that player, so
the same blocks don't trigger twice while they're still inside the time window.

| Key | Default | Fires when | Repeats |
|---|---|---|---|
| `check_block_triggers` | on, CSV | A row of `BlockBasedTriggers.csv` is reached in a build world: at least `High_Threshold` blocks of `type` placed (`action` 1) or broken (0) within `Time_Window_High` seconds (at least 15 s). Priority comes from the CSV row. | At most one per player every 3 min |
| `check_over_200_placed_actions_in_2_minutes` | on, 1 | Over 120 blocks placed in 2 minutes in a build world | After 120 more |
| `check_over_200_destroyed_actions_in_2_minutes` | off, 1 | Over 120 blocks broken in 2 minutes in a build world | After 120 more |
| `check_over_200_actions_in_2_minutes` | off, 1 | Over 40 places + breaks in 2 minutes in a build world | After 40 more |
| `check_breaks_own_block` | on, 2 | Broke 20+ blocks they had placed, within 30 s (any world, last 10 min) | At most every 10 min |
| `check_block_breaks_by_others` | on, 1 | Broke 20+ blocks other players placed, within 30 s (any world, last 10 min) | At most every 10 min |

Edits by non-players (CoreProtect users starting with `#`, such as `#tnt`) are ignored.

## Other

| Key | Default | Fires when | Repeats |
|---|---|---|---|
| `achieves_death` | on, 1 | Player died; the message includes the cause | Once per death |
| `check_airclick_burst` | on, 2 | More than `click_threshold` (5) air clicks within `time_window_ms` (3 minutes) | At most every 5 min per player |
| `check_random_checkin` | on, 10 | No trigger sent for `quiet_seconds` (300); picks a random online player | Whenever the camp is quiet that long |

## Tuning fields

Besides `enabled`, `priority` and `category`, these optional fields can be added to a
trigger's entry in `trigger_config.json`; the Trigger Manager keeps them when saving.

| Key | Field | Default |
|---|---|---|
| `check_airclick_burst` | `click_threshold`, `time_window_ms` | 5, 180000 |
| `check_question_like_observation` | `cooldown_ms` | 300000 |
| `check_high_chat_volume` | `check_high_chat_volume_threshold`, `check_high_chat_volume_minutes` | 5, 2 |
| `check_random_checkin` | `quiet_seconds` | 300 |

## Known limitations

- **Axis triggers measure x against z.** Positions are stored as (x, z), so "y" means z.
- **`check_dominant_z_axis_movement` cannot fire.** It needs (x, y, z) positions and
  the position query only has x and z. Its second rule (dz/dy < 3) would be true for
  almost everyone, so review it and add a cooldown before adding height to the query.
- **Tool inspiration only compares tool uses within one 10-second loop.** Uses a minute
  apart are not linked.
- **Prolonged NPC/POI triggers repeat about every 10 s** once over the threshold, and
  `check_ignores_nearby_npc` repeats every loop. This "breathing time" is from the 2024
  version; change the `now - 50` / `now - 80` / `now - 5` resets in `src.py` to make
  them fire once per visit.
- **`check_ignores_nearby_npc` can't tell whether a player talked to the NPC**; there is
  no interaction data, so it fires for anyone standing near one.
- **`check_appropriate_tool_use_near_poi` and `tool_use_near_expected_action` overlap**:
  a science tool used near an object that expects it fires both.
- **`check_racing_non_stopping` looks at the last 20 moves, not a time window.** A player
  standing still adds no positions.
- **Unowned-region visits fire once per player per run**, no matter how many regions.
- The **Low_Threshold** / **Time_Window_Low** columns of `BlockBasedTriggers.csv` are not
  used (see past features).

## Past features and changes

### Removed or never-working behavior

- **Low block usage (2024, `main` branch).** `check_block_triggers` also sent "Low Block
  Usage: X has placed only N … blocks in the last T seconds" when a player's count in
  `Time_Window_Low` was at or below `Low_Threshold`. It was commented out in 2025 and
  has not run since; the CSV columns remain.
- **Per-material block keys.** A 2025 draft looked up `block_<material>_placed` /
  `block_<material>_destroyed` in `trigger_config.json` (hence the old `block_grass_placed`
  and `block_stone_destroyed` entries). That code was commented out; the entries were
  removed in October 2026.
- **`time_dilation`** (2024): a testing offset added to the 20-minute checks to make them
  fire sooner. It was always 0 and is gone.
- **Restored in October 2026:** `no_observations_by_third_world`, `high_observation_count`
  and `reached_5_observations_in_world` were deleted on 27 July 2025 (commit `37acdcd`)
  but kept their config entries.
- **Build-world observations never fired in 2025.** The code called
  `self.get_wid_for_world(world)` on a DataFrame, which always failed; worlds are now
  matched by `--wid`.

### Behavior changed in October 2026

- `check_single_use_tools` now fires on the first use. Every tool command was counted
  twice, so it never fired, and "used more than once" fired after a single use.
- `check_random_checkin` now only fires after 5 quiet minutes. Since 2024 it fired every
  5 minutes regardless.
- 20-minute triggers only fire for online players and each has its own cooldown (they
  shared one, so only one of the two could fire).
- Chat triggers count each message once and no longer repeat every loop.
- `check_in_pause_box` fires on arrival instead of every 10 s.
- `check_prolonged_stop_in_region` can fire; its timer only advanced when the same
  event was re-read, so it never reached 90 s.
- `check_long_pair_close` resets when the pair splits up; separate meetings used to add up.
- `check_question_like_observation` checks observations (it read a column that doesn't
  exist) and matches whole words only.
- `check_block_triggers` sees the full time window (up to an hour; it used to see only
  the last 2 minutes) and doesn't repeat for the same blocks.
- `check_breaks_own_block` and `check_block_breaks_by_others` each have their own
  10-minute cooldown (they shared a once-per-run list).
- `check_airclick_burst` has a per-player cooldown (it was global) and reads the whole
  3-minute window (the query covered 30 s).
- `achieves_death` reports each death once (every death ever recorded was re-read each loop).
- `observation_near_poi` and the two tool-near-object triggers send one trigger per
  observation/command instead of one per matching object.
- Disabled-world lists use the current world names (they used 2024 names such as
  `TiltedEarth_Frozen`, so nothing was excluded), and `TiltedMelting` counts for
  `check_world_exploration` (it was misspelled).
- `check_possible_afk_behavior` repeats every 90 s of inactivity instead of every 30 s.

### Branch history

| Branch | Last updated | Notes |
|---|---|---|
| `June-camp-trigger-update` | Jun 2024 | SDP7 camp priority updates |
| `main` | Sep 2024 | Summer 2024 triggers; block triggers with high and low thresholds; random check-in |
| `Luc-added-Sep-2024` | Apr 2025 | Luc's block-trigger timing by material (`prev_trig_time`) |
| `June-2025`, `June-(WCA-blockbased)` | Jul 2025 | WCA camp: Trigger Manager GUI, `trigger_config.json`, observation-count triggers, new movement/NPC/command/block triggers |
| `AIED` | Jul 2025 | Death cause in `achieves_death`; None-safe NPC timer |
| `Maine2025` | Aug 2025 → Oct 2026 | U Maine camp (wids 133/134); this rewrite |
