import csv
import json
import os
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import zoneinfo
import requests
from bs4 import BeautifulSoup

# ==========================================
# CONFIGURATION
# ==========================================
NUM_TVS = 3
MATCH_DURATION_MINUTES = 115
LOCAL_TIMEZONE = zoneinfo.ZoneInfo("America/New_York")
MAX_WORKERS = 16

CACHE_FILE = "elo_cache.json"
CACHE_EXPIRY_HOURS = 12

CLUB_ELO_RANKING_URL = "https://clubelo.com/Ranking"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

OUTPUT_CSV = "tv_assignment_schedule.csv"
OUTPUT_TXT = "tv_schedule_summary.txt"

DEFAULT_ELO = 1450

# Track all UEFA, domestic top flights, secondary tiers, and domestic cups
LEAGUES = [
    # Continental (UEFA)
    "uefa.champions",
    "uefa.europa",
    "uefa.europa.conf",

    # Domestic Top 5 (Tier 1 & Tier 2)
    "eng.1", "eng.2",
    "esp.1", "esp.2",
    "ger.1",
    "ita.1",
    "fra.1",

    # European Domestic Leagues
    "por.1",  # Portugal
    "bel.1",  # Belgium
    "tur.1",  # Turkey
 
    # Domestic Cups
    "eng.fa", "eng.league_cup",
    "esp.copa_del_rey",
    "ger.dfb_pokal",
    "ita.coppa_italia",
    "fra.coupe_de_france",
]

# ==========================================
# HELPER FUNCTIONS & ALIASES
# ==========================================
def strip_accents(s):
    """Normalize and strip diacritics (e.g., 'Cádiz' -> 'cadiz', 'Köln' -> 'koln')."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    ).lower().strip()

# ==========================================
# LEAGUE DISPLAY FORMATTER
# ==========================================
LEAGUE_DISPLAY_NAMES = {
    # Domestic Cups
    "English FA Cup": "FA Cup",
    "English Carabao Cup": "EFL Cup",
    "Spanish Copa del Rey": "Copa del Rey",
    "German DFB-Pokal": "DFB-Pokal",
    "Italian Coppa Italia": "Coppa Italia",
    "French Coupe de France": "Coupe de France",
    
    # Continental
    "UEFA Champions League": "UCL",
    "UEFA Europa League": "UEL",
    "UEFA Conference League": "UECL",
    
    # Domestic Leagues
    "English Premier League": "EPL",
    "English League Championship": "Championship",
    "English League One": "League One",
    "Spanish LALIGA": "LaLiga",
    "Spanish Segunda División": "LaLiga 2",
    "German Bundesliga": "Bundesliga",
    "German 2. Bundesliga": "2. Bundesliga",
    "Italian Serie A": "Serie A",
    "Italian Serie B": "Serie B",
    "French Ligue 1": "Ligue 1",
    "French Ligue 2": "Ligue 2",
    "Dutch Eredivisie": "Eredivisie",
    "Portuguese Primeira Liga": "Liga Portugal",
    "Belgian Pro League": "Belgian Pro",
    "Scottish Premiership": "SPL",
    "Turkish Super Lig": "Süper Lig",
}

def format_league_badge(league_name):
    """Returns a clean, shortened display badge for the competition."""
    short = LEAGUE_DISPLAY_NAMES.get(league_name)
    if not short:
        # Fallback: remove redundant boilerplate if ESPN sends raw strings
        short = league_name.replace("English ", "").replace("Spanish ", "").replace("German ", "")
    return f"[{short}]"

# Only keep true language translation divergences, abbreviations, and severe typos
NAME_ALIASES = {
    # England (Critical: City & United)
    "manchester city": "man city",
    "manchester united": "man united",
    "nottingham forest": "forest",
    "athletic bilbao": "athletic",
    "athletic club": "athletic",
    "tottenham hotspur": "tottenham",
    "afc bournemouth": "bournemouth",
    "brighton & hove albion": "brighton",
    "wolverhampton wanderers": "wolves",
    "newcastle united": "newcastle",
    "west ham united": "west ham",

    # Belgium
    "union st.-gilloise": "st gillis",
    "union saint-gilloise": "st gillis",
    "royale union saint-gilloise": "st gillis",
    "rusg": "st gillis",
    "sint-truidense": "st truiden",
    "sint-truiden": "st truiden",
    "st. truidense": "st truiden",
    "st. truiden": "st truiden",
    "raal la lovuiere": "la louvière",

    # Portugal
    "academico de viseu": "ac viseu",
    "académico de viseu": "ac viseu",
    "academico viseu": "ac viseu",
    "sporting cp": "sporting",
    "sporting lisbon": "sporting",

    # Germany, France, Turkey
    "fc cologne": "köln",
    "cologne": "köln",
    "bayern munich": "bayern münchen",
    "paris saint-germain": "paris sg",
    "stade rennais": "rennes",
    "stade rennais fc": "rennes",
    "amed sfk": "amed",
    "inter milan": "inter",
    "internazionale": "inter",
    "ac milan": "milan",

    #Other
    # Greece
    "olympiacos": "olympiakos",
    "olympiacos fc": "olympiakos",
    # Netherlands
    "az alkmaar": "az",
}

# ==========================================
# 1. LOAD CLUB ELO RATINGS (CACHED)
# ==========================================
def load_cached_elos():
    if os.path.exists(CACHE_FILE):
        mtime = datetime.fromtimestamp(os.path.getmtime(CACHE_FILE), tz=timezone.utc)
        if datetime.now(timezone.utc) - mtime < timedelta(hours=CACHE_EXPIRY_HOURS):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) > 100:
                        print(f"Loaded {len(data)} club ratings from local cache.")
                        return data
            except Exception:
                pass
    return None

def save_cached_elos(data):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass

elo_dict = load_cached_elos()

if not elo_dict:
    print("Fetching club Elo ratings from Club Elo (/Ranking)...")
    elo_dict = {}
    try:
        resp = requests.get(CLUB_ELO_RANKING_URL, headers=HEADERS, timeout=12)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for span in soup.find_all("span", class_="Ast"):
                name = span.get_text(strip=True).lower()
                td = span.find_parent("td")
                if td:
                    next_td = td.find_next_sibling("td")
                    if next_td and next_td.get_text(strip=True).isdigit():
                        rating = int(next_td.get_text(strip=True))
                        if rating >= 900:
                            elo_dict[name] = rating

            if elo_dict:
                save_cached_elos(elo_dict)
                print(f"Loaded and cached {len(elo_dict)} club Elo ratings.")
    except Exception as e:
        print(f"Notice: Failed to fetch ratings ({e}). Falling back to defaults.")

if not elo_dict:
    elo_dict = {}

# Build stripped dict here after elo_dict is loaded
stripped_elo_dict = {strip_accents(k): v for k, v in elo_dict.items()}

def get_elo(team_name):
    raw_norm = team_name.lower().strip()

    # 1. Check explicit manual aliases
    aliased = NAME_ALIASES.get(raw_norm, raw_norm)
    if aliased in elo_dict:
        return elo_dict[aliased]

    # 2. Direct exact match
    if raw_norm in elo_dict:
        return elo_dict[raw_norm]

    # 3. Accent-stripped direct match
    clean_target = strip_accents(aliased)
    if clean_target in stripped_elo_dict:
        return stripped_elo_dict[clean_target]

    # 4. Fuzzy punctuation & substring match
    simplified_target = clean_target.replace(".", "").replace("-", " ")
    for k, val in stripped_elo_dict.items():
        clean_k = k.replace(".", "").replace("-", " ")
        if len(clean_k) >= 4 and (clean_k in simplified_target or simplified_target in clean_k):
            return val

    return DEFAULT_ELO

# ==========================================
# 2. DEFINE 3:00 AM BOUNDARIES (48-HOUR SPAN)
# ==========================================
now_local = datetime.now(LOCAL_TIMEZONE)
now_utc = datetime.now(timezone.utc)

today_3am = now_local.replace(hour=3, minute=0, second=0, microsecond=0)
if now_local < today_3am:
    t_start_local = today_3am - timedelta(days=1)
else:
    t_start_local = today_3am

t_split_local = t_start_local + timedelta(days=1)  # 3:00 AM tomorrow
t_end_local = t_start_local + timedelta(days=2)    # 3:00 AM day after tomorrow

t_start_utc = t_start_local.astimezone(timezone.utc)
t_split_utc = t_split_local.astimezone(timezone.utc)
t_end_utc = t_end_local.astimezone(timezone.utc)

print(
    f"Window: {t_start_local.strftime('%a %b %d, %I:%M %p')} to {t_end_local.strftime('%a %b %d, %I:%M %p %Z')}"
)
print(f"Day Split: {t_split_local.strftime('%a %b %d, %I:%M %p %Z')}")

# Identify dates in UTC covering this 48h span
date_keys = set()
curr = t_start_utc
while curr <= t_end_utc + timedelta(days=1):
    date_keys.add(curr.strftime("%Y%m%d"))
    curr += timedelta(days=1)

print(f"Querying {len(LEAGUES)} leagues across ESPN in parallel...")

tasks = []
for league_code in LEAGUES:
    for d_str in date_keys:
        tasks.append((league_code, d_str))

def fetch_espn_fixtures(task):
    league_code, d_str = task
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league_code}/scoreboard?dates={d_str}"
    extracted_events = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        data = r.json()
        league_name = data.get("leagues", [{}])[0].get("name", league_code)
        for event in data.get("events", []):
            extracted_events.append((league_name, event))
    except Exception:
        pass
    return extracted_events

raw_events = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(fetch_espn_fixtures, t) for t in tasks]
    for f in as_completed(futures):
        raw_events.extend(f.result())

seen_event_ids = set()
raw_matches = []

for league_name, event in raw_events:
    event_id = event.get("id")
    if not event_id or event_id in seen_event_ids:
        continue

    date_str = event.get("date")
    utc_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    end_dt_utc = utc_dt + timedelta(minutes=MATCH_DURATION_MINUTES)

    # Strictly require kickoff to be within the 48-hour window [t_start_utc, t_end_utc)
    if not (t_start_utc <= utc_dt < t_end_utc):
        continue

    comp = event.get("competitions", [{}])[0]
    competitors = comp.get("competitors", [])
    if len(competitors) < 2:
        continue

    home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
    away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

    home_team = home.get("team", {}).get("displayName", "Home")
    away_team = away.get("team", {}).get("displayName", "Away")

    local_dt = utc_dt.astimezone(LOCAL_TIMEZONE)

    # Day bucket classification
    day_group = "TODAY" if utc_dt < t_split_utc else "TOMORROW"

    # Status tracking (Completed, Active, or Upcoming)
    status_type = event.get("status", {}).get("type", {}).get("name", "")
    if "STATUS_FINAL" in status_type or now_utc >= end_dt_utc:
        live_status = "FINAL"
    elif utc_dt <= now_utc:
        mins_in = int((now_utc - utc_dt).total_seconds() / 60)
        if mins_in <= 45:
            live_status = f"LIVE ~{mins_in}'"
        elif mins_in <= 60:
            live_status = "LIVE HT"
        else:
            live_status = f"LIVE ~{mins_in - 15}'"
    else:
        live_status = "UPCOMING"

    broadcasts = [b.get("names", []) for b in comp.get("broadcasts", [])]
    channels = ", ".join([item for sublist in broadcasts for item in sublist]) or "Check listings"

    home_elo = get_elo(home_team)
    away_elo = get_elo(away_team)

    avg_elo = (home_elo + away_elo) / 2.0
    diff_elo = abs(home_elo - away_elo)
    sort_value = round(avg_elo - diff_elo, 1)

    seen_event_ids.add(event_id)
    raw_matches.append({
        "day_group": day_group,
        "league": league_name,
        "home_team": home_team,
        "away_team": away_team,
        "home_elo": home_elo,
        "away_elo": away_elo,
        "channels": channels,
        "match_time_dt": local_dt,
        "match_time_str": local_dt.strftime("%Y-%m-%d %I:%M %p %Z"),
        "live_status": live_status,
        "sort_value": sort_value,
        "tv_assignment": None,
    })

print(f"Total 48-hour matches captured: {len(raw_matches)}")

if not raw_matches:
    print("No matches found in this 48-hour window.")
    sys.exit(0)

# ==========================================
# 3. GLOBAL PRIORITY INTERVAL SCHEDULER
# ==========================================
tv_intervals = {f"TV {n}": [] for n in range(1, NUM_TVS + 1)}

def has_conflict(start, end, booked):
    return any(max(start, b_start) < min(end, b_end) for b_start, b_end in booked)

matches_by_priority = sorted(raw_matches, key=lambda m: m["sort_value"], reverse=True)

for m in matches_by_priority:
    start_dt = m["match_time_dt"]
    end_dt = start_dt + timedelta(minutes=MATCH_DURATION_MINUTES)
    assigned = False

    for tv_name in sorted(tv_intervals.keys()):
        if not has_conflict(start_dt, end_dt, tv_intervals[tv_name]):
            tv_intervals[tv_name].append((start_dt, end_dt))
            m["tv_assignment"] = tv_name
            assigned = True
            break

    if not assigned:
        m["tv_assignment"] = "Other Screen"

# ==========================================
# 4. EXPORT & FORMATTED SUMMARY
# ==========================================
fieldnames = [
    "day_group",
    "match_time_str",
    "live_status",
    "tv_assignment",
    "home_team",
    "away_team",
    "home_elo",
    "away_elo",
    "channels",
    "sort_value",
    "league",
]

matches_chronological = sorted(raw_matches, key=lambda m: (m["match_time_dt"], -m["sort_value"]))

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for m in matches_chronological:
        writer.writerow({k: m[k] for k in fieldnames})

today_label = t_start_local.strftime("%A, %B %d")
tomorrow_label = t_split_local.strftime("%A, %B %d")

# ==========================================
# 4. EXPORT & FORMATTED SUMMARY
# ==========================================
summary_lines = [
    "=" * 108,
    f"{'FULL 48-HOUR MULTI-SCREEN BROADCAST SCHEDULE (3 AM ANCHORED)':^108}",
    "=" * 108,
]

for target_group, group_header in [("TODAY", f"TODAY'S MATCHES ({today_label})"),
                                   ("TOMORROW", f"TOMORROW'S MATCHES ({tomorrow_label})")]:
    group_matches = [m for m in matches_chronological if m["day_group"] == target_group]
    if not group_matches:
        continue

    summary_lines.append("\n" + "#" * 108)
    summary_lines.append(f"###  {group_header}")
    summary_lines.append("#" * 108)

    current_slot = None
    for m in group_matches:
        if m["match_time_str"] != current_slot:
            current_slot = m["match_time_str"]
            summary_lines.append(f"\n--- {current_slot} ---")

        status_tag = f"[{m['live_status']}]"
        tv_tag = f"[{m['tv_assignment']}]"
        comp_tag = format_league_badge(m["league"])
        matchup = f"{m['home_team']} vs {m['away_team']}"
        elos = f"({m['home_elo']} vs {m['away_elo']})"
        score = f"Score: {m['sort_value']}"
        chan = f"[{m['channels']}]"
        
        # Display: [STATUS] [TV] [COMP] Matchup (Elos) Score [Channels]
        summary_lines.append(
            f"  {status_tag:<13} {tv_tag:<16} {comp_tag:<14} {matchup:<33} {elos:<15} {score:<14} {chan}"
        )

output_text = "\n".join(summary_lines)
print("\n" + output_text)

with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write(output_text)

print(f"\nSaved CSV to {OUTPUT_CSV} and summary to {OUTPUT_TXT}")