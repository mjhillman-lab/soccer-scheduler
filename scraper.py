#!/usr/bin/env python3
"""
Soccer Broadcast Scheduler
Fetches fixtures from ESPN, pairs with ClubElo ratings,
and assigns matches to multi-screen setups using priority interval scheduling.
"""

import csv
import io
import json
import os
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
import zoneinfo
import requests

# ==========================================
# CONFIGURATION
# ==========================================
NUM_TVS = 3
MATCH_DURATION_MINUTES = 115
LOCAL_TIMEZONE = zoneinfo.ZoneInfo("America/New_York")
MAX_WORKERS = 16

CACHE_FILE = "elo_cache.json"
CACHE_EXPIRY_HOURS = 12
DEFAULT_ELO = 1450

OUTPUT_CSV = "tv_assignment_schedule.csv"
OUTPUT_TXT = "tv_schedule_summary.txt"
OUTPUT_HTML = "index.html"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

LEAGUES = [
    # Continental (UEFA)
    "uefa.champions", "uefa.europa", "uefa.europa.conf",
    # Domestic Top Flights & 2nd Tiers
    "eng.1", "eng.2",
    "esp.1", "esp.2",
    "ger.1", "ger.2",
    "ita.1", "ita.2",
    "fra.1", "fra.2",
    # Additional European Leagues
    "por.1", "bel.1", "tur.1", "ned.1", "sco.1",
    # Domestic Cups
    "eng.fa", "eng.league_cup",
    "esp.copa_del_rey",
    "ger.dfb_pokal",
    "ita.coppa_italia",
    "fra.coupe_de_france",
]

LEAGUE_DISPLAY_NAMES = {
    "English FA Cup": "FA Cup",
    "English Carabao Cup": "EFL Cup",
    "Spanish Copa del Rey": "Copa del Rey",
    "German DFB-Pokal": "DFB-Pokal",
    "Italian Coppa Italia": "Coppa Italia",
    "French Coupe de France": "Coupe de France",
    "UEFA Champions League": "UCL",
    "UEFA Europa League": "UEL",
    "UEFA Conference League": "UECL",
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

# Manual translation & discrepancy mappings
NAME_ALIASES = {
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
    "union st.-gilloise": "st gillis",
    "union saint-gilloise": "st gillis",
    "royale union saint-gilloise": "st gillis",
    "rusg": "st gillis",
    "sint-truidense": "st truiden",
    "sint-truiden": "st truiden",
    "sporting cp": "sporting",
    "sporting lisbon": "sporting",
    "fc cologne": "köln",
    "cologne": "köln",
    "bayern munich": "bayern",
    "paris saint-germain": "paris sg",
    "stade rennais": "rennes",
    "stade rennais fc": "rennes",
    "inter milan": "inter",
    "internazionale": "inter",
    "ac milan": "milan",
    "olympiacos": "olympiakos",
    "olympiacos fc": "olympiakos",
    "az alkmaar": "az",

    # Top Flight & Continental Namesake Simplifications
    "ajax amsterdam": "ajax",
    "atlético madrid": "atletico",
    "atletico madrid": "atletico",
    "bayer leverkusen": "leverkusen",
    "celta vigo": "celta",
    "sparta prague": "sparta",
    "sk sturm graz": "sturm graz",
    "sturm graz": "sturm graz",

    # English Football League (Championship / League One)
    "coventry city": "coventry",
    "ipswich town": "ipswich",
    "peterborough united": "peterboro",
    "peterborough": "peterboro",

    # Spanish Segunda & Lower
    "deportivo": "deportivo",  # ClubElo lists Deportivo La Coruña as "depor" or "la coruna"
    "deportivo la coruna": "depor",
    "deportivo": "depor",
    "racing santander": "santander",

    # Scottish Premiership
    "heart of midlothian": "hearts",

    # Continental / UEFA Qualifiers / Smaller Leagues
    "jagiellonia bialystok": "jagiellonia",
    "nk celje": "celje",
    "omonia nicosia": "omonia",
    "hapoel be'er": "beer sheva",
}

# ==========================================
# STRING & BADGE UTILITIES
# ==========================================
def strip_accents(s: str) -> str:
    """Removes diacritics and normalizes to lower case."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    ).lower().strip()

def format_league_badge(league_name: str) -> str:
    short = LEAGUE_DISPLAY_NAMES.get(league_name)
    if not short:
        short = league_name.replace("English ", "").replace("Spanish ", "").replace("German ", "")
    return f"[{short}]"

# ==========================================
# 1. CLUB ELO ENGINE (OFFICIAL CSV API)
# ==========================================
class EloEngine:
    def __init__(self, cache_file=CACHE_FILE, expiry_hours=CACHE_EXPIRY_HOURS):
        self.cache_file = cache_file
        self.expiry_hours = expiry_hours
        self.ratings = {}
        self.stripped_ratings = {}
        self._load_or_fetch()

    def _load_or_fetch(self):
        cached = self._load_cache()
        if cached:
            self.ratings = cached
        else:
            self.ratings = self._fetch_clubelo_csv()
            if self.ratings:
                self._save_cache(self.ratings)

        # Build accent-free lookup index
        self.stripped_ratings = {strip_accents(k): v for k, v in self.ratings.items()}

        # Build de-punctuated lookup index (O(1) lookups for hyphens/spaces/apostrophes)
        self.condensed_ratings = {
            strip_accents(k).replace("-", "").replace(" ", "").replace("'", ""): v
            for k, v in self.ratings.items()
        }

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            mtime = datetime.fromtimestamp(os.path.getmtime(self.cache_file), tz=timezone.utc)
            if datetime.now(timezone.utc) - mtime < timedelta(hours=self.expiry_hours):
                try:
                    with open(self.cache_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if len(data) > 100:
                            print(f"Loaded {len(data)} club ratings from local cache.")
                            return data
                except Exception:
                    pass
        return None

    def _save_cache(self, data):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            pass

    def _fetch_clubelo_csv(self):
        """Fetches ClubElo official daily global CSV table."""
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        url = f"http://api.clubelo.com/{today_str}"
        print(f"Fetching ClubElo official daily CSV ({url})...")
        ratings = {}
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code != 200:
                # Fallback to general endpoint if today's date file is building
                r = requests.get("http://api.clubelo.com/today", headers=HEADERS, timeout=12)

            if r.status_code == 200:
                reader = csv.DictReader(io.StringIO(r.text.strip()))
                for row in reader:
                    club = row.get("Club", "").strip()
                    country = row.get("Country", "").strip()
                    elo_str = row.get("Elo", "").strip()

                    if not club or not elo_str:
                        continue

                    try:
                        elo_val = round(float(elo_str))
                    except ValueError:
                        continue

                    norm_name = club.lower()

                    # Disambiguation: prioritize UEFA nations or preserve higher Elo
                    if norm_name not in ratings or country in ("ENG", "ESP", "GER", "ITA", "FRA", "POR", "NED", "BEL"):
                        ratings[norm_name] = elo_val
                    elif elo_val > ratings[norm_name]:
                        ratings[norm_name] = elo_val

                print(f"Ingested and cached {len(ratings)} club Elo ratings from CSV.")
        except Exception as e:
            print(f"Notice: Failed to fetch ClubElo CSV ({e}). Falling back to defaults.")

        return ratings

    def get(self, team_name: str) -> int:
        norm = team_name.lower().strip()

        # 1. Explicit Alias Map
        aliased = NAME_ALIASES.get(norm, norm)
        if aliased in self.ratings:
            return self.ratings[aliased]

        # 2. Direct Match
        if norm in self.ratings:
            return self.ratings[norm]

        # 3. Accent-stripped Match
        clean = strip_accents(aliased)
        if clean in self.stripped_ratings:
            return self.stripped_ratings[clean]

        # 4. Standardized Normalization (strip common prefixes and suffixes)
        noise_words = {
            "fc", "cf", "sc", "afc", "cd", "sk", "nk",
            "city", "town", "united", "wanderers", "rovers", "albion", "athletic"
        }
        tokens = [t for t in clean.replace("-", " ").split() if t not in noise_words]
        normalized = " ".join(tokens)
        if normalized in self.stripped_ratings:
            return self.stripped_ratings[normalized]
        # 5. De-punctuated condensation check (catches hyphenation & apostrophe mismatches)
        condensed = clean.replace("-", "").replace(" ", "").replace("'", "")
        if condensed in self.condensed_ratings:
            return self.condensed_ratings[condensed]

        return DEFAULT_ELO

# ==========================================
# 2. ESPN FIXTURE FETCHER
# ==========================================
def get_48h_window():
    now_local = datetime.now(LOCAL_TIMEZONE)
    today_3am = now_local.replace(hour=3, minute=0, second=0, microsecond=0)
    t_start_local = today_3am if now_local >= today_3am else today_3am - timedelta(days=1)
    t_split_local = t_start_local + timedelta(days=1)
    t_end_local = t_start_local + timedelta(days=2)

    return {
        "start_local": t_start_local,
        "split_local": t_split_local,
        "end_local": t_end_local,
        "start_utc": t_start_local.astimezone(timezone.utc),
        "split_utc": t_split_local.astimezone(timezone.utc),
        "end_utc": t_end_local.astimezone(timezone.utc),
    }

def fetch_espn_fixtures(task, session):
    league_code, d_str = task
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league_code}/scoreboard?dates={d_str}"
    events = []
    try:
        r = session.get(url, headers=HEADERS, timeout=8)
        data = r.json()
        league_name = data.get("leagues", [{}])[0].get("name", league_code)
        for event in data.get("events", []):
            events.append((league_name, event))
    except Exception:
        pass
    return events

def harvest_matches(window, elo_engine):
    # Compute relevant date keys using local calendar days (matches ESPN's scoreboard indexing)
    date_keys = set()
    curr = window["start_local"].date()
    end_date = window["end_local"].date()
    while curr <= end_date:
        date_keys.add(curr.strftime("%Y%m%d"))
        curr += timedelta(days=1)

    tasks = [(league, d_str) for league in LEAGUES for d_str in date_keys]
    print(f"Querying {len(LEAGUES)} leagues across ESPN ({len(tasks)} parallel requests)...")

    # Configure session with connection pool matched to thread count
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    raw_events = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Pass session to each worker thread
        futures = [executor.submit(fetch_espn_fixtures, t, session) for t in tasks]
        for f in as_completed(futures):
            raw_events.extend(f.result())

    session.close()

    now_utc = datetime.now(timezone.utc)
    seen_ids = set()
    matches = []

    for league_name, event in raw_events:
        event_id = event.get("id")
        if not event_id or event_id in seen_ids:
            continue

        date_str = event.get("date")
        utc_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        end_dt_utc = utc_dt + timedelta(minutes=MATCH_DURATION_MINUTES)

        if not (window["start_utc"] <= utc_dt < window["end_utc"]):
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

        day_group = "TODAY" if utc_dt < window["split_utc"] else "TOMORROW"

        # Status determination
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

        home_elo = elo_engine.get(home_team)
        away_elo = elo_engine.get(away_team)
        avg_elo = (home_elo + away_elo) / 2.0
        diff_elo = abs(home_elo - away_elo)
        sort_value = round(avg_elo - diff_elo, 1)

        seen_ids.add(event_id)
        matches.append({
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

    return matches

# ==========================================
# 3. TV ALLOCATION (PRIORITY INTERVAL SCHEDULING)
# ==========================================
def allocate_screens(matches, num_tvs=NUM_TVS):
    tv_intervals = {f"TV {n}": [] for n in range(1, num_tvs + 1)}

    def has_conflict(start, end, booked):
        return any(max(start, b_start) < min(end, b_end) for b_start, b_end in booked)

    # Higher score = priority for screens
    by_priority = sorted(matches, key=lambda m: m["sort_value"], reverse=True)

    for m in by_priority:
        start = m["match_time_dt"]
        end = start + timedelta(minutes=MATCH_DURATION_MINUTES)
        assigned = False

        for tv_name in sorted(tv_intervals.keys()):
            if not has_conflict(start, end, tv_intervals[tv_name]):
                tv_intervals[tv_name].append((start, end))
                m["tv_assignment"] = tv_name
                assigned = True
                break

        if not assigned:
            m["tv_assignment"] = "Other Screen"

# ==========================================
# 4. EXPORTERS (CSV, TXT, HTML)
# ==========================================
def export_csv(matches, filename=OUTPUT_CSV):
    fieldnames = [
        "day_group", "match_time_str", "live_status", "tv_assignment",
        "home_team", "away_team", "home_elo", "away_elo",
        "channels", "sort_value", "league",
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in matches:
            writer.writerow({k: m[k] for k in fieldnames})

def export_text_summary(matches, window, filename=OUTPUT_TXT):
    today_lbl = window["start_local"].strftime("%A, %B %d")
    tmrw_lbl = window["split_local"].strftime("%A, %B %d")

    lines = [
        "=" * 108,
        f"{'FULL 48-HOUR MULTI-SCREEN BROADCAST SCHEDULE (3 AM ANCHORED)':^108}",
        "=" * 108,
    ]

    for group, label in [("TODAY", f"TODAY'S MATCHES ({today_lbl})"),
                         ("TOMORROW", f"TOMORROW'S MATCHES ({tmrw_lbl})")]:
        group_matches = [m for m in matches if m["day_group"] == group]
        if not group_matches:
            continue

        lines.append("\n" + "#" * 108)
        lines.append(f"###  {label}")
        lines.append("#" * 108)

        current_slot = None
        for m in group_matches:
            if m["match_time_str"] != current_slot:
                current_slot = m["match_time_str"]
                lines.append(f"\n--- {current_slot} ---")

            lines.append(
                f"  [{m['live_status']:<9}] [{m['tv_assignment']:<12}] {format_league_badge(m['league']):<14} "
                f"{m['home_team']} vs {m['away_team']:<25} ({m['home_elo']} vs {m['away_elo']}) "
                f"Score: {m['sort_value']:<6} [{m['channels']}]"
            )

    output = "\n".join(lines)
    print("\n" + output)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(output)

def export_mobile_html(matches, window, filename=OUTPUT_HTML):
    today_lbl = window["start_local"].strftime("%A, %B %d")
    tmrw_lbl = window["split_local"].strftime("%A, %B %d")

    # Generate the local update timestamp
    updated_at_str = datetime.now(LOCAL_TIMEZONE).strftime("%a, %b %d at %I:%M %p %Z")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Soccer Broadcast Board</title>
  
  <!-- Mobile & PWA Theme Metadata -->
  <meta name="theme-color" content="#121212">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">

  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background-color: #121212;
      color: #e0e0e0;
      margin: 0;
      padding: 12px;
    }}
    h1 {{ font-size: 1.25rem; text-align: center; color: #4dabf7; margin-bottom: 2px; }}
    .subtitle {{ font-size: 0.75rem; text-align: center; color: #888; margin-bottom: 4px; }}
    .updated-at {{ font-size: 0.68rem; text-align: center; color: #666; margin-bottom: 16px; }}
    .day-header {{
      background: #1e1e1e;
      border-left: 4px solid #4dabf7;
      padding: 8px 12px;
      font-size: 0.95rem;
      font-weight: bold;
      margin-top: 16px;
      border-radius: 2px;
    }}
    .slot-header {{
      font-size: 0.8rem;
      color: #aaa;
      margin: 12px 0 6px 4px;
      font-weight: 600;
    }}
    .card {{
      background: #1a1a1a;
      border: 1px solid #2a2a2a;
      border-radius: 6px;
      padding: 10px;
      margin-bottom: 8px;
    }}
    .card-top {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 6px;
    }}
    .badge {{
      font-size: 0.7rem;
      font-weight: 700;
      padding: 2px 6px;
      border-radius: 4px;
      text-transform: uppercase;
    }}
    .tv-tv1 {{ background: #2b8a3e; color: #fff; }}
    .tv-tv2 {{ background: #1971c2; color: #fff; }}
    .tv-tv3 {{ background: #e67700; color: #fff; }}
    .tv-other {{ background: #343a40; color: #adb5bd; }}
    .comp {{ font-size: 0.75rem; font-weight: 600; color: #ced4da; }}
    .matchup {{ font-size: 0.95rem; font-weight: bold; margin: 4px 0; }}
    .meta {{ font-size: 0.75rem; color: #888; display: flex; justify-content: space-between; }}
    .channel {{ color: #ffd43b; font-weight: 600; }}
  </style>
</head>
<body>
  <h1>Soccer Broadcast Board</h1>
  <div class="subtitle">48-Hour Multi-Screen Schedule (Anchored 3 AM)</div>
  <div class="updated-at">Updated: {updated_at_str}</div>
"""
    for group, label in [("TODAY", f"TODAY'S MATCHES ({today_lbl})"),
                         ("TOMORROW", f"TOMORROW'S MATCHES ({tmrw_lbl})")]:
        group_matches = [m for m in matches if m["day_group"] == group]
        if not group_matches:
            continue

        html += f'<div class="day-header">{label}</div>\n'
        current_slot = None
        for m in group_matches:
            if m["match_time_str"] != current_slot:
                current_slot = m["match_time_str"]
                html += f'<div class="slot-header">{current_slot}</div>\n'

            tv_class = "tv-other"
            if "TV 1" in m["tv_assignment"]: tv_class = "tv-tv1"
            elif "TV 2" in m["tv_assignment"]: tv_class = "tv-tv2"
            elif "TV 3" in m["tv_assignment"]: tv_class = "tv-tv3"

            html += f"""
        <div class="card">
          <div class="card-top">
            <span class="badge {tv_class}">{m['tv_assignment']}</span>
            <span class="comp">{format_league_badge(m['league'])}</span>
          </div>
          <div class="matchup">{m['home_team']} vs {m['away_team']}</div>
          <div class="meta">
            <span>Elos: {m['home_elo']} vs {m['away_elo']} (Score: {m['sort_value']})</span>
            <span class="channel">{m['channels']}</span>
          </div>
        </div>
        """
    html += "</body></html>"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    window = get_48h_window()
    print(f"Window: {window['start_local'].strftime('%a %b %d, %I:%M %p')} to {window['end_local'].strftime('%a %b %d, %I:%M %p %Z')}")

    elo_engine = EloEngine()
    matches = harvest_matches(window, elo_engine)
    print(f"Total 48-hour matches captured: {len(matches)}")

    if not matches:
        print("No matches found in this 48-hour window.")
        sys.exit(0)

    allocate_screens(matches)

    chronological_matches = sorted(matches, key=lambda m: (m["match_time_dt"], -m["sort_value"]))

    export_csv(chronological_matches)
    export_text_summary(chronological_matches, window)
    export_mobile_html(chronological_matches, window)
    print(f"\nArtifacts generated: {OUTPUT_CSV}, {OUTPUT_TXT}, {OUTPUT_HTML}")

if __name__ == "__main__":
    main()