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
import urllib.parse
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter, Retry
import zoneinfo
import requests

# ==========================================
# CONFIGURATION
# ==========================================
NUM_TVS = 3
MATCH_DURATION_MINUTES = 115
MIN_LOOKIN_MINUTES = 30
LOCAL_TIMEZONE = zoneinfo.ZoneInfo("America/New_York")
MAX_WORKERS = 16

CACHE_FILE = "elo_cache.json"
CACHE_EXPIRY_HOURS = 12
DEFAULT_ELO = 1450

OUTPUT_CSV = "tv_assignment_schedule.csv"
OUTPUT_TXT = "tv_schedule_summary.txt"
OUTPUT_HTML = "index.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.espn.com/soccer/",
    "Origin": "https://www.espn.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

LEAGUES = [
    # Continental (UEFA)
    "uefa.champions", "uefa.europa", "uefa.europa.conf",
    # Domestic Top Flights & 2nd Tiers
    "eng.1", "eng.2",
    "esp.1", "esp.2",
    "ger.1", "ger.2",
    "ita.1",
    "fra.1",
    # Additional European Leagues
    "por.1", "bel.1", "tur.1", "ned.1",
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
    "Spanish LALIGA": "LaLiga",
    "Spanish Segunda División": "LaLiga 2",
    "German Bundesliga": "Bundesliga",
    "German 2. Bundesliga": "2. Bundesliga",
    "Italian Serie A": "Serie A",
    "French Ligue 1": "Ligue 1",
    "Dutch Eredivisie": "Eredivisie",
    "Portuguese Primeira Liga": "Liga Portugal",
    "Belgian Pro League": "Belgian Pro",
    "Turkish Super Lig": "Süper Lig",
}

# Manual translation & discrepancy mappings
NAME_ALIASES = {
    # Premier League
    "manchester city": "man city",
    "manchester united": "man united",
    "nottingham forest": "forest",
    "tottenham hotspur": "tottenham",
    "afc bournemouth": "bournemouth",
    "brighton & hove albion": "brighton",
    "wolverhampton wanderers": "wolves",
    "newcastle united": "newcastle",
    "west ham united": "west ham",

    # English Football League (Championship / League One)
    "coventry city": "coventry",
    "ipswich town": "ipswich",
    "peterborough united": "peterboro",
    "peterborough": "peterboro",

    # Spain (La Liga & Segunda)
    "athletic bilbao": "athletic",
    "athletic club": "athletic",
    "atlético madrid": "atletico",
    "atletico madrid": "atletico",
    "celta vigo": "celta",
    "real betis": "betis",
    "real betis balompie": "betis",
    "deportivo la coruna": "depor",
    "deportivo": "depor",
    "racing santander": "santander",

    # Germany (Bundesliga & 2. Bundesliga)
    "fc cologne": "köln",
    "cologne": "köln",
    "bayern munich": "bayern münchen",
    "fc bayern munich": "bayern münchen",
    "bayern munchen": "bayern münchen",
    "fc bayern munchen": "bayern münchen",
    "bayern": "bayern münchen",
    "bayer leverkusen": "leverkusen",
    "tsg hoffenheim": "hoffenheim",
    "1899 hoffenheim": "hoffenheim",
    "spvgg greuther fürth": "fürth",
    "spvgg greuther furth": "fürth",
    "greuther fürth": "fürth",
    "greuther furth": "fürth",
    "1. fc magdeburg": "magdeburg",
    "magdeburg": "magdeburg",
    "vfl wolfsburg": "wolfsburg",
    "sv darmstadt 98": "darmstadt",
    "darmstadt 98": "darmstadt",
    "darmstadt": "darmstadt",
    "1. fc union berlin": "union berlin",
    "union berlin": "union berlin",

    # Italy (Serie A)
    "inter milan": "inter",
    "internazionale": "inter",
    "ac milan": "milan",
    "as roma" : "roma",

    # France (Ligue 1 & Ligue 2)
    "paris saint-germain": "paris sg",
    "stade rennais": "rennes",
    "stade rennais fc": "rennes",
    "rodez aveyron": "rodez",
    "rodez af": "rodez",
    "as nancy lorraine": "nancy",
    "as nancy-lorraine": "nancy",
    "nancy": "nancy",
    "clermont foot": "clermont",
    "clermont foot 63": "clermont",
    "stade laval": "laval",
    "stade lavallois": "laval",
    "dijon fco": "dijon",
    "stade de reims": "reims",
    "stade reims": "reims",
    "as monaco": "monaco",

    # Netherlands & Belgium
    "ajax amsterdam": "ajax",
    "az alkmaar": "az",
    "nec nijmegen": "nijmegen",
    "n.e.c.": "nijmegen",
    "nec": "nijmegen",
    "union st.-gilloise": "st gillis",
    "union saint-gilloise": "st gillis",
    "royale union saint-gilloise": "st gillis",
    "rusg": "st gillis",
    "sint-truidense": "st truiden",
    "sint-truiden": "st truiden",
    "fc groningen": "groningen",
    "kaa gent": "gent",
    "standard liege": "standard",
    "standard de liege": "standard",

    # Portugal
    "sporting cp": "sporting",
    "sporting lisbon": "sporting",
    "cs maritimo": "maritimo",
    "marítimo": "maritimo",

    # Belgium (Pro League)
    "raal la louvière": "la louviere",
    "raal la louviere": "la louviere",
    
    # Europe
    "heart of midlothian": "hearts",
    "lillestrom": "lillestrøm",
    "lillestrøm": "lillestrøm",
    "lillestrom sk": "lillestrøm",
    "lillestrøm sk": "lillestrøm",
    "lech poznan": "lech",
    "lech": "lech",
    "kks lech poznan": "lech",
    "jagiellonia bialystok": "jagiellonia",
    "sparta prague": "sparta",
    "sk sturm graz": "sturm graz",
    "sturm graz": "sturm graz",
    "olympiacos": "olympiakos",
    "olympiacos fc": "olympiakos",
    "ofi crete": "ofi",
    "ofi": "ofi",
    "levski sofia": "levski",
    "pfc levski sofia": "levski",
    "nk celje": "celje",
    "omonia nicosia": "omonia",
    "hapoel be'er": "beer sheva",

    # Turkey (Süper Lig)
    "istanbul basaksehir": "basaksehir",
    "rams basaksehir": "basaksehir",
    "istanbul başakşehir": "basaksehir",
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
        # 1. Try fetching fresh ClubElo CSV
        self.ratings = self._fetch_clubelo_csv()
        
        # 2. If fetch succeeded, update disk cache
        if self.ratings and len(self.ratings) > 100:
            self._save_cache(self.ratings)
        else:
            # 3. If fetch failed (502, 403, offline), LOAD DISK CACHE REGARDLESS OF AGE
            print("Notice: Online fetch yielded no data. Loading disk cache fallback...")
            self.ratings = self._load_cache()

        # Build accent-free lookup index
        self.stripped_ratings = {strip_accents(k): v for k, v in self.ratings.items()}

        # Build de-punctuated lookup index
        self.condensed_ratings = {
            strip_accents(k).replace("-", "").replace(" ", "").replace("'", ""): v
            for k, v in self.ratings.items()
        }

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) > 100:
                        print(f"Loaded {len(data)} club ratings from local cache.")
                        return data
            except Exception as e:
                print(f"Failed to read cache file: {e}")
        return {}

    def _save_cache(self, data):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            pass

    def _fetch_clubelo_csv(self):
        """Fetches ClubElo official daily CSV table with session retries and updates disk cache."""
        now_dt = datetime.now(timezone.utc)
        today_str = now_dt.strftime("%Y-%m-%d")
        yesterday_str = (now_dt - timedelta(days=1)).strftime("%Y-%m-%d")

        session = requests.Session()
        retries = requests.adapters.Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False,
        )
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
        session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))

        clubelo_headers = {
            "User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 14542.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        ratings = {}

        endpoints = [
            f"http://api.clubelo.com/{today_str}",
            "http://api.clubelo.com/today",
            f"http://api.clubelo.com/{yesterday_str}",
        ]

        for url in endpoints:
            try:
                print(f"Fetching ClubElo daily CSV ({url})...")
                r = session.get(url, headers=clubelo_headers, timeout=12)
                if r.status_code == 200 and len(r.text.strip()) > 500:
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

                        if norm_name not in ratings or country in ("ENG", "ESP", "GER", "ITA", "FRA", "POR", "NED", "BEL"):
                            ratings[norm_name] = elo_val
                        elif elo_val > ratings[norm_name]:
                            ratings[norm_name] = elo_val

                    if len(ratings) > 100:
                        print(f"Successfully ingested {len(ratings)} club Elo ratings from {url}. Updating disk cache.")
                        try:
                            import json
                            with open("elo_cache.json", "w", encoding="utf-8") as f:
                                json.dump(ratings, f, indent=2)
                        except Exception as ce:
                            print(f"Notice: Failed to write updated cache to disk: {ce}")
                        return ratings
                else:
                    print(f"Notice: {url} returned HTTP {r.status_code} (length: {len(r.text.strip())}). Trying next...")
            except Exception as e:
                print(f"Notice: Endpoint {url} failed ({repr(e)}). Trying next...")

        print("Notice: All ClubElo remote endpoints failed. Falling back to disk cache.")
        return ratings

    def get(self, team_name: str) -> int:
        norm = team_name.lower().strip()

        # 1. Check explicit alias mapping
        aliased = NAME_ALIASES.get(norm, norm).lower().strip()
        if aliased in self.ratings:
            return self.ratings[aliased]

        # 2. Direct match
        if norm in self.ratings:
            return self.ratings[norm]

        # 3. Accent-stripped match
        clean_alias = strip_accents(aliased)
        if clean_alias in self.stripped_ratings:
            return self.stripped_ratings[clean_alias]

        clean_norm = strip_accents(norm)
        if clean_norm in self.stripped_ratings:
            return self.stripped_ratings[clean_norm]

        # 4. Standardized noise word stripping
        noise_words = {
            "fc", "cf", "sc", "afc", "cd", "sk", "nk", "vfl", "sv", "spvgg",
            "city", "town", "united", "wanderers", "rovers", "albion", "athletic"
        }
        tokens = [t for t in clean_alias.replace("-", " ").split() if t not in noise_words]
        normalized = " ".join(tokens)
        if normalized in self.stripped_ratings:
            return self.stripped_ratings[normalized]

        # 5. Condensation check
        condensed = clean_alias.replace("-", "").replace(" ", "").replace("'", "")
        if condensed in self.condensed_ratings:
            return self.condensed_ratings[condensed]

        # 6. Automatic substring match against cache keys
        for key, val in self.ratings.items():
            if len(key) >= 5 and (key in norm or key in clean_norm):
                return val

        return DEFAULT_ELO

# ==========================================
# 2. ESPN FIXTURE FETCHER
# ==========================================

def get_48h_window():
    now_local = datetime.now(LOCAL_TIMEZONE)
    today_3am = now_local.replace(hour=3, minute=0, second=0, microsecond=0)
    t_start_local = today_3am if now_local >= today_3am else today_3am - timedelta(days=1)
    t_end_local = t_start_local + timedelta(hours=48)
    t_split_local = t_start_local + timedelta(hours=24)

    return {
        "start_local": t_start_local,
        "end_local": t_end_local,
        "split_local": t_split_local,
        "start_utc": t_start_local.astimezone(timezone.utc),
        "end_utc": t_end_local.astimezone(timezone.utc),
        "split_utc": t_split_local.astimezone(timezone.utc),
    }

def fetch_espn_fixtures(task):
    league_code, d_str = task
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/soccer/{league_code}/scoreboard?dates={d_str}"
    events = []

    # Configure a thread-local session with 2 retries and exponential backoff
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        # Bumped timeout from 10 to 15 seconds
        r = session.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"[WARN] {league_code} {d_str} returned HTTP {r.status_code}")
            return events
        data = r.json()
        league_name = data.get("leagues", [{}])[0].get("name", league_code)
        for event in data.get("events", []):
            events.append((league_name, event))
    except Exception as e:
        print(f"[ERR] {league_code} {d_str}: {e}")
    finally:
        session.close()

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

    raw_events = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_espn_fixtures, t) for t in tasks]
        for f in as_completed(futures):
            raw_events.extend(f.result())

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

        home_cand = [c for c in competitors if c.get("homeAway") == "home"]
        away_cand = [c for c in competitors if c.get("homeAway") == "away"]
        home = home_cand[0] if home_cand else competitors[0]
        away = away_cand[0] if away_cand else (competitors[1] if len(competitors) > 1 else competitors[0])
        if home.get("id") == away.get("id") and len(competitors) > 1:
            away = competitors[1]

        home_team = home.get("team", {}).get("displayName", "Home")
        away_team = away.get("team", {}).get("displayName", "Away")
        local_dt = utc_dt.astimezone(LOCAL_TIMEZONE)

        day_group = "TODAY" if utc_dt < window["split_utc"] else "TOMORROW"

        # Status determination
        status_obj = event.get("status", {})
        status_type_obj = status_obj.get("type", {})
        state = status_type_obj.get("state", "").lower()          # 'pre', 'in', 'post'
        type_name = status_type_obj.get("name", "").upper()        # 'STATUS_IN_PROGRESS', 'STATUS_HALFTIME', 'STATUS_FINAL'
        display_clock = status_obj.get("displayClock", "")

        if state == "post" or "FINAL" in type_name or now_utc >= end_dt_utc:
            live_status = "FINAL"
        elif state == "in" or "IN_PROGRESS" in type_name or "HALFTIME" in type_name or (utc_dt <= now_utc < end_dt_utc):
            if "HALFTIME" in type_name or display_clock == "HT":
                live_status = "LIVE HT"
            elif display_clock:
                live_status = f"LIVE {display_clock}"
            else:
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
        
        # --- Contract-based fallback when ESPN doesn't list a channel ---
        if channels == "Check listings":
            league_code = league_name.lower()
            exclusive_defaults = {
                # Belgium & Portugal (DAZN US)
                "bel.1": "DAZN",
                "belgian": "DAZN",
                "por.1": "DAZN",
                "primeira liga": "DAZN",
                # France
                "fra.1": "beIN SPORTS",
                "ligue 1": "beIN SPORTS",
                # Turkey
                "tur.1": "beIN SPORTS",
                "super lig": "beIN SPORTS",
                "süper lig": "beIN SPORTS",
                # Spain
                "esp.2": "ESPN+",
                "segunda": "ESPN+",
                "hypermotion": "ESPN+",
                # Netherlands
                "ned.1": "ESPN+",
                "eredivisie": "ESPN+",
                # Italy
                "ita.coppa_italia": "Paramount+",
                "coppa italia": "Paramount+",
                # Note: Championship, 2. Bundesliga, etc. intentionally omitted to remain "Check listings"
            }
            for code, default_net in exclusive_defaults.items():
                if code in league_code:
                    channels = default_net
                    break
                
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
            "sort_value": sort_value,
            "tv_assignment": None,
        })

    return matches

# ==========================================
# 3. TV ALLOCATION (PRIORITY INTERVAL SCHEDULING + LOOK-INS)
# ==========================================
def allocate_screens(matches, num_tvs=NUM_TVS):
    # screen_bookings: { 'TV 1': [(start_dt, end_dt, match_dict)], ... }
    screen_bookings = {f"TV {n}": [] for n in range(1, num_tvs + 1)}

    def has_any_conflict(start, end, booked):
        return any(max(start, b[0]) < min(end, b[1]) for b in booked)

    confirmed_linear_nets = ["usa", "nbc", "cbs sports network", "cbssn", "fs1", "fs2"]
    streaming_only_leagues = [
        "por.1", "primeira liga",
        "bel.1", "belgian",
        "tur.1", "super lig", "süper lig"
    ]

    allocatable_matches = []
    for m in matches:
        ch = m.get("channels", "").lower()
        lg = m.get("league", "").lower()

        # 1. Untelevised matches cannot take physical screens
        if ch == "check listings":
            m["tv_assignment"] = "Other Screen"
            m["is_lookin"] = False
            m["lookin_window"] = ""
            continue

        # 2. Suppress Portugal, Belgium, and Turkey unless on confirmed linear cable
        is_streaming_league = any(sub in lg for sub in streaming_only_leagues)
        is_linear_tv = any(net in ch for net in confirmed_linear_nets)

        if is_streaming_league and not is_linear_tv:
            m["tv_assignment"] = "Other Screen"
            m["is_lookin"] = False
            m["lookin_window"] = ""
            continue

        allocatable_matches.append(m)

    # Higher score = priority for screens
    by_priority = sorted(allocatable_matches, key=lambda m: m["sort_value"], reverse=True)

    # -------------------------------------------------------------
    # Pass 1: Primary Full-Match Assignments
    # -------------------------------------------------------------
    unassigned = []
    for m in by_priority:
        start = m["match_time_dt"]
        end = start + timedelta(minutes=MATCH_DURATION_MINUTES)
        assigned = False

        for tv in sorted(screen_bookings.keys(), key=lambda x: int(x.split()[1])):
            if not has_any_conflict(start, end, screen_bookings[tv]):
                screen_bookings[tv].append((start, end, m))
                m["tv_assignment"] = tv
                m["is_lookin"] = False
                m["lookin_window"] = ""
                assigned = True
                break

        if not assigned:
            unassigned.append(m)

    # -------------------------------------------------------------
    # Pass 2: Look-in Fillers (minimum 30-minute idle window)
    # -------------------------------------------------------------
    for m in unassigned:
        m_start = m["match_time_dt"]
        m_end = m_start + timedelta(minutes=MATCH_DURATION_MINUTES)
        best_tv = None
        best_window = None

        for tv in sorted(screen_bookings.keys(), key=lambda x: int(x.split()[1])):
            bookings = sorted(screen_bookings[tv], key=lambda b: b[0])

            cur_time = m_start
            idle_windows = []

            for b_start, b_end, _ in bookings:
                if b_start > cur_time:
                    gap_start = cur_time
                    gap_end = min(m_end, b_start)
                    if gap_start < gap_end:
                        idle_windows.append((gap_start, gap_end))
                cur_time = max(cur_time, b_end)

            if cur_time < m_end:
                idle_windows.append((cur_time, m_end))

            for gap_start, gap_end in idle_windows:
                duration = int((gap_end - gap_start).total_seconds() / 60)
                if duration >= MIN_LOOKIN_MINUTES:
                    best_tv = tv
                    best_window = (gap_start, gap_end, duration)
                    break
            if best_tv:
                break

        if best_tv:
            gap_start, gap_end, duration = best_window
            screen_bookings[best_tv].append((gap_start, gap_end, m))
            m["tv_assignment"] = f"{best_tv} (Look-in)"
            m["is_lookin"] = True
            m["lookin_window"] = f"{gap_start.strftime('%I:%M %p')}-{gap_end.strftime('%I:%M %p')} (~{duration}m)"
        else:
            m["tv_assignment"] = "Other Screen"
            m["is_lookin"] = False
            m["lookin_window"] = ""

# ==========================================
# 4. EXPORTERS (CSV, TXT, HTML)
# ==========================================
def export_csv(matches, filename=OUTPUT_CSV):
    fieldnames = [
        "day_group", "match_time_str", "tv_assignment",
        "lookin_window", "home_team", "away_team", "home_elo", "away_elo",
        "channels", "sort_value", "league",
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in matches:
            writer.writerow({k: m.get(k, "") for k in fieldnames})

def export_text_summary(matches, window, filename=OUTPUT_TXT):
    today_lbl = window["start_local"].strftime("%A, %B %d")
    tmrw_lbl = window["split_local"].strftime("%A, %B %d")

    lines = [
        "=" * 116,
        f"{'FULL 48-HOUR MULTI-SCREEN BROADCAST SCHEDULE (3 AM ANCHORED)':^116}",
        "=" * 116,
    ]

    for group, label in [("TODAY", f"TODAY'S MATCHES ({today_lbl})"),
                         ("TOMORROW", f"TOMORROW'S MATCHES ({tmrw_lbl})")]:
        group_matches = [m for m in matches if m["day_group"] == group]
        if not group_matches:
            continue

        lines.append("\n" + "#" * 116)
        lines.append(f"###  {label}")
        lines.append("#" * 116)

        current_slot = None
        for m in group_matches:
            if m["match_time_str"] != current_slot:
                current_slot = m["match_time_str"]
                lines.append(f"\n--- {current_slot} ---")

            # --- DEFINE TV_LABEL HERE ---
            tv_label = m.get("tv_assignment") or "Other Screen"
            if m.get("is_lookin") and m.get("lookin_window"):
                tv_label = f"{m['tv_assignment']} [{m['lookin_window']}]"

            lines.append(
                f"  [{tv_label:<24}] {format_league_badge(m['league']):<14} "
                f"{m['home_team']} vs {m['away_team']:<25} ({m['home_elo']} vs {m['away_elo']}) "
                f"Score: {m['sort_value']:<6} [{m['channels']}]"
            )

    output = "\n".join(lines)
    print("\n" + output)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(output)

def resolve_stream_url(channels: str, league: str, home: str = "", away: str = "") -> str:
    """Returns a direct streaming hub or YouTube TV search URL based on channel, league, and matchup."""
    c = channels.lower()
    l = league.lower()

    # Normalize team names to clean ASCII to prevent mojibake in URL search parameters
    clean_h = strip_accents(home).title() if home else ""
    clean_a = strip_accents(away).title() if away else ""
    query = f"{clean_h} vs {clean_a}".strip() if (clean_h and clean_a) else (clean_h or clean_a).strip()
    yt_search_url = f"https://tv.youtube.com/search?q={urllib.parse.quote(query)}"

    # 1. PRIORITY LINEAR CABLE GATE
    # Strictly confirmed domestic TV feeds (catches USA, NBC, CBSSN, FS1/2 before league streaming hubs)
    confirmed_linear_nets = ["usa", "nbc", "cbs sports network", "cbssn", "fs1", "fs2"]
    if any(net in c for net in confirmed_linear_nets):
        return yt_search_url

    # 2. DAZN (Belgium & Portugal Hubs)
    if "bel.1" in l or "belgian" in l:
        return "https://www.dazn.com/en-US/competition/Competition:4zwgbb66rif2spcoeeol2motx"
    if "por.1" in l or "primeira liga" in l or "portuguese" in l:
        return "https://www.dazn.com/en-US/competition/Competition:8yi6ejjd1zudcqtbn07haahg6"
    if "dazn" in c:
        return "https://www.dazn.com/en-US/sports"

    # 3. German Bundesliga (Top Flight Overflow to Fandango at Home)
    if ("ger.1" in l or "bundesliga" in l) and "2." not in l and "ger.2" not in l:
        return "https://athome.fandango.com/content/browse/uxrow/Live-Upcoming-Bundesliga-Matches/27372"

    # 4. Turkish Süper Lig (Dedicated 24/7 beIN Hub)
    if "super lig" in l or "süper lig" in l or "tur.1" in l:
        return "https://watch.beinsports-apps.com/events/bein-turkish-superlig-24-7-2"

    # 5. beIN SPORTS Linear (Ligue 1 / explicit cable slot via YouTube TV)
    if "bein" in c:
        return yt_search_url

    # 6. Peacock League Hubs
    if "peacock" in c:
        if "premier" in l or "eng.1" in l:
            return "https://www.peacocktv.com/sports/premier-league"
        return "https://www.peacocktv.com/sports"

    # 7. Paramount+ Competitions (Carried matches only)
    if "paramount" in c or "cbs" in c:
        if "carabao" in l or "efl cup" in l or "eng.league_cup" in l:
            return "https://www.paramountplus.com/shows/efl-cup/"
        if any(k in l for k in ["championship", "eng.2"]):
            return "https://www.paramountplus.com/shows/english-football-league/"
        if "champions league" in l or "ucl" in l or "uefa.champions" in l:
            return "https://www.paramountplus.com/shows/uefa-champions-league/"
        if "europa" in l or "uel" in l or "uecl" in l or "uefa.europa" in l:
            return "https://www.paramountplus.com/shows/uefa-europa-league/"
        if "serie a" in l or "ita.1" in l:
            return "https://www.paramountplus.com/shows/serie-a/"
        return "https://www.paramountplus.com/sports"

    # 8. ESPN+ Competitions
    if "espn+" in c:
        if any(k in l for k in ["esp.2", "segunda", "hypermotion", "laliga 2"]):
            return "https://www.espn.com/watch/catalog/20a6bdc8-ff1f-350a-9e78-a771d12ce59a/spanish-laliga-2"
        if "laliga" in l or "esp.1" in l or "spanish" in l:
            return "https://www.espn.com/watch/catalog/cf7b0c51-7c48-3e9a-8abb-0c01b1a973a0/spanish-laliga"
        if "eredivisie" in l or "ned.1" in l or "dutch" in l:
            return "https://www.espn.com/watch/catalog/e8c15234-75dd-3155-bea5-3a1d9edcfd27/dutch-eredivisie"
        if "fa cup" in l or "eng.fa" in l:
            return "https://www.espn.com/watch/catalog/5a560c57-ca5e-3ee8-8f81-a9686ae2c00e/the-fa-cup"
        return "https://www.espn.com/watch/espnplus/soccer"

    # 9. FINAL SAFETY NET (Unconfirmed rights / "Check listings" -> YouTube TV Search)
    if "check listings" in c:
        return yt_search_url

    return ""

def export_mobile_html(matches, window, filename=OUTPUT_HTML):
    today_lbl = window["start_local"].strftime("%A, %B %d")
    tmrw_lbl = window["split_local"].strftime("%A, %B %d")
    updated_at_str = datetime.now(LOCAL_TIMEZONE).strftime("%a, %b %d at %I:%M %p %Z")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Soccer Broadcast Board</title>
  
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
    .status-badge {{
      font-size: 0.68rem;
      font-weight: 700;
      padding: 2px 6px;
      border-radius: 4px;
      text-transform: uppercase;
      margin-left: 6px;
    }}
    .status-live {{ background: #c92a2a; color: #fff; animation: pulse 2s infinite; }}
    .status-final {{ background: #495057; color: #ced4da; }}
    .status-upcoming {{ background: #212529; color: #868e96; border: 1px solid #343a40; }}
    @keyframes pulse {{
      0% {{ opacity: 1; }}
      50% {{ opacity: 0.6; }}
      100% {{ opacity: 1; }}
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
    .badge-container {{
      display: flex;
      align-items: center;
      gap: 6px;
    }}
    .badge {{
      font-size: 0.7rem;
      font-weight: 700;
      padding: 2px 6px;
      border-radius: 4px;
      text-transform: uppercase;
    }}
    .tv-tv1 {{ background: #2b8a3e; color: #fff; }} /* Green */
    .tv-tv2 {{ background: #1971c2; color: #fff; }} /* Blue */
    .tv-tv3 {{ background: #e67700; color: #fff; }} /* Orange */
    .tv-tv4 {{ background: #ae3ec9; color: #fff; }} /* Grape / Purple */
    .tv-tv5 {{ background: #0ca678; color: #fff; }} /* Teal */
    .tv-tv6 {{ background: #d6336c; color: #fff; }} /* Rose / Pink */
    .tv-other {{ background: #343a40; color: #adb5bd; }} /* Slate Gray */
    .badge-lookin {{
      background: #495057;
      color: #74c0fc;
      border: 1px solid #74c0fc;
      font-size: 0.68rem;
      font-weight: 600;
      padding: 1px 5px;
      border-radius: 3px;
    }}
    .comp {{ font-size: 0.75rem; font-weight: 600; color: #ced4da; }}
    .matchup {{ font-size: 0.95rem; font-weight: bold; margin: 4px 0; }}
    .meta {{ font-size: 0.75rem; color: #888; display: flex; justify-content: space-between; }}
    .channel {{
      color: #ffd43b;
      font-weight: 600;
      text-decoration: none;
    }}
    a.channel:hover {{
      text-decoration: underline;
    }}
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
                iso_time = m["match_time_dt"].isoformat()
                html += f'<div class="slot-header" data-time="{iso_time}">{current_slot}</div>\n'

            # 1. Determine clean badge label (e.g., 'TV 1', 'TV 4', or 'Other Screen')
            raw_assignment = m.get("tv_assignment", "Other Screen")
            if "TV " in raw_assignment:
                parts = raw_assignment.split()
                tv_num = parts[1].replace("(Look-in)", "").strip()
                base_tv = f"TV {tv_num}"
                tv_class = f"tv-tv{tv_num}"
            else:
                base_tv = "Other Screen"
                tv_class = "tv-other"

            lookin_pill = ""
            if m.get("is_lookin") and m.get("lookin_window"):
                lookin_pill = f'<span class="badge-lookin">Look-in: {m["lookin_window"]}</span>'

            # 1. Match status indicator
            status_str = m.get("live_status", "UPCOMING")
            if "LIVE" in status_str:
                status_class = "status-live"
            elif "FINAL" in status_str:
                status_class = "status-final"
            else:
                status_class = "status-upcoming"

            # 2. Clickable tap-to-stream link
            stream_url = resolve_stream_url(m["channels"], m["league"], m["home_team"], m["away_team"])
            if stream_url:
                channel_html = f'<a href="{stream_url}" target="_blank" rel="noopener noreferrer" class="channel">{m["channels"]} &#8599;</a>'
            else:
                channel_html = f'<span class="channel">{m["channels"]}</span>'

            html += f"""
        <div class="card">
          <div class="card-top">
            <div class="badge-container">
              <span class="badge {tv_class}">{base_tv}</span>
              {lookin_pill}
            </div>
            <span class="comp">{format_league_badge(m['league'])}</span>
          </div>
          <div class="matchup">{m['home_team']} vs {m['away_team']}</div>
          <div class="meta">
            <span>Elos: {m['home_elo']} vs {m['away_elo']} (Score: {m['sort_value']})</span>
            {channel_html}
          </div>
        </div>
        """
    html += """
  <script>
    document.addEventListener("DOMContentLoaded", () => {
      const now = new Date().getTime();
      const lookbackMs = 120 * 60 * 1000; // 2-hour window catches ongoing matches
      const targetTime = now - lookbackMs;

      const headers = Array.from(document.querySelectorAll('.slot-header[data-time]'));
      let anchorTarget = null;

      for (const el of headers) {
        const slotTime = new Date(el.dataset.time).getTime();
        if (slotTime >= targetTime) {
          anchorTarget = el;
          break;
        }
      }

      if (!anchorTarget && headers.length > 0) {
        anchorTarget = headers[headers.length - 1];
      }

      if (anchorTarget) {
        anchorTarget.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    });
  </script>
</body>
</html>
"""
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