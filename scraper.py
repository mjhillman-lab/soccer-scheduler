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
import re
import sys
import urllib.parse
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter, Retry
import zoneinfo

# Third-party imports
import requests
from requests.adapters import HTTPAdapter, Retry
from curl_cffi import requests as cffi_requests

# ==========================================
# CONFIGURATION
# ==========================================
NUM_TVS = 3
MATCH_DURATION_MINUTES = 115
MIN_LOOKIN_MINUTES = 30
LOCAL_TIMEZONE = zoneinfo.ZoneInfo("America/New_York")
MAX_WORKERS = 16

CACHE_FILE = "elo_cache.json"
DEFAULT_ELO = 0  # Changed from 1450 to act as a missing-data sentinel

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
    "newcastle united": "newcastle",
    "brighton & hove albion": "brighton",
    "wolverhampton wanderers": "wolves",

    # English Football League (Championship / League One)
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
    "ceuta": "ca ceuta",
    "ad ceuta": "ca ceuta",
    "ad ceuta fc": "ca ceuta",

    # Germany (Bundesliga & 2. Bundesliga)
    "cologne": "köln",
    "bayern munich": "bayern münchen",
    "bayern munchen": "bayern münchen",
    "bayern": "bayern münchen",
    "bayer leverkusen": "leverkusen",
    "tsg hoffenheim": "hoffenheim",
    "1899 hoffenheim": "hoffenheim",
    "greuther fürth": "fürth",
    "greuther furth": "fürth",
    "darmstadt 98": "darmstadt",
    "st. pauli": "st pauli",

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
    "stade laval": "laval",
    "stade lavallois": "laval",
    "dijon fco": "dijon",
    "stade de reims": "reims",
    "as monaco": "monaco",

    # Netherlands & Belgium
    "ajax amsterdam": "ajax",
    "az alkmaar": "az",
    "n.e.c.": "nijmegen",
    "nec": "nijmegen",
    "psv eindhoven": "psv",
    "union st.-gilloise": "st gillis",
    "union saint-gilloise": "st gillis",
    "royale union saint-gilloise": "st gillis",
    "rusg": "st gillis",
    "sint-truidense": "st truiden",
    "sint-truiden": "st truiden",
    "kaa gent": "gent",
    "standard liege": "standard",

    # Portugal
    "sporting cp": "sporting",
    "sporting lisbon": "sporting",
    "cs maritimo": "maritimo",
    "marítimo": "maritimo",
    "estrela": "estrela amadora",
    "estrela da amadora": "estrela amadora",
    "cf estrela": "estrela amadora",
    "académico de viseu": "ac viseu",
    "academico de viseu": "ac viseu",

    # Belgium (Pro League)
    "raal la louviere": "la louviere",
    
    # Europe
    "heart of midlothian": "hearts",
    "lillestrom": "lillestrøm",
    "lillestrom sk": "lillestrøm",
    "lech poznan": "lech",
    "kks lech poznan": "lech",
    "jagiellonia bialystok": "jagiellonia",
    "sparta prague": "sparta",
    "olympiacos": "olympiakos",
    "olympiacos fc": "olympiakos",
    "ofi crete": "ofi",
    "levski sofia": "levski",
    "hapoel be'er": "beer sheva",
    "slavia prague": "slavia praha",
    "bodo/glimt": "bodoglimt",
    "bodø/glimt": "bodoglimt",
    "f.c. kobenhavn": "fc kobenhavn",
    "f.c. copenhagen": "fc kobenhavn",
    "copenhagen": "fc kobenhavn",
    "agf": "aarhus",
    "aarhus gf": "aarhus",
    "pafos": "paphos",
    "pafos fc": "paphos",
    "riga": "fk riga",
    "riga fc": "fk riga",
    "egnatia": "egnatia rrogozhine",
    "egnatia rrogozhire": "egnatia rrogozhine",
    "kairat": "fk kairat",
    "kairat almaty": "fk kairat",
    "fc kairat": "fk kairat",
    "iberia 1999": "saburtalo",
    "iberia 1999 tbilisi": "saburtalo",
    "amed sfk": "amed sk",
}

# ==========================================
# STRING & BADGE UTILITIES
# ==========================================
def strip_accents(s: str) -> str:
    """Removes diacritics, expands ligatures, and normalizes to lower case."""
    s = s.lower().strip()
    
    # Handle European characters that NFD ignores
    replacements = {
        "æ": "ae",
        "ø": "o",
        "œ": "oe",
        "ß": "ss",
        "đ": "d",  # Balkan (e.g., Bodø/Glimt, or Ludogorets/Dinamo match-ups)
        "ð": "d",  # Icelandic/Faroese
        "þ": "th", # Icelandic
        "ł": "l",  # Polish (e.g., Jagiellonia Białystok)
        "å": "a"   # Swedish/Danish/Norwegian
    }
    
    for char, replacement in replacements.items():
        s = s.replace(char, replacement)
        
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def format_league_badge(league_name: str) -> str:
    short = LEAGUE_DISPLAY_NAMES.get(league_name)
    if not short:
        short = league_name.replace("English ", "").replace("Spanish ", "").replace("German ", "")
    return f"[{short}]"

# ==========================================
# 1. CLUB ELO ENGINE (OFFICIAL CSV API)
# ==========================================
class EloEngine:
    def __init__(self, cache_file=CACHE_FILE):
        self.cache_file = cache_file
        self.ratings = {}
        self.stripped_ratings = {}
        self._load_or_fetch()

    def _load_or_fetch(self):
        # 1. Check cache age before hitting the network (7-day cooldown)
        cooldown_days = 7
        cache_valid = False
        
        if os.path.exists(self.cache_file):
            file_age_seconds = datetime.now().timestamp() - os.path.getmtime(self.cache_file)
            if file_age_seconds < (cooldown_days * 86400):
                cache_valid = True

        cached_ratings = self._load_cache()

        # If cache is fresh, ride the cache and skip network requests
        if cache_valid and len(cached_ratings) > 100:
            print(f"Notice: Cache is less than {cooldown_days} days old. Riding the cache to avoid hammering ClubElo.")
            self.ratings = cached_ratings
        else:
            # 2. Try official daily CSV API endpoints
            fresh_ratings = self._fetch_clubelo_csv()

            # 3. If CSV failed, extract from https://clubelo.com/Ranking
            if not fresh_ratings or len(fresh_ratings) < 100:
                fresh_ratings = self._fetch_clubelo_ranking_page()

            # 4. If any remote method succeeded, update cache on disk
            if fresh_ratings and len(fresh_ratings) > 100:
                cached_ratings.update(fresh_ratings)
                self.ratings = cached_ratings
                self._save_cache(self.ratings)
            else:
                print("Notice: Online fetches yielded no data. Loading disk cache fallback...")
                self.ratings = cached_ratings

        # Build lookup indices
        self.stripped_ratings = {strip_accents(k): v for k, v in self.ratings.items()}
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
                json.dump(data, f, indent=2)
            print(f"Successfully updated local cache ({len(data)} clubs).")
        except Exception as e:
            print(f"Notice: Failed to write cache to disk: {e}")

    def _fetch_clubelo_csv(self):
        """Fetches ClubElo official daily CSV table over HTTP."""
        now_dt = datetime.now(timezone.utc)
        session = requests.Session()
        retries = Retry(
            total=2,
            backoff_factor=1.0,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False,
        )
        session.mount("http://", HTTPAdapter(max_retries=retries))

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        endpoints = [
            f"http://api.clubelo.com/{(now_dt - timedelta(days=d)).strftime('%Y-%m-%d')}"
            for d in range(2)
        ]

        for url in endpoints:
            try:
                print(f"Fetching ClubElo daily CSV ({url})...")
                r = session.get(url, headers=headers, timeout=(4, 10))
                if r.status_code == 200 and len(r.text.strip()) > 500:
                    reader = csv.DictReader(io.StringIO(r.text.strip()))
                    ratings = {}
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
                        return ratings
            except Exception:
                pass
        return {}

    def _fetch_clubelo_ranking_page(self):
        """Fallback: Extracts club ratings from both the Vega JSON block and HTML table on clubelo.com/Ranking."""
        url = "https://clubelo.com/Ranking"
        
        print(f"Attempting fallback parse from {url} via curl_cffi...")
        
        try:
            # 'chrome120' perfectly mimics a real Chrome browser's TLS signature
            r = cffi_requests.get(url, impersonate="chrome120", timeout=15)
            
            if r.status_code != 200 or len(r.text) < 2000:
                print(f"Notice: Ranking page returned status {r.status_code} (length: {len(r.text)}).")
                return {}

            ratings = {}

            # 1. Parse from Vega JSON (Top 100 clubs)
            # ... [Keep all of your existing regex parsing logic below this line exactly as is]
            pattern_json = re.compile(
                r'\{[^{}]*?"(?:Elo"\s*:\s*([0-9.]+)[^{}]*?"Name"\s*:\s*"([^"]+)"|Name"\s*:\s*"([^"]+)"[^{}]*?"Elo"\s*:\s*([0-9.]+))[^{}]*?\}'
            )
            for match in pattern_json.finditer(r.text):
                elo1, name1, name2, elo2 = match.groups()
                name = name1 if name1 is not None else name2
                elo_str = elo1 if elo1 is not None else elo2
                if name and elo_str:
                    try:
                        clean_name = name.encode("utf-8").decode("unicode_escape").lower().strip()
                        ratings[clean_name] = round(float(elo_str))
                    except Exception:
                        continue

            # 2. Parse from HTML Table rows (covers 101 to ~1,700)
            # Row pattern looks like: <td><a href="...">Club Name</a></td> ... <td>1712</td>
            pattern_table = re.compile(
                r'<tr[^>]*>.*?<a[^>]*>([^<]+)</a>.*?<td[^>]*>([12][0-9]{3})</td>.*?</tr>',
                re.DOTALL
            )
            for match in pattern_table.finditer(r.text):
                name, elo_str = match.groups()
                name = name.lower().strip()
                if name and elo_str.isdigit():
                    ratings[name] = int(elo_str)

            if len(ratings) >= 50:
                print(f"Successfully extracted {len(ratings)} club ratings directly from Ranking page!")
                return ratings
            else:
                print(f"Notice: Extracted only {len(ratings)} ratings from Ranking page.")

        except Exception as e:
            print(f"Notice: Failed to parse Ranking page: {e}")

        return {}

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
            "fc", "cf", "sc", "afc", "cd", "sk", "nk", "vfl", "sv", "spvgg", "fk",
            "city", "town", "united", "wanderers", "rovers", "albion", "athletic",
            "linz", "athens"
        }
        
        # Strip periods alongside dashes/slashes so "f.c." becomes "fc" and is cleanly removed
        clean_alias_no_punct = clean_alias.replace("-", " ").replace("/", " ").replace(".", "")
        tokens_alias = [t for t in clean_alias_no_punct.split() if t not in noise_words]
        normalized_alias = " ".join(tokens_alias)
        if normalized_alias in self.stripped_ratings:
            return self.stripped_ratings[normalized_alias]

        clean_norm_no_punct = clean_norm.replace("-", " ").replace("/", " ").replace(".", "")
        tokens_norm = [t for t in clean_norm_no_punct.split() if t not in noise_words]
        normalized_norm = " ".join(tokens_norm)
        if normalized_norm in self.stripped_ratings:
            return self.stripped_ratings[normalized_norm]

        # 5. Condensation check
        condensed = clean_alias.replace("-", "").replace(" ", "").replace("'", "").replace("/", "").replace(".", "")
        if condensed in self.condensed_ratings:
            return self.condensed_ratings[condensed]

        # 6. Bi-directional Substring Match
        # Gathers all our cleaned ESPN names and ignores anything dangerously short (like "FC")
        search_terms = {clean_norm, clean_alias, normalized_alias, normalized_norm}
        search_terms = {t for t in search_terms if len(t) >= 5}

        # Prevent generic standalone names (used by giants) from hijacking lower-tier clubs
        danger_keys = {
            "atletico", "athletic", "real", "sporting", "racing", 
            "dynamo", "dinamo", "spartak", "lokomotiv", "victoria", "viktoria"
        }

        for key, val in self.ratings.items():
            clean_key = strip_accents(key)
            
            # Direction A: Is the ClubElo name hidden inside the ESPN name?
            # We block danger_keys so 'atletico' doesn't hijack 'atletico pinatarense'
            if len(clean_key) >= 5 and clean_key not in danger_keys:
                for term in search_terms:
                    if clean_key in term:
                        return val
                        
            # Direction B: Is the ESPN name hidden inside the ClubElo name?
            # (e.g., ESPN's 'egnatia' hidden in ClubElo's 'egnatia rrogozhine')
            for term in search_terms:
                if term in clean_key:
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
    start_d = window["start_local"].date()
    date_keys = {(start_d + timedelta(days=i)).strftime("%Y%m%d") for i in range(3)}

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

        watch_url = None
        for link in event.get("links", []):
            if "watch" in link.get("rel", []):
                watch_url = link.get("href")
                break

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
                
        home_elo_raw = elo_engine.get(home_team) # Returns 0 if unrated
        away_elo_raw = elo_engine.get(away_team)
        
        # Apply 1200 fallback for unrated cup teams, 1450 for unrated league teams
        is_cup = any(k in league_name.lower() for k in ["cup", "copa", "pokal", "coppa", "uefa.europa.conf"])
        
        calc_home = (1200 if is_cup else 1450) if home_elo_raw == DEFAULT_ELO else home_elo_raw
        calc_away = (1200 if is_cup else 1450) if away_elo_raw == DEFAULT_ELO else away_elo_raw

        avg_elo = (calc_home + calc_away) / 2.0
        diff_elo = abs(calc_home - calc_away)
        sort_value = round(avg_elo - diff_elo, 1)
        
        # Format explicitly for the frontend exporters
        disp_home = "Unrated" if home_elo_raw == DEFAULT_ELO else str(home_elo_raw)
        disp_away = "Unrated" if away_elo_raw == DEFAULT_ELO else str(away_elo_raw)

        seen_ids.add(event_id)
        matches.append({
            "day_group": day_group,
            "league": league_name,
            "home_team": home_team,
            "away_team": away_team,
            "home_elo": home_elo_raw,  # Kept as raw int in case you add numeric gates later
            "away_elo": away_elo_raw,
            "home_elo_disp": disp_home,
            "away_elo_disp": disp_away,
            "channels": channels,
            "match_time_dt": local_dt,
            "match_time_str": local_dt.strftime("%Y-%m-%d %I:%M %p %Z"),
            "sort_value": sort_value,
            "tv_assignment": None,
            "watch_url": watch_url,
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

            tv_label = m.get("tv_assignment") or "Other Screen"
            if m.get("is_lookin") and m.get("lookin_window"):
                tv_label = f"{m['tv_assignment']} [{m['lookin_window']}]"

            # Uses safe fallback to standard elo if the _disp keys are missing
            home_disp = m.get('home_elo_disp', m.get('home_elo'))
            away_disp = m.get('away_elo_disp', m.get('away_elo'))

            lines.append(
                f"  [{tv_label:<24}] {format_league_badge(m['league']):<14} "
                f"{m['home_team']} vs {m['away_team']:<25} ({home_disp} vs {away_disp}) "
                f"Score: {m['sort_value']:<6} [{m['channels']}]"
            )

    output = "\n".join(lines)
    print("\n" + output)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(output)

def resolve_stream_url(channels: str, league: str, home: str = "", away: str = "", watch_url: str = None) -> str:
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
        # Return the direct link exactly as ESPN provided it
        if watch_url:
            return watch_url
            
        # Fallback to catalog links if the game is too far out for a stream link
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
    /* Floating / Pinned Live Tray */
    #live-tray-wrapper {{
      margin-bottom: 24px;
      display: none; /* hidden if no live games */
    }}
    .tray-title {{
      font-size: 0.85rem;
      font-weight: 700;
      letter-spacing: 1px;
      text-transform: uppercase;
      color: #e50914;
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 8px;
      padding-left: 4px;
    }}
    .pulse-dot {{
      width: 8px;
      height: 8px;
      background: #e50914;
      border-radius: 50%;
      box-shadow: 0 0 8px #e50914;
      animation: pulse 1.5s infinite;
    }}
    @keyframes pulse {{
      0% {{ opacity: 1; transform: scale(1); }}
      50% {{ opacity: 0.4; transform: scale(1.2); }}
      100% {{ opacity: 1; transform: scale(1); }}
    }}
    #live-tray .card {{
      border-left: 4px solid #e50914;
      background: #1f1f27;
      margin-bottom: 10px;
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
  <div id="live-tray-wrapper">
    <div class="tray-title">
      <span class="pulse-dot"></span>
      Active Screen Assignments (Live Now)
    </div>
    <div id="live-tray"></div>
  </div>
"""
    if not matches:
        html += """
  <div class="day-header">NO SCHEDULED MATCHES</div>
  <div class="card" style="text-align: center; color: #888; padding: 24px 16px;">
    No televised fixtures scheduled for today or tomorrow.
  </div>
"""
    else:
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

                # 2. Clickable tap-to-stream link
                stream_url = resolve_stream_url(m["channels"], m["league"], m["home_team"], m["away_team"], m.get("watch_url"))
                if stream_url:
                    channel_html = f'<a href="{stream_url}" target="_blank" rel="noopener noreferrer" class="channel">{m["channels"]} &#8599;</a>'
                else:
                    channel_html = f'<span class="channel">{m["channels"]}</span>'

                # Calculate start and end ISO strings for the dynamic Live Tray
                start_iso = m["match_time_dt"].isoformat()
                end_iso = (m["match_time_dt"] + timedelta(minutes=MATCH_DURATION_MINUTES)).isoformat()

                html += f"""
            <div class="card" data-start="{start_iso}" data-end="{end_iso}">
            <div class="card-top">
                <div class="badge-container">
                <span class="badge {tv_class}">{base_tv}</span>
                {lookin_pill}
                </div>
                <span class="comp">{format_league_badge(m['league'])}</span>
            </div>
            <div class="matchup">{m['home_team']} vs {m['away_team']}</div>
            <div class="meta">
                <span>Elos: {m['home_elo_disp']} vs {m['away_elo_disp']} (Score: {m['sort_value']})</span>
                {channel_html}
            </div>
            </div>
            """
    html += """
  <script>
    document.addEventListener("DOMContentLoaded", () => {
      const now = new Date().getTime();
      const trayWrapper = document.getElementById("live-tray-wrapper");
      const tray = document.getElementById("live-tray");
      const allCards = document.querySelectorAll(".card[data-start]");

      const activeTvMap = {};

      allCards.forEach(card => {
        const tvBadge = card.querySelector(".badge");
        if (!tvBadge || tvBadge.classList.contains("tv-other") || tvBadge.textContent.includes("Other")) {
          return;
        }

        const tvName = tvBadge.textContent.trim();
        let start = new Date(card.dataset.start).getTime();
        let end = new Date(card.dataset.end).getTime();

        // If this match is a Look-in on this TV, adjust start/end to the look-in window!
        const lookinEl = card.querySelector(".badge-lookin, .lookin-pill");
        if (lookinEl && lookinEl.textContent.includes("-")) {
          try {
            // Text looks like: "Look-in: 10:55 AM - 11:25 AM"
            const rawTimes = lookinEl.textContent.replace(/Look-in:/i, "").trim();
            const [startStr, endStr] = rawTimes.split("-").map(s => s.trim());

            // Build full Date objects using the card's calendar date
            const baseDate = new Date(card.dataset.start);
            const datePrefix = baseDate.toLocaleDateString("en-US");

            const parsedStart = new Date(`${datePrefix} ${startStr}`).getTime();
            const parsedEnd = new Date(`${datePrefix} ${endStr}`).getTime();

            if (!isNaN(parsedStart) && !isNaN(parsedEnd)) {
              start = parsedStart;
              end = parsedEnd;
            }
          } catch (e) {
            // Fall back to kickoff timestamps if parsing fails
          }
        }

        // 1. MUST currently be within its on-screen air window
        if (now >= start && now <= end) {
          if (!activeTvMap[tvName]) {
            activeTvMap[tvName] = { card, start };
          } else {
            // If two overlap, award to the later start time
            if (start > activeTvMap[tvName].start) {
              activeTvMap[tvName] = { card, start };
            }
          }
        }
      });

      const activeCards = Object.values(activeTvMap).map(item => item.card);

      activeCards.sort((a, b) => {
        const aName = a.querySelector(".badge").textContent;
        const bName = b.querySelector(".badge").textContent;
        return aName.localeCompare(bName, undefined, { numeric: true });
      });

      if (activeCards.length > 0) {
        activeCards.forEach(card => {
          const clone = card.cloneNode(true);
          tray.appendChild(clone);
        });

        trayWrapper.style.display = "block";
        window.scrollTo(0, 0);
        return;
      }

      // Fallback: 2-hour auto-scroll if nothing on physical TVs
      const lookbackMs = 120 * 60 * 1000;
      const targetTime = now - lookbackMs;
      const headers = Array.from(document.querySelectorAll(".slot-header[data-time]"));
      let anchorTarget = null;

      for (const el of headers) {
        const slotTime = new Date(el.dataset.time).getTime();
        if (slotTime >= targetTime) {
          anchorTarget = el;
          break;
        }
      }

      if (anchorTarget) {
        anchorTarget.scrollIntoView({ behavior: "smooth", block: "start" });
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
        print("No matches found in this 48-hour window. Generating empty slate artifacts...")
        chronological_matches = []
    else:
        allocate_screens(matches)
        chronological_matches = sorted(matches, key=lambda m: (m["match_time_dt"], -m["sort_value"]))

    export_csv(chronological_matches)
    export_text_summary(chronological_matches, window)
    export_mobile_html(chronological_matches, window)
    print(f"\nArtifacts generated: {OUTPUT_CSV}, {OUTPUT_TXT}, {OUTPUT_HTML}")

if __name__ == "__main__":
    main()