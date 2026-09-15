import datetime
import zoneinfo # For timezone handling (Python 3.9+)
import requests
from bs4 import BeautifulSoup
import csv
import sys # Added for output redirection
import copy # Needed for deep copying schedules for fitness calculation

# --- Redirect print output to a file ---
output_file_name = 'tv_schedule_summary.txt'
original_stdout = sys.stdout # Store the original stdout
sys.stdout = open(output_file_name, 'w') # Redirect stdout to the file

# --- Logging setup ---
log_file_name = 'scraper_log.txt'
with open(log_file_name, 'a') as log_file:
    log_file.write(f"Script started at: {datetime.datetime.now()}\n")

# --- Web scraping setup ---
url = "http://clubelo.com/Fixtures"

try:
    page = requests.get(url)
    page.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
except requests.exceptions.RequestException as e:
    print(f"Error fetching URL: {e}")
    with open(log_file_name, 'a') as log_file:
        log_file.write(f"Script failed during URL fetch at: {datetime.datetime.now()} - Error: {e}\n\n")
    sys.stdout.close() # Close the redirected file before exiting
    sys.stdout = original_stdout # Restore original stdout
    exit() # Exit the script if the page can't be fetched

soup = BeautifulSoup(page.content, "html.parser")

y_coordinates = set()

# Find all 'text' tags with x='20' (home team names) to get their y-coordinates
home_teams_tags = soup.find_all('text', {'x': '20'})
for tag in home_teams_tags:
    y_coordinates.add(tag.get('y'))

# Discard known header Y-coordinates that are not actual match data
y_coordinates.discard('14')
y_coordinates.discard('32')

match_data = []
processed_matches = set() # To track and prevent duplicate matches

# --- Data Extraction Loop ---
for y in sorted(y_coordinates):
    home_team = soup.find('text', {'x': '20', 'y': y}).get_text(strip=True)
    away_team = soup.find('text', {'x': '235', 'y': y}).get_text(strip=True)
    time_tag = soup.find('text', {'x': '625', 'y': y})
    raw_match_time_str = time_tag.get_text(strip=True) if time_tag else '00h00Berlin time'

    match_identifier = (home_team, away_team, raw_match_time_str)
    if match_identifier in processed_matches:
        continue
    processed_matches.add(match_identifier)

    home_elo_tag = soup.find('text', {'x': '180', 'y': y})
    home_elo_text = home_elo_tag.get_text(strip=True) if home_elo_tag else '0'
    try:
        home_elo = int(home_elo_text)
    except ValueError:
        home_elo = 0

    away_elo_tag = soup.find('text', {'x': '395', 'y': y})
    away_elo_text = away_elo_tag.get_text(strip=True) if away_elo_tag else '0'
    try:
        away_elo = int(away_elo_text)
    except ValueError:
        away_elo = 0

    delta_elo_tag = soup.find('text', {'x': '450', 'y': y})
    try:
        delta_elo_text = delta_elo_tag.get_text(strip=True)
        delta_elo = int(delta_elo_text)
    except (AttributeError, ValueError):
        delta_elo = 0

    cleaned_time_str = raw_match_time_str.replace('Berlin time', '').strip().replace('h', ':')
    today_date = datetime.date.today()

    try:
        naive_dt_berlin = datetime.datetime.combine(today_date, datetime.datetime.strptime(cleaned_time_str, '%H:%M').time())
    except ValueError:
        naive_dt_berlin = datetime.datetime.combine(today_date, datetime.time(0, 0))
        print(f"Warning: Could not parse cleaned time '{cleaned_time_str}' for y={y}. Using 00:00.")

    try:
        berlin_tz = zoneinfo.ZoneInfo("Europe/Berlin")
        local_dt = naive_dt_berlin.replace(tzinfo=berlin_tz).astimezone()
    except Exception as e:
        print(f"Warning: Time conversion error for y={y} - {e}. Using fallback.")
        local_dt = naive_dt_berlin.replace(tzinfo=datetime.timezone.utc).astimezone()

    sort_value = ((home_elo + away_elo) / 2) - abs(delta_elo)

    match_data.append({
        'home_team': home_team,
        'away_team': away_team,
        'home_elo': home_elo,
        'away_elo': away_elo,
        'delta_elo': delta_elo,
        'match_time_dt': local_dt,
        'match_time_str': local_dt.strftime('%H:%M'),
        'sort_value': sort_value
    })

# --- Print all matches sorted by value to the summary file ---
print("--- All Scraped Matches (Sorted by Value) ---")
all_matches_sorted = sorted(match_data, key=lambda m: -m['sort_value'])
for match in all_matches_sorted:
    print(f"  {match['match_time_str']}: {match['home_team']} vs {match['away_team']} (Sort: {match['sort_value']:.2f})")
print("\n" + "="*50 + "\n")

# --- Dynamic TV Assignment Logic ---
num_tvs = 3
match_duration = datetime.timedelta(hours=2)
other_matches = []
priority_sorted_matches = sorted(match_data, key=lambda m: -m['sort_value'])
tv_schedules = {i: [] for i in range(1, num_tvs + 1)}

print("\n--- TV Assignment Process ---")
print("Assigning matches with 'Best Fit' logic...")

def check_conflict(match_to_check, schedule):
    start_time = match_to_check['match_time_dt']
    end_time = start_time + match_duration
    for scheduled_match in schedule:
        scheduled_start = scheduled_match['match_time_dt']
        scheduled_end = scheduled_start + match_duration
        if start_time < scheduled_end and end_time > scheduled_start:
            return True
    return False

def calculate_fitness(schedule_on_tv, day_start, day_end):
    if not schedule_on_tv:
        return (day_end - day_start).total_seconds()
    
    sorted_schedule = sorted(schedule_on_tv, key=lambda m: m['match_time_dt'])
    gaps = []
    if not day_start:
        day_start = sorted_schedule[0]['match_time_dt'] - datetime.timedelta(hours=1)
        day_end = sorted_schedule[-1]['match_time_dt'] + datetime.timedelta(hours=3)

    gaps.append((sorted_schedule[0]['match_time_dt'] - day_start).total_seconds())
    for i in range(len(sorted_schedule) - 1):
        gap_start = sorted_schedule[i]['match_time_dt'] + match_duration
        gap_end = sorted_schedule[i+1]['match_time_dt']
        gaps.append((gap_end - gap_start).total_seconds())
    gaps.append((day_end - (sorted_schedule[-1]['match_time_dt'] + match_duration)).total_seconds())
    
    return max(gaps) if gaps else 0

for match_to_schedule in priority_sorted_matches:
    possible_placements = []

    for tv_num in sorted(tv_schedules.keys()):
        if not check_conflict(match_to_schedule, tv_schedules[tv_num]):
            possible_placements.append({'type': 'simple', 'tv': tv_num, 'match': match_to_schedule})
            continue

        conflicting_matches = [m for m in tv_schedules[tv_num] if check_conflict(match_to_schedule, [m])]
        can_shuffle_all = all(c['sort_value'] < match_to_schedule['sort_value'] for c in conflicting_matches)
        
        if can_shuffle_all:
            shuffle_plan = []
            temp_schedules = copy.deepcopy(tv_schedules)
            temp_schedules[tv_num] = [m for m in temp_schedules[tv_num] if m not in conflicting_matches]
            
            all_conflicts_rehomed = True
            for conflict in conflicting_matches:
                found_new_home = False
                for other_tv in sorted(temp_schedules.keys()):
                    if other_tv == tv_num: continue
                    if not check_conflict(conflict, temp_schedules[other_tv]):
                        temp_schedules[other_tv].append(conflict)
                        shuffle_plan.append({'match': conflict, 'from_tv': tv_num, 'to_tv': other_tv})
                        found_new_home = True
                        break
                if not found_new_home:
                    all_conflicts_rehomed = False
                    break
            
            if all_conflicts_rehomed:
                possible_placements.append({'type': 'shuffle', 'tv': tv_num, 'match': match_to_schedule, 'plan': shuffle_plan})

    best_placement = None
    best_fitness_score = -1

    if possible_placements:
        day_start_time = min(m['match_time_dt'] for m in match_data) if match_data else datetime.datetime.now()
        day_end_time = max(m['match_time_dt'] for m in match_data) + match_duration if match_data else datetime.datetime.now()

        for placement in possible_placements:
            temp_schedules = copy.deepcopy(tv_schedules)
            target_tv = placement['tv']

            if placement['type'] == 'shuffle':
                for move in placement['plan']:
                    match_to_move = next(m for m in temp_schedules[move['from_tv']] if m['home_team'] == move['match']['home_team'] and m['away_team'] == move['match']['away_team'])
                    temp_schedules[move['from_tv']].remove(match_to_move)
                    temp_schedules[move['to_tv']].append(match_to_move)
            
            temp_schedules[target_tv].append(placement['match'])
            
            fitness = calculate_fitness(temp_schedules[target_tv], day_start_time, day_end_time)

            if fitness > best_fitness_score:
                best_fitness_score = fitness
                best_placement = placement

    if best_placement:
        if best_placement['type'] == 'simple':
            tv_schedules[best_placement['tv']].append(best_placement['match'])
            print(f"Scheduled on TV {best_placement['tv']}: {best_placement['match']['home_team']} vs {best_placement['match']['away_team']}")
        elif best_placement['type'] == 'shuffle':
            print(f"Optimizing schedule for {best_placement['match']['home_team']}...")
            for move in best_placement['plan']:
                match_to_move = next(m for m in tv_schedules[move['from_tv']] if m['home_team'] == move['match']['home_team'] and m['away_team'] == move['match']['away_team'])
                tv_schedules[move['from_tv']].remove(match_to_move)
                tv_schedules[move['to_tv']].append(match_to_move)
                print(f"  -> Moved {move['match']['home_team']} from TV {move['from_tv']} to TV {move['to_tv']}")
            tv_schedules[best_placement['tv']].append(best_placement['match'])
            print(f"  -> Scheduled {best_placement['match']['home_team']} on TV {best_placement['tv']}")
    else:
        other_matches.append(match_to_schedule)
        
final_tv_schedule = []
for tv_num, scheduled_matches in tv_schedules.items():
    for match in scheduled_matches:
        final_tv_schedule.append({'tv_number': f'TV {tv_num}', **match, 'scheduled_start_time': match['match_time_dt'], 'blocked_until': match['match_time_dt'] + match_duration})

# --- MODIFICATION START: Strategic Look-in Matchmaking ---
print("\n--- Strategically Assigning 'Look Ins' ---")
all_gaps = []
for tv_num in sorted(tv_schedules.keys()):
    tv_schedule_sorted = sorted([m for m in final_tv_schedule if m['tv_number'] == f'TV {tv_num}'], key=lambda x: x['scheduled_start_time'])
    
    if len(tv_schedule_sorted) > 1:
        for i in range(len(tv_schedule_sorted) - 1):
            gap_start = tv_schedule_sorted[i]['blocked_until']
            gap_end = tv_schedule_sorted[i+1]['scheduled_start_time']
            if (gap_end - gap_start) >= datetime.timedelta(minutes=30):
                all_gaps.append({'tv': tv_num, 'start': gap_start, 'end': gap_end, 'duration': (gap_end - gap_start)})

    if tv_schedule_sorted:
        last_match_end = tv_schedule_sorted[-1]['blocked_until']
        # Define an arbitrary end of day for the final gap calculation
        end_of_day = last_match_end.replace(hour=23, minute=59, second=59)
        all_gaps.append({'tv': tv_num, 'start': last_match_end, 'end': end_of_day, 'duration': (end_of_day - last_match_end)})

# Sort gaps by duration (longest first) and matches by sort value (highest first)
all_gaps.sort(key=lambda x: x['duration'], reverse=True)
available_look_in_matches = sorted(other_matches, key=lambda m: -m['sort_value'])
look_in_matches_added = []

# Match best matches to best gaps
# Match best matches to best gaps
look_in_matches_added = []
used_gaps = []
used_matches = []

while True:
    best_pairing = None
    best_score = -1

    # Find the single best possible pairing among all remaining matches and gaps
    for match in available_look_in_matches:
        if match in used_matches:
            continue
        for gap in all_gaps:
            if gap in used_gaps:
                continue

            match_start = match['match_time_dt']
            match_end = match_start + match_duration

            # Check for overlap
            if match_start < gap['end'] and match_end > gap['start']:
                look_in_start = max(match_start, gap['start'])
                look_in_end = min(match_end, gap['end'])

                if (look_in_end - look_in_start) >= datetime.timedelta(minutes=30):
                    # This is a valid pairing. Is it the best one we've found so far?
                    if match['sort_value'] > best_score:
                        best_score = match['sort_value']
                        best_pairing = {
                            'match': match,
                            'gap': gap,
                            'start': look_in_start,
                            'end': look_in_end
                        }
    
    # If we found a best pairing in the last pass, schedule it. Otherwise, we're done.
    if best_pairing:
        match = best_pairing['match']
        gap = best_pairing['gap']
        
        look_in_matches_added.append({
            'tv_number': f"TV {gap['tv']}",
            'home_team': f"Look in: {match['home_team']}",
            'away_team': match['away_team'],
            'sort_value': match['sort_value'],
            'scheduled_start_time': best_pairing['start'],
            'blocked_until': best_pairing['end']
        })
        print(f"Assigning Look In on TV {gap['tv']}: {match['home_team']} to best available gap from {best_pairing['start'].strftime('%H:%M')} to {best_pairing['end'].strftime('%H:%M')}")

        # Mark this match and gap as used so they aren't considered again
        used_matches.append(match)
        used_gaps.append(gap)
    else:
        # No more valid pairings can be made
        break

# Update the list of remaining matches
other_matches = [m for m in available_look_in_matches if m not in used_matches]

final_tv_schedule.extend(look_in_matches_added)
other_matches = available_look_in_matches # Update the list of remaining unassigned matches
# --- MODIFICATION END ---

final_tv_schedule_str = sorted(
    [{**item, 'scheduled_start_time': item['scheduled_start_time'].strftime('%H:%M'), 'blocked_until': item['blocked_until'].strftime('%H:%M')} for item in final_tv_schedule],
    key=lambda x: (int(x['tv_number'].split(' ')[1]), x['scheduled_start_time'])
)

print("\n--- Final TV Schedule Summary ---")
for tv_num in range(1, num_tvs + 1):
    tv_specific_matches = [m for m in final_tv_schedule_str if m['tv_number'] == f'TV {tv_num}']
    if tv_specific_matches:
        print(f"\n{f'TV {tv_num} Schedule:':<20}")
        for assignment in tv_specific_matches:
            print(f"  {assignment['scheduled_start_time']} - {assignment['blocked_until']}: {assignment['home_team']} vs {assignment['away_team']} (Sort: {assignment['sort_value']:.2f})")
    else:
        print(f"\n{f'TV {tv_num} Schedule:':<20} No matches assigned.")

if other_matches:
    print("\n--- Remaining Other Matches (Not on Main TVs) ---")
    for om in other_matches:
        print(f"  {om['match_time_str']}: {om['home_team']} vs {om['away_team']} (Sort: {om['sort_value']:.2f})")

tv_schedule_csv_name = "tv_assignment_schedule.csv"
tv_fieldnames = ['tv_number', 'home_team', 'away_team', 'sort_value', 'scheduled_start_time', 'blocked_until']
with open(tv_schedule_csv_name, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=tv_fieldnames)
    # Filter out extra keys before writing
    clean_schedule = [{k: v for k, v in row.items() if k in tv_fieldnames} for row in final_tv_schedule_str]
    writer.writeheader()
    writer.writerows(clean_schedule)

print(f"\nTV assignment schedule exported to {tv_schedule_csv_name}")

with open(log_file_name, 'a') as log_file:
    log_file.write(f"Script finished successfully at: {datetime.datetime.now()}\n\n")

sys.stdout.close()
sys.stdout = original_stdout
print(f"TV schedule summary saved to {output_file_name}")
