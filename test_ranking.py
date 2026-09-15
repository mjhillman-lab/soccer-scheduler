import requests
from bs4 import BeautifulSoup

url = "https://clubelo.com/Ranking"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

r = requests.get(url, headers=headers)
soup = BeautifulSoup(r.text, "html.parser")

rows = soup.find_all("tr")
print(f"Total <tr> tags on /Ranking: {len(rows)}")

count = 0
for tr in rows:
    txt = tr.get_text(strip=True)
    if any(team in txt for team in ["Arsenal", "Real Madrid", "City", "Bayern"]):
        print("=== FOUND SAMPLE TARGET ROW ===")
        print(tr.prettify())
        break
    elif txt and count < 3:
        print(f"--- SAMPLE ROW {count} ---")
        print(tr.prettify())
        count += 1
