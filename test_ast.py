import requests
from bs4 import BeautifulSoup

url = "https://clubelo.com/Ranking"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

r = requests.get(url, headers=headers)
soup = BeautifulSoup(r.text, "html.parser")

clubs = {}
# Find every Ast span
for span in soup.find_all("span", class_="Ast"):
    name = span.get_text(strip=True).lower()
    # The parent <td> contains the team link; find the next sibling <td> which holds the rating
    td = span.find_parent("td")
    if td:
        next_td = td.find_next_sibling("td")
        if next_td and next_td.get_text(strip=True).isdigit():
            rating = int(next_td.get_text(strip=True))
            if rating >= 1000:
                clubs[name] = rating

print(f"Captured {len(clubs)} clubs!")
for test in ["arsenal", "real madrid", "bayern münchen", "dortmund", "newcastle", "forest"]:
    print(f"  {test}: {clubs.get(test, 'Missing')}")
