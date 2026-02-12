import requests
from bs4 import BeautifulSoup
import csv
import logging
import re

# --------------------------
# CONFIG
# --------------------------
CATEGORIES = {
    "Best Actor": {
        "url": "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Actor",
        "skip_tables": [0,12,13,14]  # Example skip for problematic tables
    },
    "Best Actress": {
        "url": "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Actress",
        "skip_tables": [0,12,13,14]
    },
    "Best Supporting Actor": {
        "url": "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Supporting_Actor",
        "skip_tables": [0,11,12,13]
    },
    "Best Supporting Actress": {
        "url": "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Supporting_Actress",
        "skip_tables": [0,11, 12, 13]
    },
    "Best Director": {
        "url": "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Director",
        "skip_tables": [0,12,13,14]
    }
}

DEBUG = True

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger()

# --------------------------
# HELPER FUNCTIONS
# --------------------------
def fetch_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/115.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logger.info(f"Fetched page successfully: {url}")
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching page: {e}")
        return None

def clean_text(text):
    """Remove footnotes like [1], [a]"""
    return re.sub(r"\[\w+\]", "", text).strip()

def scrape_category(html, category_name, skip_tables=[]):
    """Scrape winners/nominees from a Wikipedia category page"""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    if not tables:
        logger.error(f"No tables found for {category_name}")
        return []

    data = []
    current_year = None

    for table_index, table in enumerate(tables):
        if table_index in skip_tables:
            logger.info(f"Skipping table {table_index} for {category_name}")
            continue

        rows = table.find_all("tr")
        for row_index, row in enumerate(rows):
            cols = row.find_all(["th", "td"])
            if not cols:
                continue

            # Track year
            th_col = row.find("th")
            if th_col and re.search(r"\d{4}", th_col.text):
                current_year = th_col.text.strip().split()[0]

            # Skip header rows
            if row_index == 0 and category_name in row.text:
                continue

            # Nominee name
            name_col = row.find("b") or row.find("td")
            if not name_col:
                continue
            nominee_name = clean_text(name_col.get_text())

            # Movie title
            movie_col = row.find("i")
            movie_title = clean_text(movie_col.get_text()) if movie_col else ""

            # Determine winner
            winner = bool(row.find("b"))

            if nominee_name:
                data.append({
                    "Year": current_year,
                    "Nominee": nominee_name,
                    "Movie": movie_title,
                    "Winner": winner
                })

            if DEBUG:
                print(f"[DEBUG] {category_name} | Table {table_index} Row {row_index+1} | "
                      f"Year: {current_year} | Nominee: '{nominee_name}' | Movie: '{movie_title}' | Winner: {winner}")

    logger.info(f"Scraped {len(data)} entries for {category_name}")
    return data

def save_csv(data, filename, fieldnames):
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    logger.info(f"Saved CSV: {filename}")

# --------------------------
# MAIN FUNCTION
# --------------------------
def main():
    for category_name, info in CATEGORIES.items():
        logger.info(f"Scraping category: {category_name}")
        html = fetch_page(info["url"])
        if not html:
            continue

        data = scrape_category(html, category_name, skip_tables=info.get("skip_tables", []))
        if not data:
            continue

        csv_filename = f"{category_name.replace(' ', '_')}.csv"
        save_csv(data, csv_filename, ["Year", "Nominee", "Movie", "Winner"])

if __name__ == "__main__":
    main()
