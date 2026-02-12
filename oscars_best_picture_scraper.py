import requests
from bs4 import BeautifulSoup
import csv
import logging
import re

# --------------------------
# CONFIG
# --------------------------
URL = "https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Picture"
OUTPUT_CSV = "best_picture_winners_nominees.csv"
DEBUG = True  # Set to True to see debug output for every row

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger()

# --------------------------
# FUNCTIONS
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

def clean_title(title):
    """Remove footnote markers like [1], [a], etc."""
    return re.sub(r"\[\w+\]", "", title).strip()

def scrape_movies(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    if not tables:
        logger.error("No tables found.")
        return []

    data = []
    current_year = None

    for table_index, table in enumerate(tables):
        # Skip table 12 and 13 (indexing from 0)
        if table_index in [11, 12]:
            logger.info(f"Skipping table {table_index + 1}")
            continue

        logger.info(f"Processing table {table_index + 1}")
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
            if row_index == 0 and "Film" in row.text:
                continue

            # Get title
            title_col = row.find("i") or row.find("td")
            if not title_col:
                continue
            title_text = clean_title(title_col.get_text())

            # Determine winner
            winner = False
            if title_col.find("b") or title_col.find("span", {"style":"font-weight:bold"}):
                winner = True

            if title_text:
                data.append({
                    "Year": current_year,
                    "Title": title_text,
                    "Winner": winner
                })

            # DEBUG: Print every row parsed
            if DEBUG:
                print(f"[DEBUG] Table {table_index+1} Row {row_index+1} | Year: {current_year} | Title: '{title_text}' | Winner: {winner}")

    logger.info(f"Scraped {len(data)} movies total")
    return data

def save_csv(data, filename):
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Year", "Title", "Winner"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for movie in data:
            writer.writerow(movie)
    logger.info(f"Saved data to CSV: {filename}")

# --------------------------
# MAIN
# --------------------------
def main():
    html = fetch_page(URL)
    if not html:
        logger.error("No HTML content retrieved. Exiting.")
        return

    data = scrape_movies(html)
    if not data:
        logger.error("No movies scraped. Exiting.")
        return

    save_csv(data, OUTPUT_CSV)

if __name__ == "__main__":
    main()
