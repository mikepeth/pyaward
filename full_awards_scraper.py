import requests
from bs4 import BeautifulSoup
import csv
import logging
import re

# --------------------------
# CONFIG
# --------------------------

AWARDS = {

    "Oscars": {

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
    },

        "Best Original Screenplay": {
            "url": "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Original_Screenplay",
            "skip_tables": [9,10,11]
        },

        "Best Adapted Screenplay": {
            "url": "https://en.wikipedia.org/wiki/Academy_Award_for_Best_Adapted_Screenplay",
            "skip_tables": [11,12,13]
        }
    },

    "Golden Globes": {

    "Best Actor – Drama": {
        "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Actor_%E2%80%93_Motion_Picture_Drama",
        "skip_tables": []
    },

    "Best Actor – Musical or Comedy": {
        "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Actor_%E2%80%93_Motion_Picture_Musical_or_Comedy",
        "skip_tables": [8,9]
    },

    "Best Actress – Drama": {
        "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Actress_%E2%80%93_Motion_Picture_Drama",
        "skip_tables": [9,10]
    },

    "Best Actress – Musical or Comedy": {
        "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Actress_%E2%80%93_Motion_Picture_Musical_or_Comedy",
        "skip_tables": []
    },

        "Best Supporting Actor": {
            "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Supporting_Actor_%E2%80%93_Motion_Picture",
            "skip_tables": []
        },

        "Best Supporting Actress": {
            "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Supporting_Actress_%E2%80%93_Motion_Picture",
            "skip_tables": []
        },

        "Best Director": {
            "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Director",
            "skip_tables": []
        },

        "Best Screenplay": {
            "url": "https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Screenplay",
            "skip_tables": [9,10]
        }
    },

    "SAG": {

        "Best Actor": {
            "url": "https://en.wikipedia.org/wiki/Screen_Actors_Guild_Award_for_Outstanding_Performance_by_a_Male_Actor_in_a_Leading_Role",
            "skip_tables": [0,5]
        },

        "Best Actress": {
            "url": "https://en.wikipedia.org/wiki/Screen_Actors_Guild_Award_for_Outstanding_Performance_by_a_Female_Actor_in_a_Leading_Role",
            "skip_tables": [0,5]
        },

        "Best Supporting Actor": {
            "url": "https://en.wikipedia.org/wiki/Screen_Actors_Guild_Award_for_Outstanding_Performance_by_a_Male_Actor_in_a_Supporting_Role",
            "skip_tables": [0,5]
        },

        "Best Supporting Actress": {
            "url": "https://en.wikipedia.org/wiki/Screen_Actors_Guild_Award_for_Outstanding_Performance_by_a_Female_Actor_in_a_Supporting_Role",
            "skip_tables": [0,5]
        }
    },

    "Directors Guild": {

        "Best Director": {
            "url": "https://en.wikipedia.org/wiki/Directors_Guild_of_America_Award_for_Outstanding_Directing_%E2%80%93_Feature_Film",
            "skip_tables": []
        }
    },

    "Producers Guild": {

        "Best Picture": {
            "url": "https://en.wikipedia.org/wiki/Producers_Guild_of_America_Award_for_Best_Theatrical_Motion_Picture",
            "skip_tables": [5,6]
        }
    },

    "Critics Choice": {

        "Best Actor": {
            "url": "https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Actor",
            "skip_tables": []
        },

        "Best Actress": {
            "url": "https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Actress",
            "skip_tables": []
        },

        "Best Supporting Actor": {
            "url": "https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Supporting_Actor",
            "skip_tables": []
        },

        "Best Supporting Actress": {
            "url": "https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Supporting_Actress",
            "skip_tables": []
        },

        "Best Director": {
            "url": "https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Director",
            "skip_tables": []
        },

        "Best Screenplay": {
            "url": "https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Screenplay",
            "skip_tables": []
        }
    }
}

DEBUG = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger()

# --------------------------
# HELPERS
# --------------------------

def fetch_page(url):

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text

    except requests.RequestException as e:
        logger.error(f"Error fetching page: {e}")
        return None


def clean_text(text):
    return re.sub(r"\[\w+\]", "", text).strip()


# --------------------------
# SCRAPER
# --------------------------

def scrape_category(html, award_body, category_name, skip_tables=[]):

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})

    data = []
    current_year = None

    for table_index, table in enumerate(tables):

        if table_index in skip_tables:
            continue

        rows = table.find_all("tr")

        for row_index, row in enumerate(rows):

            cols = row.find_all(["th", "td"])
            if not cols:
                continue

            th_col = row.find("th")

            if th_col and re.search(r"\d{4}", th_col.text):
                current_year = th_col.text.strip().split()[0]

            name_col = row.find("b") or row.find("td")
            if not name_col:
                continue

            nominee_name = clean_text(name_col.get_text())

            movie_col = row.find("i")
            movie_title = clean_text(movie_col.get_text()) if movie_col else ""

            winner = bool(row.find("b"))

            if nominee_name:

                record = {
                    "Award_Body": award_body,
                    "Category": category_name,
                    "Year": current_year,
                    "Nominee": nominee_name,
                    "Movie": movie_title,
                    "Winner": winner
                }

                data.append(record)

                # if DEBUG:
                #     print(record)
                if DEBUG:
                    print(f"[DEBUG] {category_name} | Table {table_index} Row {row_index+1} | "
                        f"Year: {current_year} | Nominee: '{nominee_name}' | Movie: '{movie_title}' | Winner: {winner}")

    logger.info(f"{award_body} | {category_name} -> {len(data)} rows")

    return data


# --------------------------
# SAVE CSV
# --------------------------

def save_csv(data, filename):

    fieldnames = ["Award_Body", "Category", "Year", "Nominee", "Movie", "Winner"]

    with open(filename, "w", newline="", encoding="utf-8") as f:

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in data:
            writer.writerow(row)

    logger.info(f"Saved {filename}")


# --------------------------
# MAIN
# --------------------------

def main():

    all_data = []

    for award_body, categories in AWARDS.items():

        for category_name, info in categories.items():

            logger.info(f"Scraping {award_body} - {category_name}")

            html = fetch_page(info["url"])
            if not html:
                continue

            rows = scrape_category(
                html,
                award_body,
                category_name,
                skip_tables=info.get("skip_tables", [])
            )

            all_data.extend(rows)

    save_csv(all_data, "major_awards.csv")


if __name__ == "__main__":
    main()