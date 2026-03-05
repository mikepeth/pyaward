import requests
import csv
import time
import os
from datetime import datetime

API_KEY = "6e2307437dc8bf4e637ad5ae53875510"
BASE_URL = "https://api.themoviedb.org/3"
START_YEAR = 1980
END_YEAR = 2025
TARGET_PER_YEAR = 500
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

def fetch_movies_for_year(year, target=500):
    movies = []
    page = 1
    max_pages = 25  # 20 results/page * 25 = 500

    print(f"  Fetching {year}...", end="", flush=True)

    while len(movies) < target and page <= max_pages:
        params = {
            "api_key": API_KEY,
            "sort_by": "popularity.desc",
            "primary_release_year": year,
            "include_adult": False,
            "include_video": False,
            "language": "en-US",
            "page": page,
        }
        try:
            resp = requests.get(f"{BASE_URL}/discover/movie", params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            total_pages = data.get("total_pages", 1)

            if not results:
                break

            for m in results:
                movies.append({
                    "year": year,
                    "tmdb_id": m.get("id"),
                    "title": m.get("title", "").replace("\n", " ").replace("\r", ""),
                    "original_title": m.get("original_title", "").replace("\n", " ").replace("\r", ""),
                    "release_date": m.get("release_date", ""),
                    "popularity": m.get("popularity", 0),
                    "vote_average": m.get("vote_average", 0),
                    "vote_count": m.get("vote_count", 0),
                    "overview": m.get("overview", "").replace("\n", " ").replace("\r", ""),
                    "genre_ids": "|".join(str(g) for g in m.get("genre_ids", [])),
                    "original_language": m.get("original_language", ""),
                    "poster_path": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "",
                    "backdrop_path": f"https://image.tmdb.org/t/p/w1280{m['backdrop_path']}" if m.get("backdrop_path") else "",
                    "adult": m.get("adult", False),
                })
                if len(movies) >= target:
                    break

            if page >= total_pages:
                break

            page += 1
            time.sleep(0.25)  # polite rate limiting

        except requests.exceptions.RequestException as e:
            print(f"\n    Error on page {page}: {e}")
            time.sleep(2)
            break

    print(f" {len(movies)} movies")
    return movies

def main():
    all_movies = []
    years = list(range(START_YEAR, END_YEAR + 1))

    print(f"Fetching top {TARGET_PER_YEAR} movies/year for {START_YEAR}–{END_YEAR}")
    print(f"Total years: {len(years)}\n")

    for year in years:
        movies = fetch_movies_for_year(year, TARGET_PER_YEAR)
        all_movies.extend(movies)

    # Write CSV
    output_path = os.path.join(OUTPUT_DIR, "tmdb_top500_per_year.csv")
    fieldnames = [
        "year", "tmdb_id", "title", "original_title", "release_date",
        "popularity", "vote_average", "vote_count", "overview",
        "genre_ids", "original_language", "poster_path", "backdrop_path", "adult"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_movies)

    print(f"\n✅ Done! {len(all_movies):,} total movies written to:")
    print(f"   {output_path}")

    # Summary
    print("\nSummary by decade:")
    for decade_start in range(1980, 2030, 10):
        decade_end = min(decade_start + 9, END_YEAR)
        count = sum(1 for m in all_movies if decade_start <= m["year"] <= decade_end)
        print(f"  {decade_start}s: {count:,} movies")

if __name__ == "__main__":
    main()
